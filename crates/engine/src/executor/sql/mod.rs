use serde_json::Value as Json;
use common::types::sources::SourceConnArgs;
use database_adapters::{AsyncDatabaseAdapter, DatabaseAdapter};
use sqlparser::ast::{CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, Ident, Statement, ValueWithSpan};
use crate::{CatalogError, ConnectorMeta, ConnectorType, PipelineMeta, TransformMeta};
use crate::executor::{ExecutorError, ExecutorResponse};
use crate::executor::kafka::{KafkaConnectorDeployConfig, KafkaDeploy, KafkaExecutor};
use crate::registry::{Catalog, CatalogHelpers, MemoryCatalog};

trait AstValueFormatter {
    fn formatted_string(&self) -> String;
}
impl AstValueFormatter for ValueWithSpan {
    fn formatted_string(&self) -> String {
        if self.value.to_string().starts_with("'") && self.value.to_string().ends_with("'") {
            return self.value.to_string().trim_matches('\'').to_string();
        };
        if self.value.to_string().starts_with('"') && self.value.to_string().ends_with('"') {
            return self.value.to_string().trim_matches('"').to_string();
        }
        self.value.to_string()
    }
}

pub struct SqlExecutor;


pub struct KvPairs(pub Vec<(Ident, ValueWithSpan)>);
impl From<KvPairs> for Json {
    fn from(kvs: KvPairs) -> Self {
        use serde_json::{Map, Value};
        let mut obj = Map::new();
        for (k, vws) in kvs.0 {
            obj.insert(k.value, Value::String(vws.formatted_string()));
        }
        Value::Object(obj)
    }
}

fn handle_put_op(res: Result<(), CatalogError>) -> Result<(), ExecutorError> {
    match res {
        Ok(_) => Ok(()),
        Err(e) => match e {
            CatalogError::Duplicate => Ok(()),
            _ => Err(e.into()),
        },
    }
}

impl SqlExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute<S>(
        parsed_stmts: S,
        registry: &MemoryCatalog,
        source_conn_args: SourceConnArgs,
        mut target_db_adapter: Option<Box<dyn AsyncDatabaseAdapter>>,
    ) -> Result<ExecutorResponse, ExecutorError> 
    where 
        S: IntoIterator<Item = Statement>,
    {

        for ast in parsed_stmts.into_iter() {
            match ast {
                Statement::CreateKafkaConnector(stmt) => {
                    let kafka_executor = if let Some(ref connect_host) = source_conn_args.kafka_connect {
                        KafkaExecutor::new(connect_host)
                    } else {
                        return Err(ExecutorError::ConfigError("No Kafka Connect host provided".to_string()))
                    };
                    Self::execute_create_kafka_connector_if_not_exists(&kafka_executor, stmt, registry).await?;
                }
                Statement::CreateSMTransform(stmt) => {
                    Self::execute_create_simple_message_transform_if_not_exists(stmt, registry.clone())?;
                }
                Statement::CreateSMTPipeline(stmt) => {
                    Self::execute_create_simple_message_transform_pipeline_if_not_exists(stmt, registry.clone())?;
                }
                _ => {
                    let db_adapter = match target_db_adapter.as_mut() {
                        Some(adapter) => adapter,
                        None => {
                            return Err(ExecutorError::ConfigError(
                                "No database adapter provided".to_string(),
                            ));
                        }
                    };

                    Self::execute_async_db_query(ast, db_adapter).await?
                }
            }
        }
        
        Ok(ExecutorResponse::Ok)
    }

    pub async fn execute_async_db_query(
        ast: Statement,
        db_adapter: &mut Box<dyn AsyncDatabaseAdapter>,
    ) -> Result<(), ExecutorError> {
        db_adapter.execute(&ast.to_string()).await?;
        Ok(())
    }

    pub fn execute_create_simple_message_transform_if_not_exists(
        smt_pipe: CreateSimpleMessageTransform,
        registry: MemoryCatalog,
    ) -> Result<(), ExecutorError> {
        let config: Json = KvPairs(smt_pipe.config).into();
        let meta = TransformMeta::new(smt_pipe.name.to_string(), config);
        handle_put_op(registry.put_transform(meta))
    }

    pub fn execute_create_simple_message_transform_pipeline_if_not_exists(
        smt_pipe: CreateSimpleMessageTransformPipeline,
        registry: MemoryCatalog,
    ) -> Result<(), ExecutorError> {
        let t_names = smt_pipe
            .steps
            .iter()
            .map(|s| s.name.to_string())
            .collect::<Vec<String>>();
        let t_ids = registry.get_transform_ids_by_name(t_names)?;
        let pred = match smt_pipe.pipe_predicate {
            Some(p) => Some(p.formatted_string()),
            None => None,
        };
        let pipe = PipelineMeta::new(smt_pipe.name.value, t_ids, pred);
        handle_put_op(registry.put_pipeline(pipe))
    }

    pub async fn execute_create_kafka_connector_if_not_exists<K>(
        kafka_executor: &K,
        connector_config: CreateKafkaConnector,
        registry: &MemoryCatalog,
    ) -> Result<ExecutorResponse, ExecutorError>
    where
        K: KafkaDeploy
    {
        let conn_name = connector_config.name.to_string();
        let _conn_type = connector_config.connector_type; // currently unused
        let props: Json = KvPairs(connector_config.with_properties).into();

        use serde_json::{Map, Value};
        let mut obj = match props {
            Value::Object(map) => map,
            _ => Map::new(),
        };

        let mut transform_names = Vec::new();

        for pipe_ident in connector_config.with_pipelines {
            let pipe = registry.get_pipeline(pipe_ident.value.as_str())?;
            let pipe_name = pipe.name.clone();
            for t_id in pipe.transforms {
                let t = registry.get_transform(t_id)?;
                let tname = format!("{}_{}", pipe_name, t.name);
                transform_names.push(tname.clone());

                if let Value::Object(cfg) = t.config {
                    for (k, v) in cfg {
                        obj.insert(format!("transforms.{tname}.{k}"), v);
                    }
                }
                if let Some(pred) = &pipe.predicate {
                    obj.insert(
                        format!("transforms.{tname}.predicate"),
                        Value::String(pred.clone()),
                    );
                }
            }
        }

        if !transform_names.is_empty() {
            obj.insert(
                "transforms".to_string(),
                Value::String(transform_names.join(",")),
            );
        }

        let meta = ConnectorMeta {
            name: conn_name,
            plugin: ConnectorType::Kafka,
            config: Value::Object(obj),
        };

        match registry.put_connector(meta.clone()) {
            Ok(_) => Ok(ExecutorResponse::from(
                kafka_executor.deploy_connector(
                    &KafkaConnectorDeployConfig::from(meta)
                ).await?)),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use async_trait::async_trait;
    use super::*;
    use sqlparser::ast::KafkaConnectorType::Source;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::tokenizer::{Location, Span};
    use uuid::Uuid;
    use database_adapters::DatabaseAdapterError;
    use crate::executor::kafka::{KafkaExecutorError, KafkaExecutorResponse};

    #[test]
    fn test_create_transform() {
        let smt = CreateSimpleMessageTransform {
            name: Ident {
                value: "cast_hash_cols_to_int".to_string(),
                quote_style: None,
                span: Span::new(Location::new(1, 33), Location::new(1, 54)),
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".to_string(),
                        quote_style: None,
                        span: Span::new(Location::new(2, 3), Location::new(2, 7)),
                    },
                    ValueWithSpan {
                        value: SingleQuotedString(
                            "org.apache.kafka.connect.transforms.Cast$Value".to_string(),
                        ),
                        span: Span::new(Location::new(2, 15), Location::new(2, 63)),
                    },
                ),
                (
                    Ident {
                        value: "spec".to_string(),
                        quote_style: None,
                        span: Span::new(Location::new(3, 3), Location::new(3, 7)),
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("${spec}".to_string()),
                        span: Span::new(Location::new(3, 15), Location::new(3, 24)),
                    },
                ),
                (
                    Ident {
                        value: "predicate".to_string(),
                        quote_style: None,
                        span: Span::new(Location::new(4, 3), Location::new(4, 12)),
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("${predicate}".to_string()),
                        span: Span::new(Location::new(4, 15), Location::new(4, 29)),
                    },
                ),
            ],
        };
        let registry = MemoryCatalog::new();
        let sql_exec = SqlExecutor::new();
        let res = SqlExecutor::execute_create_simple_message_transform_if_not_exists(
            smt,
            registry.clone(),
        )
        .unwrap();
        assert_eq!(res, ());
        let cat_smt = registry.get_transform("cast_hash_cols_to_int").unwrap();
        assert_eq!(cat_smt.name, "cast_hash_cols_to_int");
        assert_eq!(
            cat_smt.config["type"].as_str().unwrap(),
            "org.apache.kafka.connect.transforms.Cast$Value"
        );
        assert_eq!(cat_smt.config["spec"].as_str().unwrap(), "${spec}");
        assert_eq!(
            cat_smt.config["predicate"].as_str().unwrap(),
            "${predicate}"
        );
        println!("{:?}", cat_smt)
    }

    #[test]
    fn test_create_pipeline() {
        use serde_json::json;
        use sqlparser::ast::KafkaConnectorType::Source;

        // ── 1. set-up catalog + prerequisite transforms ────────────────────
        let registry = MemoryCatalog::new();

        // helper: create + insert transform, return its UUID
        let make_t = |name: &str| -> Uuid {
            let meta = TransformMeta::new(name.to_string(), json!({}));
            let id = meta.id;
            registry.put_transform(meta).unwrap();
            id
        };
        let id_hash_email = make_t("hash_email");
        let id_drop_pii = make_t("drop_pii");

        // ── 2. build pipeline AST (from your snippet) ───────────────────────
        use sqlparser::ast::{Ident, TransformCall, Value::SingleQuotedString};
        use sqlparser::tokenizer::{Location, Span};

        let pipe_ast = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "some_pipeline".into(),
                quote_style: None,
                span: Span::new(Location::new(2, 64), Location::new(2, 77)),
            },
            if_not_exists: true,
            connector_type: Source,
            steps: vec![
                TransformCall {
                    name: Ident {
                        value: "hash_email".into(),
                        quote_style: None,
                        span: Span::new(Location::new(3, 13), Location::new(3, 23)),
                    },
                    args: vec![(
                        Ident {
                            value: "email_addr_reg".into(),
                            quote_style: None,
                            span: Span::new(Location::new(3, 24), Location::new(3, 38)),
                        },
                        ValueWithSpan {
                            value: SingleQuotedString(".*@example.com".into()),
                            span: Span::new(Location::new(3, 41), Location::new(3, 57)),
                        },
                    )],
                },
                TransformCall {
                    name: Ident {
                        value: "drop_pii".into(),
                        quote_style: None,
                        span: Span::new(Location::new(4, 13), Location::new(4, 21)),
                    },
                    args: vec![(
                        Ident {
                            value: "fields".into(),
                            quote_style: None,
                            span: Span::new(Location::new(4, 22), Location::new(4, 28)),
                        },
                        ValueWithSpan {
                            value: SingleQuotedString("email_addr, phone_num".into()),
                            span: Span::new(Location::new(4, 31), Location::new(4, 54)),
                        },
                    )],
                },
            ],
            pipe_predicate: Some(ValueWithSpan {
                value: SingleQuotedString("some_predicate".into()),
                span: Span::new(Location::new(5, 35), Location::new(5, 51)),
            }),
        };

        // ── 3. execute helper ───────────────────────────────────────────────
        let res = SqlExecutor::execute_create_simple_message_transform_pipeline_if_not_exists(
            pipe_ast,
            registry.clone(),
        )
        .unwrap();
        assert_eq!(res, ());

        // ── 4. verify catalog contents ──────────────────────────────────────
        let cat_pipe = registry.get_pipeline("some_pipeline").unwrap();
        assert_eq!(cat_pipe.name, "some_pipeline");

        // depending on your struct: `transform_ids` or `steps`
        assert_eq!(cat_pipe.transforms, vec![id_hash_email, id_drop_pii]);

        assert_eq!(cat_pipe.predicate.as_deref(), Some("some_predicate"));
    }

    struct MockKafkaExecutor {
        called: Mutex<Vec<KafkaConnectorDeployConfig>>
    }

    #[async_trait]
    impl KafkaDeploy for MockKafkaExecutor {
        async fn deploy_connector(&self, cfg: &KafkaConnectorDeployConfig) -> Result<KafkaExecutorResponse, KafkaExecutorError> {
            self.called.lock().unwrap().push(cfg.clone());
            Ok(KafkaExecutorResponse::Ok)
        }
    }

    #[tokio::test]
    async fn test_create_kafka_connector() {
        use serde_json::json;

        let registry = MemoryCatalog::new();

        // two transforms used in two distinct pipelines
        let mask = TransformMeta::new("mask".into(), json!({"field": "a"}));
        let drop = TransformMeta::new("drop".into(), json!({"field": "b"}));
        registry.put_transform(mask.clone()).unwrap();
        registry.put_transform(drop.clone()).unwrap();

        let pipe1 = PipelineMeta::new("pipe1".into(), vec![mask.id], Some("pred1".into()));
        let pipe2 = PipelineMeta::new("pipe2".into(), vec![drop.id], Some("pred2".into()));
        registry.put_pipeline(pipe1).unwrap();
        registry.put_pipeline(pipe2).unwrap();

        let span = Span::new(Location::new(0, 0), Location::new(0, 0));
        let conn_name = Uuid::new_v4();
        let ast = CreateKafkaConnector {
            name: Ident {
                value: conn_name.into(),
                quote_style: None,
                span,
            },
            if_not_exists: true,
            connector_type: Source,
            with_properties: vec![
                (
                    Ident {
                        value: "a".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("1".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "connector.class".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("io.debezium.connector.postgresql.PostgresConnector".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "tasks.max".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("1".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "database.user".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("postgres".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "database.password".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("password".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "database.port".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("5432".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "database.hostname".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("postgres".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "database.dbname".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("foundry_dev".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "table.whitelist".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("public.test_connector_src".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "snapshot.mode".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("initial".into()),
                        span,
                    }
                ),
                (
                    Ident {
                        value: "topic.prefix".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("postgres-".into()),
                        span,
                    }
                ),
            ],
            with_pipelines: vec![
                Ident {
                    value: "pipe1".into(),
                    quote_style: None,
                    span,
                },
                Ident {
                    value: "pipe2".into(),
                    quote_style: None,
                    span,
                },
            ],
        };

        let connect_host = "http://localhost:8083";
        let mock_exec = MockKafkaExecutor { called: Mutex::new(vec![]) };

        let resp = SqlExecutor::execute_create_kafka_connector_if_not_exists(
            &mock_exec, ast, &registry,
        ).await.unwrap();
        assert_eq!(resp, ExecutorResponse::Ok);
        assert_eq!(mock_exec.called.lock().unwrap().len(), 1);



        let c = registry.get_connector(&conn_name.to_string()).unwrap();
        assert_eq!(c.name, conn_name.to_string());
        assert_eq!(c.plugin, ConnectorType::Kafka);
        assert_eq!(c.config["a"], "1");
        assert_eq!(c.config["transforms"], "pipe1_mask,pipe2_drop");
        assert_eq!(c.config["transforms.pipe1_mask.field"], "a");
        assert_eq!(c.config["transforms.pipe1_mask.predicate"], "pred1");
        assert_eq!(c.config["transforms.pipe2_drop.field"], "b");
        assert_eq!(c.config["transforms.pipe2_drop.predicate"], "pred2");
    }

    #[tokio::test]
    async fn test_execute_db_query() {
        use std::sync::{Arc, Mutex};
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        // shared recording buffer
        let executed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        // ── mock adapter that writes into the Arc ─────────────────────────
        struct MockDbAdapter {
            executed_sql: Arc<Mutex<Vec<String>>>,
        }
        
        #[async_trait]
        impl AsyncDatabaseAdapter for MockDbAdapter {
            async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
                self.executed_sql.lock().unwrap().push(sql.to_owned());
                Ok(())
            }
        }

        // ── build boxed trait object ───────────────────────────────────────
        let mut boxed: Box<dyn AsyncDatabaseAdapter> =
            Box::new(MockDbAdapter { executed_sql: executed.clone() });

        // ── run executor ───────────────────────────────────────────────────
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1;")
            .unwrap()
            .pop()
            .unwrap();
        SqlExecutor::execute_async_db_query(stmt.clone(), &mut boxed).await.unwrap();

        // ── assert via the shared Arc ──────────────────────────────────────
        let recorded = executed.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], stmt.to_string());
    }
}
