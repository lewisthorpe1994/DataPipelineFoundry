use crate::executor::kafka::{KafkaConnectorDeployConfig, KafkaDeploy, KafkaExecutor};
use crate::executor::{ExecutorError, ExecutorResponse};
use crate::registry::{Compile, MemoryCatalog, Register};
use crate::{CatalogError, PipelineDecl, TransformDecl};
use common::types::sources::SourceConnArgs;
use database_adapters::{AsyncDatabaseAdapter, DatabaseAdapter};
use serde_json::Value as Json;
use sqlparser::ast::{
    CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline,
    Ident, Statement, ValueWithSpan,
};

pub trait AstValueFormatter {
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

impl SqlExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute<S>(
        parsed_stmts: S,
        registry: &MemoryCatalog,
        source_conn_args: &SourceConnArgs,
        mut target_db_adapter: Option<&mut Box<dyn AsyncDatabaseAdapter>>,
    ) -> Result<ExecutorResponse, ExecutorError>
    where
        S: IntoIterator<Item = Statement>,
    {
        for ast in parsed_stmts.into_iter() {
            match ast {
                Statement::CreateKafkaConnector(stmt) => {
                    let kafka_executor =
                        if let Some(ref connect_host) = source_conn_args.kafka_connect {
                            KafkaExecutor::new(connect_host)
                        } else {
                            return Err(ExecutorError::ConfigError(
                                "No Kafka Connect host provided".to_string(),
                            ));
                        };
                    Self::execute_create_kafka_connector_if_not_exists(
                        &kafka_executor,
                        stmt,
                        registry,
                    )
                    .await?;
                }
                Statement::CreateTable(stmt) => {
                    let db_adapter = match target_db_adapter.as_mut() {
                        Some(adapter) => adapter,
                        None => {
                            return Err(ExecutorError::ConfigError(
                                "No database adapter provided".to_string(),
                            ));
                        }
                    };

                    Self::execute_async_db_query(Statement::CreateTable(stmt), db_adapter).await?
                }
                _ => {
                    return Err(ExecutorError::ConfigError(format!(
                        "{} not currently supported!",
                        ast
                    )))
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

    pub async fn execute_create_kafka_connector_if_not_exists<K>(
        kafka_executor: &K,
        connector_config: CreateKafkaConnector,
        registry: &MemoryCatalog,
    ) -> Result<ExecutorResponse, ExecutorError>
    where
        K: KafkaDeploy,
    {
        let conn = registry.compile_kafka_decl(&*connector_config.name.value)?;

        Ok(ExecutorResponse::from(
            kafka_executor
                .deploy_connector(&KafkaConnectorDeployConfig {
                    name: conn.name,
                    config: conn.config,
                })
                .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::kafka::{KafkaExecutorError, KafkaExecutorResponse};
    use crate::registry::Getter;
    use async_trait::async_trait;
    use database_adapters::DatabaseAdapterError;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::ast::{KafkaConnectorType, TransformCall};
    use sqlparser::tokenizer::{Location, Span};
    use std::sync::Mutex;
    use uuid::Uuid;

    struct MockKafkaExecutor {
        called: Mutex<Vec<KafkaConnectorDeployConfig>>,
    }

    #[async_trait]
    impl KafkaDeploy for MockKafkaExecutor {
        async fn deploy_connector(
            &self,
            cfg: &KafkaConnectorDeployConfig,
        ) -> Result<KafkaExecutorResponse, KafkaExecutorError> {
            self.called.lock().unwrap().push(cfg.clone());
            Ok(KafkaExecutorResponse::Ok)
        }
    }

    #[tokio::test]
    async fn test_create_kafka_connector() {
        let registry = MemoryCatalog::new();
        let smt_span = Span::new(Location::new(0, 0), Location::new(0, 0));

        let mask = CreateSimpleMessageTransform {
            name: Ident {
                value: "mask".to_string(),
                quote_style: None,
                span: Span::new(Location::new(1, 33), Location::new(1, 54)),
            },
            if_not_exists: false,
            config: vec![(
                Ident {
                    value: "field".to_string(),
                    quote_style: None,
                    span: smt_span,
                },
                ValueWithSpan {
                    value: SingleQuotedString("a".into()),
                    span: smt_span,
                },
            )],
        };

        let drop = CreateSimpleMessageTransform {
            name: Ident {
                value: "drop".to_string(),
                quote_style: None,
                span: Span::new(Location::new(1, 33), Location::new(1, 54)),
            },
            if_not_exists: false,
            config: vec![(
                Ident {
                    value: "field".to_string(),
                    quote_style: None,
                    span: smt_span,
                },
                ValueWithSpan {
                    value: SingleQuotedString("b".into()),
                    span: smt_span,
                },
            )],
        };

        // two transforms used in two distinct pipelines
        registry.register_kafka_smt(mask.clone()).unwrap();
        registry.register_kafka_smt(drop.clone()).unwrap();

        let pipe1 = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "pipe1".to_string(),
                quote_style: None,
                span: Span::new(Location::new(1, 33), Location::new(1, 54)),
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "mask".to_string(),
                    quote_style: None,
                    span: smt_span,
                },
                Vec::new(),
            )],
            pipe_predicate: Some(ValueWithSpan {
                value: SingleQuotedString("pred1".into()),
                span: smt_span,
            }),
        };

        let pipe2 = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "pipe2".to_string(),
                quote_style: None,
                span: Span::new(Location::new(1, 33), Location::new(1, 54)),
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "drop".to_string(),
                    quote_style: None,
                    span: smt_span,
                },
                Vec::new(),
            )],
            pipe_predicate: Some(ValueWithSpan {
                value: SingleQuotedString("pred2".into()),
                span: smt_span,
            }),
        };

        registry.register_smt_pipeline(pipe1).unwrap();
        registry.register_smt_pipeline(pipe2).unwrap();

        let span = Span::new(Location::new(0, 0), Location::new(0, 0));
        let conn_name = Uuid::new_v4();
        let ast = CreateKafkaConnector {
            name: Ident {
                value: conn_name.into(),
                quote_style: None,
                span,
            },
            if_not_exists: true,
            connector_type: KafkaConnectorType::Source,
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
                    },
                ),
                (
                    Ident {
                        value: "connector.class".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString(
                            "io.debezium.connector.postgresql.PostgresConnector".into(),
                        ),
                        span,
                    },
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
                    },
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
                    },
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
                    },
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
                    },
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
                    },
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
                    },
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
                    },
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
                    },
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
                    },
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
        let mock_exec = MockKafkaExecutor {
            called: Mutex::new(vec![]),
        };
        registry.register_kafka_connector(ast.clone()).unwrap();
        let resp =
            SqlExecutor::execute_create_kafka_connector_if_not_exists(&mock_exec, ast, &registry)
                .await
                .unwrap();
        assert_eq!(resp, ExecutorResponse::Ok);
        assert_eq!(mock_exec.called.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_db_query() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        use std::sync::{Arc, Mutex};

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
        let mut boxed: Box<dyn AsyncDatabaseAdapter> = Box::new(MockDbAdapter {
            executed_sql: executed.clone(),
        });

        // ── run executor ───────────────────────────────────────────────────
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1;")
            .unwrap()
            .pop()
            .unwrap();
        SqlExecutor::execute_async_db_query(stmt.clone(), &mut boxed)
            .await
            .unwrap();

        // ── assert via the shared Arc ──────────────────────────────────────
        let recorded = executed.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], stmt.to_string());
    }
}
