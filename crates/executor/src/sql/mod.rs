use crate::ExecutorError;
use registry::{Catalog, CatalogError, CatalogHelpers, MemoryCatalog, PipelineMeta, TransformMeta};
use serde_json::Value as Json;
use sqlparser::ast::{CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, Ident, ValueWithSpan};

trait AstValueFormatter {
    fn formatted_string(&self) -> String;
}
impl AstValueFormatter for ValueWithSpan {
    fn formatted_string(&self) -> String {
        if self.value.to_string().starts_with("'") && self.value.to_string().ends_with("'") {
            return self.value.to_string().trim_matches('\'').to_string()
        };
        if self.value.to_string().starts_with('"') && self.value.to_string().ends_with('"') {
            return self.value.to_string().trim_matches('"').to_string()       
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
    
    pub fn execute_create_kafka_connector_if_not_exists(
        connector_config: CreateKafkaConnector,
        registry: MemoryCatalog,
    ) -> Result<(), ExecutorError> {
        let conn_name = connector_config.name.to_string();
        let conn_type = connector_config.connector_type;
        let props: Json = KvPairs(connector_config.with_properties).into();
        let mut global_transforms_list_str = String::new();
        // fetch all transforms in the pipeline
        for name in connector_config.with_pipelines {
            let pipe = registry.get_pipeline(name.to_string().as_ref())?;
            let mut transforms: Vec<TransformMeta> = Vec::new();
            for t_id in pipe.transforms {
                let mut t = registry.get_transform(t_id)?;
                t.name = format!("{}_{}", pipe.name, t.name);
                t.config
                transforms.push(t);
            }
            // create part of the transform list 
            let transform_list_str = transforms.iter()
                .map(|t| format!("{}_{}", pipe.name, t.name.to_string()))
                .collect::<Vec<String>>().join(",");
            
            global_transforms_list_str.push_str(&transform_list_str);
            global_transforms_list_str.push(',');
        }
        
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::KafkaConnectorType::Source;
    use sqlparser::ast::TransformCall;
    use super::*;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::tokenizer::{Location, Span};
    use uuid::Uuid;

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
            let id   = meta.id;
            registry.put_transform(meta).unwrap();
            id
        };
        let id_hash_email = make_t("hash_email");
        let id_drop_pii   = make_t("drop_pii");

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

        assert_eq!(
            cat_pipe.predicate.as_deref(),
            Some("some_predicate")
        );
    }

}
