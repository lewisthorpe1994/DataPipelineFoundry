use crate::executor::sql::AstValueFormatter;
use crate::executor::ExecutorError;
use crate::{
    CatalogError, KafkaConnectorDecl, KafkaConnectorMeta, ModelDecl, PipelineDecl, TransformDecl,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, Value};
use sqlparser::ast::{CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, Query, Statement};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms_by_id: HashMap<Uuid, TransformDecl>,
    transform_name_to_id: HashMap<String, Uuid>,
    pipelines: HashMap<String, PipelineDecl>,
    connectors: HashMap<String, KafkaConnectorMeta>,
    models: HashMap<String, ModelDecl>,
}

#[derive(Debug)]
pub enum Key {
    Id(Uuid),
    Name(String),
}
impl<'a> From<&'a str> for Key {
    fn from(s: &'a str) -> Self {
        Self::Name(s.to_owned())
    }
}
impl From<Uuid> for Key {
    fn from(u: Uuid) -> Self {
        Self::Id(u)
    }
}

#[derive(Clone)]
pub struct MemoryCatalog {
    inner: Arc<RwLock<State>>,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(State::default())),
        }
    }

    /* ---------- optional durability ---------- */
    pub fn load_from(path: &str) -> Result<Self, CatalogError> {
        let json = std::fs::read_to_string(path).unwrap_or_else(|_| "{}".into());
        let state: State = serde_json::from_str(&json)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(state)),
        })
    }
    pub fn flush_to(&self, path: &str) -> Result<(), CatalogError> {
        let json = serde_json::to_string_pretty(&*self.inner.read())?;
        let tmp = format!("{path}.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}

pub trait Register: Send + Sync + 'static {
    fn register_object(&self, parsed_stmts: Vec<Statement>) -> Result<(), CatalogError>;
    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError>;
    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError>;
    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError>;
    fn register_model(&self, ast: CreateModel) -> Result<(), CatalogError>;
}
impl Register for MemoryCatalog {
    fn register_object(&self, parsed_stmts: Vec<Statement>) -> Result<(), CatalogError> {
        if parsed_stmts.len() == 1 {
            match parsed_stmts[0].clone() {
                Statement::CreateKafkaConnector(stmt) => {
                    self.register_kafka_connector(stmt)?;
                }
                Statement::CreateSMTransform(stmt) => {
                    self.register_kafka_smt(stmt)?;
                }
                Statement::CreateSMTPipeline(stmt) => {
                    self.register_smt_pipeline(stmt)?;
                }
                Statement::CreateModel(stmt) => {
                    self.register_model(stmt)?
                }
                _ => (),
            }
        } else if parsed_stmts.len() == 2 { 
            
        } else {
            return Err(CatalogError::Unsupported("Cannot register more then two statements".to_string()));
        }

        Ok(())
    }

    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError> {
        let meta = KafkaConnectorMeta::new(ast);

        let mut g = self.inner.write();
        if g.connectors.contains_key(&meta.name) {
            return Err(CatalogError::Duplicate);
        }
        g.connectors.insert(meta.name.clone(), meta);
        Ok(())
    }

    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError> {
        let meta = TransformDecl::new(ast);
        let mut g = self.inner.write();
        if g.transforms_by_id.contains_key(&meta.id)
            || g.transform_name_to_id.contains_key(&meta.name)
        {
            return Err(CatalogError::Duplicate);
        }
        g.transform_name_to_id.insert(meta.name.clone(), meta.id);
        g.transforms_by_id.insert(meta.id, meta);
        Ok(())
    }

    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError> {
        let sql = ast.clone();
        let t_ids = self.get_transform_ids_by_name(ast.clone())?;
        let pred = match ast.pipe_predicate {
            Some(p) => Some(p.formatted_string()),
            None => None,
        };
        let pipe = PipelineDecl::new(ast.name.value, t_ids, pred, sql);

        let mut g = self.inner.write();
        if g.pipelines.contains_key(&pipe.name) {
            return Err(CatalogError::Duplicate); // name already taken
        }
        g.pipelines.insert(pipe.name.clone(), pipe);
        Ok(())
    }

    fn register_model(
        &self,
        ast: CreateModel
    ) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        let model_dec = ModelDecl {
            schema: "".to_string(),
            name: "".to_string(),
            sql: CreateModel {},
            materialize: None,
            refs: vec![],
            sources: vec![],
        }
    }
}

pub trait Getter: Send + Sync + 'static {
    fn get_kafka_connector(&self, name: &str) -> Result<KafkaConnectorMeta, CatalogError>;
    fn get_kafka_smt<K>(&self, name: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>;
    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError>;
    fn get_transform_ids_by_name(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<Vec<Uuid>, CatalogError>;
}

impl Getter for MemoryCatalog {
    fn get_kafka_connector(&self, name: &str) -> Result<KafkaConnectorMeta, CatalogError> {
        self.inner
            .read()
            .connectors
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }

    fn get_kafka_smt<K>(&self, key: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>,
    {
        use Key::*;
        let s = self.inner.read();
        let k = &key.into();
        match k {
            Id(id) => s.transforms_by_id.get(&id).cloned(),
            Name(n) => s
                .transform_name_to_id
                .get(n)
                .and_then(|id| s.transforms_by_id.get(id))
                .cloned(),
        }
        .ok_or(CatalogError::NotFound(
            format!("{:?} not found", k).to_string(),
        ))
    }

    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError> {
        self.inner
            .read()
            .pipelines
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }

    fn get_transform_ids_by_name(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<Vec<Uuid>, CatalogError> {
        let mut ids = Vec::new();
        let g = self.inner.read();
        let names = ast
            .steps
            .iter()
            .map(|s| s.name.to_string())
            .collect::<Vec<String>>();

        for name in names {
            let t = g.transform_name_to_id.get(&name).cloned();
            if let Some(id) = t {
                ids.push(id);
            } else {
                return Err(CatalogError::NotFound(format!("{} not found", name)));
            }
        }

        Ok(ids)
    }
}

pub trait Compile: Send + Sync + 'static + Getter {
    fn compile_kafka_decl(&self, name: &str) -> Result<KafkaConnectorDecl, CatalogError>;
}

impl Compile for MemoryCatalog {
    fn compile_kafka_decl(&self, name: &str) -> Result<KafkaConnectorDecl, CatalogError> {
        let conn = self.get_kafka_connector(name)?;
        let mut config = match conn.config {
            Value::Object(map) => map,
            _ => Map::new(),
        };

        let mut transform_names = Vec::new();
        match conn.pipelines {
            Some(pipelines) => {
                for pipe_ident in pipelines {
                    let pipe = self.get_smt_pipeline(&pipe_ident)?;
                    let pipe_name = pipe.name.clone();
                    for t_id in pipe.transforms {
                        let t = self.get_kafka_smt(t_id)?;
                        let tname = format!("{}_{}", pipe_name, t.name);
                        transform_names.push(tname.clone());

                        if let Json::Object(cfg) = t.config {
                            for (k, v) in cfg {
                                config.insert(format!("transforms.{tname}.{k}"), v);
                            }
                        }
                        if let Some(pred) = &pipe.predicate {
                            config.insert(
                                format!("transforms.{tname}.predicate"),
                                Value::String(pred.clone()),
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        if !transform_names.is_empty() {
            config.insert(
                "transforms".to_string(),
                Value::String(transform_names.join(",")),
            );
        }

        let dec = KafkaConnectorDecl {
            kind: conn.con_type,
            name: conn.name,
            config: Json::Object(config),
            sql: conn.sql,
            reads: vec![],
            writes: vec![],
        };
        Ok(dec)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    #[test]
    fn test_select() {
        let res = sqlparser::parser::Parser::parse_sql(
            &GenericDialect,
            r#"
            drop view if exists test_vie
            create table test_vie as
            with test as (
            select *
            from {{ ref('stg_orders') }} o
            join {{ source('raw', 'stg_customers') }} c
              on o.customer_id = c.id)

             select * from test
            "#,
        )
        .unwrap();

        println!("{:?}", res);
    }
}
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use chrono::Utc;
//     use serde_json::json;
//     use uuid::Uuid;
//     use crate::ConnectorType;
//
//     // helper to create a minimal TransformMeta
//     fn make_transform(id: Uuid, name: &str) -> TransformDecl {
//         TransformDecl {
//             id,
//             name: name.to_string(),
//             config: json!({}), // empty JSON is fine for the test
//             created: Utc::now(),
//         }
//     }
//
//     #[test]
//     fn test_get_transform_ids_success() {
//         let cat = MemoryCatalog::new();
//
//         // insert two transforms
//         let id1 = Uuid::new_v4();
//         let id2 = Uuid::new_v4();
//         cat.put_transform(make_transform(id1, "hash_email"))
//             .unwrap();
//         cat.put_transform(make_transform(id2, "drop_pii")).unwrap();
//
//         // 1️⃣ single name via std::iter::once
//         let ids = cat
//             .get_transform_ids_by_name(std::iter::once("hash_email"))
//             .unwrap();
//         assert_eq!(ids, vec![id1]);
//
//         // 2️⃣ multiple names via slice
//         let names = ["hash_email", "drop_pii"];
//         let ids = cat.get_transform_ids_by_name(&names).unwrap();
//         assert_eq!(ids, vec![id1, id2]);
//     }
//
//     #[test]
//     fn test_get_transform_ids_not_found() {
//         let cat = MemoryCatalog::new();
//         let err = cat
//             .get_transform_ids_by_name(std::iter::once("does_not_exist"))
//             .unwrap_err();
//
//         assert!(matches!(err, CatalogError::NotFound));
//     }
//
//     #[test]
//     fn test_put_get_connector() {
//         let cat = MemoryCatalog::new();
//         let conn = KafkaConnectorMeta {
//             name: "my_conn".into(),
//             plugin: ConnectorType::Kafka,
//             config: json!({"a": "b"}),
//         };
//         cat.put_connector(conn.clone()).unwrap();
//         let got = cat.get_connector("my_conn").unwrap();
//         assert_eq!(got.name, conn.name);
//         assert_eq!(got.plugin, ConnectorType::Kafka);
//         assert_eq!(got.config["a"], "b");
//         assert!(matches!(
//             cat.put_connector(conn),
//             Err(CatalogError::Duplicate)
//         ));
//     }
// }

#[cfg(test)]
mod test {
    use super::*;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::ast::{Ident, ValueWithSpan};
    use sqlparser::tokenizer::{Location, Span};

    #[test]
    fn test_create_transform() {
        let registry = MemoryCatalog::new();
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
        let res = registry.register_kafka_smt(smt).unwrap();
        assert_eq!(res, ());
        let cat_smt = registry.get_kafka_smt("cast_hash_cols_to_int").unwrap();
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
}
