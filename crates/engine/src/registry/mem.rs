use parking_lot::RwLock;
use serde_json::{Value as Json, Map};
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use sqlparser::ast::{CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, Statement};
use crate::{CatalogError, KafkaConnectorMeta, KafkaConnectorDecl, ModelDecl, PipelineDecl, TransformDecl};
use crate::executor::{ExecutorError, ExecutorResponse};
use crate::executor::sql::{AstValueFormatter, KvPairs};
use crate::types::KafkaConnectorType;

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
    fn register_object<S>(
        &self,
        parsed_stmts: S,
    ) -> Result<(), ExecutorError>
    where
        S: IntoIterator<Item = Statement>;
    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError>;
    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError>;
    fn register_smt_pipeline(&self, ast: CreateSimpleMessageTransformPipeline) -> Result<(), CatalogError>;
    //fn register_model()
}
impl Register for MemoryCatalog {
    fn register_object<S>(
        &self,
        parsed_stmts: S,
    ) -> Result<(), ExecutorError>
    where
        S: IntoIterator<Item = Statement>,
    {

        for ast in parsed_stmts.into_iter() {
            match ast.clone() {
                Statement::CreateKafkaConnector(stmt) => {
                    self.register_kafka_connector(stmt)?;
                }
                Statement::CreateSMTransform(stmt) => {
                    self.register_kafka_smt(stmt)?;
                }
                Statement::CreateSMTPipeline(stmt) => {
                    self.register_smt_pipeline(stmt)?;
                }
                _ => {
                    ()
                }
            }
        }

        Ok(())
    }

    fn register_kafka_connector(
        &self,
        ast: CreateKafkaConnector,
    ) -> Result<(), CatalogError> {
        let sql = ast.clone();
        
        let meta = KafkaConnectorMeta::new(ast);

        let mut g = self.inner.write();
        if g.connectors.contains_key(&meta.name) {
            return Err(CatalogError::Duplicate);
        }
        g.connectors.insert(meta.name.clone(), meta);
        Ok(())
    }

    fn register_kafka_smt(
        &self,
        ast: CreateSimpleMessageTransform,
    ) -> Result<(), CatalogError> {
        let meta = TransformDecl::new(ast);
        let mut g = self.inner.write();
        if g.transforms_by_id.contains_key(&meta.id) || g.transform_name_to_id.contains_key(&meta.name) {
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

    // fn register_model(
    //     &self,
    //     stmt: Statement
    // ) -> Result<(), CatalogError> {
    //     stmt
    // }

}

pub trait Getter: Send + Sync + 'static {
    fn get_kafka_connector(&self, name: &str) -> Result<KafkaConnectorMeta, CatalogError>;
    fn get_kafka_smt<K>(&self, name: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>,;
    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError>;
    fn get_transform_ids_by_name(
        &self, 
        ast: CreateSimpleMessageTransformPipeline
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
            .ok_or(CatalogError::NotFound(format!("{:?} not found", k).to_string()))
    }

    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError> {
        self.inner
            .read()
            .pipelines
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }

    fn get_transform_ids_by_name(&self, ast: CreateSimpleMessageTransformPipeline) -> Result<Vec<Uuid>, CatalogError>
    {
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
                return Err(CatalogError::NotFound(format!("{} not found", name)))
            }
        }

        Ok(ids)
    }
}

/* ---------- trait ----------- */
// src/mem.rs
pub trait Catalog: Send + Sync + 'static {
    /* transforms */
    fn put_transform(&self, t: TransformDecl) -> Result<(), CatalogError>;
    fn get_transform<K>(&self, name: K) -> Result<TransformDecl, CatalogError>
    where
        K: Into<Key>;

    /* pipelines  (strict, no versions) */
    fn put_pipeline(&self, p: PipelineDecl) -> Result<(), CatalogError>;
    fn get_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError>;

    /* connectors */
    fn put_connector(&self, c: KafkaConnectorMeta) -> Result<(), CatalogError>;
    fn get_connector(&self, name: &str) -> Result<KafkaConnectorMeta, CatalogError>;

    /* tables … */
}

pub trait Compile: Send + Sync + 'static {
    fn compile_kafka_decl(&self, name: &str) -> Result<KafkaConnectorDecl, CatalogError>;
}

impl Compile for MemoryCatalog {
    fn compile_kafka_decl(&self, name: &str) -> Result<KafkaConnectorDecl, CatalogError> {
        let conn = self.get_kafka_connector(name)?;
        let props: Json = conn.config;

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

        let meta = KafkaConnectorMeta {
            name: conn_name,
            con_type: conn.con_type,
            config: Value::Object(obj),
            sql: connector_config
        };
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use super::*;

    #[test]
    fn test_select() {
        let res = sqlparser::parser::Parser::parse_sql(&GenericDialect,  r#"
            select *
            from {{ ref('stg_orders') }} o
            join {{ source('raw', 'stg_customers') }} c
              on o.customer_id = c.id
            "#).unwrap();

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
