use parking_lot::RwLock;
use serde_json::{Value as Json, Map};
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use sqlparser::ast::{CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, Statement};
use crate::{CatalogError, ConnectorMeta, ConnectorType, KafkaConnectorDecls, ModelDecl, PipelineDecl, TransformDecl};
use crate::executor::{ExecutorError, ExecutorResponse};
use crate::executor::sql::{AstValueFormatter, KvPairs};

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms_by_id: HashMap<Uuid, TransformDecl>,
    transform_name_to_id: HashMap<String, Uuid>,
    pipelines: HashMap<String, PipelineDecl>,
    connectors: HashMap<String, ConnectorMeta>,
    models: HashMap<String, ModelDecl>,
}

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
    pub fn register_object<S>(
        &self,
        parsed_stmts: S,
    ) -> Result<(), ExecutorError>
    where
        S: IntoIterator<Item = Statement>,
    {

        for ast in parsed_stmts.into_iter() {
            match ast.clone() {
                Statement::CreateKafkaConnector(stmt) => {
                    self.register_kafka_connector(stmt, ast)?;
                }
                Statement::CreateSMTransform(stmt) => {
                    self.register_kafka_smt(stmt, ast)?;
                }
                Statement::CreateSMTPipeline(stmt) => {
                    self.register_smt_pipeline(stmt, ast)?;
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
        stmt: Statement
    ) -> Result<(), CatalogError> {
        let conn_name = ast.name.to_string();
        let conn_type = ast.connector_type; 
        let props: Json = KvPairs(ast.with_properties).into();

        let mut obj = match props {
            Json::Object(map) => map,
            _ => Map::new(),
        };

        let meta = ConnectorMeta {
            name: conn_name,
            plugin: ConnectorType::Kafka,
            config: Json::Object(obj),
            sql: stmt
        };

        self.put_connector(meta.clone())
    }
    
    fn register_kafka_smt(
        &self,
        ast: CreateSimpleMessageTransform,
        stmt: Statement
    ) -> Result<(), CatalogError> {
        let config: Json = KvPairs(ast.config).into();
        let meta = TransformDecl::new(ast.name.to_string(), config, stmt);
        self.put_transform(meta)
    }

    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
        stmt: Statement
    ) -> Result<(), CatalogError> {
        let t_names = ast
            .steps
            .iter()
            .map(|s| s.name.to_string())
            .collect::<Vec<String>>();
        let t_ids = self.get_transform_ids_by_name(t_names)?;
        let pred = match ast.pipe_predicate {
            Some(p) => Some(p.formatted_string()),
            None => None,
        };
        let pipe = PipelineDecl::new(ast.name.value, t_ids, pred, stmt);
        self.put_pipeline(pipe)
    }

    // fn register_model(
    //     &self,
    //     stmt: Statement
    // ) -> Result<(), CatalogError> {
    //     stmt
    // }

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
    fn put_connector(&self, c: ConnectorMeta) -> Result<(), CatalogError>;
    fn get_connector(&self, name: &str) -> Result<ConnectorMeta, CatalogError>;

    /* tables … */
}

pub trait CatalogHelpers {
    fn get_transform_ids_by_name<I, S>(&self, names: I) -> Result<Vec<Uuid>, CatalogError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
}

impl Catalog for MemoryCatalog {
    /* ---- transforms ---- */
    fn put_transform(&self, t: TransformDecl) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.transforms_by_id.contains_key(&t.id) || g.transform_name_to_id.contains_key(&t.name) {
            return Err(CatalogError::Duplicate);
        }
        g.transform_name_to_id.insert(t.name.clone(), t.id);
        g.transforms_by_id.insert(t.id, t);
        Ok(())
    }
    fn get_transform<K>(&self, key: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>,
    {
        use Key::*;
        let s = self.inner.read();
        match key.clone().into() {
            Id(id) => s.transforms_by_id.get(&id).cloned(),
            Name(n) => s
                .transform_name_to_id
                .get(&n)
                .and_then(|id| s.transforms_by_id.get(id))
                .cloned(),
        }
        .ok_or(CatalogError::NotFound(format!("{} not found", key).to_string()))
    }

    /* ---- pipelines ---- */
    fn put_pipeline(&self, p: PipelineDecl) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.pipelines.contains_key(&p.name) {
            return Err(CatalogError::Duplicate); // name already taken
        }
        g.pipelines.insert(p.name.clone(), p);
        Ok(())
    }

    fn get_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError> {
        self.inner
            .read()
            .pipelines
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }

    /* ---- connectors ---- */
    fn put_connector(&self, c: ConnectorMeta) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.connectors.contains_key(&c.name) {
            return Err(CatalogError::Duplicate);
        }
        g.connectors.insert(c.name.clone(), c);
        Ok(())
    }

    fn get_connector(&self, name: &str) -> Result<ConnectorMeta, CatalogError> {
        self.inner
            .read()
            .connectors
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }
}

impl CatalogHelpers for MemoryCatalog {
    fn get_transform_ids_by_name<I, S>(&self, names: I) -> Result<Vec<Uuid>, CatalogError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut ids = Vec::new();
        let g = self.inner.read();
        for name in names {
            let t = g.transform_name_to_id.get(name.as_ref()).cloned();
            if let Some(id) = t {
                ids.push(id);
            } else {
                return Err(CatalogError::NotFound(format!("{} not found", name)));
            }
        }

        Ok(ids)
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
//         let conn = ConnectorMeta {
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
