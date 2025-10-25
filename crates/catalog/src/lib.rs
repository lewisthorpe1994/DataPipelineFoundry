pub mod error;
pub mod models;
mod tests;

pub use models::*;
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline, CreateSimpleMessageTransformPredicate, ModelDef,
};

use crate::error::CatalogError;
use common::config::components::global::FoundryConfig;
use common::config::components::sources::warehouse_source::DbConfig;
use common::types::kafka::{KafkaConnectorType, SinkDbConnectionInfo, SourceDbConnectionInfo};
use common::types::{Materialize, ModelRef, ParsedNode, SourceRef};
use common::utils::read_sql_file_from_path;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, Value};
use sqlparser::ast::helpers::foundry_helpers::{AstValueFormatter, MacroFnCall, MacroFnCallType};
use sqlparser::ast::{ObjectName, ObjectNamePart, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms_by_id: HashMap<Uuid, TransformDecl>,
    transform_name_to_id: HashMap<String, Uuid>,
    pipelines: HashMap<String, PipelineDecl>,
    connectors: HashMap<String, CreateKafkaConnector>,
    models: HashMap<String, ModelDecl>,
    sources: HashMap<String, DbConfig>,
    predicates: HashMap<String, PredicateDecl>,
}

#[derive(Debug)]
pub enum NodeDec {
    KafkaSmt(TransformDecl),
    KafkaSmtPipeline(PipelineDecl),
    KafkaConnector(KafkaConnectorMeta),
    Model(ModelDecl),
    WarehouseSource(WarehouseSourceDec),
}

#[derive(Debug)]
pub struct CatalogNode {
    pub name: String,
    pub declaration: NodeDec,
    pub target: Option<String>,
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

    pub fn collect_catalog_nodes(&self) -> Vec<CatalogNode> {
        let g = self.inner.read();
        let mut nodes = Vec::new();

        for (name, dec) in g.models.iter() {
            nodes.push(CatalogNode {
                name: name.to_owned(),
                declaration: NodeDec::Model(dec.clone()),
                target: Some(dec.target.clone()),
            });
        }
        for (name, dec) in g.connectors.iter() {
            let meta = KafkaConnectorMeta::new(dec.clone());
            nodes.push(CatalogNode {
                name: name.to_owned(),
                declaration: NodeDec::KafkaConnector(meta.clone()),
                target: Some(meta.target.clone()),
            });
        }
        for (name, dec) in g.pipelines.iter() {
            nodes.push(CatalogNode {
                name: name.to_owned(),
                declaration: NodeDec::KafkaSmtPipeline(dec.clone()),
                target: None,
            });
        }
        for (name, id) in g.transform_name_to_id.iter() {
            if let Some(dec) = g.transforms_by_id.get(id) {
                nodes.push(CatalogNode {
                    name: name.to_owned(),
                    declaration: NodeDec::KafkaSmt(dec.clone()),
                    target: None,
                });
            }
        }

        for (name, dec) in g.sources.iter() {
            for (s_name, schema) in &dec.database.schemas {
                for (t_name, table) in &schema.tables {
                    nodes.push(CatalogNode {
                        name: name.clone(),
                        declaration: NodeDec::WarehouseSource(WarehouseSourceDec {
                            schema: s_name.clone(),
                            table: t_name.clone(),
                            database: dec.database.name.clone(),
                        }),
                        target: None,
                    })
                }
            }
        }
        nodes
    }
}

pub trait IntoRelation {
    fn into_relation(self) -> String;
}

impl IntoRelation for ModelRef {
    fn into_relation(self) -> String {
        format_relation(&self.schema, &self.table)
    }
}

fn format_relation(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        table.to_string()
    } else {
        format!("{}.{}", schema, table)
    }
}

pub fn format_create_model_sql(sql: String, materialize: Materialize, model_name: &str) -> String {
    let model = model_name.replacen("_", ".", 1);
    let materialization = materialize.to_sql();

    format!(
        "CREATE MODEL {} AS
      DROP {} IF EXISTS {} CASCADE;
      CREATE {} {} AS {}",
        model, materialization, model, materialization, model, sql
    )
}

pub trait Register: Send + Sync + 'static {
    fn register_nodes(
        &self,
        parsed_nodes: Vec<ParsedNode>,
        warehouse_sources: HashMap<String, DbConfig>,
    ) -> Result<(), CatalogError>;
    fn register_object(
        &self,
        parsed_stmts: Vec<Statement>,
        target: Option<String>,
    ) -> Result<(), CatalogError>;
    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError>;
    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError>;
    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError>;
    fn register_model(&self, ast: CreateModel, target: String) -> Result<(), CatalogError>;
    fn register_warehouse_sources(
        &self,
        warehouse_sources: HashMap<String, DbConfig>,
    ) -> Result<(), CatalogError>;

    fn register_kafka_smt_predicate(
        &self,
        ast: CreateSimpleMessageTransformPredicate,
    ) -> Result<(), CatalogError>;
}

fn compare_node(a: &ParsedNode, b: &ParsedNode) -> std::cmp::Ordering {
    fn priority(node: &ParsedNode) -> Option<u8> {
        match node {
            ParsedNode::KafkaSmt { .. } => Some(0),
            ParsedNode::KafkaSmtPipeline { .. } => Some(1),
            ParsedNode::KafkaConnector { .. } => Some(2),
            _ => None,
        }
    }

    match (priority(a), priority(b)) {
        (Some(pa), Some(pb)) => pa.cmp(&pb),
        _ => std::cmp::Ordering::Equal,
    }
}
impl Register for MemoryCatalog {
    fn register_nodes(
        &self,
        mut parsed_nodes: Vec<ParsedNode>,
        warehouse_sources: HashMap<String, DbConfig>,
    ) -> Result<(), CatalogError> {
        parsed_nodes.sort_by(compare_node);
        self.register_warehouse_sources(warehouse_sources)?;
        for node in parsed_nodes {
            let (path, model, node_name) = match &node {
                ParsedNode::Model { node, config } => {
                    (&node.path, config.clone(), node.name.clone())
                }
                ParsedNode::KafkaConnector { node }
                | ParsedNode::KafkaSmt { node }
                | ParsedNode::KafkaSmtPipeline { node } => (&node.path, None, node.name.clone()),
            };
            let sql = read_sql_file_from_path(path)
                .map_err(|_| CatalogError::not_found(format!("{:?} not found", path)))?;
            let (formatted_sql, node_target) = if let Some(m) = model {
                (
                    format_create_model_sql(sql, m.config.materialization, &m.config.name),
                    Some(m.target),
                )
            } else {
                (sql, None)
            };

            let parsed_sql = Parser::parse_sql(&GenericDialect, &formatted_sql).map_err(|e| {
                CatalogError::sql_parser(format!(
                    "{} (node: {}, file: {})",
                    e,
                    node_name,
                    path.display()
                ))
            })?;
            self.register_object(parsed_sql, node_target)?;
        }
        Ok(())
    }
    fn register_object(
        &self,
        parsed_stmts: Vec<Statement>,
        target: Option<String>,
    ) -> Result<(), CatalogError> {
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
                Statement::CreateModel(stmt) => self.register_model(
                    stmt,
                    target.ok_or_else(|| {
                        CatalogError::missing_config("Missing target name for model")
                    })?,
                )?,
                Statement::CreateSMTPredicate(stmt) => {
                    self.register_kafka_smt_predicate(stmt)?;
                }
                _ => (),
            }
        } else {
            return Err(CatalogError::unsupported(
                "Cannot register more than two statements",
            ));
        }

        Ok(())
    }

    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError> {
        // let meta = KafkaConnectorMeta::new(ast);

        let mut g = self.inner.write();
        if g.connectors.contains_key(&ast.name().to_string()) {
            return Err(CatalogError::duplicate(&ast.name().to_string()));
        }
        g.connectors.insert(ast.name().to_string(), ast);
        Ok(())
    }

    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError> {
        let decl = TransformDecl::new(ast);
        let id = decl.id;
        let mut g = self.inner.write();
        if g.transforms_by_id.contains_key(&id) || g.transform_name_to_id.contains_key(&decl.name) {
            return Err(CatalogError::duplicate(&decl.name));
        }
        g.transform_name_to_id.insert(decl.name.clone(), id);
        g.transforms_by_id.insert(id, decl);
        Ok(())
    }

    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError> {
        let sql = ast.clone();
        let (transforms, pred) = {
            let mut transforms = Vec::new();
            let g = self.inner.read();
            for step in ast.steps {
                let name = &step.name;
                let id = g
                    .transform_name_to_id
                    .get(&name.to_string())
                    .cloned()
                    .ok_or(CatalogError::not_found(format!(
                        "{} not found",
                        name.to_string()
                    )))?;

                let args = if !step.args.is_empty() {
                    Some(
                        step.args
                            .iter()
                            .map(|(n, a)| (n.value.clone(), a.formatted_string()))
                            .collect::<HashMap<String, String>>(),
                    )
                } else {
                    None
                };

                transforms.push(PipelineTransformDecl {
                    name: name.to_string(),
                    id,
                    args,
                    alias: step.alias.map(|a| a.to_string()),
                })
            }
            let pred = match ast.pipe_predicate {
                Some(p) => Some(p.formatted_string()),
                None => None,
            };
            (transforms, pred)
        };

        let pipe = PipelineDecl::new(ast.name.value, transforms, pred, sql);

        let mut g = self.inner.write();
        if g.pipelines.contains_key(&pipe.name) {
            return Err(CatalogError::duplicate(&pipe.name)); // name already taken
        }
        g.pipelines.insert(pipe.name.clone(), pipe);
        Ok(())
    }

    fn register_model(&self, ast: CreateModel, target: String) -> Result<(), CatalogError> {
        // derive schema/name from underlying model definition
        fn split_schema_table(name: &ObjectName) -> (String, String) {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                })
                .collect();

            let table = parts.last().cloned().unwrap_or_default();
            let schema = if parts.len() >= 2 {
                parts[parts.len() - 2].clone()
            } else {
                String::new()
            };
            (schema, table)
        }

        let (schema, table_name, materialize) = match &ast.model {
            ModelDef::Table(tbl) => {
                let (s, t) = split_schema_table(&tbl.name);
                (s, t, Some(Materialize::Table))
            }
            ModelDef::View(v) => {
                let (s, t) = split_schema_table(&v.name);
                let m = if v.materialized {
                    Materialize::MaterializedView
                } else {
                    Materialize::View
                };
                (s, t, Some(m))
            }
        };

        let key = if schema.is_empty() {
            table_name.clone()
        } else {
            format!("{}.{}", schema, table_name)
        };
        let (r, s): (Vec<MacroFnCall>, Vec<MacroFnCall>) = ast
            .macro_fn_call
            .clone()
            .into_iter()
            .partition(|call| matches!(call.m_type, MacroFnCallType::Ref));

        let refs = r
            .iter()
            .map(|call| ModelRef {
                table: call.args[1].clone(),
                schema: call.args[0].clone(),
            })
            .collect::<Vec<ModelRef>>();

        let sources = s
            .iter()
            .map(|call| SourceRef {
                source_table: call.args[1].clone(),
                source_name: call.args[0].clone(),
            })
            .collect::<Vec<SourceRef>>();

        let model_dec = ModelDecl {
            schema,
            name: table_name,
            sql: ast.clone(),
            materialize,
            refs,
            sources,
            target,
        };

        let mut g = self.inner.write();
        if g.models.contains_key(&key) {
            return Err(CatalogError::duplicate(&key));
        }
        g.models.insert(key, model_dec);
        Ok(())
    }

    fn register_warehouse_sources(
        &self,
        warehouse_sources: HashMap<String, DbConfig>,
    ) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        g.sources = warehouse_sources;
        Ok(())
    }

    fn register_kafka_smt_predicate(
        &self,
        ast: CreateSimpleMessageTransformPredicate,
    ) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        let id = ast.name.value;
        let pred = PredicateDecl {
            name: id.clone(),
            class_name: ast.pred_type.value,
            pattern: ast.pattern.map(|p| p.formatted_string()),
        };

        g.predicates.insert(id, pred);

        Ok(())
    }
}

pub trait Getter: Send + Sync + 'static {
    fn get_kafka_connector(&self, name: &str) -> Result<CreateKafkaConnector, CatalogError>;
    fn get_kafka_smt<K>(&self, name: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>;
    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError>;
    fn get_transform_ids_by_name(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<Vec<Uuid>, CatalogError>;

    fn get_model(&self, name: &str) -> Result<ModelDecl, CatalogError>;
    fn get_smt_predicate(&self, name: &str) -> Result<PredicateDecl, CatalogError>;
}

impl Getter for MemoryCatalog {
    fn get_kafka_connector(&self, name: &str) -> Result<CreateKafkaConnector, CatalogError> {
        self.inner
            .read()
            .connectors
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::not_found(format!("{} not found", name)))
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
        .ok_or_else(|| CatalogError::not_found(format!("{:?} not found", k)))
    }

    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError> {
        self.inner
            .read()
            .pipelines
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::not_found(format!("{} not found", name)))
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
                return Err(CatalogError::not_found(format!("{} not found", name)));
            }
        }

        Ok(ids)
    }

    fn get_model(&self, name: &str) -> Result<ModelDecl, CatalogError> {
        let g = self.inner.read();
        g.models
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::not_found(format!("{} not found", name)))
    }

    fn get_smt_predicate(&self, name: &str) -> Result<PredicateDecl, CatalogError> {
        let g = self.inner.read();
        g.predicates
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::not_found(format!("{} not found", name)))
    }
}

pub trait Compile: Send + Sync + 'static + Getter {
    fn compile_kafka_decl(
        &self,
        name: &str,
        foundry_config: &FoundryConfig,
    ) -> Result<KafkaConnectorDecl, CatalogError>;
}

// impl Compile for MemoryCatalog {
//     fn compile_kafka_decl(
//         &self,
//         name: &str,
//         foundry_config: &FoundryConfig,
//     ) -> Result<KafkaConnectorDecl, CatalogError> {
//         let conn = self.get_kafka_connector(name)?;
//         let mut config = match conn.config {
//             Value::Object(map) => map,
//             _ => Map::new(),
//         };
//
//         let mut transform_names = Vec::new();
//         match conn.pipelines {
//             Some(pipelines) => {
//                 for pipe_ident in pipelines {
//                     let pipe = self.get_smt_pipeline(&pipe_ident)?;
//                     let pipe_name = pipe.name.clone();
//                     for transform in pipe.transforms {
//                         let t = self.get_kafka_smt(&*transform.name)?;
//                         let tname = format!("{}_{}", pipe_name, transform.name);
//                         if let Some(args) = transform.args {
//                             args
//                                 .iter()
//                                 .for_each(|(k, v)| {
//                                     config.insert(format!("transforms.{tname}.{k}"), Json::String(v.clone()));
//                                 })
//                         } else {
//                             if let Json::Object(cfg) = t.config {
//                                 for (k, v) in cfg {
//                                     config.insert(format!("transforms.{tname}.{k}"), v);
//                                 }
//                             }
//                         }
//                         transform_names.push(tname.clone());
//
//                         if let Some(pred) = &pipe.predicate {
//                             config.insert(
//                                 format!("transforms.{tname}.predicate"),
//                                 Value::String(pred.clone()),
//                             );
//                         }
//                     }
//                 }
//             }
//             _ => {}
//         }
//
//         if !transform_names.is_empty() {
//             config.insert(
//                 "transforms".to_string(),
//                 Value::String(transform_names.join(",")),
//             );
//         }
//         let cluster_config = &foundry_config
//             .get_kafka_cluster_conn(&conn.cluster_name)
//             .map_err(|e| CatalogError::not_found(format!("{}", e)))?;
//
//         let adapter_conf = foundry_config
//             .get_adapter_connection_details(&&conn.sql.db_ident.value)
//             .ok_or_else(|| {
//                 CatalogError::not_found(format!(
//                     "Adapter {} not found",
//                     conn.sql.db_ident.value
//                 ))
//             })?;
//
//         match conn.con_type {
//
//             KafkaConnectorType::Source => {
//                 let schema_config = &foundry_config.kafka_connectors.get(&conn.name);
//                 if let Some(schema_config) = schema_config {
//                     config.insert(
//                         "table.include.list".to_string(),
//                         Json::String(schema_config.table_include_list()),
//                     );
//                     config.insert(
//                         "column.include.list".to_string(),
//                         Json::String(schema_config.column_include_list()),
//                     );
//                 }
//
//                 config.insert(
//                     "kafka.bootstrap.servers".to_string(),
//                     Json::String(cluster_config.bootstrap.servers.clone()),
//                 );
//
//                 let db_config = SourceDbConnectionInfo::from(adapter_conf);
//                 let obj = db_config.to_json_map()?;
//
//                 config.extend(obj)
//             }
//             KafkaConnectorType::Sink => {
//                 let db_config = SinkDbConnectionInfo::from(adapter_conf);
//                 let obj = db_config.to_json_map()?;
//                 config.extend(obj);
//                 let collection_name_format = format!(
//                     "{}.${{source.table}}", conn.target_schema.ok_or(
//                     CatalogError::missing_config(format!("missing schema information for sink connector {}", conn.name))
//                         )?
//                 );
//
//                 config.insert("collection.name.format".to_string(), Json::String(collection_name_format));
//
//                 if config.get("topics").is_none() && config.get("topics.regex").is_none() {
//                     return Err(CatalogError::missing_config(format!("Missing a topic definition for {}", conn.name)));
//                 }
//             },
//         };
//
//         let dec = KafkaConnectorDecl {
//             kind: conn.con_type,
//             name: conn.name,
//             config: Json::Object(config),
//             sql: conn.sql,
//             reads: vec![],
//             writes: vec![],
//             cluster_name: conn.cluster_name,
//         };
//         Ok(dec)
//     }
// }
