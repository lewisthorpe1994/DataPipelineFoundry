pub mod error;
pub mod models;
mod tests;

use sqlparser::ast::{CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, ModelDef};
pub use models::*;

use common::types::{Materialize, ModelRef, ParsedNode, SourceRef};
use common::utils::read_sql_file_from_path;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, Value};
use sqlparser::ast::helpers::foundry_helpers::{AstValueFormatter, MacroFnCall, MacroFnCallType};
use sqlparser::ast::{
     ObjectName, ObjectNamePart, Statement,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use serde::ser::Error;
use uuid::Uuid;
use common::config::components::global::FoundryConfig;
use common::config::components::sources::warehouse_source::DbConfig;
use crate::error::CatalogError;
use common::types::kafka::{KafkaConnectorType, SourceDbConnectionInfo};

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms_by_id: HashMap<Uuid, TransformDecl>,
    transform_name_to_id: HashMap<String, Uuid>,
    pipelines: HashMap<String, PipelineDecl>,
    connectors: HashMap<String, KafkaConnectorMeta>,
    models: HashMap<String, ModelDecl>,
    sources: HashMap<String, DbConfig>,
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
            nodes.push(CatalogNode {
                name: name.to_owned(),
                declaration: NodeDec::KafkaConnector(dec.clone()),
                target: Some(dec.target.clone()),
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
            let dec = g.transforms_by_id.get(id).unwrap();
            nodes.push(CatalogNode {
                name: name.to_owned(),
                declaration: NodeDec::KafkaSmt(dec.clone()),
                target: None,
            });
        }

        for (name, dec) in g.sources.iter() {
            for (s_name, schema) in &dec.database.schemas {
                for (t_name,table) in &schema.tables {
                    nodes.push(CatalogNode {
                        name: name.clone(),
                        declaration: NodeDec::WarehouseSource(WarehouseSourceDec {
                            schema: s_name.clone(),
                            table: t_name.clone(),
                            database: dec.database.name.clone(),
                        }),
                        target: None
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
    fn register_object(&self, parsed_stmts: Vec<Statement>, target: Option<String>) -> Result<(), CatalogError>;
    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError>;
    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError>;
    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError>;
    fn register_model(
        &self, 
        ast: CreateModel,
        target: String
    ) -> Result<(), CatalogError>;
    fn register_warehouse_sources(
        &self,
        warehouse_sources: HashMap<String, DbConfig>,
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
                .map_err(|_| CatalogError::NotFound(format!("{:?} not found", path)))?;
            // println!("model config {:?}", config);
            let (formatted_sql, node_target) = if let Some(m) = model {
                (format_create_model_sql(sql, m.config.materialization, &m.config.name), Some(m.target))
            } else {
                (sql, None)
            };
            // println!("{}", formatted_sql);

            let parsed_sql = Parser::parse_sql(&GenericDialect, &formatted_sql).map_err(|e| {
                CatalogError::SqlParser(format!(
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
        target: Option<String>
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
                    target.ok_or(CatalogError::MissingConfig("Missing target name for model".to_string()))?
                )?,
                _ => (),
            }
        } else if parsed_stmts.len() == 2 {
        } else {
            return Err(CatalogError::Unsupported(
                "Cannot register more then two statements".to_string(),
            ));
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
        ast: CreateModel,
        target: String
    ) -> Result<(), CatalogError> {
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
            target
        };

        let mut g = self.inner.write();
        if g.models.contains_key(&key) {
            return Err(CatalogError::Duplicate);
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

    fn get_model(&self, name: &str) -> Result<ModelDecl, CatalogError>;
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

    fn get_model(&self, name: &str) -> Result<ModelDecl, CatalogError> {
        let g = self.inner.read();
        g.models
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }
}

pub trait Compile: Send + Sync + 'static + Getter {
    fn compile_kafka_decl(&self, name: &str, foundry_config: &FoundryConfig) -> Result<KafkaConnectorDecl, CatalogError>;
}

impl Compile for MemoryCatalog {
    fn compile_kafka_decl(&self, name: &str, foundry_config: &FoundryConfig) -> Result<KafkaConnectorDecl, CatalogError> {
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

        match conn.con_type {
            KafkaConnectorType::Source => {
                let cluster_config =  if let Some(config) = &foundry_config.kafka_source.get(&conn.cluster_name) {
                    config.clone()
                } else {
                    return Err(CatalogError::NotFound(format!("Cluster {} not found", conn.cluster_name)))
                };
                
                let schema_config = &foundry_config.kafka_connectors.get(&conn.name);
                if let Some(schema_config) = schema_config {
                    config.insert("table.include.list".to_string(), Json::String(schema_config.table_include_list()));
                    config.insert("column.include.list".to_string(), Json::String(schema_config.column_include_list()));

                }

                config.insert("kafka.bootstrap.servers".to_string(), Json::String(cluster_config.bootstrap.servers.clone()));
                
                let adapter_conf = foundry_config.get_adapter_connection_details(
                    &&conn.sql.db_ident.value
                ).ok_or(CatalogError::NotFound(format!("Adapter {} not found", conn.sql.db_ident.value)))?;
                
                let db_config = SourceDbConnectionInfo::from(adapter_conf);
                let obj = match serde_json::to_value(&db_config)? {
                    Value::Object(obj) => obj,
                    _ => return Err(
                        CatalogError::SerdeJson(
                            serde_json::Error::custom("expecting source db config")
                        )
                    )
                };

                config.extend(obj)
            }
            _ => ()
        };


        let dec = KafkaConnectorDecl {
            kind: conn.con_type,
            name: conn.name,
            config: Json::Object(config),
            sql: conn.sql,
            reads: vec![],
            writes: vec![],
            cluster_name: conn.cluster_name,
        };
        Ok(dec)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use common::config::loader::read_config;
    use ff_core::parser::parse_nodes;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::ast::{Ident, ValueWithSpan};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::{Location, Span};
    use test_utils::{get_root_dir, with_chdir};

    #[test]
    fn test_register_nodes_ingests_example_project() -> std::io::Result<()> {
        let cat = MemoryCatalog::new();
        let project_root = get_root_dir();

        with_chdir(&project_root, move || {
            let config = read_config(None).expect("load example project config");
            let wh_config = config.warehouse_source.clone();
            let nodes = parse_nodes(&config).expect("parse example models");
            println!("{:#?}", nodes);
            // println!("{:#?}", nodes);
            cat.register_nodes(nodes, wh_config)
                .expect("register nodes");
            // println!("{:#?}", cat.inner.read().models);
            // println!("{:?}", cat.collect_catalog_nodes());
        })?;

        Ok(())
    }

    #[test]
    fn test_register_transform() {
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

    #[test]
    fn register_pipeline_resolves_ids_and_predicate() {
        use sqlparser::ast::{KafkaConnectorType, TransformCall};

        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));

        // register two simple transforms
        let smt1 = CreateSimpleMessageTransform {
            name: Ident {
                value: "mask".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("mask".into()),
                        span,
                    },
                ),
                (
                    Ident {
                        value: "fields".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("name".into()),
                        span,
                    },
                ),
            ],
        };
        let smt2 = CreateSimpleMessageTransform {
            name: Ident {
                value: "drop".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("drop".into()),
                        span,
                    },
                ),
                (
                    Ident {
                        value: "blacklist".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("id".into()),
                        span,
                    },
                ),
            ],
        };
        registry.register_kafka_smt(smt1).unwrap();
        registry.register_kafka_smt(smt2).unwrap();

        // pipeline with both transforms and a predicate
        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "pii".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![
                TransformCall::new(
                    Ident {
                        value: "mask".into(),
                        quote_style: None,
                        span,
                    },
                    vec![],
                ),
                TransformCall::new(
                    Ident {
                        value: "drop".into(),
                        quote_style: None,
                        span,
                    },
                    vec![],
                ),
            ],
            pipe_predicate: Some(ValueWithSpan {
                value: SingleQuotedString("some_predicate".into()),
                span,
            }),
        };
        registry.register_smt_pipeline(pipe).unwrap();

        let p = registry.get_smt_pipeline("pii").unwrap();
        assert_eq!(p.transforms.len(), 2);
        assert_eq!(p.predicate.as_deref(), Some("some_predicate"));
    }

    #[test]
    fn register_duplicate_returns_error() {
        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));

        let smt = CreateSimpleMessageTransform {
            name: Ident {
                value: "dup".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![],
        };
        registry.register_kafka_smt(smt.clone()).unwrap();
        let err = registry.register_kafka_smt(smt).unwrap_err();
        matches!(err, CatalogError::Duplicate);

        // pipeline duplicate
        use sqlparser::ast::{KafkaConnectorType, TransformCall};
        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "p".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "dup".into(),
                    quote_style: None,
                    span,
                },
                vec![],
            )],
            pipe_predicate: None,
        };
        registry.register_smt_pipeline(pipe.clone()).unwrap();
        let err = registry.register_smt_pipeline(pipe).unwrap_err();
        matches!(err, CatalogError::Duplicate);

        // connector duplicate
        let conn = CreateKafkaConnector {
            name: Ident {
                value: "c".into(),
                quote_style: None,
                span,
            },
            cluster_ident: Ident {value: "c".into(), quote_style: None, span},
            if_not_exists: false,
            connector_type: sqlparser::ast::KafkaConnectorType::Source,
            with_properties: vec![],
            with_pipelines: vec![],
            db_ident: Ident {
                value: "some_db".into(),
                quote_style: None,
                span,
            }
        };
        registry.register_kafka_connector(conn.clone()).unwrap();
        let err = registry.register_kafka_connector(conn).unwrap_err();
        matches!(err, CatalogError::Duplicate);
    }

    #[test]
    fn get_transform_ids_by_name_errors_on_missing() {
        use sqlparser::ast::{KafkaConnectorType, TransformCall};
        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));
        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "p".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "missing".into(),
                    quote_style: None,
                    span,
                },
                vec![],
            )],
            pipe_predicate: None,
        };
        let err = registry.get_transform_ids_by_name(pipe).unwrap_err();
        assert!(matches!(err, CatalogError::NotFound(_)));
    }
}
