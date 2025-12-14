pub mod error;
pub mod models;

pub use models::*;
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline, CreateSimpleMessageTransformPredicate, ModelDef,
};
use std::cmp::Ordering;

use crate::error::CatalogError;
use common::config::components::sources::warehouse_source::DbConfig;
use common::types::kafka::KafkaConnectorType;
use common::types::{Materialize, ModelRef, ParsedInnerNode, ParsedNode, ResourceNode, SourceRef};
use common::utils::read_sql_file_from_path;
use parking_lot::RwLock;
use python_parser::parse_relationships;
use serde::{Deserialize, Serialize};
use sqlparser::ast::helpers::foundry_helpers::{AstValueFormatter, MacroFnCall, MacroFnCallType};
use sqlparser::ast::{ObjectName, ObjectNamePart, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
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
    python_decls: HashMap<String, PythonDecl>,
}

#[derive(Debug)]
pub enum NodeDec {
    KafkaSmt(TransformDecl),
    KafkaSmtPipeline(PipelineDecl),
    KafkaConnector(KafkaConnectorMeta),
    Model(ModelDecl),
    WarehouseSource(WarehouseSourceDec),
    Python(PythonDecl),
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

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
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
                for t_name in schema.tables.keys() {
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

        for (name, dec) in g.python_decls.iter() {
            nodes.push(CatalogNode {
                name: name.to_owned(),
                declaration: NodeDec::Python(dec.clone()),
                target: None,
            });
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

fn model_identifier_from_ref(schema: &str, table: &str) -> String {
    if table.starts_with(schema) {
        table.to_string()
    } else if table.starts_with('_') {
        format!("{}{}", schema, table)
    } else {
        format!("{}_{}", schema, table)
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
        name: &str,
    ) -> Result<(), CatalogError>;
    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError>;
    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError>;
    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError>;
    fn register_model(
        &self,
        ast: CreateModel,
        target: String,
        node: &str,
    ) -> Result<(), CatalogError>;
    fn register_warehouse_sources(
        &self,
        warehouse_sources: HashMap<String, DbConfig>,
    ) -> Result<(), CatalogError>;

    fn register_kafka_smt_predicate(
        &self,
        ast: CreateSimpleMessageTransformPredicate,
    ) -> Result<(), CatalogError>;

    fn register_python_node(
        &self,
        node: &ParsedInnerNode,
        files: &Vec<PathBuf>,
        workspace_path: &Path,
    ) -> Result<(), CatalogError>;
}

fn compare_node(a: &ParsedNode, b: &ParsedNode) -> Ordering {
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
        _ => Ordering::Equal,
    }
}

pub fn compare_catalog_node(a: &CatalogNode, b: &CatalogNode) -> Ordering {
    fn priority(node: &CatalogNode) -> u8 {
        match &node.declaration {
            NodeDec::KafkaConnector(meta) => match meta.con_type {
                KafkaConnectorType::Source => 0,
                KafkaConnectorType::Sink => 1,
            },
            NodeDec::WarehouseSource(_) => 2,
            NodeDec::Model(_) => 3,
            NodeDec::KafkaSmt(_) => 4,
            NodeDec::KafkaSmtPipeline(_) => 5,
            NodeDec::Python(_) => 6,
        }
    }

    priority(a).cmp(&priority(b))
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
            let (parsed_sql, target, node_name, node_path) = match &node {
                ParsedNode::Model { node, config } => {
                    let sql = read_sql_file_from_path(&node.path).map_err(|_| {
                        CatalogError::not_found(format!("{:?} not found", node.path))
                    })?;
                    let formatted_sql = format_create_model_sql(
                        sql,
                        config.config.materialization.clone(),
                        &config.config.name,
                    );

                    (
                        formatted_sql,
                        Some(config.target.clone()),
                        node.name.clone(),
                        node.path.clone(),
                    )
                }
                ParsedNode::KafkaConnector { node }
                | ParsedNode::KafkaSmt { node }
                | ParsedNode::KafkaSmtPipeline { node } => {
                    let sql = read_sql_file_from_path(&node.path).map_err(|_| {
                        CatalogError::not_found(format!("{:?} not found", node.path))
                    })?;

                    (sql, None, node.name.clone(), node.path.clone())
                }
                ParsedNode::Python {
                    node,
                    files,
                    workspace_path,
                } => {
                    self.register_python_node(node, files, workspace_path)?;
                    continue;
                }
            };

            let parsed = Parser::parse_sql(&GenericDialect, &parsed_sql).map_err(|e| {
                CatalogError::sql_parser(format!(
                    "{} (node: {}, file: {})",
                    e,
                    node_name,
                    &node_path.display()
                ))
            })?;
            self.register_object(parsed, target, &node.name())?;
        }
        Ok(())
    }
    fn register_object(
        &self,
        parsed_stmts: Vec<Statement>,
        target: Option<String>,
        name: &str,
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
                    name,
                )?,
                Statement::CreateSMTPredicate(stmt) => {
                    self.register_kafka_smt_predicate(stmt)?;
                }
                _ => {
                    return Err(CatalogError::unsupported(format!(
                        "Unsupported statement! got statement {:#?}",
                        parsed_stmts[0].to_string()
                    )))
                }
            }
        } else {
            return Err(CatalogError::unsupported(
                "Cannot register more than two statements",
            ));
        }

        Ok(())
    }

    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.connectors.contains_key(ast.name()) {
            return Err(CatalogError::duplicate(ast.name().to_string()));
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
                    .ok_or(CatalogError::not_found(format!("{} not found", name)))?;

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
            let pred = ast.pipe_predicate.map(|p| p.formatted_string());
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

    fn register_model(
        &self,
        ast: CreateModel,
        target: String,
        name: &str,
    ) -> Result<(), CatalogError> {
        // derive schema/name from underlying model definition
        fn split_schema_table(name: &ObjectName) -> (String, String) {
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|p| match p {
                    ObjectNamePart::Identifier(ident) => ident.value.clone(),
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

        let (r, s): (Vec<MacroFnCall>, Vec<MacroFnCall>) = ast
            .macro_fn_call
            .clone()
            .into_iter()
            .partition(|call| matches!(call.m_type, MacroFnCallType::Ref));

        let refs = r
            .iter()
            .map(|call| {
                let schema = call.args[0].clone();
                let table = call.args[1].clone();

                ModelRef {
                    name: model_identifier_from_ref(&schema, &table),
                    table,
                    schema,
                }
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
        if g.models.contains_key(name) {
            return Err(CatalogError::duplicate(name));
        }
        g.models.insert(name.to_string(), model_dec);
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

    fn register_python_node(
        &self,
        node: &ParsedInnerNode,
        files: &Vec<PathBuf>,
        workspace_path: &Path,
    ) -> Result<(), CatalogError> {
        let mut resources: HashSet<ResourceNode> = HashSet::new();
        for file in files {
            let python_str = std::fs::read_to_string(&file).map_err(|e| {
                CatalogError::io(format!("unable to read python file {}", file.display()), e)
            })?;
            let rels = parse_relationships(&python_str).map_err(|e| {
                CatalogError::python_node(format!("unable to parse relationships: {}", e))
            })?;
            resources.extend(rels);
        }
        let python_dec = PythonDecl {
            name: node.name.clone(),
            job_dir: node.path.clone(),
            workspace_path: workspace_path.to_path_buf(),
            resources,
        };

        let mut g = self.inner.write();
        g.python_decls.insert(node.name.clone(), python_dec);

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
            Id(id) => s.transforms_by_id.get(id).cloned(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use matches::assert_matches;

    fn parse_stmt(sql: &str) -> Statement {
        let mut stmts = Parser::parse_sql(&GenericDialect, sql).expect("parse_sql");
        stmts.remove(0)
    }

    fn parse_model(sql: &str) -> CreateModel {
        match parse_stmt(sql) {
            Statement::CreateModel(cm) => cm,
            other => panic!("expected CreateModel, got {:?}", other),
        }
    }

    fn parse_kafka_connector(sql: &str) -> CreateKafkaConnector {
        match parse_stmt(sql) {
            Statement::CreateKafkaConnector(conn) => conn,
            other => panic!("expected CreateKafkaConnector, got {:?}", other),
        }
    }

    fn parse_transform(sql: &str) -> CreateSimpleMessageTransform {
        match parse_stmt(sql) {
            Statement::CreateSMTransform(t) => t,
            other => panic!("expected CreateSMTransform, got {:?}", other),
        }
    }

    fn parse_pipeline(sql: &str) -> CreateSimpleMessageTransformPipeline {
        match parse_stmt(sql) {
            Statement::CreateSMTPipeline(p) => p,
            other => panic!("expected CreateSMTPipeline, got {:?}", other),
        }
    }

    #[test]
    fn register_model_extracts_refs_and_sources() {
        let catalog = MemoryCatalog::new();
        let sql = r#"
            CREATE MODEL bronze.some_model AS
            DROP VIEW IF EXISTS bronze.some_model CASCADE;
            CREATE VIEW bronze.some_model AS
            SELECT * FROM ref('bronze','dim') d
            JOIN source('warehouse','raw_table') s ON 1=1
        "#;

        let create = parse_model(sql);
        catalog
            .register_model(create, "analytics".into(), "bronze_some_model")
            .expect("register");

        let stored = catalog.get_model("bronze_some_model").expect("model exist");
        assert_eq!(stored.target, "analytics");
        assert_eq!(stored.schema, "bronze");
        assert_eq!(stored.name, "some_model");
        assert_eq!(stored.refs.len(), 1);
        assert_eq!(stored.refs[0].name, "bronze_dim");
        assert_eq!(stored.sources.len(), 1);
        assert_eq!(stored.sources[0].source_name, "warehouse");
        assert_eq!(stored.sources[0].source_table, "raw_table");
    }

    #[test]
    fn register_kafka_connector_rejects_duplicates() {
        let catalog = MemoryCatalog::new();
        let sql = r#"
            CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS dvdrental
            USING KAFKA CLUSTER 'test_cluster' (
                "database.hostname" = "localhost",
                "database.user" = "app",
                "database.password" = "secret",
                "database.dbname" = "app_db",
                "topic.prefix" = "app"
            ) WITH CONNECTOR VERSION '3.1'
            FROM SOURCE DATABASE 'adapter_source'
        "#;

        let conn = parse_kafka_connector(sql);
        catalog
            .register_kafka_connector(conn.clone())
            .expect("first insert succeeds");
        let err = catalog
            .register_kafka_connector(conn.clone())
            .expect_err("second insert should fail");
        assert_matches!(err, CatalogError::Duplicate { .. });

        // Stored connector can still be read back
        catalog
            .get_kafka_connector(conn.name())
            .expect("connector persisted");
    }

    const TRANSFORM_SQL: &str = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM hash_email (
            "type" = 'org.apache.kafka.connect.transforms.MaskField$Value'
        )
    "#;

    const PIPELINE_SQL: &str = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE preset_pipe (
            hash_email
        ) WITH PIPELINE PREDICATE 'only_customers'
    "#;

    #[test]
    fn smt_pipeline_requires_known_transforms() {
        let catalog = MemoryCatalog::new();
        let err = catalog
            .register_smt_pipeline(parse_pipeline(PIPELINE_SQL))
            .expect_err("pipeline should fail without registered transforms");
        assert_matches!(err, CatalogError::NotFound { .. });
    }

    #[test]
    fn smt_pipeline_registration_persists_steps() {
        let catalog = MemoryCatalog::new();
        catalog
            .register_kafka_smt(parse_transform(TRANSFORM_SQL))
            .expect("register transform");

        catalog
            .register_smt_pipeline(parse_pipeline(PIPELINE_SQL))
            .expect("register pipeline");

        let pipeline = catalog
            .get_smt_pipeline("preset_pipe")
            .expect("pipeline stored");
        assert_eq!(pipeline.transforms.len(), 1);
        assert_eq!(pipeline.transforms[0].name, "hash_email");
        assert_eq!(pipeline.predicate.as_deref(), Some("only_customers"));
    }
}
