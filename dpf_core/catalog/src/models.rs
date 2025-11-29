use crate::CatalogError;
use chrono::{DateTime, Utc};
use common::types::kafka::KafkaConnectorType;
use common::types::{KafkaConnectorProvider, Materialize, ModelRef, SourceRef};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sqlparser::ast::helpers::foundry_helpers::KvPairs;
use sqlparser::ast::{
    AstValueFormatter, CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline,
};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformDecl {
    pub id: Uuid,
    pub name: String,
    pub config: serde_json::Value,
    pub created: DateTime<Utc>,
    pub sql: CreateSimpleMessageTransform,
    pub predicate: Option<PredicateRefDecl>,
}

impl TransformDecl {
    pub fn new(ast: CreateSimpleMessageTransform) -> Self {
        let sql = ast.clone();

        Self {
            id: Uuid::new_v4(),
            name: ast.name.to_string(),
            config: KvPairs(ast.config).into(),
            created: Utc::now(),
            sql,
            predicate: ast.predicate.map(|p| PredicateRefDecl {
                name: p.name.formatted_string(),
                negate: p.negate,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTransformDecl {
    pub name: String,
    pub id: Uuid,
    pub args: Option<HashMap<String, String>>,
    pub alias: Option<String>,
}

// src/declarations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDecl {
    pub name: String,
    pub transforms: Vec<PipelineTransformDecl>, // ordered IDs
    pub created: DateTime<Utc>,
    pub predicate: Option<String>,
    pub sql: CreateSimpleMessageTransformPipeline,
}
impl PipelineDecl {
    pub fn new(
        name: String,
        transforms: Vec<PipelineTransformDecl>,
        predicate: Option<String>,
        sql: CreateSimpleMessageTransformPipeline,
    ) -> Self {
        Self {
            name,
            transforms,
            created: Utc::now(),
            predicate,
            sql,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConnectorMeta {
    pub name: String,
    pub con_type: KafkaConnectorType,
    pub config: serde_json::Value,
    pub sql: CreateKafkaConnector,
    pub pipelines: Option<Vec<String>>,
    pub cluster_name: String,
    pub target: String,
    pub target_schema: Option<String>, // this is only used on sink connectors
    pub conn_provider: KafkaConnectorProvider,
    pub version: String,
}
impl KafkaConnectorMeta {
    pub fn new(ast: CreateKafkaConnector) -> Self {
        let sql = ast.clone();
        let pipelines = {
            let mapped = ast
                .with_pipelines
                .iter()
                .map(|v| v.value.clone())
                .collect::<Vec<String>>();

            if mapped.is_empty() {
                None
            } else {
                Some(mapped)
            }
        };

        Self {
            name: ast.name.to_string(),
            con_type: ast.connector_type,
            config: KvPairs(ast.with_properties).into(),
            cluster_name: ast.cluster_ident.value,
            target: ast.db_ident.value,
            target_schema: ast.schema_ident.map(|s| s.value),
            sql,
            pipelines,
            conn_provider: ast.connector_provider,
            version: ast.connector_version.formatted_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateDecl {
    pub name: String,
    pub class_name: String,
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateRefDecl {
    pub name: String,
    pub negate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDecl {
    pub schema: String,
    pub name: String,
    pub sql: CreateModel,
    pub materialize: Option<Materialize>,
    pub refs: Vec<ModelRef>,
    pub sources: Vec<SourceRef>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceRef {
    SourceTable { schema: String, table: String },
    WarehouseTable { schema: String, table: String },
    KafkaTopic { name: String },
    TableTemplate { format: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConnectorDecl {
    pub kind: KafkaConnectorType,
    pub name: String,
    pub config: Json,
    pub sql: CreateKafkaConnector,
    pub reads: Vec<ResourceRef>,
    pub writes: Vec<ResourceRef>,
    pub cluster_name: String,
}
impl Display for KafkaConnectorDecl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{\"name\": {}, \"config\": {}}}",
            self.name, self.config
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledModelDecl {
    pub schema: String,
    pub name: String,
    pub sql: String,
    pub materialize: Option<Materialize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseSourceDec {
    pub schema: String,
    pub table: String,
    pub database: String,
}
impl WarehouseSourceDec {
    pub fn identifier(&self) -> String {
        format!("{}.{}.{}", self.database, self.schema, self.table)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonDecl {
    pub name: String,
    pub workspace_path: PathBuf,
    pub sources: HashSet<String>,
    pub destinations: HashSet<String>,
}
