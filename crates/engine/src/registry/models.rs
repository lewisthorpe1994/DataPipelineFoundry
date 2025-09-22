use crate::executor::sql::KvPairs;
use crate::types::KafkaConnectorType;
use chrono::{DateTime, Utc};
use common::types::{Materialize, ModelRef, SourceRef};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline, Select, Statement,
};
use uuid::Uuid;

/// A single SMT / transform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformDecl {
    pub id: Uuid,
    pub name: String,
    pub config: serde_json::Value,
    pub created: DateTime<Utc>,
    pub sql: CreateSimpleMessageTransform,
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
        }
    }
}

// src/declarations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDecl {
    pub name: String,
    pub transforms: Vec<Uuid>, // ordered IDs
    pub created: DateTime<Utc>,
    pub predicate: Option<String>,
    pub sql: CreateSimpleMessageTransformPipeline,
}
impl PipelineDecl {
    pub fn new(
        name: String,
        transforms: Vec<Uuid>,
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
            } else if mapped.len() == 1 && mapped[0].trim().is_empty() {
                None
            } else {
                Some(mapped)
            }
        };

        Self {
            name: ast.name.to_string(),
            con_type: KafkaConnectorType::from(ast.connector_type),
            config: KvPairs(ast.with_properties).into(),
            sql,
            pipelines,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDecl {
    pub schema: String,
    pub name: String,
    pub sql: CreateModel,
    pub materialize: Option<Materialize>,
    pub refs: Option<Vec<ModelRef>>,
    pub sources: Option<Vec<SourceRef>>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledModelDecl {
    pub schema: String,
    pub name: String,
    pub sql: String,
    pub materialize: Option<Materialize>,
}
