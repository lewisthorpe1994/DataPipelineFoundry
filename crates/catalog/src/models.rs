use std::collections::HashSet;
use common::types::kafka::KafkaConnectorType;
use chrono::{DateTime, Utc};
use common::types::{Materialize, ModelRef, SourceRef};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline,
};
use std::fmt::{Display, Formatter};
use uuid::Uuid;
use sqlparser::ast::helpers::foundry_helpers::KvPairs;
use crate::CatalogError;

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
    pub cluster_name: String,
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
            cluster_name: ast.cluster_ident.value,
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
    pub refs: Vec<ModelRef>,
    pub sources: Vec<SourceRef>,
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

pub trait ExecutionTarget {
    fn target_name(&self) -> Result<String, CatalogError>;
}

impl ExecutionTarget for ModelDecl {
    fn target_name(&self) -> Result<String, CatalogError> {
        let source_names = self.sources
            .iter()
            .map(|s| s.source_name.clone())
            .collect::<HashSet<String>>();
        if source_names.len() > 1 {
            return Err(CatalogError::Unsupported(
                "Cannot execute model with multiple sources".to_string(),
            ));
        } else if source_names.is_empty() {
            return Err(CatalogError::NotFound(
                "Cannot compile a model with no sources".to_string(),
            ))
        }

        Ok(source_names.into_iter().next().unwrap())

    }

}