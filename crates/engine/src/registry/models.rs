use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use uuid::Uuid;
use common::types::Materialize;
use sqlparser::ast::{Statement};
use crate::executor::kafka::KafkaConnectorType;

/// A single SMT / transform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformDecl {
    pub id: Uuid,
    pub name: String,
    pub config: serde_json::Value,
    pub created: DateTime<Utc>,
    pub sql: Statement
}
impl TransformDecl {
    pub fn new(name: String, config: serde_json::Value, statement: Statement) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            config,
            created: Utc::now(),
            sql: statement
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
    pub sql: Statement
}
impl PipelineDecl {
    pub fn new(
        name: String,
        transforms: Vec<Uuid>,
        predicate: Option<String>,
        sql: Statement
    ) -> Self {
        Self {
            name,
            transforms,
            created: Utc::now(),
            predicate,
            sql
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConnectorType {
    Kafka, // TODO - more connectors
}

/// Connector definition (Kafka, Airbyte, …)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMeta {
    pub name: String,
    pub plugin: ConnectorType, // "kafka", "airbyte", …
    pub config: serde_json::Value,
    pub sql: Statement
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDecl {
    pub schema: String,
    pub name: String,
    pub sql: Statement,
    pub materialize: Option<Materialize>,
    pub refs: Vec<String>,
    pub sources: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceRef {
    SourceTable { schema: String, table: String },
    WarehouseTable { schema: String, table: String },
    KafkaTopic { name: String },
    TableTemplate { format: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConnectorDecls {
    pub kind: KafkaConnectorType,
    pub name: String,
    pub config: Json,
    pub sql: Statement,
    pub reads: Vec<ResourceRef>,
    pub writes: Vec<ResourceRef>
}