use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single SMT / transform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformMeta {
    pub id: Uuid,
    pub name: String,
    pub config: serde_json::Value,
    pub created: DateTime<Utc>,
}
impl TransformMeta {
    pub fn new(name: String, config: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            config,
            created: Utc::now(),
        }
    }
}

// src/model.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMeta {
    pub name: String,
    pub transforms: Vec<Uuid>, // ordered IDs
    pub created: DateTime<Utc>,
    pub predicate: Option<String>,
}
impl PipelineMeta {
    pub fn new(name: String, transforms: Vec<Uuid>, predicate: Option<String>) -> Self {
        Self {
            name,
            transforms,
            created: Utc::now(),
            predicate,
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
}
//
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct TableMeta {
//     pub connector: String,
//     pub topic:     String,
//     pub target:    String,             // warehouse table
//     pub pipeline:  Option<String>,     // pipeline name
// }
