use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single SMT / transform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformMeta {
    pub id:      Uuid,
    pub name:    String,
    pub class:   String,
    pub config:  serde_json::Value,
    pub created: DateTime<Utc>,
}

/// Immutable snapshot of a pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineVersion {
    pub ver:        i32,
    pub transforms: Vec<Uuid>,         // ordered list of Transform IDs
    pub created:    DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMeta {
    pub name:    String,
    pub history: Vec<PipelineVersion>, // append-only
}

/// Connector definition (Kafka, Airbyte, …)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMeta {
    pub name:   String,
    pub plugin: String,                // "kafka", "airbyte", …
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub connector: String,
    pub topic:     String,
    pub target:    String,             // warehouse table
    pub pipeline:  Option<String>,     // pipeline name
}
