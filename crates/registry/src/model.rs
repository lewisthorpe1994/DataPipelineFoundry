use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single SMT / transform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformMeta {
    pub id:      Uuid,
    // pub order:   i32,
    pub name:    String,
    pub config:  serde_json::Value,
    pub created: DateTime<Utc>,
}

// impl From<>


// src/model.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMeta {
    pub name:        String,
    pub transforms:  Vec<Uuid>,         // ordered IDs
    pub created:     DateTime<Utc>,
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
