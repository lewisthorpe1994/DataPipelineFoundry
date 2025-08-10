use serde::{Deserialize, Serialize};
use common::types::Materialize;
use sqlparser::ast::Statement;
use serde_json::Value as Json;
use crate::executor::kafka::KafkaConnectorType;

// new: raw model declaration (what the SQL file defines)
#[derive(Debug, Clone)]
pub struct ModelDecl {
    pub schema: String,
    pub name: String,
    pub ast: Vec<Statement>,
    pub materialize: Option<Materialize>,
    pub refs: Vec<String>,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceRef {
    SourceTable { schema: String, table: String },
    WarehouseTable { schema: String, table: String },
    KafkaTopic { name: String },
    TableTemplate { format: String },            // e.g. "${topic}"
}

#[derive(Debug, Clone)]
pub struct KafkaConnectorDecls {
    pub kind: KafkaConnectorType,
    pub name: String,
    pub config: Json,
    pub ast: Vec<Statement>,
    pub reads: Vec<ResourceRef>,
    pub writes: Vec<ResourceRef>
}