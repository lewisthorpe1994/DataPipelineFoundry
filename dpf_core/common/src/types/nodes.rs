use crate::config::components::model::ResolvedModelConfig;
use crate::types::Materialize;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::ops::Deref;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceRef {
    pub source_name: String,
    pub source_table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelRef {
    pub table: String,
    pub schema: String,
    pub name: String,
}
impl ModelRef {
    pub fn new<S: Into<String>>(schema: S, table: S, name: S) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
            name: name.into(),
        }
    }
    pub fn to_identifier(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceNodeRefType {
    Source,
    Destination,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceNodeType {
    Api,
    WarehouseDb,
    SourceDb,
    Kafka,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceNode {
    pub name: String,
    pub node_type: ResourceNodeType,
    pub reference: ResourceNodeRefType
}

#[derive(Debug)]
pub enum NodeTypes {
    KafkaConnector,
    KafkaSmt,
    KafkaSmtPipeline,
}

#[derive(Debug, PartialEq)]
pub struct ParsedInnerNode {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug)]
pub enum ParsedNode {
    Model {
        node: ParsedInnerNode,
        config: ResolvedModelConfig,
    },
    KafkaConnector {
        node: ParsedInnerNode,
    },
    KafkaSmt {
        node: ParsedInnerNode,
    },
    KafkaSmtPipeline {
        node: ParsedInnerNode,
    },
    Python {
        node: ParsedInnerNode,
        files: Vec<PathBuf>,
        workspace_path: PathBuf,
    }
}

impl ParsedNode {
    pub fn name(&self) -> String {
        match self {
            ParsedNode::Model { node, .. } => node.name.clone(),
            ParsedNode::KafkaConnector { node, .. } => node.name.clone(),
            ParsedNode::KafkaSmt { node, .. } => node.name.clone(),
            ParsedNode::KafkaSmtPipeline { node, .. } => node.name.clone(),
            ParsedNode::Python { node, .. } => node.name.clone(),
        }
    }
}
