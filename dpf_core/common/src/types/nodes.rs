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

#[derive(Debug, Clone)]
pub enum RelationType {
    Source,
    Model,
    Kafka,
}
#[derive(Debug, Clone)]
pub struct Relation {
    pub relation_type: RelationType,
    pub name: String,
}
impl Relation {
    pub fn new(relation_type: RelationType, name: String) -> Self {
        Self {
            relation_type,
            name,
        }
    }
}
pub type ParsedGenericSql = String;

#[derive(Debug, Clone)]
pub struct Relations(Vec<Relation>);
impl Deref for Relations {
    type Target = Vec<Relation>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<Relation>> for Relations {
    fn from(v: Vec<Relation>) -> Self {
        Self(v)
    }
}
impl Relations {
    pub fn new(rels: Vec<Relation>) -> Self {
        Self(rels)
    }
}

pub trait CoreNodeFields {
    fn name(&self) -> String;
    fn path(&self) -> PathBuf;
    fn config(&self) -> Value;
}
#[derive(Debug)]
pub struct ModelNode {
    // represents the schema.model name
    pub model_name: String,
    pub materialization: Materialize,
    pub path: PathBuf,
}
impl ModelNode {
    pub fn new(model_name: String, materialization: Materialize, path: PathBuf) -> Self {
        Self {
            model_name,
            materialization,
            path,
        }
    }
}
impl CoreNodeFields for ModelNode {
    fn name(&self) -> String {
        self.model_name.clone()
    }

    fn path(&self) -> PathBuf {
        self.path.clone()
    }

    fn config(&self) -> Value {
        json!({"materialization": self.materialization.clone()})
    }
}

pub struct KafkaNode {
    pub name: String,
    pub relations: Relations,
    pub path: PathBuf,
}
impl CoreNodeFields for KafkaNode {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn path(&self) -> PathBuf {
        self.path.clone()
    }
    fn config(&self) -> Value {
        json!({})
    }
}
impl KafkaNode {
    pub fn new(name: String, relations: Relations, path: PathBuf) -> Self {
        Self {
            name,
            relations,
            path,
        }
    }
}

pub struct SourceNode {
    pub name: String,
    pub relations: Relations,
    pub path: PathBuf,
}
impl CoreNodeFields for SourceNode {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn path(&self) -> PathBuf {
        // No path for a source node as this represents a source db table and its downstream deps
        PathBuf::new()
    }
    fn config(&self) -> Value {
        json!({})
    }
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

pub trait Identifier {
    fn identifier(&self) -> String;
}
