use std::ops::Deref;
use std::path::PathBuf;
use serde_json::{json, Value};
use sqlparser::parser::Parser;
use crate::types::Materialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelRef {
    pub table: String,
    pub schema: String
    // todo - look at adding columns
}
impl ModelRef {
    pub fn new<S: Into<String>>(schema: S, table: S) -> Self {
        Self {
            schema: schema.into(),
            table: table.into()
        }
    }
    pub fn to_string(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

#[derive(Debug, Clone)]
pub enum RelationType {
    Source,
    Model,
    Kafka
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
            name
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
    fn relations(&self) -> Relations;
    fn path(&self) -> PathBuf;
    fn config(&self) -> Value;
}
#[derive(Debug)]
pub struct ModelNode {
    pub schema: String,
    pub model: String,
    pub relations: Relations,
    pub materialization: Option<Materialize>,
    pub path: PathBuf
}
impl ModelNode {
    pub fn new(
        schema: String,
        model: String,
        materialization: Option<Materialize>,
        relations:Relations,
        path: PathBuf
    ) -> Self {
        Self {
            schema,
            model,
            materialization,
            relations,
            path
        }
    }
}
impl CoreNodeFields for ModelNode {
    fn name(&self) -> String {
        self.model.clone()
    }

    fn relations(&self) -> Relations {
        self.relations.clone()
    }

    fn path(&self) -> PathBuf {
        self.path.clone()
    }

    fn config(&self) -> Value {
        json!({"schema": self.schema.clone(), "materialization": self.materialization.clone()})
    }
}

pub struct KafkaNode {
    pub name: String,
    pub relations: Relations,
    pub path: PathBuf,
}
impl CoreNodeFields for  KafkaNode {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn relations(&self) -> Relations {
        self.relations.clone()
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
            path
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
    fn relations(&self) -> Relations {
        self.relations.clone()
    }
    fn path(&self) -> PathBuf {
        // No path for a source node as this represents a source db table and its downstream deps
        PathBuf::new()
    }
    fn config(&self) -> Value {
        json!({})
    }
}

pub enum NodeTypes {
    Sql,
    Kafka,
    Source
}

pub struct ParsedNode<N: CoreNodeFields> {
    pub node: N,
    pub node_type: NodeTypes,
}

pub trait Identifier {
    fn identifier(&self) -> String;
}