use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use sqlparser::ast::{CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline};
use crate::error::DagError;

/// Represents an empty edge structure in a graph or similar data structure.
///
/// This struct has no fields and serves as a placeholder or marker.
/// It can be used when an edge's data does not carry any inherent value or properties.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmtpyEdge;
impl Display for EmtpyEdge {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DagNodeType {
    KafkaSourceConnector,
    KafkaSinkConnector,
    KafkaSmt,
    KafkaTopic,
    KafkaPipeline,
    Model,
    SourceDb,
    WarehouseSourceDb
}

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Clone)]
pub enum NodeAst {
    Model(CreateModel),
    KafkaSmt(CreateSimpleMessageTransform),
    KafkaSmtPipeline(CreateSimpleMessageTransformPipeline),
    KafkaConnector(CreateKafkaConnector),
}

#[derive(Debug, Clone)]
pub struct DagNode {
    pub name: String,
    pub ast: Option<NodeAst>,
    pub node_type: DagNodeType,
    pub is_executable: bool,
    pub relations: Option<HashSet<String>>,
    //source: Option<String>,
}
