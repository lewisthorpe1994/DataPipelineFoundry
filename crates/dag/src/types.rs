use crate::error::DagError;
use petgraph::Direction;
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline,
};
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};

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
    WarehouseSourceDb,
}

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Clone)]
pub enum NodeAst {
    Model(CreateModel),
    KafkaSmt(CreateSimpleMessageTransform),
    KafkaSmtPipeline(CreateSimpleMessageTransformPipeline),
    KafkaConnector(CreateKafkaConnector),
}
impl Display for NodeAst {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeAst::Model(m) => write!(f, "{}", m),
            NodeAst::KafkaSmt(m) => write!(f, "{}", m),
            NodeAst::KafkaSmtPipeline(m) => write!(f, "{}", m),
            NodeAst::KafkaConnector(m) => write!(f, "{}", m),
        }
    }
}

#[derive(Clone)]
pub struct DagNode {
    pub name: String,
    pub ast: Option<NodeAst>,
    pub compiled_obj: Option<String>,
    pub node_type: DagNodeType,
    pub is_executable: bool,
    pub relations: Option<BTreeSet<String>>,
    pub target: Option<String>,
    //source: Option<String>,
}
impl Debug for DagNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\
        DagNode {{ \
            name: {}, \
            node_type: {:?}, \
            is_executable: {}, \
            relations: {:?} \
        }}",
            self.name, self.node_type, self.is_executable, self.relations
        )
    }
}

pub enum TransitiveDirection {
    Incoming,
    Outgoing,
}

impl From<TransitiveDirection> for Direction {
    fn from(value: TransitiveDirection) -> Self {
        match value {
            TransitiveDirection::Incoming => Direction::Incoming,
            TransitiveDirection::Outgoing => Direction::Outgoing,
        }
    }
}
