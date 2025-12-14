use crate::error::DagError;
use catalog::{KafkaConnectorMeta, MemoryCatalog};
use common::config::components::global::FoundryConfig;
use common::types::ResourceNodeType;
use components::{KafkaSinkConnectorConfig, KafkaSourceConnectorConfig};
use petgraph::Direction;
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline,
};
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;

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
    Api,
    Python,
}

impl From<ResourceNodeType> for DagNodeType {
    fn from(value: ResourceNodeType) -> Self {
        match value {
            ResourceNodeType::WarehouseDb => DagNodeType::WarehouseSourceDb,
            ResourceNodeType::SourceDb => DagNodeType::SourceDb,
            ResourceNodeType::Api => DagNodeType::Api,
            ResourceNodeType::Kafka => DagNodeType::KafkaTopic,
        }
    }
}

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Clone)]
pub enum NodeAst {
    Model(Box<CreateModel>),
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

// #[derive(Clone)]
// pub struct DagNode {
//     pub name: String,
//     pub ast: Option<NodeAst>,
//     pub compiled_obj: Option<String>,
//     pub node_type: DagNodeType,
//     pub is_executable: bool,
//     pub relations: Option<BTreeSet<String>>,
//     pub target: Option<String>,
//     //source: Option<String>,
// }

#[derive(Clone, Debug)]
pub enum DagNode {
    KafkaSourceConnector {
        name: String,
        ast: NodeAst,
        compiled_obj: Option<String>,
        is_executable: bool,
        relations: Option<BTreeSet<String>>,
        target: Option<String>,
    },
    KafkaSinkConnector {
        name: String,
        ast: NodeAst,
        compiled_obj: Option<String>,
        is_executable: bool,
        relations: Option<BTreeSet<String>>,
        target: Option<String>,
    },
    KafkaSmt {
        name: String,
        ast: NodeAst,
        compiled_obj: Option<String>,
        relations: Option<BTreeSet<String>>,
    },
    KafkaTopic {
        name: String,
        relations: Option<BTreeSet<String>>,
    },
    KafkaPipeline {
        name: String,
        ast: NodeAst,
        compiled_obj: Option<String>,
        relations: Option<BTreeSet<String>>,
    },
    Model {
        name: String,
        ast: NodeAst,
        compiled_obj: Option<String>,
        is_executable: bool,
        relations: Option<BTreeSet<String>>,
        target: Option<String>,
    },
    SourceDb {
        name: String,
        relations: Option<BTreeSet<String>>,
    },
    WarehouseSourceDb {
        name: String,
        relations: Option<BTreeSet<String>>,
    },
    Api {
        name: String,
        relations: Option<BTreeSet<String>>,
    },
    Python {
        name: String,
        is_executable: bool,
        relations: Option<BTreeSet<String>>,
        job_dir: PathBuf,
    },
}
impl DagNode {
    pub fn is_executable(&self) -> &bool {
        match self {
            Self::KafkaSourceConnector { is_executable, .. } => is_executable,
            Self::KafkaSinkConnector { is_executable, .. } => is_executable,
            Self::Model { is_executable, .. } => is_executable,
            Self::Python { is_executable, .. } => is_executable,
            _ => &false,
        }
    }

    pub fn name(&self) -> &String {
        match self {
            Self::KafkaSourceConnector { name, .. } => name,
            Self::KafkaSinkConnector { name, .. } => name,
            Self::Model { name, .. } => name,
            Self::Python { name, .. } => name,
            Self::KafkaTopic { name, .. } => name,
            Self::KafkaPipeline { name, .. } => name,
            Self::WarehouseSourceDb { name, .. } => name,
            Self::SourceDb { name, .. } => name,
            Self::Api { name, .. } => name,
            Self::KafkaSmt { name, .. } => name,
        }
    }

    pub fn relations(&self) -> &Option<BTreeSet<String>> {
        match self {
            Self::KafkaSourceConnector { relations, .. } => relations,
            Self::KafkaSinkConnector { relations, .. } => relations,
            Self::Model { relations, .. } => relations,
            Self::Python { relations, .. } => relations,
            Self::KafkaTopic { relations, .. } => relations,
            Self::KafkaPipeline { relations, .. } => relations,
            Self::WarehouseSourceDb { relations, .. } => relations,
            Self::SourceDb { relations, .. } => relations,
            Self::Api { relations, .. } => relations,
            Self::KafkaSmt { relations, .. } => relations,
        }
    }
    
    pub fn compiled_executable(&self) -> Option<String> {
        match self { 
            Self::Model {compiled_obj, ..} |
            Self::KafkaSinkConnector {compiled_obj, ..} |
            Self::KafkaSourceConnector {compiled_obj, ..} |
            Self::KafkaSmt {compiled_obj, ..} |
            Self::KafkaPipeline {compiled_obj, ..}
            => compiled_obj.clone(),
            _ => None
        }
    }
    
    pub fn target(&self) -> Option<String> {
        match self { 
            Self::Model {target, ..} |
            Self::KafkaSinkConnector {target, ..} |
            Self::KafkaSourceConnector {target, ..} => target.clone(),
            _ => None
        }
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

pub struct BuildSrcKafkaConnectorNodeArgs<'a> {
    pub connector: KafkaSourceConnectorConfig,
    pub config: &'a FoundryConfig,
    pub meta: &'a KafkaConnectorMeta,
    pub catalog: &'a MemoryCatalog,
    pub connector_json: String,
    pub target: Option<String>,
    pub current_topics: &'a BTreeSet<String>,
}

pub struct BuildSinkKafkaConnectorNodeArgs<'a> {
    pub connector: KafkaSinkConnectorConfig,
    pub config: &'a FoundryConfig,
    pub meta: &'a KafkaConnectorMeta,
    pub catalog: &'a MemoryCatalog,
    pub connector_json: String,
    pub target: Option<String>,
    pub current_topics: &'a BTreeSet<String>,
}
