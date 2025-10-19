use core::fmt;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaConnectorType {
    Sink,
    Source,
}
impl Display for KafkaConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KafkaConnectorType::Sink => write!(f, "SINK"),
            KafkaConnectorType::Source => write!(f, "SOURCE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaConnectorProvider {
    Debezium,
    Confluent,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaSourceConnectorSupportedDb {
    Postgres,
}
impl Display for KafkaSourceConnectorSupportedDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KafkaSourceConnectorSupportedDb::Postgres => write!(f, "POSTGRES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaSinkConnectorSupportedDb {
    Postgres,
}
impl Display for KafkaSinkConnectorSupportedDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KafkaSinkConnectorSupportedDb::Postgres => write!(f, "POSTGRES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaConnectorSupportedDb {
    Source(KafkaSourceConnectorSupportedDb),
    Sink(KafkaSinkConnectorSupportedDb),
}

impl Display for KafkaConnectorSupportedDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KafkaConnectorSupportedDb::Source(db) => write!(f, "{}", db),
            KafkaConnectorSupportedDb::Sink(db) => write!(f, "{}", db),
        }
    }
}

impl Display for KafkaConnectorProvider {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            KafkaConnectorProvider::Debezium => write!(f, "DEBEZIUM"),
            KafkaConnectorProvider::Confluent => write!(f, "CONFLUENT"),
        }
    }
}