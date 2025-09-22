use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Warehouse,
    Kafka,
    Api,
}
impl Display for SourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::Warehouse => write!(f, "warehouse"),
            SourceType::Kafka => write!(f, "kafka"),
            SourceType::Api => write!(f, "api"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SourceConnArgs {
    pub kafka_connect: Option<String>,
}
