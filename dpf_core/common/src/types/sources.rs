use serde::Deserialize;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Deserialize, PartialEq, Hash, Eq, Copy)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Warehouse,
    Kafka,
    #[serde(rename = "source_db")]
    SourceDB,
    Api,
}
impl Display for SourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::Warehouse => write!(f, "warehouse"),
            SourceType::Kafka => write!(f, "kafka"),
            SourceType::SourceDB => write!(f, "source_db"),
            SourceType::Api => write!(f, "api"),
        }
    }
}