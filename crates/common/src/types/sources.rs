use std::fmt::{Display, Formatter};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum SourceType {
    Warehouse,
    Kafka,
    Api
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