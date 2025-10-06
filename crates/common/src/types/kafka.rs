use serde::{Deserialize, Serialize};
use sqlparser::ast::KafkaConnectorType as SqlParserKafkaConnectorType;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum KafkaConnectorType {
    Source,
    Sink,
}

impl From<SqlParserKafkaConnectorType> for KafkaConnectorType {
    fn from(value: SqlParserKafkaConnectorType) -> Self {
        match value {
            SqlParserKafkaConnectorType::Source => Self::Source,
            SqlParserKafkaConnectorType::Sink => Self::Sink,
        }
    }
}