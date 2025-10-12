use serde::{Deserialize, Serialize};
use sqlparser::ast::KafkaConnectorType as SqlParserKafkaConnectorType;
use crate::config::components::connections::AdapterConnectionDetails;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SourceDbConnectionInfo {
    #[serde(rename = "database.hostname")]
    pub hostname: String,
    #[serde(rename = "database.port")]
    pub port: String,
    #[serde(rename = "database.user")]
    pub user: String,
    #[serde(rename = "database.password")]
    pub password: String,
    #[serde(rename = "database.dbname")]
    pub dbname: String,
}
impl From<AdapterConnectionDetails> for SourceDbConnectionInfo {
    fn from(value: AdapterConnectionDetails) -> Self {
        Self {
            hostname: value.host,
            port: value.port,
            user: value.user,
            password: value.password,
            dbname: value.database
        }
    }
}