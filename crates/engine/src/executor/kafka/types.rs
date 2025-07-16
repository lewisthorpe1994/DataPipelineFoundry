use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::ConnectorMeta;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum KafkaConnectorType {
    Source,
    Sink
}
#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaConnectorDeployConfig {
    pub name: String,
    pub config: Value
}

impl From<ConnectorMeta> for KafkaConnectorDeployConfig {
    fn from(meta: ConnectorMeta) -> Self {
        Self {
            config: meta.config,
            name: meta.name,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaConnectorDeployedConfig {
    pub name: String,
    pub config: Value,
    #[serde(rename = "type")]
    pub conn_type: Option<KafkaConnectorType>
}

#[derive(PartialEq, Debug)]
pub enum KafkaExecutorResponse {
    Ok
}