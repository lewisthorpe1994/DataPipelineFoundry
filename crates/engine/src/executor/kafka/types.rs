use crate::types::KafkaConnectorType;
use crate::KafkaConnectorMeta;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaConnectorDeployConfig {
    pub name: String,
    pub config: Value,
}

impl From<KafkaConnectorMeta> for KafkaConnectorDeployConfig {
    fn from(meta: KafkaConnectorMeta) -> Self {
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
    pub conn_type: Option<KafkaConnectorType>,
}

#[derive(PartialEq, Debug)]
pub enum KafkaExecutorResponse {
    Ok,
}
