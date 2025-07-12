use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::ConnectorMeta;

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct KafkaConnectorConfig {
    name: String,
    config: Value
}

impl From<ConnectorMeta> for KafkaConnectorConfig {
    fn from(meta: ConnectorMeta) -> Self {
        Self {
            config: meta.config,
            name: meta.name,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum KafkaExecutorResponse {
    Ok
}