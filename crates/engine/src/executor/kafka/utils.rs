use async_trait::async_trait;
use reqwest::Client;
use crate::executor::ExecutorHost;
use crate::executor::kafka::{KafkaConnectorDeployedConfig, KafkaExecutorError};


#[async_trait]
pub trait KafkaConnectClient: ExecutorHost {
    async fn connector_exists(&self, conn_name: &str) -> Result<bool, KafkaExecutorError> {
        let client = Client::new();
        let url = format!("{}/connectors/{}/status", &self.host(), conn_name);
        let resp = client.get(&url).send().await?;

        if resp.status().is_success() {
            let body: serde_json::Value = resp.json().await?;
            if body["connector"]["state"] == "RUNNING" {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(KafkaExecutorError::UnexpectedError("Failed to check if connector is running".to_string()))
        }
    }

    async fn get_connector_config(&self, conn_name: &str) -> Result<KafkaConnectorDeployedConfig, KafkaExecutorError> {
        let client = Client::new();
        let url = format!("{}/connectors/{}", &self.host(), conn_name);
        let resp = client.get(&url).send().await?;
        
        if resp.status().is_success() {
            let config: KafkaConnectorDeployedConfig = resp.json().await?;
            Ok(config)
        } else {
            Err(KafkaExecutorError::UnexpectedError("Failed to fetch connector config".to_string()))
        }
    }
}
