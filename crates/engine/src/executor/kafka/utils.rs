use async_trait::async_trait;
use reqwest::Client;
use crate::executor::ExecutorHost;
use crate::executor::kafka::KafkaExecutorError;


#[async_trait]
pub trait KafkaConnectUtils: ExecutorHost {
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
}
