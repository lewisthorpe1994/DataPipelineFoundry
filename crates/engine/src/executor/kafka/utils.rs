use reqwest::Client;
use crate::executor::kafka::KafkaExecutorError;

pub async fn connector_exists(connector_name: &str, connect_host: &str) -> Result<bool, KafkaExecutorError> {
    let client = Client::new();
    let url = format!("{connect_host}/connectors/{connector_name}/status");
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