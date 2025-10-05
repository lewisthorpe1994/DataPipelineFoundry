use std::fmt::{Debug, Display, Formatter};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use common::types::kafka::KafkaConnectorType;

#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaConnectorDeployConfig {
    pub name: String,
    pub config: Value,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaConnectorDeployedConfig {
    pub name: String,
    pub config: Value,
    #[serde(rename = "type")]
    pub conn_type: Option<KafkaConnectorType>,
}

pub enum KafkaConnectClientError {
    NotFound(String),
    FailedToConnect(String),
    FailedToDeploy(String),
    UnexpectedError(String),
}

impl Debug for KafkaConnectClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaConnectClientError::FailedToConnect(err) => {
                write!(f, "Failed to connect: {}", err)
            }
            KafkaConnectClientError::FailedToDeploy(err) => {
                write!(f, "Failed to deploy: {}", err)
            }
            KafkaConnectClientError::NotFound(err) => {
                write!(f, "{} not found", err)
            }
            KafkaConnectClientError::UnexpectedError(err) => {
                write!(f, "Unexpected response: {}", err)
            }
        }
    }
}

impl Display for KafkaConnectClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaConnectClientError::FailedToConnect(err) => {
                write!(f, "Failed to connect: {}", err)
            }
            KafkaConnectClientError::FailedToDeploy(err) => {
                write!(f, "Failed to deploy: {}", err)
            }
            KafkaConnectClientError::NotFound(err) => {
                write!(f, "{} not found", err)
            }
            KafkaConnectClientError::UnexpectedError(err) => {
                write!(f, "Unexpected response: {}", err)
            }
        }
    }
}

#[derive(Deserialize)]
struct ConnectErrorBody {
    message: String,
}

impl std::error::Error for KafkaConnectClientError {}
impl From<reqwest::Error> for KafkaConnectClientError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            KafkaConnectClientError::FailedToConnect(err.to_string())
        } else if let Some(err) = err.status() {
            match err {
                StatusCode::BAD_REQUEST => {
                    KafkaConnectClientError::FailedToDeploy(err.to_string())
                }
                StatusCode::NOT_FOUND => {
                    KafkaConnectClientError::NotFound(err.to_string())
                }
                _ => KafkaConnectClientError::UnexpectedError(err.to_string()),
            }
        } else {
            KafkaConnectClientError::UnexpectedError(err.to_string())
        }
    }
}
pub struct KafkaConnectClient {
    host: String,
}
impl KafkaConnectClient {
    pub fn new(host: &str, port: &str) -> KafkaConnectClient {
        Self {
            host: format!("{}:{}", host, port),
        }
    }
    pub async fn connector_exists(&self, conn_name: &str) -> Result<bool, KafkaConnectClientError> {
        let url = format!("{}/connectors/{}/status", &self.host, conn_name);
        let client = Client::new();
        let resp = client.get(&url).send().await?;

        if resp.status().is_success() {
            let body: serde_json::Value = resp.json().await?;
            if body["connector"]["state"] == "RUNNING" {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(KafkaConnectClientError::UnexpectedError(
                "Failed to check if connector is running".to_string(),
            ))
        }
    }

    pub async fn get_connector_config(
        &self,
        conn_name: &str,
    ) -> Result<KafkaConnectorDeployedConfig, KafkaConnectClientError> {
        let client = Client::new();
        let url = format!("{}/connectors/{}", &self.host, conn_name);
        let resp = client.get(&url).send().await?;

        if resp.status().is_success() {
            let config: KafkaConnectorDeployedConfig = resp.json().await?;
            Ok(config)
        } else {
            Err(KafkaConnectClientError::UnexpectedError(
                "Failed to fetch connector config".to_string(),
            ))
        }
    }

    pub async fn deploy_connector(
        &self,
        cfg: &KafkaConnectorDeployConfig,
    ) -> Result<(), KafkaConnectClientError> {
        let client = Client::new();
        let resp = client
            .post(format!("{}/connectors", self.host))
            .json(cfg)
            .send()
            .await?;

        match resp.status() {
            StatusCode::OK
            | StatusCode::CREATED
            | StatusCode::ACCEPTED
            | StatusCode::NO_CONTENT => Ok(()),
            StatusCode::BAD_REQUEST => {
                let body: ConnectErrorBody =
                    resp.json().await.unwrap_or_else(|_| ConnectErrorBody {
                        message: "could not parse error body".into(),
                    });
                Err(KafkaConnectClientError::FailedToDeploy(body.message))
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body: ConnectErrorBody = resp.json().await?;
                Err(KafkaConnectClientError::UnexpectedError(body.message))
            }
            status => Err(KafkaConnectClientError::UnexpectedError(status.to_string())),
        }
    }
}