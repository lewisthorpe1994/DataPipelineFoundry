use common::error::diagnostics::DiagnosticMessage;
use common::types::kafka::KafkaConnectorType;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

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

#[derive(Debug, Error)]
pub enum KafkaConnectClientError {
    #[error("connector not found: {context}")]
    NotFound { context: DiagnosticMessage },
    #[error("connectivity error: {context}")]
    FailedToConnect { context: DiagnosticMessage },
    #[error("deployment failed: {context}")]
    FailedToDeploy { context: DiagnosticMessage },
    #[error("validation failed: {context}")]
    ValidationFailed { context: DiagnosticMessage },
    #[error("unexpected response: {context}")]
    UnexpectedError { context: DiagnosticMessage },
}

impl KafkaConnectClientError {
    #[track_caller]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn failed_to_connect(message: impl Into<String>) -> Self {
        Self::FailedToConnect {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn failed_to_deploy(message: impl Into<String>) -> Self {
        Self::FailedToDeploy {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::UnexpectedError {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn validation_failed(message: impl Into<String>) -> Self {
        Self::ValidationFailed {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}

#[derive(Deserialize)]
struct ConnectErrorBody {
    message: String,
}

impl From<reqwest::Error> for KafkaConnectClientError {
    #[track_caller]
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            KafkaConnectClientError::failed_to_connect(err.to_string())
        } else if let Some(err) = err.status() {
            match err {
                StatusCode::BAD_REQUEST => {
                    KafkaConnectClientError::failed_to_deploy(err.to_string())
                }
                StatusCode::NOT_FOUND => KafkaConnectClientError::not_found(err.to_string()),
                _ => KafkaConnectClientError::unexpected(format!(
                    "Unexpected error due to {} - status code {}",
                    err.to_string(),
                    err
                )),
            }
        } else {
            KafkaConnectClientError::unexpected(format!(
                "Unexpected error trying to send kafka connect request: {}",
                err
            ))
        }
    }
}
#[derive(Debug)]
pub struct KafkaConnectClient {
    host: String,
}
impl KafkaConnectClient {
    pub fn new(host: &str, port: &str) -> KafkaConnectClient {
        Self {
            host: format!("http://{}:{}", host, port),
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
            Err(KafkaConnectClientError::unexpected(
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
            Err(KafkaConnectClientError::unexpected(
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
                Err(KafkaConnectClientError::failed_to_deploy(body.message))
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body: ConnectErrorBody = resp.json().await?;
                Err(KafkaConnectClientError::unexpected(format!(
                    "Failed to deploy connector due to unexpected issue: {}",
                    body.message
                )))
            }
            status => Err(KafkaConnectClientError::unexpected(format!(
                "Unexpected error trying to deploy connector due to {} - status code {}",
                status.to_string(),
                status.as_u16()
            ))),
        }
    }

    pub async fn validate_connector(
        &self,
        connector_class: &str,
        config: &Value,
    ) -> Result<(), KafkaConnectClientError> {
        let client = Client::new();
        let url = format!(
            "{}/connector-plugins/{}/config/validate",
            &self.host, connector_class
        );

        let resp = client.post(url).json(&config).send().await?;
        let status = resp.status();

        if status.is_success() {
            let body: Value = resp.json().await?;
            let error_count = body
                .get("error_count")
                .and_then(|value| value.as_i64())
                .unwrap_or_default();

            if error_count > 0 {
                let message =
                    serde_json::to_string_pretty(&body).unwrap_or_else(|_| body.to_string());
                Err(KafkaConnectClientError::validation_failed(message))
            } else {
                Ok(())
            }
        } else {
            let fallback = format!("validation request failed with status {status}");
            let body_message = resp
                .json::<ConnectErrorBody>()
                .await
                .map(|body| body.message)
                .unwrap_or(fallback);

            match status {
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                    Err(KafkaConnectClientError::validation_failed(body_message))
                }
                StatusCode::NOT_FOUND => Err(KafkaConnectClientError::not_found(body_message)),
                _ => Err(KafkaConnectClientError::unexpected(format!(
                    "Unexpected error during connector validation: {} (status {})",
                    body_message, status
                ))),
            }
        }
    }
}
