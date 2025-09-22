mod errors;
pub mod types;
pub mod utils;

pub use errors::*;
pub use types::*;
pub use utils::*;

use crate::executor::ExecutorHost;
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;

#[derive(Deserialize)]
struct ConnectErrorBody {
    message: String,
}

pub struct KafkaExecutor {
    connect_host: String,
}
impl KafkaExecutor {
    pub fn new(connect_host: &str) -> KafkaExecutor {
        Self {
            connect_host: connect_host.to_string(),
        }
    }
}

#[async_trait]
pub trait KafkaDeploy: Send + Sync {
    async fn deploy_connector(
        &self,
        cfg: &KafkaConnectorDeployConfig,
    ) -> Result<KafkaExecutorResponse, KafkaExecutorError>;
}

#[async_trait]
impl KafkaDeploy for KafkaExecutor {
    async fn deploy_connector(
        &self,
        cfg: &KafkaConnectorDeployConfig,
    ) -> Result<KafkaExecutorResponse, KafkaExecutorError> {
        let client = Client::new();
        let resp = client
            .post(format!("{}/connectors", self.connect_host))
            .json(cfg)
            .send()
            .await?;

        match resp.status() {
            StatusCode::OK
            | StatusCode::CREATED
            | StatusCode::ACCEPTED
            | StatusCode::NO_CONTENT => Ok(KafkaExecutorResponse::Ok),
            StatusCode::BAD_REQUEST => {
                let body: ConnectErrorBody =
                    resp.json().await.unwrap_or_else(|_| ConnectErrorBody {
                        message: "could not parse error body".into(),
                    });
                Err(KafkaExecutorError::IncorrectConfig(body.message))
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body: ConnectErrorBody = resp.json().await?;
                Err(KafkaExecutorError::InternalServerError(body.message))
            }
            status => Err(KafkaExecutorError::UnexpectedError(status.to_string())),
        }
    }

    // pub async fn drop_connector(connect_host: &str, connector_name: &str) -> Result<(), KafkaExecutorError> {
    //     let client = Client::new();
    //     let resp = client.delete(
    //         format!("{}/connectors/{}", connect_host, connector_name)
    //     ).send().await?;
    //
    //     Ok(())
    // }
}

impl ExecutorHost for KafkaExecutor {
    fn host(&self) -> &str {
        self.connect_host.as_str()
    }
}

#[async_trait]
impl KafkaConnectClient for KafkaExecutor {}

#[cfg(test)]
mod test {
    use crate::executor::kafka::{
        KafkaConnectorDeployConfig, KafkaDeploy, KafkaExecutor, KafkaExecutorError,
        KafkaExecutorResponse,
    };
    use serde_json::json;
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_connector_deploy_fails_with_cfg_error() -> Result<(), KafkaExecutorError> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/connectors"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({         // <- the body your code expects
                "message": "Connector configuration is invalid and contains the following 1 error(s):\nError while validating connector config: The connection attempt failed.\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`"
            }))
            )
            .mount(&server)
            .await;

        let config: KafkaConnectorDeployConfig = serde_json::from_value(json!({
            "name": "some_test_fail_conn",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "database.user": "postgres",
                "database.password": "password",
                "database.port": "5432",
                "database.hostname": "should_fail_for_this",
                "database.dbname": "foundry_dev",
                "table.whitelist": "public.test_connector_src",
                "snapshot.mode": "initial",
                "topic.prefix": "postgres-"
            }
        }))?;
        let executor = KafkaExecutor::new(&server.uri());
        let res = executor.deploy_connector(&config).await;

        let expected_error_msg = "Connector configuration is invalid and contains the following 1 error(s):\nError while validating connector config: The connection attempt failed.\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`";
        match res {
            Ok(_) => panic!("This shouldn't have worked"),
            Err(e) => match e {
                KafkaExecutorError::IncorrectConfig(e) => {
                    assert_eq!(e.to_string(), expected_error_msg);
                }
                _ => panic!("an Unexpected error"),
            },
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_connector_deploy() -> Result<(), KafkaExecutorError> {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/connectors"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let connector_name = format!("test-pg-src-{}", Uuid::new_v4());

        let config: KafkaConnectorDeployConfig = serde_json::from_value(json!({
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "database.user": "postgres",
                "database.password": "password",
                "database.port": "5432",
                "database.hostname": "postgres",
                "database.dbname": "foundry_dev",
                "table.whitelist": "public.test_connector_src",
                "snapshot.mode": "initial",
                "topic.prefix": "postgres-"
            }
        }))?;

        let executor = KafkaExecutor::new(&server.uri());

        let resp = executor.deploy_connector(&config).await?;

        assert_eq!(resp, KafkaExecutorResponse::Ok);
        Ok(())
    }
}
