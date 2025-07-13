pub mod types;
pub mod utils;
mod errors;

pub use types::*;
pub use errors::*;

use async_trait::async_trait;
use serde::{Deserialize};
use reqwest::{Client, StatusCode};
use crate::executor::ExecutorHost;
use crate::executor::kafka::utils::KafkaConnectUtils;

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
        cfg: &KafkaConnectorConfig
    ) -> Result<KafkaExecutorResponse, KafkaExecutorError>;
}

#[async_trait]
impl KafkaDeploy for KafkaExecutor {
    async fn deploy_connector(
        &self,
        cfg: &KafkaConnectorConfig
    ) -> Result<KafkaExecutorResponse, KafkaExecutorError> {
        let client = Client::new();
        let resp = client.post(format!("{}/connectors", self.connect_host))
            .json(cfg)
            .send()
            .await?;

        match resp.status() {
            StatusCode::OK |
            StatusCode::CREATED |
            StatusCode::ACCEPTED |
            StatusCode::NO_CONTENT => Ok(KafkaExecutorResponse::Ok),
            StatusCode::BAD_REQUEST => {
                let body: ConnectErrorBody = resp.json().await.unwrap_or_else(|_| ConnectErrorBody {
                    message: "could not parse error body".into(),
                });
                Err(KafkaExecutorError::IncorrectConfig(body.message))
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
impl KafkaConnectUtils for KafkaExecutor {}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};
    use reqwest::Client;
    use serde_json::json;
    use tokio::time::sleep;
    use uuid::Uuid;
    use crate::executor::kafka::{KafkaConnectorConfig, KafkaDeploy, KafkaExecutor, KafkaExecutorError, KafkaExecutorResponse};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::{method, path};

    const PG_CFG: &str =
        "host=localhost user=postgres password=password dbname=foundry_dev";

    async fn wait_for_running(connect_host: &str, name: &str) -> Result<(), KafkaExecutorError> {
        let client = Client::new();
        let status_url = format!("{connect_host}/connectors/{name}/status");
        let deadline = Instant::now() + Duration::from_secs(40);

        loop {
            let resp = client.get(&status_url).send().await?;
            if resp.status().is_success() {
                let body: serde_json::Value = resp.json().await?;
                if body["connector"]["state"] == "RUNNING" {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(KafkaExecutorError::IncorrectConfig(
                    "connector did not reach RUNNING state in time".into(),
                ));
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    fn get_connect_host() -> String {
        std::env::var("CONNECT_HOST").unwrap_or_else(|_| "http://localhost:8083".into())
    }

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

        let config: KafkaConnectorConfig = serde_json::from_value(json!({
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
        let res = executor.deploy_connector(
            &config
        ).await;

        let expected_error_msg = "Connector configuration is invalid and contains the following 1 error(s):\nError while validating connector config: The connection attempt failed.\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`";
        match res {
            Ok(_) => panic!("This shouldn't have worked"),
            Err(e) => match e {
                KafkaExecutorError::IncorrectConfig(e) => {
                    assert_eq!(e.to_string(), expected_error_msg);
                }
                _ => panic!("an Unexpected error"),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_connector_deploy() -> Result<(), KafkaExecutorError> {
        // ---------- PREPARE PG -----------------------------------------------
        // let (mut pg, pg_conn) = tokio_postgres::connect(PG_CFG, NoTls)
        //     .await
        //     .expect("connect to Postgres");
        // // run the connection in the background
        // tokio::spawn(async move {
        //     if let Err(e) = pg_conn.await {
        //         eprintln!("pg connection error: {e}");
        //     }
        // });
        //
        // pg.batch_execute(
        //     "
        // DROP TABLE IF EXISTS test_connector_src;
        // CREATE TABLE test_connector_src (
        //     id   SERIAL PRIMARY KEY,
        //     name TEXT NOT NULL
        // );
        // INSERT INTO test_connector_src (name) VALUES ('alice'), ('bob');
        // ",
        // )
        //     .await
        //     .expect("prepare table");
        //
        // // ---------- GIVEN -----------------------------------------------------
        // let connect_host =
        //     std::env::var("CONNECT_HOST").unwrap_or_else(|_| "http://localhost:8083".into());

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/connectors"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let connector_name = format!("test-pg-src-{}", Uuid::new_v4());

        let config: KafkaConnectorConfig = serde_json::from_value(json!({
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

        let resp = executor.deploy_connector(&config)
            .await?;

        assert_eq!(resp, KafkaExecutorResponse::Ok);

        // wait_for_running(&connect_host, &connector_name)
        //     .await
        //     .expect("connector never became RUNNING");



        // ---------- CLEANUP ---------------------------------------------------
        // let _ = Client::new().delete(&status_url).send().await;
        // let _ = pg.batch_execute("DROP TABLE IF EXISTS test_connector_src;").await;

        Ok(())
    }
}