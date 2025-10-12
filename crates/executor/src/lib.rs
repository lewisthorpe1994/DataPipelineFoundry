pub mod types;
use common::config::components::global::FoundryConfig;
use common::error::diagnostics::DiagnosticMessage;
use dag::types::{DagNode, DagNodeType};
use shared_clients::kafka::{
    KafkaConnectClient, KafkaConnectClientError, KafkaConnectorDeployConfig,
};
use shared_clients::{create_db_adapter, DatabaseAdapterError};
use std::error::Error;
use thiserror::Error;

pub trait ExecutorHost {
    fn host(&self) -> &str;
}
#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("connection failed: {context}")]
    FailedToConnect {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },
    #[error("execution failed: {context}")]
    FailedToExecute {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },
    #[error("unexpected error: {context}")]
    UnexpectedError {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },
    #[error("configuration error: {context}")]
    ConfigError {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },
    #[error("I/O error: {context}")]
    IoError {
        context: DiagnosticMessage,
        #[source]
        source: std::io::Error,
    },
    #[error("node is not executable: {context}")]
    NodeNotExecutable { context: DiagnosticMessage },
    #[error("resource not found: {context}")]
    ResourceNotFound { context: DiagnosticMessage },
}

impl ExecutorError {
    #[track_caller]
    pub fn config(message: impl Into<String>) -> Self {
        Self::ConfigError {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }

    #[track_caller]
    pub fn resource_not_found(message: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn failed_to_execute(message: impl Into<String>) -> Self {
        Self::FailedToExecute {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }

    #[track_caller]
    pub fn failed_to_connect(message: impl Into<String>) -> Self {
        Self::FailedToConnect {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }

    #[track_caller]
    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::UnexpectedError {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum ExecutorResponse {
    Ok,
}

impl From<DatabaseAdapterError> for ExecutorError {
    #[track_caller]
    fn from(value: DatabaseAdapterError) -> Self {
        match value {
            DatabaseAdapterError::InvalidConnectionError { context } => {
                ExecutorError::ConfigError {
                    context,
                    source: None,
                }
            }
            DatabaseAdapterError::SyntaxError { context } => ExecutorError::FailedToExecute {
                context,
                source: None,
            },
            DatabaseAdapterError::UnexpectedError { context } => ExecutorError::UnexpectedError {
                context,
                source: None,
            },
            DatabaseAdapterError::IoError { context, source } => {
                ExecutorError::IoError { context, source }
            }
            DatabaseAdapterError::ConfigError { context } => ExecutorError::ConfigError {
                context,
                source: None,
            },
        }
    }
}

impl From<KafkaConnectClientError> for ExecutorError {
    #[track_caller]
    fn from(value: KafkaConnectClientError) -> Self {
        match value {
            KafkaConnectClientError::NotFound { context } => {
                ExecutorError::ResourceNotFound { context }
            }
            KafkaConnectClientError::FailedToConnect { context } => {
                ExecutorError::FailedToConnect {
                    context,
                    source: None,
                }
            }
            KafkaConnectClientError::FailedToDeploy { context } => ExecutorError::FailedToExecute {
                context,
                source: None,
            },
            KafkaConnectClientError::UnexpectedError { context } => {
                ExecutorError::UnexpectedError {
                    context,
                    source: None,
                }
            }
        }
    }
}

pub struct Executor {}
impl Executor {
    pub fn new() -> Self {
        Self {}
    }
}
impl Executor {
    pub async fn execute(
        node: &DagNode,
        config: &FoundryConfig,
    ) -> Result<ExecutorResponse, ExecutorError> {
        match node.node_type {
            DagNodeType::Model => {
                let executable = node.compiled_obj.as_ref().ok_or_else(|| {
                    ExecutorError::config("Expected an executable statement for model node")
                })?;

                let target_name = node.target.as_ref().ok_or_else(|| {
                    ExecutorError::config(format!(
                        "Model '{}' is missing a target connection",
                        node.name
                    ))
                })?;

                let adapter_connection_obj = config
                    .get_adapter_connection_details(target_name)
                    .ok_or_else(|| {
                        ExecutorError::config(format!(
                            "Adapter connection '{}' is not defined in the project configuration",
                            target_name
                        ))
                    })?;

                let mut adapter = create_db_adapter(adapter_connection_obj).await?;
                adapter.execute(&executable).await?;
                Ok(ExecutorResponse::Ok)
            }
            DagNodeType::KafkaSourceConnector | DagNodeType::KafkaSinkConnector => {
                let executable = node.compiled_obj.as_ref().ok_or_else(|| {
                    ExecutorError::config(
                        "Expected a rendered connector configuration but none was provided",
                    )
                })?;

                let kafka_conn_name = node.target.as_ref().ok_or_else(|| {
                    ExecutorError::config("Kafka connector node is missing a target cluster name")
                })?;

                let kafka_conn =
                    config
                        .get_kafka_cluster_conn(kafka_conn_name)
                        .ok_or_else(|| {
                            ExecutorError::config(format!(
                                "Kafka cluster '{}' is not defined in the project configuration",
                                kafka_conn_name
                            ))
                        })?;

                let kafka_client =
                    KafkaConnectClient::new(&kafka_conn.connect.host, &kafka_conn.connect.port);

                println!("{:#?}", kafka_client);

                println!("{}", &executable);
                let conn_config: KafkaConnectorDeployConfig = serde_json::from_str(&executable)
                    .map_err(|e| {
                        ExecutorError::config(format!(
                            "Failed to parse Kafka connector configuration for '{}': {e}",
                            kafka_conn_name
                        ))
                    })?;

                kafka_client.deploy_connector(&conn_config).await?;

                Ok(ExecutorResponse::Ok)
            }
            _ => Err(ExecutorError::unexpected(format!(
                "Node '{}' with type {:?} is not supported by the executor",
                node.name, node.node_type
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{Executor, ExecutorError};
    use common::config::components::connections::AdapterConnectionDetails;
    use common::config::components::sources::kafka::{
        KafkaBootstrap, KafkaConnect, KafkaSourceConfig, KafkaSourceConfigs,
    };
    use common::config::loader::read_config;
    use common::types::kafka::KafkaConnectorType;
    use dag::types::{DagNode, DagNodeType};
    use shared_clients::create_db_adapter;
    use shared_clients::kafka::KafkaConnectClient;
    use std::collections::HashMap;
    use std::time::Duration;
    use test_utils::{get_root_dir, setup_kafka, setup_postgres, with_chdir_async};
    use test_utils::{NET_HOST, PG_DB, PG_PASSWORD, PG_USER};
    use tokio::time::sleep;
    use uuid::Uuid;

    async fn set_up_table(pg_conn: AdapterConnectionDetails) {
        let mut adapter = create_db_adapter(pg_conn).await.unwrap();
        adapter
            .execute(
                "
            DROP TABLE IF EXISTS test_connector_src;
            CREATE TABLE test_connector_src (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
            INSERT INTO test_connector_src (name) VALUES ('alice'), ('bob');
            ",
            )
            .await
            .expect("prepare table");
    }

    #[tokio::test]
    async fn test_execute_db() {
        let postgres_test_container = setup_postgres().await.unwrap();
        sleep(Duration::from_secs(5)).await;
        let node = DagNode {
            name: "test_db_node".to_string(),
            node_type: DagNodeType::Model,
            is_executable: false,
            relations: None,
            target: None,
            compiled_obj: Some(
                "DROP TABLE IF EXISTS public.orders CASCADE;
CREATE TABLE public.orders AS SELECT 'test' as col"
                    .to_string(),
            ),
            ast: None,
        };
        let project_root = get_root_dir();
        with_chdir_async(&project_root, || {
            let mut config = read_config(None).expect("load example project config");

            let executor = Executor::new();

            if let Some(profile) = config.connections.get_mut(&config.connection_profile) {
                if let Some(details) = profile.get_mut(&config.warehouse_db_connection) {
                    details.host = postgres_test_container.local_host.to_string();
                    details.port = postgres_test_container.port.to_string();
                }
            }

            async move {
                Executor::execute(&node, &config)
                    .await
                    .expect("executor to run");

                let db_adapter_con = config.get_adapter_connection_details().unwrap();
                let mut adapter = create_db_adapter(db_adapter_con).await.unwrap();

                let res = adapter.query("select * from public.orders").await.unwrap();

                assert_eq!(res.len(), 1);
                assert_eq!(res[0].get::<_, String>("col"), "test".to_string());
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_create_connector() -> Result<(), ExecutorError> {
        let postgres_test_container = setup_postgres().await.unwrap();
        let kafka_test_containers = setup_kafka().await.unwrap();
        sleep(Duration::from_secs(5)).await;

        let con_name = "test";
        let sql = format!(
            r#"{{"name": "{con_name}",
"config": {{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.user": "{PG_USER}",
    "database.password": "{PG_PASSWORD}",
    "database.port": {},
    "database.hostname": "{}",
    "database.dbname": "{PG_DB}",
    "table.include.list": "public.test_connector_src",
    "snapshot.mode": "initial",
    "kafka.bootstrap.servers": "{}",
    "topic.prefix": "postgres-"
    }}
}}"#,
            5432, postgres_test_container.docker_host, kafka_test_containers.kafka_broker.host
        );

        let node = DagNode {
            name: "test_con".to_string(),
            ast: None,
            compiled_obj: Some(sql),
            node_type: DagNodeType::KafkaSourceConnector,
            is_executable: true,
            relations: None,
            target: Some("some_kafka_cluster".to_string()),
        };

        let kafka_client = KafkaConnectClient::new(
            &kafka_test_containers.kafka_connect.host,
            &kafka_test_containers.kafka_connect.port,
        );

        let project_root = get_root_dir();
        with_chdir_async(&project_root, || {
            let mut config = read_config(None).expect("load example project config");
            if let Some(profile) = config.connections.get_mut(&config.connection_profile) {
                if let Some(details) = profile.get_mut(&config.warehouse_db_connection) {
                    details.host = postgres_test_container.local_host.to_string();
                    details.port = postgres_test_container.port.to_string();
                }
            }

            config.kafka_source = KafkaSourceConfigs(HashMap::from([(
                node.target.clone().unwrap(),
                KafkaSourceConfig {
                    name: node.target.clone().unwrap(),
                    bootstrap: KafkaBootstrap {
                        servers: kafka_test_containers.kafka_broker.host.clone(),
                    },
                    connect: KafkaConnect {
                        host: NET_HOST.to_string(),
                        port: kafka_test_containers.kafka_connect.port,
                    },
                },
            )]));

            let db_adapter_con = config.get_adapter_connection_details().unwrap();
            async move {
                set_up_table(db_adapter_con).await;

                let _ = Executor::execute(&node, &config).await.unwrap();
            }
        })
        .await
        .unwrap();
        let conn_exists = kafka_client.connector_exists(con_name).await?;
        assert!(conn_exists);
        let conn_config = kafka_client.get_connector_config(con_name).await.unwrap();
        println!("{}", conn_config.config.to_string());
        assert_eq!(conn_config.conn_type.unwrap(), KafkaConnectorType::Source);
        assert_eq!(
            conn_config.config["table.include.list"],
            "public.test_connector_src"
        );
        assert_eq!(conn_config.config["snapshot.mode"], "initial");
        assert_eq!(
            conn_config.config["kafka.bootstrap.servers"],
            kafka_test_containers.kafka_broker.host
        );
        assert_eq!(conn_config.config["topic.prefix"], "postgres-");
        Ok(())
    }
}
