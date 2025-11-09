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
            KafkaConnectClientError::ValidationFailed { context } => {
                ExecutorError::FailedToExecute {
                    context,
                    source: None,
                }
            }
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
impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

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
                adapter.execute(executable).await?;
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

                let kafka_conn = config
                    .get_kafka_cluster_conn(kafka_conn_name)
                    .map_err(|e| ExecutorError::config(e.to_string()))?;

                let kafka_client =
                    KafkaConnectClient::new(&kafka_conn.connect.host, &kafka_conn.connect.port);

                let conn_config: KafkaConnectorDeployConfig = serde_json::from_str(executable)
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
}
