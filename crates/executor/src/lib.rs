pub mod types;
pub mod sql;
use dag::types::{DagNode, DagNodeType};
use shared_clients::{create_db_adapter, AsyncDatabaseAdapter, DatabaseAdapter, DatabaseAdapterError};
use std::error::Error;
use std::fmt;
use common::config::components::global::FoundryConfig;
use shared_clients::kafka::KafkaConnectClient;

pub trait ExecutorHost {
    fn host(&self) -> &str;
}
#[derive(Debug)]
pub enum ExecutorError {
    FailedToConnect(String),
    FailedToExecute(String),
    UnexpectedError(String),
    InvalidFilePath(String),
    ConfigError(String),
    ParseError(String),
    IoError(std::io::Error),
}

#[derive(PartialEq, Debug)]
pub enum ExecutorResponse {
    Ok,
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::FailedToConnect(msg) => write!(f, "Connection failed: {}", msg),
            ExecutorError::FailedToExecute(msg) => write!(f, "Execution failed: {}", msg),
            ExecutorError::InvalidFilePath(msg) => write!(f, "Invalid file path: {}", msg),
            ExecutorError::ConfigError(msg) => write!(f, "Configuration error: {}", msg),
            ExecutorError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            ExecutorError::UnexpectedError(msg) => write!(f, "Unexpected error: {}", msg),
            ExecutorError::IoError(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl Error for ExecutorError {}

impl From<KafkaExecutorError> for ExecutorError {
    fn from(e: KafkaExecutorError) -> ExecutorError {
        match e {
            KafkaExecutorError::ConnectionError(e) => ExecutorError::FailedToConnect(e.to_string()),
            KafkaExecutorError::IncorrectConfig(e) => ExecutorError::FailedToExecute(e.to_string()),
            KafkaExecutorError::IoError(e) => ExecutorError::IoError(e),
            KafkaExecutorError::UnexpectedError(e) => ExecutorError::UnexpectedError(e.to_string()),
            KafkaExecutorError::InternalServerError(e) => {
                ExecutorError::UnexpectedError(e.to_string())
            }
        }
    }
}

impl From<DatabaseAdapterError> for ExecutorError {
    fn from(value: DatabaseAdapterError) -> Self {
        ExecutorError::UnexpectedError(value.to_string())
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
        &self,
        node: &DagNode,
        config: &FoundryConfig
    ) -> Result<ExecutorResponse, ExecutorError> {
        match node.node_type {
            DagNodeType::Model => {
                let executable = node.compiled_obj.ok_or(
                    ExecutorError::ConfigError("Expected a executable string for a model but got none".to_string())
                )?;

                let adapter_connection_obj = config
                    .get_adapter_connection_details()
                    .ok_or_else(|| ExecutorError::ConfigError("missing adapter connection details".into()))?;

                let mut adapter = create_db_adapter(adapter_connection_obj).await?;
                adapter.execute(&executable).await?;
                Ok(ExecutorResponse::Ok)
            }
            DagNodeType::KafkaSourceConnector => {
                let executable = node.compiled_obj.ok_or(
                    ExecutorError::ConfigError("Expected a executable string for a model but got none".to_string())
                )?;
                let kafka_conn_name = node.target.ok_or(ExecutorError::ConfigError(
                    "Expected a kafka connection name but got none".to_string()
                ))?;
                let kafka_conn = config.get_kafka_cluster_conn(&kafka_conn_name)
                    .ok_or(ExecutorError::ConfigError(
                        format!("No kafka cluster connection details found for {}", kafka_conn_name)
                    ))?;

                let kafka_client = KafkaConnectClient::new(
                    &kafka_conn.connect.host,
                    &kafka_conn.connect.port
                );

            }
        }
    }
}