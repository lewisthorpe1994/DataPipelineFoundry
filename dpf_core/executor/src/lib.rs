mod python;
pub mod types;

use crate::python::PythonExecutor;
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

    #[track_caller]
    pub fn node_not_executable(message: impl Into<String>) -> Self {
        Self::NodeNotExecutable {
            context: DiagnosticMessage::new(message.into()),
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
        if !node.is_executable() {
            return Err(ExecutorError::node_not_executable(node.name()));
        }
        match node {
            DagNode::Model {
                name,
                compiled_obj,
                target,
                ..
            } => {
                let executable = compiled_obj.as_ref().ok_or_else(|| {
                    ExecutorError::config("Expected an executable statement for model node")
                })?;

                let target_name = target.as_ref().ok_or_else(|| {
                    ExecutorError::config(format!(
                        "Model '{}' is missing a target connection",
                        name
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
            DagNode::KafkaSourceConnector {
                name,
                compiled_obj,
                target,
                ..
            }
            | DagNode::KafkaSinkConnector {
                name,
                compiled_obj,
                target,
                ..
            } => {
                let executable = compiled_obj.as_ref().ok_or_else(|| {
                    ExecutorError::config(
                        "Expected a rendered connector configuration but none was provided",
                    )
                })?;

                let kafka_conn_name = target.as_ref().ok_or_else(|| {
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
            DagNode::Python { name, job_dir, .. } => {
                PythonExecutor::execute(name, job_dir).map_err(|e| {
                    ExecutorError::unexpected(format!("Python executor failed: {}", e))
                })?;
                Ok(ExecutorResponse::Ok)
            }
            _ => Err(ExecutorError::unexpected(format!(
                "Node '{:#?}' is not supported by the executor",
                node
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::components::connections::{Connections, ConnectionsConfig};
    use common::config::components::foundry_project::FoundryProjectConfig;
    use common::config::components::global::FoundryConfig;
    use common::config::components::model::ModelsProjects;
    use common::config::components::sources::warehouse_source::DbConfig;
    use common::config::components::sources::SourcePaths;
    use dag::types::{DagNode, NodeAst};
    use sqlparser::ast::{CreateSimpleMessageTransform, Ident};
    use std::{collections::HashMap, path::PathBuf};

    fn dummy_foundry_config() -> FoundryConfig {
        let project = FoundryProjectConfig {
            name: "test".into(),
            version: "0.1".into(),
            compile_path: "compiled".into(),
            modelling_architecture: String::new(),
            connection_profile: Connections {
                profile: "default".into(),
                path: PathBuf::new(),
            },
            models: ModelsProjects {
                dir: "models".into(),
                analytics_projects: None,
            },
            sources: SourcePaths::default(),
            python: None,
        };

        FoundryConfig::new(
            project,
            HashMap::<String, DbConfig>::new(),
            ConnectionsConfig::default(),
            None,
            HashMap::new(),
            Connections {
                profile: "default".into(),
                path: PathBuf::new(),
            },
            HashMap::new(),
            SourcePaths::default(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    fn dummy_ast(name: &str) -> NodeAst {
        NodeAst::KafkaSmt(CreateSimpleMessageTransform {
            name: Ident::new(name),
            if_not_exists: false,
            config: Vec::new(),
            preset: None,
            overrides: Vec::new(),
            predicate: None,
        })
    }

    fn model_node(compiled: Option<&str>, target: Option<&str>, executable: bool) -> DagNode {
        DagNode::Model {
            name: "bronze_model".into(),
            ast: dummy_ast("bronze_model"),
            compiled_obj: compiled.map(str::to_owned),
            is_executable: executable,
            relations: None,
            target: target.map(str::to_owned),
        }
    }

    fn kafka_source_node(
        compiled: Option<&str>,
        target: Option<&str>,
        executable: bool,
    ) -> DagNode {
        DagNode::KafkaSourceConnector {
            name: "connector".into(),
            ast: dummy_ast("connector"),
            compiled_obj: compiled.map(str::to_owned),
            is_executable: executable,
            relations: None,
            target: target.map(str::to_owned),
        }
    }

    #[tokio::test]
    async fn model_without_compiled_sql_errors() {
        let config = dummy_foundry_config();
        let node = model_node(None, Some("warehouse"), true);

        let err = Executor::execute(&node, &config)
            .await
            .expect_err("missing compiled sql");
        assert!(matches!(err, ExecutorError::ConfigError { .. }));
    }

    #[tokio::test]
    async fn model_without_target_is_rejected() {
        let config = dummy_foundry_config();
        let node = model_node(Some("SELECT 1"), None, true);

        let err = Executor::execute(&node, &config)
            .await
            .expect_err("missing target should fail");
        assert!(matches!(err, ExecutorError::ConfigError { .. }));
    }

    #[tokio::test]
    async fn node_not_marked_executable_is_rejected() {
        let config = dummy_foundry_config();
        let node = model_node(Some("SELECT 1"), Some("warehouse"), false);

        let err = Executor::execute(&node, &config)
            .await
            .expect_err("non executable");
        assert!(matches!(err, ExecutorError::NodeNotExecutable { .. }));
    }

    #[tokio::test]
    async fn kafka_connector_without_compiled_config_errors() {
        let config = dummy_foundry_config();
        let node = kafka_source_node(None, Some("cluster_a"), true);

        let err = Executor::execute(&node, &config)
            .await
            .expect_err("missing compiled connector");
        assert!(matches!(err, ExecutorError::ConfigError { .. }));
    }

    #[tokio::test]
    async fn kafka_connector_with_unknown_cluster_errors() {
        let config = dummy_foundry_config();
        let node = kafka_source_node(Some("{}"), Some("cluster_a"), true);

        let err = Executor::execute(&node, &config)
            .await
            .expect_err("cluster missing");
        assert!(matches!(err, ExecutorError::ConfigError { .. }));
    }
}
