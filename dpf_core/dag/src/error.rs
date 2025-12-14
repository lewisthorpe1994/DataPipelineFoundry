use catalog::error::CatalogError;
use common::error::diagnostics::DiagnosticMessage;
use components::errors::KafkaConnectorCompileError;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("duplicate node: {context}")]
    DuplicateNode { context: DiagnosticMessage },
    #[error("missing dependency: {context}")]
    MissingExpectedDependency { context: DiagnosticMessage },
    #[error("cycle detected involving: {0:?}")]
    CycleDetected(Vec<String>),
    #[error("AST error: {context}")]
    AstSyntax { context: DiagnosticMessage },
    #[error("I/O error: {context}")]
    Io {
        context: DiagnosticMessage,
        #[source]
        source: io::Error,
    },
    #[error("reference not found: {context}")]
    RefNotFound { context: DiagnosticMessage },
    #[error("not found: {context}")]
    NotFound { context: DiagnosticMessage },
    #[error("execution error: {context}")]
    ExecutionError { context: DiagnosticMessage },
    #[error("invalid direction: {context}")]
    InvalidDirection { context: DiagnosticMessage },
    #[error("unexpected error: {context}")]
    UnexpectedError { context: DiagnosticMessage },
}

impl DagError {
    #[track_caller]
    pub fn duplicate_node(node_name: impl Into<String>) -> Self {
        Self::DuplicateNode {
            context: DiagnosticMessage::new(format!(
                "Node '{}' was defined multiple times",
                node_name.into()
            )),
        }
    }

    #[track_caller]
    pub fn missing_dependency(detail: impl Into<String>) -> Self {
        Self::MissingExpectedDependency {
            context: DiagnosticMessage::new(detail.into()),
        }
    }

    #[track_caller]
    pub fn cycle_detected(nodes: Vec<String>) -> Self {
        Self::CycleDetected(nodes)
    }

    #[track_caller]
    pub fn ast_syntax(message: impl Into<String>) -> Self {
        Self::AstSyntax {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn ref_not_found(name: impl Into<String>) -> Self {
        Self::RefNotFound {
            context: DiagnosticMessage::new(name.into()),
        }
    }

    #[track_caller]
    pub fn execution(message: impl Into<String>) -> Self {
        Self::ExecutionError {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn invalid_direction(message: impl Into<String>) -> Self {
        Self::InvalidDirection {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn unexpected_error(message: impl Into<String>) -> Self {
        Self::UnexpectedError {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}

impl From<io::Error> for DagError {
    #[track_caller]
    fn from(value: io::Error) -> Self {
        DagError::Io {
            context: DiagnosticMessage::new(value.to_string()),
            source: value,
        }
    }
}

impl From<DagError> for JinjaError {
    fn from(err: DagError) -> Self {
        match err {
            DagError::CycleDetected(nodes) => {
                let mut message = String::from("Found cyclic references in DAG for:");
                for node in &nodes {
                    message.push_str("\n - ");
                    message.push_str(node);
                }
                JinjaError::new(JinjaErrorKind::UndefinedError, message)
            }
            other => JinjaError::new(JinjaErrorKind::UndefinedError, other.to_string()),
        }
    }
}

impl From<KafkaConnectorCompileError> for DagError {
    fn from(value: KafkaConnectorCompileError) -> Self {
        match value {
            KafkaConnectorCompileError::Duplicate { context }
            | KafkaConnectorCompileError::MissingConfig { context }
            | KafkaConnectorCompileError::ValidationError { context }
            | KafkaConnectorCompileError::Unsupported { context }
            | KafkaConnectorCompileError::ConfigError { context }
            | KafkaConnectorCompileError::SerdeJson { context, .. }
            | KafkaConnectorCompileError::UnexpectedError { context } => {
                DagError::UnexpectedError { context }
            }
            KafkaConnectorCompileError::NotFound { context } => DagError::NotFound { context },
        }
    }
}

impl From<CatalogError> for DagError {
    fn from(value: CatalogError) -> Self {
        match value {
            CatalogError::MissingConfig { context }
            | CatalogError::SqlParser { context, .. }
            | CatalogError::Unsupported { context }
            | CatalogError::SerdeYaml { context, .. }
            | CatalogError::SerdeJson { context, .. } => DagError::UnexpectedError { context },
            CatalogError::Io { context, source } => DagError::Io { context, source },
            CatalogError::Duplicate { context } => DagError::DuplicateNode { context },
            CatalogError::NotFound { context } => DagError::NotFound { context },
            CatalogError::PythonNode { context } => DagError::UnexpectedError { context },
        }
    }
}
