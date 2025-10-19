use catalog::error::CatalogError;
use common::error::DiagnosticMessage;
use thiserror::Error;
use crate::smt::TransformBuildError;

#[derive(Debug, Error)]
pub enum KafkaConnectorCompileError {
    #[error("config entry not found: {context}")]
    NotFound { context: DiagnosticMessage },
    #[error("Duplicate config entry: {context}")]
    Duplicate { context: DiagnosticMessage },
    #[error("serde json error: {context}")]
    SerdeJson {
        context: DiagnosticMessage,
        #[source]
        source: serde_json::Error,
    },
    #[error("Expected config entry: {context}")]
    MissingConfig { context: DiagnosticMessage },
    #[error("Unexpected error: {context}")]
    UnexpectedError { context: DiagnosticMessage },
    #[error("Unsupported: {context}")]
    Unsupported { context: DiagnosticMessage },
    #[error("Config error: {context}")]
    ConfigError { context: DiagnosticMessage },

}

impl KafkaConnectorCompileError {
    #[track_caller]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn duplicate(message: impl Into<String>) -> Self {
        Self::Duplicate {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn serde_json(message: impl Into<String>, source: serde_json::Error) -> Self {
        Self::SerdeJson {
            context: DiagnosticMessage::new(message.into()),
            source,
        }
    }

    #[track_caller]
    pub fn missing_config(message: impl Into<String>) -> Self {
        Self::MissingConfig {
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
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported {
            context: DiagnosticMessage::new(message.into()),
        }
    }
    
    #[track_caller]
    pub fn config(message: impl Into<String>) -> Self {
        Self::ConfigError {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}

impl From<CatalogError> for KafkaConnectorCompileError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::NotFound { context } => {
                KafkaConnectorCompileError::not_found(context.message())
            }
            CatalogError::Duplicate { context } => {
                KafkaConnectorCompileError::duplicate(context.message())
            }
            CatalogError::MissingConfig { context } => {
                KafkaConnectorCompileError::missing_config(context.message())
            }
            CatalogError::SerdeJson { context, source } => {
                KafkaConnectorCompileError::serde_json(context.message(), source)
            }
            _ => KafkaConnectorCompileError::unexpected_error("An expected error occurred"),
        }
    }
}

impl From<TransformBuildError> for KafkaConnectorCompileError {
    fn from(value: TransformBuildError) -> Self {
        match value {
            TransformBuildError::MissingType { context } => {
                KafkaConnectorCompileError::missing_config( context.message() )
            },
            TransformBuildError::InvalidValue { context } => { 
                KafkaConnectorCompileError::config( context.message() )
            }
        }
    }
}
