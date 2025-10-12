use crate::error::diagnostics::DiagnosticMessage;
use std::{error::Error as StdError, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("incorrect path: {context}")]
    IncorrectPath { context: DiagnosticMessage },
    #[error("missing connection: {context}")]
    MissingConnection { context: DiagnosticMessage },
    #[error("parse error: {context}")]
    ParseError {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
    #[error("filesystem error: {context}")]
    PathError {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
    #[error("duplicate database specification: {context}")]
    DuplicateDatabaseSpecification { context: DiagnosticMessage },
    #[error("not found: {context}")]
    NotFound { context: DiagnosticMessage },
}

impl ConfigError {
    #[track_caller]
    pub fn incorrect_path(path: impl AsRef<Path>) -> Self {
        let message = format!("Expected path '{}' to exist", path.as_ref().display());
        Self::IncorrectPath {
            context: DiagnosticMessage::new(message),
        }
    }

    #[track_caller]
    pub fn missing_connection(path: impl AsRef<Path>) -> Self {
        let message = format!(
            "Connection profile not found at '{}'. Ensure the file exists and is readable.",
            path.as_ref().display()
        );
        Self::MissingConnection {
            context: DiagnosticMessage::new(message),
        }
    }

    #[track_caller]
    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::ParseError {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }

    #[track_caller]
    pub fn duplicate_database(name: impl Into<String>) -> Self {
        let message = format!(
            "Found entries for both source and warehouse DBs for '{}'",
            name.into()
        );
        Self::DuplicateDatabaseSpecification {
            context: DiagnosticMessage::new(message),
        }
    }

    #[track_caller]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}

impl From<std::io::Error> for ConfigError {
    #[track_caller]
    fn from(err: std::io::Error) -> Self {
        let message = err.to_string();
        ConfigError::PathError {
            context: DiagnosticMessage::new(message),
            source: Some(Box::new(err)),
        }
    }
}

impl From<serde_yaml::Error> for ConfigError {
    #[track_caller]
    fn from(err: serde_yaml::Error) -> Self {
        let message = err.to_string();
        ConfigError::ParseError {
            context: DiagnosticMessage::new(message),
            source: Some(Box::new(err)),
        }
    }
}

impl From<walkdir::Error> for ConfigError {
    #[track_caller]
    fn from(err: walkdir::Error) -> Self {
        let message = err.to_string();
        ConfigError::PathError {
            context: DiagnosticMessage::new(message),
            source: Some(Box::new(err)),
        }
    }
}
