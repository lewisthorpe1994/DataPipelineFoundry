use common::error::diagnostics::DiagnosticMessage;
use std::io;
use std::io::Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("catalog entry already exists: {context}")]
    Duplicate { context: DiagnosticMessage },
    #[error("catalog lookup failed: {context}")]
    NotFound { context: DiagnosticMessage },
    #[error("serde json error: {context}")]
    SerdeJson {
        context: DiagnosticMessage,
        #[source]
        source: serde_json::Error,
    },
    #[error("serde yaml error: {context}")]
    SerdeYaml {
        context: DiagnosticMessage,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("I/O error: {context}")]
    Io {
        context: DiagnosticMessage,
        #[source]
        source: io::Error,
    },
    #[error("unsupported operation: {context}")]
    Unsupported { context: DiagnosticMessage },
    #[error("SQL parser error: {context}")]
    SqlParser { context: DiagnosticMessage },
    #[error("missing configuration: {context}")]
    MissingConfig { context: DiagnosticMessage },
    #[error("python node error: {context}")]
    PythonNode { context: DiagnosticMessage },
}

impl CatalogError {
    #[track_caller]
    pub fn duplicate(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::Duplicate {
            context: DiagnosticMessage::new(format!("Entry '{name}' already exists")),
        }
    }

    #[track_caller]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
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
    pub fn sql_parser(message: impl Into<String>) -> Self {
        Self::SqlParser {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn missing_config(message: impl Into<String>) -> Self {
        Self::MissingConfig {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn io(message: impl Into<String>, source: Error) -> Self {
        Self::Io {
            context: DiagnosticMessage::new(message.into()),
            source,
        }
    }

    #[track_caller]
    pub fn python_node(message: impl Into<String>) -> Self {
        Self::PythonNode {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}

impl From<serde_json::Error> for CatalogError {
    #[track_caller]
    fn from(err: serde_json::Error) -> Self {
        CatalogError::SerdeJson {
            context: DiagnosticMessage::new(err.to_string()),
            source: err,
        }
    }
}

impl From<serde_yaml::Error> for CatalogError {
    #[track_caller]
    fn from(err: serde_yaml::Error) -> Self {
        CatalogError::SerdeYaml {
            context: DiagnosticMessage::new(err.to_string()),
            source: err,
        }
    }
}

impl From<io::Error> for CatalogError {
    #[track_caller]
    fn from(err: io::Error) -> Self {
        CatalogError::Io {
            context: DiagnosticMessage::new(err.to_string()),
            source: err,
        }
    }
}
