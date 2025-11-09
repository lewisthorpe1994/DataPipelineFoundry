use crate::errors::ValidationError;
use common::error::DiagnosticMessage;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransformBuildError {
    #[error("SMT transform '{context}' is missing required property 'type'")]
    MissingType { context: DiagnosticMessage },
    #[error("Invalid value: {context}")]
    InvalidValue { context: DiagnosticMessage },
    #[error("Validation errors: {context}")]
    ValidationError { context: DiagnosticMessage },
}

impl TransformBuildError {
    #[track_caller]
    pub fn missing_type(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::MissingType {
            context: DiagnosticMessage::new(name),
        }
    }

    #[track_caller]
    pub fn invalid_value(key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        Self::InvalidValue {
            context: DiagnosticMessage::new(format!("value is invalid for {} key {}", value, key)),
        }
    }
}

impl ValidationError for TransformBuildError {
    #[track_caller]
    fn validation_error(message: impl Into<String>) -> Self {
        Self::ValidationError {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}
