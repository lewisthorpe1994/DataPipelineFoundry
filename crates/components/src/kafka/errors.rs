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
    #[error("Validation errors: {context}")]
    ValidationError { context: DiagnosticMessage },

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

    #[track_caller]
    pub fn validation_error(message: impl Into<String>) -> Self {
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

#[derive(Default)]
pub struct ErrorBag {
    msgs: Vec<String>,
}

impl ErrorBag {
    fn push<S: Into<String>>(&mut self, msg: S) {
        self.msgs.push(msg.into());
    }

    pub fn check_mutually_exclusive<A, B>(
        &mut self,
        name_a: &str,
        a: &Option<A>,
        name_b: &str,
        b: &Option<B>,
    ) {
        if a.is_some() && b.is_some() {
            self.push(format!("{} and {} cannot be set at the same time.", name_a, name_b));
        }
    }

    pub fn check_requires<A, B>(
        &mut self,
        parent_name: &str,
        parent: &Option<A>,
        dep_name: &str,
        dep: &Option<B>,
    ) {
        if parent.is_some() && dep.is_none() {
            self.push(format!("{} requires {} to be set.", parent_name, dep_name));
        }
    }

    pub fn check_one_of(&mut self, group_name: &str, flags: &[(&str, bool)]) {
        let count = flags.iter().filter(|(_, on)| *on).count();
        if count != 1 {
            let names = flags.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ");
            self.push(format!("Exactly one of [{}] must be set for {}.", names, group_name));
        }
    }

    pub fn check_allowed<'a>(
        &mut self,
        name: &str,
        value: Option<&'a str>,
        allowed: &[&'a str],
    ) {
        if let Some(v) = value {
            if !allowed.iter().any(|a| *a == v) {
                self.push(format!(
                    "{} has invalid value '{}'. Allowed: {}",
                    name,
                    v,
                    allowed.join(", ")
                ));
            }
        }
    }
    
    pub fn version_errors(&mut self, errors: Vec<String>) {
        self.msgs.extend(errors);
    }

    pub fn finish(self) -> Result<(), KafkaConnectorCompileError> {
        if self.msgs.is_empty() {
            Ok(())
        } else {
            // Format as a single error – tweak to match your error type API.
            Err(KafkaConnectorCompileError::validation_error(
                self.msgs.join("\n"),
            ))
        }
    }
}
