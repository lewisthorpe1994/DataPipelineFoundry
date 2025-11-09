pub mod diagnostics;
pub use crate::config::error::ConfigError;
pub use diagnostics::DiagnosticMessage;

use std::{error::Error as StdError, fmt::Debug};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FFError {
    #[error("initialisation failed: {context}")]
    Init {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>, // inner cause
    },
    #[error("compile failed: {context}")]
    Compile {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
    #[error("run failed: {context}")]
    Run {
        context: DiagnosticMessage,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
}

impl FFError {
    #[track_caller]
    pub fn init<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        let message = err.to_string();
        FFError::Init {
            context: DiagnosticMessage::new(message),
            source: Some(Box::new(err)),
        }
    }

    #[track_caller]
    pub fn init_msg(message: impl Into<String>) -> Self {
        FFError::Init {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }

    #[track_caller]
    pub fn compile<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        let message = err.to_string();
        FFError::Compile {
            context: DiagnosticMessage::new(message),
            source: Some(Box::new(err)),
        }
    }

    #[track_caller]
    pub fn compile_msg(message: impl Into<String>) -> Self {
        FFError::Compile {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }

    #[track_caller]
    pub fn run<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        let message = err.to_string();
        FFError::Run {
            context: DiagnosticMessage::new(message),
            source: Some(Box::new(err)),
        }
    }

    #[track_caller]
    pub fn run_msg(message: impl Into<String>) -> Self {
        FFError::Run {
            context: DiagnosticMessage::new(message.into()),
            source: None,
        }
    }
}
