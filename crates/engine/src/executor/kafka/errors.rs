use reqwest::Error as ReqwestError;
use serde_json::error::Error as JsonError;
use std::fmt::Display;

#[derive(Debug)]
pub enum KafkaExecutorError {
    ConnectionError(ReqwestError),
    IoError(std::io::Error),
    IncorrectConfig(String),
    UnexpectedError(String),
    InternalServerError(String),
}

impl Display for KafkaExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionError(e) => write!(f, "Connection error: {}", e),
            Self::IoError(e) => write!(f, "IO error: {}", e),
            Self::IncorrectConfig(e) => write!(f, "Incorrect configuration: {}", e),
            Self::UnexpectedError(e) => write!(f, "Unexpected error: {}", e),
            Self::InternalServerError(e) => write!(f, "Internal server error: {}", e),
        }
    }
}

impl From<JsonError> for KafkaExecutorError {
    fn from(e: JsonError) -> Self {
        Self::IncorrectConfig(e.to_string())
    }
}

impl From<ReqwestError> for KafkaExecutorError {
    fn from(e: ReqwestError) -> Self {
        Self::ConnectionError(e)
    }
}
