pub mod sql;
pub mod kafka;

use std::error::Error;
use std::fmt;
use common::types::sources::SourceConnArgs;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use crate::CatalogError;
use crate::executor::kafka::{KafkaExecutorError, KafkaExecutorResponse};
use crate::executor::sql::SqlExecutor;
use crate::registry::MemoryCatalog;
use database_adapters::{DatabaseAdapter, DatabaseAdapterError};

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
    Ok
}

impl From<KafkaExecutorResponse> for ExecutorResponse {
    fn from(resp: KafkaExecutorResponse) -> Self {
        match resp {
            KafkaExecutorResponse::Ok => ExecutorResponse::Ok,
        }
    }
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


impl From<CatalogError> for ExecutorError { 
    fn from(e: CatalogError) -> ExecutorError {
        ExecutorError::FailedToExecute(e.to_string())
    }
}
impl From<KafkaExecutorError> for ExecutorError {
    fn from(e: KafkaExecutorError) -> ExecutorError {
        match e {
            KafkaExecutorError::ConnectionError(e) => ExecutorError::FailedToConnect(e.to_string()),
            KafkaExecutorError::IncorrectConfig(e) => ExecutorError::FailedToExecute(e.to_string()),
            KafkaExecutorError::IoError(e) => ExecutorError::IoError(e),
            KafkaExecutorError::UnexpectedError(e) => ExecutorError::UnexpectedError(e.to_string()),
            KafkaExecutorError::InternalServerError(e) => ExecutorError::UnexpectedError(e.to_string()),
        }
    }
}
impl From<ParserError> for ExecutorError {
    fn from(value: ParserError) -> Self {
        ExecutorError::ParseError(value.to_string())
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
        sql: &str, 
        catalog: &MemoryCatalog, 
        source_conn_args: SourceConnArgs,
        target_db_adapter: Option<Box<dyn DatabaseAdapter>> 
    ) -> Result<ExecutorResponse, ExecutorError> {
        let ast_vec = Parser::parse_sql(&GenericDialect, sql)?;
        
        Ok(SqlExecutor::execute(ast_vec, catalog, source_conn_args, target_db_adapter).await?)
    }
}