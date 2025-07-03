pub mod sql;
pub mod database;

use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use postgres::error::Error as PostgresError;
use sqlparser::ast::Statement;

#[derive(Debug)]
pub enum ExecutorError {
    FailedToConnect(String),
    FailedToExecute(String),
    InvalidFilePath(String),
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::FailedToConnect(msg) => write!(f, "Connection failed: {}", msg),
            ExecutorError::FailedToExecute(msg) => write!(f, "Execution failed: {}", msg),
            ExecutorError::InvalidFilePath(msg) => write!(f, "Invalid file path: {}", msg),
        }
    }
}

impl Error for ExecutorError {}

impl From<PostgresError> for ExecutorError {
    fn from(e: PostgresError) -> ExecutorError {
        ExecutorError::FailedToExecute(e.to_string())
    }
}

trait Executor {
    fn execute(&self, ast: Statement) -> Result<(), ExecutorError>; // TODO - create enum result type
}