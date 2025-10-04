pub mod executor;
pub mod types;

use common::types::sources::SourceConnArgs;
use database_adapters::{AsyncDatabaseAdapter, DatabaseAdapter};
use executor::{Executor, ExecutorError, ExecutorResponse};
use std::fmt::{Debug, Display, Formatter};

pub enum EngineError {
    FailedToExecute(String),
}
impl Display for EngineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::FailedToExecute(err) => {
                write!(f, "Failed to execute: {}", err)
            }
        }
    }
}

impl std::error::Error for EngineError {}
impl Debug for EngineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::FailedToExecute(err) => {
                write!(f, "FailedToExecute: {}", err)
            }
        }
    }
}
impl From<ExecutorError> for EngineError {
    fn from(e: ExecutorError) -> Self {
        Self::FailedToExecute(e.to_string())
    }
}

pub enum EngineResponse {
    Ok,
}
impl From<ExecutorResponse> for EngineResponse {
    fn from(resp: ExecutorResponse) -> Self {
        match resp {
            ExecutorResponse::Ok => EngineResponse::Ok,
        }
    }
}
pub struct Engine {
    executor: Executor,
    pub catalog: MemoryCatalog,
}
impl Engine {
    pub fn new() -> Self {
        Self {
            executor: Executor::new(),
            catalog: MemoryCatalog::new(),
        }
    }

    pub async fn execute(
        &self,
        sql: &str,
        source_conn_args: &SourceConnArgs,
        target_db_adapter: Option<&mut Box<dyn AsyncDatabaseAdapter>>,
    ) -> Result<EngineResponse, EngineError> {
        Ok(EngineResponse::from(
            self.executor
                .execute(sql, &self.catalog, source_conn_args, target_db_adapter)
                .await?,
        ))
    }
}
