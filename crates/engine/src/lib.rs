pub mod registry;
pub mod executor;

use std::fmt::{Debug, Formatter};
use common::types::sources::SourceConnArgs;
pub use registry::model::*;
pub use registry::error::CatalogError;
use executor::{Executor, ExecutorError, ExecutorResponse};
use registry::MemoryCatalog;

pub enum EngineError {
    FailedToExecute(String),
}
impl Debug for EngineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self { 
            EngineError::FailedToExecute(err) => {write!(f,"FailedToExecute: {}", err)},
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
    catalog: MemoryCatalog,
}
impl Engine {
    pub fn new() -> Self {
        Self {
            executor: Executor::new(),
            catalog: MemoryCatalog::new(),
        }
    }
    
    pub async fn execute(&self, sql: &str, source_conn_args: SourceConnArgs) -> Result<EngineResponse, EngineError> {
        Ok(EngineResponse::from(self.executor.execute(sql, &self.catalog, source_conn_args).await?))
    }
}