mod executor;
pub mod registry;

use common::types::sources::SourceConnArgs;
pub use registry::model::*;
pub use registry::error::CatalogError;
use crate::executor::{Executor, ExecutorError, ExecutorResponse};
use crate::registry::MemoryCatalog;

pub enum EngineError {
    FailedToExecute(String),
}
impl From<ExecutorError> for EngineError {
    fn from(e: ExecutorError) -> Self {
        Self::FailedToExecute(e.to_string())
    }   
}

enum EngineResponse {
    Ok,
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
    
    pub async fn execute(&self, sql: &str, source_conn_args: SourceConnArgs) -> Result<ExecutorResponse, EngineError> {
        Ok(self.executor.execute(sql, &self.catalog, source_conn_args).await?)
    }
}