mod executor;
mod test_utils;
pub mod registry;


pub use registry::model::*;
pub use registry::error::CatalogError;
use crate::executor::{Executor, ExecutorResponse};
use crate::registry::MemoryCatalog;

enum EngineError {
    FailedToExecute(String),
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
    
    pub async fn execute(&self, sql: &str) -> Result<ExecutorResponse, EngineError> {
        Ok(self.execute(sql, self.catalog)?)
    }
}