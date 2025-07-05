use registry::{Catalog, MemoryCatalog};
use sqlparser::ast::{CreateSimpleMessageTransformPipeline, Statement};
use crate::executor::ExecutorError;

pub struct SqlExecutor;

impl SqlExecutor {
    pub fn new() -> Self {
        Self
    }
    
    pub fn execute_create_simple_message_transform_if_not_exists(
        smt_pipe: CreateSimpleMessageTransformPipeline,
        registry: MemoryCatalog
    ) -> Result<(), ExecutorError> {
        let name = smt_pipe.name;
        match registry.put_transform(&name.value) {
            Ok(_) => Ok(()),
        }
            
    }
}