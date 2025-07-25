pub mod postgres;

use std::fmt::Display;
use logging::timeit;
use tracing::info;
use common::utils::read_sql_file;
use dag::IntoDagNodes;

pub enum DatabaseAdapterError {
    InvalidConnectionError(String),
    SyntaxError(String),
    UnexpectedError(String),
    IoError(std::io::Error),
}

impl Display for DatabaseAdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseAdapterError::InvalidConnectionError(err) => {
                write!(f, "Invalid connection details: {}", err)
            }
            DatabaseAdapterError::SyntaxError(err) => {
                write!(f, "Syntax error: {}", err)
            }
            DatabaseAdapterError::UnexpectedError(err) => {
                write!(f, "Unexpected error: {}", err)
            }
            DatabaseAdapterError::IoError(err) => {
                write!(f, "I/O error: {}", err)
            }
        }
    }
}

impl From<std::io::Error> for DatabaseAdapterError {
    fn from(err: std::io::Error) -> Self {
        DatabaseAdapterError::IoError(err)
    }
}

pub trait DatabaseAdapter {
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError>;
    fn connection(&self) -> String;
    fn execute_dag_models<'a, T>(
        &mut self,
        nodes: T,
        compile_path: &str,
        models_dir: &str,
    ) -> Result<(), DatabaseAdapterError>
    where
        T: IntoDagNodes<'a>,
    {
        timeit!("Executed all models", {
            for node in nodes.into_vec() {
                timeit!(format!("Executed model {}", &node.path.display()), {
                    let sql = read_sql_file(models_dir, &node.path, compile_path)?;
                    self.execute(&sql)?
                });
            }
        });

        Ok(())
    }
}
