pub mod postgres;

use std::fmt::{Debug, Display};
use async_trait::async_trait;

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

impl Debug for DatabaseAdapterError {
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
    // fn execute_dag_models<'a, T>(
    //     &mut self,
    //     nodes: T,
    //     compile_path: &str,
    //     models_dir: &str,
    // ) -> Result<(), DatabaseAdapterError>
    // where
    //     T: IntoDagNodes<'a>,
    // {
    //     timeit!("Executed all models", {
    //         for node in nodes.into_vec() {
    //             timeit!(format!("Executed model {}", &node.path.display()), {
    //                 let sql = read_sql_file(models_dir, &node.path, compile_path)?;
    //                 self.execute(&sql)?
    //             });
    //         }
    //     });
    //
    //     Ok(())
    // }
}

impl<T: DatabaseAdapter + ?Sized> DatabaseAdapter for &mut T {
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        (**self).execute(sql)
    }

    fn connection(&self) -> String {
        (**self).connection()
    }

    // fn execute_dag_models<'a, N>(
    //     &mut self,
    //     nodes: N,
    //     compile_path: &str,
    //     models_dir: &str,
    // ) -> Result<(), DatabaseAdapterError>
    // where
    //     N: IntoDagNodes<'a>,
    // {
    //     (**self).execute_dag_models(nodes, compile_path, models_dir)
    // }
}

#[async_trait]
pub trait AsyncDatabaseAdapter {
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError>;
}

#[async_trait]                          // ← leave the default (Send) mode
impl<T> AsyncDatabaseAdapter for &mut T
where
    T: DatabaseAdapter + Send + ?Sized, //  ↑ ensure the captured &mut T _is_
{                                       //    Send (it is, if T: Send)
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        // if this call can block, wrap it in spawn_blocking!
        (**self).execute(sql)
    }
}