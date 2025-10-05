pub mod postgres;
pub mod kafka;

use crate::postgres::PostgresAdapter;
use async_trait::async_trait;
use std::fmt::{Debug, Display};
use common::config::components::connections::{AdapterConnectionDetails, DatabaseAdapterType};

pub enum DatabaseAdapterError {
    InvalidConnectionError(String),
    SyntaxError(String),
    UnexpectedError(String),
    IoError(std::io::Error),
    ConfigError(String),
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
            DatabaseAdapterError::ConfigError(err) => {
                write!(f, "Configuration error: {}", err)
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
            DatabaseAdapterError::ConfigError(err) => {
                write!(f, "Configuration error: {}", err)
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
}

impl<T: DatabaseAdapter + ?Sized> DatabaseAdapter for &mut T {
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        (**self).execute(sql)
    }

    fn connection(&self) -> String {
        (**self).connection()
    }
}

#[async_trait]
pub trait AsyncDatabaseAdapter {
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError>;
}

#[async_trait] // ← leave the default (Send) mode
impl<T> AsyncDatabaseAdapter for &mut T
where
    T: DatabaseAdapter + Send + ?Sized, //  ↑ ensure the captured &mut T _is_
{
    //    Send (it is, if T: Send)
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        // if this call can block, wrap it in spawn_blocking!
        (**self).execute(sql)
    }
}
pub type AsyncDbAdapter = Box<dyn AsyncDatabaseAdapter>;

pub async fn create_db_adapter(
    conn_details: AdapterConnectionDetails,
) -> Result<AsyncDbAdapter, DatabaseAdapterError> {
    match conn_details.adapter_type {
        DatabaseAdapterType::Postgres => Ok(Box::new(
            PostgresAdapter::new(
                conn_details.host.as_str(),
                conn_details
                    .port
                    .parse::<u16>()
                    .expect("failed to parse port"),
                conn_details.database.as_str(),
                conn_details.user.as_str(),
                conn_details.password.as_str(),
            )
            .await?,
        )),
    }
}
