pub mod kafka;
pub mod postgres;

use crate::postgres::PostgresAdapter;
use async_trait::async_trait;
use common::config::components::connections::{AdapterConnectionDetails, DatabaseAdapterType};
use common::error::diagnostics::DiagnosticMessage;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseAdapterError {
    #[error("invalid connection details: {context}")]
    InvalidConnectionError { context: DiagnosticMessage },
    #[error("SQL syntax error: {context}")]
    SyntaxError { context: DiagnosticMessage },
    #[error("unexpected database error: {context}")]
    UnexpectedError { context: DiagnosticMessage },
    #[error("I/O error: {context}")]
    IoError {
        context: DiagnosticMessage,
        #[source]
        source: std::io::Error,
    },
    #[error("configuration error: {context}")]
    ConfigError { context: DiagnosticMessage },
}

impl DatabaseAdapterError {
    #[track_caller]
    pub fn invalid_connection(message: impl Into<String>) -> Self {
        Self::InvalidConnectionError {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn syntax(message: impl Into<String>) -> Self {
        Self::SyntaxError {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::UnexpectedError {
            context: DiagnosticMessage::new(message.into()),
        }
    }

    #[track_caller]
    pub fn config(message: impl Into<String>) -> Self {
        Self::ConfigError {
            context: DiagnosticMessage::new(message.into()),
        }
    }
}

impl From<std::io::Error> for DatabaseAdapterError {
    #[track_caller]
    fn from(err: std::io::Error) -> Self {
        let message = err.to_string();
        DatabaseAdapterError::IoError {
            context: DiagnosticMessage::new(message),
            source: err,
        }
    }
}

pub trait DatabaseAdapter {
    type Row: Send + 'static;
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError>;
    fn connection(&self) -> String;
    fn query(&self, sql: &str) -> Result<Vec<Self::Row>, DatabaseAdapterError>;
}

impl<T: DatabaseAdapter + ?Sized> DatabaseAdapter for &mut T {
    type Row = T::Row;

    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        (**self).execute(sql)
    }

    fn connection(&self) -> String {
        (**self).connection()
    }
    fn query(&self, sql: &str) -> Result<Vec<Self::Row>, DatabaseAdapterError> {
        (**self).query(sql)
    }
}

#[async_trait]
pub trait AsyncDatabaseAdapter: Send + Sync {
    type Row: Send + 'static;
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError>;
    async fn query(&self, sql: &str) -> Result<Vec<Self::Row>, DatabaseAdapterError>;
}

#[async_trait] // ← leave the default (Send) mode
impl<T> AsyncDatabaseAdapter for &mut T
where
    T: DatabaseAdapter + Send + ?Sized + Sync, //  ↑ ensure the captured &mut T _is_
{
    type Row = T::Row;

    //    Send (it is, if T: Send)
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        // if this call can block, wrap it in spawn_blocking!
        (**self).execute(sql)
    }

    async fn query(&self, sql: &str) -> Result<Vec<Self::Row>, DatabaseAdapterError> {
        (**self).query(sql)
    }
}

// TODO - needs revisiting when more dbs are supported
pub type AsyncDbAdapter =
    Box<dyn AsyncDatabaseAdapter<Row = tokio_postgres::Row> + Send + Sync + 'static>;

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
