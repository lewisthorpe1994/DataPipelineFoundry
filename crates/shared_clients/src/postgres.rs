use crate::{AsyncDatabaseAdapter, DatabaseAdapterError};
use async_trait::async_trait;
use std::io::ErrorKind;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Error, NoTls, Row};
/* ----- error conversion stays almost the same ----- */

impl From<Error> for DatabaseAdapterError {
    fn from(err: Error) -> Self {
        if let Some(e) = err.as_db_error() {
            match e.code() {
                &SqlState::CONNECTION_DOES_NOT_EXIST => {
                    DatabaseAdapterError::InvalidConnectionError(e.to_string())
                }
                &SqlState::SYNTAX_ERROR => DatabaseAdapterError::SyntaxError(e.to_string()),
                &SqlState::IO_ERROR => DatabaseAdapterError::IoError(std::io::Error::new(
                    ErrorKind::Other,
                    e.to_string(),
                )),
                _ => DatabaseAdapterError::UnexpectedError(e.to_string()),
            }
        } else {
            DatabaseAdapterError::UnexpectedError(err.to_string())
        }
    }
}

/* ----- async trait definition ----- */

/* ----- async Postgres adapter ----- */

pub struct PostgresAdapter {
    pub client: Client,
    _driver: tokio::task::JoinHandle<()>, // keep the task alive
}

impl PostgresAdapter {
    /// Create and connect (spawning the connection driver in the background)
    pub async fn new(
        host: &str,
        port: u16, // ← back to u16
        db: &str,
        user: &str,
        password: &str,
    ) -> Result<Self, DatabaseAdapterError> {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            host, port, user, password, db
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        let driver = tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("postgres driver task exited: {e}");
            }
        });

        Ok(Self {
            client,
            _driver: driver,
        })
    }
}

#[async_trait]
impl AsyncDatabaseAdapter for PostgresAdapter {
    type Row = tokio_postgres::Row;

    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        self.client.batch_execute(sql).await?; // waits until server confirms
        Ok(())
    }
    async fn query(&self, sql: &str) -> Result<Vec<Self::Row>, DatabaseAdapterError> {
        let response = self.client.query(sql, &[]).await?;
        Ok(response)
    }
}
