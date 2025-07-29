use std::io::ErrorKind;
use async_trait::async_trait;
use tokio_postgres::{Client, Error, NoTls, Row};
use tokio_postgres::error::SqlState;
use crate::{AsyncDatabaseAdapter, DatabaseAdapterError};
/* ----- error conversion stays almost the same ----- */

impl From<Error> for DatabaseAdapterError {
    fn from(err: Error) -> Self {
        if let Some(e) = err.as_db_error() {
            match e.code() {
                &SqlState::CONNECTION_DOES_NOT_EXIST =>
                    DatabaseAdapterError::InvalidConnectionError(e.to_string()),
                &SqlState::SYNTAX_ERROR =>
                    DatabaseAdapterError::SyntaxError(e.to_string()),
                &SqlState::IO_ERROR =>
                    DatabaseAdapterError::IoError(std::io::Error::new(ErrorKind::Other, e.to_string())),
                _ =>
                    DatabaseAdapterError::UnexpectedError(e.to_string()),
            }
        } else {
            DatabaseAdapterError::UnexpectedError(err.to_string())
        }
    }
}

/* ----- async trait definition ----- */

/* ----- async Postgres adapter ----- */

pub struct PostgresAdapter {
    client: Client,
    _driver: tokio::task::JoinHandle<()>,   // keep the task alive
}

impl PostgresAdapter {
    /// Create and connect (spawning the connection driver in the background)
    pub async fn new(
        host: &str,
        port: u16,          // ← back to u16
        db: &str,
        user: &str,
        password: &str,
    ) -> Result<Self, DatabaseAdapterError> {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            host, port, user, password, db
        );
        println!("{}", conn_str);
        // standard tokio-postgres split
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        eprintln!("Created connection!");
        // Drive the I/O forever; panic if it dies so the real cause is visible
        let driver = tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("postgres driver task exited: {e}");
            }
        });

        Ok(Self { client, _driver: driver })
    }
}

#[async_trait]
impl AsyncDatabaseAdapter for PostgresAdapter {
    async fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        self.client.batch_execute(sql).await?;   // waits until server confirms
        Ok(())
    }
}