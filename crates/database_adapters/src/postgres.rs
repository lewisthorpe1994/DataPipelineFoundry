use std::io::ErrorKind;
use postgres::{Client, Error, NoTls}
;
use postgres::error::SqlState;
use crate::{DatabaseAdapter, DatabaseAdapterError};

impl From<Error> for DatabaseAdapterError {
    fn from(err: Error) -> Self {
        if let Some(e) = err.as_db_error() {
            match e.code() {
                &SqlState::CONNECTION_DOES_NOT_EXIST => {
                    DatabaseAdapterError::InvalidConnectionError(e.to_string())
                }
                &SqlState::SYNTAX_ERROR => {
                    DatabaseAdapterError::SyntaxError(e.to_string())
                }
                &SqlState::IO_ERROR => {
                    DatabaseAdapterError::IoError(std::io::Error::new(ErrorKind::Other, e.to_string()))
                }
                _ => {
                    DatabaseAdapterError::UnexpectedError(e.to_string())
                }
            }
        } else {
            DatabaseAdapterError::UnexpectedError(err.to_string())
        }
    }
}

pub struct PostgresAdapter {
    host: String,
    port: u16,
    db: String,
    password: String,
    user: String,
}

impl DatabaseAdapter for PostgresAdapter {
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        let mut conn = Client::connect(&self.connection(), NoTls)?;
        let resp = conn.query(sql, &[])?;

        Ok(())
    }
    fn connection(&self) -> String {
        format!("host={} user={} password={} dbname={}",
        self.host, self.user, self.password, self.db)
    }

}