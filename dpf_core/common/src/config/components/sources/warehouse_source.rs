use crate::config::traits::ConfigName;
use crate::types::schema::Database;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

// ---------------- Source Config ----------------
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DbConfig {
    pub name: String,
    pub database: Database,
}
impl ConfigName for DbConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub enum DbConfigError {
    SourceNotFound(String),
    TableNotFound(String),
}
impl Display for DbConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceNotFound(name) => write!(f, "WarehouseSource not found: {}", name),
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
        }
    }
}
impl From<DbConfigError> for JinjaError {
    fn from(err: DbConfigError) -> JinjaError {
        match err {
            DbConfigError::SourceNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
            DbConfigError::TableNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
        }
    }
}
