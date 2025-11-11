use crate::config::traits::ConfigName;
use crate::types::schema::Database;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};

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

//  ---------------- WarehouseSource Configs ----------------
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DbConfigs(HashMap<String, DbConfig>);

impl Deref for DbConfigs {
    type Target = HashMap<String, DbConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DbConfigs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl IntoIterator for DbConfigs {
    type Item = (String, DbConfig);
    type IntoIter = std::collections::hash_map::IntoIter<String, DbConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WarehouseSourceFileConfigs {
    warehouse_sources: Vec<DbConfig>,
}

impl From<HashMap<String, DbConfig>> for DbConfigs {
    fn from(value: HashMap<String, DbConfig>) -> Self {
        Self::new(value)
    }
}

impl DbConfigs {
    pub fn new(configs: HashMap<String, DbConfig>) -> Self {
        Self(configs)
    }

    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, name: &str) -> Option<&DbConfig> {
        self.0.get(name)
    }

    pub fn resolve(&self, name: &str, table: &str) -> Result<String, DbConfigError> {
        let config = self
            .get(name)
            .ok_or_else(|| DbConfigError::SourceNotFound(name.to_string()))?;

        let resolved = config
            .database
            .schemas
            .iter()
            .flat_map(|(name, obj)| {
                obj.tables
                    .keys()
                    .map(move |t_name| (config.database.name.clone(), name, t_name.clone()))
            })
            .find(|(_, _, t)| t == table);

        match resolved {
            Some((database, schema, table)) => Ok(format!("{}.{}.{}", database, schema, table)),
            None => Err(DbConfigError::TableNotFound(table.to_string())),
        }
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
