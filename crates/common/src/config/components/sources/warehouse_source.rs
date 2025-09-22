use crate::config::components::sources::SourcePaths;
use crate::config::loader::load_config;
use crate::config::traits::{ConfigName, IntoConfigVec};
use crate::types::schema::Database;
use crate::types::sources::SourceType;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};

// ---------------- Source Config ----------------
#[derive(Deserialize, Serialize, Debug)]
pub struct WarehouseSourceConfig {
    pub name: String,
    pub database: Database,
}
impl ConfigName for WarehouseSourceConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

//  ---------------- WarehouseSource Configs ----------------
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct WarehouseSourceConfigs(HashMap<String, WarehouseSourceConfig>);

impl Deref for WarehouseSourceConfigs {
    type Target = HashMap<String, WarehouseSourceConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WarehouseSourceConfigs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl IntoIterator for WarehouseSourceConfigs {
    type Item = (String, WarehouseSourceConfig);
    type IntoIter = std::collections::hash_map::IntoIter<String, WarehouseSourceConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WarehouseSourceFileConfigs {
    warehouse_sources: Vec<WarehouseSourceConfig>,
}

impl IntoConfigVec<WarehouseSourceConfig> for WarehouseSourceFileConfigs {
    fn vec(self) -> Vec<WarehouseSourceConfig> {
        self.warehouse_sources
    }
}

impl From<HashMap<String, WarehouseSourceConfig>> for WarehouseSourceConfigs {
    fn from(value: HashMap<String, WarehouseSourceConfig>) -> Self {
        Self::new(value)
    }
}

impl From<Vec<SourcePaths>> for WarehouseSourceConfigs {
    fn from(value: Vec<SourcePaths>) -> Self {
        let mut configs: WarehouseSourceConfigs = WarehouseSourceConfigs::empty();
        value
            .into_iter()
            .filter(|t| t.kind == SourceType::Warehouse)
            .for_each(|p| {
                let c: WarehouseSourceConfigs = load_config::<
                    WarehouseSourceConfig,
                    WarehouseSourceFileConfigs,
                    WarehouseSourceConfigs,
                >((&p.path).as_ref())
                .expect("s");
                configs.extend(c);
            });

        configs
    }
}

impl WarehouseSourceConfigs {
    pub fn new(configs: HashMap<String, WarehouseSourceConfig>) -> Self {
        Self(configs)
    }

    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, name: &str) -> Option<&WarehouseSourceConfig> {
        self.0.get(name)
    }

    pub fn resolve(&self, name: &str, table: &str) -> Result<String, WarehouseSourceConfigError> {
        let config = self
            .get(name)
            .ok_or_else(|| WarehouseSourceConfigError::SourceNotFound(name.to_string()))?;

        let resolved = config
            .database
            .schemas
            .iter()
            .flat_map(|schema| schema.tables.iter().map(move |t| (schema, t.name.clone())))
            .find(|(_, t)| t == table);

        match resolved {
            Some((schema, table)) => Ok(format!("{}.{}", schema.name, table)),
            None => Err(WarehouseSourceConfigError::TableNotFound(table.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum WarehouseSourceConfigError {
    SourceNotFound(String),
    TableNotFound(String),
}
impl Display for WarehouseSourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceNotFound(name) => write!(f, "WarehouseSource not found: {}", name),
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
        }
    }
}
impl From<WarehouseSourceConfigError> for JinjaError {
    fn from(err: WarehouseSourceConfigError) -> JinjaError {
        match err {
            WarehouseSourceConfigError::SourceNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
            WarehouseSourceConfigError::TableNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
        }
    }
}
