use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};
use serde::Deserialize;
use common::types::schema::Database;
use crate::config::loader::{load_config};
use crate::config::traits::{ConfigName, IntoConfigVec};
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};

// ---------------- Sources Path ----------------
#[derive(Debug, Deserialize, Clone)]
pub struct SourcesPath {
    pub name: String,
    pub path: String,
}

// ---------------- Source Config ----------------
#[derive(Deserialize, Debug)]
pub struct SourceConfig {
    pub name: String,
    pub database: Database,
}
impl ConfigName for SourceConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

//  ---------------- Source Configs ----------------
#[derive(Debug)]
pub struct SourceConfigs(HashMap<String, SourceConfig>);

impl Deref for SourceConfigs {
    type Target = HashMap<String, SourceConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SourceConfigs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl IntoIterator for SourceConfigs {
    type Item = (String, SourceConfig);
    type IntoIter = std::collections::hash_map::IntoIter<String, SourceConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Deserialize, Debug)]
pub struct SourceFileConfigs {
    sources: Vec<SourceConfig>,
}

impl IntoConfigVec<SourceConfig> for SourceFileConfigs {
    fn vec(self) -> Vec<SourceConfig> {
        self.sources
    }
}

impl From<HashMap<String, SourceConfig>> for SourceConfigs {
    fn from(value: HashMap<String, SourceConfig>) -> Self {
        Self::new(value)
    }
}

impl From<Vec<SourcesPath>> for SourceConfigs {
    fn from(value: Vec<SourcesPath>) -> Self {
        let mut configs: SourceConfigs = SourceConfigs::empty();
        value.into_iter().for_each(|p| {
            let c: SourceConfigs =
                load_config::<SourceConfig, SourceFileConfigs, SourceConfigs>((&p.path).as_ref())
                    .expect("s");
            configs.extend(c);
        });

        configs
    }
}

impl SourceConfigs {
    pub fn new(configs: HashMap<String, SourceConfig>) -> Self {
        Self(configs)
    }

    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, name: &str) -> Option<&SourceConfig> {
        self.0.get(name)
    }

    pub fn resolve(&self, name: &str, table: &str) -> Result<String, SourceConfigError> {
        let config = self
            .get(name)
            .ok_or_else(|| SourceConfigError::SourceNotFound(name.to_string()))?;

        let resolved = config
            .database
            .schemas
            .iter()
            .flat_map(|schema| schema.tables.iter().map(move |t| (schema, t.name.clone())))
            .find(|(_, t)| t == table);

        match resolved {
            Some((schema, table)) => Ok(format!("{}.{}.{}", config.name, schema.name, table)),
            None => Err(SourceConfigError::TableNotFound(table.to_string())),
        }
    }
}

pub enum SourceConfigError {
    SourceNotFound(String),
    TableNotFound(String),
}
impl Display for SourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceNotFound(name) => write!(f, "Source not found: {}", name),
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
        }
    }
}
impl Debug for SourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl From<SourceConfigError> for JinjaError {
    fn from(err: SourceConfigError) -> JinjaError {
        match err {
            SourceConfigError::SourceNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
            SourceConfigError::TableNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
        }
    }
}