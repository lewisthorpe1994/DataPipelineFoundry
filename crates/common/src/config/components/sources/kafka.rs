use crate::config::components::sources::SourcePaths;
use crate::config::loader::load_config;
use crate::config::traits::{ConfigName, IntoConfigVec};
use crate::types::schema::Table;
use crate::types::sources::SourceType;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::{format, Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};

// ---------------- KafkaSource Config ----------------
#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConnect {
    pub host: String,
    pub port: String,
}
#[derive(Deserialize, Debug, Clone)]
pub struct KafkaBootstrap {
    pub servers: String,
}
#[derive(Deserialize, Debug, Clone)]
pub struct KafkaSourceConfig {
    pub name: String,
    pub bootstrap: KafkaBootstrap,
    pub connect: KafkaConnect,
}
impl ConfigName for KafkaSourceConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

//  ---------------- KafkaSource Configs ----------------
#[derive(Debug)]
pub struct KafkaSourceConfigs(pub HashMap<String, KafkaSourceConfig>);

impl Deref for KafkaSourceConfigs {
    type Target = HashMap<String, KafkaSourceConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KafkaSourceConfigs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl IntoIterator for KafkaSourceConfigs {
    type Item = (String, KafkaSourceConfig);
    type IntoIter = std::collections::hash_map::IntoIter<String, KafkaSourceConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Deserialize, Debug)]
pub struct KafkaSourceFileConfigs {
    kafka_sources: Vec<KafkaSourceConfig>,
}

impl IntoConfigVec<KafkaSourceConfig> for KafkaSourceFileConfigs {
    fn vec(self) -> Vec<KafkaSourceConfig> {
        self.kafka_sources
    }
}

impl From<HashMap<String, KafkaSourceConfig>> for KafkaSourceConfigs {
    fn from(value: HashMap<String, KafkaSourceConfig>) -> Self {
        Self::new(value)
    }
}

impl KafkaSourceConfigs {
    pub fn new(configs: HashMap<String, KafkaSourceConfig>) -> Self {
        Self(configs)
    }

    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, name: &str) -> Option<&KafkaSourceConfig> {
        self.0.get(name)
    }

    pub fn resolve(&self, name: &str) -> Result<&KafkaSourceConfig, KafkaSourceConfigError> {
        let config = self
            .get(name)
            .ok_or_else(|| KafkaSourceConfigError::SourceNotFound(name.to_string()))?;

        Ok(config)
    }
}

#[derive(Debug)]
pub enum KafkaSourceConfigError {
    SourceNotFound(String),
    NoSources,
}
impl Display for KafkaSourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceNotFound(name) => write!(f, "KafkaSource not found: {}", name),
            Self::NoSources => write!(f, "No sources found"),
        }
    }
}
impl From<KafkaSourceConfigError> for JinjaError {
    fn from(err: KafkaSourceConfigError) -> JinjaError {
        match err {
            KafkaSourceConfigError::SourceNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
            KafkaSourceConfigError::NoSources => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                "No sources found".to_string(),
            ),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConnectorConfig {
    pub tables: HashMap<String, Table>,
    pub name: String,
}

impl KafkaConnectorConfig {
    pub fn table_include_list(&self) -> String {
        self.tables
            .keys()
            .map(|k| k.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn column_include_list(&self) -> String {
        self.tables
            .iter()
            .flat_map(|(table_name, table_meta)| {
                table_meta
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", table_name.clone(), c.name.clone()))
            })
            .collect::<Vec<String>>()
            .join(",")
    }
}

impl ConfigName for KafkaConnectorConfig {
    fn name(&self) -> &str {
        &self.name
    }
}
