use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use serde::Deserialize;
use serde_json::Value;
use walkdir::WalkDir;
use common::traits::IsFileExtension;
use common::types::Materialize;
use common::types::schema::Column;
use crate::config::error::ConfigError;
use crate::config::loader::{load_config};
use crate::config::traits::{ConfigName, FromFileConfigList, IntoConfigVec};
use crate::config::components::foundry_project::ModelLayers;

// ---------------- Models Paths  ----------------
#[derive(Debug, Deserialize)]
pub struct ModelsPaths {
    pub dir: String,
    pub layers: Option<ModelLayers>,
}

// ---------------- Models File Config  ----------------
#[derive(Deserialize, Debug)]
pub struct ModelsFileConfig {
    pub models: Vec<ModelConfig>,
}

impl IntoConfigVec<ModelConfig> for ModelsFileConfig {
    fn vec(self) -> Vec<ModelConfig> {
        self.models
    }
}

// ---------------- Model Config  ----------------
#[derive(Deserialize, Debug)]
pub struct ModelConfig {
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<Column>,
    pub serve: Option<bool>,                // TODO - requires implementation
    pub pipelines: Option<Vec<String>>,     // TODO - requires its own type
    pub quality_tests: Option<Vec<String>>, // Todo - requires its own type
    pub meta: Option<Value>,
    pub materialization: Materialize,
}

impl ConfigName for ModelConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------- Models Config  --------------
#[derive(Deserialize, Debug)]
pub struct ModelsConfig(HashMap<String, ModelConfig>); // TODO - rename to ModelConfigs

impl ModelsConfig {
    fn empty() -> Self {
        Self(HashMap::new())
    }
}
impl Deref for ModelsConfig {
    type Target = HashMap<String, ModelConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ModelsConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromIterator<(String, ModelConfig)> for ModelsConfig {
    fn from_iter<I: IntoIterator<Item = (String, ModelConfig)>>(iter: I) -> Self {
        ModelsConfig(iter.into_iter().collect::<HashMap<_, _>>())
    }
}

impl From<ModelsFileConfig> for ModelsConfig {
    fn from(value: ModelsFileConfig) -> Self {
        let mapped = value
            .models
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect::<HashMap<_, _>>();
        Self(mapped)
    }
}

impl From<HashMap<String, ModelConfig>> for ModelsConfig {
    fn from(value: HashMap<String, ModelConfig>) -> Self {
        Self(value)
    }
}

impl TryFrom<&ModelLayers> for ModelsConfig {
    type Error = ConfigError;
    fn try_from(value: &ModelLayers) -> Result<Self, Self::Error> {
        let config = value.values().flat_map(|dir| {
            let walk_dir = WalkDir::new(dir);
            walk_dir
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.path().is_extension("yml"))
                .map(|entry| {
                    let res = load_config::<ModelConfig, ModelsFileConfig, ModelsConfig>(entry.path());
                    if let Err(ref e) = res {
                        eprintln!("⛔️ could not parse {:?}: {}", entry.path(), e);
                    }
                    res
                })
                .filter_map(|c| c.ok())
                .flat_map(|c| c.0.into_iter())
        }).collect::<ModelsConfig>();
        dbg!(&config);   // see how many models you got back
        Ok(config)
    }
}
