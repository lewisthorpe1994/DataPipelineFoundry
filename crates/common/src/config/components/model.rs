use crate::config::error::ConfigError;
use crate::config::loader::load_config;
use crate::config::traits::{ConfigName, IntoConfigVec};
use crate::traits::IsFileExtension;
use crate::types::schema::Column;
use crate::types::Materialize;
use crate::utils::paths_with_ext;
use log::error;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use walkdir::WalkDir;

pub type ModelLayerName = String;
pub type ModelLayerDir = String;
pub type ModelLayers = HashMap<ModelLayerName, ModelLayerDir>;

#[derive(Debug)]
pub struct ResolvedModelLayerConfig {
    pub name: String,
    pub path: PathBuf,
    pub target: String,
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsProject {
    pub target_connection: String,
    pub layers: ModelLayers,
}

// ---------------- Models Paths  ----------------
#[derive(Debug, Deserialize)]
pub struct ModelsProjects {
    pub dir: String,
    pub analytics_projects: Option<HashMap<String, AnalyticsProject>>,
}

// ---------------- Models File Config  ----------------
#[derive(Deserialize, Debug)]
pub struct ModelsFileConfig {
    pub config: ModelConfig,
}

impl IntoConfigVec<ModelConfig> for ModelsFileConfig {
    fn vec(self) -> Vec<ModelConfig> {
        vec![self.config]
    }
}

fn default_materialization() -> Materialize {
    Materialize::View
}

// ---------------- Model Config  ----------------
#[derive(Deserialize, Debug, Clone)]
pub struct ModelConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<Column>>,
    #[serde(default)]
    pub serve: bool, // TODO - requires implementation
    #[serde(default)]
    pub pipelines: Option<Vec<String>>, // TODO - requires its own type
    // pub quality_tests: Option<Vec<String>>, // Todo - requires its own type
    #[serde(default)]
    pub meta: Option<Value>,
    #[serde(default = "default_materialization")]
    pub materialization: Materialize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: None,
            columns: None,
            serve: false,
            pipelines: None,
            meta: None,
            materialization: Materialize::View,
        }
    }
}

impl ModelConfig {
    pub fn with_name<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
}

impl ConfigName for ModelConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ResolvedModelConfig {
    pub config: ModelConfig,
    pub target: String,
    pub path: PathBuf,
}

impl ConfigName for ResolvedModelConfig {
    fn name(&self) -> &str {
        &self.config.name
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ResolvedModelsConfig(HashMap<String, ResolvedModelConfig>); // TODO - rename to ModelConfigs

impl ResolvedModelsConfig {
    fn empty() -> Self {
        Self(HashMap::new())
    }
}
impl Deref for ResolvedModelsConfig {
    type Target = HashMap<String, ResolvedModelConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&HashMap<String, ResolvedModelLayerConfig>> for ResolvedModelsConfig {
    type Error = ConfigError;

    fn try_from(value: &HashMap<String, ResolvedModelLayerConfig>) -> Result<Self, Self::Error> {
        let mut resolved = ResolvedModelsConfig::empty();

        for (k, v) in value.iter() {
            for p in paths_with_ext(&v.path, "yml").into_iter() {
                let mut config = load_config::<ModelConfig>(&p)?;
                for cfg in config.values() {
                    resolved.0.insert(
                        cfg.name.clone(),
                        ResolvedModelConfig {
                            config: cfg.clone(),
                            target: v.target.clone(),
                            path: v.path.clone()
                        },
                    );
                }
            }
        }
        Ok(resolved)
    }
}
