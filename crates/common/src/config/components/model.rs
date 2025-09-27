use crate::config::components::foundry_project::ModelLayers;
use crate::config::error::ConfigError;
use crate::config::loader::load_config;
use crate::config::traits::{ConfigName, IntoConfigVec};
use crate::traits::IsFileExtension;
use crate::types::schema::Column;
use crate::types::Materialize;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use walkdir::WalkDir;

// ---------------- Models Paths  ----------------
#[derive(Debug, Deserialize)]
pub struct ModelsPaths {
    pub dir: String,
    pub layers: Option<ModelLayers>,
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

// ---------------- Model Config  ----------------
#[derive(Deserialize, Debug, Clone)]
pub struct ModelConfig {
    pub name: String,
    pub description: Option<String>,
    pub columns: Option<Vec<Column>>,
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
#[derive(Deserialize, Debug, Clone)]
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
        let mapped = HashMap::from([(value.config.name.clone(), value.config)]);
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
        let config = value
            .values()
            .flat_map(|dir| {
                let walk_dir = WalkDir::new(dir);

                walk_dir
                    .into_iter()
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| entry.path().is_extension("yml"))
                    .map(|entry| {
                        println!("Loading model config from {:?}", entry.path());
                        let res = load_config::<ModelConfig, ModelsFileConfig, ModelsConfig>(
                            entry.path(),
                        );
                        if let Err(ref e) = res {
                            eprintln!("⛔️ could not parse {:?}: {}", entry.path(), e);
                        }
                        res
                    })
                    .filter_map(|c| c.ok())
                    .flat_map(|c| c.0.into_iter())
            })
            .collect::<ModelsConfig>();
        Ok(config)
    }
}

#[test]
fn models_config_handles_example_project() {
    use test_utils::{build_test_layers, get_root_dir};

    // Resolve the example directory at runtime
    let root = get_root_dir();

    // Build the layer map exactly as the loader does
    let layers = build_test_layers(root);

    // Exercise the existing TryFrom
    let cfg = ModelsConfig::try_from(&layers).expect("parse model configs");

    println!("{:#?}", cfg);
    assert!(cfg.contains_key("bronze_orders"));
    assert!(cfg.contains_key("silver_orders"));
    assert!(cfg.contains_key("gold_customer_metrics"));
}
