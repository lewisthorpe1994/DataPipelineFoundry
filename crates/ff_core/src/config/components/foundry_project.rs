use std::collections::HashMap;
use serde::Deserialize;
use crate::config::components::model::ModelsPaths;
use crate::config::components::source::SourcesPath;

pub type ModelLayerName = String;
pub type ModelLayerDir = String;
pub type ModelLayers = HashMap<ModelLayerName, ModelLayerDir>;
// ---------------- Foundry Project Config ----------------
#[derive(Debug, Deserialize)]
pub struct FoundryProjectConfig {
    pub project_name: String,
    pub version: String,
    pub compile_path: String,
    pub modelling_architecture: String,
    pub connection_profile: String,
    pub paths: PathsConfig,
}

#[derive(Debug, Deserialize)]
pub struct PathsConfig {
    pub models: ModelsPaths,
    pub sources: Vec<SourcesPath>,
    pub connections: String,
}
