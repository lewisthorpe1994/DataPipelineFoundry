use crate::config::components::model::ModelsPaths;
use crate::config::components::sources::SourcePaths;
use serde::Deserialize;
use std::collections::HashMap;
use database_adapters::AdapterConnectionDetails;
use crate::config::components::connections::ConnectionsConfig;

pub type ModelLayerName = String;
pub type ModelLayerDir = String;
pub type ModelLayers = HashMap<ModelLayerName, ModelLayerDir>;
// ---------------- Foundry Project Config ----------------
#[derive(Debug, Deserialize)]
pub struct FoundryProjectConfig {
    pub name: String,
    pub version: String,
    pub compile_path: String,
    pub modelling_architecture: String,
    pub connection_profile: String,
    pub paths: PathsConfig,
    pub warehouse_db_connection: String,
}

#[derive(Debug, Deserialize)]
pub struct PathsConfig {
    pub models: ModelsPaths,
    pub sources: SourcePaths,
    pub connections: String,
}