use crate::config::components::connections::Connections;
use crate::config::components::model::ModelsProjects;
use crate::config::components::sources::SourcePaths;
use serde::Deserialize;
use crate::config::components::python::PythonConfig;

// ---------------- Foundry Project Config ----------------
#[derive(Debug, Deserialize)]
pub struct FoundryProjectConfig {
    pub name: String,
    pub version: String,
    pub compile_path: String,
    #[serde(default)]
    pub modelling_architecture: String,
    pub connection_profile: Connections,
    pub models: ModelsProjects,
    pub sources: SourcePaths,
    pub python: Option<PythonConfig>
}
