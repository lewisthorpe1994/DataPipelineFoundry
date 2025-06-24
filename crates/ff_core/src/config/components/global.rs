use crate::config::components::connections::ConnectionsConfig;
use crate::config::components::foundry_project::FoundryProjectConfig;
use crate::config::components::model::ModelsConfig;
use crate::config::components::source::SourceConfigs;

// ---------------- global config ----------------
#[derive(Debug)]
pub struct FoundryConfig {
    pub project: FoundryProjectConfig,
    pub source: SourceConfigs,
    pub connections: ConnectionsConfig,
    pub models: Option<ModelsConfig>,
}
impl FoundryConfig {
    pub fn new(
        project: FoundryProjectConfig,
        source: SourceConfigs,
        connections: ConnectionsConfig,
        models: Option<ModelsConfig>,
    ) -> Self {
        Self {
            project,
            source,
            connections,
            models,
        }
    }
}