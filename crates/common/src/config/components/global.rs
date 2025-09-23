use crate::config::components::connections::ConnectionsConfig;
use crate::config::components::foundry_project::FoundryProjectConfig;
use crate::config::components::model::ModelsConfig;
use crate::config::components::sources::kafka::KafkaSourceConfigs;
use crate::config::components::sources::warehouse_source::WarehouseSourceConfigs;
use crate::config::components::sources::SourcePaths;

// ---------------- global config ----------------
#[derive(Debug)]
pub struct FoundryConfig {
    pub project: FoundryProjectConfig,
    pub warehouse_source: WarehouseSourceConfigs,
    pub kafka_source: Option<KafkaSourceConfigs>,
    pub connections: ConnectionsConfig,
    pub models: Option<ModelsConfig>,
    pub connection_profile: String,
    pub source_paths: SourcePaths,
}
impl FoundryConfig {
    pub fn new(
        project: FoundryProjectConfig,
        warehouse_source: WarehouseSourceConfigs,
        connections: ConnectionsConfig,
        models: Option<ModelsConfig>,
        connection_profile: String,
        kafka_source: Option<KafkaSourceConfigs>,
        source_paths: SourcePaths,
    ) -> Self {
        Self {
            project,
            warehouse_source,
            connections,
            models,
            connection_profile,
            kafka_source,
            source_paths,
        }
    }
}
