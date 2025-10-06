use crate::config::components::connections::{AdapterConnectionDetails, ConnectionsConfig};
use crate::config::components::foundry_project::FoundryProjectConfig;
use crate::config::components::model::ModelsConfig;
use crate::config::components::sources::kafka::{KafkaSourceConfig, KafkaSourceConfigs};
use crate::config::components::sources::warehouse_source::WarehouseSourceConfigs;
use crate::config::components::sources::SourcePaths;

// ---------------- global config ----------------
#[derive(Debug)]
pub struct FoundryConfig {
    pub project: FoundryProjectConfig,
    pub warehouse_source: WarehouseSourceConfigs,
    pub kafka_source: Option<KafkaSourceConfigs>,
    pub connections: ConnectionsConfig,
    pub warehouse_db_connection: String,
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
        warehouse_db_connection: String,
        kafka_source: Option<KafkaSourceConfigs>,
        source_paths: SourcePaths,
    ) -> Self {
        Self {
            project,
            warehouse_source,
            connections,
            models,
            connection_profile,
            warehouse_db_connection,
            kafka_source,
            source_paths,
        }
    }
}

impl FoundryConfig {
    pub fn get_adapter_connection_details(
        &self,
    ) -> Option<AdapterConnectionDetails> {
        self.connections
            .get(&self.connection_profile)
            .and_then(|sources| sources.get(&self.warehouse_db_connection))
            .cloned()
    }
    
    pub fn get_kafka_cluster_conn(&self, cluster_name: &str) -> Option<&KafkaSourceConfig> {
        if let Some(s) = &self.kafka_source {
            s.get(cluster_name)
        } else {
            None
        }
    }
}
