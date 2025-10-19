use crate::config::components::connections::{
    AdapterConnectionDetails, Connections, ConnectionsConfig,
};
use crate::config::components::foundry_project::FoundryProjectConfig;
use crate::config::components::model::ResolvedModelsConfig;
use crate::config::components::sources::kafka::{KafkaConnectorConfig, KafkaSourceConfig};
use crate::config::components::sources::warehouse_source::{DbConfig, DbConfigError};
use crate::config::components::sources::SourcePaths;
use crate::config::error::ConfigError;
use std::collections::HashMap;

// ---------------- global config ----------------
#[derive(Debug)]
pub struct FoundryConfig {
    pub project: FoundryProjectConfig,
    pub warehouse_source: HashMap<String, DbConfig>,
    pub kafka_source: HashMap<String, KafkaSourceConfig>,
    pub source_db_configs: HashMap<String, DbConfig>,
    pub connections: ConnectionsConfig,
    pub models: Option<ResolvedModelsConfig>,
    pub kafka_connectors: HashMap<String, KafkaConnectorConfig>,
    pub connection_profile: Connections,
    pub source_paths: SourcePaths,
}
impl FoundryConfig {
    pub fn new(
        project: FoundryProjectConfig,
        warehouse_source: HashMap<String, DbConfig>,
        connections: ConnectionsConfig,
        models: Option<ResolvedModelsConfig>,
        source_db_configs: HashMap<String, DbConfig>,
        connection_profile: Connections,
        kafka_source: HashMap<String, KafkaSourceConfig>,
        source_paths: SourcePaths,
        kafka_connectors: HashMap<String, KafkaConnectorConfig>,
    ) -> Self {
        Self {
            project,
            warehouse_source,
            connections,
            models,
            source_db_configs,
            connection_profile,
            kafka_source,
            source_paths,
            kafka_connectors,
        }
    }
}

impl FoundryConfig {
    pub fn get_adapter_connection_details(&self, name: &str) -> Option<AdapterConnectionDetails> {
        self.connections
            .get(&self.connection_profile.profile)
            .and_then(|sources| sources.get(name))
            .cloned()
    }

    pub fn get_kafka_cluster_conn(
        &self,
        cluster_name: &str,
    ) -> Result<&KafkaSourceConfig, ConfigError> {
        let conn = self.kafka_source.get(cluster_name);
        conn.ok_or(ConfigError::not_found(format!(
            "kafka cluster config for {} not found",
            cluster_name
        )))
    }

    pub fn resolve_db_source(&self, name: &str, table: &str) -> Result<String, ConfigError> {
        let source_db_config = self.source_db_configs.get(name);
        let warehouse_db_config = self.warehouse_source.get(name);

        let config = match (source_db_config, warehouse_db_config) {
            (Some(_), Some(_)) => return Err(ConfigError::duplicate_database(name)),
            (Some(source_db_config), None) => source_db_config,
            (None, Some(warehouse_db_config)) => warehouse_db_config,
            (None, None) => {
                return Err(ConfigError::not_found(format!(
                    "Database config '{name}' was not found"
                )))
            }
        };

        let resolved = config
            .database
            .schemas
            .iter()
            .flat_map(|(name, obj)| {
                obj.tables.iter().map(move |(t_name, table)| {
                    (config.database.name.clone(), name, t_name.clone())
                })
            })
            .find(|(_, _, t)| t == table);

        match resolved {
            Some((database, schema, table)) => Ok(format!("{}.{}.{}", database, schema, table)),
            None => Err(ConfigError::not_found(format!(
                "No table configuration for '{name}' matched '{table}'"
            ))),
        }
    }
}
