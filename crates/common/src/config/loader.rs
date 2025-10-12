use crate::config::components::foundry_project::{FoundryProjectConfig};
use crate::config::components::global::FoundryConfig;
use crate::config::components::model::{ModelLayers, ResolvedModelLayerConfig, ResolvedModelsConfig};
use crate::config::components::sources::kafka::{KafkaConnectorConfig, KafkaSourceConfig, KafkaSourceConfigs};
use crate::config::components::sources::warehouse_source::{DbConfig};
use crate::config::components::sources::{SourcePathConfig, SourcePaths};
use crate::config::error::ConfigError;
use crate::config::traits::{ConfigName, FromFileConfigList, IntoConfigVec};
use serde::de::{DeserializeOwned, Error};
use serde::Deserialize;
use serde_yaml::{self, Error as YamlError, Value};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use crate::config::components::connections::{AdapterConnectionDetails, DatabaseAdapterType};
use crate::types::sources::SourceType;
use crate::utils::paths_with_ext;

pub fn load_config<V>(path: &Path) -> Result<HashMap<String, V>, ConfigError>
where
    V: DeserializeOwned + ConfigName,
{
    let mut sources: HashMap<String, V> = HashMap::new();
    for entry in paths_with_ext(path, "yml") {
        println!("loading source from {}", entry.display());
        let file = fs::File::open(&entry)?;
        let source: V = serde_yaml::from_reader(file)?;
        sources.insert(source.name().to_string(), source);
    }

    Ok(sources)
}

pub fn read_config(project_config_path: Option<PathBuf>) -> Result<FoundryConfig, ConfigError> {
    let proj_config_file_path = if let Some(config_path) = project_config_path {
        config_path.join("foundry-project.yml")
    } else {
        "foundry-project.yml".into()
    };

    let project_file = fs::File::open(&proj_config_file_path)?;
    let proj_config: FoundryProjectConfig = serde_yaml::from_reader(project_file)?;

    let config_root = proj_config_file_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("../../../../.."));

    let connections_path = resolve_path(&config_root, Path::new(&proj_config.connection_profile.path));
    if !connections_path.exists() {
        return Err(ConfigError::MissingConnection(
            connections_path.to_string_lossy().to_string(),
        ));
    }
    let conn_file = fs::File::open(connections_path)?;
    let raw_connections: HashMap<String, Value> = serde_yaml::from_reader(conn_file)?;
    let mut connections: HashMap<String, HashMap<String, AdapterConnectionDetails>> =
        HashMap::new();

    for (profile, value) in raw_connections.into_iter() {
        let profile_connections = parse_connection_profile(value)
            .map_err(|err| ConfigError::ParseError(format!("profile {}: {}", profile, err)))?;
        connections.insert(profile, profile_connections);
    }

    let resolved_sources: SourcePaths = proj_config
        .sources
        .iter()
        .map(|(name, details)| {
            let resolved = resolve_path(&config_root, Path::new(&details.specifications));
            let resolved_definitions = match &details.definitions {
                Some(def) => {
                    let resolved_definitions = resolve_path(&config_root, Path::new(def));
                    Some(resolved_definitions)
                },
                None => None,
            };
            let resolved_root = details
                .source_root
                .as_ref()
                .map(|root| resolve_path(&config_root, Path::new(root)))
                .or_else(|| resolved.parent().map(Path::to_path_buf))
                .unwrap_or_else(|| config_root.clone());
            (
                name.clone(),
                SourcePathConfig {
                    specifications: resolved,
                    source_root: Some(resolved_root),
                    definitions: resolved_definitions,
                },
            )
        })
        .collect();

    let wh_sources = {
        let config = resolved_sources.get(&SourceType::Warehouse).ok_or(
            ConfigError::ParseError("Warehouse source not defined".to_string()),
        )?;
        load_config::<DbConfig>(&config.specifications)?
    };

    let (k_sources, k_definitions) = {
        let config = resolved_sources.get(&SourceType::Kafka);
        println!("kafka config {:#?}", config);
        match config {
            Some(config) => {
                let definitions_config = match &config.definitions {
                    Some(def) => {
                        println!("def config {:#?}", def);
                        let def_config = load_config::<KafkaConnectorConfig>(def)?;
                        def_config
                    },
                    None => HashMap::new()
                };
                (load_config::<KafkaSourceConfig>(&config.specifications)?, definitions_config)
            },
            None => (HashMap::new(), HashMap::new()),
        }
    };

    let src_db_sources = {
        let config = resolved_sources.get(&SourceType::SourceDB).ok_or(
            ConfigError::ParseError("No source databases defined!".to_string()),
        )?;
        load_config::<DbConfig>(&config.specifications)?
    };

    let models_config = if let Some(proj) = &proj_config.models.analytics_projects {
        let mut resolved_layers: HashMap<String, ResolvedModelLayerConfig> = HashMap::new();

        for (name, project) in proj {
            let configs: HashMap<String, ResolvedModelLayerConfig> = project.layers
                .iter()
                .map(|(name, proj_dir)| {
                    let full_dir = Path::new(&proj_config.models.dir).join(proj_dir);

                    let resolved = resolve_path(&config_root, &full_dir);

                    let resolved_config = ResolvedModelLayerConfig {
                        name: name.clone(),
                        path: resolved,
                        target: project.target_connection.clone(),
                    };
                    (name.clone(), resolved_config)
                }).collect();
            
            resolved_layers.extend(configs);
        }
        Some(ResolvedModelsConfig::try_from(&resolved_layers)?)
    } else {
        None
    };



    let conn_profile = proj_config.connection_profile.clone();

    let config = FoundryConfig::new(
        proj_config,
        wh_sources,
        connections,
        models_config,
        src_db_sources,
        conn_profile,
        k_sources,
        resolved_sources,
        k_definitions,
    );

    Ok(config)
}

fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn parse_connection_profile(
    value: Value,
) -> Result<HashMap<String, AdapterConnectionDetails>, YamlError> {
    // First try to interpret as a single connection definition.
    if let Ok(single) = serde_yaml::from_value::<RawConnectionDetails>(value.clone()) {
        let mut map = HashMap::new();
        map.insert("default".to_string(), single.into_adapter_details()?);
        return Ok(map);
    }

    // Otherwise expect a map of named connections.
    let nested: HashMap<String, RawConnectionDetails> = serde_yaml::from_value(value)?;
    let mut profile = HashMap::new();
    for (name, raw) in nested.into_iter() {
        profile.insert(name, raw.into_adapter_details()?);
    }
    Ok(profile)
}

#[derive(Debug, Deserialize)]
struct RawConnectionDetails {
    #[serde(default)]
    adapter: Option<DatabaseAdapterType>,
    #[serde(default)]
    adapter_type: Option<DatabaseAdapterType>,
    host: String,
    user: String,
    database: String,
    password: String,
    #[serde(deserialize_with = "deserialize_port_to_string")]
    port: String,
}

impl RawConnectionDetails {
    fn into_adapter_details(self) -> Result<AdapterConnectionDetails, YamlError> {
        let adapter_type = self
            .adapter_type
            .or(self.adapter)
            .ok_or_else(|| YamlError::custom("missing `adapter` or `adapter_type`"))?;

        Ok(AdapterConnectionDetails::new(
            self.host.as_str(),
            self.user.as_str(),
            self.database.as_str(),
            self.password.as_str(),
            self.port.as_str(),
            adapter_type,
        ))
    }
}

fn deserialize_port_to_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct PortVisitor;

    impl<'de> serde::de::Visitor<'de> for PortVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or integer port value")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value < 0 {
                return Err(E::custom("port cannot be negative"));
            }
            Ok(value.to_string())
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_owned())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }
    }

    deserializer.deserialize_any(PortVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::get_root_dir;

    #[test]
    fn test_read_config_from_example_project() {
        let project_root = get_root_dir();
        let config = read_config(Some(project_root.clone())).expect("should load example config");

        println!("{:#?}", config);
        // assert_eq!(config.project.name, "foundry-project");
        // assert_eq!(config.project.version, "1.0.0");
        // assert_eq!(config.project.compile_path, "compiled");
        // assert_eq!(config.project.connection_profile, "dev");
        // 
        // let layers = config
        //     .project
        //     .paths
        //     .models
        //     .layers
        //     .as_ref()
        //     .expect("example project defines model layers");
        // assert_eq!(layers.len(), 3);
        // assert_eq!(layers["bronze"], "foundry_models/bronze");
        // assert_eq!(layers["silver"], "foundry_models/silver");
        // assert_eq!(layers["gold"], "foundry_models/gold");
        // 
        // assert!(config.connections.contains_key("dev"));
        // let dev_connections = &config.connections["dev"];
        // assert!(dev_connections.contains_key("warehouse_source"));
        // assert!(dev_connections.contains_key("db_source"));
        // 
        // let warehouse_cfg = &config.warehouse_source;
        // assert!(warehouse_cfg.contains_key("some_orders"));
        // let some_orders = &warehouse_cfg["some_orders"];
        // assert_eq!(some_orders.database.name, "some_database");
        // 
        // let kafka_cfg = config
        //     .kafka_source
        //     .as_ref()
        //     .expect("example project defines kafka sources");
        // assert!(kafka_cfg.contains_key("some_kafka_cluster"));
        // assert_eq!(
        //     kafka_cfg["some_kafka_cluster"].bootstrap.servers,
        //     "some_comma_seperated_list"
        // );
    }

//     #[test]
//     fn test_models_config_from_example_project() {
//         let project_root = get_root_dir();
//         let config = read_config(Some(project_root.clone())).expect("load example config");
// 
//         let models = config.models.expect("example project models to load");
//         assert_eq!(models.len(), 3);
//         assert!(models.contains_key("bronze_orders"));
//         assert!(models.contains_key("silver_orders"));
//         assert!(models.contains_key("gold_customer_metrics"));
// 
//         let bronze_orders = &models["bronze_orders"];
//         assert_eq!(
//             bronze_orders.materialization,
//             crate::types::Materialize::Table
//         );
// 
//         let silver_orders = &models["silver_orders"];
//         assert_eq!(
//             silver_orders.materialization,
//             crate::types::Materialize::MaterializedView
//         );
//     }
// 
//     #[test]
//     fn test_raw_connection_allows_numeric_port_and_adapter_alias() {
//         let yaml = r#"
// adapter: postgres
// host: localhost
// user: postgres
// database: postgres
// password: postgres
// port: 5432
// "#;
// 
//         let raw: RawConnectionDetails = serde_yaml::from_str(yaml).expect("parse raw connection");
//         assert_eq!(raw.port, "5432");
//         assert!(matches!(raw.adapter, Some(DatabaseAdapterType::Postgres)));
// 
//         // Ensure we can convert into adapter details without error
//         raw.into_adapter_details().expect("connection details");
//     }
}
