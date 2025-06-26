use common::traits::IsFileExtension;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use crate::config::components::connections::{ConnectionProfile};
use crate::config::components::foundry_project::{FoundryProjectConfig, ModelLayers};
use crate::config::components::global::FoundryConfig;
use crate::config::components::model::ModelsConfig;
use crate::config::components::source::SourceConfigs;
use crate::config::error::ConfigError;
use crate::config::traits::{ConfigName, FromFileConfigList, IntoConfigVec};

pub fn load_config<T, V, Wrapper>(path: &Path) -> Result<Wrapper, ConfigError>
where
    T: ConfigName,
    V: IntoConfigVec<T> + DeserializeOwned,
    Wrapper: From<HashMap<String, T>>,
{
    let file = fs::File::open(path)?;
    let config: V = serde_yaml::from_reader(file)?;
    Ok(Wrapper::from_config_list(config.vec()))
}

pub fn read_config(project_config_path: Option<PathBuf>) -> Result<FoundryConfig, ConfigError> {
    let proj_config_path = project_config_path.unwrap_or_else(|| "foundry-project.yml".into());

    let project_file = fs::File::open(proj_config_path)?;
    let proj_config: FoundryProjectConfig = serde_yaml::from_reader(project_file)?;

    let connections_path = Path::new(&proj_config.paths.connections);
    if !connections_path.exists() {
        return Err(ConfigError::MissingConnection(
            connections_path.to_string_lossy().to_string(),
        ));
    }
    let conn_file = fs::File::open(connections_path)?;
    let connections: HashMap<String, ConnectionProfile> = serde_yaml::from_reader(conn_file)?;

    let source_config = SourceConfigs::from(proj_config.paths.sources.clone());
    let models_config = proj_config.paths.models.layers
        .as_ref()
        .map(ModelsConfig::try_from)
        .transpose()?;
    let conn_profile = proj_config.connection_profile.clone();
    
    let config = FoundryConfig::new(proj_config, source_config, connections, models_config, conn_profile);

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::schema::{Schema, Database};
    use crate::config::components::source::SourceConfig;
    use crate::config::components::source::SourceConfigError;

    #[test]
    fn test_read_config() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // 1️⃣  ── create the connections YAML file ───────────────────────────────
        let connections_yaml = r#"
dev:
  adapter: postgres
  host: localhost
  port: "5432"
  user: postgres
  password: postgres
  database: test
"#;
        let mut conn_file = NamedTempFile::new().unwrap();
        write!(conn_file, "{}", connections_yaml).unwrap();

        // 2️⃣  ── create the sources YAML file ───────────────────────────────────
        let sources_yaml = r#"
sources:
  - name: test_source
    database:
      name: some_database
      schemas:
        - name: bronze
          tables:
            - name: raw_orders
              description: Raw orders ingested from upstream system
"#;
        let mut src_file = NamedTempFile::new().unwrap();
        write!(src_file, "{}", sources_yaml).unwrap();

        // 3️⃣  ── build the *project* YAML with the real paths inserted ──────────
        let project_yaml = format!(
            r#"
project_name: test_project
version: "1.0.0"
compile_path: "compiled"
paths:
  models:
    dir: foundry_models
    layers:
      bronze: foundry_models/bronze
      silver: foundry_models/silver
      gold: foundry_models/gold
  connections: {}
  sources:
    - name: test_source
      path: {}
modelling_architecture: medallion
connection_profile: dev
"#,
            conn_file.path().display(),
            src_file.path().display()
        );

        // 4️⃣  ── write the project YAML                                             ─
        let mut project_file = NamedTempFile::new().unwrap();
        write!(project_file, "{}", project_yaml).unwrap();

        // 5️⃣  ── load and assert ────────────────────────────────────────────────
        let cfg = read_config(Some(PathBuf::from(project_file.path()))).expect("Failed to read config");

        assert_eq!(cfg.project.project_name, "test_project");
        assert_eq!(cfg.project.version, "1.0.0");
        assert_eq!(cfg.project.compile_path, "compiled");
        assert_eq!(cfg.project.paths.models.dir, "foundry_models");
        assert_eq!(cfg.project.modelling_architecture, "medallion");
        assert_eq!(cfg.project.connection_profile, "dev");

        let layers = cfg.project.paths.models.layers.as_ref().unwrap();
        assert_eq!(layers["bronze"], "foundry_models/bronze");
        assert_eq!(layers["silver"], "foundry_models/silver");
        assert_eq!(layers["gold"], "foundry_models/gold");

        assert_eq!(cfg.connections["dev"]["adapter"], "postgres");
    }

    #[test]
    fn test_resolve_missing_table() {
        let schema = Schema {
            name: "bronze".to_string(),
            description: None,
            tables: vec![],
        };

        let database = Database {
            name: "my_database".to_string(),
            schemas: vec![schema],
        };

        let source_config = SourceConfig {
            name: "bronze".to_string(),
            database,
        };

        let mut configs = HashMap::new();
        configs.insert("bronze".to_string(), source_config);
        let sources = SourceConfigs::new(configs);

        let result = sources.resolve("bronze", "non_existent_table");

        match result {
            Err(SourceConfigError::TableNotFound(name)) => {
                assert_eq!(name, "non_existent_table");
            }
            _ => panic!("Expected TableNotFound error"),
        }
    }
    
    #[test]
    fn test_with_model_config() {
        use std::io::Write;
        use tempfile::{NamedTempFile, TempDir};
        use std::fs;

        // Create a temporary directory for models
        let temp_dir = TempDir::new().unwrap();
        let models_dir = temp_dir.path().join("foundry_models");
        let bronze_dir = models_dir.join("bronze");
        fs::create_dir_all(&bronze_dir).unwrap();

        // Create a model config YAML file
        let model_yaml = r#"
models:
  - name: bronze_orders
    description: Bronze layer orders model
    materialization: table
    columns:
      - name: order_id
        data_type: bigint
        description: Order identifier
      - name: customer_name
        data_type: varchar
        description: Customer name
"#;
    
        let model_file_path = bronze_dir.join("orders.yml");
        fs::write(&model_file_path, model_yaml).unwrap();

        // Create connections YAML file
        let connections_yaml = r#"
dev:
  adapter: postgres
  host: localhost
  port: "5432"
  user: postgres
  password: postgres
  database: test
"#;
        let mut conn_file = NamedTempFile::new().unwrap();
        write!(conn_file, "{}", connections_yaml).unwrap();

        // Create sources YAML file
        let sources_yaml = r#"
sources:
  - name: test_source
    database:
      name: some_database
      schemas:
        - name: bronze
          tables:
            - name: raw_orders
              description: Raw orders ingested from upstream system
"#;
        let mut src_file = NamedTempFile::new().unwrap();
        write!(src_file, "{}", sources_yaml).unwrap();

        // Create project YAML with model layers
        let project_yaml = format!(
            r#"
project_name: test_project
version: "1.0.0"
compile_path: "compiled"
paths:
  models:
    dir: {}
    layers:
      bronze: {}
      silver: {}/silver
      gold: {}/gold
  connections: {}
  sources:
    - name: test_source
      path: {}
modelling_architecture: medallion
connection_profile: dev
"#,
            models_dir.display(),
            bronze_dir.display(),
            models_dir.display(),
            models_dir.display(),
            conn_file.path().display(),
            src_file.path().display()
        );

        let mut project_file = NamedTempFile::new().unwrap();
        write!(project_file, "{}", project_yaml).unwrap();

        // Load and assert
        let cfg = read_config(Some(PathBuf::from(project_file.path()))).expect("Failed to read config");

        // Verify models config is loaded
        assert!(cfg.models.is_some(), "Models config should be loaded");
    
        let models = cfg.models.as_ref().unwrap();
        println!("{:?}", models);
        assert!(models.contains_key("bronze_orders"), "Should contain bronze_orders model");
    
        let bronze_orders = &models["bronze_orders"];
        assert_eq!(bronze_orders.name, "bronze_orders");
        assert_eq!(bronze_orders.description, Some("Bronze layer orders model".to_string()));
        assert_eq!(bronze_orders.materialization, common::types::Materialize::Table);
        assert_eq!(bronze_orders.columns.len(), 2);
        assert_eq!(bronze_orders.columns[0].name, "order_id");
        assert_eq!(bronze_orders.columns[1].name, "customer_name");
    }
}