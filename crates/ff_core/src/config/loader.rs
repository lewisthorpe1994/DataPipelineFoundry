use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::Path;
use serde::Deserialize;

pub type ConnectionProfile = HashMap<String, String>;
pub type LayerName = String;
pub type LayerDir = String;
pub type Layers = HashMap<LayerName, LayerDir>;
#[derive(Debug, Deserialize)]
pub struct FoundryConfig {
    pub project_name: String,
    pub version: String,
    pub compile_path: String,
    pub modelling_architecture: String,
    pub connection_profile: String,
    pub paths: PathsConfig,
    #[serde(default)]
    pub connections: HashMap<String, ConnectionProfile>,
    // pub description: String,           // Omit if not in the YAML
    // pub target_engine: String,
}

#[derive(Debug, Deserialize)]
pub struct PathsConfig {
    pub models: ModelsConfig,
}

#[derive(Debug, Deserialize)]
pub struct ModelsConfig {
    pub dir: String,
    pub layers: Option<Layers>,
}


pub fn read_config(
    project_config_path: &Path,
    connections_path: &Path
) -> Result<FoundryConfig, Box<dyn Error>> {
    let project_file = fs::File::open(project_config_path)?;
    let mut config: FoundryConfig = serde_yaml::from_reader(project_file)?;

    if connections_path.exists() {
        let conn_file = fs::File::open(connections_path)?;
        let connections: HashMap<String, ConnectionProfile> = serde_yaml::from_reader(conn_file)?;
        config.connections = connections;
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_read_config() {
        let yaml = r#"
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
modelling_architecture: medallion
connection_profile: dev
"#;

        // Write project YAML to a temp file
        let mut project_temp = NamedTempFile::new().unwrap();
        write!(project_temp, "{}", yaml).unwrap();

        // Connections YAML
        let connections_yaml = r#"
dev:
  adapter: postgres
  host: localhost
  port: "5432"
  user: postgres
  password: postgres
  database: test
"#;
        let mut conn_temp = NamedTempFile::new().unwrap();
        write!(conn_temp, "{}", connections_yaml).unwrap();

        let config = read_config(project_temp.path(), conn_temp.path()).expect("Failed to read config");

        assert_eq!(config.project_name, "test_project");
        assert_eq!(config.version, "1.0.0");
        assert_eq!(config.compile_path, "compiled");
        assert_eq!(config.paths.models.dir, "foundry_models");
        assert_eq!(config.modelling_architecture, "medallion");
        assert_eq!(config.connection_profile, "dev");
        let layers = config.paths.models.layers.as_ref().unwrap();
        assert_eq!(layers["bronze"], "foundry_models/bronze");
        assert_eq!(layers["silver"], "foundry_models/silver");
        assert_eq!(layers["gold"], "foundry_models/gold");

        assert_eq!(config.connections.get("dev").unwrap()["adapter"], "postgres");
    }
}
