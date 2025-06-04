use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::Path;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct FoundryConfig {
    pub project_name: String,
    pub version: String,
    pub compile_path: String,
    pub modelling_architecture: String,
    pub connection_profile: String,
    pub paths: PathsConfig,
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
    pub layers: Option<HashMap<String, String>>,
}


pub fn read_config(
    project_config_path: &Path,
    connections_path: &Path
) -> Result<FoundryConfig, Box<dyn Error>> {
    let project_file = fs::File::open(project_config_path)?;
    let config = serde_yaml::from_reader(project_file)?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use std::fs;

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

        // Write YAML to a temp file
        let mut temp = NamedTempFile::new().unwrap();
        write!(temp, "{}", yaml).unwrap();

        // Use dummy connections path, not used in read_config
        let dummy_path = temp.path(); // Just reusing the same temp file

        let config = read_config(temp.path(), dummy_path).expect("Failed to read config");

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
    }
}
