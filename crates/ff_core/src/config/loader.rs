use common::{types::schema::Database, error::FFError};

use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::ops::Deref;
use std::path::Path;

pub type ConnectionProfile = HashMap<String, String>;
pub type LayerName = String;
pub type LayerDir = String;
pub type Layers = HashMap<LayerName, LayerDir>;

/// Foundry Config
#[derive(Debug, Deserialize)]
pub struct FoundryProjectConfig {
    pub project_name: String,
    pub version: String,
    pub compile_path: String,
    pub modelling_architecture: String,
    pub connection_profile: String,
    pub paths: PathsConfig,
}

#[derive(Debug, Deserialize)]
pub struct PathsConfig {
    pub models: ModelsConfig,
    pub sources: Vec<SourcesPath>,
    pub connections: String
}

#[derive(Debug, Deserialize)]
pub struct ModelsConfig {
    pub dir: String,
    pub layers: Option<Layers>,
}

#[derive(Debug, Deserialize)]
#[derive(Clone)]
pub struct SourcesPath {
    pub name: String,
    pub path: String,
}

/// Connections Config
pub type ConnectionsConfig = HashMap<String, ConnectionProfile>;

/// Source Config
#[derive(Deserialize, Debug)]
pub struct SourceConfig {
    pub name: String,
    pub database: Database,
}

#[derive(Debug)]
pub struct SourceConfigs(HashMap<String, SourceConfig>);

impl Deref for SourceConfigs {
    type Target = HashMap<String, SourceConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Deserialize, Debug)]
pub struct SourceFileConfigs {
    sources: Vec<SourceConfig>
}

impl From<Vec<SourcesPath>> for SourceConfigs {
    fn from(value: Vec<SourcesPath>) -> Self {
        let config: HashMap<String, SourceConfig> = value.into_iter().flat_map(|p| {
            let s_config_path = fs::File::open(p.path.clone()).expect("Failed to open source config");
            let s_file_config: SourceFileConfigs = serde_yaml::from_reader(s_config_path).expect(
                "Failed to parse source config",
            );
            s_file_config.sources.into_iter().map(|s| (s.name.clone(), s))
        })
            .collect();
        
        Self::new(config)
    }
}

impl SourceConfigs {
    pub fn new(configs: HashMap<String, SourceConfig>) -> Self {
        Self(configs)
    }

    pub fn get(&self, name: &str) -> Option<&SourceConfig> {
        self.0.get(name)
    }

    pub fn resolve(&self, name: &str, table: &str) -> Result<String, SourceConfigError> {
        let config = self
            .get(name)
            .ok_or_else(|| SourceConfigError::SourceNotFound(name.to_string()))?;

        let resolved = config
            .database
            .schemas
            .iter()
            .flat_map(|schema| schema.tables.iter().map(move |t| (schema, t.name.clone())))
            .find(|(_, t)| t == table);

        match resolved {
            Some((schema, table)) => Ok(format!("{}.{}.{}", config.name, schema.name, table)),
            None => Err(SourceConfigError::TableNotFound(table.to_string())),
        }
    }
}

pub enum SourceConfigError {
    SourceNotFound(String),
    TableNotFound(String),
}
impl Display for SourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceNotFound(name) => write!(f, "Source not found: {}", name),
            Self::TableNotFound(name) => write!(f, "Table not found: {}", name),
        }
    }
}
impl Debug for SourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl From<SourceConfigError> for JinjaError {
    fn from(err: SourceConfigError) -> JinjaError {
        match err {
            SourceConfigError::SourceNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
            SourceConfigError::TableNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
        }
    }
}

/// global config
#[derive(Debug)]
pub struct FoundryConfig {
    pub project: FoundryProjectConfig,
    pub source: SourceConfigs,
    pub connections: ConnectionsConfig,   
}
impl FoundryConfig {
    fn new(project: FoundryProjectConfig, source: SourceConfigs, connections: ConnectionsConfig) -> Self {
        Self { project, source, connections }
    }
}

pub enum ConfigError {
    IncorrectPath(String),
    MissingConnection(String),
    ParseError(serde_yaml::Error),
}

impl Error for ConfigError {}
impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncorrectPath(path) => write!(f, "Incorrect path: {}", path),
            Self::ParseError(err) => write!(f, "Parse error: {}", err),       
            Self::MissingConnection(path) => write!(f, "Missing connection: {}", path),  
        }
    }
}
impl Debug for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        Self::IncorrectPath(err.to_string())
    }
}

impl From<serde_yaml::Error> for ConfigError {
    fn from(err: serde_yaml::Error) -> Self {
        Self::ParseError(err)
    }
}

pub fn read_config(
    project_config_path: Option<&Path>,
) -> Result<FoundryConfig, ConfigError> {
    
    let proj_config_path = project_config_path.unwrap_or(Path::new("foundry-project.yml"));
    
    let project_file = fs::File::open(proj_config_path)?;
    let proj_config: FoundryProjectConfig = serde_yaml::from_reader(project_file)?;
    
    let connections_path = Path::new(&proj_config.paths.connections);
    if !connections_path.exists() {
        return Err(ConfigError::MissingConnection(connections_path.to_string_lossy().to_string()));   
    }
    let conn_file = fs::File::open(connections_path)?;
    let connections: HashMap<String, ConnectionProfile> = serde_yaml::from_reader(conn_file)?;
    
    let source_config = SourceConfigs::from(proj_config.paths.sources.clone());
    
    let config = FoundryConfig::new(proj_config, source_config, connections);

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::schema::{Schema, Table};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_config() {
        use tempfile::NamedTempFile;
        use std::io::Write;

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
        let cfg = read_config(Some(project_file.path())).expect("Failed to read config");

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
}
