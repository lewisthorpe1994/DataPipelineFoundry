use std::collections::HashMap;
use clap::Args;
use minijinja::{context, Environment};
use serde::Serialize;
use std::fmt::Display;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use common::types::sources::SourceType;

/// Templates for foundry project
const PROJECT_FILE_NAME: &str = "foundry-project.yml";
const PROJECT_TEMPLATE: &str = include_str!("templates/foundry-project.yml.j2");

/// Connections templates
const CONNECTIONS_FILE_NAME: &str = "connections.yml";
const CONNECTIONS_TEMPLATE: &str = include_str!("templates/connections.yml.j2");

// Warehouse source templates
const WAREHOUSE_FILE_NAME: &str = "warehouse-sources.yml";
const WAREHOUSE_SOURCE_TEMPLATE: &str = include_str!("templates/warehouse-sources.yml.j2");
const FOUNDRY_WAREHOUSE_SOURCE_PATH: &str = "foundry-sources/warehouse/warehouse-sources.yml";

// Kafka source templates
const KAFKA_FILE_NAME: &str = "kafka-sources.yml";
const KAFKA_SOURCE_TEMPLATE: &str = include_str!("templates/kafka-sources.yml.j2");
const FOUNDRY_KAFKA_SOURCE_PATH: &str = "foundry-sources/kafka/kafka-sources.yml";

// Api source templates
const API_FILE_NAME: &str = "api-sources.yml";
const API_SOURCE_TEMPLATE: &str = include_str!("templates/api-sources.yml.j2");
const FOUNDRY_API_SOURCE_PATH: &str = "foundry_sources/api/api-sources.yml";

// Default dirs
const DEFAULT_MODELS_DIR: &str = "foundry_models";
const DEFAULT_SOURCES_DIR: &str = "foundry_sources";

// example source names
const DEFAULT_WAREHOUSE_SOURCE_NAME: &str = "some_orders";
const DEFAULT_KAFKA_SOURCE_NAME: &str = "some_kafka_cluster";
const DEFAULT_API_SOURCE_NAME: &str = "some_api";

#[derive(Serialize, Copy, Clone)]
enum FlowArch {
    Medallion,
    SemanticMedallion,
    Kimball,
}
impl FlowArch {
    pub fn layers(&self) -> Vec<&'static str> {
        match self {
            FlowArch::Medallion => vec!["bronze", "silver", "gold"],
            FlowArch::SemanticMedallion => vec!["bronze", "silver", "gold_star", "gold_marts"],
            FlowArch::Kimball => vec!["staging", "star", "mart"],
        }
    }
}

#[derive(Serialize)]
struct FlowLayer {
    name: String,
    dir: String,
}

#[derive(Debug, Args)]
pub struct InitArgs {
    #[arg(
        long = "dir",
        short = 'd',
        default_value = ".",
        help = "Target path for project"
    )]
    pub(crate) path: PathBuf,
    #[arg(
        long = "project-name",
        short = 'n',
        default_value = "foundry-project",
        help = "project name"
    )]
    pub(crate) project_name: String,
    #[arg(
        long = "flow_arch",
        short = 'a',
        help = "flow architecture to use in project"
    )]
    pub(crate) flow_arch: Option<String>,
}

enum FileTemplates {
    Project,
    Connections,
    WarehouseSources,
    KafkaSources,
    ApiSources,
}
impl Display for FileTemplates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileTemplates::Project => write!(f, "foundry-project.yml"),
            FileTemplates::Connections => write!(f, "connections.yml"),
            FileTemplates::WarehouseSources => write!(f, "warehouse-sources.yml"),
            FileTemplates::KafkaSources => write!(f, "kafka-sources.yml"),
            FileTemplates::ApiSources => write!(f, "api-sources.yml"),
        }
    }
}

trait FileTemplate {
    fn template(&self) -> &'static str;
    fn path(&self) -> &'static str;
}

impl FileTemplate for FileTemplates {
    fn template(&self) -> &'static str {
        match self {
            FileTemplates::Project => PROJECT_TEMPLATE,
            FileTemplates::Connections => CONNECTIONS_TEMPLATE,
            FileTemplates::WarehouseSources => WAREHOUSE_SOURCE_TEMPLATE,
            FileTemplates::KafkaSources => KAFKA_SOURCE_TEMPLATE,
            FileTemplates::ApiSources => API_SOURCE_TEMPLATE,
        }
    }
    fn path(&self) -> &'static str {
        match self {
            FileTemplates::Project => PROJECT_FILE_NAME,
            FileTemplates::Connections => CONNECTIONS_FILE_NAME,
            FileTemplates::WarehouseSources => FOUNDRY_WAREHOUSE_SOURCE_PATH,
            FileTemplates::KafkaSources => FOUNDRY_KAFKA_SOURCE_PATH,
            FileTemplates::ApiSources => FOUNDRY_API_SOURCE_PATH,       
        }
    }
}

fn create_component<S, P>(
    env: &mut Environment,
    project_path: &Path,
    template_path: P,
    ctx: Option<S>,
) -> std::io::Result<()>
where
    S: Serialize,
    P: FileTemplate,
{
    env.add_template(template_path.path(), template_path.template())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let temp_path_str = template_path.path();
    let template = env
        .get_template(temp_path_str)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // render file
    let rendered = match ctx {
        Some(c) => template.render(c),
        None => template.render(()),
    };

    match rendered {
        Ok(r) => {
            let write_path = project_path.join(temp_path_str);
            if let Some(parent) = write_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(write_path)
                .expect(format!("Could not create {}", temp_path_str).as_str());

            file.write_all(r.as_bytes())?;
        }
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    }
    Ok(())
}

fn create_dir_with_placeholder<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    fs::create_dir(&path)?;
    File::create(path.as_ref().join(".gitkeep"))?;
    Ok(())
}


pub fn handle_init(
    path: &Path,
    project_name: String,
    flow_arch: Option<String>,
) -> std::io::Result<()> {
    if !path.exists() {
        println!(
            "No existing dir detected!\nCreating project parser at {}",
            path.display()
        );
        fs::create_dir_all(path)?;
    }
    println!("Initializing project at {}", path.display());

    let proj_path = path.join(&project_name);
    let models_path = proj_path.join(DEFAULT_MODELS_DIR);

    // create project dir
    fs::create_dir(&proj_path)?;

    // create models dir
    fs::create_dir(&models_path)?;

    // create flow folders
    let modelling_arch = if let Some(arch) = &flow_arch {
        match arch.as_str() {
            "medallion" => Some(FlowArch::Medallion),
            "semantic_medallion" => Some(FlowArch::SemanticMedallion),
            "kimball" => Some(FlowArch::Kimball),

            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{} is not supported as a valid flow arch", arch),
                ))
            }
        }
    } else {
        None
    };

    // create macro folder
    let macros_path = proj_path.join("macros");
    fs::create_dir(&macros_path)?;
    
    // create sub folders for different macros
    create_dir_with_placeholder(&macros_path.join("kafka"))?;
    create_dir_with_placeholder(&macros_path.join("warehouse"))?;
    create_dir_with_placeholder(&macros_path.join("api"))?;

    // create flow layer dirs
    let mut modelling_layers: Vec<FlowLayer> = Vec::new();
    if let Some(arch) = modelling_arch {
        for layer in arch.layers() {
            let layer_dir = models_path.join(layer);
            fs::create_dir(&layer_dir)?;
            modelling_layers.push(FlowLayer {
                name: layer.to_string(),
                dir: format!("{}/{}", DEFAULT_MODELS_DIR, layer),
            });
        }
    }

    let mut env = Environment::new();
    
    // create project file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::Project,
        Some(context! {
            project_name => project_name,
            models_dir => DEFAULT_MODELS_DIR,
            modelling_arch => flow_arch,
            layers => modelling_layers,
            sources => vec![
                HashMap::from([
                    ("name", DEFAULT_WAREHOUSE_SOURCE_NAME), 
                    ("path", FileTemplates::WarehouseSources.path()),
                    ("kind", &*SourceType::Warehouse.to_string()),
                ]),
                HashMap::from([
                    ("name", DEFAULT_KAFKA_SOURCE_NAME),
                    ("path", FileTemplates::KafkaSources.path()),
                    ("kind", &*SourceType::Kafka.to_string()),   
                ]),
                HashMap::from([
                    ("name", DEFAULT_API_SOURCE_NAME),
                    ("path", FileTemplates::ApiSources.path()),
                    ("kind", &*SourceType::Api.to_string()),  
                ]),           
            ]
        }),
    )?;
    
    // create connections file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::Connections,
        Option::<()>::None,
    )?;
    
    // create warehouse sources file   
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::WarehouseSources,
        Some(context! {example_warehouse_source_name => DEFAULT_WAREHOUSE_SOURCE_NAME}),
    )?;
    
    // create kafka sources file  
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::KafkaSources,
        Some(context! {example_kafka_source_name => DEFAULT_KAFKA_SOURCE_NAME})
    )?;
    
    // create api sources file  
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::ApiSources,
        Some(context! {example_api_source_name => DEFAULT_API_SOURCE_NAME}),   
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};
    use std::fs;
    use minijinja::Environment;

    fn read_to_string(path: &Path) -> String {
        fs::read_to_string(path).expect("read file")
    }

    // ---------- create_component -------------------------------------------
    #[test]
    fn test_create_component_writes_file() {
        // temp project dir
        let dir = tempdir().unwrap();
        let mut env = Environment::new();

        // write file
        create_component::<(), _>(
            &mut env,
            dir.path(),
            FileTemplates::Connections,
            None,
        )
            .expect("create_component failed");

        // path must exist
        let expected = dir.path().join("connections.yml");
        assert!(expected.exists());
        // template actually rendered (file not empty)
        let contents = read_to_string(&expected);
        assert!(!contents.trim().is_empty());
    }

    // helper to build a project and return the TempDir so it lives until
    // assertions are done
    fn init_project(flow_arch: Option<&str>) -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let proj_name = "my_proj";

        handle_init(
            dir.path(),
            proj_name.to_string(),
            flow_arch.map(|s| s.to_string()),
        )
            .expect("handle_init failed");

        // compute the path *first*
        let proj_path = dir.path().join(proj_name);

        // now move `dir`
        (dir, proj_path)
    }

    // ---------- handle_init -------------------------------------------------
    #[test]
    fn test_handle_init_creates_structure_medallion() {
        let (root, proj_path) = init_project(Some("medallion"));

        // baseline folders
        assert!(proj_path.exists());
        assert!(proj_path.join("macros").exists());
        assert!(proj_path.join("foundry_models").exists());

        // medallion layers
        for layer in ["bronze", "silver", "gold"] {
            assert!(
                proj_path.join("foundry_models").join(layer).exists(),
                "layer dir {} missing",
                layer
            );
        }

        // key files
        for file in [
            "foundry-project.yml",
            "connections.yml",
            FOUNDRY_WAREHOUSE_SOURCE_PATH,
        ] {
            assert!(
                proj_path.join(file).exists(),
                "file {} missing",
                file
            );
        }

        // quick sanity: project YAML contains project name
        let project_yaml = fs::read_to_string(proj_path.join("foundry-project.yml")).unwrap();
        assert!(project_yaml.contains("my_proj"));

        // keep TempDir alive until here
        drop(root);
    }

    #[test]
    fn test_handle_init_invalid_arch() {
        let dir = tempdir().unwrap();
        let err = handle_init(dir.path(), "x".into(), Some("unknown".into()))
            .expect_err("should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
