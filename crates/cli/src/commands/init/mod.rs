use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use clap::Args;
use minijinja::{context, Environment};
use serde::Serialize;

const PROJECT_TEMPLATE: &str = include_str!("foundry-project.yml.j2");
const CONNECTIONS_TEMPLATE: &str = include_str!("connections.yml.j2");
const DEFAULT_MODELS_DIR: &str = "foundry_models";

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
        default_value = "forgery-project",
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

pub fn handle_init(
    path: &Path,
    project_name: String,
    flow_arch: Option<String>,
) -> std::io::Result<()> {
    println!("Initializing project at {}", path.display());

    let proj_path = path.join(&project_name);
    let proj_config_path = proj_path.join("foundry-project.yml");
    let conn_config_path = proj_path.join("connections.yml");
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

            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                                                format!("{} is not supported as a valid flow arch", arch)))
        }
    } else {
        None
    };

    // create macro folder
    fs::create_dir(proj_path.join("macros"))?;

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
    env.add_template("forgery-project.yml", PROJECT_TEMPLATE)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let template = env.get_template("forgery-project.yml")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // render project.yml
    let rendered = template.render(context! {
        project_name => project_name,
        models_dir => DEFAULT_MODELS_DIR,
        modelling_arch => flow_arch,
        layers => modelling_layers
    })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let mut proj_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(proj_config_path)
        .expect("Could not create forgery-project.yml");

    // write project.yml file
    proj_file.write_all(rendered.as_bytes())?;

    // write connections.yml file
    let mut con_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(conn_config_path)
        .expect("Could not create connections.yml");
    
    con_file.write_all(CONNECTIONS_TEMPLATE.as_bytes())?;

    Ok(())
}