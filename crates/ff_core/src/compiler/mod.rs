use std::path::Path;
use minijinja::Environment;
use crate::config::loader::FoundryProjectConfig;
use crate::dag::ModelDag;

pub fn compile_models(target_path: Option<&Path>, dag: ModelDag, config: FoundryProjectConfig, env: &Environment) {
    let compile_path = target_path.unwrap_or(Path::new(config.project_name.as_str()));
    
}