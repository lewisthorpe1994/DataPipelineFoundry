use common::config::loader::read_config;
use common::error::FFError;
use ff_core::functions::compile::compile;
use std::path::PathBuf;

/// Compile the current project using settings from `foundry-project.yml`.
pub fn handle_compile(config_path: Option<PathBuf>) -> Result<(), FFError> {
    let cfg = read_config(config_path).map_err(FFError::compile)?;
    compile(&cfg)?;
    Ok(())
}
