use common::config::loader::read_config;
use common::error::FFError;
use ff_core::functions::compile::compile;

/// Compile the current project using settings from `foundry-project.yml`.
pub fn handle_compile() -> Result<(), FFError> {
    let cfg = read_config(None).map_err(FFError::compile)?;
    compile(cfg.project.compile_path)?;
    Ok(())
}
