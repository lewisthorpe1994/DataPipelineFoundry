use common::error::FFError;
use ff_core::{compiler, config::loader::read_config};

/// Compile the current project using settings from `foundry-project.yml`.
pub fn handle_compile() -> Result<(), FFError> {
    let cfg = read_config(None).map_err(|e| FFError::Compile(e.into()))?;
    compiler::compile(cfg.project.compile_path)
}

