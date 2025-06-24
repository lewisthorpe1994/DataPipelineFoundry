use common::error::FFError;
use ff_core::{functions, config::loader::read_config};

/// Run compiled models against the configured target database.
///
/// The target argument corresponds to a connection profile defined in
/// `connections.yml`.
pub fn handle_run(target: String) -> Result<(), FFError> {
    let cfg = read_config(None).map_err(|e| FFError::Compile(e.into()))?;
    functions::run::run(cfg, target)
}
