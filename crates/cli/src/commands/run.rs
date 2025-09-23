use clap::Args;
use common::error::FFError;
// use ff_core::{config::loader::read_config, functions};
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[derive(Debug, Args)]
pub struct RunArgs {
    #[arg(long = "model", short = 'm', help = "Which model to run")]
    pub(crate) model: Option<String>,
}

// /// Run compiled models against the configured target database.
// ///
// /// The target argument corresponds to a connection profile defined in
// /// `connections.yml`.
// pub fn handle_run(model_name: Option<String>, config_path: Option<PathBuf>) -> Result<(), FFError> {
//     let cfg = read_config(config_path).map_err(|e| FFError::Compile(e.into()))?;
//     let rt = Runtime::new().map_err(|e| FFError::Run(Box::new(e)))?;
//     rt.block_on(functions::run::run(cfg, model_name))
// }
