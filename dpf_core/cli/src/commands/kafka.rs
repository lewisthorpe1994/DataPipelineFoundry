use clap::{Args, Subcommand};
use common::config::loader::read_config;
use common::error::FFError;
use ff_core::functions::kafka::{handle_kafka_compile, handle_kafka_deploy};
use log::info;
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[derive(Debug, Args)]
pub struct KafkaConnectorArgs {
    /// Name of the Kafka connector to compile
    #[arg(long, value_name = "NAME")]
    pub name: String,

    /// Validate the compiled connector against the Kafka Connect REST API
    #[arg(long)]
    pub cluster: Option<String>,

    #[arg(long, value_name = "compile")]
    pub compile: bool,

    #[arg(long, value_name = "validate")]
    pub validate: bool,

    #[arg(long, value_name = "deploy")]
    pub deploy: bool,
}

#[derive(Args, Debug)]
pub struct CompileArgs {
    /// Optional nested compile subcommand. If omitted, compiles everything.
    #[command(subcommand)]
    pub cmd: Option<KafkaSubcommand>,
}
#[derive(Debug, Subcommand)]
pub enum KafkaSubcommand {
    /// Compile a single Kafka connector
    #[command(name = "connector")]
    Connector(KafkaConnectorArgs),
}

pub fn handle_kafka(args: &KafkaSubcommand, config_path: Option<PathBuf>) -> Result<(), FFError> {
    let cfg = read_config(config_path).map_err(FFError::compile)?;
    let runtime = Runtime::new().map_err(FFError::compile)?;
    // runtime.block_on(

    match &args {
        KafkaSubcommand::Connector(k) => {
            if k.compile && k.deploy {
                return Err(FFError::compile_msg(
                    "compile and deploy are not valid options together".to_string(),
                ));
            } else if k.compile {
                let compiled = runtime
                    .block_on(handle_kafka_compile(
                        k.name.clone(),
                        cfg,
                        k.validate,
                        k.cluster.clone(),
                    ))
                    .map_err(FFError::compile)?;

                let pretty_config = compiled.to_json_string().map_err(FFError::compile)?;
                info!(
                    "Compiled connector `{}` config:\n{}",
                    compiled.name, pretty_config
                );
            } else if k.deploy {
                let cluster = k.cluster.clone().ok_or(FFError::compile_msg(
                    "Cluster name must be specified for deployment".to_string(),
                ))?;
                runtime
                    .block_on(handle_kafka_deploy(k.name.clone(), cfg, cluster))
                    .map_err(FFError::run)?;
            }
        }
    }
    Ok(())
}
