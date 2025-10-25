use clap::{Args, Subcommand};
use common::config::loader::read_config;
use common::error::FFError;
use ff_core::functions::compile::{compile, compile_kafka_connector, CompileOptions};
use log::info;
use shared_clients::kafka::KafkaConnectClient;
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[derive(Debug, Subcommand)]
pub enum CompileSubcommand {
    /// Compile a single Kafka connector
    #[command(name = "kafka-connector")]
    KafkaConnector(KafkaConnectorArgs),
}

#[derive(Args, Debug)]
pub struct CompileArgs {
    /// Optional nested compile subcommand. If omitted, compiles everything.
    #[command(subcommand)]
    pub cmd: Option<CompileSubcommand>,
}

#[derive(Debug, Args)]
pub struct KafkaConnectorArgs {
    /// Name of the Kafka connector to compile
    #[arg(long, value_name = "NAME")]
    pub name: String,

    /// Validate the compiled connector against the Kafka Connect REST API
    #[arg(long)]
    pub validate_against: Option<String>,
}

/// Compile the current project using settings from `foundry-project.yml`.
pub fn handle_compile(args: &CompileArgs, config_path: Option<PathBuf>) -> Result<(), FFError> {
    let cfg = read_config(config_path).map_err(FFError::compile)?;

    match &args.cmd {
        None => {
            // default: compile everything
            compile(&cfg)?;
        }
        Some(CompileSubcommand::KafkaConnector(k)) => {
            let compiled = compile_kafka_connector(&cfg, &k.name)?;

            if let Some(cluster_name) = k.validate_against.as_ref() {
                let cluster = cfg
                    .kafka_source
                    .get(cluster_name)
                    .ok_or_else(|| {
                        FFError::compile_msg(format!(
                            "Kafka cluster '{}' not found for connector '{}'",
                            cluster_name, compiled.name
                        ))
                    })?;


                let runtime = Runtime::new().map_err(FFError::compile)?;
                let client = KafkaConnectClient::new(&cluster.connect.host, &cluster.connect.port);

                runtime
                    .block_on(client.validate_connector(
                        &compiled.config.connector_class(),
                        &compiled.to_json().map_err(FFError::compile)?,
                    ))
                    .map_err(FFError::compile)?;

                info!(
                    "Validation succeeded for connector `{}` against {}:{}",
                    compiled.name, cluster.connect.host, cluster.connect.port
                );
            }

            let pretty_config =
                serde_json::to_string_pretty(&compiled.config).map_err(FFError::compile)?;
            info!(
                "Compiled connector `{}` config:\n{}",
                compiled.name, pretty_config
            );
        }
    }
    Ok(())
}
