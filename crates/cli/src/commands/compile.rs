use clap::Args;
use common::config::loader::read_config;
use common::error::FFError;
use ff_core::functions::compile::{compile, CompileOptions};
use shared_clients::kafka::KafkaConnectClient;
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[derive(Debug, Args)]
pub struct CompileArgs {
    /// Compile a single Kafka connector by name
    #[arg(long = "kafka_connector", value_name = "NAME")]
    pub kafka_connector: Option<String>,
    /// Validate the compiled Kafka connector against the Kafka Connect API
    #[arg(long)]
    pub validate: bool,
}

/// Compile the current project using settings from `foundry-project.yml`.
pub fn handle_compile(args: &CompileArgs, config_path: Option<PathBuf>) -> Result<(), FFError> {
    if args.validate && args.kafka_connector.is_none() {
        return Err(FFError::compile_msg(
            "--validate requires specifying a --kafka_connector",
        ));
    }

    let cfg = read_config(config_path).map_err(FFError::compile)?;
    let compile_options = CompileOptions {
        kafka_connector: args.kafka_connector.clone(),
        ..Default::default()
    };

    let result = compile(&cfg, compile_options)?;

    if let Some(connector) = result.kafka_connector {
        let pretty_config =
            serde_json::to_string_pretty(&connector.config).map_err(FFError::compile)?;
        println!(
            "Compiled connector `{}` config:\n{}",
            connector.name, pretty_config
        );

        if args.validate {
            let cluster = cfg
                .kafka_source
                .get(&connector.cluster_name)
                .ok_or_else(|| {
                    FFError::compile_msg(format!(
                        "Kafka cluster '{}' not found for connector '{}'",
                        connector.cluster_name, connector.name
                    ))
                })?;

            let connector_class = connector
                .config
                .get("connector.class")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    FFError::compile_msg(
                        "Compiled connector config is missing the `connector.class` property",
                    )
                })?;

            let runtime = Runtime::new().map_err(FFError::compile)?;
            let client = KafkaConnectClient::new(&cluster.connect.host, &cluster.connect.port);

            runtime
                .block_on(client.validate_connector(
                    connector_class,
                    &connector.name,
                    &connector.config,
                ))
                .map_err(FFError::compile)?;

            println!(
                "Validation succeeded for connector `{}` against {}:{}",
                connector.name, cluster.connect.host, cluster.connect.port
            );
        }
    } else if let Some(name) = &args.kafka_connector {
        return Err(FFError::compile_msg(format!(
            "Connector '{}' was not found in the project",
            name
        )));
    }

    Ok(())
}
