use crate::functions::compile::compile_kafka_connector;
use common::config::components::global::FoundryConfig;
use common::config::components::sources::kafka::KafkaSourceConfig;
use common::error::FFError;
use components::KafkaConnector;
use log::info;
use shared_clients::kafka::KafkaConnectClient;

fn get_cluster_cfg(
    cluster_name: String,
    config: FoundryConfig,
) -> Result<KafkaSourceConfig, FFError> {
    let cluster = config
        .kafka_source
        .get(&cluster_name)
        .ok_or_else(|| {
            FFError::compile_msg(format!(
                "Kafka cluster '{}' not found for connector ",
                cluster_name,
            ))
        })?
        .to_owned();

    Ok(cluster)
}

pub async fn handle_kafka_compile(
    connector_name: String,
    config: FoundryConfig,
    validate: bool,
    cluster: Option<String>,
) -> Result<KafkaConnector, FFError> {
    let compiled = compile_kafka_connector(&config, &connector_name)?;

    if let Some(cluster_name) = cluster.as_ref() {
        let cluster = get_cluster_cfg(cluster_name.to_string(), config)?;

        if validate {
            let client = KafkaConnectClient::new(&cluster.connect.host, &cluster.connect.port);

            client
                .validate_connector(
                    &compiled.config.connector_class(),
                    &compiled.to_json().map_err(FFError::compile)?,
                )
                .await
                .map_err(FFError::compile)?;
        }

        info!(
            "Validation succeeded for connector `{}` against {}:{}",
            compiled.name, cluster.connect.host, cluster.connect.port
        );
    }

    Ok(compiled)
}

pub async fn handle_kafka_deploy(
    connector_name: String,
    config: FoundryConfig,
    cluster: String,
) -> Result<(), FFError> {
    let compiled = compile_kafka_connector(&config, &connector_name)?;
    let cluster = get_cluster_cfg(cluster, config)?;
    let client = KafkaConnectClient::new(&cluster.connect.host, &cluster.connect.port);

    client
        .deploy_connector::<KafkaConnector>(compiled)
        .await
        .map_err(FFError::compile)?;

    info!("Connector `{}` deployed successfully", connector_name);
    Ok(())
}
