const PG_DB: &str = "foundry_dev";
const PG_USER: &str = "postgres";
const PG_PASSWORD: &str = "password";
const PG_HOST: &str = "0.0.0.0";
const PG_PORT: &str = "5432";
const KAFKA_BROKER_HOST: &str = "0.0.0.0";
const KAFKA_BROKER_PORT: &str = "9092";
const KAFKA_CONNECT_HOST: &str = "0.0.0.0";
const KAFKA_CONNECT_PORT: &str = "8083";
const KAFKA_CONNECT_PLUGINS_DIR: &str = "/absolute/host/connect_plugins";
const KAFKA_CONNECT_PLUGINS_INSTALL: &str = "confluentinc/kafka-connect-jdbc:latest";

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use testcontainers::{core::{
        WaitFor,
        IntoContainerPort,
        Mount,
    }, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
    use testcontainers::core::client::docker_client_instance;
    use crate::{KAFKA_BROKER_HOST, KAFKA_BROKER_PORT, KAFKA_CONNECT_HOST, KAFKA_CONNECT_PORT, PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER};

    struct PgTestContainer {
        container: ContainerAsync<GenericImage>,
        port: &'static str,
        db_name: &'static str,
        user: &'static str,
        password: &'static str,
        host: &'static str,
    }
    
    struct KafkaTestContainer {
        container: ContainerAsync<GenericImage>,
        host: &'static str,
    }
    
    struct TestContainers {
        postgres: PgTestContainer,
        kafka_broker: KafkaTestContainer,
        kafka_connect: KafkaTestContainer
    }
    
    async fn setup_test_containers() -> Result<TestContainers, Box<dyn std::error::Error>> {
        let docker = docker_client_instance().await.unwrap();
        
        let postgres = GenericImage::new("postgres", "16")
            .with_wait_for(WaitFor::message_on_stdout(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", PG_DB)
            .with_env_var("POSTGRES_USER", PG_USER)
            .with_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
            .with_network("pg_network")
            .with_mapped_port(PG_PORT.parse::<u16>().unwrap(), PG_PORT.parse::<u16>().unwrap().tcp())
            .start()
            .await.unwrap();

        let kafka_broker = GenericImage::new("apache/kafka", "latest")
            .with_name("kafka-broker")
            .with_env_var("KAFKA_NODE_ID", "1")
            .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
            .with_env_var(
                "KAFKA_LISTENERS", 
                format!("PLAINTEXT://{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT},CONTROLLER://{KAFKA_BROKER_HOST}:9093"
                ))
            .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!("PLAINTEXT://kafka-broker:{KAFKA_BROKER_PORT}"))
            .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka-broker:9093")
            .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .with_env_var("KAFKA_NUM_PARTITIONS", "3")
            .with_network("kafka_network")
            .start()
            .await.unwrap();

        let kafka_connect = GenericImage::new("confluentinc/cp-kafka-connect", "7.7.1")
            .with_wait_for(WaitFor::message_on_stdout(
                "Kafka started (kafka.server.KafkaServer)",
            ))
            .with_name("kafka-connect")
            .with_mapped_port(KAFKA_CONNECT_PORT.parse::<u16>().unwrap(), KAFKA_CONNECT_PORT.parse::<u16>().unwrap().tcp())
            .with_network("kafka_network")
            .with_mount(
                Mount::bind_mount(
                    "/absolute/host/connect_plugins",
                    "/opt/kafka/plugins",
                )
            )
            .with_env_var("CONNECT_BOOTSTRAP_SERVERS", format!("kafka-broker:{KAFKA_BROKER_PORT}"))
            .with_env_var("CONNECT_REST_ADVERTISED_HOST_NAME", "kafka-connect")
            .with_env_var("CONNECT_GROUP_ID", "compose-connect-group")
            .with_env_var("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
            .with_env_var("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
            .with_env_var("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
            .with_env_var("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .with_env_var("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .with_env_var(
                "CONNECT_PLUGIN_PATH",
                "/usr/share/java,/usr/share/confluent-hub-components,/opt/kafka/plugins",
            )
            .with_env_var("CONFLUENT_PLUGINS_INSTALL", "confluentinc/kafka-connect-jdbc:latest")
            .start()
            .await.unwrap();
        
        Ok(TestContainers { 
            postgres: PgTestContainer {
                container: postgres,
                port: PG_PORT,
                db_name: PG_DB,
                user: PG_USER,
                password: PG_PASSWORD,
                host: PG_HOST,
            }, 
            kafka_broker: KafkaTestContainer {
                container: kafka_broker,
                host: format!("kafka-broker:{KAFKA_BROKER_PORT}").as_str(),
            }, 
            kafka_connect: KafkaTestContainer {
                container: kafka_connect,
                host: format!("{KAFKA_CONNECT_HOST}:{KAFKA_CONNECT_PORT}").as_str(),
            }})
    }

    #[tokio::test]
    async fn test_create_connector() {
        let docker = docker_client_instance().await.unwrap();

        let pg = GenericImage::new("postgres", "16")
            .with_wait_for(WaitFor::message_on_stdout(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", "foundry_dev")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "password")
            .with_network("pg_network")

            .start()
            .await.unwrap();

        let kafka_broker = GenericImage::new("apache/kafka", "latest")
            .with_name("kafka-broker")
            .with_env_var("KAFKA_NODE_ID", "1")
            .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
            .with_env_var("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
            .with_env_var("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka-broker:9092")
            .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka-broker:9093")
            .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .with_env_var("KAFKA_NUM_PARTITIONS", "3")
            .with_network("kafka_network")
            .start()
            .await.unwrap();

        let kafka_connect = GenericImage::new("confluentinc/cp-kafka-connect", "7.7.1")
            .with_wait_for(WaitFor::message_on_stdout(
                "Kafka started (kafka.server.KafkaServer)",
            ))
            .with_name("kafka-connect")
            .with_mapped_port(8083, 8083.tcp())
            .with_network("kafka_network")
            .with_mount(
                Mount::bind_mount(
                    "/absolute/host/connect_plugins",           
                    "/opt/kafka/plugins",                      
                )
            )
            .with_env_var("CONNECT_BOOTSTRAP_SERVERS", "kafka-broker:9092")
            .with_env_var("CONNECT_REST_ADVERTISED_HOST_NAME", "kafka-connect")
            .with_env_var("CONNECT_GROUP_ID", "compose-connect-group")
            .with_env_var("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
            .with_env_var("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
            .with_env_var("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
            .with_env_var("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .with_env_var("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .with_env_var(
                "CONNECT_PLUGIN_PATH",
                "/usr/share/java,/usr/share/confluent-hub-components,/opt/kafka/plugins",
            )
            .with_env_var("CONFLUENT_PLUGINS_INSTALL", "confluentinc/kafka-connect-jdbc:latest");

        let sql = r#"CREATE SIMPLE MESSAGE TRANSFORM cast_hash_cols_to_int (
  type      = 'org.apache.kafka.connect.transforms.Cast$Value',
  spec      = '${spec}',
  predicate = '${predicate}'
);"#;
    }
}