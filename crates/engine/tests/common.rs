use std::path::{Path, PathBuf};
use testcontainers::core::client::docker_client_instance;
use testcontainers::{
    core::{IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use engine::executor::kafka::KafkaConnectClient;
use engine::executor::ExecutorHost;

pub const PG_DB: &str = "foundry_dev";
pub const PG_USER: &str = "postgres";
pub const PG_PASSWORD: &str = "password";
pub const PG_HOST: &str = "0.0.0.0";
pub const PG_PORT: &str = "5432";

pub const KAFKA_BROKER_HOST: &str = "0.0.0.0";
pub const KAFKA_BROKER_PORT: &str = "9092";
pub const KAFKA_CONNECT_HOST: &str = "0.0.0.0";
pub const KAFKA_CONNECT_PORT: &str = "8083";

pub const NET: &str = "int-net";

pub struct PgTestContainer {
    pub container: ContainerAsync<GenericImage>,
    pub port: &'static str,
    pub db_name: &'static str,
    pub user: &'static str,
    pub password: &'static str,
    pub host: &'static str,
}

impl PgTestContainer {
    pub fn conn_string(&self) -> String {
        format!(
            "host={} user={} password={} dbname={} port={}",
            self.host, self.user, self.password, self.db_name, self.port
        )
    }
}

pub struct KafkaTestContainer {
    pub container: ContainerAsync<GenericImage>,
    pub host: String,
}

pub struct KafkaContainers {
    pub broker: KafkaTestContainer,
    pub connect: KafkaTestContainer,
}

pub struct TestContainers {
    pub postgres: PgTestContainer,
    pub kafka: KafkaContainers,
}

pub struct KafkaConnectTestClient {
    pub host: String,
}

impl ExecutorHost for KafkaConnectTestClient {
    fn host(&self) -> &str {
        &self.host
    }
}

impl KafkaConnectClient for KafkaConnectTestClient {}

fn plugins_host_path() -> PathBuf {
    let base_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    base_dir.join("tests").join("connect_plugins")
}

pub async fn setup_pg_container() -> Result<PgTestContainer, Box<dyn std::error::Error>> {
    let docker = docker_client_instance().await.unwrap();

    let postgres = GenericImage::new("postgres", "16")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_container_name("postgres")
        .with_env_var("POSTGRES_DB", PG_DB)
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .with_network(NET)
        .with_mapped_port(
            PG_PORT.parse::<u16>().unwrap(),
            PG_PORT.parse::<u16>().unwrap().tcp(),
        )
        .start()
        .await
        .unwrap();

    Ok(PgTestContainer {
        container: postgres,
        port: PG_PORT,
        db_name: PG_DB,
        user: PG_USER,
        password: PG_PASSWORD,
        host: PG_HOST,
    })
}

pub async fn setup_kafka_containers() -> Result<KafkaContainers, Box<dyn std::error::Error>> {
    let docker = docker_client_instance().await.unwrap();

    let kafka_broker = GenericImage::new("apache/kafka", "latest")
        .with_container_name("kafka-broker")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT},CONTROLLER://{KAFKA_BROKER_HOST}:9093"
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://kafka-broker:{KAFKA_BROKER_PORT}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka-broker:9093")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_NUM_PARTITIONS", "3")
        .with_network(NET)
        .start()
        .await
        .unwrap();

    let kafka_connect = GenericImage::new("confluentinc/cp-kafka-connect", "7.7.1")
        .with_wait_for(WaitFor::message_on_stdout(
            "INFO REST resources initialized; server is started and ready to handle requests",
        ))
        .with_container_name("kafka-connect")
        .with_mapped_port(
            KAFKA_CONNECT_PORT.parse::<u16>().unwrap(),
            KAFKA_CONNECT_PORT.parse::<u16>().unwrap().tcp(),
        )
        .with_network(NET)
        .with_mount(Mount::bind_mount(
            plugins_host_path().to_str().unwrap(),
            "/opt/kafka/plugins",
        ))
        .with_env_var(
            "CONNECT_BOOTSTRAP_SERVERS",
            format!("kafka-broker:{KAFKA_BROKER_PORT}"),
        )
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
        .await
        .unwrap();

    Ok(KafkaContainers {
        broker: KafkaTestContainer {
            container: kafka_broker,
            host: format!("kafka-broker:{KAFKA_BROKER_PORT}"),
        },
        connect: KafkaTestContainer {
            container: kafka_connect,
            host: format!("{KAFKA_CONNECT_HOST}:{KAFKA_CONNECT_PORT}"),
        },
    })
}

pub async fn setup_test_containers() -> Result<TestContainers, Box<dyn std::error::Error>> {
    Ok(TestContainers {
        postgres: setup_pg_container().await?,
        kafka: setup_kafka_containers().await?,
    })
}
