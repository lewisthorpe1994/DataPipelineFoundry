use std::path::{Path, PathBuf};
use std::time::Duration;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers::core::client::docker_client_instance;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use uuid::Uuid;
use engine::executor::ExecutorHost;
use engine::executor::kafka::KafkaConnectClient;

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
pub const NET_HOST: &str = "127.0.0.1";

pub struct PgTestContainer {
    pub container: ContainerAsync<GenericImage>,
    pub port: String,
    pub db_name: &'static str,
    pub user: &'static str,
    pub password: &'static str,
    pub docker_host: String,
    pub local_host: &'static str,
}
impl PgTestContainer {
    pub fn conn_string(&self, docker: bool) -> String {
        let host = if docker {
            self.docker_host.clone()
        } else {
            self.local_host.to_string()
        };

        format!(
            "host={} user={} password={} dbname={} port={}",
            host , self.user, self.password, self.db_name, self.port
        )
    }
}

pub struct KafkaTestContainer {
    pub container: ContainerAsync<GenericImage>,
    pub host: String,
}

pub struct KafkaTestContainers {
    pub kafka_broker: KafkaTestContainer,
    pub kafka_connect: KafkaTestContainer,
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
    // Always relative to current project root
    let base_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    base_dir.join("tests").join("connect_plugins")
}

pub async fn setup_postgres() -> Result<PgTestContainer,  Box<dyn std::error::Error>> {
    let id = Uuid::new_v4();
    let name = format!("postgres-{}", id);
    let postgres = GenericImage::new("postgres", "16")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_container_name(&name)
        .with_env_var("POSTGRES_DB", PG_DB)
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .with_network(NET)
        .with_mapped_port(
            0,
            5432u16.tcp()
        )
        .start()
        .await?;

    let pg_port = postgres.get_host_port_ipv4(5432).await?.to_string();

    Ok(PgTestContainer {
        container: postgres,
        port: pg_port,
        db_name: PG_DB,
        user: PG_USER,
        password: PG_PASSWORD,
        docker_host: name,
        local_host: NET_HOST,
    })
}

pub async fn setup_kafka() -> Result<KafkaTestContainers, Box<dyn std::error::Error>> {
    let id = Uuid::new_v4();
    let connect_name = format!("kafka-connect-{}", id);
    let broker_name = format!("kafka-broker-{}", id);

    let kafka_broker = GenericImage::new("apache/kafka", "latest")
        .with_container_name(&broker_name)
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!("PLAINTEXT://{}:{KAFKA_BROKER_PORT},CONTROLLER://{}:9093", &broker_name, &broker_name
            ))
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!("PLAINTEXT://{}:{KAFKA_BROKER_PORT}", &broker_name))
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", format!("1@{}:9093", &broker_name))
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_NUM_PARTITIONS", "3")
        .with_network(NET)
        .start()
        .await?;

    let kafka_connect = GenericImage::new("confluentinc/cp-kafka-connect", "7.7.1")
        .with_wait_for(WaitFor::message_on_stdout(
            "INFO REST resources initialized; server is started and ready to handle requests",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_container_name(&connect_name)
        .with_mapped_port(
            0,
            8083u16.tcp(),
        )
        .with_network(NET)
        .with_mount(Mount::bind_mount(
            plugins_host_path().to_str().unwrap(),
            "/opt/kafka/plugins",
        ))
        .with_env_var(
            "CONNECT_BOOTSTRAP_SERVERS",
            format!("{}:{KAFKA_BROKER_PORT}", &broker_name),
        )
        .with_env_var("CONNECT_REST_ADVERTISED_HOST_NAME", &connect_name)
        .with_env_var("CONNECT_GROUP_ID", "compose-connect-group")
        .with_env_var("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
        .with_env_var("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
        .with_env_var("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
        .with_env_var("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
        .with_env_var("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
        .with_env_var("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
        .with_env_var(
            "CONNECT_KEY_CONVERTER",
            "org.apache.kafka.connect.json.JsonConverter",
        )
        .with_env_var(
            "CONNECT_VALUE_CONVERTER",
            "org.apache.kafka.connect.json.JsonConverter",
        )
        .with_env_var(
            "CONNECT_PLUGIN_PATH",
            "/usr/share/java,/usr/share/confluent-hub-components,/opt/kafka/plugins",
        )
        .with_env_var(
            "CONFLUENT_PLUGINS_INSTALL",
            "confluentinc/kafka-connect-jdbc:latest",
        )
        .start()
        .await?;
    let connect_rest_port = kafka_connect.get_host_port_ipv4(8083).await?;

    Ok(KafkaTestContainers {
        kafka_broker: KafkaTestContainer {
            container: kafka_broker,
            host: format!("{}:{KAFKA_BROKER_PORT}", broker_name),
        },
        kafka_connect: KafkaTestContainer {
            container: kafka_connect,
            host: format!("{KAFKA_CONNECT_HOST}:{connect_rest_port}"),
        },
    })
}