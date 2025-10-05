use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::Mutex;
use std::path::{Path, PathBuf};
use std::time::Duration;
use testcontainers::core::client::docker_client_instance;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;
/// Global mutex to serialize tests that modify the process working directory.
/// Changing the directory concurrently can lead to nondeterministic failures.
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Connection details used when writing `connections.yml` for a test project.
pub fn get_root_dir() -> PathBuf {
    let workspace_root = std::env::var("CARGO_WORKSPACE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .ancestors()
                .nth(2)
                .expect("crate should live under <workspace>/crates/<crate>")
                .to_path_buf()
        });
    let root = workspace_root.join("example/foundry-project");

    root
}

pub fn build_test_layers(root: PathBuf) -> HashMap<String, String> {
    // Build the layer map exactly as the loader does
    let layers = HashMap::from([
        (
            "bronze".to_string(),
            root.join("foundry_models/bronze").display().to_string(),
        ),
        (
            "silver".to_string(),
            root.join("foundry_models/silver").display().to_string(),
        ),
        (
            "gold".to_string(),
            root.join("foundry_models/gold").display().to_string(),
        ),
    ]);
    layers
}

/// Temporarily change the current working directory for the duration of the closure.
/// Guards against concurrent `chdir` calls by taking the global `TEST_MUTEX` lock.
/// Always restores the original directory, even if the closure panics.
pub fn with_chdir<F, T>(target: impl AsRef<Path>, f: F) -> std::io::Result<T>
where
    F: FnOnce() -> T,
{
    let _lock = TEST_MUTEX.lock().unwrap();

    let original = env::current_dir()?;
    env::set_current_dir(target.as_ref())?;

    struct Reset(PathBuf);
    impl Drop for Reset {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.0);
        }
    }
    let _guard = Reset(original);

    Ok(f())
}

pub async fn with_chdir_async<F, Fut, T>(target: impl AsRef<Path>, f: F) -> std::io::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let _lock = TEST_MUTEX.lock().unwrap();

    let original = env::current_dir()?;
    env::set_current_dir(target.as_ref())?;

    struct Reset(PathBuf);
    impl Drop for Reset {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.0);
        }
    }
    let _guard = Reset(original);

    Ok(f().await)
}


pub const PG_DB: &str = "postgres";
pub const PG_USER: &str = "postgres";
pub const PG_PASSWORD: &str = "postgres";
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
            host, self.user, self.password, self.db_name, self.port
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

fn plugins_host_path() -> PathBuf {
    // Always relative to current project root
    let base_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    base_dir.join("tests").join("connect_plugins")
}

pub async fn setup_postgres() -> Result<PgTestContainer, Box<dyn std::error::Error>> {
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
        .with_mapped_port(0, 5432u16.tcp())
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
            format!(
                "PLAINTEXT://{}:{KAFKA_BROKER_PORT},CONTROLLER://{}:9093",
                &broker_name, &broker_name
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://{}:{KAFKA_BROKER_PORT}", &broker_name),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var(
            "KAFKA_CONTROLLER_QUORUM_VOTERS",
            format!("1@{}:9093", &broker_name),
        )
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
        .with_mapped_port(0, 8083u16.tcp())
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
