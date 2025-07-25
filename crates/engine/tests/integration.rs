const PG_DB: &str = "foundry_dev";
const PG_USER: &str = "postgres";
const PG_PASSWORD: &str = "password";
const PG_HOST: &str = "0.0.0.0";
const PG_PORT: &str = "5432";
const KAFKA_BROKER_HOST: &str = "0.0.0.0";
const KAFKA_BROKER_PORT: &str = "9092";
const KAFKA_CONNECT_HOST: &str = "0.0.0.0";
const KAFKA_CONNECT_PORT: &str = "8083";
const NET: &str = "int-net";

#[cfg(test)]
mod tests {
    use crate::{
        KAFKA_BROKER_HOST, KAFKA_BROKER_PORT, KAFKA_CONNECT_HOST, KAFKA_CONNECT_PORT, NET, PG_DB,
        PG_HOST, PG_PASSWORD, PG_PORT, PG_USER,
    };
    use common::types::sources::SourceConnArgs;
    use engine::executor::kafka::{KafkaConnectClient, KafkaConnectorType};
    use engine::executor::Executor;
    use engine::executor::ExecutorHost;
    use engine::registry::{Catalog, MemoryCatalog};
    use engine::{Engine, EngineError};
    use std::path::{Path, PathBuf};
    use testcontainers::core::client::docker_client_instance;
    use testcontainers::{
        core::{IntoContainerPort, Mount, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };
    use tokio_postgres::NoTls;

    struct PgTestContainer {
        container: ContainerAsync<GenericImage>,
        port: &'static str,
        db_name: &'static str,
        user: &'static str,
        password: &'static str,
        host: &'static str,
    }
    impl PgTestContainer {
        pub fn conn_string(&self) -> String {
            format!(
                "host={} user={} password={} dbname={} port={}",
                self.host, self.user, self.password, self.db_name, self.port
            )
        }
    }

    struct KafkaTestContainer {
        container: ContainerAsync<GenericImage>,
        host: String,
    }

    struct TestContainers {
        postgres: PgTestContainer,
        kafka_broker: KafkaTestContainer,
        kafka_connect: KafkaTestContainer,
    }

    struct KafkaConnectTestClient {
        host: String,
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

    async fn setup_test_containers() -> Result<TestContainers, Box<dyn std::error::Error>> {
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

        let kafka_broker = GenericImage::new("apache/kafka", "latest")
            .with_container_name("kafka-broker")
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
            .with_network(NET)
            .start()
            .await.unwrap();

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
            .await
            .unwrap();

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
                host: format!("kafka-broker:{KAFKA_BROKER_PORT}"),
            },
            kafka_connect: KafkaTestContainer {
                container: kafka_connect,
                host: format!("{KAFKA_CONNECT_HOST}:{KAFKA_CONNECT_PORT}"),
            },
        })
    }

    async fn set_up_table(pg_conn: String) {
        let (mut pg, pg_conn) = tokio_postgres::connect(&pg_conn, NoTls)
            .await
            .expect("connect to Postgres");
        // run the connection in the background
        tokio::spawn(async move {
            if let Err(e) = pg_conn.await {
                eprintln!("pg connection error: {e}");
            }
        });

        pg.batch_execute(
            "
        DROP TABLE IF EXISTS test_connector_src;
        CREATE TABLE test_connector_src (
            id   SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        INSERT INTO test_connector_src (name) VALUES ('alice'), ('bob');
        ",
        )
        .await
        .expect("prepare table");
    }

    #[tokio::test]
    async fn test_create_connector() -> Result<(), EngineError> {
        let test_containers = setup_test_containers().await.unwrap();
        set_up_table(test_containers.postgres.conn_string()).await;

        let con_name = "test";

        let sql = format!(
            r#"CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS {con_name} (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "{PG_USER}",
        "database.password" = "{PG_PASSWORD}",
        "database.port" = "{PG_PORT}",
        "database.hostname" = "postgres",
        "database.dbname" = "{PG_DB}",
        "table.include.list" = "public.test_connector_src",
        "snapshot.mode" = "initial",
        "kafka.bootstrap.servers" = "kafka-broker:{KAFKA_BROKER_PORT}",
        "topic.prefix" = "postgres-");"#
        );

        let engine = Engine::new();
        let connect_host = format!("http://{}", test_containers.kafka_connect.host);
        let kafka_args = SourceConnArgs {
            kafka_connect: Some(connect_host.clone()),
        };
        let resp = engine.execute(sql.as_str(), kafka_args, None).await?;
        let kafka_executor = KafkaConnectTestClient { host: connect_host };
        let conn_exists = kafka_executor.connector_exists(con_name).await.unwrap();
        assert!(conn_exists);
        let conn_config = kafka_executor.get_connector_config(con_name).await.unwrap();
        assert_eq!(conn_config.conn_type.unwrap(), KafkaConnectorType::Source);
        assert_eq!(
            conn_config.config["table.include.list"],
            "public.test_connector_src"
        );
        assert_eq!(conn_config.config["snapshot.mode"], "initial");
        assert_eq!(
            conn_config.config["kafka.bootstrap.servers"],
            format!("kafka-broker:{KAFKA_BROKER_PORT}")
        );
        assert_eq!(conn_config.config["topic.prefix"], "postgres-");
        Ok(())
    }

    #[tokio::test]
    async fn test_create_connector_with_pipeline() -> Result<(), EngineError> {
        let test_containers = setup_test_containers().await.unwrap();
        set_up_table(test_containers.postgres.conn_string()).await;

        let con_name = "test";
        let sql = format!(
            r#"CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS {con_name} (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "{PG_USER}",
        "database.password" = "{PG_PASSWORD}",
        "database.port" = "{PG_PORT}",
        "database.hostname" = "postgres",
        "database.dbname" = "{PG_DB}",
        "table.include.list" = "public.test_connector_src",
        "snapshot.mode" = "initial",
        "kafka.bootstrap.servers" = "kafka-broker:{KAFKA_BROKER_PORT}",
        "topic.prefix" = "postgres-")
        WITH PIPELINES(pii_pipeline);"#
        );

        let engine = Engine::new();

        // ---- create required transforms and pipeline ----------------------
        let smt_mask = r#"CREATE SIMPLE MESSAGE TRANSFORM mask_field (
            type = 'org.apache.kafka.connect.transforms.MaskField$Value',
            fields = 'name',
            replacement = 'X'
        );"#;
        engine
            .execute(
                smt_mask,
                SourceConnArgs {
                    kafka_connect: None,
                },
                None
            )
            .await?;

        let smt_drop = r#"CREATE SIMPLE MESSAGE TRANSFORM drop_id (
            type = 'org.apache.kafka.connect.transforms.ReplaceField$Value',
            blacklist = 'id'
        );"#;
        engine
            .execute(
                smt_drop,
                SourceConnArgs {
                    kafka_connect: None,
                },
                None
            )
            .await?;

        let pipe_sql = r#"
CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS pii_pipeline SOURCE (
    mask_field,
    drop_id
) WITH PIPELINE PREDICATE 'some_predicate';
"#;
        engine
            .execute(
                pipe_sql,
                SourceConnArgs {
                    kafka_connect: None,
                },
                None
            )
            .await?;

        // ---- deploy connector --------------------------------------------
        let connect_host = format!("http://{}", test_containers.kafka_connect.host);
        let kafka_args = SourceConnArgs {
            kafka_connect: Some(connect_host.clone()),
        };
        engine.execute(sql.as_str(), kafka_args, None).await?;

        let kafka_executor = KafkaConnectTestClient { host: connect_host };
        let conn_exists = kafka_executor.connector_exists(con_name).await.unwrap();
        assert!(conn_exists);
        let conn_config = kafka_executor.get_connector_config(con_name).await.unwrap();
        assert_eq!(conn_config.conn_type.unwrap(), KafkaConnectorType::Source);
        assert_eq!(
            conn_config.config["transforms"],
            "pii_pipeline_mask_field,pii_pipeline_drop_id"
        );
        assert_eq!(
            conn_config.config["transforms.pii_pipeline_mask_field.fields"],
            "name"
        );
        assert_eq!(
            conn_config.config["transforms.pii_pipeline_mask_field.replacement"],
            "X"
        );
        assert_eq!(
            conn_config.config["transforms.pii_pipeline_mask_field.predicate"],
            "some_predicate"
        );
        assert_eq!(
            conn_config.config["transforms.pii_pipeline_drop_id.blacklist"],
            "id"
        );
        assert_eq!(
            conn_config.config["transforms.pii_pipeline_drop_id.predicate"],
            "some_predicate"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_create_transform_via_executor() {
        let exec = Executor::new();
        let catalog = MemoryCatalog::new();
        let sql = r#"CREATE SIMPLE MESSAGE TRANSFORM cast_hash_cols_to_int (
  type      = 'org.apache.kafka.connect.transforms.Cast$Value',
  spec      = '${spec}',
  predicate = '${predicate}'
);"#;
        exec.execute(
            sql,
            &catalog,
            SourceConnArgs {
                kafka_connect: None,
            },
            None
        )
        .await
        .unwrap();
        let smt = catalog
            .get_transform("cast_hash_cols_to_int")
            .expect("transform exists");
        assert_eq!(smt.name, "cast_hash_cols_to_int");
        assert_eq!(
            smt.config["type"],
            "org.apache.kafka.connect.transforms.Cast$Value"
        );
        assert_eq!(smt.config["spec"], "${spec}");
        assert_eq!(smt.config["predicate"], "${predicate}");
    }

    #[tokio::test]
    async fn test_create_pipeline_via_executor() {
        let exec = Executor::new();
        let catalog = MemoryCatalog::new();

        exec.execute(
            "CREATE SIMPLE MESSAGE TRANSFORM hash_email (type = 'hash');",
            &catalog,
            SourceConnArgs {
                kafka_connect: None,
            },
            None
        )
        .await
        .unwrap();
        exec.execute(
            "CREATE SIMPLE MESSAGE TRANSFORM drop_pii (type = 'drop');",
            &catalog,
            SourceConnArgs {
                kafka_connect: None,
            },
            None
        )
        .await
        .unwrap();

        let sql = r#"
CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS some_pipeline SOURCE (
    hash_email(email_addr_reg = '.*@example.com'),
    drop_pii(fields = 'email_addr, phone_num')
) WITH PIPELINE PREDICATE 'some_predicate';
"#;
        exec.execute(
            sql,
            &catalog,
            SourceConnArgs {
                kafka_connect: None,
            },
            None
        )
        .await
        .unwrap();

        let pipe = catalog
            .get_pipeline("some_pipeline")
            .expect("pipeline exists");
        let t1 = catalog.get_transform("hash_email").unwrap();
        let t2 = catalog.get_transform("drop_pii").unwrap();
        assert_eq!(pipe.name, "some_pipeline");
        assert_eq!(pipe.transforms, vec![t1.id, t2.id]);
        assert_eq!(pipe.predicate.as_deref(), Some("some_predicate"));
    }
}
