use common::types::sources::SourceConnArgs;
use database_adapters::postgres::PostgresAdapter;
use database_adapters::AsyncDatabaseAdapter;
use engine::Engine;
use tokio::time::{sleep, Duration};
use tokio_postgres::NoTls;

use crate::common_test_utils::setup_postgres;

mod common_test_utils;

#[cfg(test)]
mod tests {
    use crate::common_test_utils;
    use crate::common_test_utils::{
        setup_postgres, KafkaConnectTestClient, KAFKA_BROKER_PORT, PG_DB, PG_PASSWORD, PG_PORT,
        PG_USER,
    };
    use common::types::sources::SourceConnArgs;
    use common_test_utils::setup_kafka;
    use engine::kafka::{KafkaConnectClient};
    use engine::{Engine, EngineError};
    use std::time::Duration;
    use tokio::time::sleep;
    use tokio_postgres::NoTls;
    use engine::registry::Getter;
    use engine::types::KafkaConnectorType;

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
        let postgres_test_container = setup_postgres().await.unwrap();
        let kafka_test_containers = setup_kafka().await.unwrap();
        sleep(Duration::from_secs(5)).await;
        set_up_table(postgres_test_container.conn_string(false)).await;

        let con_name = "test";
        println!("{}", postgres_test_container.port);

        let sql = format!(
            r#"CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS {con_name} (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "{PG_USER}",
        "database.password" = "{PG_PASSWORD}",
        "database.port" = {},
        "database.hostname" = "{}",
        "database.dbname" = "{PG_DB}",
        "table.include.list" = "public.test_connector_src",
        "snapshot.mode" = "initial",
        "kafka.bootstrap.servers" = "{}",
        "topic.prefix" = "postgres-");"#,
            5432, postgres_test_container.docker_host, kafka_test_containers.kafka_broker.host
        );

        println!("{}", sql);

        let engine = Engine::new();
        let connect_host = format!("http://{}", kafka_test_containers.kafka_connect.host);
        let kafka_args = SourceConnArgs {
            kafka_connect: Some(connect_host.clone()),
        };
        let resp = engine.execute(sql.as_str(), &kafka_args, None).await?;
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
            kafka_test_containers.kafka_broker.host
        );
        assert_eq!(conn_config.config["topic.prefix"], "postgres-");
        Ok(())
    }

    #[tokio::test]
    async fn test_create_connector_with_pipeline() -> Result<(), EngineError> {
        let postgres_test_container = setup_postgres().await.unwrap();
        let kafka_test_containers = setup_kafka().await.unwrap();
        sleep(Duration::from_secs(5)).await;
        set_up_table(postgres_test_container.conn_string(false)).await;

        let con_name = "test";
        let sql = format!(
            r#"CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS {con_name} (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "{PG_USER}",
        "database.password" = "{PG_PASSWORD}",
        "database.port" = {},
        "database.hostname" = "{}",
        "database.dbname" = "{PG_DB}",
        "table.include.list" = "public.test_connector_src",
        "snapshot.mode" = "initial",
        "topic.prefix" = "postgres-",
        "kafka.bootstrap.servers" = "{}")
        WITH PIPELINES(pii_pipeline);"#,
            5432, postgres_test_container.docker_host, kafka_test_containers.kafka_broker.host
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
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
            )
            .await?;

        let smt_drop = r#"CREATE SIMPLE MESSAGE TRANSFORM drop_id (
            type = 'org.apache.kafka.connect.transforms.ReplaceField$Value',
            blacklist = 'id'
        );"#;
        engine
            .execute(
                smt_drop,
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
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
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
            )
            .await?;

        // ---- deploy connector --------------------------------------------
        let connect_host = format!("http://{}", kafka_test_containers.kafka_connect.host);
        let kafka_args = SourceConnArgs {
            kafka_connect: Some(connect_host.clone()),
        };
        engine.execute(sql.as_str(), &kafka_args, None).await?;

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
        let engine = Engine::new();
        let sql = r#"CREATE SIMPLE MESSAGE TRANSFORM cast_hash_cols_to_int (
  type      = 'org.apache.kafka.connect.transforms.Cast$Value',
  spec      = '${spec}',
  predicate = '${predicate}'
);"#;
        engine
            .execute(
                sql,
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
            )
            .await
            .unwrap();
        let smt = engine
            .catalog
            .get_kafka_smt("cast_hash_cols_to_int")
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
        let engine = Engine::new();

        engine
            .execute(
                "CREATE SIMPLE MESSAGE TRANSFORM hash_email (type = 'hash');",
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
            )
            .await
            .unwrap();

        engine
            .execute(
                "CREATE SIMPLE MESSAGE TRANSFORM drop_pii (type = 'drop');",
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
            )
            .await
            .unwrap();

        let sql = r#"
CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS some_pipeline SOURCE (
    hash_email(email_addr_reg = '.*@example.com'),
    drop_pii(fields = 'email_addr, phone_num')
) WITH PIPELINE PREDICATE 'some_predicate';
"#;
        engine
            .execute(
                sql,
                &SourceConnArgs {
                    kafka_connect: None,
                },
                None,
            )
            .await
            .unwrap();

        let pipe = engine
            .catalog
            .get_smt_pipeline("some_pipeline")
            .expect("pipeline exists");
        let t1 = engine.catalog.get_kafka_smt("hash_email").unwrap();
        let t2 = engine.catalog.get_kafka_smt("drop_pii").unwrap();
        assert_eq!(pipe.name, "some_pipeline");
        assert_eq!(pipe.transforms, vec![t1.id, t2.id]);
        assert_eq!(pipe.predicate.as_deref(), Some("some_predicate"));
    }
}

#[tokio::test]
async fn test_execute_db_query_against_postgres() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg = setup_postgres().await.unwrap();

    sleep(Duration::from_secs(5)).await;

    let adapter = PostgresAdapter::new(
        &pg.local_host,
        pg.port.parse().unwrap(),
        &pg.db_name,
        &pg.user,
        &pg.password,
    )
    .await
    .unwrap();
    let mut boxed: Box<dyn AsyncDatabaseAdapter> = Box::new(adapter);

    let engine = Engine::new();
    engine
        .execute(
            "CREATE TABLE ints (id INT);",
            &SourceConnArgs {
                kafka_connect: None,
            },
            Some(&mut boxed),
        )
        .await
        .unwrap();

    let (mut client, conn) = tokio_postgres::connect(&pg.conn_string(false), NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let row = client
        .query_one(
            "SELECT to_regclass('public.ints') IS NOT NULL AS exists",
            &[],
        )
        .await?;
    let exists: bool = row.get(0);
    assert!(exists);

    Ok(())
}
