CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS test_src_connector (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "postgres",
        "database.password" = "postgres",
        "database.port" = 5432,
        "database.hostname" = "postgres",
        "database.dbname" = "postgres",
        "table.include.list" = "public.orders",
        "snapshot.mode" = "initial",
        "kafka.bootstrap.servers" = "kafka_broker:9092",
        "topic.prefix" = "postgres-"
)
WITH PIPELINES(pii_pipeline);
