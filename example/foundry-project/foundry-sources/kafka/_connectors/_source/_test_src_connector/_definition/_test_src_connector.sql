CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS test_src_connector
USING KAFKA CLUSTER 'some_kafka_cluster' (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "postgres",
        "database.password" = "postgres",
        "database.port" = 5432,
        "database.hostname" = "postgres",
        "database.dbname" = "postgres",
        "table.include.list" = "public.orders",
        "snapshot.mode" = "initial",
        "topic.prefix" = "postgres-"
)
WITH PIPELINES(pii_pipeline);
