CREATE SOURCE KAFKA CONNECTOR KIND SINK IF NOT EXISTS film_rental_inventory_customer_payment_sink
USING KAFKA CLUSTER 'some_kafka_cluster' (
    "connector.class" =  "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max" = "1",
    "insert.mode" = "upsert",
    "topics.regex" = "postgres-(*)",
    "delete.enabled" = "false"
)
INTO WAREHOUSE DATABASE 'dvd_rental_analytics' USING SCHEMA 'bronze';