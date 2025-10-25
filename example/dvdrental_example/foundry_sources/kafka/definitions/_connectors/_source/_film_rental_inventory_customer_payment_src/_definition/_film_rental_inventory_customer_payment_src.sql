CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS film_rental_inventory_customer_payment_src
USING KAFKA CLUSTER 'some_kafka_cluster' (
    "tasks.max" = "1",
    "snapshot.mode" = "initial",
    "topic.prefix" = "postgres-"
)
WITH CONNECTOR VERSION '3.1' AND PIPELINES(unwrap_router)
FROM SOURCE DATABASE 'dvd_rental';

