CREATE KAFKA CONNECTOR KIND DEBEZIUM SOURCE IF NOT EXISTS film_rental_inventory_customer_payment_src
USING KAFKA CLUSTER 'some_kafka_cluster' (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "table.include.list" = "public.inventory, public.payment, public.rental, public.film, public.customer",
        "snapshot.mode" = "initial",
        "topic.prefix" = "postgres-"
)
WITH PIPELINES(unwrap_router)
FROM SOURCE DATABASE 'dvd_rental';