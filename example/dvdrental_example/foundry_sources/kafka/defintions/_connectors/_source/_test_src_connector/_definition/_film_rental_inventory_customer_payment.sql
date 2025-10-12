CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS film_rental_inv_cust_pymt
USING KAFKA CLUSTER 'some_kafka_cluster' (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "table.include.list" = "public.inventory, public.payment, public.rental, public.film, public.customer",
        "snapshot.mode" = "initial",
        "topic.prefix" = "postgres-"
)
WITH PIPELINES(pii_pipeline)
FROM SOURCE DATABASE 'dvd_rentals';