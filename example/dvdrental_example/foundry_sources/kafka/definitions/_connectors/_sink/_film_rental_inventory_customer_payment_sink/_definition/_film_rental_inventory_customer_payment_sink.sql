CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK IF NOT EXISTS film_rental_inventory_customer_payment_sink
USING KAFKA CLUSTER 'some_kafka_cluster' (
    "tasks.max" = "1",
    "insert.mode" = "insert",
    "delete.enabled" = "false",
    "topics.regex" = "dvdrental\.([^.]+)"
) WITH CONNECTOR VERSION '3.1'
INTO WAREHOUSE DATABASE 'dvd_rental_analytics' USING SCHEMA 'raw';