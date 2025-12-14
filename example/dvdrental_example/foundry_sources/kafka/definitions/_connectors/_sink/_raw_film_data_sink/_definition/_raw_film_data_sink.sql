CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK IF NOT EXISTS raw_film_data_sink
USING KAFKA CLUSTER 'some_kafka_cluster' (
    "tasks.max" = "1",
    "insert.mode" = "insert",
    "delete.enabled" = "false",
    "topics.regex" = "dvdrental\.([^.]+)"
) WITH CONNECTOR VERSION '3.1'
INTO WAREHOUSE DATABASE 'dvdrental_analytics' USING SCHEMA 'raw';