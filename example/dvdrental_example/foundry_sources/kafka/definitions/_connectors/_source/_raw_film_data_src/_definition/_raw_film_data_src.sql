CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS raw_film_data_src
USING KAFKA CLUSTER 'some_kafka_cluster' (
    "tasks.max" = "1",
    "snapshot.mode" = "initial",
    "topic.prefix" = "postgres"
)
WITH CONNECTOR VERSION '3.1' AND PIPELINES(preset_pipe)
FROM SOURCE DATABASE 'dvd_rental';

