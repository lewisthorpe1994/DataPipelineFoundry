CREATE KAFKA SIMPLE MESSAGE TRANSFORM reroute
PRESET debezium.by_logical_table_router
EXTEND ("topic.regex" = 'postgres-(*)', "topic.replacement" = "$2");
