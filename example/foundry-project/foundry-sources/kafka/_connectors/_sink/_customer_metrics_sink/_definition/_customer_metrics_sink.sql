CREATE SOURCE KAFKA CONNECTOR KIND SINK IF NOT EXISTS customer_orders_sink (
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max" = "1",
    "topics" = "postgres-.public.orders",
    "connection.url" = "jdbc:postgresql://postgres:5432/postgres",
    "connection.user" = "postgres",
    "connection.password" = "postgres",
    "insert.mode" = "upsert",
    "pk.mode" = "record_key",
    "pk.fields" = "customer_id",
    "table.name.format" = "bronze.raw_orders"
)
WITH PIPELINES(customer_metrics_sink_pipeline);
