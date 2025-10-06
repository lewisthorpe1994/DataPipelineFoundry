CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS customer_metrics_sink_pipeline SINK (
    unwrap_after
);
