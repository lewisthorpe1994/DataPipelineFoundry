CREATE SIMPLE MESSAGE TRANSFORM unwrap_after (
    "type" = 'io.debezium.transforms.ExtractNewRecordState',
    "drop.tombstones" = 'true',
    "delete.handling.mode" = 'rewrite'
);
