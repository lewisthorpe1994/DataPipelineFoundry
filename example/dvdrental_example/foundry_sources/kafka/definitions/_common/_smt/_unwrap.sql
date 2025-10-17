CREATE KAFKA SIMPLE MESSAGE TRANSFORM unwrap (
    type = 'io.debezium.transforms.ExtractNewRecordState',
    "delete.tombstone.handling.mode" = 'tombstone',
    "delete.handling.mode" = 'drop'
);
