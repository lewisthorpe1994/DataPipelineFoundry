CREATE SIMPLE MESSAGE TRANSFORM drop_id (
    type = 'org.apache.kafka.connect.transforms.ReplaceField$Value',
    blacklist = 'id'
);
