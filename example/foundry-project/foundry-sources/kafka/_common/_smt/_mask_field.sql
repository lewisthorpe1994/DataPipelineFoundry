CREATE SIMPLE MESSAGE TRANSFORM mask_field (
    type = 'org.apache.kafka.connect.transforms.MaskField$Value',
    fields = 'name',
    replacement = 'X'
);