CREATE SIMPLE MESSAGE TRANSFORM reroute (
    type = 'io.debezium.transforms.ByLogicalTableRouter',
    "topic.regex" = '',
    "topic.replacement" = ''
);
