CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS unwrap_router (
    unwrap,
    reroute("topic.regex" = 'postgres-(*)', "topic.replacement" = "$2")
);
