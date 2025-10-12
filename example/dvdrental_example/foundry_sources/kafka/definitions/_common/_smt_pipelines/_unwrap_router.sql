CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS pii_pipeline SOURCE (
    unwrap,
    reroute("topic.regex" = 'postgres-(*)', "topic.replacement" = "$2")
) WITH PIPELINE PREDICATE 'some_predicate';
