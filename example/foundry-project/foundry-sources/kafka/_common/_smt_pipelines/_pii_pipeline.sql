CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS pii_pipeline SOURCE (
    mask_field,
    drop_id
) WITH PIPELINE PREDICATE 'some_predicate';