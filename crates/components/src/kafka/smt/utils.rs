use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use crate::smt::transforms::custom::Custom;
use crate::smt::transforms::debezium::{
    ByLogicalTableRouter, ContentBasedRouter, DecodeLogicalDecodingMessageContent,
    ExtractNewRecordState, Filter, HeaderToValue, OutboxEventRouter, PartitionRouting,
    TimezoneConverter,
};
use crate::smt::{SmtClass, SmtKind, SmtPreset, Transform};
use crate::version_consts::DBZ_VERSION_3_3;
use common::error::DiagnosticMessage;
use connector_versioning::Version;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use thiserror::Error;

fn predicate_of(kind: &SmtKind) -> Option<&PredicateRef> {
    match kind {
        SmtKind::ExtractNewRecordState(smt) => smt.predicate.as_ref(),
        SmtKind::ByLogicalTableRouter(smt) => smt.predicate.as_ref(),
        SmtKind::Custom(smt) => smt.predicate.as_ref(),
        SmtKind::Filter(smt) => smt.predicate.as_ref(),
        SmtKind::HeaderToValue(smt) => smt.predicate.as_ref(),
        SmtKind::OutboxEventRouter(smt) => smt.predicate.as_ref(),
        SmtKind::PartitionRouting(smt) => smt.predicate.as_ref(),
        SmtKind::TimezoneConverter(smt) => smt.predicate.as_ref(),
        SmtKind::ContentBasedRouter(smt) => smt.predicate.as_ref(),
        SmtKind::DecodeLogicalMessageContent(smt) => smt.predicate.as_ref(),
    }
}

pub fn builtin_preset_config(name: &str) -> Option<HashMap<String, String>> {
    let preset = SmtPreset::try_from(name).ok()?;

    let mut map = HashMap::new();

    match preset {
        SmtPreset::DebeziumExtractNewRecordState => {
            map.insert(
                "type".to_string(),
                SmtClass::ExtractNewRecordState.class_name().to_string(),
            );
            map.insert("drop.tombstones".to_string(), "true".to_string());
            map.insert("delete.handling.mode".to_string(), "rewrite".to_string());
        }
        SmtPreset::DebeziumByLogicalTableRouter => {
            map.insert(
                "type".to_string(),
                SmtClass::ByLogicalTableRouter.class_name().to_string(),
            );
        }
    }

    Some(map)
}

pub fn build_transform_from_config(
    name: impl Into<String>,
    mut config: HashMap<String, String>,
    predicate: Option<PredicateRef>,
    version: Version,
) -> Result<Transform, TransformBuildError> {
    let transform_name = name.into();
    let class = config
        .remove("type")
        .ok_or_else(|| TransformBuildError::missing_type(transform_name.clone()))?;

    match class.as_str() {
        "io.debezium.transforms.ExtractNewRecordState" => {
            let mut smt = ExtractNewRecordState::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::ExtractNewRecordState(smt),
            })
        }
        "io.debezium.transforms.ByLogicalTableRouter" => {
            let mut smt = ByLogicalTableRouter::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::ByLogicalTableRouter(smt),
            })
        }
        "io.debezium.transforms.ContentBasedRouter" => {
            let mut smt = ContentBasedRouter::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::ContentBasedRouter(smt),
            })
        }
        "io.debezium.transforms.DecodeLogicalDecodingMessageContent" => {
            let mut smt = DecodeLogicalDecodingMessageContent::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::DecodeLogicalMessageContent(smt),
            })
        }
        "io.debezium.transforms.Filter" => {
            let mut smt = Filter::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::Filter(smt),
            })
        }
        "io.debezium.transforms.HeaderToValue" => {
            let mut smt = HeaderToValue::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::HeaderToValue(smt),
            })
        }
        "io.debezium.transforms.OutboxEventRouter" => {
            let mut smt = OutboxEventRouter::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::OutboxEventRouter(smt),
            })
        }
        "io.debezium.transforms.PartitionRouting" => {
            let mut smt = PartitionRouting::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::PartitionRouting(smt),
            })
        }
        "io.debezium.transforms.TimezoneConverter" => {
            let mut smt = TimezoneConverter::new(config, version)?;
            smt.predicate = predicate;
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::TimezoneConverter(smt),
            })
        }
        _ => {
            let mut props = config;
            props.insert("type".to_string(), class.clone());
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::Custom(Custom {
                    class,
                    props,
                    predicate,
                }),
            })
        }
    }
}

/* ---------- A list of transforms ---------- */
#[derive(Debug, Clone, Default)]
pub struct Transforms(pub Vec<Transform>);

/* ---------- Serialize Transforms into flat Kafka Connect keys ---------- */
impl Serialize for Transforms {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut flat = BTreeMap::<String, Value>::new();

        let names = self
            .0
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        flat.insert("transforms".to_string(), Value::String(names));

        for transform in &self.0 {
            let prefix = format!("transforms.{}.", transform.name);
            flat.insert(
                format!("{prefix}type"),
                Value::String(transform.kind.class_name().into_owned()),
            );

            match &transform.kind {
                SmtKind::ExtractNewRecordState(smt) => populate_extract(&mut flat, &prefix, smt),
                SmtKind::ByLogicalTableRouter(smt) => populate_route(&mut flat, &prefix, smt),
                SmtKind::ContentBasedRouter(smt) => {
                    populate_content_based_router(&mut flat, &prefix, smt)
                }
                SmtKind::DecodeLogicalMessageContent(smt) => {
                    populate_decode_logical_message_content(&mut flat, &prefix, smt)
                }
                SmtKind::Filter(smt) => {
                    insert_optional(&mut flat, &prefix, "topic.regex", &smt.topic_regex);
                    insert_optional(&mut flat, &prefix, "language", &smt.language);
                    insert_optional(&mut flat, &prefix, "condition", &smt.condition);
                    insert_optional(&mut flat, &prefix, "expression", &smt.expression);
                    insert_optional(
                        &mut flat,
                        &prefix,
                        "null.handling.mode",
                        &smt.null_handling_mode,
                    );
                }
                SmtKind::HeaderToValue(smt) => {
                    insert_optional(&mut flat, &prefix, "headers", &smt.headers);
                    insert_optional(&mut flat, &prefix, "fields", &smt.fields);
                    insert_optional(&mut flat, &prefix, "operation", &smt.operation);
                }
                SmtKind::OutboxEventRouter(smt) => {
                    insert_optional(&mut flat, &prefix, "id", &smt.id);
                    insert_optional(&mut flat, &prefix, "aggregatetype", &smt.aggregatetype);
                    insert_optional(&mut flat, &prefix, "aggregateid", &smt.aggregateid);
                    insert_optional(&mut flat, &prefix, "payload", &smt.payload);
                }
                SmtKind::PartitionRouting(smt) => {
                    insert_optional(
                        &mut flat,
                        &prefix,
                        "partition.payload.fields",
                        &smt.partition_payload_fields,
                    );
                    insert_optional(
                        &mut flat,
                        &prefix,
                        "partition.topic.num",
                        &smt.partition_topic_num,
                    );
                    insert_optional(
                        &mut flat,
                        &prefix,
                        "partition.hash.function",
                        &smt.partition_hash_function,
                    );
                }
                SmtKind::TimezoneConverter(smt) => {
                    insert_optional(
                        &mut flat,
                        &prefix,
                        "converted.timezone",
                        &smt.converted_timezone,
                    );
                    insert_optional(&mut flat, &prefix, "include.list", &smt.include_list);
                    insert_optional(&mut flat, &prefix, "exclude.list", &smt.exclude_list);
                }
                SmtKind::Custom(custom) => populate_custom(&mut flat, &prefix, custom),
            }

            if let Some(pred) = predicate_of(&transform.kind) {
                pred.write_flat_to_map(&mut flat, &prefix);
            }
        }

        flat.serialize(serializer)
    }
}

fn insert_optional<T>(map: &mut BTreeMap<String, Value>, prefix: &str, key: &str, value: &Option<T>)
where
    T: Serialize,
{
    if let Some(ref v) = value {
        if let Ok(json) = serde_json::to_value(v) {
            map.insert(format!("{prefix}{key}"), json);
        }
    }
}

fn populate_extract(map: &mut BTreeMap<String, Value>, prefix: &str, smt: &ExtractNewRecordState) {
    insert_optional(map, prefix, "drop.tombstones", &smt.drop_tombstones);
    insert_optional(
        map,
        prefix,
        "delete.handling.mode",
        &smt.delete_handling_mode,
    );
    insert_optional(
        map,
        prefix,
        "delete.handling.tombstone.mode",
        &smt.delete_handling_tombstone_mode,
    );
    insert_optional(map, prefix, "add.headers", &smt.add_headers);
    insert_optional(map, prefix, "route.by.field", &smt.route_by_field);
    insert_optional(map, prefix, "add.fields.prefix", &smt.add_fields_prefix);
    insert_optional(map, prefix, "add.fields", &smt.add_fields);
    insert_optional(map, prefix, "add.headers.prefix", &smt.add_headers_prefix);
    insert_optional(
        map,
        prefix,
        "drop.fields.header.name",
        &smt.drop_fields_header_name,
    );
    insert_optional(
        map,
        prefix,
        "drop.fields.from.key",
        &smt.drop_fields_from_key,
    );
    insert_optional(
        map,
        prefix,
        "drop.fields.keep.schema.compatible",
        &smt.drop_fields_keep_schema_compatible,
    );
    insert_optional(
        map,
        prefix,
        "replace.null.with.default",
        &smt.replace_null_with_default,
    );
}

fn populate_route(map: &mut BTreeMap<String, Value>, prefix: &str, smt: &ByLogicalTableRouter) {
    insert_optional(map, prefix, "topic.regex", &smt.topic_regex);
    insert_optional(map, prefix, "topic.replacement", &smt.topic_replacement);
    insert_optional(
        map,
        prefix,
        "key.enforce.uniqueness",
        &smt.key_enforce_uniqueness,
    );
    insert_optional(map, prefix, "key.field.name", &smt.key_field_name);
    insert_optional(map, prefix, "key.field.regex", &smt.key_field_regex);
    insert_optional(
        map,
        prefix,
        "key.field.replacement",
        &smt.key_field_replacement,
    );
    insert_optional(
        map,
        prefix,
        "schema.name.adjustment.mode",
        &smt.schema_name_adjustment_mode,
    );
    insert_optional(
        map,
        prefix,
        "logical.table.cache.size",
        &smt.logical_table_cache_size,
    );
}

fn populate_content_based_router(
    map: &mut BTreeMap<String, Value>,
    prefix: &str,
    smt: &ContentBasedRouter,
) {
    insert_optional(map, prefix, "topic.regex", &smt.topic_regex);
    insert_optional(map, prefix, "language", &smt.language);
    insert_optional(map, prefix, "topic.expression", &smt.topic_expression);
    insert_optional(map, prefix, "null.handling.mode", &smt.null_handling_mode);
}

fn populate_decode_logical_message_content(
    map: &mut BTreeMap<String, Value>,
    prefix: &str,
    smt: &DecodeLogicalDecodingMessageContent,
) {
    insert_optional(map, prefix, "fields.null.include", &smt.converted_timezone);
}

fn populate_filter(map: &mut BTreeMap<String, Value>, prefix: &str, smt: &Filter) {
    insert_optional(map, prefix, "topic.regex", &smt.topic_regex);
    insert_optional(map, prefix, "language", &smt.language);
    insert_optional(map, prefix, "condition", &smt.condition);
    insert_optional(map, prefix, "expression", &smt.expression);
    insert_optional(map, prefix, "null.handling.mode", &smt.null_handling_mode);
}

fn populate_header_to_value(map: &mut BTreeMap<String, Value>, prefix: &str, smt: &HeaderToValue) {
    insert_optional(map, prefix, "headers", &smt.headers);
    insert_optional(map, prefix, "fields", &smt.fields);
    insert_optional(map, prefix, "operation", &smt.operation);
}

fn populate_outbox_event_router(
    map: &mut BTreeMap<String, Value>,
    prefix: &str,
    smt: &OutboxEventRouter,
) {
    insert_optional(map, prefix, "id", &smt.id);
    insert_optional(map, prefix, "aggregatetype", &smt.aggregatetype);
    insert_optional(map, prefix, "aggregateid", &smt.aggregateid);
    insert_optional(map, prefix, "payload", &smt.payload);
}

fn populate_partition_routing(
    map: &mut BTreeMap<String, Value>,
    prefix: &str,
    smt: &PartitionRouting,
) {
    insert_optional(
        map,
        prefix,
        "partition.payload.fields",
        &smt.partition_payload_fields,
    );
    insert_optional(map, prefix, "partition.topic.num", &smt.partition_topic_num);
    insert_optional(
        map,
        prefix,
        "partition.hash.function",
        &smt.partition_hash_function,
    );
}

fn populate_timezone_converter(
    map: &mut BTreeMap<String, Value>,
    prefix: &str,
    smt: &TimezoneConverter,
) {
    insert_optional(map, prefix, "converted.timezone", &smt.converted_timezone);
    insert_optional(map, prefix, "include.list", &smt.include_list);
    insert_optional(map, prefix, "exclude.list", &smt.exclude_list);
}

fn populate_custom(map: &mut BTreeMap<String, Value>, prefix: &str, custom: &Custom) {
    for (key, value) in &custom.props {
        if key == "type" {
            continue;
        }
        map.insert(format!("{prefix}{key}"), Value::String(value.clone()));
    }
}

pub trait Transformable {
    fn set_transforms(&mut self, transforms: &Option<Transforms>);
}
