use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use crate::smt::transforms::custom::Custom;
use crate::smt::transforms::debezium::{ByLogicalTableRouter, ExtractNewRecordState};
use crate::smt::{SmtClass, SmtKind, SmtPreset, Transform};
use crate::version_consts::DBZ_VERSION_3_3;
use common::error::DiagnosticMessage;
use connector_versioning::Version;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::borrow::Cow;
use std::collections::HashMap;
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
        // We emit:
        // transforms: "unwrap,route"
        // transforms.unwrap.type: "..."
        // transforms.unwrap.<prop>: "..."
        // transforms.route.type: "..."
        // transforms.route.<prop>: "..."
        let mut map = serializer.serialize_map(None)?;

        // 1) transforms: comma-separated names
        let names = self
            .0
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        map.serialize_entry("transforms", &names)?;

        // 2) per-transform config
        for t in &self.0 {
            let prefix = format!("transforms.{}.", t.name);

            // type
            map.serialize_entry(&(prefix.clone() + "type"), &t.kind.class_name())?;

            // fields by SMT kind
            match &t.kind {
                SmtKind::ExtractNewRecordState(smt) => {
                    if let Some(v) = smt.drop_tombstones {
                        map.serialize_entry(&(prefix.clone() + "drop.tombstones"), &v)?;
                    }
                    if let Some(v) = smt.delete_handling_mode.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "delete.handling.mode"), v)?;
                    }
                    if let Some(v) = smt.delete_handling_tombstone_mode.as_ref() {
                        map.serialize_entry(
                            &(prefix.clone() + "delete.handling.tombstone.mode"),
                            v,
                        )?;
                    }
                    if let Some(v) = smt.add_headers.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "add.headers"), v)?;
                    }
                    if let Some(v) = smt.route_by_field.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "route.by.field"), v)?;
                    }
                    if let Some(v) = smt.add_fields_prefix.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "add.fields.prefix"), v)?;
                    }
                    if let Some(v) = smt.add_fields.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "add.fields"), v)?;
                    }
                    if let Some(v) = smt.add_headers_prefix.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "add.headers.prefix"), v)?;
                    }
                    if let Some(v) = smt.drop_fields_header_name.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "drop.fields.header.name"), v)?;
                    }
                    if let Some(v) = smt.drop_fields_from_key {
                        map.serialize_entry(&(prefix.clone() + "drop.fields.from.key"), &v)?;
                    }
                    if let Some(v) = smt.drop_fields_keep_schema_compatible {
                        map.serialize_entry(
                            &(prefix.clone() + "drop.fields.keep.schema.compatible"),
                            &v,
                        )?;
                    }
                    if let Some(v) = smt.replace_null_with_default {
                        map.serialize_entry(&(prefix.clone() + "replace.null.with.default"), &v)?;
                    }
                    if let Some(pred) = predicate_of(&t.kind) {
                        pred.write_flat(&mut map, &prefix)?;
                    }
                }
                SmtKind::ByLogicalTableRouter(smt) => {
                    if let Some(v) = smt.topic_regex.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "topic.regex"), v)?;
                    }
                    if let Some(v) = smt.topic_replacement.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "topic.replacement"), v)?;
                    }
                    if let Some(v) = smt.key_enforce_uniqueness {
                        map.serialize_entry(&(prefix.clone() + "key.enforce.uniqueness"), &v)?;
                    }
                    if let Some(v) = smt.key_field_name.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "key.field.name"), v)?;
                    }
                    if let Some(v) = smt.key_field_regex.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "key.field.regex"), v)?;
                    }
                    if let Some(v) = smt.key_field_replacement.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "key.field.replacement"), v)?;
                    }
                    if let Some(v) = smt.schema_name_adjustment_mode.as_ref() {
                        map.serialize_entry(&(prefix.clone() + "schema.name.adjustment.mode"), v)?;
                    }
                    if let Some(v) = smt.logical_table_cache_size {
                        map.serialize_entry(&(prefix.clone() + "logical.table.cache.size"), &v)?;
                    }
                    if let Some(pred) = predicate_of(&t.kind) {
                        pred.write_flat(&mut map, &prefix)?;
                    }
                }
                SmtKind::Custom(custom) => {
                    for (k, v) in &custom.props {
                        if k == "type" {
                            continue;
                        }
                        map.serialize_entry(&(prefix.clone() + k), v)?;
                    }
                    if let Some(pred) = predicate_of(&t.kind) {
                        pred.write_flat(&mut map, &prefix)?;
                    }
                }
            }
        }

        map.end()
    }
}

pub trait Transformable {
    fn set_transforms(&mut self, transforms: &Option<Transforms>);
}
