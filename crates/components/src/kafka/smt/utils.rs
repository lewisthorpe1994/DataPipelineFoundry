use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use common::error::DiagnosticMessage;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use thiserror::Error;
use crate::smt::{SmtClass, SmtKind, SmtPreset, Transform};

fn predicate_of(kind: &SmtKind) -> Option<&PredicateRef> {
    match kind {
        SmtKind::ExtractNewRecordState(smt) => smt.predicate.as_ref(),
        SmtKind::ByLogicalTableRouter(smt) => smt.predicate.as_ref(),
        SmtKind::Custom(smt) => smt.predicate.as_ref(),
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
) -> Result<Transform, TransformBuildError> {
    let transform_name = name.into();
    let class = config
        .remove("type")
        .ok_or_else(|| TransformBuildError::missing_type(transform_name.clone()))?;

    match class.as_str() {
        "io.debezium.transforms.ExtractNewRecordState" => {
            let drop_tombstones = take_bool(&mut config, "drop.tombstones")?;
            let delete_handling_mode = config.remove("delete.handling.mode");
            let add_headers = config.remove("add.headers");
            let route_by_field = config.remove("route.by.field");

            if config.is_empty() {
                Ok(Transform {
                    name: transform_name,
                    kind: SmtKind::ExtractNewRecordState {
                        drop_tombstones,
                        delete_handling_mode,
                        add_headers,
                        route_by_field,
                        predicate,
                    },
                })
            } else {
                let mut props = config;
                if let Some(value) = drop_tombstones {
                    props.insert("drop.tombstones".to_string(), value.to_string());
                }
                if let Some(value) = delete_handling_mode {
                    props.insert("delete.handling.mode".to_string(), value);
                }
                if let Some(value) = add_headers {
                    props.insert("add.headers".to_string(), value);
                }
                if let Some(value) = route_by_field {
                    props.insert("route.by.field".to_string(), value);
                }
                props.insert("type".to_string(), class.clone());
                Ok(Transform {
                    name: transform_name,
                    kind: SmtKind::Custom {
                        class,
                        props,
                        predicate,
                    },
                })
            }
        }
        "io.debezium.transforms.ByLogicalTableRouter" => {
            let topic_regex = config.remove("topic.regex");
            let topic_replacement = config.remove("topic.replacement");
            let key_field_regex = config.remove("key.field.regex");
            let key_field_replacement = config.remove("key.field.replacement");

            if config.is_empty() {
                Ok(Transform {
                    name: transform_name,
                    kind: SmtKind::ByLogicalTableRouter {
                        topic_regex,
                        topic_replacement,
                        key_field_regex,
                        key_field_replacement,
                        predicate,
                    },
                })
            } else {
                let mut props = config;
                if let Some(value) = topic_regex {
                    props.insert("topic.regex".to_string(), value);
                }
                if let Some(value) = topic_replacement {
                    props.insert("topic.replacement".to_string(), value);
                }
                if let Some(value) = key_field_regex {
                    props.insert("key.field.regex".to_string(), value);
                }
                if let Some(value) = key_field_replacement {
                    props.insert("key.field.replacement".to_string(), value);
                }
                props.insert("type".to_string(), class.clone());
                Ok(Transform {
                    name: transform_name,
                    kind: SmtKind::Custom {
                        class,
                        props,
                        predicate,
                    },
                })
            }
        }
        _ => {
            config.insert("type".to_string(), class.clone());
            Ok(Transform {
                name: transform_name,
                kind: SmtKind::Custom {
                    class,
                    props: config,
                    predicate,
                },
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
                SmtKind::ExtractNewRecordState {
                    drop_tombstones,
                    delete_handling_mode,
                    add_headers,
                    route_by_field,
                    predicate,
                } => {
                    if let Some(v) = drop_tombstones {
                        map.serialize_entry(&(prefix.clone() + "drop.tombstones"), v)?;
                    }
                    if let Some(v) = delete_handling_mode {
                        map.serialize_entry(&(prefix.clone() + "delete.handling.mode"), v)?;
                    }
                    if let Some(v) = add_headers {
                        map.serialize_entry(&(prefix.clone() + "add.headers"), v)?;
                    }
                    if let Some(v) = route_by_field {
                        map.serialize_entry(&(prefix.clone() + "route.by.field"), v)?;
                    }
                    if let Some(pred) = predicate_of(&t.kind) {
                        pred.write_flat(&mut map, &prefix)?;
                    }
                }
                SmtKind::ByLogicalTableRouter {
                    topic_regex,
                    topic_replacement,
                    key_field_regex,
                    key_field_replacement,
                    predicate,
                } => {
                    if let Some(v) = topic_regex {
                        map.serialize_entry(&(prefix.clone() + "topic.regex"), v)?;
                    }
                    if let Some(v) = topic_replacement {
                        map.serialize_entry(&(prefix.clone() + "topic.replacement"), v)?;
                    }
                    if let Some(v) = key_field_regex {
                        map.serialize_entry(&(prefix.clone() + "key.field.regex"), v)?;
                    }
                    if let Some(v) = key_field_replacement {
                        map.serialize_entry(&(prefix.clone() + "key.field.replacement"), v)?;
                    }
                    if let Some(pred) = predicate_of(&t.kind) {
                        pred.write_flat(&mut map, &prefix)?;
                    }
                }
                SmtKind::Custom {
                    class,
                    props,
                    predicate,
                } => {
                    for (k, v) in props {
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
