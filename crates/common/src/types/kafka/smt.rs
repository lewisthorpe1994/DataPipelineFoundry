use std::borrow::Cow;
use std::collections::HashMap;
use serde::{Serialize, Serializer};
use serde::ser::SerializeMap;
use crate::types::kafka::predicates::PredicateRef;

#[derive(Debug, Clone)]
pub enum SmtKind {
    ExtractNewRecordState {
        drop_tombstones: Option<String>,
        delete_handling_mode: Option<String>,
        add_headers: Option<String>,
        route_by_field: Option<String>,
        predicate: Option<PredicateRef>,
    },
    ByLogicalTableRouter {
        topic_regex: Option<String>,
        topic_replacement: Option<String>,
        key_field_regex: Option<String>,
        key_field_replacement: Option<String>,
        predicate: Option<PredicateRef>,
    },
    Custom {
        class: String,
        props: HashMap<String, String>,
        predicate: Option<PredicateRef>,
    },
    // You can add more SMTs here as needed
}

impl SmtKind {
    fn class_name(&self) -> Cow<'_, str> {
        match self {
            SmtKind::ExtractNewRecordState { .. } =>
                Cow::Borrowed("io.debezium.transforms.ExtractNewRecordState"),
            SmtKind::ByLogicalTableRouter { .. } =>
                Cow::Borrowed("io.debezium.transforms.ByLogicalTableRouter"),
            SmtKind::Custom { class, .. } =>
                Cow::Owned(class.clone()),
        }
    }
}

fn predicate_of(kind: &SmtKind) -> Option<&PredicateRef> {
    match kind {
        SmtKind::ExtractNewRecordState { predicate, .. } => predicate.as_ref(),
        SmtKind::ByLogicalTableRouter { predicate, .. } => predicate.as_ref(),
        SmtKind::Custom { predicate, .. } => predicate.as_ref(),
    }
}

/* ---------- One transform = name + kind ---------- */
#[derive(Debug, Clone)]
pub struct Transform {
    pub name: String,  // e.g. "unwrap", "route"
    pub kind: SmtKind,
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
        let names = self.0.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(",");
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
                    predicate
                } => {
                    if let Some(v) = drop_tombstones { map.serialize_entry(&(prefix.clone() + "drop.tombstones"), v)?; }
                    if let Some(v) = delete_handling_mode { map.serialize_entry(&(prefix.clone() + "delete.handling.mode"), v)?; }
                    if let Some(v) = add_headers { map.serialize_entry(&(prefix.clone() + "add.headers"), v)?; }
                    if let Some(v) = route_by_field { map.serialize_entry(&(prefix.clone() + "route.by.field"), v)?; }
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
                    if let Some(v) = topic_regex { map.serialize_entry(&(prefix.clone() + "topic.regex"), v)?; }
                    if let Some(v) = topic_replacement { map.serialize_entry(&(prefix.clone() + "topic.replacement"), v)?; }
                    if let Some(v) = key_field_regex { map.serialize_entry(&(prefix.clone() + "key.field.regex"), v)?; }
                    if let Some(v) = key_field_replacement { map.serialize_entry(&(prefix.clone() + "key.field.replacement"), v)?; }
                    if let Some(pred) = predicate_of(&t.kind) {
                        pred.write_flat(&mut map, &prefix)?;
                    }
                }
                SmtKind::Custom { class, props, predicate } => {
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