use serde::{ser::SerializeMap, Serialize, Serializer};
use std::borrow::Cow;
use std::collections::HashMap;
use sqlparser::ast::{AstValueFormatter, PredicateReference};
/* ====================== Predicates ====================== */

#[derive(Debug, Clone)]
pub enum PredicateKind {
    /// Matches topic names via regex: `predicates.<name>.pattern`
    TopicNameMatches { pattern: String },
    /// True when the record is a tombstone (no extra props)
    RecordIsTombstone,
    /// True when a header with this key exists: `predicates.<name>.header.key`
    HasHeaderKey { name: String },
    /// Fully custom predicate (3rd party or your own)
    Custom {
        class: String,
        props: HashMap<String, String>,
    },
}

impl PredicateKind {
    fn class_name(&self) -> Cow<'static, str> {
        match self {
            PredicateKind::TopicNameMatches { .. } => {
                Cow::Borrowed("org.apache.kafka.connect.transforms.predicates.TopicNameMatches")
            }
            PredicateKind::RecordIsTombstone => {
                Cow::Borrowed("org.apache.kafka.connect.transforms.predicates.RecordIsTombstone")
            }
            PredicateKind::HasHeaderKey { .. } => {
                Cow::Borrowed("org.apache.kafka.connect.transforms.predicates.HasHeaderKey")
            }
            PredicateKind::Custom { class, .. } => Cow::Owned(class.clone()),
        }
    }
}

/// A declared predicate = name + kind (like your `Transform`)
#[derive(Debug, Clone)]
pub struct Predicate {
    pub name: String, // e.g. "onlyOrders", "isTombstone"
    pub kind: PredicateKind,
}

/// A list of predicates that serializes to flat Kafka Connect keys
#[derive(Debug, Clone, Default)]
pub struct Predicates(pub Vec<Predicate>);

impl Serialize for Predicates {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        // 1) predicates: comma-separated list of names (in order)
        let names = self
            .0
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        map.serialize_entry("predicates", &names)?;

        // 2) Per-predicate keys
        for p in &self.0 {
            let prefix = format!("predicates.{}.", p.name);
            // type
            map.serialize_entry(&(prefix.clone() + "type"), &p.kind.class_name())?;

            // kind-specific props
            match &p.kind {
                PredicateKind::TopicNameMatches { pattern } => {
                    map.serialize_entry(&(prefix.clone() + "pattern"), pattern)?;
                }
                PredicateKind::RecordIsTombstone => {
                    // no extra props
                }
                PredicateKind::HasHeaderKey { name } => {
                    map.serialize_entry(&(prefix.clone() + "name"), name)?;
                }
                PredicateKind::Custom { props, .. } => {
                    for (k, v) in props {
                        map.serialize_entry(&(prefix.clone() + k), v)?;
                    }
                }
            }
        }

        map.end()
    }
}

/* ====================== Referencing predicates from SMTs ====================== */

/// Reference a declared predicate from an SMT, with optional negation.
#[derive(Debug, Clone)]
pub struct PredicateRef {
    pub name: String,         // must match a name in `Predicates`
    pub negate: Option<bool>, // sets `transforms.<smt>.predicate.negate`
}

impl PredicateRef {
    pub(crate) fn write_flat<M: SerializeMap>(
        &self,
        map: &mut M,
        smt_prefix: &str,
    ) -> Result<(), M::Error> {
        map.serialize_entry(&(smt_prefix.to_string() + "predicate"), &self.name)?;
        if let Some(neg) = self.negate {
            map.serialize_entry(&(smt_prefix.to_string() + "negate"), &neg)?;
        }
        Ok(())
    }
}

impl From<&PredicateReference> for PredicateRef {
    fn from(value: &PredicateReference) -> Self {
        PredicateRef {
            name: value.name.formatted_string(),
            negate: value.negate.then_some(true),
        }
    }
}
