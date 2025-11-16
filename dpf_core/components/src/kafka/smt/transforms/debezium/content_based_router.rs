use crate::errors::ValidationError;
use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use crate::traits::{ComponentVersion, ValidateVersion};
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned as ConnectorVersionedDerive;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, ConnectorVersionedDerive)]
#[parser(error = crate::smt::errors::TransformBuildError)]
pub struct ContentBasedRouter {
    #[serde(skip)]
    #[compat(always)]
    version: Version,

    #[serde(rename = "topic.regex", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub topic_regex: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub language: Option<String>,

    #[serde(rename = "topic.expression", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub topic_expression: Option<String>,

    #[serde(rename = "null.handling.mode", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    #[allowed_values(range = "3.0..=3.3", values = ["keep", "drop", "evaluate"])]
    pub null_handling_mode: Option<String>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,
}

impl ComponentVersion for ContentBasedRouter {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for ContentBasedRouter where E: ValidationError {}

impl ContentBasedRouter {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        Self::generated_new(config, version)
    }
}
