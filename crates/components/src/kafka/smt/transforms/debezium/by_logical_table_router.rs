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
pub struct ByLogicalTableRouter {
    #[serde(rename = "topic.regex", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub topic_regex: Option<String>,

    #[serde(rename = "topic.replacement", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub topic_replacement: Option<String>,

    #[serde(
        rename = "key.enforce.uniqueness",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub key_enforce_uniqueness: Option<bool>,

    #[serde(rename = "key.field.name", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub key_field_name: Option<String>,

    #[serde(rename = "key.field.regex", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub key_field_regex: Option<String>,

    #[serde(
        rename = "key.field.replacement",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub key_field_replacement: Option<String>,

    #[serde(
        rename = "schema.name.adjustment.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    #[allowed_values(range = "3.0..=3.3", values = ["none", "avro"])]
    pub schema_name_adjustment_mode: Option<String>,

    #[serde(
        rename = "logical.table.cache.size",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub logical_table_cache_size: Option<i32>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,

    #[serde(skip)]
    version: Version,
}

impl ComponentVersion for ByLogicalTableRouter {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for ByLogicalTableRouter where E: ValidationError {}

impl ByLogicalTableRouter {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let mut smt = Self::generated_new(config, version)?;

        if let Some(regex) = smt.topic_regex.as_ref() {
            if regex.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "topic.regex must not be empty".to_string(),
                ));
            }
        }

        if let Some(replacement) = smt.topic_replacement.as_ref() {
            if replacement.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "topic.replacement must not be empty".to_string(),
                ));
            }
        }

        if let Some(size) = smt.logical_table_cache_size {
            if size <= 0 {
                return Err(TransformBuildError::validation_error(
                    "logical.table.cache.size must be greater than zero".to_string(),
                ));
            }
        }

        smt.validate_key_constraints()?;
        smt.validate_topic_constraints()?;

        Ok(smt)
    }

    fn validate_key_constraints(&self) -> Result<(), TransformBuildError> {
        let enforce = self.key_enforce_uniqueness.unwrap_or(true);
        if !enforce {
            if self.key_field_name.is_some()
                || self.key_field_regex.is_some()
                || self.key_field_replacement.is_some()
            {
                return Err(TransformBuildError::validation_error(
                    "key field options cannot be set when key.enforce.uniqueness is false"
                        .to_string(),
                ));
            }
            return Ok(());
        }

        if self.key_field_regex.is_some() && self.key_field_replacement.is_none() {
            return Err(TransformBuildError::validation_error(
                "key.field.replacement must be provided when key.field.regex is set".to_string(),
            ));
        }

        if self.key_field_replacement.is_some() && self.key_field_regex.is_none() {
            return Err(TransformBuildError::validation_error(
                "key.field.regex must be provided when key.field.replacement is set".to_string(),
            ));
        }

        Ok(())
    }

    fn validate_topic_constraints(&self) -> Result<(), TransformBuildError> {
        if self.topic_replacement.is_some() && self.topic_regex.is_none() {
            return Err(TransformBuildError::validation_error(
                "topic.regex must be provided when topic.replacement is set".to_string(),
            ));
        }
        Ok(())
    }
}
