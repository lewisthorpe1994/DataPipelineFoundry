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
pub struct ExtractNewRecordState {
    #[compat(range = "3.0..=3.1")]
    #[serde(rename = "drop.tombstones", skip_serializing_if = "Option::is_none")]
    pub drop_tombstones: Option<bool>,

    #[serde(
        rename = "delete.handling.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.1")]
    #[allowed_values(range = "3.0..=3.1", values = ["rewrite", "drop", "none"])]
    pub delete_handling_mode: Option<String>,

    #[serde(
        rename = "delete.handling.tombstone.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    #[allowed_values(range = "3.0..=3.1", values = ["drop", "tombstone", "rewrite", "rewrite-with-tombstone"])]
    #[allowed_values(range = "3.2..=3.3", values = ["drop", "tombstone", "rewrite", "rewrite-with-tombstone", "delete-to-tombstone"])]
    pub delete_handling_tombstone_mode: Option<String>,

    #[serde(rename = "add.headers", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub add_headers: Option<String>,

    #[serde(rename = "route.by.field", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub route_by_field: Option<String>,

    #[serde(rename = "add.fields.prefix", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub add_fields_prefix: Option<String>,

    #[serde(rename = "add.fields", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub add_fields: Option<String>,

    #[serde(rename = "add.headers.prefix", skip_serializing_if = "Option::is_none")]
    #[compat(range = "3.0..=3.3")]
    pub add_headers_prefix: Option<String>,

    #[serde(
        rename = "drop.fields.header.name",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub drop_fields_header_name: Option<String>,

    #[serde(
        rename = "drop.fields.from.key",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub drop_fields_from_key: Option<bool>,

    #[serde(
        rename = "drop.fields.keep.schema.compatible",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub drop_fields_keep_schema_compatible: Option<bool>,

    #[serde(
        rename = "replace.null.with.default",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(range = "3.0..=3.3")]
    pub replace_null_with_default: Option<bool>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,

    #[serde(skip)]
    #[compat(always)]
    version: Version,
}

impl ComponentVersion for ExtractNewRecordState {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for ExtractNewRecordState where E: ValidationError {}

impl ExtractNewRecordState {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let smt = Self::generated_new(config, version)?;

        if let Some(arg) = smt.route_by_field.as_ref() {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "route.by.field must not be empty",
                ));
            }
        }

        if let Some(arg) = smt.add_fields.as_ref() {
            if arg.contains(' ') {
                return Err(TransformBuildError::validation_error(
                    "add.fields must be a comma separated list of field names without spaces",
                ));
            }
        }

        if let Some(arg) = smt.add_headers.as_ref() {
            if arg.contains(' ') {
                return Err(TransformBuildError::validation_error(
                    "add.headers must be a comma separated list of header names without spaces",
                ));
            }
        }

        if let Some(arg) = smt.drop_fields_header_name.as_ref() {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "drop.fields.header.name must not be empty",
                ));
            }
        }

        Ok(smt)
    }
}

#[cfg(test)]
mod tests {
    use crate::smt::errors::TransformBuildError;
    use crate::smt::transforms::debezium::ExtractNewRecordState;
    use connector_versioning::Version;
    use std::collections::HashMap;

    #[test]
    fn extract_new_record_state_rejects_incompatible_field_for_version() {
        // Arrange: config supplies a field only valid from 3.2 onward.
        let mut config = HashMap::new();
        config.insert(
            "delete.handling.tombstone.mode".to_string(),
            "delete-to-tombstone".to_string(),
        );
        config.insert("drop.tombstones".to_string(), "true".to_string());

        // Act: attempt to build against version 3.1, where that value isn't allowed.
        let result = ExtractNewRecordState::generated_new(config, Version::new(3, 1));

        // Assert: builder should return a validation error.
        assert!(matches!(
            result,
            Err(TransformBuildError::ValidationError { .. })
        ));
    }

    #[test]
    fn extract_new_record_state_allows_drop_tombstones_for_supported_versions() {
        let mut config = HashMap::new();
        config.insert("drop.tombstones".to_string(), "true".to_string());

        let result = ExtractNewRecordState::generated_new(config, Version::new(3, 1));

        assert!(result.is_ok());
    }

    #[test]
    fn extract_new_record_state_rejects_drop_tombstones_after_3_1() {
        let mut config = HashMap::new();
        config.insert("drop.tombstones".to_string(), "true".to_string());

        let result = ExtractNewRecordState::generated_new(config, Version::new(3, 2));

        assert!(matches!(
            result,
            Err(TransformBuildError::ValidationError { .. })
        ));
    }

    #[test]
    fn extract_new_record_state_rejects_empty_route_by_field() {
        let mut config = HashMap::new();
        config.insert("route.by.field".to_string(), " ".to_string());

        let result = ExtractNewRecordState::new(config, Version::new(3, 3));

        assert!(matches!(
            result,
            Err(TransformBuildError::ValidationError { .. })
        ));
    }

    #[test]
    fn extract_new_record_state_allows_delete_to_tombstone_for_3_2() {
        let mut config = HashMap::new();
        config.insert(
            "delete.handling.tombstone.mode".to_string(),
            "delete-to-tombstone".to_string(),
        );

        let result = ExtractNewRecordState::generated_new(config, Version::new(3, 2));

        assert!(result.is_ok());
    }
}
