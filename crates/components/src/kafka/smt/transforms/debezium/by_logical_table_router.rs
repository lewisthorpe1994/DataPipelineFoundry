use crate::errors::ValidationError;
use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use crate::traits::{ComponentVersion, ParseUtils, ValidateVersion};
use connector_versioning::{values_map, versions_vec, ValueSpec, Version};
use std::collections::HashMap;
use crate::version_consts::{DBZ_ALL_SUPPORTED_VERSIONS, DBZ_VERSION_3_0, DBZ_VERSION_3_1, DBZ_VERSION_3_2, DBZ_VERSION_3_3};

const SCHEMA_NAME_ADJUSTMENT_MODE_VALUES: &[ValueSpec] = &[
    ValueSpec {
        version: DBZ_VERSION_3_0,
        values: &["none", "avro"],
    },
    ValueSpec {
        version: DBZ_VERSION_3_1,
        values: &["none", "avro"],
    },
    ValueSpec {
        version: DBZ_VERSION_3_2,
        values: &["none", "avro"],
    },
    ValueSpec {
        version: DBZ_VERSION_3_3,
        values: &["none", "avro"],
    },
];

#[derive(Debug, Clone)]
pub struct ByLogicalTableRouter {
    pub topic_regex: Option<String>,
    pub topic_replacement: Option<String>,
    pub key_enforce_uniqueness: Option<bool>,
    pub key_field_name: Option<String>,
    pub key_field_regex: Option<String>,
    pub key_field_replacement: Option<String>,
    pub schema_name_adjustment_mode: Option<String>,
    pub logical_table_cache_size: Option<i32>,
    pub predicate: Option<PredicateRef>,
    version: Version,
}

type FieldSetter =
    fn(&mut ByLogicalTableRouter, &mut HashMap<String, String>) -> Result<(), TransformBuildError>;

const FIELD_SETTERS: &[FieldSetter] = &[
    ByLogicalTableRouter::set_topic_regex,
    ByLogicalTableRouter::set_topic_replacement,
    ByLogicalTableRouter::set_key_enforce_uniqueness,
    ByLogicalTableRouter::set_key_field_name,
    ByLogicalTableRouter::set_key_field_regex,
    ByLogicalTableRouter::set_key_field_replacement,
    ByLogicalTableRouter::set_schema_name_adjustment_mode,
    ByLogicalTableRouter::set_logical_table_cache_size,
];

impl ComponentVersion for ByLogicalTableRouter {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for ByLogicalTableRouter where E: ValidationError {}

impl ByLogicalTableRouter {
    fn empty(version: Version) -> Self {
        Self {
            topic_regex: None,
            topic_replacement: None,
            key_enforce_uniqueness: None,
            key_field_name: None,
            key_field_regex: None,
            key_field_replacement: None,
            schema_name_adjustment_mode: None,
            logical_table_cache_size: None,
            predicate: None,
            version,
        }
    }

    pub fn new(
        mut config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let mut smt = Self::empty(version);
        for setter in FIELD_SETTERS {
            setter(&mut smt, &mut config)?;
        }

        smt.validate_key_constraints()?;
        smt.validate_topic_constraints()?;

        Ok(smt)
    }

    fn set_topic_regex(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "topic.regex";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "topic.regex must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
        }
        self.topic_regex = val;
        Ok(())
    }

    fn set_topic_replacement(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "topic.replacement";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "topic.replacement must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
        }
        self.topic_replacement = val;
        Ok(())
    }

    fn set_key_enforce_uniqueness(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "key.enforce.uniqueness";
        let val = config.parse::<bool>(field_name)?;
        if val.is_some() {
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
        }
        self.key_enforce_uniqueness = val;
        Ok(())
    }

    fn set_key_field_name(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "key.field.name";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "key.field.name must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
        }
        self.key_field_name = val;
        Ok(())
    }

    fn set_key_field_regex(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "key.field.regex";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "key.field.regex must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
        }
        self.key_field_regex = val;
        Ok(())
    }

    fn set_key_field_replacement(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "key.field.replacement";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "key.field.replacement must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
        }
        self.key_field_replacement = val;
        Ok(())
    }

    fn set_schema_name_adjustment_mode(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "schema.name.adjustment.mode";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
            let allowed = values_map(SCHEMA_NAME_ADJUSTMENT_MODE_VALUES);
            self.validate_field_value_supported_in_version(allowed, field_name, arg)?;
        }
        self.schema_name_adjustment_mode = val;
        Ok(())
    }

    fn set_logical_table_cache_size(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "logical.table.cache.size";
        let val = config.parse::<i32>(field_name)?;
        if let Some(cache_size) = val {
            self.validate_field_supported_in_version(
                versions_vec(DBZ_ALL_SUPPORTED_VERSIONS),
                field_name,
            )?;
            if cache_size <= 0 {
                return Err(TransformBuildError::validation_error(
                    "logical.table.cache.size must be greater than zero",
                ));
            }
        }
        self.logical_table_cache_size = val;
        Ok(())
    }

    fn validate_key_constraints(&self) -> Result<(), TransformBuildError> {
        let enforce = self.key_enforce_uniqueness.unwrap_or(true);
        if !enforce {
            if self.key_field_name.is_some()
                || self.key_field_regex.is_some()
                || self.key_field_replacement.is_some()
            {
                return Err(TransformBuildError::validation_error(
                    "key field options cannot be set when key.enforce.uniqueness is false",
                ));
            }
            return Ok(());
        }

        if self.key_field_regex.is_some() && self.key_field_replacement.is_none() {
            return Err(TransformBuildError::validation_error(
                "key.field.replacement must be provided when key.field.regex is set",
            ));
        }

        if self.key_field_replacement.is_some() && self.key_field_regex.is_none() {
            return Err(TransformBuildError::validation_error(
                "key.field.regex must be provided when key.field.replacement is set",
            ));
        }

        Ok(())
    }

    fn validate_topic_constraints(&self) -> Result<(), TransformBuildError> {
        if self.topic_replacement.is_some() && self.topic_regex.is_none() {
            return Err(TransformBuildError::validation_error(
                "topic.regex must be provided when topic.replacement is set",
            ));
        }

        Ok(())
    }
}
