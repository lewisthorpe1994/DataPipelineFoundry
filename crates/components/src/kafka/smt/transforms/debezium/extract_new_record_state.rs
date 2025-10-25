use crate::errors::ValidationError;
use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use crate::traits::{ComponentVersion, ParseUtils, ValidateVersion};
use connector_versioning::{values_map, versions_vec, ValueSpec, Version};
use std::collections::HashMap;
use crate::version_consts::{DBZ_ALL_SUPPORTED_VERSIONS, DBZ_VERSION_3_0, DBZ_VERSION_3_1, DBZ_VERSION_3_2, DBZ_VERSION_3_3};

const DROP_TOMBSTONES_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const DELETE_HANDLING_MODE_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;

const DELETE_HANDLING_MODE_VALUE_SPECS: &[ValueSpec] = &[
    ValueSpec {
        version: DBZ_VERSION_3_0,
        values: &["rewrite", "drop", "none"],
    },
    ValueSpec {
        version: DBZ_VERSION_3_1,
        values: &["rewrite", "drop", "none"],
    },
];

const DELETE_HANDLING_TOMBSTONE_MODE_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;

const DELETE_HANDLING_TOMBSTONE_MODE_VALUE_SPECS: &[ValueSpec] = &[
    ValueSpec {
        version: DBZ_VERSION_3_0,
        values: &["drop", "tombstone", "rewrite", "rewrite-with-tombstone"],
    },
    ValueSpec {
        version: DBZ_VERSION_3_1,
        values: &["drop", "tombstone", "rewrite", "rewrite-with-tombstone"],
    },
    ValueSpec {
        version: DBZ_VERSION_3_2,
        values: &[
            "drop",
            "tombstone",
            "rewrite",
            "rewrite-with-tombstone",
            "delete-to-tombstone",
        ],
    },
    ValueSpec {
        version: DBZ_VERSION_3_3,
        values: &[
            "drop",
            "tombstone",
            "rewrite",
            "rewrite-with-tombstone",
            "delete-to-tombstone",
        ],
    },
];

const ROUTE_BY_FIELD_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const ADD_FIELDS_PREFIX_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const ADD_FIELDS_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const ADD_HEADERS_PREFIX_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const ADD_HEADERS_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const DROP_FIELDS_HEADER_NAME_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const DROP_FIELDS_FROM_KEY_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;
const REPLACE_NULL_WITH_DEFAULT_VERSIONS: &[Version] = DBZ_ALL_SUPPORTED_VERSIONS;


#[derive(Debug, Clone)]
pub struct ExtractNewRecordState {
    pub drop_tombstones: Option<bool>,
    pub delete_handling_mode: Option<String>,
    pub delete_handling_tombstone_mode: Option<String>,
    pub add_headers: Option<String>,
    pub route_by_field: Option<String>,
    pub add_fields_prefix: Option<String>,
    pub add_fields: Option<String>,
    pub add_headers_prefix: Option<String>,
    pub drop_fields_header_name: Option<String>,
    pub drop_fields_from_key: Option<bool>,
    pub drop_fields_keep_schema_compatible: Option<bool>,
    pub replace_null_with_default: Option<bool>,
    pub predicate: Option<PredicateRef>,
    version: Version,
}

type FieldSetter =
    fn(&mut ExtractNewRecordState, &mut HashMap<String, String>) -> Result<(), TransformBuildError>;

const FIELD_SETTERS: &[FieldSetter] = &[
    ExtractNewRecordState::set_drop_tombstones,
    ExtractNewRecordState::set_delete_handling_mode,
    ExtractNewRecordState::set_delete_handling_tombstone_mode,
    ExtractNewRecordState::set_route_by_field,
    ExtractNewRecordState::set_add_fields_prefix,
    ExtractNewRecordState::set_add_fields,
    ExtractNewRecordState::set_add_headers_prefix,
    ExtractNewRecordState::set_add_headers,
    ExtractNewRecordState::set_drop_fields_header_name,
    ExtractNewRecordState::set_drop_fields_from_key,
    ExtractNewRecordState::set_drop_fields_keep_schema_compatible,
    ExtractNewRecordState::set_replace_null_with_default,
];

impl ComponentVersion for ExtractNewRecordState {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for ExtractNewRecordState
where
    E: ValidationError,
{}

impl ExtractNewRecordState {
    fn empty(version: Version) -> Self {
        Self {
            drop_tombstones: None,
            delete_handling_mode: None,
            delete_handling_tombstone_mode: None,
            add_headers: None,
            route_by_field: None,
            add_fields_prefix: None,
            add_fields: None,
            add_headers_prefix: None,
            drop_fields_header_name: None,
            drop_fields_from_key: None,
            drop_fields_keep_schema_compatible: None,
            replace_null_with_default: None,
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
        Ok(smt)
    }

    fn set_drop_tombstones(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        self.validate_field_supported_in_version(
            versions_vec(DROP_TOMBSTONES_VERSIONS),
            "drop.tombstones",
        )?;

        let val = config.parse::<bool>("drop.tombstones")?;
        self.drop_tombstones = val;
        Ok(())
    }

    fn set_delete_handling_mode(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "delete.handling.mode";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            self.validate_field_supported_in_version(
                versions_vec(DELETE_HANDLING_MODE_VERSIONS),
                field_name,
            )?;

            let allowed = values_map(DELETE_HANDLING_MODE_VALUE_SPECS);
            self.validate_field_value_supported_in_version(allowed, field_name, arg)?;
        }
        self.delete_handling_mode = val;

        Ok(())
    }

    fn set_delete_handling_tombstone_mode(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let val = config.parse::<String>("delete.handling.tombstone.mode")?;

        if let Some(arg) = &val {
            self.validate_field_supported_in_version(
                versions_vec(DELETE_HANDLING_TOMBSTONE_MODE_VERSIONS),
                "delete.handling.tombstone.mode",
            )?;

            let allowed = values_map(DELETE_HANDLING_TOMBSTONE_MODE_VALUE_SPECS);
            self.validate_field_value_supported_in_version(
                allowed,
                "delete.handling.tombstone.mode",
                arg,
            )?;
        }

        self.delete_handling_tombstone_mode = val;
        Ok(())
    }

    fn set_route_by_field(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "route.by.field";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "route.by.field must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(ROUTE_BY_FIELD_VERSIONS),
                field_name,
            )?;
        }
        self.route_by_field = val;
        Ok(())
    }

    fn set_add_fields_prefix(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "add.fields.prefix";
        let val = config.parse::<String>(field_name)?;
        if val.is_some() {
            self.validate_field_supported_in_version(
                versions_vec(ADD_FIELDS_PREFIX_VERSIONS),
                field_name,
            )?;
        }
        self.add_fields_prefix = val;
        Ok(())
    }

    fn set_add_fields(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "add.fields";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            self.validate_field_supported_in_version(
                versions_vec(ADD_FIELDS_VERSIONS),
                field_name,
            )?;
            if arg.contains(' ') {
                return Err(TransformBuildError::validation_error(
                    "add.fields must be a comma separated list of field names without spaces",
                ));
            }
        }
        self.add_fields = val;
        Ok(())
    }

    fn set_add_headers_prefix(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "add.headers.prefix";
        let val = config.parse::<String>(field_name)?;
        if val.is_some() {
            self.validate_field_supported_in_version(
                versions_vec(ADD_HEADERS_PREFIX_VERSIONS),
                field_name,
            )?;
        }
        self.add_headers_prefix = val;
        Ok(())
    }

    fn set_add_headers(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "add.headers";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            self.validate_field_supported_in_version(
                versions_vec(ADD_HEADERS_VERSIONS),
                field_name,
            )?;

            if arg.contains(' ') {
                return Err(TransformBuildError::validation_error(
                    "add.headers must be a comma separated list of header names without spaces",
                ));
            }
        }
        self.add_headers = val;
        Ok(())
    }

    fn set_drop_fields_header_name(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "drop.fields.header.name";
        let val = config.parse::<String>(field_name)?;
        if let Some(arg) = &val {
            if arg.trim().is_empty() {
                return Err(TransformBuildError::validation_error(
                    "drop.fields.header.name must not be empty",
                ));
            }
            self.validate_field_supported_in_version(
                versions_vec(DROP_FIELDS_HEADER_NAME_VERSIONS),
                field_name,
            )?;
        }
        self.drop_fields_header_name = val;
        Ok(())
    }

    fn set_drop_fields_from_key(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "drop.fields.from.key";
        let val = config.parse::<bool>(field_name)?;
        if val.is_some() {
            self.validate_field_supported_in_version(
                versions_vec(DROP_FIELDS_FROM_KEY_VERSIONS),
                field_name,
            )?;
        }
        self.drop_fields_from_key = val;
        Ok(())
    }

    fn set_drop_fields_keep_schema_compatible(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "drop.fields.keep.schema.compatible";
        let val = config.parse::<bool>(field_name)?;
        if val.is_some() {
            self.validate_field_supported_in_version(
                versions_vec(DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE_VERSIONS),
                field_name,
            )?;
        }
        self.drop_fields_keep_schema_compatible = val;
        Ok(())
    }

    fn set_replace_null_with_default(
        &mut self,
        config: &mut HashMap<String, String>,
    ) -> Result<(), TransformBuildError> {
        let field_name = "replace.null.with.default";
        let val = config.parse::<bool>(field_name)?;
        if val.is_some() {
            self.validate_field_supported_in_version(
                versions_vec(REPLACE_NULL_WITH_DEFAULT_VERSIONS),
                field_name,
            )?;
        }
        self.replace_null_with_default = val;
        Ok(())
    }
}
