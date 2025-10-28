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
pub struct HeaderToValue {
    #[serde(skip)]
    #[compat(always)]
    version: Version,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub headers: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub fields: Option<String>,

    #[compat(always)]
    #[allowed_values(range = "3.0..=3.3", values = ["move", "copy"])]
    pub operation: Option<String>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,
}

impl ComponentVersion for HeaderToValue {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for HeaderToValue where E: ValidationError {}

impl HeaderToValue {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let con = Self::generated_new(config, version)?;

        if let Some(arg) = con.headers.as_ref() {
            if arg.contains(' ') {
                return Err(TransformBuildError::validation_error(
                    "headers must be a comma separated list of header names without spaces",
                ));
            }
        } else {
            return Err(TransformBuildError::validation_error(
                "headers must be specified",
            ));
        }

        if let Some(arg) = con.fields.as_ref() {
            if arg.contains(' ') {
                return Err(TransformBuildError::validation_error(
                    "fields must be a comma separated list of field names without spaces",
                ));
            }
        } else {
            return Err(TransformBuildError::validation_error(
                "fields must be specified",
            ));
        }

        if con.operation.is_none() {
            return Err(TransformBuildError::validation_error(
                "operation must be specified",
            ));
        }

        Ok(con)
    }
}
