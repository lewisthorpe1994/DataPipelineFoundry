use crate::errors::ValidationError;
use crate::kafka::utils::validate_comma_seperated_list;
use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;
use crate::traits::{ComponentVersion, ValidateVersion};
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned as ConnectorVersionedDerive;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::format;

#[derive(Debug, Clone, Serialize, ConnectorVersionedDerive)]
#[parser(error = crate::smt::errors::TransformBuildError)]
pub struct DecodeLogicalDecodingMessageContent {
    #[serde(skip)]
    #[compat(always)]
    version: Version,

    #[serde(
        rename = "fields.null.include",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    #[allowed_values(range = "3.0..=3.3", values = ["true", "false"])]
    pub converted_timezone: Option<bool>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,
}

impl ComponentVersion for DecodeLogicalDecodingMessageContent {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for DecodeLogicalDecodingMessageContent where E: ValidationError {}

impl DecodeLogicalDecodingMessageContent {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let smt = Self::generated_new(config, version)?;

        Ok(smt)
    }
}
