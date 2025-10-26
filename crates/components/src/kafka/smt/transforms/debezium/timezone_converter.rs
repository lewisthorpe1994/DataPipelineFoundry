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
pub struct TimezoneConverter {
    #[serde(skip)]
    #[compat(always)]
    version: Version,

    #[serde(rename = "converted.timezone", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub converted_timezone: Option<String>,

    #[serde(rename = "include.list", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub include_list: Option<String>,

    #[serde(rename = "exclude.list", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub exclude_list: Option<String>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,
}

impl ComponentVersion for TimezoneConverter {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for TimezoneConverter where E: ValidationError {}

impl TimezoneConverter {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let smt = Self::generated_new(config, version)?;

        if smt.include_list.is_none() && smt.exclude_list.is_none() {
            return Err(TransformBuildError::validation_error(
                "include.list or exclude.list must be specified",
            ));
        }

        if smt.include_list.is_some() && smt.exclude_list.is_some() {
            return Err(TransformBuildError::validation_error(
                "include.list and exclude.list cannot be specified at the same time",
            ));
        }

        if let Some(arg) = smt.include_list.as_ref() {
            validate_comma_seperated_list(arg, "include.list")?;
            let split = arg.split(',').collect::<Vec<&str>>();
            for item in split {
                if !item.contains(":") {
                    return Err(TransformBuildError::validation_error(
                        format!(
                        "Expected include.list to be a comma separated list of rules that \
                        determine the fields to be converted in the pattern: source:<tablename>, \
                        source:<tablename>:<fieldname>, topic:<topicname>, topic:<topicname>:<fieldname>\
                        <matchname>:<fieldname> but found: {}", item
                        )
                    ));
                }
            }
        }

        if let Some(arg) = smt.exclude_list.as_ref() {
            validate_comma_seperated_list(arg, "exclude.list")?;
            let split = arg.split(',').collect::<Vec<&str>>();
            for item in split {
                if !item.contains(":") {
                    return Err(TransformBuildError::validation_error(
                        format!(
                            "Expected include.list to be a comma separated list of rules that \
                        determine the fields to be converted in the pattern: source:<tablename>, \
                        source:<tablename>:<fieldname>, topic:<topicname>, topic:<topicname>:<fieldname>\
                        <matchname>:<fieldname> but found: {}", item
                        )
                    ));
                }
            }
        }

        Ok(smt)
    }
}
