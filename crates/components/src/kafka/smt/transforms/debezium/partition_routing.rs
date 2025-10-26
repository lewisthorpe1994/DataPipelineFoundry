use std::collections::HashMap;
use serde::Serialize;
use crate::traits::{ComponentVersion, ValidateVersion};
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned as ConnectorVersionedDerive;
use crate::errors::ValidationError;
use crate::predicates::PredicateRef;
use crate::smt::errors::TransformBuildError;

#[derive(Debug, Clone, Serialize, ConnectorVersionedDerive)]
#[parser(error = crate::smt::errors::TransformBuildError)]
pub struct PartitionRouting {
    #[serde(skip)]
    #[compat(always)]
    version: Version,

    #[serde(rename = "partition.payload.fields", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub partition_payload_fields: Option<String>,

    #[serde(rename = "partition.topic.num", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub partition_topic_num: Option<i32>,

    #[serde(rename = "partition.hash.function", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    #[allowed_values(range = "3.0..=3.3", values = ["java", "murmur"])]
    pub partition_hash_function: Option<String>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,
}

impl ComponentVersion for PartitionRouting {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for PartitionRouting where E: ValidationError {}

impl PartitionRouting {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        let smt = Self::generated_new(config, version)?;

        if let Some(arg) = smt.partition_payload_fields.as_ref() {
            if arg.contains(" ") {
                return Err(TransformBuildError::validation_error(
                    "partition.payload.fields must be a comma separated list of field names without spaces",
                ));
            }
        } else {
            return Err(TransformBuildError::validation_error(
                "partition.payload.fields must be specified",
            ));
        }

        if smt.partition_topic_num.is_none() {
            return Err(TransformBuildError::validation_error(
                "partition.topic.num must be specified",
            ))
        }

        Ok(smt)
    }
}