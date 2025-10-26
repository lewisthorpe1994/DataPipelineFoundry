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
pub struct OutboxEventRouter {
    #[serde(skip)]
    #[compat(always)]
    version: Version,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub aggregatetype: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub aggregateid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub payload: Option<String>,

    #[serde(skip)]
    #[compat(always)]
    pub predicate: Option<PredicateRef>,
}

impl ComponentVersion for OutboxEventRouter {
    fn version(&self) -> Version {
        self.version
    }
}

impl<E> ValidateVersion<E> for OutboxEventRouter where E: ValidationError {}

impl OutboxEventRouter {
    pub fn new(
        config: HashMap<String, String>,
        version: Version,
    ) -> Result<Self, TransformBuildError> {
        Ok(Self::generated_new(config, version)?)
    }
}