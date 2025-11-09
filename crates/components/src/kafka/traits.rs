use crate::errors::{KafkaConnectorCompileError, ValidationError};
use std::collections::HashMap;
use std::str::FromStr;

pub trait RaiseErrorOnNone<T> {
    fn raise_on_none(self, field: &str) -> Result<T, KafkaConnectorCompileError>;
}

impl<T> RaiseErrorOnNone<T> for Option<T> {
    fn raise_on_none(self, field: &str) -> Result<T, KafkaConnectorCompileError> {
        self.ok_or(KafkaConnectorCompileError::missing_config(field))
    }
}

pub trait ParseUtils<E> {
    fn parse<T>(&mut self, field: &str) -> Result<Option<T>, E>
    where
        T: FromStr;
}
impl<E> ParseUtils<E> for HashMap<String, String>
where
    E: ValidationError,
{
    fn parse<T>(&mut self, field: &str) -> Result<Option<T>, E>
    where
        T: FromStr,
    {
        let removed = self.remove(field);
        let parsed = if let Some(raw) = removed {
            Some(raw.parse::<T>().map_err(|_| E::validation_error(field))?)
        } else {
            None
        };

        Ok(parsed)
    }
}

use connector_versioning::{
    values_for_version, version_supported, ConnectorVersioned, ValueSpec, Version,
};
use serde_json::{Map as JsonMap, Value as Json};

impl<E> ParseUtils<E> for JsonMap<String, Json>
where
    E: ValidationError,
{
    fn parse<T>(&mut self, field: &str) -> Result<Option<T>, E>
    where
        T: FromStr,
        E: ValidationError,
    {
        let v = match self.remove(field) {
            None => return Ok(None),
            Some(v) => v,
        };

        // Turn JSON into a plain string WITHOUT quotes for strings.
        let raw = match v {
            Json::String(s) => s,
            Json::Number(n) => n.to_string(),
            Json::Bool(b) => b.to_string(),
            Json::Null => return Ok(None), // treat null as missing
            other => other.to_string(),    // arrays/objects as JSON
        };

        raw.parse::<T>()
            .map(Some)
            .map_err(|_| E::validation_error(field))
    }
}

pub trait ComponentVersion {
    fn version(&self) -> Version;
}

pub trait ValidateVersion<E>
where
    E: ValidationError,
    Self: ComponentVersion + ConnectorVersioned,
{
    fn validate_field_supported_in_version(
        &self,
        supported: &[Version],
        field: &str,
    ) -> Result<(), E> {
        if !version_supported(self.version(), supported) {
            return Err(E::validation_error(format!(
                "{} is not supported for this connector version: {}",
                field,
                self.version()
            )));
        }
        Ok(())
    }
    fn validate_field_value_supported_in_version(
        &self,
        supported: &[ValueSpec],
        field: &str,
        value: &str,
    ) -> Result<(), E> {
        let error_msg = format!(
            "{} is not supported for this connector version: {}",
            field,
            self.version()
        );
        let supported_values =
            values_for_version(supported, self.version()).ok_or(E::validation_error(&error_msg))?;

        if !supported_values.iter().any(|candidate| candidate == &value) {
            return Err(E::validation_error(&error_msg));
        }
        Ok(())
    }
}

//let supported_versions = vec![
//             Version::new(3, 0), Version::new(3, 1)
//         ];
//         if !supported_versions.contains(&self.version) {
//             return Err(TransformBuildError::validation_error(
//                 format!("drop.tombstones is not supported for this connector version: {}", self.version)
//             ))
//         }
