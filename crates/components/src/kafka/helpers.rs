use std::collections::HashMap;
use std::str::FromStr;
use serde_json::{Map, Value};
use crate::errors::KafkaConnectorCompileError;
use crate::smt::TransformBuildError;

pub fn take_bool(map: &mut HashMap<String, String>, key: &str)
                 -> Result<Option<bool>, TransformBuildError>
{
    match map.remove(key) {
        None => Ok(None),
        Some(raw) => raw.parse::<bool>()
            .map(Some)
            .map_err(|_| TransformBuildError::invalid_value(key, raw)),
    }
}

pub trait ParseUtils {
    fn parse<T>(&mut self, field: &str) -> Result<Option<T>, KafkaConnectorCompileError>
    where
        T: FromStr;
}

pub trait RaiseErrorOnNone<T>: {
    fn raise_on_none(self, field: &str) -> Result<T, KafkaConnectorCompileError>;
}

impl<T> RaiseErrorOnNone<T> for Option<T> {
    fn raise_on_none(self, field: &str) -> Result<T, KafkaConnectorCompileError> {
        self.ok_or(KafkaConnectorCompileError::missing_config(field))
    }
}

impl ParseUtils for HashMap<String, String> {
    fn parse<T>(&mut self, field: &str) -> Result<Option<T>, KafkaConnectorCompileError>
    where
        T: FromStr,
    {
        let removed = self.remove(field);
        let parsed = if let Some(raw) = removed {
            Some(raw
                .parse::<T>()
                .map_err(|_| KafkaConnectorCompileError::validation_error(field))?)
        } else {
            None
        };

        Ok(parsed)
    }


}

use serde_json::{Map as JsonMap, Value as Json};
impl ParseUtils for JsonMap<String, Json> {
    fn parse<T>(&mut self, field: &str) -> Result<Option<T>, KafkaConnectorCompileError>
    where
        T: FromStr,
    {
        let v = match self.remove(field) {
            None => return Ok(None),
            Some(v) => v,
        };

        // Turn JSON into a plain string WITHOUT quotes for strings.
        let raw = match v {
            Json::String(s) => s,
            Json::Number(n) => n.to_string(),
            Json::Bool(b)   => b.to_string(),
            Json::Null      => return Ok(None), // treat null as missing
            other           => other.to_string(), // arrays/objects as JSON
        };

        raw.parse::<T>()
            .map(Some)
            .map_err(|_| KafkaConnectorCompileError::validation_error(field))
    }
}
