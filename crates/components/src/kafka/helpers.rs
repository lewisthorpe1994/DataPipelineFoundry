use std::collections::HashMap;
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