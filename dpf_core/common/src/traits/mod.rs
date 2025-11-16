use serde::ser::Error;
use serde::Serialize;
use serde_json::Value;
use std::path::Path;

pub trait IsFileExtension {
    fn is_extension(&self, ext: &str) -> bool;
}
impl IsFileExtension for Path {
    fn is_extension(&self, ext: &str) -> bool {
        self.extension().and_then(|e| e.to_str()) == Some(ext)
    }
}

pub trait ToSerdeMap {
    fn to_json_map(&self) -> Result<serde_json::Map<String, serde_json::Value>, serde_json::Error>
    where
        Self: Serialize,
    {
        let obj = match serde_json::to_value(self)? {
            Value::Object(obj) => obj,
            _ => return Err(serde_json::Error::custom("expecting source db config")),
        };
        Ok(obj)
    }
}
