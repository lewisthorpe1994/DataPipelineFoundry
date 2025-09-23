use crate::types::sources::SourceType;
use serde::Deserialize;
use std::collections::HashMap;

pub mod kafka;
pub mod warehouse_source;

#[derive(Debug, Clone, Deserialize)]
pub struct SourcePathConfig {
    pub path: String,
    #[serde(default)]
    pub source_root: Option<String>,
    pub kind: SourceType,
}

pub type SourcePaths = HashMap<String, SourcePathConfig>;
