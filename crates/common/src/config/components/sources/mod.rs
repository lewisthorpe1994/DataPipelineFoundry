use crate::types::sources::SourceType;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

pub mod kafka;
pub mod warehouse_source;

#[derive(Debug, Clone, Deserialize)]
pub struct SourcePathConfig {
    pub specifications: PathBuf,
    #[serde(default)]
    pub source_root: Option<PathBuf>,
    pub definitions: Option<PathBuf>,
}

pub type SourcePaths = HashMap<SourceType, SourcePathConfig>;
