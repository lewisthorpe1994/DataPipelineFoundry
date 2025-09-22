use crate::types::sources::SourceType;
use serde::Deserialize;

pub mod kafka;
pub mod warehouse_source;

#[derive(Debug, Clone, Deserialize)]
pub struct SourcePaths {
    pub name: String,
    pub path: String,
    pub kind: SourceType,
}
