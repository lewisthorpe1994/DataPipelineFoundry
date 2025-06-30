use serde::Deserialize;
use common::types::sources::SourceType;

pub mod warehouse_source;
pub mod kafka;

#[derive(Debug, Clone, Deserialize)]
pub struct SourcePaths {
    pub name: String,
    pub path: String,
    pub kind: SourceType,
}