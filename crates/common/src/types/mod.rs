pub mod nodes;
pub mod schema;
pub mod sources;

pub use nodes::*;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Materialize {
    #[default]
    View,
    Table,
    #[serde(rename = "materialized_view")]
    MaterializedView,
}

impl Materialize {
    pub fn to_sql(&self) -> String {
        let materialised = match self {
            Self::View => "VIEW",
            Self::Table => "TABLE",
            Self::MaterializedView => "MATERIALIZED VIEW",
        };

        materialised.to_string().to_uppercase()
    }
}
