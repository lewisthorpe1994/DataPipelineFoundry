pub mod schema;
pub mod sources;
pub mod nodes;

use std::ops::Deref;
use serde::{Deserialize, Serialize};
pub use nodes::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Materialize {
    #[default]
    View,
    Table,
    MaterializedView,
}

impl Materialize {
    pub fn to_sql(&self) -> String {
        let materialised = match self { 
            Self::View => "VIEW",
            Self::Table => "TABLE",
            Self::MaterializedView => "MATERIALISED VIEW",
        };
        
        materialised.to_string().to_uppercase()
    }
}