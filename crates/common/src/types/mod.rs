use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum Materialize {
    #[default]
    View,
    Table,
    MaterializedView,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelRef {
    pub table: String,
    pub schema: String
    // todo - look at adding columns
}
impl ModelRef {
    pub fn new<S: Into<String>>(schema: S, table: S) -> Self {
        Self {
            schema: schema.into(),
            table: table.into()
        }
    }
    pub fn to_string(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

