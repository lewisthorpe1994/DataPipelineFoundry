use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct Table {
    pub columns: Vec<Column>,
    pub description: Option<String>,
    // TODO - add more meta fields
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct Column {
    pub name: String,
    description: Option<String>,
    data_type: Option<String>, // TODO - implement enum
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct Schema {
    pub description: Option<String>,
    pub tables: HashMap<String, Table>,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct Database {
    pub name: String,
    pub schemas: HashMap<String, Schema>,
}
