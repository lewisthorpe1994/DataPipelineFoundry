use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
#[derive(Deserialize, Debug, Serialize)]
pub struct Table {
    pub name: String,
    pub description: Option<String>,
    // TODO - add more meta fields
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Column {
    pub name: String,
    description: Option<String>,
    data_type: String, // TODO - implement enum
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Schema {
    pub name: String,
    pub description: Option<String>,
    pub tables: Vec<Table>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Database {
    pub name: String,
    pub schemas: Vec<Schema>,
}
