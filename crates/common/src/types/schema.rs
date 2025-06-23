use std::fmt::{Debug, Display};
use serde::Deserialize;
#[derive(Deserialize, Debug)]
pub struct Table {
    pub name: String,
    pub description: Option<String>,
    // TODO - add more meta fields
}

#[derive(Deserialize, Debug)]
pub struct Column {
    pub name: String,
    description: Option<String>,
    data_type: String, // TODO - implement enum
}

#[derive(Deserialize, Debug)]
pub struct Schema {
    pub name: String,
    pub description: Option<String>,
    pub tables: Vec<Table>,
}

#[derive(Deserialize, Debug)]
pub struct Database {
    pub name: String,
    pub schemas: Vec<Schema>,
}
