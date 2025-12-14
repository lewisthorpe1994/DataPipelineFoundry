use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

///  ---------------- Connections Config ----------------
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseAdapterType {
    Postgres,
}
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AdapterConnectionDetails {
    pub host: String,
    pub user: String,
    pub database: String,
    pub password: String,
    pub port: String,
    pub adapter_type: DatabaseAdapterType,
}
impl AdapterConnectionDetails {
    pub fn new(
        host: &str,
        user: &str,
        database: &str,
        password: &str,
        port: &str,
        adapter_type: DatabaseAdapterType,
    ) -> Self {
        Self {
            host: host.to_string(),
            user: user.to_string(),
            database: database.to_string(),
            password: password.to_string(),
            port: port.to_string(),
            adapter_type,
        }
    }
}
pub type ConnectionsConfig = HashMap<String, HashMap<String, AdapterConnectionDetails>>;

#[derive(Debug, Deserialize, Clone)]
pub struct Connections {
    pub profile: String,
    pub path: PathBuf,
}
