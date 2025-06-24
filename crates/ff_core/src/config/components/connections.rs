use std::collections::HashMap;
use serde::Deserialize;

///  ---------------- Connections Config ----------------
pub type ConnectionsConfig = HashMap<String, DatabaseConnection>;

#[derive(Debug, Deserialize)]
pub struct DatabaseConnection {
    pub adapter: String,
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: String,
    pub database: String
}

impl DatabaseConnection {
    pub fn to_conn_str(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}", 
            self.host, self.port, self.user, self.password, self.database
        )
    }
}