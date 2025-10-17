use serde::{Deserialize, Serialize};
use crate::config::components::connections::AdapterConnectionDetails;
use crate::traits::ToSerdeMap;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SourceDbConnectionInfo {
    #[serde(rename = "database.hostname")]
    pub hostname: String,
    #[serde(rename = "database.port")]
    pub port: String,
    #[serde(rename = "database.user")]
    pub user: String,
    #[serde(rename = "database.password")]
    pub password: String,
    #[serde(rename = "database.dbname")]
    pub dbname: String,
}
impl From<AdapterConnectionDetails> for SourceDbConnectionInfo {
    fn from(value: AdapterConnectionDetails) -> Self {
        Self {
            hostname: value.host,
            port: value.port,
            user: value.user,
            password: value.password,
            dbname: value.database,
        }
    }
}
impl ToSerdeMap for SourceDbConnectionInfo {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SinkDbConnectionInfo {
    #[serde(rename = "connection.url")]
    pub url: String,
    #[serde(rename = "connection.username")]
    pub username: String,
    #[serde(rename = "connection.password")]
    pub password: String,
}

impl From<AdapterConnectionDetails> for SinkDbConnectionInfo {
    fn from(value: AdapterConnectionDetails) -> Self {
        let url = format!("jdbc:postgresql://{}/{}", value.host, value.database);
        Self {
            url,
            username: value.user,
            password: value.password,
        }
    }
}

impl ToSerdeMap for SinkDbConnectionInfo {}