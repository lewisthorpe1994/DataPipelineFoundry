use crate::config::traits::ConfigName;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ApiAuthKind {
    None,
    Bearer,
    Basic,
    ApiKeyHeader,
    ApiKeyQuery,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ApiAuth {
    pub kind: ApiAuthKind,
    pub token: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub header_name: Option<String>,
    pub query_name: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

fn default_method() -> HttpMethod {
    HttpMethod::Get
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ApiEndpointConfig {
    pub path: String,
    #[serde(default = "default_method")]
    pub method: HttpMethod,
    #[serde(default)]
    pub query_params: HashMap<String, String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub body_template: Option<String>,
    #[serde(default)]
    pub schema_name: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ApiSourceConfig {
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub default_headers: HashMap<String, String>,
    #[serde(default)]
    pub auth: Option<ApiAuth>,
    #[serde(default)]
    pub endpoints: HashMap<String, ApiEndpointConfig>,
}

impl ConfigName for ApiSourceConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub struct ApiSourceConfigs(pub HashMap<String, ApiSourceConfig>);

impl Deref for ApiSourceConfigs {
    type Target = HashMap<String, ApiSourceConfig>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ApiSourceConfigs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<String, ApiSourceConfig>> for ApiSourceConfigs {
    fn from(value: HashMap<String, ApiSourceConfig>) -> Self {
        Self(value)
    }
}

impl ApiSourceConfigs {
    pub fn get(&self, name: &str) -> Option<&ApiSourceConfig> {
        self.0.get(name)
    }

    pub fn resolve(&self, name: &str) -> Result<&ApiSourceConfig, ApiSourceConfigError> {
        self.get(name)
            .ok_or_else(|| ApiSourceConfigError::SourceNotFound(name.to_string()))
    }
}

#[derive(Debug)]
pub enum ApiSourceConfigError {
    SourceNotFound(String),
}

impl Display for ApiSourceConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceNotFound(name) => write!(f, "API source not found: {}", name),
        }
    }
}

impl From<ApiSourceConfigError> for JinjaError {
    fn from(err: ApiSourceConfigError) -> JinjaError {
        match err {
            ApiSourceConfigError::SourceNotFound(name) => JinjaError::new(
                JinjaErrorKind::UndefinedError,
                format!("Source not found: {}", name),
            ),
        }
    }
}
