use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("name already exists")]
    Duplicate,
    #[error("{0}")]
    NotFound(String),
    #[error("serde error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("serde yaml error: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Unsupported(String),
    #[error("sql parser error: {0}")]
    SqlParser(String),
}
