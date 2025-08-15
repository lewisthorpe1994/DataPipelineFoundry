use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("name already exists")]
    Duplicate,
    #[error("{0}")]
    NotFound(String),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    
}
