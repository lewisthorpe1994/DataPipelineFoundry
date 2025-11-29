use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[derive(Clone)]
pub struct PythonConfig {
    pub workspace_dir: String,
    pub version: Option<String>,
}