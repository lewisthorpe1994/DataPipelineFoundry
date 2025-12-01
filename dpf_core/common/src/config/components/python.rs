use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct PythonConfig {
    pub workspace_dir: String,
    pub version: Option<String>,
}
