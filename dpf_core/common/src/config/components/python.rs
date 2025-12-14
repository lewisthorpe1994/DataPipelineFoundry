use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct PythonConfig {
    pub workspace_dir: String,
    pub version: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PythonJobConfig {
    pub name: String,
    pub job_path: PathBuf,
}
