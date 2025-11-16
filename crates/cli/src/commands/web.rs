use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use clap::Args;
use common::config::loader::read_config;
use common::error::FFError;
use foundry_web::{init_logging, run_backend, BackendConfig};
use tokio::runtime::Runtime;

#[derive(Debug, Args)]
pub struct WebArgs {
    /// Optional path to a manifest file. Defaults to <compile_path>/manifest.json
    #[arg(long)]
    pub manifest: Option<PathBuf>,

    /// Address to bind the backend server
    #[arg(long, default_value = "0.0.0.0:8085")]
    pub addr: String,

    /// Directory of built frontend assets to serve (optional)
    #[arg(long)]
    pub static_dir: Option<PathBuf>,

    /// Skip launching the Vite dev server
    #[arg(long)]
    pub no_frontend: bool,

    /// Path to the frontend workspace (defaults to foundry_web/ui)
    #[arg(long)]
    pub frontend_dir: Option<PathBuf>,
}

pub fn handle_web(args: WebArgs, config_path: Option<PathBuf>) -> Result<(), FFError> {
    let cfg = read_config(config_path.clone()).map_err(FFError::compile)?;

    let project_root = resolve_project_root(&config_path)?;
    let manifest_path =
        resolve_manifest_path(args.manifest, &project_root, &cfg.project.compile_path)?;

    let static_dir = args.static_dir;

    let frontend_dir = args.frontend_dir.unwrap_or_else(|| default_frontend_dir());

    let mut frontend_child = if args.no_frontend {
        None
    } else {
        Some(spawn_frontend(&frontend_dir)?)
    };

    init_logging();
    let rt = Runtime::new().map_err(FFError::run)?;
    let result = rt.block_on(run_backend(BackendConfig {
        manifest: manifest_path,
        addr: args.addr,
        static_dir,
    }));

    if let Some(child) = frontend_child.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }

    result.map_err(FFError::run)
}

fn resolve_project_root(config_path: &Option<PathBuf>) -> Result<PathBuf, FFError> {
    if let Some(root) = config_path {
        Ok(root.clone())
    } else {
        std::env::current_dir().map_err(FFError::run)
    }
}

fn resolve_manifest_path(
    explicit: Option<PathBuf>,
    project_root: &Path,
    compile_path: &str,
) -> Result<PathBuf, FFError> {
    let manifest_path = if let Some(path) = explicit {
        path
    } else {
        let base = if Path::new(compile_path).is_absolute() {
            PathBuf::from(compile_path)
        } else {
            project_root.join(compile_path)
        };
        base.join("manifest.json")
    };

    if !manifest_path.exists() {
        return Err(FFError::run_msg(format!(
            "manifest not found at {}",
            manifest_path.display()
        )));
    }

    Ok(manifest_path)
}

fn default_frontend_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    workspace_root.join("foundry_web/ui")
}

fn spawn_frontend(dir: &Path) -> Result<Child, FFError> {
    if !dir.exists() {
        return Err(FFError::run_msg(format!(
            "frontend directory '{}' does not exist",
            dir.display()
        )));
    }

    Command::new("npm")
        .arg("run")
        .arg("dev")
        .current_dir(dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .stdin(Stdio::inherit())
        .spawn()
        .map_err(FFError::run)
}
