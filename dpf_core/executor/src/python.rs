use common::error::DiagnosticMessage;
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use thiserror::Error;
use log::{info, warn};

#[derive(Debug, Clone, Copy)]
enum PythonLogMode {
    Log,
    Raw,
}

fn python_log_mode() -> PythonLogMode {
    match env::var("DPF_PYTHON_LOG_MODE").as_deref() {
        Ok("raw") => PythonLogMode::Raw,
        _ => PythonLogMode::Log,
    }
}

#[derive(Error, Debug)]
pub enum PythonExecutorError {
    #[error("Uv binary not found. Make sure uv is installed and in your PATH.")]
    UvNotFound,
    #[error("Execution error: {content}\nExit code: {code:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}")]
    FailedToExecute {
        content: DiagnosticMessage,
        code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    #[error("Unexpected error: {content}")]
    UnexpectedError { content: DiagnosticMessage },
    #[error("IO error: {content}")]
    Io { content: DiagnosticMessage },
}
impl PythonExecutorError {
    #[track_caller]
    pub fn uv_not_found() -> Self {
        Self::UvNotFound
    }

    #[track_caller]
    pub fn failed_to_execute(
        content: impl Into<String>,
        code: Option<i32>,
        stdout: String,
        stderr: String,
    ) -> Self {
        Self::FailedToExecute {
            content: DiagnosticMessage::new(content.into()),
            code,
            stdout,
            stderr,
        }
    }

    #[track_caller]
    pub fn unexpected_error(content: impl Into<String>) -> Self {
        Self::UnexpectedError {
            content: DiagnosticMessage::new(content.into()),
        }
    }

    #[track_caller]
    pub fn io(content: impl Into<String>) -> Self {
        Self::Io {
            content: DiagnosticMessage::new(content.into()),
        }
    }
}

pub struct PythonExecutor {}
impl PythonExecutor {
    pub fn execute(
        job_name: &str,
        job_dir: impl Into<PathBuf>,
    ) -> Result<(), PythonExecutorError> {
        let job_dir = job_dir.into();
        let project_dir = if job_dir
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
        {
            job_dir
                .parent()
                .map(PathBuf::from)
                .unwrap_or_else(|| job_dir.clone())
        } else {
            job_dir.clone()
        };
        let env_dir = if project_dir.is_absolute() {
            project_dir.join(".venv")
        } else {
            env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(project_dir.join(".venv"))
        };
        let cache_dir = project_dir.join(".uv-cache");
        let mut command = Command::new("uv");
        if env::var_os("UV_PROJECT_ENVIRONMENT").is_none() {
            command.env("UV_PROJECT_ENVIRONMENT", env_dir);
        }
        if env::var_os("UV_CACHE_DIR").is_none() {
            command.env("UV_CACHE_DIR", cache_dir);
        }
        info!("python jobs dir {}", job_dir.display());

        let is_path_like = job_name.contains('/') || job_name.contains('\\');
        let is_python_file = job_name.ends_with(".py");

        let mut command = command
            .arg("run")
            .arg("--reinstall-package")
            .arg("dpf_python")
            .arg("--project")
            .arg(job_dir)
            // Ensure Python logs are flushed immediately.
            .env("PYTHONUNBUFFERED", "1")
            .args(if is_path_like || is_python_file {
                vec!["python".to_string(), "-u".to_string(), job_name.to_string()]
            } else {
                vec![
                    "python".to_string(),
                    "-u".to_string(),
                    "-m".to_string(),
                    job_name.to_string(),
                ]
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| PythonExecutorError::io(format!("Failed to execute uv: {}", e)))?;

        let stdout_buf = Arc::new(Mutex::new(String::new()));
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let log_mode = python_log_mode();

        let stdout_handle = {
            let stdout = command
                .stdout
                .take()
                .ok_or_else(|| PythonExecutorError::unexpected_error("Failed to capture stdout"))?;
            let stdout_buf = Arc::clone(&stdout_buf);
            let job_name = job_name.to_string();
            let log_mode = log_mode;
            thread::spawn(move || {
                let mut reader = BufReader::new(stdout);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => {
                            match log_mode {
                                PythonLogMode::Log => {
                                    let msg = line.trim_end_matches(&['\r', '\n'][..]);
                                    if !msg.is_empty() {
                                        info!("[python:{}] {}", job_name, msg);
                                    }
                                }
                                PythonLogMode::Raw => {
                                    let mut out = io::stdout().lock();
                                    let _ = out.write_all(line.as_bytes());
                                    let _ = out.flush();
                                }
                            }
                            if let Ok(mut buf) = stdout_buf.lock() {
                                buf.push_str(&line);
                            }
                        }
                        Err(_) => break,
                    }
                }
            })
        };

        let stderr_handle = {
            let stderr = command
                .stderr
                .take()
                .ok_or_else(|| PythonExecutorError::unexpected_error("Failed to capture stderr"))?;
            let stderr_buf = Arc::clone(&stderr_buf);
            let job_name = job_name.to_string();
            let log_mode = log_mode;
            thread::spawn(move || {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => {
                            match log_mode {
                                PythonLogMode::Log => {
                                    let msg = line.trim_end_matches(&['\r', '\n'][..]);
                                    if !msg.is_empty() {
                                        warn!("[python:{}] {}", job_name, msg);
                                    }
                                }
                                PythonLogMode::Raw => {
                                    let mut err = io::stderr().lock();
                                    let _ = err.write_all(line.as_bytes());
                                    let _ = err.flush();
                                }
                            }
                            if let Ok(mut buf) = stderr_buf.lock() {
                                buf.push_str(&line);
                            }
                        }
                        Err(_) => break,
                    }
                }
            })
        };

        let status = command
            .wait()
            .map_err(|e| PythonExecutorError::io(format!("Failed to wait for uv: {}", e)))?;

        let _ = stdout_handle.join();
        let _ = stderr_handle.join();

        if !status.success() {
            return Err(PythonExecutorError::failed_to_execute(
                format!("Failed to execute uv job '{}'", job_name),
                status.code(),
                stdout_buf.lock().map(|s| s.clone()).unwrap_or_default(),
                stderr_buf.lock().map(|s| s.clone()).unwrap_or_default(),
            ));
        }

        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::io::ErrorKind;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use tempfile::TempDir;
    use test_utils::TEST_MUTEX;

    struct EnvGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
            let original = env::var_os(key);
            env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => env::set_var(self.key, value),
                None => env::remove_var(self.key),
            }
        }
    }

    fn assert_uv_available() {
        let available = Command::new("uv")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);

        assert!(
            available,
            "uv binary is required for these tests; ensure it is installed and on PATH"
        );
    }

    fn detect_python() -> Option<String> {
        for candidate in ["python3", "python"] {
            if let Ok(output) = Command::new(candidate)
                .arg("-c")
                .arg("import sys; print(sys.executable)")
                .output()
            {
                if output.status.success() {
                    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if !path.is_empty() {
                        return Some(path);
                    }
                }
            }
        }
        None
    }

    fn create_env_shim(env_path: &Path, python: &str) -> PathBuf {
        let bin_dir = env_path.join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir for uv project env");

        let python_bin = bin_dir.join("python");
        if !python_bin.exists() {
            symlink_or_copy(python, &python_bin);
        }

        let python3_bin = bin_dir.join("python3");
        if !python3_bin.exists() {
            symlink_or_copy(&python_bin, &python3_bin);
        }

        python_bin
    }

    fn symlink_or_copy(src: impl AsRef<Path>, dest: &Path) {
        if let Err(err) = symlink(&src, dest) {
            if err.kind() != ErrorKind::AlreadyExists {
                if let Err(link_err) = fs::hard_link(&src, dest) {
                    if link_err.kind() != ErrorKind::AlreadyExists {
                        fs::copy(&src, dest).expect("copy python binary into uv env");
                    }
                }
            }
        }
    }

    fn write_pyproject(jobs_dir: &Path) {
        let pyproject = r#"[project]
name = "executor-test"
version = "0.0.0"
requires-python = ">=3.8"
dependencies = []
"#;

        fs::write(jobs_dir.join("pyproject.toml"), pyproject).expect("write pyproject.toml");
    }

    fn write_executable_script(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write python script");
        let mut permissions = fs::metadata(path)
            .expect("read script metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("set script executable");
    }

    #[test]
    fn executes_python_job_via_uv() {
        let _lock = TEST_MUTEX.lock().unwrap();

        assert_uv_available();
        let temp_dir = TempDir::new().expect("create temp dir");
        let _out_guard = EnvGuard::set(
            "DPF_TEST_OUTPUT_DIR",
            temp_dir
                .path()
                .to_str()
                .expect("temp dir should be valid utf-8"),
        );
        let jobs_dir = temp_dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir).expect("create jobs dir");
        write_pyproject(&jobs_dir);

        let cache_dir = jobs_dir.join(".uv-cache");
        fs::create_dir_all(&cache_dir).expect("create cache dir");
        let _cache_guard = EnvGuard::set("UV_CACHE_DIR", &cache_dir);

        let script_path = jobs_dir.join("sample_job.py");
        write_executable_script(
            &script_path,
            r#"#!/usr/bin/env python3
import os
import sys
from pathlib import Path

out_dir = Path(os.environ["DPF_TEST_OUTPUT_DIR"])
out_dir.joinpath("argv_value").write_text("\n".join(sys.argv))
"#,
        );

        PythonExecutor::execute(
            script_path
                .to_str()
                .expect("script path should be valid utf-8"),
            &jobs_dir,
        )
        .expect("executor should succeed");

        let argv_lines: Vec<_> = fs::read_to_string(temp_dir.path().join("argv_value"))
            .expect("argv value should exist")
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(argv_lines.len(), 1);
        assert_eq!(
            std::path::Path::new(&argv_lines[0]).file_name(),
            Some(std::ffi::OsStr::new("sample_job.py"))
        );
    }

    #[test]
    fn surfaces_uv_failure_with_output() {
        let _lock = TEST_MUTEX.lock().unwrap();

        assert_uv_available();
        let temp_dir = TempDir::new().expect("create temp dir");
        let jobs_dir = temp_dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir).expect("create jobs dir");
        write_pyproject(&jobs_dir);

        let cache_dir = jobs_dir.join(".uv-cache");
        fs::create_dir_all(&cache_dir).expect("create cache dir");
        let _cache_guard = EnvGuard::set("UV_CACHE_DIR", &cache_dir);

        let script_path = jobs_dir.join("failing_job.py");
        write_executable_script(
            &script_path,
            r#"#!/usr/bin/env python3
import sys
print("expected-stdout")
print("expected-stderr", file=sys.stderr)
sys.exit(42)
"#,
        );

        let err = PythonExecutor::execute(
            script_path
                .to_str()
                .expect("script path should be valid utf-8"),
            &jobs_dir,
        )
        .expect_err("uv failure should bubble up");

        match err {
            PythonExecutorError::FailedToExecute {
                code,
                stdout,
                stderr,
                ..
            } => {
                assert_eq!(code, Some(42));
                assert!(stdout.contains("expected-stdout"));
                assert!(stderr.contains("expected-stderr"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
