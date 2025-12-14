use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn main() {
    println!("cargo:rerun-if-env-changed=UV_SKIP_INSTALL");
    println!("cargo:rerun-if-env-changed=UV_FORCE_INSTALL");

    if env::var_os("UV_SKIP_INSTALL").is_some() {
        println!("cargo:warning=skipping uv install because UV_SKIP_INSTALL is set");
        return;
    }

    let force_install = env::var_os("UV_FORCE_INSTALL").is_some();
    let install_dir = cargo_bin_dir();

    if !force_install && uv_available() {
        return;
    }

    if let Some(dir) = &install_dir {
        println!(
            "cargo:warning=uv not found on PATH; attempting install into {}",
            dir.display()
        );
    } else {
        println!("cargo:warning=uv not found on PATH; attempting install");
    }

    install_uv(install_dir.as_deref()).unwrap_or_else(|err| {
        panic!("failed to install uv: {err}");
    });

    if !uv_available() {
        let path_hint = install_dir
            .as_ref()
            .map(|p| format!(" Ensure {} is on your PATH.", p.display()))
            .unwrap_or_default();
        panic!("uv installation completed but uv is still not available on PATH.{path_hint}");
    }
}

fn uv_available() -> bool {
    Command::new("uv")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn cargo_bin_dir() -> Option<PathBuf> {
    let home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")));
    home.map(|p| p.join("bin"))
}

#[cfg(windows)]
fn install_uv(install_dir: Option<&Path>) -> Result<(), String> {
    let mut cmd = Command::new("powershell");
    cmd.args([
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "iwr https://astral.sh/uv/install.ps1 -useb | iex",
    ]);

    if let Some(dir) = install_dir {
        cmd.env("UV_INSTALL_DIR", dir);
    }

    let status = cmd
        .status()
        .map_err(|err| format!("unable to launch PowerShell: {err}"))?;

    if !status.success() {
        return Err(format!("uv installer exited with status {status}"));
    }

    Ok(())
}

#[cfg(not(windows))]
fn install_uv(install_dir: Option<&Path>) -> Result<(), String> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("curl -LsSf https://astral.sh/uv/install.sh | sh");

    if let Some(dir) = install_dir {
        cmd.env("UV_INSTALL_DIR", dir);
    }

    let status = cmd
        .status()
        .map_err(|err| format!("unable to launch shell installer: {err}"))?;

    if !status.success() {
        return Err(format!("uv installer exited with status {status}"));
    }

    Ok(())
}
