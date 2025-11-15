use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let ui_dir = manifest_dir
        .parent()
        .expect("backend crate not inside foundry_web")
        .join("ui");

    let package_json = ui_dir.join("package.json");
    let lock_file = ui_dir.join("package-lock.json");

    println!("cargo:rerun-if-changed={}", package_json.display());
    println!("cargo:rerun-if-changed={}", lock_file.display());

    if !needs_install(&ui_dir, &lock_file) {
        return;
    }

    println!(
        "cargo:warning=running `npm install` in {}",
        ui_dir.display()
    );
    let status = Command::new("npm")
        .arg("install")
        .current_dir(&ui_dir)
        .status()
        .expect("failed to spawn npm; ensure Node.js/npm are installed");

    if !status.success() {
        panic!("`npm install` failed with status {status:?}");
    }

    let _ = fs::write(ui_dir.join(".npm-install.stamp"), b"npm install completed");
}

fn needs_install(ui_dir: &Path, lock_file: &Path) -> bool {
    let stamp = ui_dir.join(".npm-install.stamp");

    if !stamp.exists() {
        return true;
    }

    match (mod_time(lock_file), mod_time(&stamp)) {
        (Some(lock_time), Some(stamp_time)) => lock_time > stamp_time,
        _ => true,
    }
}

fn mod_time(path: &Path) -> Option<std::time::SystemTime> {
    fs::metadata(path).and_then(|m| m.modified()).ok()
}
