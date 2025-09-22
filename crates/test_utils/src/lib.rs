use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Global mutex to serialize tests that modify the process working directory.
/// Changing the directory concurrently can lead to nondeterministic failures.
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Connection details used when writing `connections.yml` for a test project.
pub fn get_root_dir() -> PathBuf {
    let workspace_root = std::env::var("CARGO_WORKSPACE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .ancestors()
                .nth(2)
                .expect("crate should live under <workspace>/crates/<crate>")
                .to_path_buf()
        });
    let root = workspace_root.join("example/foundry-project");

    root
}

pub fn build_test_layers(root: PathBuf) -> HashMap<String, String> {
    // Build the layer map exactly as the loader does
    let layers = HashMap::from([
        (
            "bronze".to_string(),
            root.join("foundry_models/bronze").display().to_string(),
        ),
        (
            "silver".to_string(),
            root.join("foundry_models/silver").display().to_string(),
        ),
        (
            "gold".to_string(),
            root.join("foundry_models/gold").display().to_string(),
        ),
    ]);
    layers
}

/// Temporarily change the current working directory for the duration of the closure.
/// Guards against concurrent `chdir` calls by taking the global `TEST_MUTEX` lock.
/// Always restores the original directory, even if the closure panics.
pub fn with_chdir<F, T>(target: impl AsRef<Path>, f: F) -> std::io::Result<T>
where
    F: FnOnce() -> T,
{
    let _lock = TEST_MUTEX.lock().unwrap();

    let original = env::current_dir()?;
    env::set_current_dir(target.as_ref())?;

    struct Reset(PathBuf);
    impl Drop for Reset {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.0);
        }
    }
    let _guard = Reset(original);

    Ok(f())
}
