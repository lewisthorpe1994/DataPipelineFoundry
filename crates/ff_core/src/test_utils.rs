use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Global mutex used to serialize tests that change the process working
/// directory. Changing the current directory in parallel tests can lead to
/// unpredictable failures, therefore tests that rely on it should lock this
/// mutex.
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

