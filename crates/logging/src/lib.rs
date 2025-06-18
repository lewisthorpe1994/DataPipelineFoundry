// src/logging.rs
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
pub fn init_logger() {
    // Optionally allow RUST_LOG to override levels
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info") // fallback log level
    });

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(false)
                .with_level(true)
                .with_thread_names(true)
                .with_line_number(false)
                .with_file(false)
                .pretty() // 👈 Fancy pre-built output
        )
        .with(filter)
        .init();
}
