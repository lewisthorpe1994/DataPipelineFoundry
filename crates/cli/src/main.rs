mod commands;

use clap::{Parser, Subcommand};
use commands::{handle_init, handle_compile, handle_run};
use crate::commands::init::InitArgs;
use tracing_subscriber::{fmt};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use time::format_description::FormatItem;
use time::macros::format_description;

#[derive(Parser)]
#[command(name = "forgery")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Create a new project scaffold
    Init(InitArgs),
    /// Compile the DAG and emit SQL
    Compile,
    /// Run the model DAG using the chosen target (postgres or datafusion)
    Run {
        #[arg(short, long, default_value = "postgres", help = "Execution backend")]
        target: String,
    },
    /// Print the model dependency graph
    Graph,
    /// Clean generated files
    Clean,
}


fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info") // fallback log level
    });
    let time_format = format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:2]");
    
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_timer(fmt::time::LocalTime::new(time_format))
                .with_target(false)
                .with_level(true)
                .with_thread_names(false)
                .with_line_number(false)
                .with_file(false)
                .with_span_events(fmt::format::FmtSpan::NONE) // 👈 Disable span name output
                .compact() // 👈 Fancy pre-built output
        )
        .with(filter)
        .init();
    let cli = Cli::parse();
    
    match cli.command {
        Cmd::Init(args) => {
            if let Err(e) = handle_init(&args.path, args.project_name, args.flow_arch) {
                eprintln!("Failed to initialize project at {}: {}", args.path.display(), e);
                std::process::exit(1);
            }
        }
        Cmd::Compile => {
            if let Err(e) = handle_compile() {
                eprintln!("Compilation failed: {}", e);
                std::process::exit(1);
            }
        }
        Cmd::Run { target } => {
            if let Err(e) = handle_run(target) {
                eprintln!("Run failed: {}", e);
                std::process::exit(1);
            }
        }
        _ => unimplemented!("Not implemented yet!"),
    }
}
