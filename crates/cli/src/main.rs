mod commands;

use crate::commands::compile::handle_compile;
use crate::commands::handle_init;
use crate::commands::init::InitArgs;
use crate::commands::kafka::{handle_kafka, KafkaSubcommand};
use crate::commands::run::RunArgs;
use clap::{Parser, Subcommand};
use common::error::FFError;
use std::path::PathBuf;
use time::macros::format_description;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "foundry")]
pub struct Cli {
    #[arg(
        long = "config-path",
        short = 'c',
        help = "path to config file",
        global = true
    )]
    pub config_path: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Create a new project scaffold
    Init(InitArgs),
    /// Compile the DAG and emit SQL
    Compile,
    /// Commands for kafka connectors
    #[command(subcommand)]
    Kafka(KafkaSubcommand),
    /// Run the model DAG using the chosen target
    Run(RunArgs),
    /// Print the model dependency graph
    Graph,
    /// Clean generated files
    Clean,
}
fn run_cmd(func: Result<(), FFError>) {
    if let Err(e) = func {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info") // fallback log level
    });
    let time_format =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:2]");

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
                .compact(), // 👈 Fancy pre-built output
        )
        .with(filter)
        .init();
    let cli = Cli::parse();

    match cli.command {
        Cmd::Init(args) => {
            if let Err(e) = handle_init(&args.path, args.project_name, args.flow_arch) {
                eprintln!(
                    "Failed to initialize project at {}: {}",
                    args.path.display(),
                    e
                );
                std::process::exit(1);
            }
        }
        Cmd::Compile => {
            if let Err(e) = handle_compile(cli.config_path.clone()) {
                eprintln!("Compilation failed: {}", e);
                std::process::exit(1);
            }
        }
        Cmd::Kafka(args) => run_cmd(handle_kafka(&args, cli.config_path.clone())),
        Cmd::Run(args) => {
            // if let Err(e) = handle_run(args.model, cli.config_path) {
            //     eprintln!("Run failed: {}", e);
            //     std::process::exit(1);
            // }
            unimplemented!("Not implemented yet!");
        }
        _ => unimplemented!("Not implemented yet!"),
    }
}
