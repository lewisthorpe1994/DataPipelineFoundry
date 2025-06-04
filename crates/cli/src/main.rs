mod commands;

use std::path::PathBuf;
use clap::{Parser, Subcommand};
use commands::handle_init;
use crate::commands::init::InitArgs;

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
    let cli = Cli::parse();
    
    match cli.command {
        Cmd::Init(args) => {
            if let Err(e) = handle_init(&args.path, args.project_name, args.flow_arch) {
                eprintln!("Failed to initialize project at {}: {}", args.path.display(), e);
                std::process::exit(1);
            }
        }
        _ => unimplemented!("Not implemented yet!"),
    }
}
