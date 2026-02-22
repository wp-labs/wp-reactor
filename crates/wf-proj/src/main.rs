use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod cmd_explain;
mod cmd_fmt;
mod cmd_lint;

#[derive(Parser)]
#[command(name = "wf-proj", about = "WarpFusion project tools for rule developers")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Explain compiled rules in human-readable form
    Explain {
        /// Path to the .wfl rule file
        file: PathBuf,

        /// Schema file glob patterns (e.g. "schemas/*.wfs")
        #[arg(short, long)]
        schemas: Vec<String>,

        /// Variable substitutions in KEY=VALUE format
        #[arg(long)]
        var: Vec<String>,
    },

    /// Run lint checks on a .wfl rule file
    Lint {
        /// Path to the .wfl rule file
        file: PathBuf,

        /// Schema file glob patterns (e.g. "schemas/*.wfs")
        #[arg(short, long)]
        schemas: Vec<String>,

        /// Variable substitutions in KEY=VALUE format
        #[arg(long)]
        var: Vec<String>,
    },

    /// Format .wfl rule files
    Fmt {
        /// Input .wfl files to format
        files: Vec<PathBuf>,

        /// Write formatted output back to the files (in-place)
        #[arg(short, long)]
        write: bool,

        /// Check if files are already formatted (exit 1 if not)
        #[arg(long)]
        check: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Explain {
            file,
            schemas,
            var,
        } => {
            cmd_explain::run(file, schemas, var)?;
        }

        Commands::Lint {
            file,
            schemas,
            var,
        } => {
            cmd_lint::run(file, schemas, var)?;
        }

        Commands::Fmt {
            files,
            write,
            check,
        } => {
            cmd_fmt::run(files, write, check)?;
        }
    }

    Ok(())
}
