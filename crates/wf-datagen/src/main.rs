use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod cmd_bench;
mod cmd_gen;
mod cmd_helpers;
mod cmd_lint;
mod cmd_verify;

#[derive(Parser)]
#[command(name = "wf-datagen", about = "WarpFusion test data generator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate test data from a .wfg scenario file
    Gen {
        /// Path to the .wfg scenario file
        #[arg(long)]
        scenario: PathBuf,

        /// Output format: "jsonl" or "arrow" ("arrow-ipc"/"ipc" aliases)
        #[arg(long, default_value = "jsonl")]
        format: String,

        /// Output directory
        #[arg(long)]
        out: PathBuf,

        /// Additional .wfs schema files (beyond those in `use` declarations)
        #[arg(long)]
        ws: Vec<PathBuf>,

        /// Additional .wfl rule files (beyond those in `use` declarations)
        #[arg(long)]
        wfl: Vec<PathBuf>,

        /// Disable oracle generation even if the .wfg has an oracle block
        #[arg(long)]
        no_oracle: bool,
    },
    /// Lint (validate) a .wfg scenario file
    Lint {
        /// Path to the .wfg scenario file
        scenario: PathBuf,

        /// Additional .wfs schema files (beyond those in `use` declarations)
        #[arg(long)]
        ws: Vec<PathBuf>,

        /// Additional .wfl rule files (beyond those in `use` declarations)
        #[arg(long)]
        wfl: Vec<PathBuf>,
    },
    /// Verify actual alerts against oracle expectations
    Verify {
        /// Path to the oracle (expected) JSONL file
        #[arg(long)]
        expected: PathBuf,

        /// Path to the actual alerts JSONL file
        #[arg(long)]
        actual: PathBuf,

        /// Score tolerance for matching (overrides meta file if set)
        #[arg(long)]
        score_tolerance: Option<f64>,

        /// Time tolerance for matching in seconds (overrides meta file if set)
        #[arg(long)]
        time_tolerance: Option<f64>,

        /// Path to oracle meta JSON with tolerances (written by gen)
        #[arg(long)]
        meta: Option<PathBuf>,

        /// Output format: "json" or "markdown" (default: json)
        #[arg(long, default_value = "json")]
        format: String,
    },
    /// Measure pure generation throughput (no disk I/O)
    Bench {
        /// Path to the .wfg scenario file
        #[arg(long)]
        scenario: PathBuf,

        /// Additional .wfs schema files (beyond those in `use` declarations)
        #[arg(long)]
        ws: Vec<PathBuf>,

        /// Additional .wfl rule files (beyond those in `use` declarations)
        #[arg(long)]
        wfl: Vec<PathBuf>,

        /// Sustained bench duration (e.g. "30s", "2m"). Omit for single-shot.
        #[arg(long)]
        duration: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Gen {
            scenario,
            format,
            out,
            ws,
            wfl,
            no_oracle,
        } => cmd_gen::run(scenario, format, out, ws, wfl, no_oracle),
        Commands::Lint { scenario, ws, wfl } => cmd_lint::run(scenario, ws, wfl),
        Commands::Verify {
            expected,
            actual,
            score_tolerance,
            time_tolerance,
            meta,
            format,
        } => cmd_verify::run(expected, actual, score_tolerance, time_tolerance, meta, format),
        Commands::Bench {
            scenario,
            ws,
            wfl,
            duration,
        } => cmd_bench::run(scenario, ws, wfl, duration),
    }
}
