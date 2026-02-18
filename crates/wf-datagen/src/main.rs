use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};

use wf_datagen::datagen::generate;
use wf_datagen::oracle::run_oracle;
use wf_datagen::output::arrow_ipc::write_arrow_ipc;
use wf_datagen::output::jsonl::{
    read_alerts_jsonl, read_oracle_jsonl, write_jsonl, write_oracle_jsonl,
};
use wf_datagen::validate::validate_wfg;
use wf_datagen::verify::verify;
use wf_datagen::wfg_ast::WfgFile;
use wf_datagen::wfg_parser::parse_wfg;

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

        /// Run the reference evaluator and output oracle alerts
        #[arg(long)]
        oracle: bool,
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

        /// Score tolerance for matching (default: 0.01)
        #[arg(long, default_value = "0.01")]
        score_tolerance: f64,
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
            oracle,
        } => {
            let normalized_format = match format.as_str() {
                "jsonl" => "jsonl",
                "arrow" | "arrow-ipc" | "ipc" => "arrow",
                _ => "",
            };
            if normalized_format.is_empty() {
                anyhow::bail!(
                    "unsupported format: '{}'. Supported: 'jsonl', 'arrow' ('arrow-ipc' alias).",
                    format
                );
            }

            let wfg_content = std::fs::read_to_string(&scenario).context("reading .wfg file")?;
            let wfg = parse_wfg(&wfg_content).context("parsing .wfg file")?;

            let (mut schemas, mut wfl_files) = load_from_uses(&wfg, &scenario)?;
            schemas.extend(load_ws_files(&ws)?);
            wfl_files.extend(load_wfl_files(&wfl)?);

            let errors = validate_wfg(&wfg, &schemas, &wfl_files);
            if !errors.is_empty() {
                eprintln!("Validation errors:");
                for e in &errors {
                    eprintln!("  {}", e);
                }
                anyhow::bail!("{} validation error(s) found", errors.len());
            }

            // Compile WFL rules
            let mut rule_plans = Vec::new();
            for wfl_file in &wfl_files {
                match wf_lang::compile_wfl(wfl_file, &schemas) {
                    Ok(plans) => rule_plans.extend(plans),
                    Err(e) => {
                        eprintln!("Warning: WFL compilation failed: {}", e);
                    }
                }
            }

            let result = generate(&wfg, &schemas, &rule_plans)?;

            match normalized_format {
                "jsonl" => {
                    let output_file = out.join(format!("{}.jsonl", wfg.scenario.name));
                    write_jsonl(&result.events, &output_file)?;
                    println!(
                        "Generated {} events -> {}",
                        result.events.len(),
                        output_file.display()
                    );
                }
                "arrow" => {
                    let output_file = out.join(format!("{}.arrow", wfg.scenario.name));
                    write_arrow_ipc(&result.events, &output_file)?;
                    println!(
                        "Generated {} events -> {}",
                        result.events.len(),
                        output_file.display()
                    );
                }
                _ => unreachable!(),
            }

            // Oracle evaluation
            if oracle && !rule_plans.is_empty() {
                let start = wfg.scenario.time_clause.start.parse().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid start time '{}': {}",
                        wfg.scenario.time_clause.start,
                        e
                    )
                })?;
                let duration = wfg.scenario.time_clause.duration;

                let oracle_result =
                    run_oracle(&result.events, &rule_plans, &start, &duration)?;

                let oracle_file =
                    out.join(format!("{}.oracle.jsonl", wfg.scenario.name));
                write_oracle_jsonl(&oracle_result.alerts, &oracle_file)?;
                println!(
                    "Oracle: {} alerts -> {}",
                    oracle_result.alerts.len(),
                    oracle_file.display()
                );
            }

            Ok(())
        }
        Commands::Lint { scenario, ws, wfl } => {
            let wfg_content = std::fs::read_to_string(&scenario).context("reading .wfg file")?;
            let wfg = parse_wfg(&wfg_content).context("parsing .wfg file")?;

            let (mut schemas, mut wfl_files) = load_from_uses(&wfg, &scenario)?;
            schemas.extend(load_ws_files(&ws)?);
            wfl_files.extend(load_wfl_files(&wfl)?);

            let errors = validate_wfg(&wfg, &schemas, &wfl_files);
            if errors.is_empty() {
                println!("OK");
            } else {
                for e in &errors {
                    eprintln!("{}", e);
                }
                std::process::exit(1);
            }
            Ok(())
        }
        Commands::Verify {
            expected,
            actual,
            score_tolerance,
        } => {
            let oracle_alerts = read_oracle_jsonl(&expected)
                .with_context(|| format!("reading expected: {}", expected.display()))?;
            let actual_alerts = read_alerts_jsonl(&actual)
                .with_context(|| format!("reading actual: {}", actual.display()))?;

            let report = verify(&oracle_alerts, &actual_alerts, score_tolerance);

            let json = serde_json::to_string_pretty(&report)?;
            println!("{}", json);

            if report.status == "pass" {
                std::process::exit(0);
            } else {
                std::process::exit(1);
            }
        }
    }
}

/// Auto-load .wfs and .wfl files referenced by `use` declarations in the .wfg file.
///
/// Paths in `use` declarations are resolved relative to the .wfg file's directory.
fn load_from_uses(
    wfg: &WfgFile,
    wsc_path: &Path,
) -> anyhow::Result<(Vec<wf_lang::WindowSchema>, Vec<wf_lang::ast::WflFile>)> {
    let base_dir = wsc_path.parent().unwrap_or_else(|| Path::new("."));

    let mut schemas = Vec::new();
    let mut wfl_files = Vec::new();

    for use_decl in &wfg.uses {
        let resolved = base_dir.join(&use_decl.path);
        let ext = resolved.extension().and_then(|e| e.to_str()).unwrap_or("");

        match ext {
            "wfs" => {
                let content = std::fs::read_to_string(&resolved).with_context(|| {
                    format!(
                        "reading .wfs file from use declaration: {} (resolved: {})",
                        use_decl.path,
                        resolved.display()
                    )
                })?;
                let parsed = wf_lang::parse_wfs(&content)
                    .with_context(|| format!("parsing .wfs file: {}", resolved.display()))?;
                schemas.extend(parsed);
            }
            "wfl" => {
                let content = std::fs::read_to_string(&resolved).with_context(|| {
                    format!(
                        "reading .wfl file from use declaration: {} (resolved: {})",
                        use_decl.path,
                        resolved.display()
                    )
                })?;
                let parsed = wf_lang::parse_wfl(&content)
                    .with_context(|| format!("parsing .wfl file: {}", resolved.display()))?;
                wfl_files.push(parsed);
            }
            other => {
                anyhow::bail!(
                    "unsupported file extension '{}' in use declaration: {}",
                    other,
                    use_decl.path
                );
            }
        }
    }

    Ok((schemas, wfl_files))
}

fn load_ws_files(paths: &[PathBuf]) -> anyhow::Result<Vec<wf_lang::WindowSchema>> {
    let mut schemas = Vec::new();
    for path in paths {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading .wfs file: {}", path.display()))?;
        let parsed = wf_lang::parse_wfs(&content)
            .with_context(|| format!("parsing .wfs file: {}", path.display()))?;
        schemas.extend(parsed);
    }
    Ok(schemas)
}

fn load_wfl_files(paths: &[PathBuf]) -> anyhow::Result<Vec<wf_lang::ast::WflFile>> {
    let mut files = Vec::new();
    for path in paths {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading .wfl file: {}", path.display()))?;
        let parsed = wf_lang::parse_wfl(&content)
            .with_context(|| format!("parsing .wfl file: {}", path.display()))?;
        files.push(parsed);
    }
    Ok(files)
}
