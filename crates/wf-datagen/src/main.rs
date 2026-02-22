use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use rand::SeedableRng;
use rand::rngs::StdRng;

use wf_datagen::datagen::fault_gen::apply_faults;
use wf_datagen::datagen::generate;
use wf_datagen::oracle::{OracleTolerances, extract_oracle_tolerances, run_oracle};
use wf_datagen::output::arrow_ipc::write_arrow_ipc;
use wf_datagen::output::jsonl::{
    read_alerts_jsonl, read_oracle_jsonl, write_jsonl, write_oracle_jsonl,
};
use wf_datagen::loader::load_from_uses;
use wf_datagen::validate::validate_wfg;
use wf_datagen::verify::verify;
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

            let (mut schemas, mut wfl_files) = load_from_uses(&wfg, &scenario, &HashMap::new())?;
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
            let mut compile_errors = Vec::new();
            for wfl_file in &wfl_files {
                match wf_lang::compile_wfl(wfl_file, &schemas) {
                    Ok(plans) => rule_plans.extend(plans),
                    Err(e) => {
                        compile_errors.push(e);
                    }
                }
            }

            // When oracle is enabled (syntax-priority: .wfg has oracle block),
            // WFL compilation failures are fatal — the user explicitly expects
            // oracle output, so silently skipping it is wrong.
            let oracle_requested = wfg.scenario.oracle.is_some() && !no_oracle;
            if !compile_errors.is_empty() {
                if oracle_requested {
                    for e in &compile_errors {
                        eprintln!("Error: WFL compilation failed: {}", e);
                    }
                    anyhow::bail!(
                        "WFL compilation failed and oracle block is present; \
                         fix the WFL errors or use --no-oracle"
                    );
                } else {
                    for e in &compile_errors {
                        eprintln!("Warning: WFL compilation failed: {}", e);
                    }
                }
            }

            // Generate clean events
            let result = generate(&wfg, &schemas, &rule_plans)?;

            // Oracle evaluation (on CLEAN events, before faults)
            // Syntax priority: oracle runs when .wfg has oracle block, unless --no-oracle
            let oracle_enabled = oracle_requested && !rule_plans.is_empty();
            let mut oracle_alert_count = 0;
            if oracle_enabled {
                let start = wfg.scenario.time_clause.start.parse().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid start time '{}': {}",
                        wfg.scenario.time_clause.start,
                        e
                    )
                })?;
                let duration = wfg.scenario.time_clause.duration;

                // SC7: only evaluate rules that have inject coverage
                let injected_rules: HashSet<String> = wfg
                    .scenario
                    .injects
                    .iter()
                    .map(|i| i.rule.clone())
                    .collect();

                let oracle_result = run_oracle(
                    &result.events,
                    &rule_plans,
                    &start,
                    &duration,
                    Some(&injected_rules),
                )?;
                oracle_alert_count = oracle_result.alerts.len();

                let oracle_file = out.join(format!("{}.oracle.jsonl", wfg.scenario.name));
                write_oracle_jsonl(&oracle_result.alerts, &oracle_file)?;
                println!(
                    "Oracle: {} alerts -> {}",
                    oracle_result.alerts.len(),
                    oracle_file.display()
                );

                // Write tolerances sidecar so `verify` can read them as defaults
                let tolerances = wfg
                    .scenario
                    .oracle
                    .as_ref()
                    .map(extract_oracle_tolerances)
                    .unwrap_or_default();
                let meta_file = out.join(format!("{}.oracle.meta.json", wfg.scenario.name));
                let meta_json = serde_json::to_string_pretty(&tolerances)?;
                std::fs::write(&meta_file, meta_json)?;
            }
            let _ = oracle_alert_count;

            // Apply faults (after oracle, on clean events)
            let has_faults = wfg.scenario.faults.is_some();
            let output_events = if let Some(faults) = &wfg.scenario.faults {
                let mut fault_rng = StdRng::seed_from_u64(wfg.scenario.seed.wrapping_add(1));
                let fault_result = apply_faults(result.events, faults, &mut fault_rng);
                eprintln!("Faults applied: {}", fault_result.stats);
                fault_result.events
            } else {
                result.events
            };

            // Post-fault oracle (M33 P2): run oracle again on faulted events
            // so verify can compare clean vs faulted outcomes.
            if oracle_enabled && has_faults {
                let start = wfg.scenario.time_clause.start.parse().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid start time '{}': {}",
                        wfg.scenario.time_clause.start,
                        e
                    )
                })?;
                let duration = wfg.scenario.time_clause.duration;

                let injected_rules: HashSet<String> = wfg
                    .scenario
                    .injects
                    .iter()
                    .map(|i| i.rule.clone())
                    .collect();

                let faulted_oracle = run_oracle(
                    &output_events,
                    &rule_plans,
                    &start,
                    &duration,
                    Some(&injected_rules),
                )?;

                let faulted_oracle_file =
                    out.join(format!("{}.faulted-oracle.jsonl", wfg.scenario.name));
                write_oracle_jsonl(&faulted_oracle.alerts, &faulted_oracle_file)?;
                println!(
                    "Faulted oracle: {} alerts -> {}",
                    faulted_oracle.alerts.len(),
                    faulted_oracle_file.display()
                );
            }

            // Write output
            match normalized_format {
                "jsonl" => {
                    let output_file = out.join(format!("{}.jsonl", wfg.scenario.name));
                    write_jsonl(&output_events, &output_file)?;
                    println!(
                        "Generated {} events -> {}",
                        output_events.len(),
                        output_file.display()
                    );
                }
                "arrow" => {
                    let output_file = out.join(format!("{}.arrow", wfg.scenario.name));
                    write_arrow_ipc(&output_events, &output_file)?;
                    println!(
                        "Generated {} events -> {}",
                        output_events.len(),
                        output_file.display()
                    );
                }
                _ => unreachable!(),
            }

            Ok(())
        }
        Commands::Lint { scenario, ws, wfl } => {
            let wfg_content = std::fs::read_to_string(&scenario).context("reading .wfg file")?;
            let wfg = parse_wfg(&wfg_content).context("parsing .wfg file")?;

            let (mut schemas, mut wfl_files) = load_from_uses(&wfg, &scenario, &HashMap::new())?;
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
            time_tolerance,
            meta,
            format,
        } => {
            // Load tolerances: CLI flags > meta file > defaults
            let base_tolerances = if let Some(meta_path) = &meta {
                let content = std::fs::read_to_string(meta_path)
                    .with_context(|| format!("reading meta: {}", meta_path.display()))?;
                serde_json::from_str::<OracleTolerances>(&content)
                    .with_context(|| format!("parsing meta: {}", meta_path.display()))?
            } else {
                OracleTolerances::default()
            };

            let effective_score_tol = score_tolerance.unwrap_or(base_tolerances.score_tolerance);
            let effective_time_tol = time_tolerance.unwrap_or(base_tolerances.time_tolerance_secs);

            let oracle_alerts = read_oracle_jsonl(&expected)
                .with_context(|| format!("reading expected: {}", expected.display()))?;
            let actual_alerts = read_alerts_jsonl(&actual)
                .with_context(|| format!("reading actual: {}", actual.display()))?;

            let report = verify(
                &oracle_alerts,
                &actual_alerts,
                effective_score_tol,
                effective_time_tol,
            );

            match format.as_str() {
                "markdown" | "md" => {
                    println!("{}", report.to_markdown());
                }
                _ => {
                    let json = serde_json::to_string_pretty(&report)?;
                    println!("{}", json);
                }
            }

            if report.status == "pass" {
                std::process::exit(0);
            } else {
                std::process::exit(1);
            }
        }
        Commands::Bench {
            scenario,
            ws,
            wfl,
            duration: bench_duration,
        } => {
            let wfg_content = std::fs::read_to_string(&scenario).context("reading .wfg file")?;
            let wfg = parse_wfg(&wfg_content).context("parsing .wfg file")?;

            let (mut schemas, mut wfl_files) = load_from_uses(&wfg, &scenario, &HashMap::new())?;
            schemas.extend(load_ws_files(&ws)?);
            wfl_files.extend(load_wfl_files(&wfl)?);

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

            let sustained = bench_duration
                .map(|s| parse_bench_duration(&s))
                .transpose()?;

            match sustained {
                Some(target_dur) => {
                    // Sustained bench: repeatedly generate for the target duration
                    let wall_start = std::time::Instant::now();
                    let mut iterations: u64 = 0;
                    let mut total_events: u64 = 0;

                    while wall_start.elapsed() < target_dur {
                        let result = generate(&wfg, &schemas, &rule_plans)?;
                        total_events += result.events.len() as u64;
                        iterations += 1;
                    }

                    let elapsed = wall_start.elapsed();
                    let secs = elapsed.as_secs_f64();
                    let eps = if secs > 0.0 {
                        total_events as f64 / secs
                    } else {
                        f64::INFINITY
                    };

                    println!("Iterations: {}", iterations);
                    println!("Events:     {}", total_events);
                    println!("Duration:   {:.3}s", secs);
                    println!("Throughput: {:.0} events/sec", eps);
                }
                None => {
                    // Single-shot bench (original behavior)
                    let start = std::time::Instant::now();
                    let result = generate(&wfg, &schemas, &rule_plans)?;
                    let elapsed = start.elapsed();

                    let events = result.events.len();
                    let secs = elapsed.as_secs_f64();
                    let eps = if secs > 0.0 {
                        events as f64 / secs
                    } else {
                        f64::INFINITY
                    };

                    println!("Events:     {}", events);
                    println!("Duration:   {:.3}s", secs);
                    println!("Throughput: {:.0} events/sec", eps);
                }
            }

            Ok(())
        }
    }
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

/// Parse a human-friendly duration string (e.g. "30s", "2m", "1h") into `std::time::Duration`.
fn parse_bench_duration(s: &str) -> anyhow::Result<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty duration string");
    }

    let (num_str, suffix) = if let Some(stripped) = s.strip_suffix('s') {
        (stripped, "s")
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, "m")
    } else if let Some(stripped) = s.strip_suffix('h') {
        (stripped, "h")
    } else {
        // Assume seconds if no suffix
        (s, "s")
    };

    let value: f64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration number: '{}'", num_str))?;

    let secs = match suffix {
        "s" => value,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        _ => unreachable!(),
    };

    if secs <= 0.0 {
        anyhow::bail!("bench duration must be positive, got '{}'", s);
    }

    Ok(std::time::Duration::from_secs_f64(secs))
}
