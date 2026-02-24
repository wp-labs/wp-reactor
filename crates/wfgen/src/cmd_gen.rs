use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Context;
use rand::SeedableRng;
use rand::rngs::StdRng;

use wfgen::datagen::fault_gen::apply_faults;
use wfgen::datagen::generate;
use wfgen::loader::load_from_uses;
use wfgen::oracle::{extract_oracle_tolerances, run_oracle};
use wfgen::output::arrow_ipc::write_arrow_ipc;
use wfgen::output::jsonl::{write_jsonl, write_oracle_jsonl};
use wfgen::validate::validate_wfg;
use wfgen::wfg_parser::parse_wfg;

use crate::cmd_helpers::{load_wfl_files, load_ws_files};

pub(crate) fn run(
    scenario: PathBuf,
    format: String,
    out: PathBuf,
    ws: Vec<PathBuf>,
    wfl: Vec<PathBuf>,
    no_oracle: bool,
) -> anyhow::Result<()> {
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

        let faulted_oracle_file = out.join(format!("{}.faulted-oracle.jsonl", wfg.scenario.name));
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
