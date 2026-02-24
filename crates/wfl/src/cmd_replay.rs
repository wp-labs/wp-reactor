use std::collections::HashMap;
use std::io::{BufRead, BufReader, IsTerminal};
use std::path::PathBuf;

use anyhow::Result;

use wf_core::alert::AlertRecord;
use wf_core::rule::{
    CepStateMachine, CloseReason, Event, RuleExecutor, StepResult, Value, WindowLookup,
};
use wf_lang::WindowSchema;
use wf_lang::plan::RulePlan;

const GREEN: &str = "\x1b[1;32m";
const RED: &str = "\x1b[1;31m";
const YELLOW: &str = "\x1b[1;38;5;208m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

/// Result of replaying events through compiled rules.
pub struct ReplayResult {
    pub alerts: Vec<AlertRecord>,
    pub event_count: u64,
    pub match_count: u64,
    pub error_count: u64,
}

/// CLI entry point: load files → replay → print output.
pub fn run(
    file: PathBuf,
    schemas: Vec<String>,
    input: PathBuf,
    alias: String,
    vars: Vec<String>,
) -> Result<()> {
    use wf_config::project::{load_schemas, load_wfl, parse_vars};

    let cwd = std::env::current_dir()?;
    let var_map = parse_vars(&vars)?;
    let color = std::io::stderr().is_terminal();

    let all_schemas = load_schemas(&schemas, &cwd)?;
    let source = load_wfl(&file, &var_map)?;

    let reader = BufReader::new(
        std::fs::File::open(&input)
            .map_err(|e| anyhow::anyhow!("failed to open {}: {}", input.display(), e))?,
    );

    let result = replay_events(&source, &all_schemas, reader, &alias, color)?;

    for alert in &result.alerts {
        match serde_json::to_string(alert) {
            Ok(s) => println!("{}", s),
            Err(e) => eprintln!("ERROR: failed to serialize alert: {}", e),
        }
    }

    // Summary
    eprintln!("---");
    if color {
        let ev = result.event_count;
        let mc = result.match_count;
        let ec = result.error_count;
        eprint!("{BOLD}Replay complete:{RESET} {ev} events processed, ");
        if mc > 0 {
            eprint!("{GREEN}{mc} matches{RESET}");
        } else {
            eprint!("{DIM}0 matches{RESET}");
        }
        eprint!(", ");
        if ec > 0 {
            eprintln!("{RED}{ec} errors{RESET}");
        } else {
            eprintln!("{DIM}0 errors{RESET}");
        }
    } else {
        eprintln!(
            "Replay complete: {} events processed, {} matches, {} errors",
            result.event_count, result.match_count, result.error_count
        );
    }

    Ok(())
}

/// Pure-logic replay: parse WFL source, compile, and replay events from reader.
///
/// Returns all alerts plus statistics. This function is testable without
/// filesystem access.
pub fn replay_events<R: BufRead>(
    wfl_source: &str,
    schemas: &[WindowSchema],
    reader: R,
    alias: &str,
    color: bool,
) -> Result<ReplayResult> {
    let wfl_file =
        wf_lang::parse_wfl(wfl_source).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;
    let plans = wf_lang::compile_wfl(&wfl_file, schemas)?;

    if plans.is_empty() {
        return Ok(ReplayResult {
            alerts: vec![],
            event_count: 0,
            match_count: 0,
            error_count: 0,
        });
    }

    replay_with_plans(&plans, schemas, reader, alias, color)
}

/// Stub [`WindowLookup`] for replay mode.
///
/// Replay operates without a live window store, so join lookups and
/// `window.has()` guards always return `None` (no data available).
struct NullWindowLookup;

impl WindowLookup for NullWindowLookup {
    fn snapshot_field_values(
        &self,
        _window: &str,
        _field: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }

    fn snapshot(&self, _window: &str) -> Option<Vec<std::collections::HashMap<String, Value>>> {
        None
    }
}

/// Replay events against pre-compiled rule plans.
fn replay_with_plans<R: BufRead>(
    plans: &[RulePlan],
    schemas: &[WindowSchema],
    reader: R,
    alias: &str,
    color: bool,
) -> Result<ReplayResult> {
    let mut engines: Vec<(CepStateMachine, RuleExecutor)> = plans
        .iter()
        .map(|plan| {
            // Resolve time_field from the schema that matches the replay alias.
            // For multi-source rules (events { a: win_a, b: win_b }), the alias
            // determines which window's time_field to use for event-time extraction.
            let time_field = plan
                .binds
                .iter()
                .find(|b| b.alias == alias)
                .and_then(|b| schemas.iter().find(|s| s.name == b.window))
                .and_then(|s| s.time_field.clone());

            let limits = plan.limits_plan.clone();
            let sm = CepStateMachine::with_limits(
                plan.name.clone(),
                plan.match_plan.clone(),
                time_field,
                limits,
            );
            let executor = RuleExecutor::new(plan.clone());
            (sm, executor)
        })
        .collect();

    let lookup = NullWindowLookup;
    let mut alerts = Vec::new();
    let mut event_count: u64 = 0;
    let mut match_count: u64 = 0;
    let mut error_count: u64 = 0;

    // -- Event loop --
    for line_result in reader.lines() {
        let line = line_result?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                if color {
                    eprintln!(
                        "{YELLOW}WARN{RESET}: skipping invalid JSON on line {}: {}",
                        event_count + 1,
                        e
                    );
                } else {
                    eprintln!(
                        "WARN: skipping invalid JSON on line {}: {}",
                        event_count + 1,
                        e
                    );
                }
                error_count += 1;
                continue;
            }
        };

        let event = json_to_event(&json);
        event_count += 1;

        for (sm, executor) in &mut engines {
            match sm.advance_with(alias, &event, Some(&lookup)) {
                StepResult::Matched(ctx) => {
                    match executor.execute_match_with_joins(&ctx, &lookup) {
                        Ok(alert) => {
                            alerts.push(alert);
                            match_count += 1;
                        }
                        Err(e) => {
                            if color {
                                eprintln!("{RED}ERROR{RESET}: execute_match failed: {}", e);
                            } else {
                                eprintln!("ERROR: execute_match failed: {}", e);
                            }
                            error_count += 1;
                        }
                    }
                }
                StepResult::Advance | StepResult::Accumulate => {}
            }
        }
    }

    // -- EOF: close all remaining instances --
    for (sm, executor) in &mut engines {
        for close in &sm.close_all(CloseReason::Eos) {
            match executor.execute_close_with_joins(close, &lookup) {
                Ok(Some(alert)) => {
                    alerts.push(alert);
                    match_count += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    if color {
                        eprintln!("{RED}ERROR{RESET}: execute_close failed: {}", e);
                    } else {
                        eprintln!("ERROR: execute_close failed: {}", e);
                    }
                    error_count += 1;
                }
            }
        }
    }

    Ok(ReplayResult {
        alerts,
        event_count,
        match_count,
        error_count,
    })
}

/// Convert a serde_json::Value (object) into our Event type.
pub fn json_to_event(json: &serde_json::Value) -> Event {
    let mut fields = HashMap::new();
    if let serde_json::Value::Object(map) = json {
        for (key, val) in map {
            let v = match val {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        Value::Number(f)
                    } else {
                        continue;
                    }
                }
                serde_json::Value::String(s) => Value::Str(s.clone()),
                serde_json::Value::Bool(b) => Value::Bool(*b),
                _ => continue, // skip arrays, objects, nulls
            };
            fields.insert(key.clone(), v);
        }
    }
    Event { fields }
}
