use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::Result;

use wf_config::project::{load_schemas, load_wfl, parse_vars};
use wf_core::rule::{CepStateMachine, Event, RuleExecutor, StepResult, Value};

pub fn run(
    file: PathBuf,
    schemas: Vec<String>,
    input: PathBuf,
    alias: String,
    vars: Vec<String>,
) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let var_map = parse_vars(&vars)?;

    // Load schemas
    let all_schemas = load_schemas(&schemas, &cwd)?;

    // Load and preprocess the .wfl file
    let source = load_wfl(&file, &var_map)?;

    // Parse
    let wfl_file = wf_lang::parse_wfl(&source).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;

    // Compile
    let plans = wf_lang::compile_wfl(&wfl_file, &all_schemas)?;

    if plans.is_empty() {
        println!("No rules compiled.");
        return Ok(());
    }

    // Build state machines and executors for each rule
    let mut engines: Vec<(CepStateMachine, RuleExecutor)> = plans
        .into_iter()
        .map(|plan| {
            let time_field = all_schemas
                .iter()
                .find(|s| plan.binds.iter().any(|b| b.window == s.name))
                .and_then(|s| s.time_field.clone());

            let limits = plan.limits_plan.clone();
            let sm = CepStateMachine::with_limits(
                plan.name.clone(),
                plan.match_plan.clone(),
                time_field,
                limits,
            );
            let executor = RuleExecutor::new(plan);
            (sm, executor)
        })
        .collect();

    // Read NDJSON
    let reader = BufReader::new(
        std::fs::File::open(&input)
            .map_err(|e| anyhow::anyhow!("failed to open {}: {}", input.display(), e))?,
    );

    let mut event_count: u64 = 0;
    let mut match_count: u64 = 0;
    let mut error_count: u64 = 0;

    for line_result in reader.lines() {
        let line = line_result?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "WARN: skipping invalid JSON on line {}: {}",
                    event_count + 1,
                    e
                );
                error_count += 1;
                continue;
            }
        };

        let event = json_to_event(&json);
        event_count += 1;

        for (sm, executor) in &mut engines {
            match sm.advance(&alias, &event) {
                StepResult::Matched(ctx) => match executor.execute_match(&ctx) {
                    Ok(alert) => {
                        match serde_json::to_string(&alert) {
                            Ok(s) => println!("{}", s),
                            Err(e) => eprintln!("ERROR: failed to serialize alert: {}", e),
                        }
                        match_count += 1;
                    }
                    Err(e) => {
                        eprintln!("ERROR: execute_match failed: {}", e);
                        error_count += 1;
                    }
                },
                StepResult::Advance | StepResult::Accumulate => {}
            }
        }
    }

    // Print summary
    eprintln!("---");
    eprintln!(
        "Replay complete: {} events processed, {} matches, {} errors",
        event_count, match_count, error_count
    );

    Ok(())
}

/// Convert a serde_json::Value (object) into our Event type.
fn json_to_event(json: &serde_json::Value) -> Event {
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
