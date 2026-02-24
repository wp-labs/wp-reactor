use std::path::PathBuf;
use std::process;

use anyhow::Result;

use wf_config::project::{load_schemas, load_wfl, parse_vars};
use wf_core::rule::contract::run_contract;

pub fn run(file: PathBuf, schemas: Vec<String>, vars: Vec<String>) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let var_map = parse_vars(&vars)?;

    // Load schemas
    let all_schemas = load_schemas(&schemas, &cwd)?;

    // Load and preprocess the .wfl file
    let source = load_wfl(&file, &var_map)?;

    // Parse
    let wfl_file = wf_lang::parse_wfl(&source).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;

    // Compile rules into plans
    let plans = wf_lang::compile_wfl(&wfl_file, &all_schemas)?;

    if wfl_file.contracts.is_empty() {
        eprintln!("No contracts found.");
        return Ok(());
    }

    let mut total = 0;
    let mut passed = 0;
    let mut failed = 0;

    for contract in &wfl_file.contracts {
        total += 1;

        let plan = match plans.iter().find(|p| p.name == contract.rule_name) {
            Some(p) => p,
            None => {
                eprintln!(
                    "FAIL  {} — target rule `{}` not found",
                    contract.name, contract.rule_name
                );
                failed += 1;
                continue;
            }
        };

        let time_field = all_schemas
            .iter()
            .find(|s| plan.binds.iter().any(|b| b.window == s.name))
            .and_then(|s| s.time_field.clone());

        match run_contract(contract, plan, time_field) {
            Ok(result) => {
                if result.passed {
                    eprintln!("PASS  {} ({})", contract.name, contract.rule_name);
                    passed += 1;
                } else {
                    eprintln!("FAIL  {} ({})", contract.name, contract.rule_name);
                    for f in &result.failures {
                        eprintln!("      {}", f);
                    }
                    failed += 1;
                }
            }
            Err(e) => {
                eprintln!(
                    "FAIL  {} ({}) — error: {}",
                    contract.name, contract.rule_name, e
                );
                failed += 1;
            }
        }
    }

    eprintln!(
        "\n{} contracts: {} passed, {} failed",
        total, passed, failed
    );

    if failed > 0 {
        process::exit(1);
    }

    Ok(())
}
