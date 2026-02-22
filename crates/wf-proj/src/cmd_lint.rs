use std::path::PathBuf;
use std::process;

use anyhow::Result;

use wf_lang::Severity;

use wf_config::project::{load_schemas, load_wfl, parse_vars};

pub fn run(file: PathBuf, schemas: Vec<String>, vars: Vec<String>) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let var_map = parse_vars(&vars)?;

    // Load schemas
    let all_schemas = load_schemas(&schemas, &cwd)?;

    // Load and preprocess the .wfl file
    let source = load_wfl(&file, &var_map)?;

    // Parse
    let wfl_file = wf_lang::parse_wfl(&source)
        .map_err(|e| anyhow::anyhow!("parse error: {e}"))?;

    // Run error-level checks
    let errors = wf_lang::check_wfl(&wfl_file, &all_schemas);

    // Run lint-level checks
    let warnings = wf_lang::lint_wfl(&wfl_file, &all_schemas);

    let total = errors.len() + warnings.len();
    let mut has_errors = false;

    // Print all diagnostics
    for diag in errors.iter().chain(warnings.iter()) {
        if diag.severity == Severity::Error {
            has_errors = true;
        }
        eprintln!("{diag}");
    }

    if total == 0 {
        eprintln!("No issues found.");
    } else {
        let error_count = errors.len();
        let warning_count = warnings.len();
        eprintln!(
            "\n{} error(s), {} warning(s)",
            error_count, warning_count
        );
    }

    if has_errors {
        process::exit(1);
    }

    Ok(())
}
