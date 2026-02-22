use std::path::PathBuf;

use anyhow::Result;

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

    // Compile (runs check_wfl internally)
    let plans = wf_lang::compile_wfl(&wfl_file, &all_schemas)?;

    // Explain
    let explanations = wf_lang::explain::explain_rules(&plans, &all_schemas);

    for expl in &explanations {
        print!("{expl}");
    }

    Ok(())
}
