use std::io::{BufReader, IsTerminal};
use std::path::PathBuf;

use anyhow::Context;

use wfgen::oracle::OracleTolerances;
use wfgen::output::jsonl::read_oracle_jsonl;
use wfgen::verify::{ActualAlert, verify};

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    schemas: Vec<String>,
    input: PathBuf,
    vars: Vec<String>,
    expected: PathBuf,
    score_tolerance: Option<f64>,
    time_tolerance: Option<f64>,
    meta: Option<PathBuf>,
    format: String,
) -> anyhow::Result<()> {
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
    let replay = crate::cmd_replay::replay_events(&source, &all_schemas, reader, color)?;

    let actual: Vec<ActualAlert> = replay
        .alerts
        .into_iter()
        .map(|a| ActualAlert {
            rule_name: a.rule_name,
            score: a.score,
            entity_type: a.entity_type,
            entity_id: a.entity_id,
            origin: a.origin.as_str().to_string(),
            fired_at: a.fired_at,
        })
        .collect();

    let expected_alerts = read_oracle_jsonl(&expected)
        .with_context(|| format!("reading expected: {}", expected.display()))?;

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

    let report = verify(
        &expected_alerts,
        &actual,
        effective_score_tol,
        effective_time_tol,
    );

    eprintln!("---");
    eprintln!(
        "Replay complete: {} events processed, {} matches, {} errors",
        replay.event_count, replay.match_count, replay.error_count
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
