//! End-to-end integration test driven by `wf-datagen`.
//!
//! Uses the full datagen pipeline: `.wfg` scenario → event generation → oracle
//! prediction → FusionEngine execution → alert verification.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};
use wf_config::FusionConfig;
use wf_datagen::datagen::stream_gen::GenEvent;
use wf_runtime::lifecycle::FusionEngine;
use wf_runtime::tracing_init::{DomainFormat, FileFields};

use wf_lang::{BaseType, FieldType, WindowSchema};

/// Build a length-prefixed TCP frame from an Arrow IPC payload.
fn make_tcp_frame(ipc_payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + ipc_payload.len());
    frame.extend_from_slice(&(ipc_payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(ipc_payload);
    frame
}

/// Convert GenEvents into length-prefixed TCP frames grouped by window_name.
///
/// For each window group:
/// 1. Look up the WindowSchema to get field definitions and stream name.
/// 2. Build an Arrow Schema with typed columns (all nullable=true).
/// 3. Build Arrow arrays from the JSON field values in each GenEvent.
/// 4. Encode as IPC and wrap in a length-prefixed frame.
fn events_to_tcp_frames(events: &[GenEvent], schemas: &[WindowSchema]) -> Vec<(String, Vec<u8>)> {
    // Group events by window_name
    let mut groups: HashMap<String, Vec<&GenEvent>> = HashMap::new();
    for event in events {
        groups
            .entry(event.window_name.clone())
            .or_default()
            .push(event);
    }

    let mut frames = Vec::new();

    for (window_name, group_events) in &groups {
        let schema = schemas
            .iter()
            .find(|s| s.name == *window_name)
            .unwrap_or_else(|| panic!("schema not found for window '{window_name}'"));

        let stream_name = schema
            .streams
            .first()
            .unwrap_or_else(|| panic!("no stream defined for window '{window_name}'"));

        // Build Arrow schema from field definitions
        let arrow_fields: Vec<Field> = schema
            .fields
            .iter()
            .map(|f| {
                let dt = base_type_to_arrow_dt(&f.field_type);
                Field::new(&f.name, dt, true)
            })
            .collect();
        let arrow_schema = Arc::new(Schema::new(arrow_fields));

        // Build columns
        let columns: Vec<Arc<dyn arrow::array::Array>> = schema
            .fields
            .iter()
            .map(|field_def| build_arrow_column(field_def, group_events))
            .collect();

        let batch = RecordBatch::try_new(arrow_schema, columns)
            .unwrap_or_else(|e| panic!("failed to build RecordBatch for '{window_name}': {e}"));

        let ipc_payload = wp_arrow::ipc::encode_ipc(stream_name, &batch)
            .unwrap_or_else(|e| panic!("encode_ipc failed for '{stream_name}': {e}"));

        frames.push((stream_name.clone(), make_tcp_frame(&ipc_payload)));
    }

    frames
}

fn base_type_to_arrow_dt(ft: &FieldType) -> DataType {
    let base = match ft {
        FieldType::Base(b) => b,
        FieldType::Array(b) => b,
    };
    match base {
        BaseType::Chars | BaseType::Ip | BaseType::Hex => DataType::Utf8,
        BaseType::Digit => DataType::Int64,
        BaseType::Float => DataType::Float64,
        BaseType::Bool => DataType::Boolean,
        BaseType::Time => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

fn build_arrow_column(
    field_def: &wf_lang::FieldDef,
    events: &[&GenEvent],
) -> Arc<dyn arrow::array::Array> {
    let base = match &field_def.field_type {
        FieldType::Base(b) => b,
        FieldType::Array(b) => b,
    };
    let name = &field_def.name;

    match base {
        BaseType::Chars | BaseType::Ip | BaseType::Hex => {
            let values: Vec<Option<String>> = events
                .iter()
                .map(|e| {
                    e.fields
                        .get(name)
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .collect();
            Arc::new(StringArray::from(values))
        }
        BaseType::Digit => {
            let values: Vec<Option<i64>> = events
                .iter()
                .map(|e| e.fields.get(name).and_then(|v| v.as_i64()))
                .collect();
            Arc::new(Int64Array::from(values))
        }
        BaseType::Float => {
            let values: Vec<Option<f64>> = events
                .iter()
                .map(|e| e.fields.get(name).and_then(|v| v.as_f64()))
                .collect();
            Arc::new(Float64Array::from(values))
        }
        BaseType::Bool => {
            let values: Vec<Option<bool>> = events
                .iter()
                .map(|e| e.fields.get(name).and_then(|v| v.as_bool()))
                .collect();
            Arc::new(BooleanArray::from(values))
        }
        BaseType::Time => {
            let values: Vec<Option<i64>> = events
                .iter()
                .map(|e| {
                    // First try: the field is a timestamp string in the JSON fields
                    if let Some(v) = e.fields.get(name) {
                        if let Some(s) = v.as_str()
                            && let Ok(dt) = s.parse::<DateTime<Utc>>()
                        {
                            return dt.timestamp_nanos_opt();
                        }
                        if let Some(n) = v.as_i64() {
                            return Some(n);
                        }
                    }
                    // Fallback: use the event's timestamp
                    e.timestamp.timestamp_nanos_opt()
                })
                .collect();
            Arc::new(TimestampNanosecondArray::from(values))
        }
    }
}

#[tokio::test]
async fn e2e_datagen_brute_force() {
    // ---- Artifact directory ----
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let artifact_dir = manifest_dir.join("../../target/test-artifacts/e2e_datagen");
    std::fs::create_dir_all(&artifact_dir).expect("failed to create artifact dir");
    let alert_path = artifact_dir.join("alerts.jsonl");
    let _ = std::fs::remove_file(&alert_path);

    // ---- Tracing (test_writer + file) ----
    let log_file_name = "e2e_datagen.log";
    let _ = std::fs::remove_file(artifact_dir.join(log_file_name));
    let file_appender = tracing_appender::rolling::never(&artifact_dir, log_file_name);
    let (non_blocking, _log_guard) = tracing_appender::non_blocking(file_appender);
    let _ = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .event_format(DomainFormat::new())
                .with_test_writer()
                .with_filter(EnvFilter::try_new("info").unwrap()),
        )
        .with(
            fmt::layer()
                .event_format(DomainFormat::new())
                .fmt_fields(FileFields::default())
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_filter(EnvFilter::try_new("debug").unwrap()),
        )
        .try_init();

    // ---- Parse .wfg scenario ----
    let base_dir = manifest_dir.join("../../examples");
    let wfg_path = base_dir.join("scenarios/brute_force.wfg");
    let wfg_content = std::fs::read_to_string(&wfg_path).expect("failed to read brute_force.wfg");
    let wfg =
        wf_datagen::wfg_parser::parse_wfg(&wfg_content).expect("failed to parse .wfg scenario");

    // ---- Parse .wfs schemas ----
    let wfs_path = base_dir.join("schemas/security.wfs");
    let wfs_content = std::fs::read_to_string(&wfs_path).expect("failed to read security.wfs");
    let schemas = wf_lang::parse_wfs(&wfs_content).expect("failed to parse .wfs schemas");

    // ---- Preprocess + parse + compile .wfl rules ----
    let wfl_path = base_dir.join("rules/brute_force.wfl");
    let wfl_raw = std::fs::read_to_string(&wfl_path).expect("failed to read brute_force.wfl");
    let vars = HashMap::from([("FAIL_THRESHOLD".into(), "3".into())]);
    let preprocessed =
        wf_lang::preprocess_vars(&wfl_raw, &vars).expect("failed to preprocess .wfl vars");
    let wfl_file = wf_lang::parse_wfl(&preprocessed).expect("failed to parse .wfl rules");
    let rule_plans =
        wf_lang::compile_wfl(&wfl_file, &schemas).expect("failed to compile .wfl rules");

    // ---- Validate scenario ----
    let validation_errors = wf_datagen::validate::validate_wfg(&wfg, &schemas, &[wfl_file]);
    assert!(
        validation_errors.is_empty(),
        "scenario validation failed: {:?}",
        validation_errors
    );

    // ---- Generate events ----
    let gen_result = wf_datagen::datagen::generate(&wfg, &schemas, &rule_plans)
        .expect("event generation failed");
    let events = gen_result.events;
    assert!(
        !events.is_empty(),
        "datagen produced zero events — scenario misconfigured?"
    );

    // ---- Oracle prediction (SC7: only injected rules) ----
    let start: DateTime<Utc> = wfg
        .scenario
        .time_clause
        .start
        .parse()
        .expect("invalid scenario start time");
    let duration = wfg.scenario.time_clause.duration;

    let injected_rules: HashSet<String> = wfg
        .scenario
        .injects
        .iter()
        .map(|i| i.rule.clone())
        .collect();
    let oracle_result = wf_datagen::oracle::run_oracle(
        &events,
        &rule_plans,
        &start,
        &duration,
        Some(&injected_rules),
    )
    .expect("oracle evaluation failed");
    let oracle_alerts = &oracle_result.alerts;

    // ---- Build FusionConfig (inline TOML, port=0) ----
    let toml_str = format!(
        r#"
[server]
listen = "tcp://127.0.0.1:0"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"

[alert]
sinks = ["file://{}"]

[vars]
FAIL_THRESHOLD = "3"
"#,
        alert_path.display()
    );
    let config: FusionConfig = toml_str.parse().expect("failed to parse config TOML");

    // ---- Start engine ----
    let engine = FusionEngine::start(config, &base_dir)
        .await
        .expect("FusionEngine::start failed");
    let addr = engine.listen_addr();

    // ---- Convert GenEvents → Arrow TCP frames ----
    let tcp_frames = events_to_tcp_frames(&events, &schemas);

    // ---- TCP send ----
    let mut stream = TcpStream::connect(addr).await.expect("TCP connect failed");

    for (_stream_name, frame) in &tcp_frames {
        stream.write_all(frame).await.expect("TCP write failed");
    }
    stream.flush().await.expect("TCP flush failed");

    // ---- Wait for processing + shutdown ----
    tokio::time::sleep(Duration::from_millis(500)).await;
    engine.shutdown();
    drop(stream);
    engine.wait().await.expect("engine.wait failed");

    // ---- Read actual alerts ----
    let actual = wf_datagen::output::jsonl::read_alerts_jsonl(&alert_path)
        .unwrap_or_else(|e| panic!("failed to read alerts from {}: {e}", alert_path.display()));

    // ---- Extract oracle tolerances ----
    let tolerances = wfg
        .scenario
        .oracle
        .as_ref()
        .map(wf_datagen::oracle::extract_oracle_tolerances)
        .unwrap_or_default();

    // ---- Run verify and write diagnostic report ----
    let report = wf_datagen::verify::verify(
        oracle_alerts,
        &actual,
        tolerances.score_tolerance,
        tolerances.time_tolerance_secs,
    );
    let report_md = report.to_markdown();
    let report_path = artifact_dir.join("verify_report.md");
    std::fs::write(&report_path, &report_md)
        .unwrap_or_else(|e| panic!("failed to write report to {}: {e}", report_path.display()));

    // ---- Entity-level verification ----
    //
    // The oracle uses synthetic event-based time while the engine uses wall-clock
    // time, so per-alert time/close_reason matching won't align. Instead, verify
    // at the entity level: every entity the oracle predicted should fire must
    // appear in the actual alerts, and vice versa. Scores must also match.

    // Unique entity_ids from oracle alerts
    let oracle_entities: HashSet<String> =
        oracle_alerts.iter().map(|a| a.entity_id.clone()).collect();

    // Unique entity_ids from actual alerts
    let actual_entities: HashSet<String> = actual.iter().map(|a| a.entity_id.clone()).collect();

    // Check: oracle entities ⊆ actual entities (no missing)
    let missing: Vec<&String> = oracle_entities.difference(&actual_entities).collect();
    assert!(
        missing.is_empty(),
        "entities predicted by oracle but missing from engine output: {:?}\n\
         oracle entities: {}\n\
         actual entities: {}",
        missing,
        oracle_entities.len(),
        actual_entities.len()
    );

    // Check: actual entities ⊆ oracle entities (no unexpected)
    let unexpected: Vec<&String> = actual_entities.difference(&oracle_entities).collect();
    assert!(
        unexpected.is_empty(),
        "entities in engine output but not predicted by oracle: {:?}\n\
         oracle entities: {}\n\
         actual entities: {}",
        unexpected,
        oracle_entities.len(),
        actual_entities.len()
    );

    // Check: all actual alert scores match expected (70.0)
    let expected_score = 70.0;
    for alert in &actual {
        assert!(
            (alert.score - expected_score).abs() <= tolerances.score_tolerance,
            "alert for entity {} has score {}, expected {}",
            alert.entity_id,
            alert.score,
            expected_score
        );
    }

    // Check: all alerts reference the correct rule
    for alert in &actual {
        assert_eq!(
            alert.rule_name, "brute_force_then_scan",
            "unexpected rule_name: {}",
            alert.rule_name
        );
    }
}
