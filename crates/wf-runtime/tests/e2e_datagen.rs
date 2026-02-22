//! End-to-end integration test driven by `wf-datagen`.
//!
//! Uses the full datagen pipeline: `.wfg` scenario → event generation → oracle
//! prediction → FusionEngine execution → alert verification.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};
use wf_config::FusionConfig;
use wf_runtime::lifecycle::FusionEngine;
use wf_runtime::tracing_init::{DomainFormat, FileFields};

/// Build a length-prefixed TCP frame from an Arrow IPC payload.
fn make_tcp_frame(ipc_payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + ipc_payload.len());
    frame.extend_from_slice(&(ipc_payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(ipc_payload);
    frame
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

    // ---- Load scenario (.wfg → schemas + rules) ----
    let base_dir = manifest_dir.join("../../examples");
    let wfg_path = base_dir.join("scenarios/brute_force.wfg");
    let vars = HashMap::from([("FAIL_THRESHOLD".into(), "3".into())]);
    let loaded = wf_datagen::loader::load_scenario(&wfg_path, &vars)
        .expect("failed to load scenario");

    // ---- Validate scenario ----
    let validation_errors =
        wf_datagen::validate::validate_wfg(&loaded.wfg, &loaded.schemas, &loaded.wfl_files);
    assert!(
        validation_errors.is_empty(),
        "scenario validation failed: {:?}",
        validation_errors
    );

    // ---- Generate events ----
    let gen_result =
        wf_datagen::datagen::generate(&loaded.wfg, &loaded.schemas, &loaded.rule_plans)
            .expect("event generation failed");
    let events = gen_result.events;
    assert!(
        !events.is_empty(),
        "datagen produced zero events — scenario misconfigured?"
    );

    // ---- Oracle prediction (SC7: only injected rules) ----
    let start: DateTime<Utc> = loaded
        .wfg
        .scenario
        .time_clause
        .start
        .parse()
        .expect("invalid scenario start time");
    let duration = loaded.wfg.scenario.time_clause.duration;

    let injected_rules: HashSet<String> = loaded
        .wfg
        .scenario
        .injects
        .iter()
        .map(|i| i.rule.clone())
        .collect();
    let oracle_result = wf_datagen::oracle::run_oracle(
        &events,
        &loaded.rule_plans,
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

    // ---- Convert GenEvents → typed Arrow batches → TCP frames ----
    let batches =
        wf_datagen::output::arrow_ipc::events_to_typed_batches(&events, &loaded.schemas)
            .expect("events_to_typed_batches failed");

    // ---- TCP send ----
    let mut stream = TcpStream::connect(addr).await.expect("TCP connect failed");

    for (stream_name, batch) in &batches {
        let ipc_payload = wp_arrow::ipc::encode_ipc(stream_name, batch)
            .unwrap_or_else(|e| panic!("encode_ipc failed for '{stream_name}': {e}"));
        stream
            .write_all(&make_tcp_frame(&ipc_payload))
            .await
            .expect("TCP write failed");
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
    let tolerances = loaded
        .wfg
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

    // ---- Verify report must pass ----
    assert_eq!(
        report.status, "pass",
        "verify report failed:\n{}",
        report_md
    );

    // Check: all alerts reference the correct rule
    for alert in &actual {
        assert_eq!(
            alert.rule_name, "brute_force_then_scan",
            "unexpected rule_name: {}",
            alert.rule_name
        );
    }
}
