//! End-to-end integration test driven by `wfgen`.
//!
//! Uses the full datagen pipeline: `.wfg` scenario → event generation → oracle
//! prediction → Reactor execution → alert verification.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};
use wf_config::FusionConfig;
use wf_runtime::lifecycle::Reactor;
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
    let alert_path = artifact_dir.join("alerts/all.jsonl");
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
    let wfg_path = base_dir.join("count/scenarios/brute_force.wfg");
    let vars = HashMap::from([("FAIL_THRESHOLD".into(), "3".into())]);
    let loaded = wfgen::loader::load_scenario(&wfg_path, &vars).expect("failed to load scenario");

    // ---- Validate scenario ----
    let validation_errors =
        wfgen::validate::validate_wfg(&loaded.wfg, &loaded.schemas, &loaded.wfl_files);
    assert!(
        validation_errors.is_empty(),
        "scenario validation failed: {:?}",
        validation_errors
    );

    // ---- Generate events ----
    let gen_result = wfgen::datagen::generate(&loaded.wfg, &loaded.schemas, &loaded.rule_plans)
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
    let oracle_result = wfgen::oracle::run_oracle(
        &events,
        &loaded.rule_plans,
        &start,
        &duration,
        Some(&injected_rules),
    )
    .expect("oracle evaluation failed");
    let oracle_alerts = &oracle_result.alerts;
    let tolerances = loaded
        .wfg
        .scenario
        .oracle
        .as_ref()
        .map(wfgen::oracle::extract_oracle_tolerances)
        .unwrap_or_default();

    // ---- Build FusionConfig (inline TOML, port=0, connector-based sinks) ----
    let toml_str = format!(
        r#"
sinks = "sinks"
work_root = "{}"

[server]
listen = "tcp://127.0.0.1:0"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "count/schemas/*.wfs"
rules   = "count/rules/*.wfl"

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

[vars]
FAIL_THRESHOLD = "3"
"#,
        artifact_dir.display()
    );
    let config: FusionConfig = toml_str.parse().expect("failed to parse config TOML");

    // ---- Start engine ----
    let reactor = Reactor::start(config, &base_dir)
        .await
        .expect("Reactor::start failed");
    let addr = reactor.listen_addr();

    // ---- Convert GenEvents → typed Arrow batches → TCP frames ----
    let batches = wfgen::output::arrow_ipc::events_to_typed_batches(&events, &loaded.schemas)
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
    stream.shutdown().await.expect("TCP shutdown(write) failed");
    drop(stream);

    // ---- Wait for processing to converge before shutdown ----
    //
    // A fixed sleep here is flaky: if we shutdown too early, windows flush as
    // `close:eos`, which mismatches the oracle's expected `close:timeout`.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_report_md = String::new();
    loop {
        if alert_path.exists() {
            match wfgen::output::jsonl::read_alerts_jsonl(&alert_path) {
                Ok(actual_now) => {
                    let report_now = wfgen::verify::verify(
                        oracle_alerts,
                        &actual_now,
                        tolerances.score_tolerance,
                        tolerances.time_tolerance_secs,
                    );
                    if report_now.status == "pass" {
                        break;
                    }
                    last_report_md = report_now.to_markdown();
                }
                Err(_) => {
                    // Sink may be writing the current line; retry on next poll.
                }
            }
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for runtime alerts to converge before shutdown.\n{}",
                if last_report_md.is_empty() {
                    "no verify report yet".to_string()
                } else {
                    format!("last verify report:\n{last_report_md}")
                }
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // ---- Shutdown after we already matched oracle output ----
    reactor.shutdown();
    reactor.wait().await.expect("reactor.wait failed");

    // ---- Read actual alerts (catch_all sink writes to alerts/all.jsonl) ----
    let actual = wfgen::output::jsonl::read_alerts_jsonl(&alert_path)
        .unwrap_or_else(|e| panic!("failed to read alerts from {}: {e}", alert_path.display()));

    // ---- Run verify and write diagnostic report ----
    let report = wfgen::verify::verify(
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
