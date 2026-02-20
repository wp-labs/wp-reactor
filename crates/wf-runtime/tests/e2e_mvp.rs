//! End-to-end MVP integration test.
//!
//! Proves the full pipeline: TCP → Arrow IPC → Receiver → Router →
//! Scheduler → CEP rule → Alert file.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use wf_config::FusionConfig;
use wf_runtime::lifecycle::FusionEngine;

/// Build a length-prefixed TCP frame from an Arrow IPC payload.
fn make_tcp_frame(ipc_payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + ipc_payload.len());
    frame.extend_from_slice(&(ipc_payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(ipc_payload);
    frame
}

#[tokio::test]
async fn e2e_brute_force_alert() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let alert_path = tmpdir.path().join("alerts.jsonl");

    // -- Build config from inline TOML with port 0 and tempdir alert sink --
    let toml_str = format!(
        r#"
[server]
listen = "tcp://127.0.0.1:0"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
window_schemas = "schemas/*.wfs"
wfl_rules = "rules/*.wfl"

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

    // base_dir points to the examples/ directory so .wfs/.wfl are resolved.
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let base_dir = manifest_dir.join("../../examples");

    // -- Start engine --
    let engine = FusionEngine::start(config, &base_dir)
        .await
        .expect("FusionEngine::start failed");
    let addr = engine.listen_addr();

    // -- Connect TCP and send 3 "failed" auth events --
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("action", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    let base_ts: i64 = 1_700_000_000_000_000_000; // arbitrary epoch nanos
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringArray::from(vec![
                "10.0.0.1", "10.0.0.1", "10.0.0.1",
            ])),
            Arc::new(StringArray::from(vec!["admin", "admin", "admin"])),
            Arc::new(StringArray::from(vec!["failed", "failed", "failed"])),
            Arc::new(TimestampNanosecondArray::from(vec![
                base_ts,
                base_ts + 1_000_000_000,
                base_ts + 2_000_000_000,
            ])),
        ],
    )
    .expect("failed to build RecordBatch");

    let ipc_payload = wp_arrow::ipc::encode_ipc("syslog", &batch).expect("encode_ipc failed");
    let tcp_frame = make_tcp_frame(&ipc_payload);

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("TCP connect failed");
    stream
        .write_all(&tcp_frame)
        .await
        .expect("TCP write failed");
    stream.flush().await.expect("TCP flush failed");

    // Allow time for TCP → Scheduler pipeline delivery.
    // Actual latency is <10ms; 200ms gives ample margin for slow CI.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // -- Shutdown (flush triggers on-close evaluation) --
    engine.shutdown();
    // Drop TCP stream so the receiver's event_tx can close.
    drop(stream);
    engine.wait().await.expect("engine.wait failed");

    // -- Verify alert output --
    let alert_content = std::fs::read_to_string(&alert_path)
        .unwrap_or_else(|e| panic!("failed to read alert file {}: {e}", alert_path.display()));

    let lines: Vec<&str> = alert_content.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "expected exactly 1 alert line, got {}. Full alert file:\n{alert_content}",
        lines.len()
    );

    let alert: serde_json::Value =
        serde_json::from_str(lines[0]).expect("failed to parse alert JSON");

    assert_eq!(
        alert["rule_name"].as_str().unwrap(),
        "brute_force_then_scan",
        "unexpected rule_name: {alert}"
    );
    assert_eq!(
        alert["entity_id"].as_str().unwrap(),
        "10.0.0.1",
        "unexpected entity_id: {alert}"
    );
    assert_eq!(
        alert["entity_type"].as_str().unwrap(),
        "ip",
        "unexpected entity_type: {alert}"
    );
    let score = alert["score"].as_f64().unwrap();
    assert!(
        (score - 70.0).abs() < f64::EPSILON,
        "expected score 70.0, got {score}"
    );
    assert!(
        alert["close_reason"].as_str().is_some(),
        "expected close_reason to be present (on-close path), got: {alert}"
    );
}
