//! Round-4 coverage tests for the sink layer (`sink/dispatch.rs` +
//! `sink/runtime.rs`): the dispatcher's route / default / error / monitor
//! delivery paths (incl. failing sinks), the `resolve_sinks` default
//! fallback, lifecycle `stop_all` / `stop_monitor_sinks`, the runtime's
//! `send_str` / `send_records` / `send_column_batch` (incl. payload-blind),
//! `stop`, and the `Debug` impls that the earlier suites skip.
//!
//! Only test code lives here — no production logic is modified.

use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::Mutex;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkHandle, SinkReason, SinkResult,
    SinkSpec as ResolvedSinkSpec,
};
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::alert::{AlertColumnBuilder, AlertOrigin, OutputRecord};
use crate::match_engine::Value;
use crate::sink::dispatch::SinkDispatcher;
use crate::sink::runtime::{SinkRuntime, WfMetaDisableMatcher};

// ---------------------------------------------------------------------------
// Test sinks
// ---------------------------------------------------------------------------

/// Captures the last delivered record; every send succeeds.
#[derive(Clone, Default)]
struct CaptureSink {
    record: Arc<StdMutex<Option<DataRecord>>>,
}

/// Every send fails (used for the error-escalation paths).
#[derive(Clone, Default)]
struct FailingSink;

#[async_trait::async_trait]
impl AsyncCtrl for CaptureSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncRawDataSink for CaptureSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Ok(())
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Ok(())
    }
    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Ok(())
    }
    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncRecordSink for CaptureSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        *self.record.lock().unwrap() = Some(data.clone());
        Ok(())
    }
    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        *self.record.lock().unwrap() = data.first().map(|record| record.as_ref().clone());
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncCtrl for FailingSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Err(SinkReason::sink("stop failed"))
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncRawDataSink for FailingSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::sink("boom"))
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::sink("boom"))
    }
    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::sink("boom"))
    }
    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::sink("boom"))
    }
}

#[async_trait::async_trait]
impl AsyncRecordSink for FailingSink {
    async fn sink_record(&mut self, _data: &DataRecord) -> SinkResult<()> {
        Err(SinkReason::sink("boom"))
    }
    async fn sink_records(&mut self, _data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        Err(SinkReason::sink("boom"))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sink_spec(name: &str) -> ResolvedSinkSpec {
    ResolvedSinkSpec {
        group: "test".to_string(),
        name: name.to_string(),
        kind: "test".to_string(),
        connector_id: name.to_string(),
        params: Default::default(),
        filter: None,
    }
}

fn capture_runtime(
    captured: Arc<StdMutex<Option<DataRecord>>>,
    output_fields: Option<Vec<String>>,
    wf_meta_disable: Vec<String>,
) -> SinkRuntime {
    SinkRuntime {
        name: "capture".to_string(),
        spec: sink_spec("capture"),
        handle: Mutex::new(SinkHandle::new(Box::new(CaptureSink { record: captured }))),
        tags: Vec::new(),
        output_fields,
        wf_meta_disable: wf_meta_disable.clone(),
        wf_meta_disable_matcher: WfMetaDisableMatcher::new(&wf_meta_disable),
        parallel: 1,
        payload_blind: false,
    }
}

fn failing_runtime(name: &str) -> SinkRuntime {
    SinkRuntime {
        name: name.to_string(),
        spec: sink_spec(name),
        handle: Mutex::new(SinkHandle::new(Box::new(FailingSink))),
        tags: Vec::new(),
        output_fields: None,
        wf_meta_disable: Vec::new(),
        wf_meta_disable_matcher: WfMetaDisableMatcher::new(&[]),
        parallel: 1,
        payload_blind: false,
    }
}

fn payload_blind_runtime(name: &str) -> SinkRuntime {
    SinkRuntime {
        payload_blind: true,
        ..failing_runtime(name)
    }
}

fn sample_record() -> DataRecord {
    let mut record = DataRecord::default();
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Chars,
        "message",
        ModelValue::from("hello"),
    )));
    record
}

fn sample_output_record() -> OutputRecord {
    OutputRecord {
        wfx_id: "id-r4".into(),
        rule_name: Arc::from("r4_rule"),
        score: 50.0,
        entity_type: Arc::from("ip"),
        entity_id: "10.0.0.1".into(),
        origin: AlertOrigin::Event,
        fired_at: "2026-08-23T00:00:00Z".into(),
        emit_time: Arc::from("2026-08-23T00:00:01Z"),
        matched_rows: Vec::new(),
        summary: Arc::from("r4"),
        yield_target: Arc::from("out"),
        yield_fields: vec![("count".into(), Value::Number(3.0))],
        yield_field_types: Arc::from([]),
        event_time_nanos: 0,
        machine_id: Arc::from(""),
        scope_key: Arc::from(""),
    }
}

// ---------------------------------------------------------------------------
// runtime.rs — send_str / send_records / send_column_batch / stop / Debug
// ---------------------------------------------------------------------------

#[tokio::test]
async fn send_str_delivers_raw_payload() {
    let captured = Arc::new(StdMutex::new(None));
    let runtime = capture_runtime(Arc::clone(&captured), None, Vec::new());
    runtime.send_str("{\"raw\":true}").await.expect("send_str");
}

#[tokio::test]
async fn send_records_passthrough_and_transformed_lanes() {
    // Passthrough (no projection, no wf_meta_disable): shares the caller's Arc.
    let captured = Arc::new(StdMutex::new(None));
    let runtime = capture_runtime(Arc::clone(&captured), None, Vec::new());
    let record = Arc::new(sample_record());
    runtime
        .send_records(&[Arc::clone(&record)])
        .await
        .expect("passthrough send_records");
    assert_eq!(captured.lock().unwrap().as_ref().unwrap().items.len(), 1);

    // Projection path: only the requested field survives.
    let captured = Arc::new(StdMutex::new(None));
    let runtime = capture_runtime(
        Arc::clone(&captured),
        Some(vec!["message".to_string()]),
        Vec::new(),
    );
    runtime
        .send_records(&[record.clone()])
        .await
        .expect("projected send_records");
    let got = captured.lock().unwrap().clone().expect("captured");
    assert_eq!(got.items.len(), 1);
    assert_eq!(got.items[0].get_name(), "message");

    // wf_meta_disable path: reserved-prefix field gets marked Ignore.
    let mut meta_record = sample_record();
    meta_record.push(FieldStorage::from_owned(Field::new(
        DataType::Chars,
        "__wfu_score",
        ModelValue::from("80"),
    )));
    let captured = Arc::new(StdMutex::new(None));
    let runtime = capture_runtime(Arc::clone(&captured), None, vec!["__wfu_*".to_string()]);
    runtime
        .send_records(&[Arc::new(meta_record)])
        .await
        .expect("filtered send_records");
    let got = captured.lock().unwrap().clone().expect("captured");
    assert_eq!(
        got.field("__wfu_score").unwrap().get_meta(),
        &DataType::Ignore
    );

    // Sink failure surfaces as Err.
    let failing = failing_runtime("failing");
    assert!(failing.send_records(&[record]).await.is_err());
}

#[tokio::test]
async fn send_column_batch_payload_blind_and_row_paths() {
    // Payload-blind: confirmed without touching the payload (a failing sink
    // would still succeed).
    let blind = payload_blind_runtime("blind");
    let mut builder = AlertColumnBuilder::new(Arc::from("out"));
    builder
        .append_record(&sample_output_record())
        .expect("append record");
    let batch = builder.finish();
    blind
        .send_column_batch(&batch)
        .await
        .expect("payload-blind sink confirms");
    assert_eq!(batch.len(), 1);

    // Row-oriented path: reconstructs DataRecords and hands them to the sink.
    let captured = Arc::new(StdMutex::new(None));
    let runtime = capture_runtime(Arc::clone(&captured), None, Vec::new());
    runtime
        .send_column_batch(&batch)
        .await
        .expect("row-oriented send_column_batch");
    let got = captured.lock().unwrap().clone().expect("captured");
    assert!(
        got.field("__wfu_rule_name").is_some(),
        "reconstructed record carries the rule name"
    );
}

#[tokio::test]
async fn stop_and_debug_lanes() {
    let captured = Arc::new(StdMutex::new(None));
    let runtime = capture_runtime(Arc::clone(&captured), None, Vec::new());
    runtime.stop().await.expect("graceful stop");
    // Failing stop surfaces as Err.
    let failing = failing_runtime("failing");
    assert!(failing.stop().await.is_err());

    // Debug impls (previously skipped by the coverage suites).
    let _ = format!("{runtime:?}");
    let matcher = WfMetaDisableMatcher::new(&["__wfu_*".to_string()]);
    let _ = format!("{matcher:?}");
}

// ---------------------------------------------------------------------------
// dispatch.rs — route / default / error / monitor paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_matched_route_sends_to_route_sinks() {
    let captured = Arc::new(StdMutex::new(None));
    let sink = Arc::new(capture_runtime(Arc::clone(&captured), None, Vec::new()));
    let dispatcher = SinkDispatcher::new(
        vec![(vec!["security_*".to_string()], vec![Arc::clone(&sink)])],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let record = sample_record();
    let (matched, had_error) = dispatcher.dispatch("security_alerts", &record).await;
    assert_eq!(matched, 1);
    assert!(!had_error);
    assert!(
        captured.lock().unwrap().is_some(),
        "matched route received the record"
    );
}

#[tokio::test]
async fn dispatch_route_miss_falls_back_to_default_sinks() {
    let captured = Arc::new(StdMutex::new(None));
    let default = Arc::new(capture_runtime(Arc::clone(&captured), None, Vec::new()));
    let dispatcher = SinkDispatcher::new(
        vec![(
            vec!["security_*".to_string()],
            vec![Arc::new(failing_runtime("route"))],
        )],
        vec![default],
        Vec::new(),
        Vec::new(),
    );
    let (matched, had_error) = dispatcher
        .dispatch("network_alerts", &sample_record())
        .await;
    assert_eq!(matched, 0, "no business route matched");
    assert!(!had_error);
    assert!(captured.lock().unwrap().is_some(), "default sink used");
}

#[tokio::test]
async fn dispatch_route_send_error_escalates_to_error_sinks() {
    let captured = Arc::new(StdMutex::new(None));
    let error_sink = Arc::new(capture_runtime(Arc::clone(&captured), None, Vec::new()));
    let dispatcher = SinkDispatcher::new(
        vec![(
            vec!["security_*".to_string()],
            vec![Arc::new(failing_runtime("route"))],
        )],
        Vec::new(),
        vec![error_sink],
        Vec::new(),
    );
    let (matched, had_error) = dispatcher
        .dispatch("security_alerts", &sample_record())
        .await;
    assert_eq!(matched, 1);
    assert!(had_error, "route send failure reported");
    assert!(
        captured.lock().unwrap().is_some(),
        "error sink received the record"
    );
}

#[tokio::test]
async fn dispatch_default_error_escalates_and_error_sink_error_is_swallowed() {
    let dispatcher = SinkDispatcher::new(
        Vec::new(),
        vec![Arc::new(failing_runtime("default"))],
        vec![Arc::new(failing_runtime("error"))],
        Vec::new(),
    );
    let (matched, had_error) = dispatcher.dispatch("anything", &sample_record()).await;
    assert_eq!(matched, 0);
    assert!(had_error, "default sink failure surfaced");
    // `has_no_default_sinks`: false here (a default sink exists).
    assert!(!dispatcher.has_no_default_sinks());
}

#[test]
fn has_no_default_sinks_and_resolve_default_fallback() {
    // No defaults configured → true.
    let empty = SinkDispatcher::new(Vec::new(), Vec::new(), Vec::new(), Vec::new());
    assert!(empty.has_no_default_sinks());

    // resolve_sinks: route match dedups identical sink Arcs; miss → defaults.
    let shared = Arc::new(capture_runtime(
        Arc::new(StdMutex::new(None)),
        None,
        Vec::new(),
    ));
    let dispatcher = SinkDispatcher::new(
        vec![
            (
                vec!["a_*".to_string()],
                vec![Arc::clone(&shared), Arc::clone(&shared)],
            ),
            (vec!["b_*".to_string()], vec![Arc::clone(&shared)]),
        ],
        vec![Arc::clone(&shared)],
        Vec::new(),
        Vec::new(),
    );
    let resolved = dispatcher.resolve_sinks("a_win");
    assert_eq!(resolved.len(), 1, "duplicate sink deduped by identity");
    assert!(Arc::ptr_eq(&resolved[0], &shared));
    let resolved = dispatcher.resolve_sinks("c_win");
    assert_eq!(resolved.len(), 1, "route miss falls back to defaults");
    assert!(Arc::ptr_eq(&resolved[0], &shared));
}

#[tokio::test]
async fn monitor_dispatch_stop_and_stop_monitor_lanes() {
    let captured = Arc::new(StdMutex::new(None));
    let monitor = Arc::new(capture_runtime(Arc::clone(&captured), None, Vec::new()));
    let failing_monitor = Arc::new(failing_runtime("monitor_fail"));
    let dispatcher = SinkDispatcher::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![Arc::clone(&monitor), Arc::clone(&failing_monitor)],
    );
    assert!(dispatcher.has_monitor_sinks());

    // Successful monitor dispatch + a failing monitor's error is swallowed.
    dispatcher.dispatch_to_monitor(&sample_record()).await;
    assert!(captured.lock().unwrap().is_some());

    // Accessors.
    assert_eq!(dispatcher.monitor_sinks().len(), 2);
    assert_eq!(dispatcher.error_sinks().len(), 0);
    assert_eq!(dispatcher.all_sinks().len(), 2);

    // stop_all stops every unique sink; stop_monitor_sinks the monitor ones.
    dispatcher.stop_all().await;
    dispatcher.stop_monitor_sinks().await;
}

#[tokio::test]
async fn stop_all_tolerates_stop_failures() {
    let dispatcher = SinkDispatcher::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![Arc::new(failing_runtime("monitor"))],
    );
    dispatcher.stop_all().await;
    dispatcher.stop_monitor_sinks().await;
}
