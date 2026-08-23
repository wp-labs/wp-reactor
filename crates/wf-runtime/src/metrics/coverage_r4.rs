//! metrics/mod.rs 第四轮补测（注册于 metrics/mod.rs）。
//!
//! 覆盖点:
//! - `to_records` 的 `alert_emitted_detail` 逐 (rule, machine, scope) 记录导出
//!   （此前 `inc_alert_emitted_detail` 的填充已有测试, 但 to_records 的导出
//!   循环未覆盖）。
//! - `percentile` 溢出桶分支（target 落在最后一个上界之后的溢出桶 →
//!   `lower * 2.0` 估算）。
//! - `add_receiver_window_miss` 的 `count == 0` 早退分支。

use super::*;
use std::time::Duration;

fn metrics() -> RuntimeMetrics {
    let mut source_types = BTreeMap::new();
    source_types.insert("s1".to_string(), "file".to_string());
    RuntimeMetrics::new(
        &["r1".to_string()],
        &["w1".to_string()],
        &["s1".to_string()],
        source_types,
    )
}

fn record_field<'a>(record: &'a MetricsRecord, name: &str) -> Option<&'a str> {
    record
        .fields
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

#[test]
fn to_records_exports_alert_emitted_detail_per_scope() {
    let m = metrics();
    m.inc_alert_emitted("r1", "machine-a", "sip=10.0.0.1");
    m.inc_alert_emitted("r1", "machine-a", "sip=10.0.0.2");
    m.inc_alert_emitted("r1", "machine-b", "sip=10.0.0.3");

    let records = m.snapshot().to_records();
    let detail: Vec<&MetricsRecord> = records
        .iter()
        .filter(|r| {
            r.fields
                .iter()
                .any(|(k, v)| k == "name" && v == "emitted_detail")
        })
        .collect();
    assert_eq!(detail.len(), 3, "one detail record per (machine, scope)");
    let field = record_field;
    assert_eq!(field(detail[0], "label"), Some("r1"));
    assert_eq!(field(detail[0], "machine"), Some("machine-a"));
    assert_eq!(field(detail[0], "scope_key"), Some("sip=10.0.0.1"));
    assert_eq!(field(detail[0], "value"), Some("1"));
    // stage/name 形状与其它 alert 记录一致。
    assert_eq!(field(detail[0], "stage"), Some("alert"));
}

#[test]
fn percentile_overflow_bucket_uses_lower_bound_doubling() {
    // 单上界 1ms：所有观测都 > 1ms → target 落在溢出桶（index == len）。
    let hist = Histogram::from_seconds_bounds(&[0.001]);
    for _ in 0..10 {
        hist.observe_duration(Duration::from_secs(5));
    }
    let snap = hist.snapshot();
    let p50 = percentile(&snap, 0.50);
    // 溢出桶估计: lower = 0.001, upper = lower * 2 = 0.002;
    // count=10, target=ceil(5)=5, excess=5 → frac=0.5 → 0.001 + 0.001*0.5。
    assert!((p50 - 0.0015).abs() < 1e-9, "got {p50}");
    // p99 同样落在溢出桶: target=ceil(9.9)=10, excess=0 → frac=1 → 0.002。
    let p99 = percentile(&snap, 0.99);
    assert!((p99 - 0.002).abs() < 1e-9, "got {p99}");
}

#[test]
fn add_receiver_window_miss_zero_count_is_noop() {
    let m = metrics();
    // count == 0 → 早退, 不产生任何记录。
    m.add_receiver_window_miss("s1", "empty", 0);
    let snap = m.snapshot();
    assert!(snap.receiver_window_misses.is_empty());
    // 非零 → 正常计数（对照）。
    m.add_receiver_window_miss("s1", "unknown_stream_schema", 3);
    let snap = m.snapshot();
    assert_eq!(
        snap.receiver_window_misses
            .get("s1")
            .and_then(|r| r.get("unknown_stream_schema")),
        Some(&3)
    );
}
