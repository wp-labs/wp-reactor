use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::metrics::RuntimeMetrics;
use wf_engine::match_engine::Value;
use wf_engine::window::Router;

pub(crate) const WINDOW_MISS_WINDOW_NAME: &str = "__window_miss";
const MAX_PAYLOAD_SAMPLE_BYTES: usize = 512;
const MAX_WINDOW_MISS_ROWS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.Receiver")]
pub(crate) enum WindowMissReason {
    UnknownStreamSchema,
    MissingStreamTagField,
}

impl WindowMissReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnknownStreamSchema => "unknown_stream_schema",
            Self::MissingStreamTagField => "missing_stream_tag_field",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Receiver")]
pub(crate) struct WindowMiss {
    pub(crate) stream_tag_field: String,
    pub(crate) stream_tag: Option<String>,
    pub(crate) reason: WindowMissReason,
    pub(crate) sample_payload: String,
    pub(crate) payload_bytes: usize,
    pub(crate) rows: usize,
}

impl WindowMiss {
    pub(crate) fn new(
        stream_tag_field: impl Into<String>,
        stream_tag: Option<String>,
        reason: WindowMissReason,
        sample_payload: impl AsRef<str>,
        rows: usize,
    ) -> Self {
        let sample_payload = sample_payload.as_ref();
        Self {
            stream_tag_field: stream_tag_field.into(),
            stream_tag,
            reason,
            sample_payload: truncate_payload_sample(sample_payload),
            payload_bytes: sample_payload.len(),
            rows,
        }
    }
}

pub(crate) fn report_window_miss(
    source_name: &str,
    source_kind: &str,
    miss: &WindowMiss,
    metrics: Option<&Arc<RuntimeMetrics>>,
    router: Option<&Router>,
) {
    if let Some(metrics) = metrics {
        metrics.add_receiver_window_miss(source_name, miss.reason.as_str(), miss.rows);
    }
    let should_log = router
        .map(|router| record_window_miss(router, source_name, source_kind, miss))
        .unwrap_or(true);
    if should_log {
        wf_warn!(
            conn,
            miss_window = WINDOW_MISS_WINDOW_NAME,
            source = %source_name,
            kind = %source_kind,
            stream_tag_field = %miss.stream_tag_field,
            stream_tag = %miss.stream_tag.as_deref().unwrap_or(""),
            reason = %miss.reason.as_str(),
            rows = miss.rows,
            payload_bytes = miss.payload_bytes,
            sample = %miss.sample_payload,
            "input skipped by window miss"
        );
    }
}

pub(crate) fn record_batch_window_miss(
    source_name: &str,
    source_kind: &str,
    stream_tag_field: &str,
    stream_tag: &str,
    rows: usize,
    metrics: Option<&Arc<RuntimeMetrics>>,
    router: Option<&Router>,
) {
    let miss = WindowMiss::new(
        stream_tag_field,
        Some(stream_tag.to_string()),
        WindowMissReason::UnknownStreamSchema,
        format!("record_batch rows={rows}"),
        rows,
    );
    report_window_miss(source_name, source_kind, &miss, metrics, router);
}

fn record_window_miss(
    router: &Router,
    source_name: &str,
    source_kind: &str,
    miss: &WindowMiss,
) -> bool {
    let Some(provider) = router.registry().get_provider(WINDOW_MISS_WINDOW_NAME) else {
        return true;
    };
    let mut provider = provider
        .write()
        .expect("window miss provider lock poisoned");
    let stream_tag = miss.stream_tag.as_deref().unwrap_or("");
    let reason = miss.reason.as_str();
    let now = now_nanos() as f64;
    provider.update_rows(|rows| {
        record_window_miss_row(
            rows,
            source_name,
            source_kind,
            miss,
            stream_tag,
            reason,
            now,
        )
    })
}

fn record_window_miss_row(
    rows: &mut Vec<HashMap<String, Value>>,
    source_name: &str,
    source_kind: &str,
    miss: &WindowMiss,
    stream_tag: &str,
    reason: &str,
    now: f64,
) -> bool {
    if let Some(existing_idx) = rows.iter().position(|row| {
        str_field(row, "source_name") == Some(source_name)
            && str_field(row, "stream_tag") == Some(stream_tag)
            && str_field(row, "reason") == Some(reason)
    }) {
        let mut existing = rows.remove(existing_idx);
        let count = number_field(&existing, "count").unwrap_or(0.0) + miss.rows as f64;
        existing.insert("count".into(), Value::Number(count));
        existing.insert("last_seen".into(), Value::Number(now));
        existing.insert(
            "payload_bytes".into(),
            Value::Number(miss.payload_bytes as f64),
        );
        if !miss.sample_payload.is_empty() {
            existing.insert(
                "raw_payload".into(),
                Value::Str(miss.sample_payload.clone().into()),
            );
        }
        rows.push(existing);
        return false;
    }

    let mut row = HashMap::new();
    row.insert(
        "source_name".into(),
        Value::Str(source_name.to_string().into()),
    );
    row.insert(
        "source_kind".into(),
        Value::Str(source_kind.to_string().into()),
    );
    row.insert(
        "stream_tag_field".into(),
        Value::Str(miss.stream_tag_field.clone().into()),
    );
    row.insert(
        "stream_tag".into(),
        Value::Str(stream_tag.to_string().into()),
    );
    row.insert("reason".into(), Value::Str(reason.to_string().into()));
    row.insert(
        "raw_payload".into(),
        Value::Str(miss.sample_payload.clone().into()),
    );
    row.insert(
        "payload_bytes".into(),
        Value::Number(miss.payload_bytes as f64),
    );
    row.insert("first_seen".into(), Value::Number(now));
    row.insert("last_seen".into(), Value::Number(now));
    row.insert("count".into(), Value::Number(miss.rows as f64));
    rows.push(row);

    if rows.len() > MAX_WINDOW_MISS_ROWS {
        let overflow = rows.len() - MAX_WINDOW_MISS_ROWS;
        rows.drain(0..overflow);
    }
    true
}

fn str_field<'a>(row: &'a HashMap<String, Value>, key: &str) -> Option<&'a str> {
    match row.get(key) {
        Some(Value::Str(value)) => Some(value),
        _ => None,
    }
}

fn number_field(row: &HashMap<String, Value>, key: &str) -> Option<f64> {
    match row.get(key) {
        Some(Value::Number(value)) => Some(*value),
        _ => None,
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn truncate_payload_sample(payload: &str) -> String {
    if payload.len() <= MAX_PAYLOAD_SAMPLE_BYTES {
        return payload.to_string();
    }

    let mut end = MAX_PAYLOAD_SAMPLE_BYTES;
    while !payload.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &payload[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use wf_engine::window::{ProviderWindow, Router, WindowRegistry};

    fn make_window_miss_router() -> Router {
        let mut registry = WindowRegistry::build(vec![]).unwrap();
        registry
            .register_provider(
                WINDOW_MISS_WINDOW_NAME.to_string(),
                ProviderWindow::new(
                    WINDOW_MISS_WINDOW_NAME.to_string(),
                    "internal://window_miss".to_string(),
                    None,
                ),
            )
            .unwrap();
        Router::new(registry)
    }

    #[test]
    fn truncates_on_char_boundary() {
        let payload = format!("{}{}", "a".repeat(MAX_PAYLOAD_SAMPLE_BYTES - 1), "文");
        let sample = truncate_payload_sample(&payload);
        assert!(sample.ends_with("..."));
        assert!(sample.len() <= MAX_PAYLOAD_SAMPLE_BYTES + 3);
    }

    #[test]
    fn report_window_miss_updates_builtin_provider_snapshot() {
        let router = make_window_miss_router();
        let miss = WindowMiss::new(
            "wp_oml_name",
            Some("unknown".to_string()),
            WindowMissReason::UnknownStreamSchema,
            r#"{"wp_oml_name":"unknown"}"#,
            2,
        );

        report_window_miss("source_a", "kafka", &miss, None, Some(&router));
        report_window_miss("source_a", "kafka", &miss, None, Some(&router));

        let rows = router
            .registry()
            .provider_snapshot(WINDOW_MISS_WINDOW_NAME)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("stream_tag"),
            Some(&Value::Str("unknown".into()))
        );
        assert_eq!(rows[0].get("count"), Some(&Value::Number(4.0)));
    }

    #[test]
    fn window_miss_capacity_keeps_recently_updated_rows() {
        let router = make_window_miss_router();
        for i in 0..MAX_WINDOW_MISS_ROWS {
            let miss = WindowMiss::new(
                "wp_oml_name",
                Some(format!("tag_{i}")),
                WindowMissReason::UnknownStreamSchema,
                format!(r#"{{"wp_oml_name":"tag_{i}"}}"#),
                1,
            );
            report_window_miss("source_a", "kafka", &miss, None, Some(&router));
        }

        let active_old_key = WindowMiss::new(
            "wp_oml_name",
            Some("tag_0".to_string()),
            WindowMissReason::UnknownStreamSchema,
            r#"{"wp_oml_name":"tag_0","updated":true}"#,
            1,
        );
        report_window_miss("source_a", "kafka", &active_old_key, None, Some(&router));

        let new_key = WindowMiss::new(
            "wp_oml_name",
            Some("tag_new".to_string()),
            WindowMissReason::UnknownStreamSchema,
            r#"{"wp_oml_name":"tag_new"}"#,
            1,
        );
        report_window_miss("source_a", "kafka", &new_key, None, Some(&router));

        let rows = router
            .registry()
            .provider_snapshot(WINDOW_MISS_WINDOW_NAME)
            .unwrap();
        assert_eq!(rows.len(), MAX_WINDOW_MISS_ROWS);
        assert_eq!(
            rows.iter()
                .find(|row| str_field(row, "stream_tag") == Some("tag_0"))
                .and_then(|row| number_field(row, "count")),
            Some(2.0)
        );
        assert!(
            rows.iter()
                .all(|row| str_field(row, "stream_tag") != Some("tag_1"))
        );
        assert!(
            rows.iter()
                .any(|row| str_field(row, "stream_tag") == Some("tag_new"))
        );
    }
}
