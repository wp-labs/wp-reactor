use std::collections::{HashMap, HashSet};

use wf_core::rule::{Value, WindowLookup, batch_to_events, batch_to_timestamped_rows};
use wf_core::window::Router;

// ---------------------------------------------------------------------------
// RegistryLookup -- WindowLookup adapter backed by the shared Router
// ---------------------------------------------------------------------------

/// Implements [`WindowLookup`] by snapshotting windows from the shared
/// [`Router`]'s registry. Used for `window.has()` guards and join evaluation.
pub(super) struct RegistryLookup<'a>(pub(super) &'a Router);

impl WindowLookup for RegistryLookup<'_> {
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>> {
        let batches = self.0.registry().snapshot(window)?;
        let mut values = HashSet::new();
        for batch in &batches {
            for event in batch_to_events(batch) {
                if let Some(val) = event.fields.get(field) {
                    // Convert all value types to string for set membership
                    match val {
                        Value::Str(s) => {
                            values.insert(s.clone());
                        }
                        Value::Number(n) => {
                            values.insert(n.to_string());
                        }
                        Value::Bool(b) => {
                            values.insert(b.to_string());
                        }
                        Value::Array(_) => {
                            // Arrays are not supported for has() membership checks
                        }
                    }
                }
            }
        }
        Some(values)
    }

    fn snapshot(&self, window: &str) -> Option<Vec<HashMap<String, Value>>> {
        let batches = self.0.registry().snapshot(window)?;
        let mut rows = Vec::new();
        for batch in &batches {
            for event in batch_to_events(batch) {
                rows.push(event.fields);
            }
        }
        Some(rows)
    }

    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, HashMap<String, Value>)>> {
        let win_lock = self.0.registry().get_window(window)?;
        let win = win_lock.read().expect("window lock poisoned");
        let time_col = win.time_col_index()?;
        let batches = win.snapshot();
        drop(win);

        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(batch_to_timestamped_rows(batch, time_col));
        }
        Some(rows)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
    use wf_core::window::{WindowDef, WindowParams, WindowRegistry};

    fn ts_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("ip", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]))
    }

    fn test_config() -> WindowConfig {
        WindowConfig {
            name: "default".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
        }
    }

    fn make_def(name: &str, streams: Vec<&str>) -> WindowDef {
        WindowDef {
            params: WindowParams {
                name: name.into(),
                schema: ts_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
            },
            streams: streams.into_iter().map(String::from).collect(),
            config: test_config(),
        }
    }

    #[test]
    fn snapshot_with_timestamps_returns_correct_rows() {
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("threat_intel", vec!["feed"])]).unwrap();
        let router = Router::new(reg);

        let ts1: i64 = 1_000_000_000;
        let ts2: i64 = 2_000_000_000;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![ts1, ts2])),
                Arc::new(StringArray::from(vec!["10.0.0.1", "10.0.0.2"])),
                Arc::new(Int64Array::from(vec![80, 95])),
            ],
        )
        .unwrap();

        router.route("feed", batch).unwrap();

        let lookup = RegistryLookup(&router);
        let rows = lookup
            .snapshot_with_timestamps("threat_intel")
            .expect("window should exist");

        assert_eq!(rows.len(), 2);

        // Row 0: ts=1s
        assert_eq!(rows[0].0, ts1);
        assert_eq!(rows[0].1["ip"], Value::Str("10.0.0.1".into()));
        assert_eq!(rows[0].1["score"], Value::Number(80.0));
        // Time column should also be present as a field
        assert_eq!(rows[0].1["ts"], Value::Number(ts1 as f64));

        // Row 1: ts=2s
        assert_eq!(rows[1].0, ts2);
        assert_eq!(rows[1].1["ip"], Value::Str("10.0.0.2".into()));
        assert_eq!(rows[1].1["score"], Value::Number(95.0));
    }

    #[test]
    fn snapshot_with_timestamps_none_for_missing_window() {
        let reg = WindowRegistry::build(vec![]).unwrap();
        let router = Router::new(reg);
        let lookup = RegistryLookup(&router);

        assert!(lookup.snapshot_with_timestamps("nonexistent").is_none());
    }

    #[test]
    fn snapshot_with_timestamps_none_for_no_time_column() {
        // Schema without a time column
        let schema = Arc::new(Schema::new(vec![
            Field::new("ip", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]));
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "no_ts".into(),
                schema: schema.clone(),
                time_col_index: None,
                over: Duration::from_secs(3600),
            },
            streams: vec!["data".into()],
            config: test_config(),
        }])
        .unwrap();
        let router = Router::new(reg);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["10.0.0.1"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();
        router.route("data", batch).unwrap();

        let lookup = RegistryLookup(&router);
        // time_col_index is None → snapshot_with_timestamps returns None
        assert!(lookup.snapshot_with_timestamps("no_ts").is_none());
    }
}
