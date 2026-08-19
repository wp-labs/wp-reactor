use std::collections::HashSet;
use std::sync::Arc;

use wf_engine::match_engine::{
    Event, JoinKey, JoinRow, Value, WindowLookup, column_scalar_string, columnar_join_rows,
    columnar_timestamped_join_rows,
};
use wf_engine::window::Router;

// ---------------------------------------------------------------------------
// RegistryLookup -- WindowLookup adapter backed by the shared Router
// ---------------------------------------------------------------------------

/// Implements [`WindowLookup`] by snapshotting windows from the shared
/// [`Router`]'s registry. Used for `window.has()` guards and join evaluation.
pub(super) struct RegistryLookup<'a>(pub(super) &'a Router);

impl WindowLookup for RegistryLookup<'_> {
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>> {
        let win = self.0.registry().get_window(window)?;
        let generation = win.generation();
        // Cache hit: same content generation → return the cached distinct set
        // without rescanning the window (a `window.has()` per rule event is
        // O(distinct) instead of O(window rows)).
        if let Some(cached) = self
            .0
            .registry()
            .has_field_values(window, field, generation)
        {
            return Some(cached.as_ref().clone());
        }
        // Cache miss / stale: read only the referenced column straight from
        // each batch — the whole window is never materialized into Event
        // HashMaps. The string form (Str text / f64 Display / Bool) is
        // byte-identical to the old `batch_to_events` + `fields.get` path via
        // the shared `extract_field_value` conversion; structured/null cells
        // are skipped exactly like the Array/Object branch.
        let mut values = HashSet::new();
        for batch in win.snapshot() {
            let Ok(col_idx) = batch.schema_ref().index_of(field) else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if let Some(s) = column_scalar_string(&batch, col_idx, row) {
                    values.insert(s);
                }
            }
        }
        let set = Arc::new(values);
        self.0
            .registry()
            .put_has_field_values(window, field, generation, Arc::clone(&set));
        Some(set.as_ref().clone())
    }

    fn snapshot(&self, window: &str) -> Option<Vec<JoinRow>> {
        // Try provider window first (rows are already-materialized maps → wrap
        // them as Event rows).
        if let Some(rows) = self.0.registry().provider_snapshot(window) {
            return Some(
                rows.into_iter()
                    .map(|row| {
                        JoinRow::Event(Arc::new(Event {
                            fields: row
                                .into_iter()
                                .map(|(k, v)| (k.into(), v))
                                .collect(),
                        }))
                    })
                    .collect(),
            );
        }
        // Buffer window: columnar rows straight from the batches — no
        // whole-window Event/HashMap materialization.
        let batches = self.0.registry().snapshot(window)?;
        Some(columnar_join_rows(batches))
    }

    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, JoinRow)>> {
        let win = self.0.registry().get_window(window)?;
        let time_col = win.time_col_index()?;
        Some(columnar_timestamped_join_rows(win.snapshot(), time_col))
    }

    // `key_field` is only used when forwarding to the trait's scan fallback.
    #[allow(clippy::only_used_in_recursion)]
    fn join_lookup(&self, window: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let win = self.0.registry().get_window(window)?;
        let key = JoinKey::from_value(key)?;
        // Indexed lookup if the window has a maintained join index (the index
        // stores already-materialized `Arc<Event>`s — wrap them directly, no
        // per-lookup HashMap conversion); otherwise fall back to the trait's
        // columnar snapshot scan.
        if let Some(events) = win.join_lookup(&key) {
            return Some(events.into_iter().map(JoinRow::Event).collect());
        }
        WindowLookup::join_lookup(self, window, key_field, &key_converted_back(&key)?)
    }
}

/// Convert a [`JoinKey`] back to a [`Value`] for the scan fallback.
fn key_converted_back(key: &JoinKey) -> Option<Value> {
    match key {
        JoinKey::Int(i) => Some(Value::Number(*i as f64)),
        JoinKey::Str(s) => Some(Value::Str(s.as_str().into())),
        JoinKey::Bool(b) => Some(Value::Bool(*b)),
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
    use wf_engine::window::{WindowDef, WindowParams, WindowRegistry};

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
            table: None,
        }
    }

    fn make_def(name: &str, streams: Vec<&str>) -> WindowDef {
        WindowDef {
            params: WindowParams {
                name: name.into(),
                schema: ts_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: streams.into_iter().map(String::from).collect(),
            config: test_config(),
        }
    }

    #[tokio::test]
    async fn snapshot_with_timestamps_returns_correct_rows() {
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

        router.route("feed", batch).await.unwrap();

        let lookup = RegistryLookup(&router);
        let rows = lookup
            .snapshot_with_timestamps("threat_intel")
            .expect("window should exist");

        assert_eq!(rows.len(), 2);

        // Row 0: ts=1s
        assert_eq!(rows[0].0, ts1);
        assert_eq!(rows[0].1.field_value("ip"), Some(Value::Str("10.0.0.1".into())));
        assert_eq!(rows[0].1.field_value("score"), Some(Value::Number(80.0)));
        // Time column should also be present as a field
        assert_eq!(
            rows[0].1.field_value("ts"),
            Some(Value::Number(ts1 as f64))
        );

        // Row 1: ts=2s
        assert_eq!(rows[1].0, ts2);
        assert_eq!(
            rows[1].1.field_value("ip"),
            Some(Value::Str("10.0.0.2".into()))
        );
        assert_eq!(rows[1].1.field_value("score"), Some(Value::Number(95.0)));
    }

    #[tokio::test]
    async fn join_lookup_uses_window_index() {
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("threat_intel", vec!["feed"])]).unwrap();
        let router = Router::new(reg);
        // Configure the window as a join target keyed on `ip`.
        router
            .registry()
            .get_window("threat_intel")
            .unwrap()
            .set_join_key("ip".into());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_000_000_000,
                    2_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["10.0.0.1", "10.0.0.2"])),
                Arc::new(Int64Array::from(vec![80, 95])),
            ],
        )
        .unwrap();
        router.route("feed", batch).await.unwrap();

        let lookup = RegistryLookup(&router);
        let rows = lookup
            .join_lookup("threat_intel", "ip", &Value::Str("10.0.0.1".into()))
            .expect("indexed window should return rows");
        assert_eq!(rows.len(), 1, "one row matches key ip=10.0.0.1");
        assert_eq!(rows[0].field_value("ip"), Some(Value::Str("10.0.0.1".into())));
        assert_eq!(rows[0].field_value("score"), Some(Value::Number(80.0)));

        // No match → empty (not None — the window IS indexed).
        let none = lookup
            .join_lookup("threat_intel", "ip", &Value::Str("9.9.9.9".into()))
            .expect("indexed window exists");
        assert!(none.is_empty(), "unknown key → empty rows");
    }

    #[tokio::test]
    async fn snapshot_field_values_reads_single_column() {
        // window.has() membership: reads ONLY the referenced column from each
        // batch — no Event HashMap materialization. The string form matches the
        // Event path: Int64 → f64 Display, Utf8 → its text, Bool → true/false;
        // null cells and a missing field are skipped/empty.
        let schema = Arc::new(Schema::new(vec![
            Field::new("ip", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "t".into(),
                schema: schema.clone(),
                time_col_index: None,
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec!["feed".into()],
            config: test_config(),
        }])
        .unwrap();
        let router = Router::new(reg);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("10.0.0.1"),
                    Some("10.0.0.1"),
                    None,
                    Some("10.0.0.2"),
                ])),
                Arc::new(Int64Array::from(vec![Some(80), Some(95), Some(80), Some(100)])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(true),
                ])),
            ],
        )
        .unwrap();
        router.route("feed", batch).await.unwrap();

        let lookup = RegistryLookup(&router);
        let ips = lookup.snapshot_field_values("t", "ip").unwrap();
        assert_eq!(
            ips,
            HashSet::from(["10.0.0.1".to_string(), "10.0.0.2".to_string()])
        );
        let scores = lookup.snapshot_field_values("t", "score").unwrap();
        assert_eq!(
            scores,
            HashSet::from(["80".to_string(), "95".to_string(), "100".to_string()])
        );
        let active = lookup.snapshot_field_values("t", "active").unwrap();
        assert_eq!(
            active,
            HashSet::from(["true".to_string(), "false".to_string()])
        );
        // Missing field → empty set (not None); the window exists.
        assert!(lookup.snapshot_field_values("t", "nope").unwrap().is_empty());
    }

    #[tokio::test]
    async fn snapshot_field_values_caches_and_refreshes_on_append() {
        // Repeated `window.has()` on an unchanged window hits the cache
        // (O(distinct)); after an append the window generation bumps and the
        // set refreshes with the new distinct value.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ip",
            DataType::Utf8,
            true,
        )]));
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "t".into(),
                schema: schema.clone(),
                time_col_index: None,
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec!["feed".into()],
            config: test_config(),
        }])
        .unwrap();
        let router = Router::new(reg);
        let gen_before = router.registry().get_window("t").unwrap().generation();

        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec![Some("10.0.0.1"), Some("10.0.0.2")]))],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(router.registry().get_window("t").unwrap().generation() > gen_before);

        let lookup = RegistryLookup(&router);
        let first = lookup.snapshot_field_values("t", "ip").unwrap();
        assert_eq!(
            first,
            HashSet::from(["10.0.0.1".to_string(), "10.0.0.2".to_string()])
        );
        // Same generation → cache hit, identical result.
        let second = lookup.snapshot_field_values("t", "ip").unwrap();
        assert_eq!(first, second);

        // Append a new distinct value → generation bumps → refreshed set.
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(vec![Some("10.0.0.3")]))],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let third = lookup.snapshot_field_values("t", "ip").unwrap();
        assert_eq!(
            third,
            HashSet::from([
                "10.0.0.1".to_string(),
                "10.0.0.2".to_string(),
                "10.0.0.3".to_string(),
            ])
        );
    }

    #[test]
    fn snapshot_with_timestamps_none_for_missing_window() {
        let reg = WindowRegistry::build(vec![]).unwrap();
        let router = Router::new(reg);
        let lookup = RegistryLookup(&router);

        assert!(lookup.snapshot_with_timestamps("nonexistent").is_none());
    }

    #[tokio::test]
    async fn snapshot_with_timestamps_none_for_no_time_column() {
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
                materialize_fields: None,
                defer_materialization: false,
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
        router.route("data", batch).await.unwrap();

        let lookup = RegistryLookup(&router);
        // time_col_index is None → snapshot_with_timestamps returns None
        assert!(lookup.snapshot_with_timestamps("no_ts").is_none());
    }
}
