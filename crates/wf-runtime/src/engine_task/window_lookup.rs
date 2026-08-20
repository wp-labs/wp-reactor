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
///
/// M2 (seq-watermark consistency): carries an optional `max_seq`. When set,
/// every window read is bounded to batches with `seq <= max_seq` — the strict
/// visibility ceiling for the batch being processed (see
/// `window-actor-pull-model.md` §3.5). `None` reads the full window (legacy
/// behavior).
pub(super) struct RegistryLookup<'a> {
    router: &'a Router,
    /// Visibility ceiling: only batches with `seq <= max_seq` are seen across
    /// `snapshot` / has / join reads. `None` = full window (no watermark).
    max_seq: Option<u64>,
    /// When set, the watermark only applies to this source window (the window
    /// whose batch is currently being processed). Join targets are a *different*
    /// window with their own seq space, so they must be read at their full,
    /// committed state — never bounded by the source window's batch seq. `None`
    /// keeps the legacy behavior of bounding every window (used by tests).
    source_window: Option<&'a str>,
}

impl<'a> RegistryLookup<'a> {
    /// No seq watermark — reads the full window (legacy behavior).
    pub(super) fn new(router: &'a Router) -> Self {
        Self {
            router,
            max_seq: None,
            source_window: None,
        }
    }

    /// Seq-watermarked lookup bounded across *every* window (legacy/test
    /// behavior). Prefer [`Self::with_source_watermark`] on the hot path so a
    /// join target is not wrongly bounded by the source window's seq.
    #[cfg(test)]
    pub(super) fn with_max_seq(router: &'a Router, max_seq: Option<u64>) -> Self {
        Self {
            router,
            max_seq,
            source_window: None,
        }
    }

    /// Seq-watermarked lookup where the watermark applies **only** to
    /// `source_window` (the window whose batch is being processed). Every other
    /// window — a join target with its own independent seq space — is read at
    /// its full committed state, which also keeps the join on the O(1) index
    /// path instead of the O(window) scan fallback.
    pub(super) fn with_source_watermark(
        router: &'a Router,
        max_seq: Option<u64>,
        source_window: &'a str,
    ) -> Self {
        Self {
            router,
            max_seq,
            source_window: Some(source_window),
        }
    }

    /// Effective watermark for a specific window read.
    fn eff_max_seq(&self, window: &str) -> Option<u64> {
        match self.source_window {
            Some(src) if src == window => self.max_seq,
            Some(_) => None,
            None => self.max_seq,
        }
    }
}

impl<'a> WindowLookup for RegistryLookup<'a> {
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>> {
        let win = self.router.registry().get_window(window)?;
        let generation = win.generation();
        // Cache hit: same content generation → return the cached distinct set
        // without rescanning the window (a `window.has()` per rule event is
        // O(distinct) instead of O(window rows)). The cache is keyed to the
        // full window's generation, so it is only valid for the *unbounded*
        // (`max_seq = None`) view — a watermarked read must re-scan with the
        // seq cut set by `max_seq`.
        let eff_max_seq = self.eff_max_seq(window);
        if eff_max_seq.is_none()
            && let Some(cached) = self
                .router
                .registry()
                .has_field_values(window, field, generation)
        {
            return Some(cached.as_ref().clone());
        }
        // Cache miss / stale / watermarked: read only the referenced column from
        // each (visible) batch — the whole window is never materialized into
        // Event HashMaps.
        let mut values = HashSet::new();
        for batch in win.snapshot_up_to(eff_max_seq) {
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
        // Only the unbounded view is safe to cache (generation-keyed); bounded
        // views are re-scanned per unique watermark.
        if eff_max_seq.is_none() {
            self.router.registry().put_has_field_values(
                window,
                field,
                generation,
                Arc::clone(&set),
            );
        }
        Some(set.as_ref().clone())
    }

    fn snapshot(&self, window: &str) -> Option<Vec<JoinRow>> {
        // Try provider window first (rows are already-materialized maps → wrap
        // them as Event rows). Provider windows are table-backed and do not
        // carry a log seq space, so `max_seq` is not applied here.
        if let Some(rows) = self.router.registry().provider_snapshot(window) {
            return Some(
                rows.into_iter()
                    .map(|row| {
                        JoinRow::Event(Arc::new(Event {
                            fields: row.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                        }))
                    })
                    .collect(),
            );
        }
        // Buffer window: columnar rows straight from the (seq-bounded) batches —
        // no whole-window Event/HashMap materialization.
        let win = self.router.registry().get_window(window)?;
        Some(columnar_join_rows(
            win.snapshot_up_to(self.eff_max_seq(window)),
        ))
    }

    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, JoinRow)>> {
        let win = self.router.registry().get_window(window)?;
        let time_col = win.time_col_index()?;
        Some(columnar_timestamped_join_rows(
            win.snapshot_up_to(self.eff_max_seq(window)),
            time_col,
        ))
    }

    fn join_lookup(&self, window: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let win = self.router.registry().get_window(window)?;
        let join_key = JoinKey::from_value(key)?;
        // Indexed lookup if the window has a maintained join index (the index
        // stores already-materialized `Arc<Event>`s — wrap them directly, no
        // per-lookup HashMap conversion). The index is built incrementally
        // across the *full* window up to the current generation, so it is not
        // seq-cut safe: under a `max_seq` watermark we bypass it and scan.
        if self.eff_max_seq(window).is_none()
            && let Some(events) = win.join_lookup(&join_key)
        {
            return Some(events.into_iter().map(JoinRow::Event).collect());
        }
        // Scan fallback (bounded watermark, or no index): filter the seq-bounded
        // snapshot by key equality. Rows are keyed through the same
        // `JoinKey::from_value` truncation the join index uses, so the bounded
        // scan is byte-identical to the unbounded index path (both truncate
        // floats to `Int` and compare with exact scalar equality). Inlined here
        // rather than delegating to the trait's default `join_lookup` — that
        // default re-dispatches to this override and would recurse.
        let rows = self.snapshot(window)?;
        Some(
            rows.into_iter()
                .filter(|row| {
                    row.field_value(key_field)
                        .and_then(|v| JoinKey::from_value(&v))
                        .is_some_and(|row_key| row_key == join_key)
                })
                .collect(),
        )
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

        let lookup = RegistryLookup::new(&router);
        let rows = lookup
            .snapshot_with_timestamps("threat_intel")
            .expect("window should exist");

        assert_eq!(rows.len(), 2);

        // Row 0: ts=1s
        assert_eq!(rows[0].0, ts1);
        assert_eq!(
            rows[0].1.field_value("ip"),
            Some(Value::Str("10.0.0.1".into()))
        );
        assert_eq!(rows[0].1.field_value("score"), Some(Value::Number(80.0)));
        // Time column should also be present as a field
        assert_eq!(rows[0].1.field_value("ts"), Some(Value::Number(ts1 as f64)));

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

        let lookup = RegistryLookup::new(&router);
        let rows = lookup
            .join_lookup("threat_intel", "ip", &Value::Str("10.0.0.1".into()))
            .expect("indexed window should return rows");
        assert_eq!(rows.len(), 1, "one row matches key ip=10.0.0.1");
        assert_eq!(
            rows[0].field_value("ip"),
            Some(Value::Str("10.0.0.1".into()))
        );
        assert_eq!(rows[0].field_value("score"), Some(Value::Number(80.0)));

        // No match → empty (not None — the window IS indexed).
        let none = lookup
            .join_lookup("threat_intel", "ip", &Value::Str("9.9.9.9".into()))
            .expect("indexed window exists");
        assert!(none.is_empty(), "unknown key → empty rows");
    }

    #[tokio::test]
    async fn source_watermark_does_not_bound_join_target() {
        // M2 regression: a single global `max_seq` used to bound *every* window
        // read, including join targets. Join targets are a different window with
        // their own seq space, so bounding them by the source window's seq both
        // mis-filtered rows and forced the O(window) scan fallback on every join
        // lookup (q3 pull-mode stall). This locks the fix: the watermark applies
        // only to the source window.
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![
            make_def("src", vec!["src_feed"]),
            make_def("join_tgt", vec!["join_feed"]),
        ])
        .unwrap();
        let router = Router::new(reg);
        router
            .registry()
            .get_window("join_tgt")
            .unwrap()
            .set_join_key("ip".into());

        // Join target gets two batches (seq 0 and seq 1).
        router
            .route(
                "join_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "join_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![2_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.2"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Source window gets one batch (seq 0).
        router
            .route(
                "src_feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![3_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.9"])),
                        Arc::new(Int64Array::from(vec![1])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Processing `src` batch seq 0: the join target must NOT be bounded by
        // src's seq 0 — it has its own seq space (seq 0,1) and must see both
        // rows through the index.
        let lookup = RegistryLookup::with_source_watermark(&router, Some(0), "src");
        let rows = lookup
            .join_lookup("join_tgt", "ip", &Value::Str("10.0.0.2".into()))
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "join-target seq-1 row must be visible despite src seq-0 watermark"
        );
        assert_eq!(
            rows[0].field_value("ip"),
            Some(Value::Str("10.0.0.2".into()))
        );

        // The source window itself IS bounded: `snapshot("src")` under the
        // seq-0 watermark sees only its own seq-0 batch (one row).
        assert_eq!(
            lookup.snapshot("src").unwrap().len(),
            1,
            "source window must still respect its own watermark"
        );
    }

    #[tokio::test]
    async fn source_watermark_scopes_has_lookup() {
        // `window.has()` (snapshot_field_values) must be scoped identically to
        // `snapshot`: the source window's distinct values are bounded by the
        // watermark, while a join target's are not (full window + cache).
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![
            make_def("src", vec!["src_feed"]),
            make_def("join_tgt", vec!["join_feed"]),
        ])
        .unwrap();
        let router = Router::new(reg);

        // join_tgt: seq 0 -> ip A, seq 1 -> ip B.
        router
            .route(
                "join_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "join_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![2_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.2"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // src: seq 0 -> ip C, seq 1 -> ip D.
        router
            .route(
                "src_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![3_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.9"])),
                        Arc::new(Int64Array::from(vec![1])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "src_feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![4_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.10"])),
                        Arc::new(Int64Array::from(vec![2])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Processing `src` batch seq 0.
        let lookup = RegistryLookup::with_source_watermark(&router, Some(0), "src");

        // Join target has() sees the full window (both distinct IPs).
        assert_eq!(
            lookup.snapshot_field_values("join_tgt", "ip").unwrap(),
            HashSet::from(["10.0.0.1".to_string(), "10.0.0.2".to_string()]),
            "join-target has() must not be bounded by the source watermark"
        );

        // Source has() is bounded to seq 0 (only its first batch's IP).
        assert_eq!(
            lookup.snapshot_field_values("src", "ip").unwrap(),
            HashSet::from(["10.0.0.9".to_string()]),
            "source has() must respect its own watermark"
        );
    }

    #[tokio::test]
    async fn source_watermark_self_join_still_bounded() {
        // A self-join (join target == source window) must STILL respect the
        // watermark: `eff_max_seq` returns the bound for the source window, so
        // the join skips the unbounded index and scans the bounded snapshot.
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("w", vec!["feed"])]).unwrap();
        let router = Router::new(reg);
        router
            .registry()
            .get_window("w")
            .unwrap()
            .set_join_key("ip".into());

        // seq 0 -> ip A, seq 1 -> ip B.
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![2_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.2"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Source window == join target, so the seq-0 watermark bounds the join:
        // the seq-1 row must be hidden, and the seq-0 row must match.
        let lookup = RegistryLookup::with_source_watermark(&router, Some(0), "w");
        let none = lookup
            .join_lookup("w", "ip", &Value::Str("10.0.0.2".into()))
            .unwrap();
        assert_eq!(
            none.len(),
            0,
            "self-join must not see rows beyond the source watermark"
        );
        let hit = lookup
            .join_lookup("w", "ip", &Value::Str("10.0.0.1".into()))
            .unwrap();
        assert_eq!(hit.len(), 1, "self-join must see the in-watermark row");
    }

    #[tokio::test]
    async fn source_watermark_scopes_snapshot_with_timestamps() {
        // `snapshot_with_timestamps` (asof join) must be scoped like `snapshot`:
        // join target unbounded, source bounded.
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![
            make_def("src", vec!["src_feed"]),
            make_def("join_tgt", vec!["join_feed"]),
        ])
        .unwrap();
        let router = Router::new(reg);

        // join_tgt: two batches (seq 0, seq 1).
        router
            .route(
                "join_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "join_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![2_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.2"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // src: two batches (seq 0, seq 1).
        router
            .route(
                "src_feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![3_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.9"])),
                        Arc::new(Int64Array::from(vec![1])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "src_feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![4_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.10"])),
                        Arc::new(Int64Array::from(vec![2])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let lookup = RegistryLookup::with_source_watermark(&router, Some(0), "src");

        // Join target timestamps unbounded (both rows).
        assert_eq!(
            lookup.snapshot_with_timestamps("join_tgt").unwrap().len(),
            2,
            "join-target asof rows must not be bounded by the source watermark"
        );

        // Source timestamps bounded to seq 0 (one row).
        assert_eq!(
            lookup.snapshot_with_timestamps("src").unwrap().len(),
            1,
            "source asof rows must respect the source watermark"
        );
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
                Arc::new(Int64Array::from(vec![
                    Some(80),
                    Some(95),
                    Some(80),
                    Some(100),
                ])),
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

        let lookup = RegistryLookup::new(&router);
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
        assert!(
            lookup
                .snapshot_field_values("t", "nope")
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn snapshot_field_values_caches_and_refreshes_on_append() {
        // Repeated `window.has()` on an unchanged window hits the cache
        // (O(distinct)); after an append the window generation bumps and the
        // set refreshes with the new distinct value.
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::Utf8, true)]));
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
                    vec![Arc::new(StringArray::from(vec![
                        Some("10.0.0.1"),
                        Some("10.0.0.2"),
                    ]))],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(router.registry().get_window("t").unwrap().generation() > gen_before);

        let lookup = RegistryLookup::new(&router);
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
        let lookup = RegistryLookup::new(&router);

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

        let lookup = RegistryLookup::new(&router);
        // time_col_index is None → snapshot_with_timestamps returns None
        assert!(lookup.snapshot_with_timestamps("no_ts").is_none());
    }

    // -- M2: seq-watermark consistency (window-actor-pull-model.md §3.5) ----

    /// `max_seq` bounds window snapshot + has() reads to batches with
    /// `seq <= max_seq` — the strict visibility ceiling when a rule task is
    /// processing batch N. (The bounded join path falls back to the bounded
    /// snapshot scan, so it is covered by the snapshot assertion here.)
    #[tokio::test]
    async fn max_seq_bounds_snapshot_and_has() {
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("w", vec!["feed"])]).unwrap();
        let router = Router::new(reg);
        router
            .registry()
            .get_window("w")
            .unwrap()
            .set_join_key("ip".into());

        // Batch A (ip 10.0.0.1) lands as seq 0; batch B (ip 10.0.0.2) as seq 1.
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![2_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.2"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Unbounded (legacy) view sees both batches / both distinct IPs.
        let full = RegistryLookup::new(&router);
        assert_eq!(
            full.snapshot_field_values("w", "ip").unwrap(),
            HashSet::from(["10.0.0.1".to_string(), "10.0.0.2".to_string()])
        );
        assert_eq!(full.snapshot("w").unwrap().len(), 2);

        // Watermarked to seq 0 (processing the first batch): only batch A's
        // IP is visible; the later batch must be hidden.
        let bounded = RegistryLookup::with_max_seq(&router, Some(0));
        assert_eq!(
            bounded.snapshot_field_values("w", "ip").unwrap(),
            HashSet::from(["10.0.0.1".to_string()]),
            "has() must not see batches beyond the processed seq"
        );
        assert_eq!(
            bounded.snapshot("w").unwrap().len(),
            1,
            "snapshot must not include batches beyond the processed seq"
        );

        // `max_seq=None` through the watermarked constructor == the full view.
        let bounded_none = RegistryLookup::with_max_seq(&router, None);
        assert_eq!(
            bounded_none.snapshot_field_values("w", "ip").unwrap().len(),
            2
        );

        // Bounded join: skips the (unbounded) join index and scans only the
        // visible batches, so the seq-1 batch must not match.
        let nothing = bounded
            .join_lookup("w", "ip", &Value::Str("10.0.0.2".into()))
            .unwrap();
        assert_eq!(nothing.len(), 0);
        let hit = bounded
            .join_lookup("w", "ip", &Value::Str("10.0.0.1".into()))
            .unwrap();
        assert_eq!(hit.len(), 1);
    }

    /// Unbounded join on an **unindexed** window exercises the inlined scan
    /// fallback (the path that previously recursed through the trait default
    /// and overflowed the stack). Must return the matching row without
    /// recursing.
    #[tokio::test]
    async fn join_lookup_scan_fallback_unindexed_does_not_recurse() {
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("w", vec!["feed"])]).unwrap();
        let router = Router::new(reg);
        // NOTE: no `set_join_key` — the window has no join index, so
        // `join_lookup` must take the scan fallback.
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let lookup = RegistryLookup::new(&router);
        let rows = lookup
            .join_lookup("w", "ip", &Value::Str("10.0.0.1".into()))
            .unwrap();
        assert_eq!(rows.len(), 1, "scan fallback matches the single keyed row");
        assert_eq!(
            rows[0].field_value("ip"),
            Some(Value::Str("10.0.0.1".into()))
        );
        // Non-matching key → empty (not None — the window exists).
        let none = lookup
            .join_lookup("w", "ip", &Value::Str("9.9.9.9".into()))
            .unwrap();
        assert!(none.is_empty());
    }

    /// `max_seq` also bounds `snapshot_with_timestamps`: only batches with
    /// `seq <= max_seq` contribute timestamped rows.
    #[tokio::test]
    async fn max_seq_bounds_snapshot_with_timestamps() {
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("w", vec!["feed"])]).unwrap();
        let router = Router::new(reg);

        let ts1: i64 = 1_000_000_000;
        let ts2: i64 = 2_000_000_000;
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![ts1])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![80])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        router
            .route(
                "feed",
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![ts2])),
                        Arc::new(StringArray::from(vec!["10.0.0.2"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let full = RegistryLookup::new(&router);
        assert_eq!(full.snapshot_with_timestamps("w").unwrap().len(), 2);

        let bounded = RegistryLookup::with_max_seq(&router, Some(0));
        let rows = bounded.snapshot_with_timestamps("w").unwrap();
        assert_eq!(rows.len(), 1, "only the seq-0 batch is visible");
        assert_eq!(rows[0].0, ts1);
    }
}
