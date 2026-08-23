use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use wf_engine::match_engine::{
    AsofLookup, Event, JoinKey, JoinRow, Value, WindowLookup, column_scalar_string,
    columnar_join_rows, columnar_timestamped_join_rows, values_equal,
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
        // no whole-window Event/HashMap materialization. The window's
        // `materialize_fields` projection keeps enrich to the read set.
        let win = self.router.registry().get_window(window)?;
        let projection = win.materialize_fields().cloned();
        Some(columnar_join_rows(
            win.snapshot_up_to(self.eff_max_seq(window)),
            projection,
        ))
    }

    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, JoinRow)>> {
        let win = self.router.registry().get_window(window)?;
        let time_col = win.time_col_index()?;
        let projection = win.materialize_fields().cloned();
        Some(columnar_timestamped_join_rows(
            win.snapshot_up_to(self.eff_max_seq(window)),
            time_col,
            projection,
        ))
    }

    fn join_lookup(&self, window: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        // Provider (table-backed) windows: no buffer window / join index — scan
        // the static rows by exact key equality (`values_equal`). Provider tables
        // are small static snapshots, so O(rows) per lookup is expected (P4 side
        // input). The scan is intentional here — the index-based path below
        // cannot be used because providers live outside the buffer-window
        // registry (join-key indexes are only maintained for buffer windows).
        if self.router.registry().get_provider(window).is_some() {
            let rows = self.snapshot(window)?;
            return Some(
                rows.into_iter()
                    .filter(|row| {
                        row.field_value(key_field)
                            .is_some_and(|v| values_equal(&v, key))
                    })
                    .collect(),
            );
        }
        let win = self.router.registry().get_window(window)?;
        let join_key = JoinKey::from_value(key)?;
        // Indexed lookup with seq-cut（M2 pull 一致性）: 索引行带 batch seq，
        // `max_seq` 过滤后只返回读者已拉取的 batch 的行（2026-08：索引此前无
        // seq 感知，pull 模式被迫全量扫描 → q13 等 join 查询 CPU/积压瓶颈）。
        // `None`（窗口无索引）→ 回退 snapshot 扫描。
        if let Some(rows) = win.join_lookup(&join_key, self.eff_max_seq(window)) {
            return Some(rows);
        }
        // Scan fallback (no index): filter the seq-bounded snapshot by key
        // equality. Rows are keyed through the same `JoinKey::from_value`
        // truncation the join index uses, so the bounded scan is byte-identical
        // to the unbounded index path (both truncate floats to `Int` and compare
        // with exact scalar equality). Inlined here rather than delegating to
        // the trait's default `join_lookup` — that default re-dispatches to this
        // override and would recurse.
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

    fn asof_candidates(
        &self,
        window: &str,
        key_field: &str,
        key: &Value,
    ) -> Option<Vec<(i64, JoinRow)>> {
        let win = self.router.registry().get_window(window)?;
        let join_key = JoinKey::from_value(key)?;
        // Indexed asof lookup（seq-cut）: 索引行带 batch seq + 原始时间戳,
        // `max_seq` 过滤后 O(1)（2026-08 前 pull 模式回退全量扫描）。
        // `None`（无索引）→ 回退 timestamped 扫描。
        if let Some(rows) = win.join_lookup_timestamped(&join_key, self.eff_max_seq(window)) {
            return Some(rows);
        }
        // Scan fallback: filter the seq-bounded timestamped snapshot by key. The
        // key is truncated through `JoinKey::from_value` exactly like the index,
        // and the timestamps come from the same raw `Timestamp(Ns)` column the
        // index stores, so both paths are byte-identical.
        let rows = self.snapshot_with_timestamps(window)?;
        Some(
            rows.into_iter()
                .filter(|(_, row)| {
                    row.field_value(key_field)
                        .and_then(|v| JoinKey::from_value(&v))
                        .is_some_and(|row_key| row_key == join_key)
                })
                .collect(),
        )
    }

    fn asof_lookup_max(
        &self,
        window: &str,
        _key_field: &str,
        key: &Value,
        event_time_nanos: i64,
        within: Option<&Duration>,
    ) -> AsofLookup {
        let Some(win) = self.router.registry().get_window(window) else {
            return AsofLookup::Fallback;
        };
        let Some(join_key) = JoinKey::from_value(key) else {
            return AsofLookup::Fallback;
        };
        let min_ts = within.map_or(i64::MIN, |d| {
            let nanos = i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
            event_time_nanos.saturating_sub(nanos)
        });
        // 索引 asof 快路径（seq-cut: max_seq 过滤后取最新行; 2026-08 前 pull
        // 模式回退 `asof_candidates` 全量扫描）。`Fallback`（无索引）由调用方
        // 走 asof_candidates。
        win.join_lookup_asof(
            &join_key,
            event_time_nanos,
            min_ts,
            self.eff_max_seq(window),
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

    /// P4 side input：provider 窗口 join_lookup——无 buffer 窗口/索引，按精确
    /// 键等值扫描静态行（`values_equal`）。此前 `get_window` 对 provider 返回
    /// None → join 静默 miss（事件不富化也不丢，结果错误）。
    #[tokio::test]
    async fn join_lookup_provider_window_scans_static_rows() {
        use std::collections::HashMap;
        use wf_engine::window::ProviderWindow;

        let mut reg = WindowRegistry::build(vec![]).unwrap();
        let mut pw = ProviderWindow::new(
            "person_table".into(),
            "SELECT * FROM person_table".into(),
            None,
        );
        pw.load(vec![
            {
                let mut m = HashMap::new();
                m.insert("id".to_string(), Value::Number(5.0));
                m.insert("state".to_string(), Value::Str("CA".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("id".to_string(), Value::Number(7.0));
                m.insert("state".to_string(), Value::Str("ID".into()));
                m
            },
        ]);
        reg.register_provider("person_table".to_string(), pw)
            .unwrap();
        let router = Router::new(reg);

        let lookup = RegistryLookup::new(&router);
        let rows = lookup
            .join_lookup("person_table", "id", &Value::Number(7.0))
            .expect("provider join lookup must hit, not miss");
        assert_eq!(rows.len(), 1, "exactly one provider row matches id=7");
        assert_eq!(rows[0].field_value("state"), Some(Value::Str("ID".into())));

        // 未知键 → Some(空集)（窗口存在，只是无匹配）——与 buffer 窗口一致
        let none = lookup
            .join_lookup("person_table", "id", &Value::Number(999.0))
            .expect("provider window exists");
        assert!(none.is_empty(), "unknown key → empty rows");

        // 不存在的窗口 → None（保持既有语义）
        assert!(
            lookup
                .join_lookup("no_such_window", "id", &Value::Number(5.0))
                .is_none()
        );
    }

    #[tokio::test]
    async fn asof_candidates_uses_index_and_matches_scan() {
        // The asof fast path must return the same rows (and the same raw
        // timestamps) as the timestamped scan fallback: the index stores the
        // raw `Timestamp(Ns)` i64, so epoch-nanos values survive without the
        // f64 round-trip the eager event-time path applies.
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("threat_intel", vec!["feed"])]).unwrap();
        let router = Router::new(reg);
        router
            .registry()
            .get_window("threat_intel")
            .unwrap()
            .set_join_key("ip".into());

        // `ts1` is large enough that `ts1 as f64 as i64` would round it; the raw
        // index path must keep it exact.
        let ts1: i64 = 1_767_225_600_000_000_123;
        let ts2: i64 = 1_767_225_600_000_000_456;
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

        let key = Value::Str("10.0.0.1".into());

        // Indexed path (full window → `eff_max_seq = None`).
        let indexed = RegistryLookup::new(&router)
            .asof_candidates("threat_intel", "ip", &key)
            .expect("window exists");
        assert_eq!(indexed.len(), 1, "one row matches key ip=10.0.0.1");
        assert_eq!(indexed[0].0, ts1, "raw epoch nanos preserved exactly");
        assert_eq!(
            indexed[0].1.field_value("ip"),
            Some(Value::Str("10.0.0.1".into()))
        );

        // Scan fallback (watermarked → index bypassed). seq 0 is the only batch.
        let scanned = RegistryLookup::with_max_seq(&router, Some(0))
            .asof_candidates("threat_intel", "ip", &key)
            .expect("window exists");
        assert_eq!(scanned.len(), 1);

        // Both paths must be byte-identical (ts + joined field values).
        assert_eq!(indexed.len(), scanned.len());
        for ((its, irow), (sts, srow)) in indexed.iter().zip(&scanned) {
            assert_eq!(its, sts);
            assert_eq!(irow.field_value("ip"), srow.field_value("ip"));
            assert_eq!(irow.field_value("score"), srow.field_value("score"));
        }
    }

    #[tokio::test]
    async fn asof_lookup_max_fast_path() {
        let schema = ts_schema();
        let reg = WindowRegistry::build(vec![make_def("threat_intel", vec!["feed"])]).unwrap();
        let router = Router::new(reg);
        router
            .registry()
            .get_window("threat_intel")
            .unwrap()
            .set_join_key("ip".into());

        // Same key ip=10.0.0.1 at ts=1s and ts=3s → per-key max_ts = 3s.
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
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(vec![3_000_000_000])),
                        Arc::new(StringArray::from(vec!["10.0.0.1"])),
                        Arc::new(Int64Array::from(vec![95])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let lookup = RegistryLookup::new(&router);
        let key = Value::Str("10.0.0.1".into());

        // Fast-path hit: max_ts=3s within [2s, 5s] → latest row (score 95).
        match lookup.asof_lookup_max(
            "threat_intel",
            "ip",
            &key,
            5_000_000_000,
            Some(&Duration::from_secs(3)),
        ) {
            AsofLookup::Hit(row) => {
                assert_eq!(row.field_value("score"), Some(Value::Number(95.0)));
                assert_eq!(row.field_value("ip"), Some(Value::Str("10.0.0.1".into())));
            }
            AsofLookup::Miss => panic!("expected Hit, got Miss"),
            AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
        }

        // Too old: max_ts=3s < min_ts=4s (within 1s) → Miss (no scan needed).
        assert!(matches!(
            lookup.asof_lookup_max(
                "threat_intel",
                "ip",
                &key,
                5_000_000_000,
                Some(&Duration::from_secs(1)),
            ),
            AsofLookup::Miss
        ));

        // Too new: max_ts=3s > event_time=2s → index scans and returns the
        // latest row ≤ 2s (ts=1s, score 80) — no caller-side fallback.
        match lookup.asof_lookup_max("threat_intel", "ip", &key, 2_000_000_000, None) {
            AsofLookup::Hit(row) => {
                assert_eq!(row.field_value("score"), Some(Value::Number(80.0)));
            }
            AsofLookup::Miss => panic!("expected Hit for max_ts > event_time, got Miss"),
            AsofLookup::Fallback => panic!("expected Hit for max_ts > event_time, got Fallback"),
        }

        // Unknown key → Miss.
        assert!(matches!(
            lookup.asof_lookup_max(
                "threat_intel",
                "ip",
                &Value::Str("9.9.9.9".into()),
                5_000_000_000,
                None,
            ),
            AsofLookup::Miss
        ));

        // within=None (no lower bound): max_ts=3s <= event_time=3s → Hit.
        match lookup.asof_lookup_max("threat_intel", "ip", &key, 3_000_000_000, None) {
            AsofLookup::Hit(row) => {
                assert_eq!(row.field_value("score"), Some(Value::Number(95.0)));
            }
            AsofLookup::Miss => panic!("expected Hit with within=None, got Miss"),
            AsofLookup::Fallback => panic!("expected Hit with within=None, got Fallback"),
        }
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
