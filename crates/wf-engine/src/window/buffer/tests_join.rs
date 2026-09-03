//! join 索引测试（2026-09-04 自 tests.rs 拆出；`#[path]` 兄弟子模块）：索引维护/
//! 查找/asof 快路径、分片、增量驱逐、并发，与 join 索引 append 微基准。

use super::*;

// -- join index ------------------------------------------------------------

#[test]
fn join_index_maintained_on_append_and_evict() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Append two batches with overlapping key values.
    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();
    win.append(make_batch(
        &test_schema(),
        &[3_000_000, 4_000_000],
        &[42, 44],
    ))
    .unwrap();

    // Lookup by key: value 42 has 2 rows, 44 has 1, 999 has none.
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(2),
        "two rows with value 42 indexed"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(44), None)
            .map(|v| v.len()),
        Some(1),
        "one row with value 44 indexed"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(999), None)
            .map(|v| v.len()),
        Some(0),
        "indexed but no match → empty (not None)"
    );

    // Expire all batches: over=3600s, now=4000s → cutoff=400s >> event times
    // (1-4ms), so all batches are time-evicted and index entries removed.
    win.evict_expired(4_000_000_000_000);
    assert!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "index cleared after eviction"
    );
}

#[test]
fn join_key_from_value_conversion() {
    use crate::match_engine::EngineHashMap;
    assert_eq!(
        JoinKey::from_value(&Value::Number(42.0)),
        Some(JoinKey::Int(42)),
        "number → Int"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Str("abc".into())),
        Some(JoinKey::Str("abc".into())),
        "string → Str"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Bool(true)),
        Some(JoinKey::Bool(true)),
        "bool → Bool"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Array(vec![])),
        None,
        "array → None (rejected at compile time)"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Object(EngineHashMap::default())),
        None,
        "object → None"
    );
}

#[test]
fn join_index_absent_without_set_join_key() {
    let win = test_window(3600, usize::MAX);
    assert!(
        win.join_lookup("value", &JoinKey::Int(1), None).is_none(),
        "no join index → None (caller falls back to scan)"
    );
    // The asof fast path must also fall back (not Miss) without an index: the
    // caller then runs the full timestamped scan.
    assert!(matches!(
        win.join_lookup_asof("value", &JoinKey::Int(1), 5_000_000_000, 0, None),
        AsofLookup::Fallback
    ));
}

#[test]
fn join_index_multi_key_fields_each_get_own_index() {
    // 2026-08-30 混跑 q8 卡死根因修复：同一窗口被不同规则以不同 key join
    // （q8 按 seller / q20 按 id），每个 key 字段各建索引——后注册者不再被
    // 首键独占而回退全窗扫描（deferred join O(全窗) × pending 数 → 卡死）。
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("id", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let win = Window::new(
        WindowParams {
            name: "multi_key_win".into(),
            schema: Arc::clone(&schema),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    // q20 先注册 id、q8 后注册 seller——两个索引并存（旧实现首键独占）。
    win.set_join_key("id".into());
    win.set_join_key("seller".into());
    assert!(win.has_join_key("id"), "id 索引已建");
    assert!(win.has_join_key("seller"), "seller 索引已建");
    assert!(!win.has_join_key("nope"), "未注册字段无索引");

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_000_000, 2_000_000, 3_000_000,
            ])),
            Arc::new(Int64Array::from(vec![11, 22, 33])),
            Arc::new(Int64Array::from(vec![101, 102, 103])),
        ],
    )
    .unwrap();
    win.append(batch).unwrap();

    // id 索引 O(1) 命中。
    assert_eq!(
        win.join_lookup("id", &JoinKey::Int(22), None)
            .map(|v| v.len()),
        Some(1),
        "id=22 经 id 索引命中"
    );
    // seller 索引 O(1) 命中（旧实现此处回退全窗扫描）。
    assert_eq!(
        win.join_lookup("seller", &JoinKey::Int(103), None)
            .map(|v| v.len()),
        Some(1),
        "seller=103 经 seller 索引命中"
    );
    // timestamped 双索引各自命中。
    assert_eq!(
        win.join_lookup_timestamped("id", &JoinKey::Int(11), None)
            .map(|v| v.len()),
        Some(1)
    );
    assert_eq!(
        win.join_lookup_timestamped("seller", &JoinKey::Int(102), None)
            .map(|v| v.len()),
        Some(1)
    );
    // 未注册字段 → None（调用方回退扫描语义保留）。
    assert!(win.join_lookup("nope", &JoinKey::Int(22), None).is_none());
}

#[test]
fn join_index_built_for_existing_batches_on_set_join_key() {
    let win = test_window(3600, usize::MAX);
    // Data appended before the window is configured as a join target.
    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000], &[44]))
        .unwrap();
    win.set_join_key("value".into());
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(1),
        "existing rows indexed by set_join_key"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(44), None)
            .map(|v| v.len()),
        Some(1),
        "rows from a later batch indexed"
    );
}

#[test]
fn join_index_updated_on_oldest_eviction() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();
    win.append(make_batch(
        &test_schema(),
        &[3_000_000, 4_000_000],
        &[44, 45],
    ))
    .unwrap();

    // evict_oldest (memory-pressure path) must drop the first batch's keys.
    assert!(
        win.evict_oldest().is_some(),
        "evict_oldest returns byte size"
    );
    assert!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "key 42 (first batch) removed after evict_oldest"
    );
    assert!(
        win.join_lookup("value", &JoinKey::Int(43), None)
            .is_none_or(|v| v.is_empty()),
        "key 43 (first batch) removed after evict_oldest"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(44), None)
            .map(|v| v.len()),
        Some(1),
        "key 44 (second batch) still indexed"
    );
}

#[test]
fn join_index_duplicate_key_keeps_all_rows() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // Two rows with the same key 42 in different batches.
    win.append(make_batch(&test_schema(), &[1_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[2_000_000], &[42]))
        .unwrap();
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(2),
        "both rows with key 42 kept"
    );
    // Evict one batch → one row remains.
    win.evict_oldest();
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(1),
        "one row removed on evict, one kept"
    );
}

#[test]
fn join_index_stays_columnar_without_materializing_parsed_events() {
    // The columnar join index (set_join_key + append + lookup) must never
    // trigger `TimedBatch::events()`, so a join-target window with no rule
    // subscription keeps its batches fully columnar — the Q22 `person_events`
    // RSS win. `join_lookup` works off the `(batch, row)` locators directly.
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();

    // Columnar lookup still works.
    let rows = win
        .join_lookup("value", &JoinKey::Int(42), None)
        .expect("indexed window should return rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].field_value("value"), Some(Value::Number(42.0)));

    // And the batch's `parsed_events` stayed uninitialized.
    assert!(
        !win.any_parsed_events_materialized(),
        "join index must not materialize parsed events"
    );
}

#[test]
fn join_lookup_asof_max_fast_path() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Same key 42 at ts=1s and ts=3s → per-key max_ts = 3s.
    win.append(make_batch(&test_schema(), &[1_000_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000_000], &[42]))
        .unwrap();

    // Fast-path hit: max_ts=3s falls within [2s, 5s] → returns the latest row.
    match win.join_lookup_asof(
        "value",
        &JoinKey::Int(42),
        5_000_000_000,
        2_000_000_000,
        None,
    ) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(3_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // max_ts too old (3s < min_ts=4s) → Miss (no scan needed).
    assert!(matches!(
        win.join_lookup_asof(
            "value",
            &JoinKey::Int(42),
            5_000_000_000,
            4_000_000_000,
            None
        ),
        AsofLookup::Miss
    ));
    // Miss must be consistent with the fallback scan: every candidate ts is
    // below min_ts, so `find_asof_row` would also return `None`.
    let cands = win
        .join_lookup_timestamped("value", &JoinKey::Int(42), None)
        .unwrap();
    assert!(
        cands.iter().all(|(ts, _)| *ts < 4_000_000_000),
        "Miss implies all candidate timestamps are below the asof lower bound"
    );

    // max_ts too new (3s > event_time=2s): a smaller row (ts=1s) qualifies, so
    // the index scans and returns it directly — no caller-side fallback scan.
    match win.join_lookup_asof("value", &JoinKey::Int(42), 2_000_000_000, 0, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(1_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit for max_ts > event_time, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit for max_ts > event_time, got Fallback"),
    }

    // Unknown key → Miss.
    assert!(matches!(
        win.join_lookup_asof("value", &JoinKey::Int(99), 5_000_000_000, 0, None),
        AsofLookup::Miss
    ));

    // Boundary: max_ts == min_ts (3s == 3s) → still a hit (inclusive lower bound).
    match win.join_lookup_asof(
        "value",
        &JoinKey::Int(42),
        5_000_000_000,
        3_000_000_000,
        None,
    ) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(3_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit at inclusive lower bound, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit at inclusive lower bound, got Fallback"),
    }

    // Boundary: max_ts == event_time (3s == 3s) → still a hit (inclusive upper bound).
    match win.join_lookup_asof(
        "value",
        &JoinKey::Int(42),
        3_000_000_000,
        2_000_000_000,
        None,
    ) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(3_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit at inclusive upper bound, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit at inclusive upper bound, got Fallback"),
    }
}

#[test]
fn join_lookup_asof_max_scans_when_max_is_future() {
    // When a key's running max_ts is newer than the event time (person X has a
    // future event in the same/next bucket), the index must still return the
    // greatest timestamp <= event_time — without falling back to the caller's
    // full candidate scan. Rows are appended out of time order within/across
    // batches, so the scan must not assume sorted order.
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Key 42 at ts = 5s, 1s, 9s, 3s (append order != ts order; max_ts = 9s).
    for ts in [
        5_000_000_000i64,
        1_000_000_000,
        9_000_000_000,
        3_000_000_000,
    ] {
        win.append(make_batch(&test_schema(), &[ts], &[42]))
            .unwrap();
    }

    // event_time=7s, min_ts=0: max_ts(9s) > 7s → scan picks 5s (greatest ≤ 7s).
    match win.join_lookup_asof("value", &JoinKey::Int(42), 7_000_000_000, 0, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(5_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // Tight window [4s, 6s]: 5s qualifies, 3s/1s below, 9s above → 5s.
    match win.join_lookup_asof(
        "value",
        &JoinKey::Int(42),
        6_000_000_000,
        4_000_000_000,
        None,
    ) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(5_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // No candidate in [8s, 9s] below event_time=9s: max_ts==9s (== event_time)
    // is the fast-path hit, not the scan path.
    match win.join_lookup_asof(
        "value",
        &JoinKey::Int(42),
        9_000_000_000,
        8_000_000_000,
        None,
    ) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(9_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // No candidate in [7.5s, 8.5s] (max_ts=9s > event_time=8.5s, all rows ≤7.5s
    // or =9s are outside [7.5s,8.5s]) → Miss.
    assert!(matches!(
        win.join_lookup_asof(
            "value",
            &JoinKey::Int(42),
            8_500_000_000,
            7_500_000_000,
            None
        ),
        AsofLookup::Miss
    ));
}

#[test]
fn join_lookup_asof_max_miss_without_timestamps() {
    // A join-indexed window with no time column has no per-row timestamps, so
    // the asof fast path must report `Miss` (the timestamped scan would also
    // return no candidates, so `find_asof_row` would be `None`).
    let schema = test_schema_no_time();
    let win = Window::new(
        WindowParams {
            name: "no_time".into(),
            schema: schema.clone(),
            time_col_index: None,
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    win.set_join_key("value".into());
    win.append(make_batch_no_time(&schema, &[42])).unwrap();

    assert!(matches!(
        win.join_lookup_asof("value", &JoinKey::Int(42), 5_000_000_000, 0, None),
        AsofLookup::Miss
    ));
}

// -- join index 分片（2026-08-25，q4 100M 断崖修复） ------------------------
//
// `JoinIndex` 从单锁整表改为 64 片独立 RwLock：写者（index_batch）逐片短暂
// 持写锁、查找只锁 key 所在片。deferred_bench `index_contention` 实测单锁在
// 写者活跃时读者吞吐塌到 2%（0.15M vs 7.98M ops/s 天花板），分片恢复 43–46×。
// 以下测试保护分片的三个不变量：选片确定性/摊开、跨片查找/驱逐正确、asof
// 快路径跨片可用。

#[test]
fn join_index_shards_spread_and_deterministic() {
    use crate::window::buffer::JoinIndex;

    // 片数必须是 2 的幂（选片 = hash & mask）。
    let mask = JOIN_INDEX_SHARDS - 1;
    assert_eq!(
        JOIN_INDEX_SHARDS & mask,
        0,
        "shard count must be a power of two"
    );

    // 确定性：同一 key 恒落同一片，且落在界内。
    let a = JoinIndex::shard_of(&JoinKey::Int(42), mask);
    let b = JoinIndex::shard_of(&JoinKey::Int(42), mask);
    assert_eq!(a, b, "same key must map to the same shard");
    assert!(a < JOIN_INDEX_SHARDS);

    // 实际摊开：256 个连续 int key 必须覆盖至少一半分片（防选片塌缩——
    // 塌缩会让分片退化成单锁，重演 q4 100M 锁竞争）。
    let mut seen = std::collections::HashSet::new();
    for k in 0..256i64 {
        seen.insert(JoinIndex::shard_of(&JoinKey::Int(k), mask));
    }
    assert!(
        seen.len() >= 32,
        "256 consecutive int keys must spread across >=32 shards, got {}",
        seen.len()
    );

    // Str/Bool 键也落在界内。
    assert!(JoinIndex::shard_of(&JoinKey::Str("abc".into()), mask) < JOIN_INDEX_SHARDS);
    assert!(JoinIndex::shard_of(&JoinKey::Bool(true), mask) < JOIN_INDEX_SHARDS);
}

/// 跨片不变量：append 到全部分片上的行都能查到；驱逐后全部消失；asof 快
/// 路径跨片可用（每片维护自己的 max_ts，必须与整表语义一致）。
#[test]
fn join_index_sharded_lookup_evict_and_asof_span_all_shards() {
    use crate::window::buffer::JoinIndex;

    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // 512 个递增 key：按 shard_of 挑出覆盖所有片的键集（防测试数据碰巧
    // 只落少数片而漏测真实分布）。
    let mask = JOIN_INDEX_SHARDS - 1;
    let mut per_shard: Vec<Vec<i64>> = vec![Vec::new(); JOIN_INDEX_SHARDS];
    let mut k = 0i64;
    for (s, shard) in per_shard.iter_mut().enumerate() {
        loop {
            if JoinIndex::shard_of(&JoinKey::Int(k), mask) == s {
                shard.push(k);
                k += 1;
                break;
            }
            k += 1;
        }
    }
    let keys: Vec<i64> = per_shard.into_iter().flatten().collect();
    assert!(keys.len() >= 64, "one representative key per shard");

    // 每个 key 两行（不同 ts）→ lookup 行数 = 2。
    for (i, key) in keys.iter().enumerate() {
        let ts = 1_000_000_000i64 + (i as i64) * 100;
        win.append(make_batch(&test_schema(), &[ts], &[*key]))
            .unwrap();
        win.append(make_batch(&test_schema(), &[ts + 50], &[*key]))
            .unwrap();
    }
    for key in &keys {
        assert_eq!(
            win.join_lookup("value", &JoinKey::Int(*key), None)
                .map(|v| v.len()),
            Some(2),
            "key {key} must be found with both rows (regardless of shard)"
        );
        // asof：两行都在 [ts, ts+50]，event_time 取后行 → 命中后行。
        match win.join_lookup_asof("value", &JoinKey::Int(*key), i64::MAX, i64::MIN, None) {
            AsofLookup::Hit(row) => {
                assert_eq!(row.field_value("value"), Some(Value::Number(*key as f64)));
            }
            AsofLookup::Miss => panic!("expected asof Hit for key {key}, got Miss"),
            AsofLookup::Fallback => panic!("expected asof Hit for key {key}, got Fallback"),
        }
    }

    // 驱逐所有 batch（时间驱逐，over=3600s）→ 全部分片清空。
    win.evict_expired(4_000_000_000_000);
    for key in &keys {
        assert!(
            win.join_lookup("value", &JoinKey::Int(*key), None)
                .is_none_or(|v| v.is_empty()),
            "key {key} must be removed from its shard after eviction"
        );
    }
}

// -- join index 增量驱逐（batch_keys registry，2026-08-25 q4 100M 主因修复）--
//
// 旧 `remove_batch` 每驱逐一批就全索引扫描（retain + max_ts 重算，O(全行数)——
// 100M 时 33M 行 × 每批，evictor 线程独占一核）。新实现按 `batch_keys[seq]`
// 只清该批贡献过的 key。以下测试保护增量语义：跨批 key 只删本批行、未触及
// key 完全不受影响、max_ts 在 max 行被删后正确回落。

#[test]
fn join_index_incremental_remove_only_touches_the_evicted_batch() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // 4 个单行批：seq0=42, seq1=43, seq2=42（跨批 key）, seq3=44
    for (ts, v) in [1_000_000i64, 2_000_000, 3_000_000, 4_000_000]
        .into_iter()
        .zip([42i64, 43, 42, 44])
    {
        win.append(make_batch(&test_schema(), &[ts], &[v])).unwrap();
    }
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(2),
        "both batches' rows visible before eviction"
    );

    // 弹掉 seq0（key 42 的第一行）→ 42 只剩 seq2 的一行；43/44 不受影响。
    win.evict_oldest();
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(1),
        "cross-batch key keeps only the surviving batch's row"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(43), None)
            .map(|v| v.len()),
        Some(1),
        "untouched batch's key must be unaffected"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(44), None)
            .map(|v| v.len()),
        Some(1),
        "untouched key must be unaffected by another batch's eviction"
    );

    // 弹掉 seq1（key 43）→ 43 清空；42/44 仍在。
    win.evict_oldest();
    assert!(
        win.join_lookup("value", &JoinKey::Int(43), None)
            .is_none_or(|v| v.is_empty()),
        "evicted batch's sole key must be cleared"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(1)
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(44), None)
            .map(|v| v.len()),
        Some(1)
    );

    // 弹掉 seq2（第二个 42）→ 42 清空；44 仍在。
    win.evict_oldest();
    assert!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "last 42 row removed"
    );
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(44), None)
            .map(|v| v.len()),
        Some(1)
    );
}

#[test]
fn join_index_incremental_remove_recomputes_max_ts() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // 同 key 42：seq0 在 ts=5s，seq1 在 ts=1s（max_ts 缓存 = 5s）。
    win.append(make_batch(&test_schema(), &[5_000_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[1_000_000_000], &[42]))
        .unwrap();
    match win.join_lookup_asof("value", &JoinKey::Int(42), 9_000_000_000, 0, None) {
        AsofLookup::Hit(row) => assert_eq!(
            row.field_value("ts"),
            Some(Value::Number(5_000_000_000.0)),
            "max_ts = 5s before eviction"
        ),
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // 驱逐 ts=5s 的批 → max_ts 必须回落到剩余行的 1s（asof 不再命中旧 max）。
    win.evict_oldest();
    match win.join_lookup_asof("value", &JoinKey::Int(42), 9_000_000_000, 0, None) {
        AsofLookup::Hit(row) => assert_eq!(
            row.field_value("ts"),
            Some(Value::Number(1_000_000_000.0)),
            "max_ts must drop to the surviving row after the max row is evicted"
        ),
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }
    assert_eq!(
        win.join_lookup("value", &JoinKey::Int(42), None)
            .map(|v| v.len()),
        Some(1)
    );
}

/// 增量驱逐的 **registry 缺失回退**（防御路径）：未注册 seq 的 `remove_batch`
/// 走全量扫描（no-op），已注册 seq 正常增量删除；registry 条目随驱逐清理。
/// 直接构造 `JoinIndex`（buffer 私有字段，测试子模块可见）以注入缺失态——
/// 生产路径（append 必注册）难触发，此为防回归兜底。
#[test]
fn join_index_remove_batch_fallback_when_registry_missing() {
    use crate::window::buffer::JoinIndex;

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int64, false),
    ]));
    let index = JoinIndex {
        key_field: "value".into(),
        projection: None,
        shards: (0..JOIN_INDEX_SHARDS)
            .map(|_| parking_lot::RwLock::new(crate::match_engine::EngineHashMap::default()))
            .collect(),
        mask: JOIN_INDEX_SHARDS - 1,
        batch_keys: parking_lot::RwLock::new(crate::match_engine::EngineHashMap::default()),
    };
    let batch = Arc::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64])),
                Arc::new(Int64Array::from(vec![42i64])),
            ],
        )
        .unwrap(),
    );
    let ts_list = vec![Some(1_000_000_000i64)];
    index.index_batch(&batch, &ts_list, 7);

    // 已注册：lookup 命中。
    assert_eq!(
        index.lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "indexed row visible"
    );

    // 未注册 seq：回退全量扫描，无匹配行 → 行保留（no-op，不 panic）。
    index.remove_batch(99);
    assert_eq!(
        index.lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "registry-miss removal must not touch unrelated rows"
    );

    // 已注册 seq：增量删除 + registry 条目清理。
    index.remove_batch(7);
    assert!(
        index
            .lookup(&JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "registered seq removal must clear the rows"
    );
    assert!(
        index.batch_keys.read().is_empty(),
        "batch_keys entry must be dropped on removal"
    );
}

/// 并发 append + 驱逐 + lookup 无死锁/无 panic（分片锁 + batch_keys 锁的锁序
/// 回归：index_batch 先片锁后 registry 锁、remove_batch 先 registry 后片锁，
/// 均不跨锁等待另一把——此测试钉死交错路径）。
#[test]
fn join_index_concurrent_append_evict_lookup_no_deadlock() {
    use std::sync::atomic::AtomicBool;
    use std::thread;

    let win = Arc::new(test_window(3600, usize::MAX));
    win.set_join_key("value".into());
    let stop = Arc::new(AtomicBool::new(false));

    // 写者：持续 append（key 循环，跨分片分布）。
    let w = Arc::clone(&win);
    let sw = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut k = 0i64;
        while !sw.load(Ordering::Relaxed) {
            let times: Vec<i64> = (0..32)
                .map(|i| 1_000_000_000i64 + i * 1000 + k * 32)
                .collect();
            let vals: Vec<i64> = (0..32).map(|i| (k + i) % 16).collect();
            w.append(make_batch(w.schema(), &times, &vals)).unwrap();
            k += 1;
        }
    });

    // 驱逐者：持续弹 front（时间驱逐同路径：remove_batch_from_index）。
    let e = Arc::clone(&win);
    let se = Arc::clone(&stop);
    let evictor = thread::spawn(move || {
        while !se.load(Ordering::Relaxed) {
            e.evict_oldest();
        }
    });

    // 读者（主线程）：持续随机 key 查找（join_lookup + asof 两路径）。
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut hit = 0usize;
    for _ in 0..20_000 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = JoinKey::Int(((state >> 33) % 16) as i64);
        if win.join_lookup("value", &key, None).is_some() {
            hit += 1;
        }
        let _ = win.join_lookup_asof("value", &key, i64::MAX, i64::MIN, None);
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer join");
    evictor.join().expect("evictor join");
    // 只断言无死锁/无 panic（hit 数随交错变化，无固定期望）。
    std::hint::black_box(hit);
}

// -- join 索引 append 微基准（2026-08-24 q9/q4 性能归因） ---------------------
//
// q9/q4 的 join 目标（bid_events，30M 时 27.6M 行）每次 append 都要 index_batch
// （逐行按 key 建列式 JoinIndex）；q8 的 join 目标只有 auction_events 1.8M 行，
// 差 15×，正好对应 q9（~0.9M EPS）比 q8（23M EPS）慢 ~25×。本基准量出 append
// 有无 join 索引的 ns/row 差，把「索引维护」从「纯 append」里分离出来。
//
// 运行：
//   cargo test --release -p wf-engine join_index_append_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine join_index_append_bench -- --ignored --nocapture"]
fn join_index_append_bench() {
    use std::time::Instant;

    const N: usize = 1_000_000;
    const BATCH: usize = 2000; // 对齐 bench 帧大小（每批 ~2000 行）

    // 数据：ts 递增（避免驱逐触发，隔离纯 append+index），value 0..9999（10k 去重键，
    // 近似 q9 bid.auction 的去重度）。
    fn make(win: &Window, batch_idx: usize) -> RecordBatch {
        let schema = win.schema().clone();
        let base = batch_idx * BATCH;
        let times: Vec<i64> = (0..BATCH).map(|i| (base + i) as i64 * 100_000).collect();
        let values: Vec<i64> = (0..BATCH).map(|i| (base + i) as i64 % 10_000).collect();
        make_batch(&schema, &times, &values)
    }

    // baseline：无 join 索引（纯 append）
    let win = test_window(3600, usize::MAX);
    let start = Instant::now();
    for b in 0..(N / BATCH) {
        win.append(make(&win, b)).unwrap();
    }
    let baseline_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // join 目标：set_join_key 后 append（append + index_batch）
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    let start = Instant::now();
    for b in 0..(N / BATCH) {
        win.append(make(&win, b)).unwrap();
    }
    let indexed_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // set_join_key 初始建索引（空窗 → 无行，仅建空结构；真实引擎 spawn 时调用，
    // 不计入每行成本，但单独量一下以防 rebuild 开销被误读）。
    let win = test_window(3600, usize::MAX);
    let start = Instant::now();
    win.set_join_key("value".into());
    let set_key_ns = start.elapsed().as_nanos() as f64;

    eprintln!("[join-index-append-bench] N={N}, batch={BATCH}, keys=10000");
    eprintln!(
        "[join-index-append-bench] {:<28} {:>9.1} ns/row  ({:>6.2}M rows/s)",
        "append (no index)",
        baseline_ns,
        1e9 / baseline_ns / 1e6
    );
    eprintln!(
        "[join-index-append-bench] {:<28} {:>9.1} ns/row  ({:>6.2}M rows/s)",
        "append + join index",
        indexed_ns,
        1e9 / indexed_ns / 1e6
    );
    eprintln!(
        "[join-index-append-bench] {:<28} {:>9.1} ns/row  ({:>5.1}% overhead)",
        "index_batch (diff)",
        indexed_ns - baseline_ns,
        (indexed_ns - baseline_ns) / baseline_ns * 100.0
    );
    eprintln!(
        "[join-index-append-bench] set_join_key (empty) = {:.1} ns",
        set_key_ns
    );
}
