//! 改进逻辑单元级性能基准（数据驱动改进的依据）。
//!
//! 测量对象（本会话的免物化改进）：
//! - `ColumnarEvent::field_value`（状态机列式逐读）vs `Event` HashMap 读
//! - `ColumnarEvent::to_event`（emit 路径 trigger_event 重建）vs `Event::clone`
//! - `JoinRow::field_value`（join 列式行）vs `HashMap<String, Value>::get`
//! - `column_scalar_string`（window.has 单列建 set）vs `batch_to_events` + to_string
//! - `window.has` 缓存命中（Arc<HashSet> clone）vs 冷扫描（单列建 set）
//!
//! 运行：cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture
use std::sync::Arc;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::match_engine::cep::{EngineHashMap, MatchedContext, StepData, WindowLookup};
use crate::match_engine::{
    AsofLookup, ColumnarEvent, FieldSource, JoinKey, JoinRow, RuleExecutor, Value,
    batch_raw_ts_nanos, batch_to_events, build_field_index, column_scalar_string,
    columnar_join_rows, columnar_timestamped_join_rows,
};
use crate::window::{Window, WindowParams};

use super::helpers::{branch, count_ge, simple_key, simple_plan, simple_rule_plan, step, str_val};
use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{JoinCondPlan, JoinPlan};

/// nexmark bid_events 形态的 7 字段批（auction/bidder/price/channel/url/dateTime/extra），
/// 少量 null 对齐真实数据。返回 (batch, price 列索引)。
fn bid_batch(n: usize) -> (RecordBatch, usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, true),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let auction: Vec<i64> = (0..n as i64).collect();
    let bidder: Vec<i64> = (0..n as i64).map(|i| i % 1000).collect();
    let price: Vec<Option<i64>> = (0..n as i64)
        .map(|i| (i % 7 != 0).then_some(i % 200))
        .collect();
    let channel: Vec<String> = (0..n).map(|i| format!("ch{}", i % 8)).collect();
    let url: Vec<String> = (0..n)
        .map(|i| format!("http://example.com/{}", i % 1000))
        .collect();
    let date_time: Vec<i64> = (0..n as i64)
        .map(|i| 1_700_000_000_000_000_000 + i)
        .collect();
    let extra: Vec<Option<String>> = (0..n)
        .map(|i| (i % 3 != 0).then(|| "x".to_string()))
        .collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)) as ArrayRef,
            Arc::new(Int64Array::from(price)) as ArrayRef,
            Arc::new(StringArray::from(channel)) as ArrayRef,
            Arc::new(StringArray::from(url)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(date_time)) as ArrayRef,
            Arc::new(StringArray::from(extra)) as ArrayRef,
        ],
    )
    .unwrap();
    let price_idx = 2;
    (batch, price_idx)
}

/// 每行 `field_value(name)`（状态机热读：key/分支字段/守卫回退）。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn columnar_field_value_vs_eager() {
    let n = 1_000_000usize;
    let (batch, _) = bid_batch(n);
    let index = build_field_index(&batch);
    let events = batch_to_events(&batch);

    let mut acc = 0.0f64;
    let start = Instant::now();
    for ev in &events {
        if let Some(Value::Number(v)) = ev.field_value("price") {
            acc += v;
        }
    }
    let eager = start.elapsed();
    let eager_per = eager.as_secs_f64() * 1e9 / n as f64;

    let mut acc_c = 0.0f64;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        if let Some(Value::Number(v)) = ce.field_value("price") {
            acc_c += v;
        }
    }
    let col = start.elapsed();
    let col_per = col.as_secs_f64() * 1e9 / n as f64;

    eprintln!(
        "[columnar-bench] field_value('price'): eager {eager_per:6.1} ns/op  columnar {col_per:6.1} ns/op  ({:.2}x)",
        eager_per / col_per
    );
    assert_eq!(acc, acc_c, "eager vs columnar field_value must agree");
}

/// 每行 `to_event()`（emit 路径 trigger_event 重建）vs `Event::clone()`。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn to_event_vs_event_clone() {
    let n = 1_000_000usize;
    let (batch, _) = bid_batch(n);
    let index = build_field_index(&batch);
    let events = batch_to_events(&batch);

    let mut total = 0usize;
    let start = Instant::now();
    for ev in &events {
        let cloned = ev.clone(); // Event::to_event == self.clone()
        total += cloned.fields.len();
    }
    let eager = start.elapsed();
    let eager_per = eager.as_secs_f64() * 1e9 / n as f64;

    let mut total_c = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        let rebuilt = ce.to_event();
        total_c += rebuilt.fields.len();
    }
    let col = start.elapsed();
    let col_per = col.as_secs_f64() * 1e9 / n as f64;

    eprintln!(
        "[columnar-bench] to_event: clone {eager_per:6.1} ns/op  columnar-rebuild {col_per:6.1} ns/op  ({:.2}x)",
        eager_per / col_per
    );
    assert_eq!(total, total_c, "cloned vs rebuilt field counts must agree");
}

/// `JoinRow::field_value`（join 列式行）vs `HashMap<String, Value>::get`。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn join_row_field_value_vs_map_get() {
    let n = 1_000_000usize;
    let (batch, _) = bid_batch(n);
    let rows = columnar_join_rows(vec![batch.clone()], None);
    let map_rows: Vec<std::collections::HashMap<String, Value>> = batch_to_events(&batch)
        .into_iter()
        .map(|ev| {
            ev.fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
        })
        .collect();

    let mut acc = 0.0f64;
    let start = Instant::now();
    for row in &map_rows {
        if let Some(Value::Number(v)) = row.get("price") {
            acc += v;
        }
    }
    let map_per = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    let mut acc_c = 0.0f64;
    let start = Instant::now();
    for row in &rows {
        if let Some(Value::Number(v)) = row.field_value("price") {
            acc_c += v;
        }
    }
    let col_per = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    eprintln!(
        "[columnar-bench] join field_value('price'): map.get {map_per:6.1} ns/op  columnar {col_per:6.1} ns/op  ({:.2}x)",
        map_per / col_per
    );
    assert_eq!(acc, acc_c, "map vs columnar join row must agree");
}

/// `column_scalar_string`（window.has 单列建 set）vs `batch_to_events` + to_string。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn column_scalar_string_vs_batch_to_events() {
    let n = 1_000_000usize;
    let (batch, price_idx) = bid_batch(n);
    let events = batch_to_events(&batch);

    let mut set = std::collections::HashSet::new();
    let start = Instant::now();
    for ev in &events {
        if let Some(val) = ev.fields.get("price") {
            set.insert(match val {
                Value::Str(s) => s.to_string(),
                Value::Number(v) => v.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Array(_) | Value::Object(_) => continue,
            });
        }
    }
    let eager = start.elapsed();
    let eager_per = eager.as_secs_f64() * 1e9 / n as f64;

    let mut set_c = std::collections::HashSet::new();
    let start = Instant::now();
    for row in 0..n {
        if let Some(s) = column_scalar_string(&batch, price_idx, row) {
            set_c.insert(s);
        }
    }
    let col = start.elapsed();
    let col_per = col.as_secs_f64() * 1e9 / n as f64;

    eprintln!(
        "[columnar-bench] has() set-build: batch_to_events {eager_per:6.1} ns/row  single-column {col_per:6.1} ns/row  ({:.2}x)  distinct {} vs {}",
        eager_per / col_per,
        set.len(),
        set_c.len()
    );
    assert_eq!(set, set_c, "set contents must agree");
}

/// `window.has` 缓存命中（Arc<HashSet> clone）vs 冷扫描（单列建 set）。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn has_cache_hit_vs_cold_scan() {
    let n = 1_000_000usize;
    let (batch, price_idx) = bid_batch(n);
    // 冷扫描 = 单列读建 set（一次 has 求值，窗口内容变化时）。
    let start = Instant::now();
    for _ in 0..1_000usize {
        let mut set = std::collections::HashSet::new();
        for row in 0..n {
            if let Some(s) = column_scalar_string(&batch, price_idx, row) {
                set.insert(s);
            }
        }
        std::hint::black_box(set.len());
    }
    let cold = start.elapsed();
    let cold_per = cold.as_secs_f64() * 1e9 / 1000.0 / 1e6; // ms per has() eval

    // 缓存命中 = Arc<HashSet> clone（每次 has 求值的固定开销）。
    let set = Arc::new(std::collections::HashSet::from(["10.0.0.1".to_string()]));
    let start = Instant::now();
    for _ in 0..1_000_000usize {
        std::hint::black_box(Arc::clone(&set));
    }
    let hit = start.elapsed();
    let hit_ns = hit.as_secs_f64() * 1e9 / 1_000_000.0;

    eprintln!(
        "[columnar-bench] has() eval: cold-scan {cold_per:8.2} ms/eval (1M rows)  cache-hit {hit_ns:5.1} ns/eval  ({:.0}x)",
        cold_per * 1e6 / hit_ns
    );
}

// ---------------------------------------------------------------------------
// Join index（列式行定位符）性能基准
//
// 测量对象（asof/snapshot join 走 hash index 的免物化改进）：
// - `Window::join_lookup`（O(1) 列式 hash 查找）vs `columnar_join_rows` 全量扫描
// - `Window::join_lookup_timestamped`（asof O(1)）vs 全量 timestamped 扫描
// 两对都验证“Q22 asof join 从 O(window rows) 降到 O(1)”的量级。
//
// 运行：cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture
// ---------------------------------------------------------------------------

/// 构造一个 `ts` + `key` + `payload` 三列的 join-target 窗口，索引 `n` 行
/// （`key` 列 0..n 唯一），返回 `(窗口, ts 列索引)`。
fn join_bench_window(n: usize) -> (Window, usize) {
    use std::time::Duration;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let win = Window::new(
        WindowParams {
            name: "join_bench".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "join_bench".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    win.set_join_key("key".into());

    let ts: Vec<i64> = (0..n as i64)
        .map(|i| 1_700_000_000_000_000_000 + i)
        .collect();
    let key: Vec<i64> = (0..n as i64).collect();
    let payload: Vec<String> = (0..n).map(|i| format!("p{}", i % 100)).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(ts)) as ArrayRef,
            Arc::new(Int64Array::from(key)) as ArrayRef,
            Arc::new(StringArray::from(payload)) as ArrayRef,
        ],
    )
    .unwrap();
    win.append(batch).unwrap();
    (win, 0)
}

/// snapshot join：`join_lookup`（O(1) hash 查找）vs `columnar_join_rows` 全量扫描。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn join_index_lookup_vs_full_scan() {
    let n = 1_000_000usize;
    let (win, _) = join_bench_window(n);

    // Indexed lookup：R 次点查，统计 ns/lookup。
    let r = 1_000_000usize;
    let start = Instant::now();
    let mut idx_hits = 0usize;
    for i in 0..r {
        let key = (i as i64) % (n as i64);
        if let Some(rows) = win.join_lookup("key", &JoinKey::Int(key), None) {
            idx_hits += rows.len();
        }
    }
    let idx_ns = start.elapsed().as_secs_f64() * 1e9 / r as f64;

    // Full scan：S 次全量扫描（每次 snapshot + 建 100 万行 + 过滤一个 key），
    // 统计 ns/scan。
    let s = 10usize;
    let start = Instant::now();
    let mut scan_hits = 0usize;
    for i in 0..s {
        let key = (i as i64) % (n as i64);
        let rows = columnar_join_rows(win.snapshot(), None);
        scan_hits += rows
            .iter()
            .filter(|row| row.field_value("key") == Some(Value::Number(key as f64)))
            .count();
    }
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / s as f64;

    eprintln!(
        "[join-index-bench] snapshot lookup: index {idx_ns:8.1} ns/lookup  full-scan {scan_ns:8.1} ns/scan  ({:.0}x)",
        scan_ns / idx_ns
    );
    assert_eq!(idx_hits, r, "each indexed lookup must return one row");
    assert_eq!(scan_hits, s, "each full scan must find one row");
}

/// asof join：`join_lookup_timestamped`（O(1)）vs 全量 timestamped 扫描。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn join_index_timestamped_lookup_vs_full_scan() {
    let n = 1_000_000usize;
    let (win, ts_col) = join_bench_window(n);

    // Indexed asof lookup：R 次点查，统计 ns/lookup。
    let r = 1_000_000usize;
    let start = Instant::now();
    let mut idx_hits = 0usize;
    for i in 0..r {
        let key = (i as i64) % (n as i64);
        if let Some(rows) = win.join_lookup_timestamped("key", &JoinKey::Int(key), None) {
            idx_hits += rows.len();
        }
    }
    let idx_ns = start.elapsed().as_secs_f64() * 1e9 / r as f64;

    // Full timestamped scan：S 次全量扫描。
    let s = 10usize;
    let start = Instant::now();
    let mut scan_hits = 0usize;
    for i in 0..s {
        let key = (i as i64) % (n as i64);
        let rows = columnar_timestamped_join_rows(win.snapshot(), ts_col, None);
        scan_hits += rows
            .iter()
            .filter(|(_, row)| row.field_value("key") == Some(Value::Number(key as f64)))
            .count();
    }
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / s as f64;

    eprintln!(
        "[join-index-bench] asof lookup:    index {idx_ns:8.1} ns/lookup  full-scan {scan_ns:8.1} ns/scan  ({:.0}x)",
        scan_ns / idx_ns
    );
    assert_eq!(idx_hits, r, "each indexed asof lookup must return one row");
    assert_eq!(scan_hits, s, "each full asof scan must find one row");
}

/// asof `lookup_asof_max`：快路径（`max_ts <= event_time`，O(1)）vs 扫描路径
/// （`max_ts > event_time`，需要线性扫 key 的 N 行挑最大 ≤ event_time 的行）。
/// 模拟 Q22 里「每个 bid 回看该 bidder 的 ~N 个 person 版本」的两种热路径成本。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn join_index_asof_max_scan_vs_fast_path() {
    use std::time::Duration;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

    let n = 200usize; // 每个 key 的 person 版本数（Q22 单 bidder ≈ 200）
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let win = Window::new(
        WindowParams {
            name: "asof_bench".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "asof_bench".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    win.set_join_key("key".into());

    // 单 key=42，N 行，ts 均匀 1s..N s（append 顺序 == ts 顺序，max_ts = N s）。
    let ts: Vec<i64> = (0..n as i64)
        .map(|i| 1_700_000_000_000_000_000 + (i + 1) * 1_000_000_000)
        .collect();
    let keys: Vec<i64> = vec![42; n];
    let payload: Vec<String> = (0..n).map(|i| format!("p{i}")).collect();
    win.append(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(ts)) as ArrayRef,
                Arc::new(Int64Array::from(keys)) as ArrayRef,
                Arc::new(StringArray::from(payload)) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .unwrap();

    let key = JoinKey::Int(42);
    let max_ts = 1_700_000_000_000_000_000 + n as i64 * 1_000_000_000;
    let r = 1_000_000usize;

    // 快路径：event_time == max_ts（max_ts <= event_time）→ 反向找到 max_ts 即返回。
    let start = Instant::now();
    let mut fast_hits = 0usize;
    for _ in 0..r {
        if matches!(
            win.join_lookup_asof("key", &key, max_ts, 0, None),
            AsofLookup::Hit(_)
        ) {
            fast_hits += 1;
        }
    }
    let fast_ns = start.elapsed().as_secs_f64() * 1e9 / r as f64;

    // 扫描路径：event_time 在中间（max_ts > event_time）→ 线性扫 N 行。
    let mid_ts = 1_700_000_000_000_000_000 + (n as i64 / 2) * 1_000_000_000;
    let start = Instant::now();
    let mut scan_hits = 0usize;
    for _ in 0..r {
        if matches!(
            win.join_lookup_asof("key", &key, mid_ts, 0, None),
            AsofLookup::Hit(_)
        ) {
            scan_hits += 1;
        }
    }
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / r as f64;

    eprintln!(
        "[join-index-bench] asof max:      fast {fast_ns:8.1} ns/lookup  scan(N={n}) {scan_ns:8.1} ns/lookup  ({:.1}x)",
        scan_ns / fast_ns
    );
    assert_eq!(fast_hits, r);
    assert_eq!(scan_hits, r);
}

// ---------------------------------------------------------------------------
// `execute_match_with_joins` 端到端性能 profile
//
// 测量 `RuleExecutor::execute_match_with_joins`（build_eval_context +
// execute_joins[asof] + build_match_alert）在「asof 候选版本数 N」下的单次
// 耗时，量化 Q22 里「每个 bid 遍历该 bidder 的 N 个 person 版本」的成本。
//
// 运行：cargo test --release -p wf-engine execute_match_with_joins -- --ignored --nocapture
// ---------------------------------------------------------------------------

/// 一个只服务 asof join 的 [`WindowLookup`]：`asof_candidates` 原样返回
/// 预置的 `N` 个 `(raw_ts, JoinRow)` 候选；`asof_lookup_max` 若 `fast_path`
/// 则模拟 index 的 per-key `max_ts` 快路径（O(1) 直接返回 max 候选），否则
/// 返回 `None`（退化为全量 `asof_candidates` 线性扫）。
struct BenchAsofLookup {
    candidates: Vec<(i64, JoinRow)>,
    max_row: Option<(i64, JoinRow)>,
    fast_path: bool,
}

impl BenchAsofLookup {
    fn new(candidates: Vec<(i64, JoinRow)>, fast_path: bool) -> Self {
        let max_row = candidates.iter().max_by_key(|(ts, _)| *ts).cloned();
        Self {
            candidates,
            max_row,
            fast_path,
        }
    }
}

impl WindowLookup for BenchAsofLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }

    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }

    fn snapshot_with_timestamps(&self, _w: &str) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.candidates.clone())
    }

    fn asof_candidates(
        &self,
        _w: &str,
        _key_field: &str,
        _key: &Value,
    ) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.candidates.clone())
    }

    fn asof_lookup_max(
        &self,
        _w: &str,
        _key_field: &str,
        _key: &Value,
        event_time: i64,
        within: Option<&Duration>,
    ) -> AsofLookup {
        if !self.fast_path {
            return AsofLookup::Fallback;
        }
        let Some((max_ts, _)) = self.max_row.as_ref() else {
            return AsofLookup::Miss;
        };
        let min_ts = within.map_or(i64::MIN, |d| {
            let nanos = i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
            event_time.saturating_sub(nanos)
        });
        if *max_ts < min_ts {
            return AsofLookup::Miss;
        }
        if *max_ts > event_time {
            return AsofLookup::Fallback;
        }
        match self.max_row.as_ref() {
            Some((_, row)) => AsofLookup::Hit(row.clone()),
            None => AsofLookup::Fallback,
        }
    }
}

/// 构造 `n` 个列式 person 候选（`JoinRow::Columnar`，同一 batch 的 `n` 行），
/// 时间戳均匀分布在 `[bid_ts - within, bid_ts]`，全部落在 asof 的时间窗内。
fn person_batch_candidates(n: usize, bid_ts: i64, within: Duration) -> Vec<(i64, JoinRow)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]));
    let span = within.as_nanos() as i64;
    let ids: Vec<String> = (0..n).map(|_| "10.0.0.1".to_string()).collect();
    let names: Vec<String> = (0..n).map(|_| "person".to_string()).collect();
    let tss: Vec<i64> = (0..n)
        .map(|i| bid_ts - span + (span * i as i64 / n as i64))
        .collect();
    let batch = Arc::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(tss)) as ArrayRef,
            ],
        )
        .unwrap(),
    );
    let index = build_field_index(&batch);
    (0..n)
        .map(|row| {
            let ts = batch_raw_ts_nanos(&batch, 2, row).expect("timestamped row");
            (
                ts,
                JoinRow::Columnar {
                    batch: Arc::clone(&batch),
                    row,
                    index: Arc::clone(&index),
                    projection: None,
                },
            )
        })
        .collect()
}

/// 构造一个带 asof join 的 [`RuleExecutor`]（左键 `sip`，右键 `id`）。
fn asof_join_executor(within: Duration) -> RuleExecutor {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_asof_bench",
        match_plan,
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![JoinPlan {
        right_window: "person_events".to_string(),
        mode: JoinMode::Asof {
            within: Some(within),
        },
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".to_string()),
            right: FieldRef::Simple("id".to_string()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    RuleExecutor::new(rule_plan)
}

/// `execute_match_with_joins` 单次耗时随 asof 候选版本数 `N` 的缩放。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine execute_match_with_joins -- --ignored --nocapture"]
fn execute_match_with_joins_asof_scaling() {
    let bid_ts: i64 = 1_767_225_600_000_000_000;
    let within = Duration::from_secs(300);
    let exec = asof_join_executor(within);

    let matched = MatchedContext {
        rule_name: "r_asof_bench".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: bid_ts,
        event_first_time_nanos: bid_ts,
        event_last_time_nanos: bid_ts,
        first_match_time_nanos: None,
        evidence_first_time_nanos: bid_ts,
        evidence_last_time_nanos: bid_ts,
        window_start_time_nanos: bid_ts - 600_000_000_000,
        window_end_time_nanos: bid_ts + 600_000_000_000,
        machine_id: String::new(),
        trigger_event: None,
    };

    for fast in [true, false] {
        let mode = if fast { "fast(max_ts)" } else { "linear(scan)" };
        for n in [1usize, 10, 100, 200, 500, 1000] {
            let lookup = BenchAsofLookup::new(person_batch_candidates(n, bid_ts, within), fast);
            let reps = 100_000usize;
            let start = Instant::now();
            for _ in 0..reps {
                let record = exec
                    .execute_match_with_joins(&matched, &lookup)
                    .unwrap()
                    .unwrap();
                std::hint::black_box(&record);
            }
            let ns = start.elapsed().as_secs_f64() * 1e9 / reps as f64;
            eprintln!(
                "[exec-join-bench] execute_match_with_joins {mode:>12} candidates={n:>5}  {ns:8.1} ns/op"
            );
        }
    }
}

/// 列式 `not` 基准（issue #22）：`not (auction == 5)` vs `auction != 5`，
/// 每批 1000 行 × 2000 轮。release 下断言 not 与 != 同量级（< 2x），
/// 防止把 `not` 的列式实现退化成逐行 fallback（每行重编译/重扫描）。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn columnar_not_overhead_bounded() {
    use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
    use wf_lang::ast::{BinOp, Expr, FieldRef};

    let n = 1_000usize;
    let (batch, _) = bid_batch(n);
    let view = ColumnarBatch::from_all_fields(&batch);

    let field = |name: &str| Expr::Field(FieldRef::Simple(name.to_string()));
    let not_expr = Expr::Not(Box::new(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("auction")),
        right: Box::new(Expr::Number(5.0)),
    }));
    let ne_expr = Expr::BinOp {
        op: BinOp::Ne,
        left: Box::new(field("auction")),
        right: Box::new(Expr::Number(5.0)),
    };

    let rounds = 2_000usize;
    let start = Instant::now();
    let mut mask = arrow::array::BooleanArray::new_null(0);
    for _ in 0..rounds {
        mask = eval_guard_columnar(&not_expr, &view);
    }
    let not_el = start.elapsed();
    let not_eps = n as f64 * rounds as f64 / not_el.as_secs_f64();

    let start = Instant::now();
    for _ in 0..rounds {
        mask = eval_guard_columnar(&ne_expr, &view);
    }
    let ne_el = start.elapsed();
    let ne_eps = n as f64 * rounds as f64 / ne_el.as_secs_f64();

    let ratio = not_el.as_secs_f64() / ne_el.as_secs_f64();
    eprintln!(
        "[columnar-not-bench] not vs != ratio={:.2}x  not={:.0} ev/s  !=={:.0} ev/s",
        ratio, not_eps, ne_eps
    );
    assert_eq!(mask.len(), n, "列式 not mask 行数必须等于批行数");
    assert!(
        ratio < 2.0,
        "columnar `not` 相对 `!=` 开销过高：{:.2}x (not {:?} vs != {:?})",
        ratio,
        not_el,
        ne_el
    );
}

/// cidr_match 性能保护：列式路径必须可用、mask 正确，且吞吐不能倒退到解释器
/// （子网编译期解析一次；逐行只有 IP 字符串解析）。debug 模式时间噪声大，
/// 用宽松界（列式相对解释 < 1.0x，即列式不慢于解释）守护回归。
#[test]
#[ignore = "性能断言（列式 vs 行式相对比例）：全量并发下比例失真会误报 → 隔离串行跑: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn columnar_cidr_match_overhead_bounded() {
    use crate::match_engine::cep::eval_expr;
    use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};

    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, true)]));
    let vals: Vec<Option<&str>> = (0..n)
        .map(|i| match i % 5 {
            0 => None,
            1 | 2 => Some("10.23.45.67"), // 命中 10/8
            3 => Some("172.16.9.9"),      // 不命中
            _ => Some("fe80::1"),         // v6 不匹配 v4 网段
        })
        .collect();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vals)) as ArrayRef]).unwrap();
    let view = ColumnarBatch::from_all_fields(&batch);

    let field = |name: &str| Expr::Field(FieldRef::Simple(name.to_string()));
    let cidr_expr = Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".into(),
        args: vec![field("sip"), Expr::StringLit("10.0.0.0/8".into())],
    };
    assert!(
        wf_lang::columnar::expr_is_columnar(&cidr_expr),
        "cidr_match 字面量形态必须列式"
    );

    let rounds = 400usize;
    // Best-of-5 timing: parallel debug-test runs (the crate suite runs multiple
    // test binaries) starve one path transiently; the minimum of several
    // samples keeps the "columnar not slower than interpreted" assertion
    // robust to contention noise.
    let mut col_el = Duration::MAX;
    let mut mask = arrow::array::BooleanArray::new_null(0);
    for _ in 0..5 {
        let start = Instant::now();
        for _ in 0..rounds {
            mask = eval_guard_columnar(&cidr_expr, &view);
        }
        col_el = col_el.min(start.elapsed());
    }
    let col_eps = n as f64 * rounds as f64 / col_el.as_secs_f64();

    // mask 正确性：null → false；10/8 命中为 true；其余 false。
    assert_eq!(mask.len(), n);
    for (row, expect) in [(1, true), (2, true), (3, false), (4, false), (0, false)] {
        assert_eq!(mask.value(row), expect, "row {row}");
    }

    // 解释路径对照（相同轮次，公平比较每行成本）。
    let events = batch_to_events(&batch);
    let mut row_el = Duration::MAX;
    let mut count = 0usize;
    for _ in 0..5 {
        let start = Instant::now();
        for _ in 0..rounds {
            count = 0;
            for ev in &events {
                if eval_expr(&cidr_expr, ev) == Some(Value::Bool(true)) {
                    count += 1;
                }
            }
        }
        row_el = row_el.min(start.elapsed());
    }
    let row_eps = n as f64 * rounds as f64 / row_el.as_secs_f64();
    // 每行 i%5∈{1,2} 命中 → 每轮 400 行。
    assert_eq!(count, 400, "每轮解释路径命中数应为 400");

    let ratio = col_el.as_secs_f64() / row_el.as_secs_f64();
    eprintln!(
        "[columnar-cidr-bench] col={:.0} ev/s  row={:.0} ev/s  col/row={:.2}x",
        col_eps, row_eps, ratio
    );
    assert!(
        ratio < 1.0,
        "columnar cidr_match 不应慢于解释路径：{:.2}x (col {:?} vs row {:?})",
        ratio,
        col_el,
        row_el
    );
}

/// regex_match 性能保护：字面量 pattern 编译期编译一次，列式吞吐必须远快于
/// 解释路径（解释路径每事件重编译正则，release 实测 ~19.6µs/ev vs 预编译
/// 13ns/ev，相差 1465 倍；这里只需证明列式可用且明显更快）。
#[test]
#[ignore = "性能断言（列式 vs 行式相对比例）：全量并发下比例失真会误报 → 隔离串行跑: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn columnar_regex_match_overhead_bounded() {
    use crate::match_engine::cep::eval_expr;
    use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};

    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "action",
        DataType::Utf8,
        true,
    )]));
    let vals: Vec<Option<&str>> = (0..n)
        .map(|i| match i % 4 {
            0 => None,
            1 | 2 => Some("failed_login"),
            _ => Some("success"),
        })
        .collect();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vals)) as ArrayRef]).unwrap();
    let view = ColumnarBatch::from_all_fields(&batch);

    let field = |name: &str| Expr::Field(FieldRef::Simple(name.to_string()));
    let re_expr = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".into(),
        args: vec![field("action"), Expr::StringLit("fail.*".into())],
    };
    assert!(
        wf_lang::columnar::expr_is_columnar(&re_expr),
        "regex_match 字面量形态必须列式"
    );

    let rounds = 400usize;
    // Best-of-5 timing（并行 debug 测试会有瞬时争抢，取最小值最稳健）。
    let mut col_el = Duration::MAX;
    let mut mask = arrow::array::BooleanArray::new_null(0);
    for _ in 0..5 {
        let start = Instant::now();
        for _ in 0..rounds {
            mask = eval_guard_columnar(&re_expr, &view);
        }
        col_el = col_el.min(start.elapsed());
    }
    let col_eps = n as f64 * rounds as f64 / col_el.as_secs_f64();

    // mask 正确性：null → false；fail.* 命中为 true。
    assert_eq!(mask.len(), n);
    for (row, expect) in [(1, true), (2, true), (3, false), (0, false)] {
        assert_eq!(mask.value(row), expect, "row {row}");
    }

    // 解释路径对照（相同轮次）。
    let events = batch_to_events(&batch);
    let mut row_el = Duration::MAX;
    let mut count = 0usize;
    for _ in 0..5 {
        let start = Instant::now();
        for _ in 0..rounds {
            count = 0;
            for ev in &events {
                if eval_expr(&re_expr, ev) == Some(Value::Bool(true)) {
                    count += 1;
                }
            }
        }
        row_el = row_el.min(start.elapsed());
    }
    let row_eps = n as f64 * rounds as f64 / row_el.as_secs_f64();
    // 每行 i%4∈{1,2} 命中 → 每轮 500 行。
    assert_eq!(count, 500, "每轮解释路径命中数应为 500");

    let ratio = col_el.as_secs_f64() / row_el.as_secs_f64();
    eprintln!(
        "[columnar-regex-bench] col={:.0} ev/s  row={:.0} ev/s  col/row={:.2}x",
        col_eps, row_eps, ratio
    );
    // debug 下 regex::is_match 本身开销巨大（~560ns/ev）且受并行负载影响大，
    // 收紧到 <1.0 会误报；<2.0 仍能抓住真正的回归——列式内核逐行重编译正则
    // 会带来 ~2000x 的退化（release 实测 col 9ns vs 重编译 19.6µs）。
    assert!(
        ratio < 2.0,
        "columnar regex_match 不应显著慢于解释路径：{:.2}x (col {:?} vs row {:?})",
        ratio,
        col_el,
        row_el
    );
}

/// contains / startswith / endswith 性能保护：列式路径必须可用、mask 正确，
/// 且吞吐不慢于解释路径（列式避免 per-event 的 Value 克隆 + 双次 eval 分发）。
#[test]
#[ignore = "性能断言（列式 vs 行式相对比例）：全量并发下比例失真会误报 → 隔离串行跑: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn columnar_str_search_overhead_bounded() {
    use crate::match_engine::cep::eval_expr;
    use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};

    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
    ]));
    let vals: Vec<Option<&str>> = (0..n)
        .map(|i| match i % 4 {
            0 => None,
            1 | 2 => Some("failed_login"),
            _ => Some("success"),
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vals)) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1); n])) as ArrayRef,
        ],
    )
    .unwrap();
    let view = ColumnarBatch::from_all_fields(&batch);

    let field = |name: &str| Expr::Field(FieldRef::Simple(name.to_string()));
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "contains".into(),
        args: vec![field("action"), Expr::StringLit("fail".into())],
    };
    assert!(
        wf_lang::columnar::expr_is_columnar(&expr),
        "contains 字面量形态必须列式"
    );

    let rounds = 400usize;
    // Best-of-5 timing（并行 debug 测试会有瞬时争抢，取最小值最稳健）。
    let mut col_el = Duration::MAX;
    let mut mask = arrow::array::BooleanArray::new_null(0);
    for _ in 0..5 {
        let start = Instant::now();
        for _ in 0..rounds {
            mask = eval_guard_columnar(&expr, &view);
        }
        col_el = col_el.min(start.elapsed());
    }
    let col_eps = n as f64 * rounds as f64 / col_el.as_secs_f64();

    // mask 正确性：null → false；含 "fail" 为 true。
    assert_eq!(mask.len(), n);
    for (row, expect) in [(1, true), (2, true), (3, false), (0, false)] {
        assert_eq!(mask.value(row), expect, "row {row}");
    }

    // 解释路径对照（相同轮次）。
    let events = batch_to_events(&batch);
    let mut row_el = Duration::MAX;
    let mut count = 0usize;
    for _ in 0..5 {
        let start = Instant::now();
        for _ in 0..rounds {
            count = 0;
            for ev in &events {
                if eval_expr(&expr, ev) == Some(Value::Bool(true)) {
                    count += 1;
                }
            }
        }
        row_el = row_el.min(start.elapsed());
    }
    let row_eps = n as f64 * rounds as f64 / row_el.as_secs_f64();
    // 每行 i%4∈{1,2} 命中 → 每轮 500 行。
    assert_eq!(count, 500, "每轮解释路径命中数应为 500");

    let ratio = col_el.as_secs_f64() / row_el.as_secs_f64();
    eprintln!(
        "[columnar-str-bench] col={:.0} ev/s  row={:.0} ev/s  col/row={:.2}x",
        col_eps, row_eps, ratio
    );
    assert!(
        ratio < 1.0,
        "columnar contains 不应慢于解释路径：{:.2}x (col {:?} vs row {:?})",
        ratio,
        col_el,
        row_el
    );
}

/// 编译树缓存（review #5）：同一 executor 处理多个同 schema batch 时，编译树
/// 跨 batch 复用应显著快于每 batch 重新编译。用 `regex_match` 作为守卫（正则
/// 编译是重常量，每 batch 重编译的浪费最明显）。
#[test]
fn compiled_guard_cache_beats_per_batch_compile() {
    use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
    use wf_lang::plan::BindPlan;

    let n = 200usize;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "action",
        DataType::Utf8,
        true,
    )]));
    let batches: Vec<RecordBatch> = (0..n)
        .map(|i| {
            let vals: Vec<Option<&str>> = (0..64)
                .map(|r| {
                    if (r + i) % 2 == 0 {
                        Some("failed_login")
                    } else {
                        Some("success")
                    }
                })
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vals)) as ArrayRef],
            )
            .unwrap()
        })
        .collect();

    let field = |name: &str| Expr::Field(FieldRef::Simple(name.to_string()));
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".into(),
        args: vec![field("action"), Expr::StringLit("fail.*".into())],
    };
    let mut plan = simple_rule_plan(
        "cache_bench",
        simple_plan(
            vec![FieldRef::Simple("action".into())],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(5.0),
        "action",
        field("action"),
    );
    plan.binds = vec![BindPlan {
        alias: "b".into(),
        window: "w".into(),
        filter: Some(filter.clone()),
    }];
    let exec = RuleExecutor::new(plan);

    // 方案 A：executor 编译缓存（跨 batch 复用）。
    let mut col_el = Duration::MAX;
    let mut last = arrow::array::BooleanArray::new_null(0);
    for _ in 0..3 {
        let start = Instant::now();
        for b in &batches {
            last = exec.bind_filter_columnar_mask("b", b).expect("列式 mask");
        }
        col_el = col_el.min(start.elapsed());
    }
    assert_eq!(last.len(), 64);

    // 方案 B：每 batch 直接编译（无缓存）。
    let mut row_el = Duration::MAX;
    for _ in 0..3 {
        let start = Instant::now();
        for b in &batches {
            let view = ColumnarBatch::from_all_fields(b);
            std::hint::black_box(eval_guard_columnar(&filter, &view));
        }
        row_el = row_el.min(start.elapsed());
    }

    let per = |d: Duration| d.as_secs_f64() / (n as f64 * 64.0) * 1e9;
    let ratio = col_el.as_secs_f64() / row_el.as_secs_f64();
    eprintln!(
        "[columnar-cache-bench] cached={:.1} ns/ev  per-batch={:.1} ns/ev  cached/per-batch={:.2}x",
        per(col_el),
        per(row_el),
        ratio
    );
    assert!(
        ratio < 1.0,
        "编译缓存应快于每 batch 编译：{:.2}x (cached {:?} vs per-batch {:?})",
        ratio,
        col_el,
        row_el
    );
}

/// 输出链 cell 求值向量化（perf-diag 墙①）：fmt 类输出函数的批量 cell 求值
/// （编译一次 + 整批 eval_vec）应快于逐行解释（每行 eval 分发 + 字段查找 +
/// Value 构造）。
#[test]
#[ignore = "性能断言（列式 vs 行式相对比例）：全量并发下比例失真会误报 → 隔离串行跑: cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture"]
fn columnar_output_func_cell_beats_per_row() {
    use crate::match_engine::cep::eval_expr;
    use crate::match_engine::columnar::{ColumnarBatch, compile_guard, cscalar_to_value};

    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                (0..n).map(|i| Some(format!("evt_{i}"))).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
        ],
    )
    .unwrap();

    let field = |name: &str| Expr::Field(FieldRef::Simple(name.to_string()));
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("evt={}|n={}".into()),
            field("action"),
            field("count"),
        ],
    };
    assert!(wf_lang::columnar::columnar_output_expr(&expr));

    // 方案 A：批量 cell 求值（编译一次 + 整批 eval_vec）。
    let view = ColumnarBatch::from_all_fields(&batch);
    let plan = compile_guard(&expr, &view).expect("fmt 应可编译");
    let mut col_el = Duration::MAX;
    for _ in 0..5 {
        let start = Instant::now();
        let cvec = plan.eval_vec(&view, n);
        for row in 0..n {
            std::hint::black_box(match cvec.scalar_at(row) {
                Some(s) => cscalar_to_value(&s),
                None => Value::Str(smol_str::SmolStr::default()),
            });
        }
        col_el = col_el.min(start.elapsed());
    }

    // 方案 B：逐行解释。
    let events = batch_to_events(&batch);
    let mut row_el = Duration::MAX;
    for _ in 0..5 {
        let start = Instant::now();
        for ev in &events {
            std::hint::black_box(
                eval_expr(&expr, ev).unwrap_or_else(|| Value::Str(smol_str::SmolStr::default())),
            );
        }
        row_el = row_el.min(start.elapsed());
    }

    let per = |d: Duration| d.as_secs_f64() / n as f64 * 1e9;
    let ratio = col_el.as_secs_f64() / row_el.as_secs_f64();
    eprintln!(
        "[columnar-cell-bench] batch={:.1} ns/ev  per-row={:.1} ns/ev  batch/per-row={:.2}x",
        per(col_el),
        per(row_el),
        ratio
    );
    assert!(
        ratio < 1.0,
        "批量 cell 求值应快于逐行解释：{:.2}x (batch {:?} vs per-row {:?})",
        ratio,
        col_el,
        row_el
    );
}
