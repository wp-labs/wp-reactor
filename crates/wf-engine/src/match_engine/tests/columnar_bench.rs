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
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::match_engine::{
    ColumnarEvent, FieldSource, Value, batch_to_events, build_field_index, column_scalar_string,
    columnar_join_rows,
};

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
    let price: Vec<Option<i64>> = (0..n as i64).map(|i| (i % 7 != 0).then_some(i % 200)).collect();
    let channel: Vec<String> = (0..n).map(|i| format!("ch{}", i % 8)).collect();
    let url: Vec<String> = (0..n).map(|i| format!("http://example.com/{}", i % 1000)).collect();
    let date_time: Vec<i64> = (0..n as i64).map(|i| 1_700_000_000_000_000_000 + i).collect();
    let extra: Vec<Option<String>> = (0..n).map(|i| (i % 3 != 0).then(|| "x".to_string())).collect();
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
    let rows = columnar_join_rows(vec![batch.clone()]);
    let map_rows: Vec<std::collections::HashMap<String, Value>> = batch_to_events(&batch)
        .into_iter()
        .map(|ev| ev.fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
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
