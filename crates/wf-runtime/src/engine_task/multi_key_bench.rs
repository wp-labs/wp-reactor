//! 多 key join 索引基准（2026-08-30 混跑 q8 卡死修复验证）。
//!
//! 背景：同一窗口被不同规则以**不同 key** join（nexmark：q8 按 `seller` /
//! q20 按 `id` 共窗 auction_events）。旧实现 join 索引**首键独占**
//! （`set_join_key` 幂等、后注册者 no-op）→ 后注册规则的 `asof_candidates`
//! 回退**全窗扫描**（`snapshot_with_timestamps` 逐行物化 JoinRow + 过滤），
//! 混跑背压下窗口驻留 10 万+ 行 × deferred pending 数千 → O(pending×全窗) 卡死。
//! 新实现每 key 字段各建索引 → 后注册者也 O(1)。
//!
//! 本基准走**生产路径**（`RegistryLookup::asof_candidates` + 真实 Window），
//! 同一窗口先只建 id 索引（测 seller 查询的全窗扫描回退 = 旧行为），再补建
//! seller 索引（测 O(1) = 新行为），多组窗内行数展示缩放。
//!
//! 运行：
//!   cargo test --release -p wf-runtime multi_key_bench -- --ignored --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_engine::match_engine::{Value, WindowLookup};
use wf_engine::window::{Router, WindowDef, WindowParams, WindowRegistry};

use super::window_lookup::RegistryLookup;

/// auction_events 简化 schema：ts + q20 的 id + q8 的 seller。
fn bench_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("id", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]))
}

fn bench_window_def() -> WindowDef {
    WindowDef {
        params: WindowParams {
            name: "auction_events".into(),
            schema: bench_schema(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["auction_events".into()],
        config: WindowConfig {
            name: "auction_events".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    }
}

/// n 行 auction：id=i、seller=i（高基数贴近真实——q8 按 seller 查，每 key 恰
/// 一行，索引路径 O(1)；扫描路径仍要逐行物化+过滤全部 n 行）。
fn make_batch(schema: &SchemaRef, n: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(TimestampNanosecondArray::from(
                (0..n as i64)
                    .map(|i| 1_000_000_000 + i * 100)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

/// 时间预算测量：跑到 ~250ms，返回 ns/op。key 循环 [1..=n] 全命中（q8 的
/// pending 都是真实 seller，扫描与索引两侧同口径）。
fn measure_ns_per_call(lookup: &RegistryLookup<'_>, n: usize) -> f64 {
    // warmup（首调含索引/扫描结构预热）
    let _ = lookup.asof_candidates("auction_events", "seller", &Value::Number(1.0));
    let budget = Duration::from_millis(250);
    let start = Instant::now();
    let mut ops = 0u64;
    loop {
        for _ in 0..64 {
            let key = Value::Number((ops as usize % n) as f64 + 1.0);
            let rows = lookup.asof_candidates("auction_events", "seller", &key);
            std::hint::black_box(&rows);
            ops += 1;
        }
        if start.elapsed() >= budget {
            break;
        }
    }
    start.elapsed().as_secs_f64() * 1e9 / ops as f64
}

#[tokio::test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime multi_key_bench -- --ignored --nocapture"]
async fn multi_key_join_index_vs_scan_fallback() {
    let sizes: [usize; 4] = [10_000, 50_000, 100_000, 200_000];
    eprintln!();
    eprintln!(
        "=== 多 key join 索引 vs 全窗扫描回退（RegistryLookup::asof_candidates per call）==="
    );
    eprintln!("窗口 auction_events(id,seller)：q20 先建 id 索引；按 seller 查询（q8 形态）");
    eprintln!("旧 = 仅 id 索引（seller 回退全窗扫描）| 新 = id+seller 双索引（O(1)）");
    eprintln!(
        "{:<10} {:>12} {:>12} {:>8}   （q8 混跑形态：5000 pending 的扫描开销）",
        "窗内行数", "旧: 全窗扫描", "新: O(1) 索引", "加速比"
    );
    for n in sizes {
        let registry = WindowRegistry::build(vec![bench_window_def()]).unwrap();
        let router = Arc::new(Router::new(registry));
        router
            .route("auction_events", make_batch(&bench_schema(), n))
            .await
            .unwrap();
        let win = router.registry().get_window("auction_events").unwrap();
        win.set_join_key("id".into()); // q20 先注册

        // 旧行为：seller 无索引 → asof_candidates 回退全窗扫描。
        let lookup = RegistryLookup::new(&router);
        let old_ns = measure_ns_per_call(&lookup, n);

        // 新行为：补建 seller 索引（多 key 支持）→ O(1)。
        win.set_join_key("seller".into());
        let new_ns = measure_ns_per_call(&lookup, n);

        let speedup = old_ns / new_ns;
        let old_total_s = old_ns * 5000.0 / 1e9;
        eprintln!(
            "{:<10} {:>7.1} µs {:>8.0} ns {:>7.1}×   （旧全窗扫描: {:.1}s / 5000 pending）",
            n,
            old_ns / 1000.0,
            new_ns,
            speedup,
            old_total_s,
        );
    }
    eprintln!();
}
