//! Q17 stats 归并热路径 SoA 对照微基准（2026-08-27）。
//!
//! 对比两个**真实生产函数**（同一批/掩码/行序列, 隔离变量——桶查找/哈希两版本
//! 相同, 差异即累积循环本身）:
//!   accumulate_column_row : Classic——枚举分派 + `Box` 解引用 + 同列重复读取
//!                            （sum/avg/min/max 各自 `column_i128_at` 读 price 4 次）
//!   accumulate_soa        : SoA——counts/sums/mins/maxs 平行数组直写（无枚举/
//!                            Box）+ 同字段分组共享 1 次列读取（q17 price 4 度量
//!                            1 次读）
//!
//! 数据形状 = q17 真实: 8 度量（4 count 分档 + min/max/avg/sum）, 3 个唯一 where
//! （price 分档）, 键 auction（Int64）。每行命中 ~100%（单桶重复累积——累积
//! 循环的成本度量, 与生产命中路径同构）。
//!
//! 运行:
//!   cargo test --release -p wf-engine stats_soa_bench -- --ignored --nocapture
//!
//! 结论口径（2026-08-27 首跑）: 见 bench 输出 + 提交说明; ns/事件为两路径差值。

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::ScopeKey;
use crate::match_engine::executor::StatsExecutor;
use crate::match_engine::executor::{
    NumericSoALayout, RowFieldLayout, StatsAccum, StatsBucket, StatsBucketAccs,
    accumulate_column_row, accumulate_soa, comps_hash, measure_values_soa,
};

const N: usize = 1_000_000;
const AUCTIONS: i64 = 100; // 在航 auction 窗口（~100% 命中, 与 q17 同构）

// ---------------------------------------------------------------------------
// q17 形状构造
// ---------------------------------------------------------------------------

fn price_field() -> Expr {
    Expr::Field(FieldRef::Qualified("b".into(), "price".into()))
}

fn price_lt(v: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Lt,
        left: Box::new(price_field()),
        right: Box::new(Expr::Number(v)),
    }
}

fn price_range(lo: f64, hi: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::And,
        left: Box::new(Expr::BinOp {
            op: BinOp::Ge,
            left: Box::new(price_field()),
            right: Box::new(Expr::Number(lo)),
        }),
        right: Box::new(price_lt(hi)),
    }
}

/// q17 计划: 8 度量（total + r1/r2/r3 分档 + min/max/avg/sum）。
fn q17_plan() -> StatsPlan {
    let mk =
        |label: &str, agg: StatsAggPlan, field: Option<&str>, w: Option<Expr>| StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: w,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        };
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(86_400)),
        keys: vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            mk("total", StatsAggPlan::Count, None, None),
            mk("r1", StatsAggPlan::Count, None, Some(price_lt(10_000.0))),
            mk(
                "r2",
                StatsAggPlan::Count,
                None,
                Some(price_range(10_000.0, 1_000_000.0)),
            ),
            mk(
                "r3",
                StatsAggPlan::Count,
                None,
                Some(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(price_field()),
                    right: Box::new(Expr::Number(1_000_000.0)),
                }),
            ),
            mk("minp", StatsAggPlan::Min, Some("price"), None),
            mk("maxp", StatsAggPlan::Max, Some("price"), None),
            mk("avgp", StatsAggPlan::Avg, Some("price"), None),
            mk("sump", StatsAggPlan::Sum, Some("price"), None),
        ],
        tracked_bind_fields: std::collections::HashMap::new(),
    }
}

/// q17 形状批: Int64 auction（100 键循环）+ Int64 price（对数均匀 [100, 1e8)）。
fn q17_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let auction: Vec<i64> = (0..n).map(|i| (i as i64) % AUCTIONS).collect();
    // 对数均匀: price = 100 * (1e6)^(i/n) 近似——与 nexmark 官方域同构。
    let price: Vec<i64> = (0..n)
        .map(|i| {
            let t = i as f64 / n as f64;
            (100.0 * 1e6f64.powf(t)).round() as i64
        })
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(price)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// q17 真实形状批（stats_task 层归因用）: auction + price + dateTime 时间列
/// （单调递增 65.2µs/事件, 对齐 q17 数据步长; 1d 窗口下批内单段）。
fn q17_batch_with_time(n: usize) -> RecordBatch {
    use arrow::array::TimestampNanosecondArray;
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let auction: Vec<i64> = (0..n).map(|i| (i as i64) % AUCTIONS).collect();
    let price: Vec<i64> = (0..n)
        .map(|i| {
            let t = i as f64 / n as f64;
            (100.0 * 1e6f64.powf(t)).round() as i64
        })
        .collect();
    let time: Vec<i64> = (0..n)
        .map(|i| 1_750_000_000_000_000_000i64 + i as i64 * 65_200)
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(price)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(time)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// 3 个唯一 where 的批级 mask（q17: price 分档）。
fn q17_masks(batch: &RecordBatch) -> Vec<BooleanArray> {
    let price = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mk = |f: &dyn Fn(i64) -> bool| {
        BooleanArray::from(
            (0..price.len())
                .map(|i| f(price.value(i)))
                .collect::<Vec<_>>(),
        )
    };
    vec![
        mk(&|p| p < 10_000),
        mk(&|p| (10_000..1_000_000).contains(&p)),
        mk(&|p| p >= 1_000_000),
    ]
}

// ---------------------------------------------------------------------------
// bench
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn q17_stats_accum_soa() {
    let plan = q17_plan();
    let batch = q17_batch(N);
    let masks = q17_masks(&batch);
    let n_measures = plan.measures.len();
    // 批级预解析（同生产 process_batch_rows）: count 无字段 → None; 数值度量 → price 列。
    let price_col = batch.schema().index_of("price").ok();
    let measure_field_cols: Vec<Option<usize>> = plan
        .measures
        .iter()
        .map(|m| if m.field.is_some() { price_col } else { None })
        .collect();
    // 去重后 where 索引（3 个唯一条件; 与 with_row_fields 同构）。
    let measure_where: Vec<Option<usize>> =
        vec![None, Some(0), Some(1), Some(2), None, None, None, None];
    let measure_field_idx: Vec<Option<usize>> = vec![None; n_measures]; // 无子集
    let row_layout = Arc::new(RowFieldLayout::all_other(&[]));

    // 旧路径桶（Classic）: 8 个 Numeric Box——生产 `accs_for_plan` 同款。
    let mut classic: Vec<StatsAccum> = plan
        .measures
        .iter()
        .map(|m| StatsAccum::for_measure(&m.agg))
        .collect();
    // 新路径桶（SoA）。
    let layout = NumericSoALayout::build(&plan);
    let mut soa = layout.zeros();

    // 预热（分支预测/缓存稳定后计时; 不进计时区间）。
    for row in 0..10_000 {
        accumulate_column_row(
            &mut classic,
            &plan,
            &measure_where,
            &measure_field_idx,
            None,
            None,
            &batch,
            &masks,
            row,
            &row_layout,
            &measure_field_cols,
        );
        accumulate_soa(
            &mut soa,
            &layout,
            &measure_where,
            &measure_field_cols,
            &batch,
            &masks,
            row,
        );
    }

    // —— 旧路径（Classic）——
    let t0 = Instant::now();
    for row in 0..N {
        accumulate_column_row(
            &mut classic,
            &plan,
            &measure_where,
            &measure_field_idx,
            None,
            None,
            &batch,
            &masks,
            row,
            &row_layout,
            &measure_field_cols,
        );
    }
    let old_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;

    // —— 新路径（SoA）——
    let t0 = Instant::now();
    for row in 0..N {
        accumulate_soa(
            &mut soa,
            &layout,
            &measure_where,
            &measure_field_cols,
            &batch,
            &masks,
            row,
        );
    }
    let new_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;

    // 正确性对拍: 两桶最终度量值一致（SoA vs Classic）。
    let classic_values: Vec<f64> = plan
        .measures
        .iter()
        .zip(classic.iter())
        .map(|(m, acc)| match m.agg {
            StatsAggPlan::Count => acc.numeric().count as f64,
            StatsAggPlan::Sum => acc.numeric().sum as f64,
            StatsAggPlan::Avg => {
                let n = acc.numeric().count;
                if n == 0 {
                    0.0
                } else {
                    acc.numeric().sum as f64 / n as f64
                }
            }
            StatsAggPlan::Min => acc.numeric().min.unwrap_or(0) as f64,
            StatsAggPlan::Max => acc.numeric().max.unwrap_or(0) as f64,
            _ => unreachable!(),
        })
        .collect();
    let soa_values = measure_values_soa(&plan, &soa, &layout);
    let mut mismatch: Option<(usize, f64, f64)> = None;
    for (i, (a, b)) in classic_values.iter().zip(soa_values.iter()).enumerate() {
        if (a - b).abs() > 1e-9 {
            mismatch = Some((i, *a, *b));
            break;
        }
    }

    eprintln!(
        "== q17 stats 累积循环对照（N={}, 8 度量, 3 where 分档, 单桶重复命中）==",
        N
    );
    eprintln!("  Classic (枚举+Box+同列4次读): {:.2} ns/事件", old_ns);
    eprintln!("  SoA     (数组直写+同列1次读): {:.2} ns/事件", new_ns);
    eprintln!(
        "  差值 {:.2} ns/事件（{:.1}%）",
        old_ns - new_ns,
        (old_ns - new_ns) / old_ns * 100.0
    );
    match mismatch {
        Some((i, a, b)) => {
            eprintln!("  ⚠ 正确性对拍不一致: 度量 {i} Classic={a} SoA={b}");
        }
        None => eprintln!("  正确性对拍: 一致（8 度量逐值相等）"),
    }

    // 生产完整路径（SoA 分派已接入）: 对照同一批经 StatsExecutor::process_batch_rows。
    let mut exec = StatsExecutor::new(q17_plan());
    let t0 = Instant::now();
    let ok = exec.process_batch_rows(&batch, None);
    let full_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    eprintln!(
        "  生产完整路径 process_batch_rows（含桶查找/哈希/分派）: {:.2} ns/事件（列式前置={}）",
        full_ns, ok
    );
    let bucket = exec
        .window
        .find_bucket(&crate::match_engine::ScopeKey::Int(0));
    eprintln!(
        "  → 生产桶形态: {}",
        match bucket.map(|b| matches!(b, StatsBucketAccs::Numeric(_))) {
            Some(true) => "SoA（纯数值计划已接入）",
            Some(false) => "Classic（未接入?）",
            None => "空桶（异常）",
        }
    );
}

// ---------------------------------------------------------------------------
// q17 rules 段逐分量归因（2026-08-27，#2 采样前的组件级量化）
// ---------------------------------------------------------------------------
//
// 目标: 钉死 q17 rules 段每事件 ~0.58µs·核（diag 核·s 17.3/30M）的成本构成——
// hash（FNV）/ 桶查找（真实规模 180 万桶表的 cache miss）/ SoA 累积 各占多少。
// 结论方向（#3 radix 分区 / 哈希替换 / 热点缓存）由本数据决定。
//
// 关键: 桶查找必须用**真实规模表**（180 万桶 → foldhash 表 ~32MB >> L2）——
// 小表会低估查找成本（cache miss 是主墙候选）。

/// 大表构造（真实 q17 规模）: 直接插桶（buckets 为 pub 字段）。
fn build_large_window(plan: &StatsPlan, n_buckets: usize) -> StatsExecutor {
    let mut exec = StatsExecutor::new(plan.clone());
    let layout = NumericSoALayout::build(plan);
    exec.window.buckets.reserve(n_buckets);
    for i in 0..n_buckets {
        let h = comps_hash(&[ScopeKey::Int(i as i64)]);
        exec.window.buckets.entry(h).or_insert_with(|| {
            vec![StatsBucket {
                scope_key: ScopeKey::Int(i as i64),
                accs: StatsBucketAccs::Numeric(layout.zeros()),
                touch: 0,
            }]
        });
    }
    exec
}

#[test]
#[ignore]
fn q17_rules_breakdown() {
    let plan = q17_plan();
    let layout = NumericSoALayout::build(&plan);
    let batch = q17_batch(N);
    let masks = q17_masks(&batch);
    let price_col = batch.schema().index_of("price").unwrap();
    let measure_field_cols: Vec<Option<usize>> = plan
        .measures
        .iter()
        .map(|m| m.field.as_ref().map(|_| price_col))
        .collect();
    let measure_where: Vec<Option<usize>> =
        vec![None, Some(0), Some(1), Some(2), None, None, None, None];

    // 访问序列: 热点 100 auction 循环（q17 活跃集 ~100）+ 少量冷键穿插。
    const N_LOOKUP: usize = 5_000_000;
    const N_BUCKETS: usize = 1_800_000; // q17 实测 emitted 1,799,180
    let auction_at = |i: usize| (i as i64) % AUCTIONS;

    // —— 大表（180 万桶）——
    let mut exec = build_large_window(&plan, N_BUCKETS);
    // 预热: 访问热点键若干次, 建立 TLB/cache 分布。
    for i in 0..100_000 {
        let a = auction_at(i);
        let _ = exec.window.keyed_bucket_mut(
            comps_hash(&[ScopeKey::Int(a)]),
            &[ScopeKey::Int(a)],
            &plan,
        );
    }

    // 1. hash only
    let t0 = Instant::now();
    for i in 0..N_LOOKUP {
        let a = auction_at(i);
        std::hint::black_box(comps_hash(&[ScopeKey::Int(a)]));
    }
    let hash_ns = t0.elapsed().as_secs_f64() * 1e9 / N_LOOKUP as f64;

    // 2. 桶查找（大表, 热点命中）
    let t0 = Instant::now();
    for i in 0..N_LOOKUP {
        let a = auction_at(i);
        let h = comps_hash(&[ScopeKey::Int(a)]);
        let accs = exec.window.keyed_bucket_mut(h, &[ScopeKey::Int(a)], &plan);
        std::hint::black_box(accs);
    }
    let lookup_large_ns = t0.elapsed().as_secs_f64() * 1e9 / N_LOOKUP as f64;

    // 3. SoA 累积（独立桶）
    let mut soa = layout.zeros();
    let t0 = Instant::now();
    for i in 0..N_LOOKUP {
        let row = i % N;
        accumulate_soa(
            &mut soa,
            &layout,
            &measure_where,
            &measure_field_cols,
            &batch,
            &masks,
            row,
        );
    }
    let accum_ns = t0.elapsed().as_secs_f64() * 1e9 / N_LOOKUP as f64;

    // 4. 完整行（大表查找 + 累积, 不含 hash——与 2+3 差值对照）
    let t0 = Instant::now();
    for i in 0..N_LOOKUP {
        let a = auction_at(i);
        let h = comps_hash(&[ScopeKey::Int(a)]);
        if let Some(StatsBucketAccs::Numeric(soa)) =
            exec.window.keyed_bucket_mut(h, &[ScopeKey::Int(a)], &plan)
        {
            let row = i % N;
            accumulate_soa(
                soa,
                &layout,
                &measure_where,
                &measure_field_cols,
                &batch,
                &masks,
                row,
            );
        }
    }
    let full_ns = t0.elapsed().as_secs_f64() * 1e9 / N_LOOKUP as f64;

    // —— 小表对照（100 桶, L1/L2 命中）——
    let mut small = build_large_window(&plan, 100);
    for i in 0..10_000 {
        let a = auction_at(i);
        let _ = small.window.keyed_bucket_mut(
            comps_hash(&[ScopeKey::Int(a)]),
            &[ScopeKey::Int(a)],
            &plan,
        );
    }
    let t0 = Instant::now();
    for i in 0..N_LOOKUP {
        let a = auction_at(i);
        let h = comps_hash(&[ScopeKey::Int(a)]);
        let accs = small.window.keyed_bucket_mut(h, &[ScopeKey::Int(a)], &plan);
        std::hint::black_box(accs);
    }
    let lookup_small_ns = t0.elapsed().as_secs_f64() * 1e9 / N_LOOKUP as f64;

    // —— 冷键全表随机对照（每次不同键, 真实随机 miss）——
    const N_COLD: usize = 1_000_000;
    let mut cold_exec = build_large_window(&plan, N_BUCKETS);
    let mut cold_state = 0x9E37_79B9_7F4A_7C15u64;
    let t0 = Instant::now();
    for _i in 0..N_COLD {
        // xorshift——确定性伪随机键（覆盖全表不同位置）
        cold_state ^= cold_state << 13;
        cold_state ^= cold_state >> 7;
        cold_state ^= cold_state << 17;
        let k = (cold_state % N_BUCKETS as u64) as i64;
        let h = comps_hash(&[ScopeKey::Int(k)]);
        let accs = cold_exec
            .window
            .keyed_bucket_mut(h, &[ScopeKey::Int(k)], &plan);
        std::hint::black_box(accs);
    }
    let lookup_cold_ns = t0.elapsed().as_secs_f64() * 1e9 / N_COLD as f64;

    // —— 生产入口完整路径（大表 + process_batch_rows, 含批级前置/mask/分派）——
    let mut prod = build_large_window(&plan, N_BUCKETS);
    let t0 = Instant::now();
    let ok = prod.process_batch_rows(&batch, None);
    let prod_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;

    eprintln!(
        "== q17 rules 段逐分量（N_LOOKUP={}, 大表 {} 桶, 热点 {} auction）==",
        N_LOOKUP, N_BUCKETS, AUCTIONS
    );
    eprintln!("  hash(comps_hash FNV)      : {:>6.2} ns/事件", hash_ns);
    eprintln!(
        "  bucket lookup 大表        : {:>6.2} ns/事件（{:.0}%）",
        lookup_large_ns,
        lookup_large_ns / (hash_ns + lookup_large_ns + accum_ns) * 100.0
    );
    eprintln!(
        "  bucket lookup 小表(对照)  : {:>6.2} ns/事件",
        lookup_small_ns
    );
    eprintln!(
        "  bucket lookup 冷随机(对照): {:>6.2} ns/事件",
        lookup_cold_ns
    );
    eprintln!(
        "  ├ 热点局部性收益(冷-热)   : {:>6.2} ns/事件（q17 活跃 ~100 auction → 热路径 ≈ 大表行）",
        lookup_cold_ns - lookup_large_ns
    );
    eprintln!(
        "  accumulate_soa            : {:>6.2} ns/事件（{:.0}%）",
        accum_ns,
        accum_ns / (hash_ns + lookup_large_ns + accum_ns) * 100.0
    );
    eprintln!(
        "  三件合计                  : {:>6.2} ns/事件",
        hash_ns + lookup_large_ns + accum_ns
    );
    eprintln!("  完整行(查找+累积,无hash)  : {:>6.2} ns/事件", full_ns);
    eprintln!(
        "  process_batch_rows 大表   : {:>6.2} ns/事件（列式前置={}）",
        prod_ns, ok
    );
    eprintln!(
        "  → diag 实测 rules 每事件   : ~577 ns·核（核·s 17.3 / 30M）——剩余差距在生产外围（批级/窗口/多核）"
    );
}

/// stats_task 层归因（2026-08-27）: 量化 `process_batch_from` 在
/// `process_batch_rows` 之外的开销组件——max_time 全批扫描 / domain 构造 /
/// 段扫（时间列逐行读取）/ 行域分支。基准 = process_batch_rows 单核 68ns。
#[test]
#[ignore]
fn q17_stats_task_layer() {
    use crate::match_engine::event_bridge::{batch_event_time_nanos_at, batch_time_col_index};

    let _plan = q17_plan();
    let batch = q17_batch_with_time(N);
    let time_col = batch_time_col_index(&batch, Some("dateTime")).unwrap();
    let n = batch.num_rows();

    // 1. batch_max_time 等价（全批时间扫描取 max）
    let t0 = Instant::now();
    let mut max_t = i64::MIN;
    for r in 0..n {
        let t = batch_event_time_nanos_at(&batch, time_col, r);
        if t > max_t {
            max_t = t;
        }
    }
    std::hint::black_box(max_t);
    let max_time_ns = t0.elapsed().as_secs_f64() * 1e9 / n as f64;

    // 2. domain 构造（全批路径: Vec<u32> 0..n）
    let t0 = Instant::now();
    let domain: Vec<u32> = (0..n as u32).collect();
    std::hint::black_box(&domain);
    let domain_ns = t0.elapsed().as_secs_f64() * 1e9 / n as f64;

    // 3. 段扫（1d 窗口批内单段: 逐行时间读取直到越界; black_box 防循环消除）
    let t0 = Instant::now();
    let mut j = 0usize;
    while j < n {
        let t = std::hint::black_box(batch_event_time_nanos_at(
            &batch,
            time_col,
            domain[j] as usize,
        ));
        if t >= max_t {
            break;
        }
        j += 1;
    }
    let seg_ns = t0.elapsed().as_secs_f64() * 1e9 / n as f64;

    // 4. process_batch_rows 行域分支 vs None
    let mut exec = StatsExecutor::new(q17_plan());
    let t0 = Instant::now();
    let ok = exec.process_batch_rows(&batch, Some(&domain));
    let with_rows_ns = t0.elapsed().as_secs_f64() * 1e9 / n as f64;

    let mut exec2 = StatsExecutor::new(q17_plan());
    let t0 = Instant::now();
    let ok2 = exec2.process_batch_rows(&batch, None);
    let none_ns = t0.elapsed().as_secs_f64() * 1e9 / n as f64;

    // 5. 批大小效应（验证 sample 的 eval_vec 热点）: 生产 frame_mb=8 → 批 ~8192 行,
    // eval_vec 的每批固定开销在小批下摊薄不足 → 每事件成本放大。
    const BATCH_ROWS: usize = 8192;
    let small_batch = q17_batch_with_time(BATCH_ROWS);
    let mut exec3 = StatsExecutor::new(q17_plan());
    // 多次小批（等价 30M 行）摊批数, 首批发热排除
    let small_rounds = 30_000_000 / BATCH_ROWS;
    let t0 = Instant::now();
    for _ in 0..small_rounds {
        let ok3 = exec3.process_batch_rows(&small_batch, None);
        std::hint::black_box(ok3);
    }
    let small_ns = t0.elapsed().as_secs_f64() * 1e9 / 30_000_000_f64;

    eprintln!("== q17 stats_task 层归因（N={}, 批内单段 1d 窗）==", N);
    eprintln!(
        "  process_batch_rows None 基线 : {:>6.2} ns/事件（列式前置={}）",
        none_ns, ok2
    );
    eprintln!(
        "  process_batch_rows Some(rows): {:>6.2} ns/事件（列式前置={}）",
        with_rows_ns, ok
    );
    eprintln!(
        "  ├ 行域分支增量             : {:>6.2} ns/事件",
        with_rows_ns - none_ns
    );
    eprintln!(
        "  max_time 全批扫描          : {:>6.2} ns/事件",
        max_time_ns
    );
    eprintln!("  domain Vec<u32> 构造       : {:>6.2} ns/事件", domain_ns);
    eprintln!("  段扫（时间列逐行）         : {:>6.2} ns/事件", seg_ns);
    let task_extra = (with_rows_ns - none_ns) + max_time_ns + domain_ns + seg_ns;
    eprintln!(
        "  stats_task 层合计附加       : {:>6.2} ns/事件（None 基线 + 附加 = {:.1}）",
        task_extra,
        none_ns + task_extra
    );
    eprintln!("  —— 批大小效应（sample 的 eval_vec 热点验证）——");
    eprintln!(
        "  8192 行小批 ×{} 批       : {:>6.2} ns/事件（vs 1M 行单批 {:.2}）",
        small_rounds, small_ns, none_ns
    );
    eprintln!("  → 若小批显著更贵: eval_vec/guard 每批固定开销是主墙, 方向 = mask 计算优化");
    eprintln!("  → diag 577 ns·核 剩余差距   : 投递/多核/窗口 close/ack 等 wf-runtime 外围");
}
