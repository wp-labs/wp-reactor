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
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::executor::{
    RowFieldLayout, StatsAccum, StatsBucketAccs, accumulate_column_row, accumulate_soa,
    measure_values_soa, NumericSoALayout,
};
use crate::match_engine::executor::StatsExecutor;

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
    let mk = |label: &str, agg: StatsAggPlan, field: Option<&str>, w: Option<Expr>| {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: w,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        }
    };
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(86_400)),
        keys: vec![Expr::Field(FieldRef::Qualified("b".into(), "auction".into()))],
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

/// 3 个唯一 where 的批级 mask（q17: price 分档）。
fn q17_masks(batch: &RecordBatch) -> Vec<BooleanArray> {
    let price = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mk = |f: &dyn Fn(i64) -> bool| {
        BooleanArray::from((0..price.len()).map(|i| f(price.value(i))).collect::<Vec<_>>())
    };
    vec![
        mk(&|p| p < 10_000),
        mk(&|p| p >= 10_000 && p < 1_000_000),
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
        .map(|m| m.field.as_ref().map(|_| price_col).flatten())
        .collect();
    // 去重后 where 索引（3 个唯一条件; 与 with_row_fields 同构）。
    let measure_where: Vec<Option<usize>> = vec![
        None,
        Some(0),
        Some(1),
        Some(2),
        None,
        None,
        None,
        None,
    ];
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
        accumulate_soa(&mut soa, &layout, &measure_where, &measure_field_cols, &batch, &masks, row);
    }

    // —— 旧路径（Classic）——
    let t0 = Instant::now();
    for row in 0..N {
        std::hint::black_box(accumulate_column_row(
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
        ));
    }
    let old_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;

    // —— 新路径（SoA）——
    let t0 = Instant::now();
    for row in 0..N {
        std::hint::black_box(accumulate_soa(
            &mut soa, &layout, &measure_where, &measure_field_cols, &batch, &masks, row,
        ));
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
    eprintln!(
        "  Classic (枚举+Box+同列4次读): {:.2} ns/事件",
        old_ns
    );
    eprintln!(
        "  SoA     (数组直写+同列1次读): {:.2} ns/事件",
        new_ns
    );
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
    let bucket = exec.window.find_bucket(&crate::match_engine::ScopeKey::Int(0));
    eprintln!(
        "  → 生产桶形态: {}",
        match bucket.map(|b| matches!(b, StatsBucketAccs::Numeric(_))) {
            Some(true) => "SoA（纯数值计划已接入）",
            Some(false) => "Classic（未接入?）",
            None => "空桶（异常）",
        }
    );
}
