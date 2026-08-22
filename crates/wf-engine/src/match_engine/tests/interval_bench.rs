//! P2 interval join（`within` 时间谓词，eager）热路径微基准。
//!
//! 与 snapshot（Q3/Q13/Q20 路径）和 asof（Q22 路径）join 对比，量化 interval 分支的
//! 增量成本：界求值（常量界纯算术 vs 行内字段界 `eval_expr`）+ 区间过滤 +
//! 最早/最新选择 + 富化。
//!
//! 运行：
//!   cargo test --release -p wf-engine interval_bench -- --ignored --nocapture
//!
//! 三条路径都通过「索引已按 key 过滤」的 lookup 替身（`join_lookup`/`asof_candidates`
//! 直接返回候选），等价真实 buffer hash index 的 O(1) 读路径——对比的是 join 逻辑本身。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use wf_lang::ast::{Bound, BoundVal, Expr, FieldRef, JoinMode, WithinSpec};
use wf_lang::plan::{JoinCondPlan, JoinPlan};

use crate::match_engine::JoinRow;
use crate::match_engine::executor::execute_joins;
use crate::match_engine::match_engine::{EngineHashMap, Event, Value, WindowLookup};

const N: usize = 1_000_000;
const NOW: i64 = 1_750_000_000_000_000_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// 右窗候选行：`(ts_nanos, id, price)`。
fn timed_row(ts: i64, id: f64, price: f64) -> (i64, JoinRow) {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Number(id));
    fields.insert("price".into(), Value::Number(price));
    (ts, JoinRow::Event(Arc::new(Event { fields })))
}

/// 模拟「索引已按 key 过滤」的 lookup：`join_lookup`（无时间戳）与 `asof_candidates`
///（带时间戳）都直接返回候选——等价真实 buffer hash index 的 O(1) 路径。
struct CandidatesLookup {
    rows: Vec<(i64, JoinRow)>,
}

impl WindowLookup for CandidatesLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.rows.iter().map(|(_, r)| r.clone()).collect())
    }
    fn join_lookup(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<JoinRow>> {
        Some(self.rows.iter().map(|(_, r)| r.clone()).collect())
    }
    fn asof_candidates(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.rows.clone())
    }
}

/// `on aid == right.id` 的单条件 join。
fn join_with(mode: JoinMode, within: Option<WithinSpec>) -> JoinPlan {
    JoinPlan {
        right_window: "bid_events".to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("aid".into()),
            right: FieldRef::Simple("id".into()),
        }],
        within,
        reduce: None,
        emit_at: None,
    }
}

/// `within [t-10s, t]` 常量界（`within 10s` 糖的等价形态）。
fn within_lookback() -> WithinSpec {
    WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(10),
                neg: true,
            },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::ZERO,
                neg: false,
            },
        },
    }
}

/// `within [lo_f, hi_f]` 行内字段界（左行绝对时间）。
fn within_field_bounds() -> WithinSpec {
    WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Simple("lo_f".into()))),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Simple("hi_f".into()))),
        },
    }
}

fn report(name: &str, per_ns: f64, baseline_ns: f64) {
    let mps = 1e9 / per_ns / 1e6;
    eprintln!(
        "[interval-bench] {:<22} {:>7.1} ns/event  ({:>5.1}M events/s)  = {:>5.1}% of snapshot",
        name,
        per_ns,
        mps,
        per_ns / baseline_ns * 100.0
    );
}

/// 跑一轮 `execute_joins`，返回每事件平均纳秒。
fn run_joins(
    joins: &[JoinPlan],
    lookup: &CandidatesLookup,
    ctx_fields: &[(&str, f64)],
    n: usize,
) -> f64 {
    let mut base = Event {
        fields: EngineHashMap::default(),
    };
    for (k, v) in ctx_fields {
        base.fields.insert((*k).into(), Value::Number(*v));
    }
    let start = Instant::now();
    let mut hits = 0usize;
    for _ in 0..n {
        let mut c = base.clone();
        if execute_joins(joins, &mut c, lookup, NOW) {
            hits += 1;
        }
        std::hint::black_box(&c);
    }
    let per_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    assert!(hits > 0, "{:?} must hit", joins[0].mode);
    per_ns
}

// ---------------------------------------------------------------------------
// Release-only 热路径分解基准
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine interval_bench -- --ignored --nocapture"]
fn interval_join_hot_paths() {
    // 4 行候选：2 行在 [t-10s, t] 内（NOW-8s、NOW-2s），2 行在外（NOW-20s、NOW+5s）
    let rows = vec![
        timed_row(NOW - 20_000_000_000, 1.0, 10.0),
        timed_row(NOW - 8_000_000_000, 1.0, 20.0),
        timed_row(NOW - 2_000_000_000, 1.0, 30.0),
        timed_row(NOW + 5_000_000_000, 1.0, 40.0),
    ];
    let lookup = CandidatesLookup { rows: rows.clone() };

    // snapshot baseline：join_lookup + find_matching_row（Q3/Q13/Q20 路径）
    let snapshot = run_joins(
        &[join_with(JoinMode::Snapshot, None)],
        &lookup,
        &[("aid", 1.0)],
        N,
    );
    report("snapshot", snapshot, snapshot);

    // asof baseline：asof_candidates + find_asof_row（Q22 fallback 路径）
    let asof = run_joins(
        &[join_with(
            JoinMode::Asof {
                within: Some(Duration::from_secs(1800)),
            },
            None,
        )],
        &lookup,
        &[("aid", 1.0)],
        N,
    );
    report("asof(within 1800s)", asof, snapshot);

    // interval：常量界（界求值纯算术）
    let interval_dur = run_joins(
        &[join_with(JoinMode::Inner, Some(within_lookback()))],
        &lookup,
        &[("aid", 1.0)],
        N,
    );
    report("interval-dur", interval_dur, snapshot);

    // interval：行内字段界（界求值走 eval_expr，ctx 提供 lo_f/hi_f）
    let interval_field = run_joins(
        &[join_with(JoinMode::Inner, Some(within_field_bounds()))],
        &lookup,
        &[
            ("aid", 1.0),
            ("lo_f", (NOW - 10_000_000_000) as f64),
            ("hi_f", NOW as f64),
        ],
        N,
    );
    report("interval-field", interval_field, snapshot);

    // 候选行数对区间过滤成本的影响：8 行（1 命中）与 64 行（~5 命中）
    for n_cand in [8usize, 64] {
        let span = 120_000_000_000i64; // [t-60s, t+60s]，1/6 在 [t-10s, t] 内
        let rows: Vec<(i64, JoinRow)> = (0..n_cand)
            .map(|i| {
                let ts = NOW - 60_000_000_000 + span * (i as i64) / (n_cand as i64 - 1);
                timed_row(ts, 1.0, i as f64)
            })
            .collect();
        let lookup = CandidatesLookup { rows };
        let per = run_joins(
            &[join_with(JoinMode::Inner, Some(within_lookback()))],
            &lookup,
            &[("aid", 1.0)],
            N / 4,
        );
        report(&format!("interval-cand{}", n_cand), per, snapshot);
    }
}

// ---------------------------------------------------------------------------
// 常规（debug 可跑）宽松回归测试：interval 不能灾难性慢于 snapshot
// ---------------------------------------------------------------------------

#[test]
fn interval_join_overhead_bounded() {
    // 宽松上限：interval（常量界）每事件耗时 ≤ snapshot × 8 + 20ns 容差。
    // 防止候选过滤/选择逻辑引入灾难性开销（如意外的全窗扫描/每次克隆大结构）。
    let n = 20_000;
    let rows = vec![
        timed_row(NOW - 20_000_000_000, 1.0, 10.0),
        timed_row(NOW - 8_000_000_000, 1.0, 20.0),
        timed_row(NOW - 2_000_000_000, 1.0, 30.0),
        timed_row(NOW + 5_000_000_000, 1.0, 40.0),
    ];
    let lookup = CandidatesLookup { rows: rows.clone() };

    let snapshot = run_joins(
        &[join_with(JoinMode::Snapshot, None)],
        &lookup,
        &[("aid", 1.0)],
        n,
    );
    let interval = run_joins(
        &[join_with(JoinMode::Inner, Some(within_lookback()))],
        &lookup,
        &[("aid", 1.0)],
        n,
    );

    eprintln!(
        "[interval-bench] debug sanity: snapshot {:.1} ns/event, interval {:.1} ns/event",
        snapshot, interval
    );
    assert!(
        interval <= snapshot * 8.0 + 20.0,
        "interval join must stay within 8x of snapshot join: snapshot={snapshot:.1}ns interval={interval:.1}ns"
    );
}
