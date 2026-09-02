//! q13b 形态（on-each + snapshot join + 列式输出）微基准——join lookup
//! **索引 vs 全表扫描**的 ns/行归因（2026-08-23 q13 性能定位）。
//!
//! 背景：q13 双规则链的 q13b（`on each m` + `join side_input snapshot on
//! m.mod_key == side_input.key`）在 daemon 里 ~0.46ms/行（20s/批 × 37k 行）——
//! 怀疑是 provider 窗口（side_input 10000 行）无 join 索引 → `join_lookup`
//! 全表扫描（O(rows) per key）。本基准在同一进程内直接测两个 lookup 实现的
//! ns/行：全表扫描（复现卡顿量级）vs 哈希索引（O(1)），并与无 join 基线对比。
//!
//! 运行：
//!   cargo test --release -p wf-engine q13b_join_bench -- --ignored --nocapture
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan,
    YieldField, YieldPlan,
};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::event_bridge::ColumnarEvent;
use crate::match_engine::{
    EngineHashMap, Event, JoinKey, JoinRow, RuleExecutor, Value, WindowLookup,
};

const N: usize = 1_000_000;
const ALERT_BATCH_SIZE: usize = 256;
const NANOS: i64 = 1_750_000_000_000_000_000;
/// side_input 静态表行数（q13 权威 side_input.txt：key 0..9999）。
const SIDE_ROWS: usize = 10_000;

fn simple_match_plan() -> MatchPlan {
    MatchPlan {
        keys: vec![],
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
        event_steps: vec![],
        close_steps: vec![],
        close_mode: wf_lang::ast::CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

/// q13b 形状的 RuleExecutor：`on each m` + `join side_input snapshot on
/// m.mod_key == side_input.key` + yield（id=bidder / alert_type 常量 /
/// detail=side_input.value（右窗富化）/ request_count 常量）。
fn q13b_plan_rule(with_join: bool) -> RuleExecutor {
    let mut plan = RulePlan {
        conv_window: None,
        name: "q13b_bench".into(),
        binds: vec![BindPlan {
            alias: "m".into(),
            window: "bid_mod".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: simple_match_plan(),
        each_plan: Some(EachPlan {
            alias: "m".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: if with_join {
            vec![JoinPlan {
                right_window: "side_input".into(),
                mode: wf_lang::ast::JoinMode::Snapshot,
                conds: vec![JoinCondPlan {
                    left: FieldRef::Qualified("m".into(), "mod_key".into()),
                    right: FieldRef::Qualified("side_input".into(), "key".into()),
                }],
                within: None,
                reduce: None,
                emit_at: None,
            }]
        } else {
            vec![]
        },
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q13_sidejoin".into()),
                },
                YieldField {
                    name: "detail".into(),
                    value: Expr::Field(FieldRef::Qualified("side_input".into(), "value".into())),
                },
                YieldField {
                    name: "request_count".into(),
                    value: Expr::Number(1.0),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    plan.binds[0].alias = "m".into();
    plan.binds[0].window = "bid_mod".into();
    RuleExecutor::new(plan)
}

/// bid_mod 形状批（mod_key 0..9999 均匀分布，模拟 q13a 输出）。
fn bid_mod_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
    ]));
    let id: Vec<i64> = (0..n as i64).collect();
    let bidder: Vec<i64> = (0..n as i64).map(|i| i % 100_000).collect();
    let auction: Vec<i64> = (0..n as i64).map(|i| i * 7).collect();
    let price: Vec<i64> = (0..n as i64).map(|i| (i * 37) % 1_000_000).collect();
    let date_time: Vec<i64> = (0..n as i64).map(|i| NANOS + i).collect();
    let mod_key: Vec<i64> = (0..n as i64).map(|i| i % SIDE_ROWS as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(id)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(Int64Array::from(mod_key)),
        ],
    )
    .unwrap()
}

/// side_input 静态表行（key 0..9999 → "value-<key>"），JoinRow::Event 形态。
fn side_input_rows() -> Vec<JoinRow> {
    (0..SIDE_ROWS)
        .map(|k| {
            let mut fields = EngineHashMap::default();
            fields.insert("key".into(), Value::Number(k as f64));
            fields.insert("value".into(), Value::Str(format!("value-{k}").into()));
            JoinRow::Event(Arc::new(Event { fields }))
        })
        .collect()
}

/// 全表扫描 lookup：snapshot 返回全部行 → 默认 `join_lookup`（O(rows) filter）。
/// 复现 provider 窗口无索引时的 q13b 行为（每个唯一 key 一次 O(10000) 扫描）。
struct ScanLookup {
    rows: Vec<JoinRow>,
}
impl WindowLookup for ScanLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.rows.clone())
    }
}

/// 哈希索引 lookup：join_lookup O(1)（q13 provider join 索引修复后的形态）。
struct IndexedLookup {
    index: HashMap<JoinKey, Vec<JoinRow>>,
}
impl WindowLookup for IndexedLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }
    fn join_lookup(&self, _w: &str, _kf: &str, key: &Value) -> Option<Vec<JoinRow>> {
        Some(self.index.get(&JoinKey::from_value(key)?)?.clone())
    }
}

fn report(name: &str, per_ns: f64, baseline_ns: f64) {
    let mps = 1e9 / per_ns / 1e6;
    eprintln!(
        "[q13b-join-bench] {:<28} {:>9.1} ns/row  ({:>6.1}M rows/s)  = {:>6.1}% of baseline",
        name,
        per_ns,
        mps,
        per_ns / baseline_ns * 100.0
    );
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q13b_join_bench -- --ignored --nocapture"]
fn q13b_join_index_vs_scan_per_row() {
    let exec_join = q13b_plan_rule(true);
    let exec_plain = q13b_plan_rule(false);
    assert!(
        exec_join.each_plan_columnar_safe() && !exec_join.live_joins().is_empty(),
        "bench rule must pass the columnar join gate"
    );
    assert!(
        exec_plain.each_plan_columnar_safe() && exec_plain.live_joins().is_empty(),
        "plain rule: columnar safe without joins"
    );

    let batch = bid_mod_batch(N);
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..N).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();

    let side_rows = side_input_rows();
    let scan_lookup = ScanLookup {
        rows: side_rows.clone(),
    };
    let mut index: HashMap<JoinKey, Vec<JoinRow>> = HashMap::new();
    for row in side_rows.iter() {
        let key = row.field_value("key").unwrap();
        index
            .entry(JoinKey::from_value(&key).unwrap())
            .or_default()
            .push(row.clone());
    }
    let indexed_lookup = IndexedLookup { index };

    // ---- baseline：无 join 列式 each（q13a 形态） ----
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let start = Instant::now();
    for chunk in rows.chunks(ALERT_BATCH_SIZE) {
        let _ = exec_plain.execute_each_direct_batch_columnar(
            chunk,
            NANOS,
            &mut builder,
            &mut appended,
        );
    }
    let baseline_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- join + 全表扫描（provider 无索引，复现 q13b 卡顿量级） ----
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let start = Instant::now();
    for chunk in rows.chunks(ALERT_BATCH_SIZE) {
        let _ = exec_join.execute_each_direct_batch_columnar_join(
            chunk,
            &scan_lookup,
            NANOS,
            &mut builder,
            &mut appended,
        );
    }
    let scan_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- join + 哈希索引（provider set_join_key 后，O(1)） ----
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let start = Instant::now();
    for chunk in rows.chunks(ALERT_BATCH_SIZE) {
        let _ = exec_join.execute_each_direct_batch_columnar_join(
            chunk,
            &indexed_lookup,
            NANOS,
            &mut builder,
            &mut appended,
        );
    }
    let indexed_ns = start.elapsed().as_nanos() as f64 / N as f64;

    eprintln!("[q13b-join-bench] side_input rows = {SIDE_ROWS}, N = {N}");
    report("no-join columnar each", baseline_ns, baseline_ns);
    report("join + full-scan lookup", scan_ns, baseline_ns);
    report("join + hash-index lookup", indexed_ns, baseline_ns);
    eprintln!(
        "[q13b-join-bench] 索引相对全表扫描加速 = {:.1}x",
        scan_ns / indexed_ns
    );
    // 防御断言：索引应显著快于全表扫描（>50x；索引 O(1) vs 扫描 O(10000)）。
    assert!(
        indexed_ns * 50.0 < scan_ns,
        "索引必须显著快于全表扫描：scan={scan_ns:.1}ns indexed={indexed_ns:.1}ns"
    );
}
