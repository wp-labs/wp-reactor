//! 批级 join-then-key（2026-08-23，q4/q6）一致性对拍：
//! `precompute_join_then_keys` + `advance_at_with_masks_key`（批级去重 lookup +
//! 预解析 scope key）必须与逐事件内部解析（`advance_at_with_masks` 内部
//! `resolve_key_join_scope_key`）产生**相同**的状态机结果序列——int / float
//! 驱动 key、热点重复、join miss、null 左字段全覆盖。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_engine::match_engine::event_bridge::ColumnarEvent;
use wf_engine::match_engine::{
    CepStateMachine, EngineHashMap, Event, JoinRow, Value, WindowLookup,
};
use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{AggPlan, BranchPlan, JoinKeyPlan, MatchPlan as _Plan, StepPlan, WindowSpec};

use super::super::tests::{empty_tracked_bind_fields, empty_tracked_plain_fields};
use super::precompute_join_then_keys;

/// auction_events 窗口替身：id → category。join_lookup 按 id 精确返回一行。
struct MockAuctionLookup {
    map: HashMap<i64, f64>,
}

impl WindowLookup for MockAuctionLookup {
    fn snapshot_field_values(
        &self,
        _w: &str,
        _f: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(
            self.map
                .iter()
                .map(|(&id, &cat)| {
                    let mut f = EngineHashMap::default();
                    f.insert("id".into(), Value::Number(id as f64));
                    f.insert("category".into(), Value::Number(cat));
                    JoinRow::Event(Arc::new(Event { fields: f }))
                })
                .collect(),
        )
    }
    fn join_lookup(&self, _w: &str, _kf: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let Value::Number(n) = key else {
            return Some(vec![]);
        };
        let id = *n as i64;
        let Some(&cat) = self.map.get(&id) else {
            return Some(vec![]); // join miss
        };
        let mut f = EngineHashMap::default();
        f.insert("id".into(), Value::Number(id as f64));
        f.insert("category".into(), Value::Number(cat));
        Some(vec![JoinRow::Event(Arc::new(Event { fields: f }))])
    }
}

/// q4 形状的 key_join plan：`match<category:10m:fixed>`，键来自
/// `b.auction == auction_events.id` 的 join 右窗 category；on event count>=1。
fn q4_key_join_plan() -> _Plan {
    _Plan {
        keys: vec![FieldRef::Simple("category".into())],
        key_map: None,
        key_join: Some(JoinKeyPlan {
            join_idx: 0,
            right_window: "auction_events".into(),
            left_field: FieldRef::Qualified("b".into(), "auction".into()),
            right_key_field: "id".into(),
            right_field: "category".into(),
            key_name: "category".into(),
        }),
        window_spec: WindowSpec::Fixed(Duration::from_secs(600)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("c".into()),
                source: "b".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::new(),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

const TS: i64 = 1_750_000_000_000_000_000;

/// 逐行跑同一批事件，路径 A（内部解析）vs 路径 B（批级预解析）——断言
/// StepResult 序列逐位一致。
fn assert_paths_agree(batch: &RecordBatch, lookup: &MockAuctionLookup) {
    let row_domain: Vec<usize> = (0..batch.num_rows()).collect();
    let plan = q4_key_join_plan();
    let kjp = plan.key_join.as_ref().unwrap();

    // 路径 A：逐事件内部 resolve。
    let mut sm_a = CepStateMachine::new("q".into(), plan.clone(), None);
    let mut results_a = Vec::new();
    for (i, &row) in row_domain.iter().enumerate() {
        let ev = ColumnarEvent::new(batch, row);
        let r = sm_a.advance_at_with_masks("b", &ev, TS + i as i64, Some(lookup), row, None);
        results_a.push(r);
    }

    // 路径 B：批级预解析 + key override。
    let keys = precompute_join_then_keys(batch, &row_domain, kjp, lookup);
    assert_eq!(keys.len(), row_domain.len());
    let mut sm_b = CepStateMachine::new("q".into(), plan, None);
    let mut results_b = Vec::new();
    for (i, &row) in row_domain.iter().enumerate() {
        let ev = ColumnarEvent::new(batch, row);
        let r = sm_b.advance_at_with_masks_key(
            "b",
            &ev,
            TS + i as i64,
            Some(lookup),
            row,
            None,
            Some(&keys[i]),
        );
        results_b.push(r);
    }

    assert_eq!(
        results_a, results_b,
        "批级预解析 vs 逐事件内部解析 StepResult 序列必须一致"
    );
    // 实例状态也一致（同输入同推进）。
    assert_eq!(sm_a.instance_count(), sm_b.instance_count());
}

#[test]
fn precomputed_key_matches_internal_resolution_int_keys() {
    // auction id 1001..1010；bid 引用的 auction 含热点重复、miss（9999）、
    // 以及窗口外 id（0 —— MockLookup 无该行 → miss）。
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            1001, 1002, 1001, 1003, 9999, 1001, 1002, 1004,
        ])) as ArrayRef],
    )
    .unwrap();
    let lookup = MockAuctionLookup {
        map: HashMap::from([(1001, 10.0), (1002, 20.0), (1003, 10.0), (1004, 30.0)]),
    };
    assert_paths_agree(&batch, &lookup);
}

#[test]
fn precomputed_key_matches_internal_resolution_null_and_miss() {
    // null 左字段（Some→None 混合）+ 全 miss 的 key。
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            Some(1001),
            None,
            Some(7777),
            Some(1001),
        ])) as ArrayRef],
    )
    .unwrap();
    let lookup = MockAuctionLookup {
        map: HashMap::from([(1001, 10.0)]),
    };
    assert_paths_agree(&batch, &lookup);
}

#[test]
fn precomputed_key_matches_internal_resolution_float_keys() {
    // float 左 key：1.5 → JoinKey::Int(1) 截断 → 桶 id=1 → values_equal(1.5, 1)
    // = false → miss；2.0 → 精确命中。两条路径都必须走同一复核。
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![1.5, 2.0, 2.0000001, 1.0])) as ArrayRef],
    )
    .unwrap();
    let lookup = MockAuctionLookup {
        map: HashMap::from([(1, 10.0), (2, 20.0)]),
    };
    assert_paths_agree(&batch, &lookup);
}
