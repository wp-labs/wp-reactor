//! Round-4 coverage-fill tests for the executor layer. 本文件保留
//! `executor/mod.rs` 行式工具面（yield coercion branches、bind filter /
//! columnar gates、where / machine-id / scope-key）的断言，共享 harness
//! （plan / RowsLookup / one_cond_join 构造）留此供子模块 `use super::*`
//! 复用。按主题拆出的兄弟 `#[path]` 子模块（同目录文件）：
//! - `coverage_r4_context`：executor/context.rs（join 模式 / 区间 / asof /
//!   eval-context 窄化）
//! - `coverage_r4_close_each`：executor/close_exec.rs 与 each_exec.rs
//! - `coverage_r4_match_alert`：match-alert 行式 / ctx-free / 列式批对拍
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, CloseMode, CmpOp, Expr, FieldRef, JoinMode, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan,
    RulePlan, ScorePlan, StepPlan, WindowSpec, YieldPlan,
};
use wf_lang::{BaseType, FieldType};

use crate::match_engine::cep::{EngineHashMap, Event, StepData, Value, WindowLookup};
use crate::match_engine::{JoinRow, RuleExecutor};

// ---------------------------------------------------------------------------
// Helpers (local copies — `tests::helpers` is not reachable from this module)
// ---------------------------------------------------------------------------

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

fn simple_key(name: &str) -> FieldRef {
    FieldRef::Simple(name.to_string())
}

fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

fn branch(source: &str, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: None,
        agg,
    }
}

fn step(branches: Vec<BranchPlan>) -> StepPlan {
    StepPlan { branches }
}

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

fn simple_rule_plan(
    name: &str,
    match_plan: MatchPlan,
    score_expr: Expr,
    entity_type: &str,
    entity_id_expr: Expr,
) -> RulePlan {
    RulePlan {
        conv_window: None,
        name: name.to_string(),
        binds: vec![BindPlan {
            alias: "fail".to_string(),
            window: "w".to_string(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan,
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: entity_type.to_string(),
            entity_id_expr,
        },
        yield_plan: YieldPlan {
            target: "alerts".to_string(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan { expr: score_expr },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    }
}

fn default_match_plan() -> MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    )
}

fn step_data(label: Option<&str>, measure_value: f64) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: label.map(|s| s.to_string()),
        measure_value,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values: EngineHashMap::default(),
    }
}

/// A `WindowLookup` holding fixed rows for `join_lookup` / asof paths.
struct RowsLookup {
    rows: Vec<JoinRow>,
    ts_rows: Vec<(i64, JoinRow)>,
}

impl RowsLookup {
    fn new(rows: Vec<JoinRow>) -> Self {
        Self {
            rows,
            ts_rows: Vec::new(),
        }
    }
    fn with_ts(ts_rows: Vec<(i64, JoinRow)>) -> Self {
        Self {
            rows: ts_rows.iter().map(|(_, r)| r.clone()).collect(),
            ts_rows,
        }
    }
}

impl WindowLookup for RowsLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.rows.clone())
    }
    fn snapshot_with_timestamps(&self, _w: &str) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.ts_rows.clone())
    }
    fn join_lookup(&self, _w: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        Some(
            self.rows
                .iter()
                .filter(|row| {
                    row.field_value(key_field)
                        .is_some_and(|v| crate::match_engine::values_equal(&v, key))
                })
                .cloned()
                .collect(),
        )
    }
    fn asof_candidates(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.ts_rows.clone())
    }
    fn asof_lookup_max(
        &self,
        _w: &str,
        _kf: &str,
        _k: &Value,
        _t: i64,
        _within: Option<&Duration>,
    ) -> crate::match_engine::AsofLookup {
        crate::match_engine::AsofLookup::Fallback
    }
}

fn join_row_event(fields: Vec<(&str, Value)>) -> JoinRow {
    JoinRow::Event(Arc::new(event(fields)))
}

fn one_cond_join(mode: JoinMode) -> JoinPlan {
    JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("bidder".into()),
            right: FieldRef::Simple("id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }
}

// ---------------------------------------------------------------------------
// executor/mod.rs — yield coercion branches
// ---------------------------------------------------------------------------

#[test]
fn coerce_yield_value_branches() {
    let coerce =
        |name: &str, t: Option<FieldType>, v: Value| -> crate::error::CoreResult<Option<Value>> {
            RuleExecutor::coerce_yield_field_value_with(name, t.as_ref(), v)
        };

    // No type → passthrough.
    assert_eq!(coerce("x", None, num(1.0)).unwrap(), Some(num(1.0)));

    // Chars: Str passthrough, Number rendered, non-finite Number → Err.
    let chars = Some(FieldType::Base(BaseType::Chars));
    assert_eq!(
        coerce("x", chars.clone(), str_val("a")).unwrap(),
        Some(str_val("a"))
    );
    assert_eq!(
        coerce("x", chars.clone(), num(3.5)).unwrap(),
        Some(str_val("3.5"))
    );
    assert_eq!(
        coerce("x", chars.clone(), Value::Bool(true)).unwrap(),
        Some(str_val("true"))
    );
    assert!(coerce("x", chars.clone(), num(f64::NAN)).is_err());
    // Chars from an array → JSON rendering.
    assert_eq!(
        coerce("x", chars.clone(), Value::Array(vec![num(1.0), num(2.0)])).unwrap(),
        Some(str_val("[1.0,2.0]"))
    );
    // Chars from an object → JSON rendering (sorted keys).
    assert_eq!(
        coerce(
            "x",
            chars,
            Value::Object(EngineHashMap::from_iter([
                ("b".into(), num(2.0)),
                ("a".into(), num(1.0)),
            ]))
        )
        .unwrap(),
        Some(str_val(r#"{"a":1.0,"b":2.0}"#))
    );

    // Digit: integer ok; fractional / non-finite / non-number → Err.
    let digit = Some(FieldType::Base(BaseType::Digit));
    assert_eq!(
        coerce("d", digit.clone(), num(3.0)).unwrap(),
        Some(num(3.0))
    );
    assert!(coerce("d", digit.clone(), num(3.5)).is_err());
    assert!(coerce("d", digit.clone(), num(f64::NAN)).is_err());
    assert!(coerce("d", digit.clone(), str_val("x")).is_err());

    // Float: finite ok; non-finite / non-number → Err.
    let float = Some(FieldType::Base(BaseType::Float));
    assert_eq!(
        coerce("f", float.clone(), num(1.5)).unwrap(),
        Some(num(1.5))
    );
    assert!(coerce("f", float.clone(), num(f64::INFINITY)).is_err());
    assert!(coerce("f", float.clone(), str_val("x")).is_err());

    // Bool: bool ok; non-bool → Err.
    let bool_ty = Some(FieldType::Base(BaseType::Bool));
    assert_eq!(
        coerce("b", bool_ty.clone(), Value::Bool(true)).unwrap(),
        Some(Value::Bool(true))
    );
    assert!(coerce("b", bool_ty.clone(), num(1.0)).is_err());

    // Time: number ok; invalid epoch / non-number → Err.
    let time = Some(FieldType::Base(BaseType::Time));
    assert_eq!(
        coerce("t", time.clone(), num(1_700_000_000_000.0)).unwrap(),
        Some(num(1_700_000_000_000.0))
    );
    assert!(coerce("t", time.clone(), num(f64::NAN)).is_err());
    assert!(coerce("t", time.clone(), str_val("x")).is_err());

    // Ip: valid / invalid / non-str.
    let ip = Some(FieldType::Base(BaseType::Ip));
    assert_eq!(
        coerce("i", ip.clone(), str_val("10.0.0.1")).unwrap(),
        Some(str_val("10.0.0.1"))
    );
    assert!(coerce("i", ip.clone(), str_val("not-an-ip")).is_err());
    assert!(coerce("i", ip.clone(), num(1.0)).is_err());

    // Hex: number ok (non-negative integer), str ok (with/without 0x), invalid → Err.
    let hex = Some(FieldType::Base(BaseType::Hex));
    assert_eq!(
        coerce("h", hex.clone(), num(255.0)).unwrap(),
        Some(num(255.0))
    );
    assert!(coerce("h", hex.clone(), num(-1.0)).is_err());
    assert_eq!(
        coerce("h", hex.clone(), str_val("ff")).unwrap(),
        Some(str_val("ff"))
    );
    assert_eq!(
        coerce("h", hex.clone(), str_val("0xFF")).unwrap(),
        Some(str_val("0xFF"))
    );
    assert!(coerce("h", hex.clone(), str_val("zz")).is_err());
    assert!(coerce("h", hex.clone(), Value::Bool(true)).is_err());

    // Array type: array ok; non-array → Err.
    let arr_ty = Some(FieldType::ArrayAny);
    assert_eq!(
        coerce("a", arr_ty.clone(), Value::Array(vec![])).unwrap(),
        Some(Value::Array(vec![]))
    );
    assert!(coerce("a", arr_ty.clone(), num(1.0)).is_err());

    // Object type: object ok; non-object → Err.
    let obj_ty = Some(FieldType::Object);
    assert_eq!(
        coerce("o", obj_ty.clone(), Value::Object(EngineHashMap::default())).unwrap(),
        Some(Value::Object(EngineHashMap::default()))
    );
    assert!(coerce("o", obj_ty.clone(), num(1.0)).is_err());
}

#[test]
fn empty_string_omission_for_non_chars_targets() {
    // A missing field yields the empty-string fallback; for non-Chars targets
    // that empty string is treated as an absent/optional field → Ok(None).
    let float = Some(FieldType::Base(BaseType::Float));
    let res = RuleExecutor::coerce_yield_field_value_with("f", float.as_ref(), str_val(""));
    assert_eq!(res.unwrap(), None);
    // Chars keeps the empty string.
    let chars = Some(FieldType::Base(BaseType::Chars));
    let res = RuleExecutor::coerce_yield_field_value_with("c", chars.as_ref(), str_val(""));
    assert_eq!(res.unwrap(), Some(str_val("")));
}

// ---------------------------------------------------------------------------
// executor/mod.rs — bind filter / columnar gates / misc helpers
// ---------------------------------------------------------------------------

#[test]
fn bind_filter_map_path_and_columnar_gates() {
    // >24 binds forces the map path.
    let mut plan = simple_rule_plan(
        "many_binds",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    plan.binds = (0..30)
        .map(|i| BindPlan {
            alias: format!("b{i}"),
            window: "w".to_string(),
            filter: None,
        })
        .collect();
    plan.binds[29].alias = "target".into();
    let exec = RuleExecutor::new(plan);
    // event_matches_alias with no filter → true via the map path.
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(exec.event_matches_alias("target", &ev, None));

    // each_plan_columnar_safe: no each plan → false; with each plan + no filter → true.
    let mut plan2 = simple_rule_plan(
        "each",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    assert!(!RuleExecutor::new(plan2.clone()).each_plan_columnar_safe());
    plan2.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    assert!(RuleExecutor::new(plan2).each_plan_columnar_safe());

    // bind_filters_columnar_safe / bind_filter_columnar_mask with no filter.
    let exec3 = RuleExecutor::new(simple_rule_plan(
        "bf",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    ));
    assert!(exec3.bind_filters_columnar_safe("w"));

    // branch_guard_masks on a plan with no guards returns empty masks.
    let masks = exec3.branch_guard_masks(&RecordBatch::new_empty(Arc::new(Schema::empty())));
    assert!(masks.is_empty());

    // is_aux_bind_alias: "fail" is a branch source → not aux.
    assert!(!exec3.is_aux_bind_alias("fail"));
    assert!(exec3.is_aux_bind_alias("other"));

    // static_yield_target / plan / output_config accessors.
    assert_eq!(exec3.static_yield_target().as_ref(), "alerts");
    assert_eq!(exec3.plan().name, "bf");
    assert!(exec3.output_config().time_format.contains("%"));
}

#[test]
fn where_ok_and_machine_id_and_scope_key() {
    let mut plan = simple_rule_plan(
        "w",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.where_ok(&event(vec![("flag", Value::Bool(true))])));
    assert!(!exec.where_ok(&event(vec![("flag", Value::Bool(false))])));
    assert!(!exec.where_ok(&event(vec![])));
    // No where clause → always ok.
    let exec2 = RuleExecutor::new(simple_rule_plan(
        "w2",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    ));
    assert!(exec2.where_ok(&event(vec![])));

    // build_machine_id: empty → rule name, else passthrough.
    let m1 = exec2.build_machine_id("");
    assert_eq!(m1.as_ref(), "w2");
    let m2 = exec2.build_machine_id("host-1");
    assert_eq!(m2.as_ref(), "host-1");

    // build_scope_key renders `name=value` pairs.
    let sk = exec2.build_scope_key(&[simple_key("sip")], &[str_val("10.0.0.1")]);
    assert_eq!(sk.as_ref(), "sip=10.0.0.1");
}

// ---- 兄弟子模块（2026-09-04 按主题拆分；#[path] 相对本文件目录）----
#[path = "coverage_r4_close_each.rs"]
mod coverage_r4_close_each;
#[path = "coverage_r4_context.rs"]
mod coverage_r4_context;
#[path = "coverage_r4_match_alert.rs"]
mod coverage_r4_match_alert;
