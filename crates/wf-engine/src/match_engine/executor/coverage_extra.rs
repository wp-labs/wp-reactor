//! Coverage-fill tests for the executor layer。本文件保留共享 harness / 构造器
//! （plan 构造、RowsLookup 假窗口、event / Value 便捷函数）与 `executor/mod.rs` 行式
//! 工具面，供各兄弟子模块经 `use super::*` 复用。按主题拆出的兄弟 `#[path]` 子模块
//! （同目录文件，2026-09-04）：
//! - `coverage_extra_exec`：mod.rs 行式工具面（构造 / 静态预计算、bind / alias 掩码、
//!   coerce 类型矩阵、branch-guard 门控）
//! - `coverage_extra_join`：context.rs（eval-context / execute_joins）+ match_exec 带 join
//! - `coverage_extra_each`：each_exec 行式路径与 gate 分支
//! - `coverage_extra_each_columnar`：each 列式 vs 行式逐位对拍
//! - `coverage_extra_columnar_expr`：P4 gap-3/5/6/7 列式 where / 表达式 cvec 对拍
//! - `coverage_extra_columnar_fallback`：P4 gap-4 列式逐行解释回退对拍
//! - `coverage_extra_close_deferred`：close_exec + deferred_exec（行式 / 列式挂起）
//! - `coverage_extra_yield_fire`：lets / yield（system vars / meta）/ trigger / fire 投影

use crate::match_engine::cep::{
    AsofLookup, CloseOutput, CloseReason, EngineHashMap, Event, MatchedContext, StepData, Value,
    WindowLookup,
};
use crate::match_engine::{JoinRow, RuleExecutor};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EachPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldField, YieldPlan,
};
use wf_lang::{BaseType, FieldType};

// ---------------------------------------------------------------------------
// Helpers (local copies — `tests::helpers` is not reachable from this module)
// ---------------------------------------------------------------------------

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

fn branch_guard(source: &str, guard: Option<Expr>, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard,
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

fn plan_with_close(
    keys: Vec<FieldRef>,
    event_steps: Vec<StepPlan>,
    close_steps: Vec<StepPlan>,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps,
        close_steps,
        close_mode: CloseMode::And,
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

/// Minimal rule plan (mirrors `tests::helpers::simple_rule_plan`).
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

/// Default match plan: single key `sip`, one step `fail count >= 1`.
fn default_match_plan() -> MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    )
}

fn default_matched_context() -> MatchedContext {
    MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 1.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        trigger_event: None,
    }
}

fn step_data(
    label: Option<&str>,
    measure_value: f64,
    field_values: EngineHashMap<String, Vec<Value>>,
) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: label.map(|s| s.to_string()),
        measure_value,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values,
    }
}

fn close_output(
    event_ok: bool,
    close_ok: bool,
    close_mode: CloseMode,
    event_step_data: Vec<StepData>,
    close_step_data: Vec<StepData>,
) -> CloseOutput {
    CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok,
        close_ok,
        close_mode,
        event_emitted: false,
        event_step_data,
        close_step_data,
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    }
}

/// `on each` rule: score 42.5, entity = `e.sip`, two float yields.
fn each_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q1_pass",
        simple_plan(vec![], vec![]),
        Expr::Number(42.5),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "auction_id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction_id".into())),
        },
        YieldField {
            name: "price".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "price".into())),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("auction_id".into(), FieldType::Base(BaseType::Float)),
            ("price".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
}

struct EmptyLookup;
impl WindowLookup for EmptyLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }
}

/// Join lookup holding a fixed candidate set; `asof_lookup_max` outcome is
/// configurable (default `Fallback` → candidate scan).
struct RowsLookup {
    rows: Vec<JoinRow>,
    ts_rows: Vec<(i64, JoinRow)>,
    asof_outcome: Option<AsofLookup>,
}

impl RowsLookup {
    fn new(rows: Vec<JoinRow>) -> Self {
        Self {
            rows,
            ts_rows: Vec::new(),
            asof_outcome: None,
        }
    }
    fn with_ts(ts_rows: Vec<(i64, JoinRow)>) -> Self {
        Self {
            rows: ts_rows.iter().map(|(_, r)| r.clone()).collect(),
            ts_rows,
            asof_outcome: None,
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
    fn asof_candidates(
        &self,
        _w: &str,
        key_field: &str,
        key: &Value,
    ) -> Option<Vec<(i64, JoinRow)>> {
        // 契约（types.rs asof_candidates 文档）：候选 = key_field == key 的行。
        // 2026-08-26 q4a 条件复核冗余跳过后，测试 lookup 必须遵守该契约
        //（否则错误 key 的行会绕过复核被 reduce 选中）。
        Some(
            self.ts_rows
                .iter()
                .filter(|(_, row)| {
                    row.field_value(key_field)
                        .is_some_and(|v| crate::match_engine::values_equal(&v, key))
                })
                .cloned()
                .collect(),
        )
    }
    fn asof_lookup_max(
        &self,
        _w: &str,
        _kf: &str,
        _k: &Value,
        _t: i64,
        _within: Option<&Duration>,
    ) -> AsofLookup {
        self.asof_outcome.clone().unwrap_or(AsofLookup::Fallback)
    }
}

fn join_row(key: &str, key_val: f64, extra: Vec<(&str, Value)>) -> JoinRow {
    let mut fields = EngineHashMap::default();
    fields.insert(key.into(), num(key_val));
    for (name, value) in extra {
        fields.insert(name.into(), value);
    }
    JoinRow::Event(Arc::new(Event { fields }))
}

// ---- 兄弟子模块（2026-09-04 按主题拆分；#[path] 相对本文件目录）----
#[path = "coverage_extra_close_deferred.rs"]
mod coverage_extra_close_deferred;
#[path = "coverage_extra_columnar_expr.rs"]
mod coverage_extra_columnar_expr;
#[path = "coverage_extra_columnar_fallback.rs"]
mod coverage_extra_columnar_fallback;
#[path = "coverage_extra_each.rs"]
mod coverage_extra_each;
#[path = "coverage_extra_each_columnar.rs"]
mod coverage_extra_each_columnar;
#[path = "coverage_extra_exec.rs"]
mod coverage_extra_exec;
#[path = "coverage_extra_join.rs"]
mod coverage_extra_join;
#[path = "coverage_extra_yield_fire.rs"]
mod coverage_extra_yield_fire;
