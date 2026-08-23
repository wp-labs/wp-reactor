//! Q22 `match` 路径（state machine + asof join）的逐分量微基准——事件处理基线。
//!
//! 与 each_bench（Q1 on-each 路径）互补：这里测的是 match 路径的完整事件处理，
//! 分解成「advance（状态机推进）」和「exec（execute_match_with_joins：build_eval_context
//! + join + build_match_alert）」两个阶段，作为后续优化的性能基线。
//!
//! 运行：
//!   cargo test --release -p wf-engine match_bench -- --ignored --nocapture
//!
//! 测量对象（Q22 `q22_asof_person` 真实形状：score=10.0、entity=digit(b.auction)、
//! yield 4 字段、`join person_events asof within 1800s on b.bidder == person_events.id`）：
//!   baseline : advance + execute_match_with_joins（完整事件处理）
//!   advance  : CepStateMachine::advance_at（状态机推进：key 提取/实例维护/step 求值）
//!   exec     : execute_match_with_joins（eval context + join + alert 构建）
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{JoinCondPlan, JoinPlan, MatchPlan, RulePlan, StepPlan, YieldField};
use wf_lang::{BaseType, FieldType};

use crate::match_engine::executor::{CloseCtxFields, build_eval_context, execute_joins};
use crate::match_engine::match_engine::BindData;
use crate::match_engine::{
    AsofLookup, CepStateMachine, EngineHashMap, Event, JoinRow, MatchedContext, RuleExecutor,
    StepData, StepResult, Value, WindowLookup,
};

use super::helpers::{branch, count_ge, simple_key, simple_plan, simple_rule_plan, step};

const N: usize = 1_000_000;
const NOW: i64 = 1_750_000_000_000_000_000;

/// Q22 `q22_asof_person` 形状的 plan（match 部分 + rule 部分）。
fn q22_plan() -> (MatchPlan, RulePlan) {
    let match_plan = simple_plan(
        vec![simple_key("auction")],
        vec![step(vec![branch("b", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "q22_asof_person",
        match_plan.clone(),
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    rule_plan.binds[0].alias = "b".into();
    rule_plan.binds[0].window = "bid_events".into();
    rule_plan.joins = vec![JoinPlan {
        right_window: "person_events".to_string(),
        mode: JoinMode::Asof {
            within: Some(Duration::from_secs(1800)),
        },
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("bidder".to_string()),
            right: FieldRef::Simple("id".to_string()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    rule_plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q22_asof".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::StringLit("asof joined person".into()),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    (match_plan, rule_plan)
}

fn yield_types() -> HashMap<String, FieldType> {
    HashMap::from([
        ("id".into(), FieldType::Base(BaseType::Float)),
        ("alert_type".into(), FieldType::Base(BaseType::Chars)),
        ("detail".into(), FieldType::Base(BaseType::Chars)),
        ("request_count".into(), FieldType::Base(BaseType::Float)),
    ])
}

fn bid_event(auction: i64, bidder: i64) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("auction".into(), Value::Number(auction as f64));
    fields.insert("bidder".into(), Value::Number(bidder as f64));
    Event { fields }
}

fn person_event(id: i64) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Number(id as f64));
    fields.insert("name".into(), Value::Str("person".into()));
    Event { fields }
}

/// 固定返回一条 person 命中的 [`WindowLookup`]（asof 快路径 Hit）。
struct HitLookup(Arc<Event>);

impl WindowLookup for HitLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }

    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }

    fn asof_lookup_max(
        &self,
        _w: &str,
        _key_field: &str,
        _key: &Value,
        _event_time_nanos: i64,
        _within: Option<&Duration>,
    ) -> AsofLookup {
        AsofLookup::Hit(JoinRow::Event(Arc::clone(&self.0)))
    }
}

fn matched_context(trigger: Arc<Event>) -> MatchedContext {
    MatchedContext {
        rule_name: "q22_asof_person".to_string(),
        scope_key: vec![Value::Number(1.0)],
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
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW + 600_000_000_000,
        machine_id: String::new(),
        trigger_event: Some(trigger),
    }
}

struct Report {
    name: &'static str,
    per_ns: f64,
}

impl Report {
    fn line(&self, baseline_ns: f64) {
        let mps = 1e9 / self.per_ns / 1e6;
        eprintln!(
            "[match-bench] {:<24} {:>7.1} ns/event  ({:>5.1}M events/s)  = {:>5.1}% of baseline",
            self.name,
            self.per_ns,
            mps,
            self.per_ns / baseline_ns * 100.0
        );
    }
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine match_bench -- --ignored --nocapture"]
fn q22_match_pipeline_components() {
    let (match_plan, rule_plan) = q22_plan();
    let keys = match_plan.keys.clone();
    let step_plans: Vec<&StepPlan> = match_plan.event_steps.iter().collect();
    let joins = rule_plan.joins.clone();
    let exec = RuleExecutor::new_with_yield_field_types(rule_plan, yield_types());
    let person = Arc::new(person_event(1));
    let lookup = HitLookup(Arc::clone(&person));

    // ---- baseline：完整事件处理（advance + execute_match_with_joins） ----
    let mut machine = CepStateMachine::new(
        "q22_asof_person".into(),
        match_plan.clone(),
        Some("dateTime".into()),
    );
    let mut matched = 0usize;
    let start = Instant::now();
    for i in 0..N {
        let ev = bid_event((i as i64) % 1000, (i as i64) % 1000);
        if let StepResult::Matched(ctx) = machine.advance_at("b", &ev, NOW + i as i64) {
            let rec = exec
                .execute_match_with_joins(&ctx, &lookup)
                .unwrap()
                .unwrap();
            std::hint::black_box(&rec);
            matched += 1;
        }
    }
    let baseline_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert!(matched > 0, "baseline must match events");
    Report {
        name: "baseline(advance+exec)",
        per_ns: baseline_ns,
    }
    .line(baseline_ns);

    // ---- advance：仅状态机推进（不 exec） ----
    let mut machine = CepStateMachine::new(
        "q22_asof_person".into(),
        match_plan.clone(),
        Some("dateTime".into()),
    );
    let mut matched = 0usize;
    let start = Instant::now();
    for i in 0..N {
        let ev = bid_event((i as i64) % 1000, (i as i64) % 1000);
        if matches!(
            machine.advance_at("b", &ev, NOW + i as i64),
            StepResult::Matched(_)
        ) {
            matched += 1;
        }
    }
    let advance_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert!(matched > 0, "advance must match events");
    Report {
        name: "advance(state machine)",
        per_ns: advance_ns,
    }
    .line(baseline_ns);

    // ---- exec：仅 execute_match_with_joins（复用同一 matched ctx） ----
    let ctx = matched_context(Arc::new(bid_event(1, 1)));
    let start = Instant::now();
    for _ in 0..N {
        let rec = exec
            .execute_match_with_joins(&ctx, &lookup)
            .unwrap()
            .unwrap();
        std::hint::black_box(&rec);
    }
    let exec_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "exec(match_with_joins)",
        per_ns: exec_ns,
    }
    .line(baseline_ns);

    // ---- exec 内部三个子阶段（build_eval_context / execute_joins / build_match_alert） ----
    let scope_key = vec![Value::Number(1.0)];
    let step_data = vec![StepData {
        satisfied_branch_index: 0,
        label: None,
        measure_value: 1.0,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values: EngineHashMap::default(),
    }];
    let bind_data: Vec<BindData> = vec![];
    let trigger = Arc::new(bid_event(1, 1));
    let matched = matched_context(Arc::clone(&trigger));

    // build_eval_context
    let start = Instant::now();
    for _ in 0..N {
        let ctx = build_eval_context(
            &keys,
            &scope_key,
            &step_data,
            &bind_data,
            &step_plans,
            Some(&trigger),
            &CloseCtxFields::All,
        );
        std::hint::black_box(&ctx);
    }
    let build_ctx_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "exec.build_eval_context",
        per_ns: build_ctx_ns,
    }
    .line(baseline_ns);

    // execute_joins（复用同一个未 enrich 的 ctx）
    let base_ctx = build_eval_context(
        &keys,
        &scope_key,
        &step_data,
        &bind_data,
        &step_plans,
        Some(&trigger),
        &CloseCtxFields::All,
    );
    let start = Instant::now();
    for _ in 0..N {
        let mut c = base_ctx.clone();
        execute_joins(&joins, &mut c, &lookup, NOW);
        std::hint::black_box(&c);
    }
    let join_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "exec.execute_joins",
        per_ns: join_ns,
    }
    .line(baseline_ns);

    // build_match_alert（复用已 enrich 的 ctx）
    let mut enriched = base_ctx.clone();
    execute_joins(&joins, &mut enriched, &lookup, NOW);
    let start = Instant::now();
    for _ in 0..N {
        let rec = exec.build_match_alert(&matched, &enriched, NOW).unwrap();
        std::hint::black_box(&rec);
    }
    let alert_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "exec.build_match_alert",
        per_ns: alert_ns,
    }
    .line(baseline_ns);
}
