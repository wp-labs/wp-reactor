//! `@first_match_time` 语义测试（issue #82）：窗口实例首次完整命中（match fire
//! 或 qualified close）记录引擎处理墙钟；accu 重复命中保持首次值；新实例周期
//! （reset/新窗口）重新记录；未命中/未注入墙钟 → None。
//!
//! 测试直接驱动 `CepStateMachine`：墙钟通过 `set_processing_wall` 注入，与
//! rule_task 每批注入的口径一致（事件时间与处理墙钟独立）。

use std::collections::HashSet;
use std::time::Duration;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BranchPlan, ExceedAction, LimitsPlan, MatchPlan, RateSpec, WindowSpec,
};

use super::types::Value;
use super::{CepStateMachine, Event, MatchedContext, StepResult, close_is_qualified};

/// 墙钟基准：固定值仅用于断言语义（记录的是处理墙钟，不是事件时间）。
const WALL_1: i64 = 1_750_000_000_000_000_000;
const WALL_2: i64 = WALL_1 + 10_000_000_000;
const WALL_3: i64 = WALL_1 + 20_000_000_000;

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
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

fn step(branches: Vec<BranchPlan>) -> wf_lang::plan::StepPlan {
    wf_lang::plan::StepPlan { branches }
}

/// 单步 count>=1、key=sip、sliding 60s 的 match 计划。`mode`/`accu` 由调用方定。
fn simple_plan(mode: MatchMode, accu: bool) -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
        event_steps: vec![step(vec![branch("e", count_ge(1.0))])],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: mode,
        accu,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

/// And 模式 + close 步骤（req 事件步 + c 关步骤）的计划：事件路径只置 event_ok、
/// 首次命中发生在 close。`mode` 可换 Or 验证事件先 fire、close 保持的路径。
fn plan_with_close_mode(close_mode: CloseMode) -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
        event_steps: vec![step(vec![branch("req", count_ge(1.0))])],
        close_steps: vec![step(vec![branch("c", count_ge(1.0))])],
        close_mode,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

fn plan_with_close() -> MatchPlan {
    plan_with_close_mode(CloseMode::And)
}

/// issue #82 示例同构：seq 模式 + 固定窗口 + `on event<accu>` 单步 count>=1。
fn plan_seq_fixed_accu() -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        event_steps: vec![step(vec![branch("e", count_ge(1.0))])],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: true,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

/// 带速率限制（count/per 窗口）的 accu 计划，用于验证 throttle 抑制分支。
fn plan_seq_accu_with_throttle() -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
        event_steps: vec![step(vec![branch("e", count_ge(1.0))])],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: true,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

fn sip_ev() -> Event {
    event(vec![("sip", str_val("10.0.0.1"))])
}

/// 断言 `advance_at` 产生一次 Matched fire，返回其 ctx（沿用调用点作用域）。
fn expect_matched(
    sm: &mut CepStateMachine,
    alias: &str,
    ev: &Event,
    event_time_nanos: i64,
) -> MatchedContext {
    match sm.advance_at(alias, ev, event_time_nanos) {
        StepResult::Matched(ctx) => ctx,
        other => panic!("expected a Matched fire, got {other:?}"),
    }
}

#[test]
fn first_match_none_when_no_wall_injected() {
    // 驱动方未注入处理墙钟（单测/测试驱动）→ @first_match_time 无值（None），
    // 与 @emit_time 未提供时一致；accu/close 也不会凭空生成。
    let mut sm = CepStateMachine::new("r".into(), simple_plan(MatchMode::Seq, false), None);
    let ctx = expect_matched(&mut sm, "e", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx.first_match_time_nanos, None);
}

#[test]
fn first_match_records_wall_on_first_fire_and_keeps_across_accu() {
    // any 模式 + accu：同实例跨多次 fire（不同批次墙钟），首次命中墙钟恒定。
    let mut sm = CepStateMachine::new("r".into(), simple_plan(MatchMode::Any, true), None);
    sm.set_processing_wall(WALL_1);
    let ctx1 = expect_matched(&mut sm, "e", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx1.first_match_time_nanos, Some(WALL_1));
    // 候选/证据：单事件实例两组都 = [1s,1s]。
    assert_eq!(ctx1.event_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx1.event_last_time_nanos, 1_000_000_000);
    assert_eq!(ctx1.evidence_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx1.evidence_last_time_nanos, 1_000_000_000);

    sm.set_processing_wall(WALL_2);
    let ctx2 = expect_matched(&mut sm, "e", &sip_ev(), 2_000_000_000);
    assert_eq!(
        ctx2.first_match_time_nanos,
        Some(WALL_1),
        "accu 重复命中必须保持首次命中墙钟，而不是当前批次墙钟"
    );
    // accu 重复 fire：候选跨度随窗口累积增长 [1s,2s]；证据同样累积——branch
    // 状态跨 rearm 保留（step_states kept），StepData 证据起点仍是窗口首事件。
    assert_eq!(ctx2.event_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx2.event_last_time_nanos, 2_000_000_000);
    assert_eq!(ctx2.evidence_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx2.evidence_last_time_nanos, 2_000_000_000);

    // 窗口到期 close：实例从未 reset，first_match 保持首次 fire 的墙钟。
    sm.set_processing_wall(WALL_3);
    let outs = sm.scan_expired_at(70_000_000_000);
    assert_eq!(outs.len(), 1);
    assert_eq!(
        outs[0].first_match_time_nanos,
        Some(WALL_1),
        "accu 实例 close 输出保留首次命中墙钟"
    );
    // Or 模式无 close 步：close 证据回退到实例最后事件；候选保持窗口跨度。
    assert_eq!(outs[0].event_first_time_nanos, 1_000_000_000);
    assert_eq!(outs[0].event_last_time_nanos, 2_000_000_000);
    assert_eq!(outs[0].evidence_first_time_nanos, 2_000_000_000);
    assert_eq!(outs[0].evidence_last_time_nanos, 2_000_000_000);
}

#[test]
fn first_match_resets_on_new_instance_cycle() {
    // 非 accu：每次 fire 后 reset（新实例周期）→ 新命中重新记录当前墙钟。
    let mut sm = CepStateMachine::new("r".into(), simple_plan(MatchMode::Any, false), None);
    sm.set_processing_wall(WALL_1);
    let ctx1 = expect_matched(&mut sm, "e", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx1.first_match_time_nanos, Some(WALL_1));

    sm.set_processing_wall(WALL_2);
    let ctx2 = expect_matched(&mut sm, "e", &sip_ev(), 2_000_000_000);
    assert_eq!(
        ctx2.first_match_time_nanos,
        Some(WALL_2),
        "新实例周期（reset）重新记录 first_match_time"
    );
}

#[test]
fn first_match_close_path_records_close_wall_when_first_hit_at_close() {
    // And 模式：事件路径不 fire，首次完整命中发生在 close → 记录 close 处理墙钟。
    let mut sm = CepStateMachine::new("r".into(), plan_with_close(), None);
    sm.set_processing_wall(WALL_1);
    // 事件路径只置 event_ok（And），无 Matched fire。
    assert!(matches!(
        sm.advance_at("req", &sip_ev(), 1_000_000_000),
        StepResult::Advance | StepResult::Accumulate
    ));
    // close 步在独立事件上累积（alias c）。
    sm.advance_at("c", &sip_ev(), 2_000_000_000);

    sm.set_processing_wall(WALL_2);
    let outs = sm.scan_expired_at(70_000_000_000);
    assert_eq!(outs.len(), 1);
    assert!(
        close_is_qualified(&outs[0]),
        "event 步 + close 步都满足 → qualified close"
    );
    assert_eq!(
        outs[0].first_match_time_nanos,
        Some(WALL_2),
        "首次命中发生在 close → first_match_time = close 处理墙钟"
    );
}

#[test]
fn first_match_none_for_close_that_never_qualifies() {
    // close 不 qualified（从未完整命中）→ first_match_time 无值。
    let mut sm = CepStateMachine::new("r".into(), plan_with_close(), None);
    sm.advance_at("req", &sip_ev(), 1_000_000_000); // event 步满足
    // 不给 close 步喂事件 → close 步不满足 → unqualified。
    sm.set_processing_wall(WALL_1);
    let outs = sm.scan_expired_at(70_000_000_000);
    assert_eq!(outs.len(), 1);
    assert!(!close_is_qualified(&outs[0]));
    assert_eq!(outs[0].first_match_time_nanos, None);
}

#[test]
fn event_first_time_is_first_candidate_event_not_bucket_start() {
    // issue #82 方案 A：fixed 窗口里 `@event_first_time` 是桶内首条被接受
    // 事件（示例 08:00:03），不是桶起点（08:00:00）——不能复用 created_at。
    let mut plan = simple_plan(MatchMode::Seq, false);
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(10));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    sm.set_processing_wall(WALL_1);
    // 桶 [0s,10s)，首事件 3s 触发命中。
    let ctx = expect_matched(&mut sm, "e", &sip_ev(), 3_000_000_000);
    assert_eq!(ctx.window_start_time_nanos, 0, "桶起点");
    assert_eq!(
        ctx.event_first_time_nanos, 3_000_000_000,
        "候选首 = 桶内首事件"
    );
    assert_eq!(ctx.event_last_time_nanos, 3_000_000_000);
    assert_eq!(ctx.evidence_first_time_nanos, 3_000_000_000);
    assert_eq!(ctx.evidence_last_time_nanos, 3_000_000_000);
}

#[test]
fn first_match_ignores_event_time_advance_without_wall_change() {
    // 事件时间推进 ≠ 处理墙钟：只有 set_processing_wall 才应改变记录值。
    let mut sm = CepStateMachine::new("r".into(), simple_plan(MatchMode::Any, true), None);
    sm.set_processing_wall(WALL_1);
    let ctx1 = expect_matched(&mut sm, "e", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx1.first_match_time_nanos, Some(WALL_1));
    // 事件时间大幅推进、墙钟不变 → 值不变。
    let ctx2 = expect_matched(&mut sm, "e", &sip_ev(), 59_000_000_000);
    assert_eq!(ctx2.first_match_time_nanos, Some(WALL_1));
}

#[test]
fn first_match_seq_fixed_window_accu_repeat_fires_keep_first_wall() {
    // issue #82 示例同构：seq 模式 + fixed 窗口 + `on event<accu>`——同桶内
    // 多次输出全部保留首次命中墙钟（跨批次墙钟推进也不变）。
    let mut sm = CepStateMachine::new("r".into(), plan_seq_fixed_accu(), None);
    sm.set_processing_wall(WALL_1);
    let ctx1 = expect_matched(&mut sm, "e", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx1.first_match_time_nanos, Some(WALL_1));

    // 同桶第二个事件（不同批次墙钟）→ accu 重复 fire，保持 WALL_1。
    sm.set_processing_wall(WALL_2);
    let ctx2 = expect_matched(&mut sm, "e", &sip_ev(), 2_000_000_000);
    assert_eq!(ctx2.first_match_time_nanos, Some(WALL_1));

    // 桶到期（10s fixed，事件在 [0,10s)）→ close 输出也保持首次命中墙钟。
    sm.set_processing_wall(WALL_3);
    let outs = sm.scan_expired_at(11_000_000_000);
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].first_match_time_nanos, Some(WALL_1));
}

#[test]
fn first_match_or_close_keeps_event_fire_wall_when_close_later_qualifies() {
    // Or close 模式：事件 fire 先命中（记录 WALL_E）；close 步随后满足、close
    // qualified 时不得用 close 墙钟覆盖首次命中值。
    let mut sm = CepStateMachine::new("r".into(), plan_with_close_mode(CloseMode::Or), None);
    sm.set_processing_wall(WALL_1);
    let ctx = expect_matched(&mut sm, "req", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx.first_match_time_nanos, Some(WALL_1));

    // close 步在独立事件上累积（alias c），不触发事件步。
    sm.set_processing_wall(WALL_2);
    sm.advance_at("c", &sip_ev(), 2_000_000_000);

    // close 用更晚的墙钟扫描 → qualified close 保留事件 fire 的首次值。
    sm.set_processing_wall(WALL_3);
    let outs = sm.scan_expired_at(70_000_000_000);
    assert_eq!(outs.len(), 1);
    assert!(close_is_qualified(&outs[0]), "close 步已满足 → qualified");
    assert_eq!(
        outs[0].first_match_time_nanos,
        Some(WALL_1),
        "close 不得覆盖已 fire 的首次命中墙钟"
    );
}

#[test]
fn first_match_throttled_accu_fire_keeps_prior_first_wall() {
    // rate-limit 抑制分支（ExceedAction::Throttle + accu → rearm）：被抑制的
    // 输出不算命中，且 rearm 不得清掉已记录的首次命中墙钟。
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::Throttle,
        disk_provider: None,
        max_disk_bytes: None,
    };
    let plan = plan_seq_accu_with_throttle();
    let mut sm = CepStateMachine::with_limits("r".into(), plan, None, Some(limits));

    // 第一次 fire（消耗当窗口额度）→ 记录 WALL_1。
    sm.set_processing_wall(WALL_1);
    let ctx1 = expect_matched(&mut sm, "e", &sip_ev(), 1_000_000_000);
    assert_eq!(ctx1.first_match_time_nanos, Some(WALL_1));

    // 同窗口第二事件被 throttle 抑制（accu → rearm，无 Matched）。
    sm.set_processing_wall(WALL_2);
    assert!(matches!(
        sm.advance_at("e", &sip_ev(), 2_000_000_000),
        StepResult::Accumulate
    ));

    // 窗口旋转后额度恢复：fire 仍然保持首次值 WALL_1（rearm 保留）。
    sm.set_processing_wall(WALL_3);
    let ctx3 = expect_matched(&mut sm, "e", &sip_ev(), 61_000_000_000);
    assert_eq!(
        ctx3.first_match_time_nanos,
        Some(WALL_1),
        "被 throttle 抑制的 rearm 不得清空/改写首次命中墙钟"
    );
}
