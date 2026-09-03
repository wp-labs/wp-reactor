//! Round-4 coverage-fill tests for `match_engine/mod.rs` — the
//! `CepStateMachine` driver branches the existing suites leave cold:
//!
//! - window specs: fixed-window bucket keys, session-window expiry refresh
//! - limits enforcement: `max_instances` (Throttle / DropOldest / FailRule),
//!   `max_memory_bytes`, and the shared-limits (P2b) budget paths
//! - emit rate limiting: `max_throttle` suppression + reset, `accu` rearm,
//!   FailRule latch, and raw-conv mode skipping inline throttle
//! - match modes: `Any` (unordered), OR-mode `event_emitted`, AND-mode
//!   `event_ok`
//! - join-then-key (Path A) resolution and its miss lanes
//! - seq `within` / negation resets
//! - expiry scans: stale candidates, session re-queue, skip-non-alerting,
//!   conv filtering, close_all, memory recalibration
//! - plumbing accessors and `extract_event_time` from a configured time field
use std::sync::Arc;

use std::collections::HashSet;
use std::time::Duration;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure, Transform};
use wf_lang::plan::{
    AggPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, ExceedAction, JoinKeyPlan,
    LimitsPlan, MatchPlan, RateSpec, SeqPlan, SeqSkipPlan, SeqStepPlan, SortKeyPlan, WindowSpec,
};

use super::types::{CloseReason, Value};
use super::{CepStateMachine, EngineHashMap, Event, SharedLimits, StepResult};

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

fn num(n: f64) -> Value {
    Value::Number(n)
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

fn simple_key(name: &str) -> FieldRef {
    FieldRef::Simple(name.to_string())
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

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<wf_lang::plan::StepPlan>) -> MatchPlan {
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
        tracked_bind_fields: std::collections::HashMap::new(),
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
    event_steps: Vec<wf_lang::plan::StepPlan>,
    close_steps: Vec<wf_lang::plan::StepPlan>,
    window_dur: Duration,
    close_mode: CloseMode,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(window_dur),
        event_steps,
        close_steps,
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

fn limits(
    on_exceed: ExceedAction,
    max_instances: Option<usize>,
    max_memory: Option<usize>,
) -> Option<LimitsPlan> {
    Some(LimitsPlan {
        max_memory_bytes: max_memory,
        max_instances,
        max_throttle: None,
        on_exceed,
        disk_provider: None,
        max_disk_bytes: None,
    })
}

// ---------------------------------------------------------------------------
// Window specs: fixed buckets + session refresh
// ---------------------------------------------------------------------------

#[test]
fn fixed_window_buckets_and_close_drains_oldest() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Bucket A (t=0) matches immediately.
    assert!(matches!(sm.advance_at("e", &e, 0), StepResult::Matched(_)));
    // Bucket B (t=60s) — same key, different bucket → second instance.
    assert!(matches!(
        sm.advance_at("e", &e, 60_000_000_000),
        StepResult::Matched(_)
    ));
    assert_eq!(sm.instance_count(), 2);
    // close() resolves the oldest bucket first, then the next.
    let first = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(first.is_some());
    assert_eq!(sm.instance_count(), 1);
    let second = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(second.is_some());
    assert_eq!(sm.instance_count(), 0);
    // Closing a missing key → None.
    assert!(
        sm.close(&[str_val("10.0.0.9")], CloseReason::Flush)
            .is_none()
    );
}

#[test]
fn session_window_refreshes_expiry_and_scan_requeues() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Session(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // First event at t=0 (matches, resets; instance alive).
    sm.advance_at("e", &e, 0);
    // Later events refresh the session expiry (last_event → last + 60s).
    assert!(matches!(
        sm.advance_at("e", &e, 30_000_000_000),
        StepResult::Matched(_)
    ));
    sm.advance_at("e", &e, 40_000_000_000);
    // Watermark before the candidate expiry: no closes.
    assert!(sm.scan_expired_at(59_000_000_000).is_empty());
    // The stale candidate (60s) is popped but the refreshed instance expiry
    // (100s) is beyond the watermark → re-queued, still no close.
    assert!(sm.scan_expired_at(91_000_000_000).is_empty());
    // Past the refreshed expiry → the instance expires.
    let out = sm.scan_expired_at(101_000_000_000);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].close_reason, CloseReason::Timeout);
    // scan_expired_at with the machine's own watermark (40s) is empty.
    assert!(sm.scan_expired().is_empty());
}

// ---------------------------------------------------------------------------
// Limits: max_instances / max_memory_bytes (+ shared budget)
// ---------------------------------------------------------------------------

#[test]
fn limits_max_instances_throttle_skips_new_keys() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::Throttle, Some(1), None),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance_at("e", &e1, 0), StepResult::Matched(_)));
    // Second distinct key over the 1-instance cap → throttled (skip).
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert_eq!(sm.advance_at("e", &e2, 1_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn limits_max_instances_drop_oldest_evicts() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::DropOldest, Some(1), None),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance_at("e", &e1, 0), StepResult::Matched(_)));
    // New key evicts the oldest instance and admits the new one.
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert!(matches!(
        sm.advance_at("e", &e2, 1_000),
        StepResult::Matched(_)
    ));
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn limits_fail_rule_latches_and_rejects_future_events() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::FailRule, Some(1), None),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("e", &e1, 0);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    // Over the cap → FailRule latches and skips.
    assert_eq!(sm.advance_at("e", &e2, 1_000), StepResult::Accumulate);
    // All future events (even for existing keys) are rejected.
    assert_eq!(sm.advance_at("e", &e1, 2_000), StepResult::Accumulate);
}

#[test]
fn limits_max_memory_throttle_and_fail_rule() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    // A 1-byte budget is always exceeded by any instance base cost.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan.clone(),
        None,
        limits(ExceedAction::Throttle, None, Some(1)),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);

    // DropOldest: no instances to evict → skip.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan.clone(),
        None,
        limits(ExceedAction::DropOldest, None, Some(1)),
    );
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);

    // FailRule → latch + release reserved slot.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::FailRule, Some(5), Some(1)),
    );
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
}

#[test]
fn shared_limits_budget_across_shards() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let shared = SharedLimits::new();
    // One shared instance slot: shard A takes it, shard B is throttled.
    let mut sm_a = CepStateMachine::with_limits_shared(
        "r".into(),
        plan.clone(),
        None,
        limits(ExceedAction::Throttle, Some(1), None),
        Arc::clone(&shared),
    );
    let mut sm_b = CepStateMachine::with_limits_shared(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::Throttle, Some(1), None),
        Arc::clone(&shared),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(
        sm_a.advance_at("e", &e1, 0),
        StepResult::Matched(_)
    ));
    // Shard B's new key over the shared cap → throttled.
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert_eq!(sm_b.advance_at("e", &e2, 1_000), StepResult::Accumulate);
    // Shared fail: FailRule on one shard rejects events on the other.
    let mut sm_fail = CepStateMachine::with_limits_shared(
        "r2".into(),
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        None,
        limits(ExceedAction::FailRule, Some(1), None),
        Arc::clone(&shared),
    );
    let mut sm_other = CepStateMachine::with_limits_shared(
        "r2".into(),
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        None,
        limits(ExceedAction::FailRule, Some(5), None),
        Arc::clone(&shared),
    );
    let e = event(vec![("sip", str_val("10.0.0.9"))]);
    sm_fail.advance_at("e", &e, 0);
    sm_other.advance_at("e", &e, 1_000);
    // The other shard observes the shared fail latch → rejects.
    assert_eq!(sm_other.advance_at("e", &e, 2_000), StepResult::Accumulate);
    // raw-conv mode + shared memory budget.
    let shared2 = SharedLimits::new();
    let mut sm_mem = CepStateMachine::with_limits_shared(
        "m".into(),
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        None,
        limits(ExceedAction::Throttle, None, Some(1)),
        Arc::clone(&shared2),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm_mem.advance_at("e", &e, 0), StepResult::Accumulate);
    // Recalibrate on a machine that tracks memory.
    sm_mem.recalibrate_memory();
}

// ---------------------------------------------------------------------------
// Emit rate limiting
// ---------------------------------------------------------------------------

#[test]
fn rate_limit_suppresses_match_reset_and_fail_rule() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let rate = RateSpec {
        count: 0, // never allows an emit
        per: Duration::from_secs(60),
    };
    // Throttle: the match is suppressed and the instance reset.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan.clone(),
        None,
        Some(LimitsPlan {
            max_memory_bytes: None,
            max_instances: None,
            max_throttle: Some(rate.clone()),
            on_exceed: ExceedAction::Throttle,
            disk_provider: None,
            max_disk_bytes: None,
        }),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    // DropOldest behaves like Throttle for the emit path.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan.clone(),
        None,
        Some(LimitsPlan {
            max_memory_bytes: None,
            max_instances: None,
            max_throttle: Some(rate.clone()),
            on_exceed: ExceedAction::DropOldest,
            disk_provider: None,
            max_disk_bytes: None,
        }),
    );
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    // FailRule → rule latched.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(LimitsPlan {
            max_memory_bytes: None,
            max_instances: None,
            max_throttle: Some(rate.clone()),
            on_exceed: ExceedAction::FailRule,
            disk_provider: None,
            max_disk_bytes: None,
        }),
    );
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
}

#[test]
fn memory_limit_non_growable_drop_oldest_admission() {
    // 2026-08-31 limits 摊还：单步 count（不可增长）的 DropOldest 准入仍然生效
    // ——新实例准入路径的逐事件检查被保留（摊掉的是非新事件的冗余检查）。
    // 单步 1 branch 实例 base ≈ 240（128 结构 + 32 键 + 80 分支）；budget=400
    // 装得下 1 个、装不下 2 个。
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::DropOldest, None, Some(400)),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance_at("e", &e1, 0), StepResult::Matched(_)));
    assert_eq!(sm.instance_count(), 1);
    // 第二个 key 准入：240+240=480 >= 400 → DropOldest 逐出最旧（key1）后准入。
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert!(matches!(
        sm.advance_at("e", &e2, 1_000),
        StepResult::Matched(_)
    ));
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn steady_instance_state_accumulates_across_events_get_mut() {
    // 2026-09-02 get_mut 摊还：steady（非新实例 + 非 memory_grows_per_event）
    // 事件走 get_mut——同一实例跨事件累积 count 1→…→5，不重复建实例、不 panic
    // （get_mut 的 expect 不变量：探针后无 limits 变更 → 键必在）。带 limits
    // 配置（真实 qradar 形态）但 steady 时两 limits 块均跳过。
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::Throttle, Some(1000), Some(1 << 20)),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // 前 4 个事件在同一实例上累积（count 1..4）→ Accumulate；
    // 第 5 个触发 count 5 → Matched；全程单实例。
    for t in [0, 1_000, 2_000, 3_000] {
        assert_eq!(sm.advance_at("e", &e, t), StepResult::Accumulate, "t={t}");
        assert_eq!(
            sm.instance_count(),
            1,
            "t={t} 同一 key 恒单实例（get_mut 复用）"
        );
    }
    assert!(matches!(
        sm.advance_at("e", &e, 4_000),
        StepResult::Matched(_)
    ));
    assert_eq!(
        sm.instance_count(),
        1,
        "reset 后实例仍在 map（后续事件复用）"
    );
    // 再累积一轮，确认 reset 后的稳态仍正常推进（get_mut 二次稳态）。
    assert_eq!(sm.advance_at("e", &e, 5_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn growing_rule_memory_drop_oldest_evicts_current_without_get_mut() {
    // 2026-09-02 get_mut 摊还的保底：memory_grows_per_event（多步）规则逐事件
    // max_memory 检查仍走 entry 路径——共享预算被（模拟的其他 shard）抬高后，
    // 本机唯一实例（当前 key）在 DropOldest 下被逐出且无法重建（预算仍超）→
    // Accumulate + 0 实例。若误把 steady 分支的 get_mut（expect）用于增长规则
    // 会在此 panic——本测试锁定 entry 分支选择。
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("e", count_ge(1.0))]),
            step(vec![branch("e", count_ge(1.0))]),
        ],
    );
    let shared = SharedLimits::new();
    let mut sm = CepStateMachine::with_limits_shared(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::DropOldest, None, Some(2000)),
        Arc::clone(&shared),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // step1 命中 → Advance（实例 A 入场，base << 2000）。
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Advance);
    assert_eq!(sm.instance_count(), 1);
    // 抬高共享预算到必然超限 → 下一事件（增长规则，非新实例）逐出最旧（=A 自身，
    // evicting_current）且预算仍超 → 放弃重建（N2 槽位归还），Accumulate + 0。
    shared.add_memory(2000);
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn memory_limit_amortized_shared_admission() {
    // 2026-08-31 limits 摊还 + shared（P2b）：单步 count（不可增长）规则在
    // 摊还后仍通过共享镜像做**准入**控制——shard B 的新 key 在共享总量
    // 240+240>=400 时被 Throttle（摊掉的是跨 shard 逐事件检查，准入不变）。
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let shared = SharedLimits::new();
    let mut sm_a = CepStateMachine::with_limits_shared(
        "r".into(),
        plan.clone(),
        None,
        limits(ExceedAction::Throttle, None, Some(400)),
        Arc::clone(&shared),
    );
    let mut sm_b = CepStateMachine::with_limits_shared(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::Throttle, None, Some(400)),
        Arc::clone(&shared),
    );
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(
        sm_a.advance_at("e", &e1, 0),
        StepResult::Matched(_)
    ));
    // shard B 新 key：共享 240+240 >= 400 → Throttle（不插入）。
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    assert_eq!(sm_b.advance_at("e", &e2, 1_000), StepResult::Accumulate);
    assert_eq!(sm_b.instance_count(), 0);
}

#[test]
fn rate_limit_accu_rearm_keeps_accumulating() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.accu = true;
    let rate = RateSpec {
        count: 0,
        per: Duration::from_secs(60),
    };
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(LimitsPlan {
            max_memory_bytes: None,
            max_instances: None,
            max_throttle: Some(rate),
            on_exceed: ExceedAction::Throttle,
            disk_provider: None,
            max_disk_bytes: None,
        }),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    // accu path: the instance rearmed instead of reset — a later event still
    // exists on the same instance (count keeps growing).
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn raw_conv_mode_skips_inline_close_throttle() {
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
        CloseMode::And,
    );
    plan.needs_field_history = true;
    let rate = RateSpec {
        count: 0,
        per: Duration::from_secs(60),
    };
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(LimitsPlan {
            max_memory_bytes: None,
            max_instances: None,
            max_throttle: Some(rate),
            on_exceed: ExceedAction::Throttle,
            disk_provider: None,
            max_disk_bytes: None,
        }),
    );
    sm.set_raw_conv_mode();
    assert!(sm.raw_conv_mode());
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    // Raw-conv mode bypasses the inline throttle — the close still qualifies.
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok && out.close_ok);
}

// ---------------------------------------------------------------------------
// Match modes: Any / OR event_emitted / AND event_ok
// ---------------------------------------------------------------------------

#[test]
fn any_mode_fires_once_all_steps_satisfied() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("a", count_ge(1.0))]),
            step(vec![branch("b", count_ge(1.0))]),
        ],
    );
    plan.match_mode = MatchMode::Any;
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // First step satisfied → still accumulating.
    assert_eq!(sm.advance_at("a", &e, 0), StepResult::Accumulate);
    // Second step satisfied → match fires.
    assert!(matches!(
        sm.advance_at("b", &e, 1_000),
        StepResult::Matched(_)
    ));
    // A repeated satisfied step does not re-fire (flags already set).
    assert_eq!(sm.advance_at("a", &e, 2_000), StepResult::Accumulate);
}

#[test]
fn or_mode_emits_immediately_then_accumulates() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
        CloseMode::Or,
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Event path fires immediately (OR mode).
    assert!(matches!(
        sm.advance_at("req", &e, 0),
        StepResult::Matched(_)
    ));
    // event_emitted → subsequent events just accumulate.
    assert_eq!(sm.advance_at("req", &e, 1_000), StepResult::Accumulate);
    // Close still produces output (the event fire is recorded).
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_emitted);
}

#[test]
fn and_mode_marks_event_ok_then_close_fires() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
        CloseMode::And,
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Event steps complete → event_ok, no emit yet.
    assert_eq!(sm.advance_at("req", &e, 0), StepResult::Advance);
    // Event already ok → subsequent events accumulate.
    assert_eq!(sm.advance_at("req", &e, 1_000), StepResult::Accumulate);
    // Close step satisfies → close emits.
    sm.advance_at("c", &e, 2_000);
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok && out.close_ok);
}

// ---------------------------------------------------------------------------
// Join-then-key (Path A)
// ---------------------------------------------------------------------------

struct KeyJoinLookup {
    rows: Vec<JoinRowLike>,
}

struct JoinRowLike {
    fields: EngineHashMap<smol_str::SmolStr, Value>,
}

impl JoinRowLike {
    fn field(&self, name: &str) -> Option<Value> {
        self.fields.get(name).cloned()
    }
}

impl KeyJoinLookup {
    fn new(rows: Vec<JoinRowLike>) -> Self {
        Self { rows }
    }
}

impl crate::match_engine::cep::WindowLookup for KeyJoinLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<crate::match_engine::JoinRow>> {
        None
    }
    fn join_lookup(
        &self,
        _w: &str,
        key_field: &str,
        key: &Value,
    ) -> Option<Vec<crate::match_engine::JoinRow>> {
        Some(
            self.rows
                .iter()
                .filter(|r| {
                    r.field(key_field)
                        .is_some_and(|v| crate::match_engine::values_equal(&v, key))
                })
                .map(|r| {
                    crate::match_engine::JoinRow::Event(Arc::new(Event {
                        fields: r.fields.clone(),
                    }))
                })
                .collect(),
        )
    }
}

#[test]
fn key_join_resolves_key_from_window_and_skips_on_miss() {
    let mut plan = simple_plan(
        vec![simple_key("category")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.key_join = Some(JoinKeyPlan {
        join_idx: 0,
        right_window: "auction".into(),
        left_field: FieldRef::Simple("auction".into()),
        right_key_field: "id".into(),
        right_field: "category".into(),
        key_name: "category".into(),
    });
    let lookup = KeyJoinLookup::new(vec![JoinRowLike {
        fields: EngineHashMap::from_iter([
            ("id".into(), num(7.0)),
            ("category".into(), str_val("electronics")),
        ]),
    }]);

    // No lookup provided → skip.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    let e = event(vec![("auction", num(7.0))]);
    assert_eq!(sm.advance_at_with("e", &e, 0, None), StepResult::Accumulate);

    // Missing join-left field → skip.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    let e = event(vec![]);
    assert_eq!(
        sm.advance_at_with("e", &e, 0, Some(&lookup)),
        StepResult::Accumulate
    );

    // Key not found in window → skip.
    let empty_lookup = KeyJoinLookup::new(vec![]);
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    let e = event(vec![("auction", num(7.0))]);
    assert_eq!(
        sm.advance_at_with("e", &e, 0, Some(&empty_lookup)),
        StepResult::Accumulate
    );

    // A matching row exists but with a wrong right-key value → skip (the
    // values_equal re-verification rejects it).
    let wrong = KeyJoinLookup::new(vec![JoinRowLike {
        fields: EngineHashMap::from_iter([("id".into(), num(8.0))]),
    }]);
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    let e = event(vec![("auction", num(7.0))]);
    assert_eq!(
        sm.advance_at_with("e", &e, 0, Some(&wrong)),
        StepResult::Accumulate
    );

    // Key field absent on the joined row → skip.
    let no_key = KeyJoinLookup::new(vec![JoinRowLike {
        fields: EngineHashMap::from_iter([("id".into(), num(7.0))]),
    }]);
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    let e = event(vec![("auction", num(7.0))]);
    assert_eq!(
        sm.advance_at_with("e", &e, 0, Some(&no_key)),
        StepResult::Accumulate
    );

    // Happy path: key resolved from the joined row → match fires.
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("auction", num(7.0))]);
    assert!(matches!(
        sm.advance_at_with("e", &e, 0, Some(&lookup)),
        StepResult::Matched(_)
    ));
}

// ---------------------------------------------------------------------------
// Seq: within / negation resets
// ---------------------------------------------------------------------------

fn seq_plan(consec: bool) -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("scan", count_ge(1.0))]),
            step(vec![branch("login", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("scan", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: Some(Duration::from_secs(5)),
                branch: branch("login", count_ge(1.0)),
            },
        ],
    });
    plan
}

#[test]
fn seq_within_violation_resets_chain() {
    let mut sm = CepStateMachine::new("r".into(), seq_plan(false), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // The second step lands outside the 5s `within` gap → chain reset.
    assert_eq!(
        sm.advance_at("login", &e, 60_000_000_000),
        StepResult::Accumulate
    );
    // A fresh chain still fires when within the gap.
    let mut sm = CepStateMachine::new("r".into(), seq_plan(false), None);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    assert!(matches!(
        sm.advance_at("login", &e, 1_000),
        StepResult::Matched(_)
    ));
}

#[test]
fn seq_negation_violation_suppresses_fire() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("scan", count_ge(1.0))]),
            step(vec![branch("login", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("scan", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: true,
                within: None,
                branch: branch("fail", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("login", count_ge(1.0)),
            },
        ],
    });
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    sm.advance_at("scan", &e, 0);
    // A `fail` event violates the negation.
    sm.advance_at("fail", &e, 1_000);
    // The final step is suppressed.
    assert_eq!(sm.advance_at("login", &e, 2_000), StepResult::Accumulate);
}

// ---------------------------------------------------------------------------
// Expiry scans: skip-non-alerting / conv / close_all / recalibrate
// ---------------------------------------------------------------------------

#[test]
fn scan_expired_skip_non_alerting_and_stale_candidate() {
    // AND-mode close-less rule: skip-non-alerting drops every instance (a
    // close-less And rule never sets `event_ok`), while the plain scan emits
    // the non-qualifying close.
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
    );
    plan.close_mode = CloseMode::And;
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(60));

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    assert!(
        sm.scan_expired_at_skip_non_alerting(61_000_000_000)
            .is_empty(),
        "close-less And instance skipped"
    );
    // The plain scan still emits the non-qualifying close.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    assert_eq!(sm.scan_expired_at(61_000_000_000).len(), 1);

    // OR-mode close-less rule → always skipped by skip-non-alerting.
    let mut or_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
    );
    or_plan.window_spec = WindowSpec::Sliding(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), or_plan, None);
    sm.advance_at("req", &e, 0);
    assert!(
        sm.scan_expired_at_skip_non_alerting(61_000_000_000)
            .is_empty()
    );

    // Unbounded budget variant processes the whole heap.
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    sm.advance_at("req", &e, 0);
    assert!(
        sm.scan_expired_at_skip_non_alerting_unbounded(61_000_000_000)
            .is_empty()
    );
}

#[test]
fn scan_and_close_all_with_conv_filtering() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
        CloseMode::And,
    );
    let conv = ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![
                ConvOpPlan::Sort(vec![SortKeyPlan {
                    expr: Expr::Field(FieldRef::Simple("req".into())),
                    descending: true,
                }]),
                ConvOpPlan::Top(10),
            ],
        }],
    };
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let out = sm.scan_expired_at_with_conv(61_000_000_000, Some(&conv));
    assert_eq!(out.len(), 1, "qualifying close passes conv");
    // scan_expired_at_with_conv with no conv plan → passthrough.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    assert_eq!(sm.scan_expired_at_with_conv(61_000_000_000, None).len(), 1);
    // skip-non-alerting variants.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    assert_eq!(
        sm.scan_expired_at_with_conv_skip_non_alerting(61_000_000_000, Some(&conv))
            .len(),
        1
    );
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    assert_eq!(
        sm.scan_expired_at_with_conv_skip_non_alerting_unbounded(61_000_000_000, Some(&conv))
            .len(),
        1
    );
    // close_all_with_conv flushes everything.
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs = sm.close_all_with_conv(CloseReason::Flush, Some(&conv));
    assert_eq!(outs.len(), 1);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn close_all_sorted_and_clears_heaps() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("req", &e1, 0);
    sm.advance_at("req", &e2, 0);
    let outs = sm.close_all(CloseReason::Flush);
    assert_eq!(outs.len(), 2);
    assert_eq!(sm.instance_count(), 0);
    // The expiry heap / pending set are cleared with the instances.
    assert_eq!(sm.scan_expired_at(1_000_000_000_000).len(), 0);
}

#[test]
fn close_all_session_skips_tail_unexpired_sessions() {
    // 2026-08-23 q11 修复：session 尾部未超时会话（last_event + gap > wm）
    // 释放实例但不发射（oracle/Flink 事件时间到末尾即止）。
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Session(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("e", &e, 0);
    sm.advance_at("e", &e, 30_000_000_000); // 会话延长 → last_event = 30s
    assert_eq!(sm.watermark_nanos(), 30_000_000_000);
    // 尾部会话（30s + 60s gap = 90s > wm 30s）未超时 → 释放但不输出。
    let outs = sm.close_all(CloseReason::Flush);
    assert!(outs.is_empty(), "未超时尾部会话不发射");
    assert_eq!(sm.instance_count(), 0, "实例仍被释放（无泄漏）");
}

#[test]
fn close_all_session_emits_sessions_expired_by_watermark() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Session(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("e", &e1, 0);
    sm.advance_at("e", &e1, 30_000_000_000); // last_event = 30s，会话 90s 到期
    // 另一 key 的事件把 watermark 推到 91s（不刷新 10.0.0.1 的会话）——
    // 会话已完整（90s ≤ wm 91s）→ close_all 发射。
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("e", &e2, 91_000_000_000);
    let outs = sm.close_all(CloseReason::Flush);
    assert_eq!(outs.len(), 1, "已完整会话在 flush 收口发射");
    assert_eq!(outs[0].close_reason, CloseReason::Flush);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn close_all_fixed_aligns_wm_without_over_aligning_exact_boundary() {
    // 2026-08-30（q7 尾桶修复的边界修正）：fixed 窗 flush 收口把水位向上对齐到
    // 桶边界（近似 oracle 的 eos 扫收口）。但向上对齐必须是**真 ceil**——旧
    // `div_euclid+1` 在 wm 恰为桶边界时多对齐一档（把下一桶误判完整）。本用例：
    // wm 恰在 10s 边界（T+10s），桶 [T, T+10s) 已完整 → 发射；下一桶
    // [T+10s, T+20s) 未完整（无事件）→ 不得发射。
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
        vec![step(vec![branch("e", count_ge(1.0))])],
        Duration::from_secs(10),
        CloseMode::And,
    );
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(10));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let t = 1_000_000_000_000i64; // 桶 [t-10s, t)
    sm.advance_at("e", &event(vec![("sip", str_val("10.0.0.1"))]), t - 1);
    assert_eq!(sm.watermark_nanos(), t - 1);
    // 另一 key 把水位推到桶边界 t（桶 [t-10s, t) 完整）。
    sm.advance_at("e", &event(vec![("sip", str_val("10.0.0.2"))]), t);
    let outs = sm.close_all(CloseReason::Flush);
    assert_eq!(
        outs.len(),
        1,
        "wm 恰在桶边界：只有已完整桶发射（旧 div_euclid+1 会把下一桶也误判完整）"
    );
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn close_all_hop_aligns_by_slide_not_size() {
    // 2026-08-30（hop 尾窗过度收口回归）：hop 窗在 **slide** 边界收口
    // （w_end = k*slide + size），flush 水位对齐必须取 slide 粒度——用 size
    // 会把 end ∈ (wm, ceil(wm/size)) 段的未收口 hop 窗误判完整并发射。
    // 本用例：hop(10s, 2s)，wm 恰在 2s 边界 T+6s，尾部窗 end=T+8s/T+10s
    // 均 > 对齐后水位 → 不得发射（旧 size 对齐把 wm 顶到 T+10s → 误发 2 条）。
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
        vec![step(vec![branch("e", count_ge(1.0))])],
        Duration::from_secs(10),
        CloseMode::And,
    );
    plan.window_spec = WindowSpec::Hop {
        size: Duration::from_secs(10),
        slide: Duration::from_secs(2),
    };
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let t = 1_700_000_000_000_000_000i64; // 2s/10s 均整除
    // 事件落在 T（尾窗 [T-2s, T+8s) 与 [T, T+10s) 各 count≥1）。
    sm.advance_at("e", &event(vec![("sip", str_val("10.0.0.1"))]), t);
    // 把水位推到 T+6s（恰为 2s 边界）：窗 [T-2s, T+8s) / [T, T+10s) 未到
    // 收口点，且 flush 对齐（slide=2s → T+6s）后仍不完整 → 不发射。
    sm.advance_at(
        "e",
        &event(vec![("sip", str_val("10.0.0.2"))]),
        t + 6_000_000_000,
    );
    assert_eq!(sm.watermark_nanos(), t + 6_000_000_000);
    let outs = sm.close_all(CloseReason::Flush);
    // 完整窗（end ≤ 对齐水位 T+6s）：w_start ∈ {T-8s, T-6s, T-4s} → 3 条。
    // 未收口窗（end = T+8s / T+10s）不得发射——旧 size 对齐把 wm 顶到
    // T+10s，会把它们也误发（共 5 条）。
    assert_eq!(
        outs.len(),
        3,
        "hop flush 只发射 end≤slide 对齐水位的完整窗（旧 size 对齐误发 5 条）"
    );
    assert_eq!(sm.instance_count(), 0, "实例仍被释放（无泄漏）");
}

#[test]
fn close_all_hop_window_end_exact_uses_window_start_not_event_time() {
    // 2026-08-30（hop 完整性判定精度回归）：hop 实例的 `created_at` = 窗口起点
    // （advance_window 的 `created = window_start.unwrap_or(now_nanos)`），close_all
    // 的 w_start = floor(created_at/slide)*slide 因此精确。若 created_at 被误改为
    // 事件时间（如 sliding），事件落在窗口后半段时 w_start 推导会偏移 → 窗口
    // end 虚高 → 误判未完整 → 丢窗口。本用例：事件 T+6s 落在窗口 [T-2s, T+8s)
    // 后半段，wm=T+8s（= 窗口 end，恰为 slide 边界）→ 窗口必须判完整发射；
    // 其它覆盖窗口（end > T+8s）不发射。
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
        vec![step(vec![branch("e", count_ge(1.0))])],
        Duration::from_secs(10),
        CloseMode::And,
    );
    plan.window_spec = WindowSpec::Hop {
        size: Duration::from_secs(10),
        slide: Duration::from_secs(2),
    };
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let t = 1_700_000_000_000_000_000i64; // 2s/10s 均整除
    // 事件 t=T+6s 扇入窗口 w_start ∈ {T-2s, T, T+2s, T+4s, T+6s}。
    sm.advance_at(
        "e",
        &event(vec![("sip", str_val("10.0.0.1"))]),
        t + 6_000_000_000,
    );
    // 另一 key 把水位推到 T+8s（恰为 slide 边界，= 窗口 [T-2s, T+8s) 的 end）。
    sm.advance_at(
        "e",
        &event(vec![("sip", str_val("10.0.0.2"))]),
        t + 8_000_000_000,
    );
    assert_eq!(sm.watermark_nanos(), t + 8_000_000_000);
    let outs = sm.close_all(CloseReason::Flush);
    assert_eq!(
        outs.len(),
        1,
        "窗口 [T-2s, T+8s) 在 wm=end 时完整发射（w_start 用窗口起点非事件时间）"
    );
    assert_eq!(sm.instance_count(), 0, "实例仍被释放（无泄漏）");
}

#[test]
fn recalibrate_memory_recomputes_exact_estimate() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(60));
    // A generous budget admits instances and enables memory tracking.
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        limits(ExceedAction::Throttle, None, Some(usize::MAX)),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 0);
    let tracked = sm.estimated_memory_bytes_for_test();
    assert!(tracked > 0);
    sm.recalibrate_memory();
    // After recalibration the estimate reflects the exact instance sum.
    assert!(sm.estimated_memory_bytes_for_test() >= tracked);
    // Expiring the instance releases its memory.
    assert!(!sm.scan_expired_at(61_000_000_000).is_empty());
    assert_eq!(sm.estimated_memory_bytes_for_test(), 0);
}

#[test]
fn push_expiry_candidate_dedups_per_key() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Matched → reset → push again: the second push is deduplicated.
    assert!(matches!(
        sm.advance_at("req", &e, 0),
        StepResult::Matched(_)
    ));
    assert!(matches!(
        sm.advance_at("req", &e, 1_000),
        StepResult::Matched(_)
    ));
    // A single expiry candidate still closes the instance exactly once.
    assert_eq!(sm.scan_expired_at(2_000).len(), 0);
    assert_eq!(sm.scan_expired_at(301_000_000_000).len(), 1);
}

// ---------------------------------------------------------------------------
// Plumbing accessors / time-field extraction
// ---------------------------------------------------------------------------

#[test]
fn accessors_and_time_field_extraction() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, Some("ts".into()));
    assert_eq!(sm.rule_name(), "r");
    assert_eq!(sm.time_field(), Some("ts"));
    assert_eq!(sm.watermark_nanos(), 0);
    let e = event(vec![("sip", str_val("10.0.0.1")), ("ts", num(1234.0))]);
    assert_eq!(sm.event_time_nanos(&e), 1234);
    // Non-numeric / missing time field → 0.
    let e2 = event(vec![("sip", str_val("10.0.0.1")), ("ts", str_val("x"))]);
    assert_eq!(sm.event_time_nanos(&e2), 0);
    let e3 = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.event_time_nanos(&e3), 0);
    // advance() extracts the time from the configured field automatically.
    assert!(matches!(sm.advance("e", &e), StepResult::Matched(_)));
    // advance_with / advance_at / advance_at_with are the explicit-time routes.
    let mut sm2 = CepStateMachine::new(
        "r".into(),
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        None,
    );
    assert!(matches!(
        sm2.advance_with("e", &e3, None),
        StepResult::Matched(_)
    ));
    let mut sm3 = CepStateMachine::new(
        "r".into(),
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        None,
    );
    assert!(matches!(
        sm3.advance_at("e", &e3, 5_000),
        StepResult::Matched(_)
    ));
    let mut sm4 = CepStateMachine::new(
        "r".into(),
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        None,
    );
    assert!(matches!(
        sm4.advance_at_with("e", &e3, 6_000, None),
        StepResult::Matched(_)
    ));
    // Plan accessor.
    assert_eq!(sm2.plan().keys.len(), 1);
}

#[test]
fn memory_grows_per_event_predicate() {
    // 2026-08-31 limits 摊还的 growability 判定：纯单步 count 规则不可增长
    // （逐事件 max_memory 检查摊掉），多步/close/accu/history/seq/distinct
    // 可增长（保留逐事件检查）。
    use super::super::plan_memory_grows_per_event;

    // 单步 count → false（摊还目标：qradar c/g/s 家族、真实单步计数规则）
    let single = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(3.0))])],
    );
    assert!(!plan_memory_grows_per_event(&single), "单步 count 不可增长");

    // 多步（completed_steps 累积）→ true
    let multi = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("e", count_ge(1.0))]),
            step(vec![branch("e", count_ge(1.0))]),
        ],
    );
    assert!(plan_memory_grows_per_event(&multi), "多步可增长");

    // distinct → true（distinct_set 无上限累积）
    let distinct = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch(
            "e",
            AggPlan {
                transforms: vec![Transform::Distinct],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(2.0),
            },
        )])],
    );
    assert!(plan_memory_grows_per_event(&distinct), "distinct 可增长");

    // close 步骤 → true
    let close = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
        CloseMode::Or,
    );
    assert!(plan_memory_grows_per_event(&close), "close 可增长");

    // accu / needs_field_history / seq 单 flag → true
    let mut accu = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    accu.accu = true;
    assert!(plan_memory_grows_per_event(&accu), "accu 可增长");

    let mut hist = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    hist.needs_field_history = true;
    assert!(plan_memory_grows_per_event(&hist), "history 可增长");

    let mut seq = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    seq.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![],
    });
    assert!(plan_memory_grows_per_event(&seq), "seq 可增长");
}
