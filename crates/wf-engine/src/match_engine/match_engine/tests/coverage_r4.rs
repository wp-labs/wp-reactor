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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
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
        }),
    );
    assert_eq!(sm.advance_at("e", &e, 0), StepResult::Accumulate);
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
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

impl crate::match_engine::match_engine::WindowLookup for KeyJoinLookup {
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
