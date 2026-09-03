// `on event<accu>` — within-window accumulation (wp-labs/warp-fusion#65).
//
// After the block fires, count and evidence keep accumulating without reset,
// and each subsequent qualifying event re-fires with the running cumulative
// values until the window expires.

use std::time::Duration;

use wf_lang::plan::{MatchPlan, WindowSpec};

use crate::match_engine::cep::{CepStateMachine, StepResult};

use super::helpers::*;

fn accu_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("s", count_ge(2.0))])],
    );
    plan.accu = true;
    plan
}

/// Drive `count` events through the machine, returning `(match_count, measures, window_starts)`.
fn drive(plan: MatchPlan, n_events: u32, ts_step_nanos: i64) -> (usize, Vec<f64>, Vec<i64>) {
    let mut sm = CepStateMachine::new("accu".into(), plan, None);
    let mut matches = Vec::new();
    let mut window_starts = Vec::new();
    for i in 0..n_events {
        let ts = (i as i64 + 1) * ts_step_nanos;
        let step = sm.advance_at("s", &event(vec![("sip", str_val("10.0.0.1"))]), ts);
        if let StepResult::Matched(ctx) = step {
            matches.push(ctx.step_data[0].measure_value);
            window_starts.push(ctx.window_start_time_nanos);
        }
    }
    (matches.len(), matches, window_starts)
}

#[test]
fn accu_fires_every_subsequent_event_with_running_count() {
    // 5 events, threshold 2: fires on 2,3,4,5 with running counts.
    let (n, counts, window_starts) = drive(accu_plan(), 5, 1_000_000_000);
    assert_eq!(
        counts,
        vec![2.0, 3.0, 4.0, 5.0],
        "running cumulative counts"
    );
    assert_eq!(n, 4);
    // Same window start across fires — accumulation, not a fresh window.
    assert!(
        window_starts.windows(2).all(|w| w[0] == w[1]),
        "window must persist across accu fires: {window_starts:?}"
    );
}

#[test]
fn default_resets_after_each_fire() {
    // Non-accu: fires on 2 and 4 (each crossing resets the count to a new window).
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("s", count_ge(2.0))])],
    );
    let (n, counts, window_starts) = drive(plan, 5, 1_000_000_000);
    assert_eq!(counts, vec![2.0, 2.0], "reset after each fire");
    assert_eq!(n, 2);
    assert_ne!(
        window_starts[0], window_starts[1],
        "fresh window after reset"
    );
}

#[test]
fn accu_restarts_after_window_expiry() {
    // 10s window: events at 1s/2s fire (count 2), then a gap > 10s expires the
    // instance, and the next two events fire again from a fresh window.
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("s", count_ge(2.0))])],
    );
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(10));
    plan.accu = true;

    let mut sm = CepStateMachine::new("accu".into(), plan, None);
    let mut counts = Vec::new();
    let mut window_starts = Vec::new();

    // Burst 1: events at 1s, 2s → fire count 2.
    for ts in [1_000_000_000, 2_000_000_000] {
        if let StepResult::Matched(ctx) =
            sm.advance_at("s", &event(vec![("sip", str_val("10.0.0.1"))]), ts)
        {
            counts.push(ctx.step_data[0].measure_value);
            window_starts.push(ctx.window_start_time_nanos);
        }
    }
    // Idle past the 10s window, then burst 2 at 30s/31s → fresh window, count 2.
    // Expiry is watermark-driven, so scan it explicitly.
    sm.scan_expired_at(15_000_000_000);
    for ts in [30_000_000_000, 31_000_000_000] {
        if let StepResult::Matched(ctx) =
            sm.advance_at("s", &event(vec![("sip", str_val("10.0.0.1"))]), ts)
        {
            counts.push(ctx.step_data[0].measure_value);
            window_starts.push(ctx.window_start_time_nanos);
        }
    }

    assert_eq!(counts, vec![2.0, 2.0], "each burst fires count 2");
    assert_ne!(
        window_starts[0], window_starts[1],
        "window restarted after expiry"
    );
}

#[test]
fn accu_with_distinct_does_not_refire_on_duplicates() {
    // A `distinct` transform on a field value: duplicate values are filtered
    // out, so they neither count nor re-fire.
    use wf_lang::ast::{CmpOp, Expr, FieldSelector, Measure, Transform};
    use wf_lang::plan::{AggPlan, BranchPlan};

    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "s".to_string(),
            field: Some(FieldSelector::Dot("event_id".to_string())),
            guard: None,
            agg: AggPlan {
                transforms: vec![Transform::Distinct],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(2.0),
            },
        }])],
    );
    plan.accu = true;

    let mut sm = CepStateMachine::new("accu".into(), plan, None);
    let mut counts = Vec::new();
    for (i, event_id) in ["e1", "e2", "e2", "e3"].iter().enumerate() {
        let ts = (i as i64 + 1) * 1_000_000_000;
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", str_val(event_id)),
            ]),
            ts,
        );
        if let StepResult::Matched(ctx) = step {
            counts.push(ctx.step_data[0].measure_value);
        }
    }
    // distinct count: e1,e2 = 2 (fire), e2 duplicate filtered, e3 = 3 (fire).
    assert_eq!(counts, vec![2.0, 3.0], "duplicate must not re-fire");
}

#[test]
fn accu_with_any_single_step_fires_every_subsequent_event() {
    // `on event<accu> any { ... }` with a single step: equivalent to seq.
    use wf_lang::ast::MatchMode;

    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("s", count_ge(2.0))])],
    );
    plan.match_mode = MatchMode::Any;
    plan.accu = true;

    let (n, counts, window_starts) = drive(plan, 5, 1_000_000_000);
    assert_eq!(counts, vec![2.0, 3.0, 4.0, 5.0]);
    assert_eq!(n, 4);
    assert!(
        window_starts.windows(2).all(|w| w[0] == w[1]),
        "any+accu must keep the same window: {window_starts:?}"
    );
}

#[test]
fn accu_guard_filtered_events_do_not_refire() {
    // A guard-filtered event neither increments the count nor re-fires.
    use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan};

    let guard = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("action".into()))),
        right: Box::new(Expr::StringLit("fail".into())),
    };
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "s".to_string(),
            field: None,
            guard: Some(guard),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(2.0),
            },
        }])],
    );
    plan.accu = true;

    let mut sm = CepStateMachine::new("accu".into(), plan, None);
    let mut counts = Vec::new();
    // fail, fail → fire count 2; ok (filtered, no count); fail → fire count 3.
    for (i, action) in ["fail", "fail", "ok", "fail"].iter().enumerate() {
        let ts = (i as i64 + 1) * 1_000_000_000;
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("action", str_val(action)),
            ]),
            ts,
        );
        if let StepResult::Matched(ctx) = step {
            counts.push(ctx.step_data[0].measure_value);
        }
    }
    assert_eq!(
        counts,
        vec![2.0, 3.0],
        "guard-filtered event must not re-fire"
    );
}

#[test]
fn accu_throttle_suppresses_alert_but_keeps_accumulation() {
    // max_throttle = 1 per 100s, on_exceed = throttle. The re-fires after the
    // first alert are suppressed, but the running count must survive them and
    // show up on the next allowed fire (not a reset count).
    use std::time::Duration;
    use wf_lang::plan::{ExceedAction, LimitsPlan, RateSpec};

    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("s", count_ge(2.0))])],
    );
    plan.accu = true;
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(100),
        }),
        on_exceed: ExceedAction::Throttle,
        disk_provider: None,
        max_disk_bytes: None,
    };
    let mut sm = CepStateMachine::with_limits("accu".into(), plan, None, Some(limits));

    let mut measures = Vec::new();
    // Events at 1s..5s: evt_2 fires (count 2); evt_3..5 are throttled re-fires.
    for i in 0..5 {
        let ts = (i as i64 + 1) * 1_000_000_000;
        if let StepResult::Matched(ctx) =
            sm.advance_at("s", &event(vec![("sip", str_val("10.0.0.1"))]), ts)
        {
            measures.push(ctx.step_data[0].measure_value);
        }
    }
    assert_eq!(
        measures,
        vec![2.0],
        "re-fires within the throttle window are suppressed"
    );

    // Past the 100s throttle window the next fire is allowed and must show the
    // RUNNING count (accumulation survived the throttled re-fires), not a reset.
    if let StepResult::Matched(ctx) = sm.advance_at(
        "s",
        &event(vec![("sip", str_val("10.0.0.1"))]),
        200_000_000_000,
    ) {
        measures.push(ctx.step_data[0].measure_value);
    }
    assert_eq!(
        measures,
        vec![2.0, 6.0],
        "accumulation must survive throttled re-fires"
    );
}

#[test]
fn accu_fixed_window_accumulates_within_bucket() {
    // Fixed windows: accu accumulates within the bucket and restarts after it.
    use wf_lang::plan::WindowSpec;

    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("s", count_ge(2.0))])],
    );
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(10));
    plan.accu = true;

    let mut sm = CepStateMachine::new("accu".into(), plan, None);
    let mut counts = Vec::new();
    // Events 1s..5s within the fixed 10s bucket: fires 2,3,4,5.
    for i in 0..5 {
        let ts = (i as i64 + 1) * 1_000_000_000;
        if let StepResult::Matched(ctx) =
            sm.advance_at("s", &event(vec![("sip", str_val("10.0.0.1"))]), ts)
        {
            counts.push(ctx.step_data[0].measure_value);
        }
    }
    assert_eq!(
        counts,
        vec![2.0, 3.0, 4.0, 5.0],
        "accumulate within a fixed bucket"
    );
}

#[test]
fn accu_instances_accumulate_independently() {
    // Different scope keys (sip) are separate instances with independent counts.
    let plan = accu_plan();
    let mut sm = CepStateMachine::new("accu".into(), plan, None);

    let mut a_counts = Vec::new();
    let mut b_counts = Vec::new();
    // a: 3 events (fires at 2,3); b: 2 events (fires at 2).
    for (i, (alias, sip)) in [
        ("s", "10.0.0.1"),
        ("s", "10.0.0.1"),
        ("s", "10.0.0.2"),
        ("s", "10.0.0.1"),
        ("s", "10.0.0.2"),
    ]
    .iter()
    .enumerate()
    {
        let ts = (i as i64 + 1) * 1_000_000_000;
        if let StepResult::Matched(ctx) =
            sm.advance_at(alias, &event(vec![("sip", str_val(sip))]), ts)
        {
            let count = ctx.step_data[0].measure_value;
            if *sip == "10.0.0.1" {
                a_counts.push(count);
            } else {
                b_counts.push(count);
            }
        }
    }
    assert_eq!(
        a_counts,
        vec![2.0, 3.0],
        "instance A accumulates independently"
    );
    assert_eq!(b_counts, vec![2.0], "instance B accumulates independently");
}
