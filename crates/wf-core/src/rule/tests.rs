use std::time::Duration;

use wf_lang::ast::{CmpOp, Expr, FieldRef, FieldSelector, Measure, Transform};
use wf_lang::plan::{AggPlan, BranchPlan, MatchPlan, StepPlan, WindowSpec};

use super::match_engine::{CepStateMachine, Event, StepResult, Value};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string())
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

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: steps,
        close_steps: vec![],
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

// ---------------------------------------------------------------------------
// Test 1: single_step_threshold
// ---------------------------------------------------------------------------

#[test]
fn single_step_threshold() {
    // 3 events → Accumulate, Accumulate, Matched
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("rule1".to_string(), plan);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);

    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.rule_name, "rule1");
        assert_eq!(ctx.scope_key, vec!["10.0.0.1"]);
        assert_eq!(ctx.step_data.len(), 1);
        assert_eq!(ctx.step_data[0].measure_value, 3.0);
    } else {
        panic!("expected Matched");
    }
}

// ---------------------------------------------------------------------------
// Test 2: multi_step_sequential
// ---------------------------------------------------------------------------

#[test]
fn multi_step_sequential() {
    // step2 events before step1 don't match; step1 done → Advance; step2 done → Matched
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("fail", count_ge(2.0))]),
            step(vec![branch("scan", count_ge(1.0))]),
        ],
    );
    let mut sm = CepStateMachine::new("rule2".to_string(), plan);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // scan event before step1 is done — should accumulate (wrong step)
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);

    // first fail event — accumulate
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    // second fail event — step1 satisfied → Advance
    assert_eq!(sm.advance("fail", &e), StepResult::Advance);

    // now scan should match step2
    if let StepResult::Matched(ctx) = sm.advance("scan", &e) {
        assert_eq!(ctx.step_data.len(), 2);
    } else {
        panic!("expected Matched");
    }
}

// ---------------------------------------------------------------------------
// Test 3: or_branch_first_wins
// ---------------------------------------------------------------------------

#[test]
fn or_branch_first_wins() {
    // Two branches in one step; branch 0 completes first
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![
            branch("fail", count_ge(2.0)),  // branch 0
            branch("error", count_ge(1.0)), // branch 1
        ])],
    );
    let mut sm = CepStateMachine::new("rule3".to_string(), plan);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.step_data[0].satisfied_branch_index, 0);
    } else {
        panic!("expected Matched");
    }
}

// ---------------------------------------------------------------------------
// Test 4: composite_key_isolation
// ---------------------------------------------------------------------------

#[test]
fn composite_key_isolation() {
    let plan = simple_plan(
        vec![simple_key("sip"), simple_key("dport")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("rule4".to_string(), plan);

    let e1 = event(vec![("sip", str_val("10.0.0.1")), ("dport", num(22.0))]);
    let e2 = event(vec![("sip", str_val("10.0.0.1")), ("dport", num(80.0))]);

    // Two events to each key — both accumulate (threshold=3)
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate); // key1 count=1
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate); // key2 count=1
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate); // key1 count=2
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate); // key2 count=2
    assert_eq!(sm.instance_count(), 2);

    // Third event to key1 → matched
    if let StepResult::Matched(ctx) = sm.advance("fail", &e1) {
        assert_eq!(ctx.scope_key, vec!["10.0.0.1", "22"]);
    } else {
        panic!("expected Matched for key1");
    }

    // key2 still needs one more
    if let StepResult::Matched(ctx) = sm.advance("fail", &e2) {
        assert_eq!(ctx.scope_key, vec!["10.0.0.1", "80"]);
    } else {
        panic!("expected Matched for key2");
    }
}

// ---------------------------------------------------------------------------
// Test 5: guard_filter_skips
// ---------------------------------------------------------------------------

#[test]
fn guard_filter_skips() {
    // events not matching `action == "failed"` don't count
    let guard = Expr::BinOp {
        op: wf_lang::ast::BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        right: Box::new(Expr::StringLit("failed".to_string())),
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(2.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule5".to_string(), plan);

    let ok_event = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("success")),
    ]);
    let fail_event = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);

    // success events don't count
    assert_eq!(sm.advance("auth", &ok_event), StepResult::Accumulate);
    assert_eq!(sm.advance("auth", &ok_event), StepResult::Accumulate);

    // first failed event → accumulate
    assert_eq!(sm.advance("auth", &fail_event), StepResult::Accumulate);
    // second failed event → matched
    assert!(matches!(sm.advance("auth", &fail_event), StepResult::Matched(_)));
}

// ---------------------------------------------------------------------------
// Test 6: distinct_transform
// ---------------------------------------------------------------------------

#[test]
fn distinct_transform() {
    // duplicate dport values not counted; 3 unique > 2 → Matched
    let agg = AggPlan {
        transforms: vec![Transform::Distinct],
        measure: Measure::Count,
        cmp: CmpOp::Gt,
        threshold: Expr::Number(2.0),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "conn".to_string(),
            field: Some(FieldSelector::Dot("dport".to_string())),
            guard: None,
            agg,
        }])],
    );
    let mut sm = CepStateMachine::new("rule6".to_string(), plan);

    let mk = |port: f64| {
        event(vec![
            ("sip", str_val("10.0.0.1")),
            ("dport", num(port)),
        ])
    };

    // port 22 twice — only counted once
    assert_eq!(sm.advance("conn", &mk(22.0)), StepResult::Accumulate);
    assert_eq!(sm.advance("conn", &mk(22.0)), StepResult::Accumulate); // dup, still count=1

    // port 80 — count=2
    assert_eq!(sm.advance("conn", &mk(80.0)), StepResult::Accumulate);

    // port 443 — count=3 > 2 → Matched
    assert!(matches!(sm.advance("conn", &mk(443.0)), StepResult::Matched(_)));
}

// ---------------------------------------------------------------------------
// Test 7: source_matching
// ---------------------------------------------------------------------------

#[test]
fn source_matching() {
    // events with wrong alias don't contribute to branch
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule7".to_string(), plan);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // wrong alias
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);

    // correct alias
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));
}

// ---------------------------------------------------------------------------
// Test 8: no_key_match (empty keys → shared instance)
// ---------------------------------------------------------------------------

#[test]
fn no_key_match() {
    // all events share one instance
    let plan = simple_plan(vec![], vec![step(vec![branch("alert", count_ge(3.0))])]);
    let mut sm = CepStateMachine::new("rule8".to_string(), plan);

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    assert_eq!(sm.advance("alert", &e1), StepResult::Accumulate);
    assert_eq!(sm.advance("alert", &e2), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1); // all in one instance

    assert!(matches!(sm.advance("alert", &e3), StepResult::Matched(_)));
}

// ---------------------------------------------------------------------------
// Test 9: sum_measure
// ---------------------------------------------------------------------------

#[test]
fn sum_measure() {
    // sum(bytes) reaches threshold
    let agg = AggPlan {
        transforms: vec![],
        measure: Measure::Sum,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(1000.0),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "traffic".to_string(),
            field: Some(FieldSelector::Dot("bytes".to_string())),
            guard: None,
            agg,
        }])],
    );
    let mut sm = CepStateMachine::new("rule9".to_string(), plan);

    let mk = |bytes: f64| {
        event(vec![
            ("sip", str_val("10.0.0.1")),
            ("bytes", num(bytes)),
        ])
    };

    assert_eq!(sm.advance("traffic", &mk(400.0)), StepResult::Accumulate); // sum=400
    assert_eq!(sm.advance("traffic", &mk(500.0)), StepResult::Accumulate); // sum=900

    if let StepResult::Matched(ctx) = sm.advance("traffic", &mk(200.0)) {
        // sum=1100
        assert!((ctx.step_data[0].measure_value - 1100.0).abs() < f64::EPSILON);
    } else {
        panic!("expected Matched");
    }
}

// ---------------------------------------------------------------------------
// Test 10: missing_key_skips
// ---------------------------------------------------------------------------

#[test]
fn missing_key_skips() {
    // event without key field → Accumulate (skipped)
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("rule10".to_string(), plan);

    // event missing "sip" field
    let e_no_key = event(vec![("dport", num(22.0))]);
    assert_eq!(sm.advance("fail", &e_no_key), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0); // no instance created

    // event with "sip" field → should match immediately (count >= 1)
    let e_ok = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance("fail", &e_ok), StepResult::Matched(_)));
}

// ---------------------------------------------------------------------------
// Test 11: instance_resets_after_match
// ---------------------------------------------------------------------------

#[test]
fn instance_resets_after_match() {
    // same key can match again after reset
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule11".to_string(), plan);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First match
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // Second match — instance was reset, counts from zero again
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.rule_name, "rule11");
        assert_eq!(ctx.step_data[0].measure_value, 2.0);
    } else {
        panic!("expected second Matched");
    }
}
