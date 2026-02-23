//! L2 feature tests: limits, key_map, window.has(), baseline(), joins.

use std::collections::{HashMap, HashSet};

use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{
    ExceedAction, JoinCondPlan, JoinPlan, KeyMapPlan, LimitsPlan, MatchPlan, WindowSpec,
};

use crate::rule::RuleExecutor;
use crate::rule::match_engine::{
    CepStateMachine, MatchedContext, StepData, StepResult, Value, WindowLookup,
};

use super::helpers::*;

// ---------------------------------------------------------------------------
// Mock WindowLookup
// ---------------------------------------------------------------------------

struct MockWindowLookup {
    field_values: HashMap<(String, String), HashSet<String>>,
    snapshots: HashMap<String, Vec<HashMap<String, Value>>>,
}

impl MockWindowLookup {
    fn new() -> Self {
        Self {
            field_values: HashMap::new(),
            snapshots: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn add_field_values(&mut self, window: &str, field: &str, values: Vec<&str>) {
        self.field_values.insert(
            (window.to_string(), field.to_string()),
            values.into_iter().map(|s| s.to_string()).collect(),
        );
    }

    fn add_snapshot(&mut self, window: &str, rows: Vec<HashMap<String, Value>>) {
        self.snapshots.insert(window.to_string(), rows);
    }
}

impl WindowLookup for MockWindowLookup {
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>> {
        self.field_values
            .get(&(window.to_string(), field.to_string()))
            .cloned()
    }

    fn snapshot(&self, window: &str) -> Option<Vec<HashMap<String, Value>>> {
        self.snapshots.get(window).cloned()
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build a HashMap row from key-value pairs.
fn row(fields: Vec<(&str, Value)>) -> HashMap<String, Value> {
    fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

/// Build a simple snapshot JoinPlan: `join <window> snapshot on left == right`.
fn snapshot_join(window: &str, left_field: &str, right_field: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode: wf_lang::ast::JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left_field.to_string()),
            right: FieldRef::Simple(right_field.to_string()),
        }],
    }
}

// ===========================================================================
// Limits: max_cardinality with Throttle
// ===========================================================================

#[test]
fn limits_max_cardinality_throttle() {
    // Use count >= 2 so instances stay alive after the first event
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: None,
        max_cardinality: Some(2),
        max_emit_rate: None,
        on_exceed: ExceedAction::Throttle,
    };
    let mut sm = CepStateMachine::with_limits("rule_lim".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // First two keys create instances
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Third key is throttled — max_cardinality reached
    assert_eq!(sm.advance("fail", &e3), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Existing keys still advance normally
    assert!(matches!(sm.advance("fail", &e1), StepResult::Matched(_)));
}

// ===========================================================================
// Limits: max_cardinality with DropOldest
// ===========================================================================

#[test]
fn limits_max_cardinality_drop_oldest() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let limits = LimitsPlan {
        max_state_bytes: None,
        max_cardinality: Some(2),
        max_emit_rate: None,
        on_exceed: ExceedAction::DropOldest,
    };
    let mut sm = CepStateMachine::with_limits("rule_lim".to_string(), plan, None, Some(limits));

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    // Create 2 instances at different timestamps
    assert_eq!(sm.advance_at("fail", &e1, 100), StepResult::Accumulate);
    assert_eq!(sm.advance_at("fail", &e2, 200), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // Third key evicts the oldest (10.0.0.1, created at t=100)
    assert_eq!(sm.advance_at("fail", &e3, 300), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);

    // 10.0.0.1 was evicted — re-inserting it evicts the next oldest (10.0.0.2)
    assert_eq!(sm.advance_at("fail", &e1, 400), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 2);
}

// ===========================================================================
// Key mapping: extract_key with key_map
// ===========================================================================

#[test]
fn key_map_extracts_from_alias_field() {
    use std::time::Duration;

    let key_map = vec![
        KeyMapPlan {
            logical_name: "ip".to_string(),
            source_alias: "login".to_string(),
            source_field: "src_ip".to_string(),
        },
        KeyMapPlan {
            logical_name: "ip".to_string(),
            source_alias: "dns".to_string(),
            source_field: "client_ip".to_string(),
        },
    ];

    let plan = MatchPlan {
        keys: vec![FieldRef::Simple("ip".to_string())],
        key_map: Some(key_map),
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: vec![step(vec![
            branch("login", count_ge(1.0)),
            branch("dns", count_ge(1.0)),
        ])],
        close_steps: vec![],
    };

    let mut sm = CepStateMachine::new("rule_km".to_string(), plan, None);

    // "login" event with "src_ip" field — should extract key from src_ip
    let e1 = event(vec![("src_ip", str_val("10.0.0.1"))]);
    let result = sm.advance("login", &e1);
    assert!(matches!(result, StepResult::Matched(_)));
    if let StepResult::Matched(ctx) = result {
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
    }
}

// ===========================================================================
// Join: snapshot mode enriches eval context
// ===========================================================================

#[test]
fn join_snapshot_enriches_context() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join",
        match_plan,
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("geo_lookup", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "geo_lookup",
        vec![row(vec![
            ("ip", str_val("10.0.0.1")),
            ("country", str_val("US")),
            ("city", str_val("NYC")),
        ])],
    );

    let matched = MatchedContext {
        rule_name: "r_join".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert_eq!(alert.rule_name, "r_join");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Join: entity from joined field
// ===========================================================================

#[test]
fn join_entity_from_joined_field() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join_entity",
        match_plan,
        Expr::Number(80.0),
        "host",
        // entity_id uses "hostname" which comes from the join
        Expr::Field(FieldRef::Simple("hostname".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("asset_db", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "asset_db",
        vec![row(vec![
            ("ip", str_val("10.0.0.1")),
            ("hostname", str_val("web-server-01")),
        ])],
    );

    let matched = MatchedContext {
        rule_name: "r_join_entity".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert_eq!(alert.entity_id, "web-server-01");
}

// ===========================================================================
// Join: no matching row — entity falls back to non-joined fields
// ===========================================================================

#[test]
fn join_no_match_falls_through() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join_norow",
        match_plan,
        Expr::Number(50.0),
        "ip",
        // entity_id uses "sip" which exists in eval context from keys
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("asset_db", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    // Window has rows but none match "10.0.0.1"
    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "asset_db",
        vec![row(vec![
            ("ip", str_val("192.168.1.1")),
            ("hostname", str_val("other-host")),
        ])],
    );

    let matched = MatchedContext {
        rule_name: "r_join_norow".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    // No join match — entity falls back to "sip" from keys
    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");
}

// ===========================================================================
// Join: close with joins
// ===========================================================================

#[test]
fn join_close_with_joins() {
    use crate::rule::match_engine::{CloseOutput, CloseReason};

    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join_close",
        match_plan,
        Expr::Number(60.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("asset_db", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "asset_db",
        vec![row(vec![("ip", str_val("10.0.0.1")), ("risk", num(95.0))])],
    );

    let close = CloseOutput {
        rule_name: "r_join_close".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 3.0,
        }],
        close_step_data: vec![],
        watermark_nanos: 0,
    };

    let alert = exec.execute_close_with_joins(&close, &wl).unwrap().unwrap();
    assert_eq!(alert.close_reason.as_deref(), Some("timeout"));
    assert!((alert.score - 60.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Limits: no limits plan → unlimited instances
// ===========================================================================

#[test]
fn no_limits_allows_unlimited_instances() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("rule_nolim".to_string(), plan, None);

    // Create 100 different keys
    for i in 0..100 {
        let e = event(vec![("sip", str_val(&format!("10.0.0.{}", i)))]);
        sm.advance("fail", &e);
    }
    assert_eq!(sm.instance_count(), 100);
}

// ===========================================================================
// Execute match without joins (backward compat)
// ===========================================================================

#[test]
fn execute_match_without_joins_still_works() {
    let plan = simple_rule_plan(
        "r_compat",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r_compat".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    // Old API still works
    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert!((alert.score - 50.0).abs() < f64::EPSILON);
}

// ===========================================================================
// IfThenElse expression evaluation
// ===========================================================================

#[test]
fn if_then_else_true_branch() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::Number(80.0)),
        else_expr: Box::new(Expr::Number(40.0)),
    };
    let event = Event {
        fields: HashMap::new(),
    };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(80.0)));
}

#[test]
fn if_then_else_false_branch() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(false)),
        then_expr: Box::new(Expr::Number(80.0)),
        else_expr: Box::new(Expr::Number(40.0)),
    };
    let event = Event {
        fields: HashMap::new(),
    };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(40.0)));
}

#[test]
fn if_then_else_nested() {
    use crate::rule::match_engine::{Event, eval_expr};

    // if true then (if false then 1 else 2) else 3
    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::IfThenElse {
            cond: Box::new(Expr::Bool(false)),
            then_expr: Box::new(Expr::Number(1.0)),
            else_expr: Box::new(Expr::Number(2.0)),
        }),
        else_expr: Box::new(Expr::Number(3.0)),
    };
    let event = Event {
        fields: HashMap::new(),
    };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(2.0)));
}

#[test]
fn if_then_else_with_field_condition() {
    use crate::rule::match_engine::{Event, eval_expr};

    // if action == "failed" then 80 else 40
    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
            right: Box::new(Expr::StringLit("failed".to_string())),
        }),
        then_expr: Box::new(Expr::Number(80.0)),
        else_expr: Box::new(Expr::Number(40.0)),
    };

    let mut fields = HashMap::new();
    fields.insert("action".to_string(), Value::Str("failed".to_string()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(80.0)));

    let mut fields2 = HashMap::new();
    fields2.insert("action".to_string(), Value::Str("success".to_string()));
    let event2 = Event { fields: fields2 };
    assert_eq!(eval_expr(&expr, &event2), Some(Value::Number(40.0)));
}

// ===========================================================================
// regex_match
// ===========================================================================

#[test]
fn regex_match_matches() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".to_string())),
            Expr::StringLit("fail.*".to_string()),
        ],
    };
    let mut fields = HashMap::new();
    fields.insert("action".to_string(), Value::Str("failed_login".to_string()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Bool(true)));
}

#[test]
fn regex_match_no_match() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".to_string())),
            Expr::StringLit("^success$".to_string()),
        ],
    };
    let mut fields = HashMap::new();
    fields.insert("action".to_string(), Value::Str("failed".to_string()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Bool(false)));
}

// ===========================================================================
// time_diff
// ===========================================================================

#[test]
fn time_diff_returns_seconds() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_diff".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("t1".to_string())),
            Expr::Field(FieldRef::Simple("t2".to_string())),
        ],
    };
    let mut fields = HashMap::new();
    // 5 seconds apart in nanos
    fields.insert("t1".to_string(), Value::Number(10_000_000_000.0)); // 10s in nanos
    fields.insert("t2".to_string(), Value::Number(5_000_000_000.0)); // 5s in nanos
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(5.0)));
}

#[test]
fn time_diff_absolute_value() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_diff".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("t1".to_string())),
            Expr::Field(FieldRef::Simple("t2".to_string())),
        ],
    };
    let mut fields = HashMap::new();
    // Reversed order: t1 < t2
    fields.insert("t1".to_string(), Value::Number(5_000_000_000.0));
    fields.insert("t2".to_string(), Value::Number(10_000_000_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(5.0)));
}

// ===========================================================================
// time_bucket
// ===========================================================================

#[test]
fn time_bucket_floors_to_interval() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_bucket".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0), // 60 second interval
        ],
    };
    let mut fields = HashMap::new();
    // 75 seconds in nanos
    fields.insert("ts".to_string(), Value::Number(75_000_000_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    // 75s / 60s = 1.25 → floor = 1 → 60s in nanos
    assert_eq!(result, Some(Value::Number(60_000_000_000.0)));
}

#[test]
fn time_bucket_exact_boundary() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_bucket".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(300.0), // 5 minute interval
        ],
    };
    let mut fields = HashMap::new();
    // Exactly 600 seconds in nanos (2 * 300s)
    fields.insert("ts".to_string(), Value::Number(600_000_000_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    // Should stay at 600s
    assert_eq!(result, Some(Value::Number(600_000_000_000.0)));
}
