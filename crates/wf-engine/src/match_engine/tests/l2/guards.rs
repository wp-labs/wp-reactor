use super::*;

// ---------------------------------------------------------------------------
// Compound expression context preservation — Issue #9
// ---------------------------------------------------------------------------

/// window.has() inside `a && window.has(...)` must work (not lose context).
#[test]
fn compound_expr_and_window_has() {
    use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan};

    // Guard: status == "fail" && threat_list.has(sip)
    let guard = Expr::BinOp {
        op: BinOp::And,
        left: Box::new(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("status".to_string()))),
            right: Box::new(Expr::StringLit("fail".to_string())),
        }),
        right: Box::new(Expr::FuncCall {
            qualifier: Some("threat_list".to_string()),
            name: "has".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("sip".to_string()))],
        }),
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: Some(guard),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }])],
    );

    let mut sm = CepStateMachine::new("compound_has".into(), plan, None);

    // Set up window lookup with threat_list containing "10.0.0.1"
    let mut lookup = MockWindowLookup::new();
    lookup.add_field_values("threat_list", "sip", vec!["10.0.0.1"]);

    // Event with status=fail and sip in threat_list → should match
    let e = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("status", str_val("fail")),
    ]);
    let result = sm.advance_with("fail", &e, Some(&lookup));
    assert!(
        matches!(result, StepResult::Matched(_)),
        "compound guard with has() should match; got {:?}",
        result
    );
}

/// window.has() inside `a && window.has(...)` with non-matching value.
#[test]
fn compound_expr_and_window_has_no_match() {
    use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan};

    let guard = Expr::BinOp {
        op: BinOp::And,
        left: Box::new(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("status".to_string()))),
            right: Box::new(Expr::StringLit("fail".to_string())),
        }),
        right: Box::new(Expr::FuncCall {
            qualifier: Some("threat_list".to_string()),
            name: "has".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("sip".to_string()))],
        }),
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: Some(guard),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }])],
    );

    let mut sm = CepStateMachine::new("compound_has_miss".into(), plan, None);

    let mut lookup = MockWindowLookup::new();
    lookup.add_field_values("threat_list", "sip", vec!["10.0.0.1"]);

    // sip NOT in threat_list → guard should fail
    let e = event(vec![
        ("sip", str_val("192.168.1.1")),
        ("status", str_val("fail")),
    ]);
    let result = sm.advance_with("fail", &e, Some(&lookup));
    assert!(
        matches!(result, StepResult::Accumulate),
        "non-matching has() in compound guard should not match; got {:?}",
        result
    );
}

/// baseline() inside comparison `baseline(...) > 3` must preserve baselines
/// context through the BinOp evaluation.
#[test]
fn compound_expr_baseline_in_comparison() {
    use std::collections::HashMap as Map;
    use wf_lang::ast::BinOp;

    // Expression: baseline(x, 300) > 2.0
    let expr = Expr::BinOp {
        op: BinOp::Gt,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "baseline".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("x".to_string())),
                Expr::Number(300.0),
            ],
        }),
        right: Box::new(Expr::Number(2.0)),
    };

    // Use eval_expr_ext directly with a baselines store to verify context flows
    use crate::match_engine::match_engine::eval_expr_ext;
    let mut baselines = Map::new();

    // Feed varying values to build baseline with nonzero stddev.
    // Alternating 45 and 55 gives mean=50, stddev=5.
    for i in 0..20 {
        let x = if i % 2 == 0 { 45.0 } else { 55.0 };
        let e = event(vec![("x", num(x))]);
        eval_expr_ext(&expr, &e, None, &mut baselines);
    }

    // Now test with outlier: (200 - 50) / 5 = 30 → definitely > 2.0
    let outlier = event(vec![("x", num(200.0))]);
    let result = eval_expr_ext(&expr, &outlier, None, &mut baselines);
    assert_eq!(
        result,
        Some(Value::Bool(true)),
        "baseline(200) on mean≈50 should be > 2.0"
    );
}

// ---------------------------------------------------------------------------
// window.has() with non-string fields — Issue #10
// ---------------------------------------------------------------------------

/// window.has() must work with numeric field values.
#[test]
fn window_has_numeric_field() {
    use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan};

    // Guard: threat_list.has(port)
    let guard = Expr::FuncCall {
        qualifier: Some("threat_list".to_string()),
        name: "has".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("port".to_string()))],
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: Some(guard),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }])],
    );

    let mut sm = CepStateMachine::new("has_numeric".into(), plan, None);

    // threat_list has port "22" as string (from snapshot_field_values)
    let mut lookup = MockWindowLookup::new();
    lookup.add_field_values("threat_list", "port", vec!["22"]);

    // Event port=22 as number → value_to_string gives "22"
    let e = event(vec![("sip", str_val("10.0.0.1")), ("port", num(22.0))]);
    let result = sm.advance_with("fail", &e, Some(&lookup));
    assert!(
        matches!(result, StepResult::Matched(_)),
        "has() with numeric field should match; got {:?}",
        result
    );
}

/// window.has() infers the column from a nested path's leaf member, not its root
/// object field — so `has(e.roles_obj.source.process.uid)` looks up the `uid`
/// column (wp-labs/warp-fusion#64).
#[test]
fn window_has_nested_path_infers_leaf_field() {
    use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure, PathSegment};
    use wf_lang::plan::{AggPlan, BranchPlan};

    let guard = Expr::FuncCall {
        qualifier: Some("threat_list".to_string()),
        name: "has".to_string(),
        args: vec![Expr::Field(FieldRef::Path {
            alias: "fail".to_string(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("source".into()),
                PathSegment::Field("process".into()),
                PathSegment::Field("uid".into()),
            ],
        })],
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: Some(guard),
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }])],
    );

    let mut sm = CepStateMachine::new("has_nested".into(), plan, None);

    // Window's "uid" column contains the nested value. If the column were
    // inferred from the path root ("roles_obj"), the lookup would miss and the
    // guard would fail.
    let mut lookup = MockWindowLookup::new();
    lookup.add_field_values(
        "threat_list",
        "uid",
        vec!["d22b3fbcb9e77cb86834f6a18e2e0f68"],
    );

    let e = event(vec![
        ("sip", str_val("10.0.0.1")),
        (
            "roles_obj",
            Value::Object(HashMap::from([(
                "source".to_string(),
                Value::Object(HashMap::from([(
                    "process".to_string(),
                    Value::Object(HashMap::from([(
                        "uid".to_string(),
                        str_val("d22b3fbcb9e77cb86834f6a18e2e0f68"),
                    )])),
                )])),
            )])),
        ),
    ]);
    let result = sm.advance_with("fail", &e, Some(&lookup));
    assert!(
        matches!(result, StepResult::Matched(_)),
        "window.has(nested path) must infer the leaf column 'uid'; got {:?}",
        result
    );
}
