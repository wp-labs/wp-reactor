use super::*;

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
