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

// ===========================================================================
// replace / trim / mvcount / mvjoin / mvindex / mvappend / split / mvdedup
// ===========================================================================

#[test]
fn replace_regex_substitution() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "replace".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".to_string())),
            Expr::StringLit("fail.*".to_string()),
            Expr::StringLit("blocked".to_string()),
        ],
    };
    let mut fields = HashMap::new();
    fields.insert("action".to_string(), Value::Str("failed_login".to_string()));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Str("blocked".to_string()))
    );
}

#[test]
fn startswith_and_endswith_work() {
    use crate::rule::match_engine::{Event, eval_expr};

    let starts = Expr::FuncCall {
        qualifier: None,
        name: "startswith".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("failed".to_string()),
        ],
    };
    let ends = Expr::FuncCall {
        qualifier: None,
        name: "endswith".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("root".to_string()),
        ],
    };
    let mut fields = HashMap::new();
    fields.insert(
        "msg".to_string(),
        Value::Str("failed_login_root".to_string()),
    );
    let event = Event { fields };
    assert_eq!(eval_expr(&starts, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&ends, &event), Some(Value::Bool(true)));
}

#[test]
fn substr_supports_one_based_and_negative_start() {
    use crate::rule::match_engine::{Event, eval_expr};

    let mut fields = HashMap::new();
    fields.insert("msg".to_string(), Value::Str("abcdef".to_string()));
    let event = Event { fields };

    let one_based = Expr::FuncCall {
        qualifier: None,
        name: "substr".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::Number(2.0),
            Expr::Number(3.0),
        ],
    };
    assert_eq!(
        eval_expr(&one_based, &event),
        Some(Value::Str("bcd".to_string()))
    );

    let negative = Expr::FuncCall {
        qualifier: None,
        name: "substr".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::Neg(Box::new(Expr::Number(2.0))),
        ],
    };
    assert_eq!(
        eval_expr(&negative, &event),
        Some(Value::Str("ef".to_string()))
    );
}

#[test]
fn trim_removes_surrounding_whitespace() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "trim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let mut fields = HashMap::new();
    fields.insert("msg".to_string(), Value::Str("  hello\t".to_string()));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Str("hello".to_string()))
    );
}

#[test]
fn mvcount_array_returns_length() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvcount".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("vals".to_string()))],
    };
    let mut fields = HashMap::new();
    fields.insert(
        "vals".to_string(),
        Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
        ]),
    );
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(3.0)));
}

#[test]
fn mvjoin_array_with_separator() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvjoin".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::StringLit("|".to_string()),
        ],
    };
    let mut fields = HashMap::new();
    fields.insert(
        "vals".to_string(),
        Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
        ]),
    );
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Str("a|b|c".to_string()))
    );
}

#[test]
fn mvindex_single_and_range() {
    use crate::rule::match_engine::{Event, eval_expr};

    let mut fields = HashMap::new();
    fields.insert(
        "vals".to_string(),
        Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
            Value::Str("d".to_string()),
        ]),
    );
    let event = Event { fields };

    let single = Expr::FuncCall {
        qualifier: None,
        name: "mvindex".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::Neg(Box::new(Expr::Number(1.0))),
        ],
    };
    assert_eq!(
        eval_expr(&single, &event),
        Some(Value::Str("d".to_string()))
    );

    let range = Expr::FuncCall {
        qualifier: None,
        name: "mvindex".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::Number(1.0),
            Expr::Number(2.0),
        ],
    };
    assert_eq!(
        eval_expr(&range, &event),
        Some(Value::Array(vec![
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
        ]))
    );
}

#[test]
fn mvappend_flattens_arrays_and_scalars() {
    use crate::rule::match_engine::{Event, eval_expr};

    let mut fields = HashMap::new();
    fields.insert(
        "vals".to_string(),
        Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
        ]),
    );
    let event = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvappend".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::StringLit("c".to_string()),
            Expr::FuncCall {
                qualifier: None,
                name: "split".to_string(),
                args: vec![
                    Expr::StringLit("d,e".to_string()),
                    Expr::StringLit(",".to_string()),
                ],
            },
        ],
    };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
            Value::Str("d".to_string()),
            Value::Str("e".to_string()),
        ]))
    );
}

#[test]
fn split_text_to_array() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "split".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("csv".to_string())),
            Expr::StringLit(",".to_string()),
        ],
    };
    let mut fields = HashMap::new();
    fields.insert("csv".to_string(), Value::Str("a,b,,c".to_string()));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str(String::new()),
            Value::Str("c".to_string()),
        ]))
    );
}

#[test]
fn mvdedup_removes_duplicates_keep_order() {
    use crate::rule::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvdedup".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("vals".to_string()))],
    };
    let mut fields = HashMap::new();
    fields.insert(
        "vals".to_string(),
        Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("a".to_string()),
            Value::Str("c".to_string()),
            Value::Str("b".to_string()),
        ]),
    );
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
        ]))
    );
}
