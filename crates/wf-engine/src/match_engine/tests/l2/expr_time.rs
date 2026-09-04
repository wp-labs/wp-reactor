//! L2 表达式求值——时间与数值内建（2026-09-04 自 expr.rs 拆出；`#[path]` 兄弟子模块，
//! 共享 import 在父文件 expr.rs，此处经 `use super::*` 复用）。主题：time_diff 秒差、
//! time_bucket / bucket_end 分桶（边界与非法区间拒绝）、数值·strftime 内建杂项
//! （abs/ceil/floor/round/sqrt/pow/log/exp/clamp/sign/trunc/is_finite + ltrim/rtrim/
//! concat/indexof/replace_plain/startswith_any/endswith_any/coalesce/isnull/mvsort/
//! mvreverse）、now / now_s / now_ms / now_us / now_ns（同表达式内共享时间戳）。

use super::*;

// ===========================================================================
// time_diff
// ===========================================================================

#[test]
fn time_diff_returns_seconds() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_diff".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("t1".to_string())),
            Expr::Field(FieldRef::Simple("t2".to_string())),
        ],
    };
    let mut fields = EngineHashMap::default();
    // 5 seconds apart in epoch milliseconds.
    fields.insert("t1".into(), Value::Number(1_700_000_005_000.0));
    fields.insert("t2".into(), Value::Number(1_700_000_000_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(5.0)));
}

#[test]
fn time_diff_absolute_value() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_diff".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("t1".to_string())),
            Expr::Field(FieldRef::Simple("t2".to_string())),
        ],
    };
    let mut fields = EngineHashMap::default();
    // Reversed order: t1 < t2.
    fields.insert("t1".into(), Value::Number(1_700_000_000_000.0));
    fields.insert("t2".into(), Value::Number(1_700_000_005_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(5.0)));
}

// ===========================================================================
// time_bucket
// ===========================================================================

#[test]
fn time_bucket_floors_to_interval() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_bucket".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0), // 60 second interval
        ],
    };
    let mut fields = EngineHashMap::default();
    // 75 seconds after an epoch millisecond timestamp.
    fields.insert("ts".into(), Value::Number(1_700_000_075_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(1_700_000_040_000.0)));
}

#[test]
fn time_bucket_exact_boundary() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_bucket".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(300.0), // 5 minute interval
        ],
    };
    let mut fields = EngineHashMap::default();
    // Exact 5-minute bucket boundary in epoch milliseconds.
    fields.insert("ts".into(), Value::Number(1_700_000_100_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(1_700_000_100_000.0)));
}

// ===========================================================================
// bucket_end（P2 join within 内建；Q8 形态上开桶）
// ===========================================================================

/// `bucket_end(t, 60s)` = 桶末 = `time_bucket(t) + interval`。
#[test]
fn bucket_end_returns_bucket_upper_edge() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "bucket_end".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0),
        ],
    };
    let mut fields = EngineHashMap::default();
    // ts = 75s（epoch ms）→ 60s 桶 [1_700_000_040_000, 1_700_000_100_000)，桶末 = 1_700_000_100_000
    fields.insert("ts".into(), Value::Number(1_700_000_075_000.0));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Number(1_700_000_100_000.0))
    );
}

/// 恰在桶边界：t = 1_700_000_040_000（60s 桶界）→ 桶末 = 1_700_000_100_000（移入下桶）。
#[test]
fn bucket_end_at_exact_boundary_moves_to_next_bucket() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "bucket_end".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("ts".into(), Value::Number(1_700_000_040_000.0));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Number(1_700_000_100_000.0))
    );
}

#[test]
fn time_bucket_rejects_non_positive_or_non_finite_interval() {
    use crate::match_engine::cep::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
    };
    for interval in [0.0, -60.0, f64::INFINITY, f64::NAN] {
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "time_bucket".to_string(),
            args: vec![Expr::Number(1_700_000_075_000.0), Expr::Number(interval)],
        };
        assert_eq!(eval_expr(&expr, &event), None);
    }
}

// ===========================================================================
// abs / round / ceil / floor / strftime / strptime
// ===========================================================================

#[test]
fn math_functions_work() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut fields = EngineHashMap::default();
    fields.insert("n".into(), Value::Number(-12.345));
    fields.insert("p".into(), Value::Number(16.0));
    fields.insert("ts".into(), Value::Number(0.0));
    fields.insert("msg".into(), Value::Str("  failed_login_root  ".into()));
    fields.insert(
        "arr".into(),
        Value::Array(vec![
            Value::Str("b".into()),
            Value::Str("a".into()),
            Value::Str("c".into()),
        ]),
    );
    let event = Event { fields };

    let abs_expr = Expr::FuncCall {
        qualifier: None,
        name: "abs".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let ceil_expr = Expr::FuncCall {
        qualifier: None,
        name: "ceil".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let floor_expr = Expr::FuncCall {
        qualifier: None,
        name: "floor".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let round_expr = Expr::FuncCall {
        qualifier: None,
        name: "round".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("n".to_string())),
            Expr::Number(2.0),
        ],
    };
    let fmt_expr = Expr::FuncCall {
        qualifier: None,
        name: "strftime".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::StringLit("%Y-%m-%d".to_string()),
        ],
    };
    let sqrt_expr = Expr::FuncCall {
        qualifier: None,
        name: "sqrt".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("p".to_string()))],
    };
    let pow_expr = Expr::FuncCall {
        qualifier: None,
        name: "pow".to_string(),
        args: vec![Expr::Number(2.0), Expr::Number(8.0)],
    };
    let log_expr = Expr::FuncCall {
        qualifier: None,
        name: "log".to_string(),
        args: vec![Expr::Number(100.0), Expr::Number(10.0)],
    };
    let exp_expr = Expr::FuncCall {
        qualifier: None,
        name: "exp".to_string(),
        args: vec![Expr::Number(1.0)],
    };
    let clamp_expr = Expr::FuncCall {
        qualifier: None,
        name: "clamp".to_string(),
        args: vec![Expr::Number(120.0), Expr::Number(0.0), Expr::Number(100.0)],
    };
    let sign_expr = Expr::FuncCall {
        qualifier: None,
        name: "sign".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let trunc_expr = Expr::FuncCall {
        qualifier: None,
        name: "trunc".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let finite_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_finite".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let ltrim_expr = Expr::FuncCall {
        qualifier: None,
        name: "ltrim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let rtrim_expr = Expr::FuncCall {
        qualifier: None,
        name: "rtrim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let concat_expr = Expr::FuncCall {
        qualifier: None,
        name: "concat".to_string(),
        args: vec![
            Expr::StringLit("ip=".to_string()),
            Expr::StringLit("1.1.1.1".to_string()),
        ],
    };
    let index_expr = Expr::FuncCall {
        qualifier: None,
        name: "indexof".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("login".to_string()),
        ],
    };
    let replace_plain_expr = Expr::FuncCall {
        qualifier: None,
        name: "replace_plain".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("_".to_string()),
            Expr::StringLit("-".to_string()),
        ],
    };
    let sw_any_expr = Expr::FuncCall {
        qualifier: None,
        name: "startswith_any".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("  fail".to_string()),
            Expr::StringLit("deny".to_string()),
        ],
    };
    let ew_any_expr = Expr::FuncCall {
        qualifier: None,
        name: "endswith_any".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("root  ".to_string()),
            Expr::StringLit("deny".to_string()),
        ],
    };
    let coalesce_expr = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("missing".to_string())),
            Expr::StringLit("fallback".to_string()),
        ],
    };
    let isnull_expr = Expr::FuncCall {
        qualifier: None,
        name: "isnull".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("missing".to_string()))],
    };
    let isnotnull_expr = Expr::FuncCall {
        qualifier: None,
        name: "isnotnull".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let mvsort_expr = Expr::FuncCall {
        qualifier: None,
        name: "mvsort".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("arr".to_string()))],
    };
    let mvreverse_expr = Expr::FuncCall {
        qualifier: None,
        name: "mvreverse".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("arr".to_string()))],
    };

    assert_eq!(eval_expr(&abs_expr, &event), Some(Value::Number(12.345)));
    assert_eq!(eval_expr(&ceil_expr, &event), Some(Value::Number(-12.0)));
    assert_eq!(eval_expr(&floor_expr, &event), Some(Value::Number(-13.0)));
    assert_eq!(eval_expr(&round_expr, &event), Some(Value::Number(-12.35)));
    assert_eq!(
        eval_expr(&fmt_expr, &event),
        Some(Value::Str("1970-01-01".into()))
    );
    assert_eq!(eval_expr(&sqrt_expr, &event), Some(Value::Number(4.0)));
    assert_eq!(eval_expr(&pow_expr, &event), Some(Value::Number(256.0)));
    assert_eq!(eval_expr(&log_expr, &event), Some(Value::Number(2.0)));
    assert_eq!(
        eval_expr(&exp_expr, &event),
        Some(Value::Number(std::f64::consts::E))
    );
    assert_eq!(eval_expr(&clamp_expr, &event), Some(Value::Number(100.0)));
    assert_eq!(eval_expr(&sign_expr, &event), Some(Value::Number(-1.0)));
    assert_eq!(eval_expr(&trunc_expr, &event), Some(Value::Number(-12.0)));
    assert_eq!(eval_expr(&finite_expr, &event), Some(Value::Bool(true)));
    assert_eq!(
        eval_expr(&ltrim_expr, &event),
        Some(Value::Str("failed_login_root  ".into()))
    );
    assert_eq!(
        eval_expr(&rtrim_expr, &event),
        Some(Value::Str("  failed_login_root".into()))
    );
    assert_eq!(
        eval_expr(&concat_expr, &event),
        Some(Value::Str("ip=1.1.1.1".into()))
    );
    assert_eq!(eval_expr(&index_expr, &event), Some(Value::Number(9.0)));
    assert_eq!(
        eval_expr(&replace_plain_expr, &event),
        Some(Value::Str("  failed-login-root  ".into()))
    );
    assert_eq!(eval_expr(&sw_any_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&ew_any_expr, &event), Some(Value::Bool(true)));
    assert_eq!(
        eval_expr(&coalesce_expr, &event),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(eval_expr(&isnull_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&isnotnull_expr, &event), Some(Value::Bool(true)));
    assert_eq!(
        eval_expr(&mvsort_expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
    assert_eq!(
        eval_expr(&mvreverse_expr, &event),
        Some(Value::Array(vec![
            Value::Str("c".into()),
            Value::Str("a".into()),
            Value::Str("b".into()),
        ]))
    );
}

#[test]
fn now_functions_work() {
    use crate::match_engine::cep::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
    };
    let now_expr = Expr::FuncCall {
        qualifier: None,
        name: "now".to_string(),
        args: vec![],
    };
    let now_s_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_s".to_string(),
        args: vec![],
    };
    let now_ms_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_ms".to_string(),
        args: vec![],
    };
    let now_us_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_us".to_string(),
        args: vec![],
    };
    let now_ns_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_ns".to_string(),
        args: vec![],
    };
    let fmt_expr = Expr::FuncCall {
        qualifier: None,
        name: "strftime".to_string(),
        args: vec![now_expr.clone(), Expr::StringLit("%Y".to_string())],
    };
    let invalid_expr = Expr::FuncCall {
        qualifier: None,
        name: "now".to_string(),
        args: vec![Expr::Number(1.0)],
    };

    let Some(Value::Number(now_millis)) = eval_expr(&now_expr, &event) else {
        panic!("now() should return a numeric timestamp");
    };
    let Some(Value::Number(now_s)) = eval_expr(&now_s_expr, &event) else {
        panic!("now_s() should return a numeric timestamp");
    };
    let Some(Value::Number(now_ms)) = eval_expr(&now_ms_expr, &event) else {
        panic!("now_ms() should return a numeric timestamp");
    };
    let Some(Value::Number(now_us)) = eval_expr(&now_us_expr, &event) else {
        panic!("now_us() should return a numeric timestamp");
    };
    let Some(Value::Number(now_ns)) = eval_expr(&now_ns_expr, &event) else {
        panic!("now_ns() should return a numeric timestamp");
    };
    let Some(Value::Str(year)) = eval_expr(&fmt_expr, &event) else {
        panic!("strftime(now(), ...) should format the current time");
    };

    assert!(now_millis > 1_000_000_000_000.0);
    assert!(now_ns > 1_000_000_000_000_000_000.0);
    assert!(now_us > 1_000_000_000_000_000.0);
    assert!(now_ms > 1_000_000_000_000.0);
    assert!(now_s > 1_000_000_000.0);
    assert!(year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(eval_expr(&invalid_expr, &event), None);
}

#[test]
fn now_functions_share_timestamp_within_expression() {
    use crate::match_engine::cep::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::BinOp {
        op: BinOp::Sub,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "now_ms".to_string(),
            args: vec![],
        }),
        right: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "now".to_string(),
            args: vec![],
        }),
    };

    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(0.0)));
}
