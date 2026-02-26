use orion_error::prelude::*;

use crate::error::{CoreReason, CoreResult};
use crate::rule::match_engine::{
    Event, Value, eval_expr, field_ref_name, value_to_string, values_equal,
};

/// Evaluate a yield/derive expression with L3 function support.
///
/// L3 functions (collect_set, collect_list, first, last, stddev, percentile)
/// need access to the collected values from step execution. These values are
/// stored in `_step_{i}_values` and `_step_{i}_source` fields in the eval context.
pub(super) fn eval_yield_expr(expr: &wf_lang::ast::Expr, ctx: &Event) -> Option<Value> {
    eval_expr_with_l3(expr, ctx)
}

fn eval_expr_with_l3(expr: &wf_lang::ast::Expr, ctx: &Event) -> Option<Value> {
    use wf_lang::ast::{BinOp, Expr};

    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Field(fr) => ctx.fields.get(field_ref_name(fr)).cloned(),
        Expr::Neg(inner) => match eval_expr_with_l3(inner, ctx)? {
            Value::Number(n) => Some(Value::Number(-n)),
            _ => None,
        },
        Expr::BinOp { op, left, right } => match op {
            BinOp::And => eval_logic_and_with_l3(left, right, ctx),
            BinOp::Or => eval_logic_or_with_l3(left, right, ctx),
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                let lv = eval_expr_with_l3(left, ctx)?;
                let rv = eval_expr_with_l3(right, ctx)?;
                Some(Value::Bool(compare_values(*op, &lv, &rv)))
            }
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                let lv = eval_expr_with_l3(left, ctx)?;
                let rv = eval_expr_with_l3(right, ctx)?;
                let ln = coerce_to_f64(&lv)?;
                let rn = coerce_to_f64(&rv)?;
                let out = match op {
                    BinOp::Add => ln + rn,
                    BinOp::Sub => ln - rn,
                    BinOp::Mul => ln * rn,
                    BinOp::Div => {
                        if rn == 0.0 {
                            return None;
                        }
                        ln / rn
                    }
                    BinOp::Mod => {
                        if rn == 0.0 {
                            return None;
                        }
                        ln % rn
                    }
                    _ => unreachable!(),
                };
                Some(Value::Number(out))
            }
            _ => None,
        },
        Expr::InList {
            expr: target,
            list,
            negated,
        } => {
            let target_val = eval_expr_with_l3(target, ctx)?;
            let found = list.iter().any(|item| {
                eval_expr_with_l3(item, ctx)
                    .map(|v| values_equal(&target_val, &v))
                    .unwrap_or(false)
            });
            Some(Value::Bool(if *negated { !found } else { found }))
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => match eval_expr_with_l3(cond, ctx) {
            Some(Value::Bool(true)) => eval_expr_with_l3(then_expr, ctx),
            Some(Value::Bool(false)) => eval_expr_with_l3(else_expr, ctx),
            _ => None,
        },
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            if qualifier.is_some() {
                return eval_expr(expr, ctx);
            }
            if is_l3_func(name) {
                return eval_l3_func(name, args, ctx);
            }
            if args.iter().any(contains_l3_func) {
                return eval_builtin_func_with_l3(name, args, ctx);
            }
            eval_expr(expr, ctx)
        }
        _ => None,
    }
}

fn eval_logic_and_with_l3(
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &Event,
) -> Option<Value> {
    let lv = eval_expr_with_l3(left, ctx);
    let rv = eval_expr_with_l3(right, ctx);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => Some(Value::Bool(false)),
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => Some(Value::Bool(true)),
        _ => None,
    }
}

fn eval_logic_or_with_l3(
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &Event,
) -> Option<Value> {
    let lv = eval_expr_with_l3(left, ctx);
    let rv = eval_expr_with_l3(right, ctx);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(Value::Bool(true)),
        (Some(Value::Bool(false)), Some(Value::Bool(false))) => Some(Value::Bool(false)),
        _ => None,
    }
}

fn compare_values(op: wf_lang::ast::BinOp, lv: &Value, rv: &Value) -> bool {
    use wf_lang::ast::BinOp;
    match op {
        BinOp::Eq => values_equal(lv, rv),
        BinOp::Ne => !values_equal(lv, rv),
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => match (lv, rv) {
            (Value::Number(a), Value::Number(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            (Value::Str(a), Value::Str(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            (Value::Bool(a), Value::Bool(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            _ => false,
        },
        _ => false,
    }
}

fn coerce_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

fn is_l3_func(name: &str) -> bool {
    matches!(
        name,
        "collect_set" | "collect_list" | "first" | "last" | "stddev" | "percentile"
    )
}

fn contains_l3_func(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::FuncCall { name, args, .. } => is_l3_func(name) || args.iter().any(contains_l3_func),
        Expr::BinOp { left, right, .. } => contains_l3_func(left) || contains_l3_func(right),
        Expr::Neg(inner) => contains_l3_func(inner),
        Expr::InList { expr, list, .. } => {
            contains_l3_func(expr) || list.iter().any(contains_l3_func)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => contains_l3_func(cond) || contains_l3_func(then_expr) || contains_l3_func(else_expr),
        _ => false,
    }
}

fn eval_builtin_func_with_l3(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
) -> Option<Value> {
    match name {
        "contains" => {
            if args.len() != 2 {
                return None;
            }
            let haystack = match eval_expr_with_l3(&args[0], ctx)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let needle = match eval_expr_with_l3(&args[1], ctx)? {
                Value::Str(s) => s,
                _ => return None,
            };
            Some(Value::Bool(haystack.contains(&needle)))
        }
        "lower" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx)? {
                Value::Str(s) => Some(Value::Str(s.to_lowercase())),
                _ => None,
            }
        }
        "upper" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx)? {
                Value::Str(s) => Some(Value::Str(s.to_uppercase())),
                _ => None,
            }
        }
        "len" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx)? {
                Value::Str(s) => Some(Value::Number(s.len() as f64)),
                _ => None,
            }
        }
        "regex_match" => {
            if args.len() != 2 {
                return None;
            }
            let hay = match eval_expr_with_l3(&args[0], ctx)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let pat = match eval_expr_with_l3(&args[1], ctx)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let re = regex::Regex::new(&pat).ok()?;
            Some(Value::Bool(re.is_match(&hay)))
        }
        "time_diff" => {
            if args.len() != 2 {
                return None;
            }
            let t1 = match eval_expr_with_l3(&args[0], ctx)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let t2 = match eval_expr_with_l3(&args[1], ctx)? {
                Value::Number(n) => n,
                _ => return None,
            };
            Some(Value::Number((t1 - t2).abs() / 1_000_000_000.0))
        }
        "time_bucket" => {
            if args.len() != 2 {
                return None;
            }
            let t = match eval_expr_with_l3(&args[0], ctx)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let interval = match eval_expr_with_l3(&args[1], ctx)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let interval_nanos = interval * 1_000_000_000.0;
            if interval_nanos == 0.0 {
                return None;
            }
            let bucketed = (t / interval_nanos).floor() * interval_nanos;
            Some(Value::Number(bucketed))
        }
        _ => None,
    }
}

fn eval_l3_func(name: &str, args: &[wf_lang::ast::Expr], ctx: &Event) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let step_indices = resolve_step_indices(ctx, args.first());
    let values = flatten_step_values(ctx, &step_indices);
    match name {
        "collect_set" => {
            if args.len() != 1 {
                return None;
            }
            let mut out: Vec<Value> = Vec::new();
            for v in values {
                if !out.iter().any(|seen| values_equal(seen, &v)) {
                    out.push(v);
                }
            }
            Some(Value::Array(out))
        }
        "collect_list" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Array(values))
        }
        "first" => {
            if args.len() != 1 {
                return None;
            }
            values.first().cloned()
        }
        "last" => {
            if args.len() != 1 {
                return None;
            }
            values.last().cloned()
        }
        "stddev" => {
            if args.len() != 1 {
                return None;
            }
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => Some(*n),
                    _ => None,
                })
                .collect();
            if nums.len() < 2 {
                return Some(Value::Number(0.0));
            }
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let variance = nums.iter().map(|n| (n - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            Some(Value::Number(variance.sqrt()))
        }
        "percentile" => {
            if args.len() != 2 {
                return None;
            }
            let p = match eval_expr_with_l3(&args[1], ctx)? {
                Value::Number(n) => n.clamp(0.0, 100.0) / 100.0,
                _ => return None,
            };
            let mut nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => Some(*n),
                    _ => None,
                })
                .collect();
            if nums.is_empty() {
                return Some(Value::Number(0.0));
            }
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((nums.len() - 1) as f64 * p).round() as usize;
            Some(Value::Number(nums[idx.min(nums.len() - 1)]))
        }
        _ => None,
    }
}

fn flatten_step_values(ctx: &Event, step_indices: &[usize]) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_values(ctx, *idx) {
            out.extend_from_slice(values);
        }
    }
    out
}

fn resolve_step_indices(ctx: &Event, arg: Option<&wf_lang::ast::Expr>) -> Vec<usize> {
    let all = step_indices(ctx);
    if all.is_empty() {
        return all;
    }
    let Some(alias) = arg.and_then(extract_source_alias) else {
        return all;
    };
    all.iter()
        .copied()
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s == alias))
        .collect()
}

fn step_indices(ctx: &Event) -> Vec<usize> {
    let mut out: Vec<usize> = ctx
        .fields
        .keys()
        .filter_map(|k| parse_step_field_index(k, "_values"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_step_field_index(key: &str, suffix: &str) -> Option<usize> {
    let body = key.strip_prefix("_step_")?.strip_suffix(suffix)?;
    body.parse::<usize>().ok()
}

fn get_step_values(ctx: &Event, step_idx: usize) -> Option<&[Value]> {
    let field_name = format!("_step_{}_values", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
        _ => None,
    }
}

fn get_step_source<'a>(ctx: &'a Event, step_idx: usize) -> Option<&'a str> {
    let field_name = format!("_step_{}_source", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn extract_source_alias(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

/// Evaluate the score expression and clamp to `[0, 100]`.
///
pub(super) fn eval_score(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<f64> {
    let val = eval_yield_expr(expr, ctx);
    let raw = match val {
        Some(Value::Number(n)) => n,
        Some(other) => {
            return StructError::from(CoreReason::RuleExec)
                .with_detail(format!(
                    "score expression evaluated to non-numeric value: {:?}",
                    other
                ))
                .err();
        }
        None => {
            return StructError::from(CoreReason::RuleExec)
                .with_detail("score expression evaluated to None")
                .err();
        }
    };
    Ok(clamp_score(raw))
}

fn clamp_score(v: f64) -> f64 {
    v.clamp(0.0, 100.0)
}

/// Evaluate the entity_id expression.
///
pub(super) fn eval_entity_id(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<String> {
    let val = eval_yield_expr(expr, ctx);
    match val {
        Some(v) => Ok(value_to_string(&v)),
        None => StructError::from(CoreReason::RuleExec)
            .with_detail("entity_id expression evaluated to None")
            .err(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use wf_lang::ast::{BinOp, Expr, FieldRef};

    fn make_test_event(values: Vec<Value>) -> Event {
        let mut fields = std::collections::HashMap::new();
        fields.insert("_step_0_values".to_string(), Value::Array(values));
        fields.insert("_step_0_source".to_string(), Value::Str("e".to_string()));
        Event { fields }
    }

    #[test]
    fn test_first_returns_first_value() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "first".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(10.0)));
    }

    #[test]
    fn test_last_returns_last_value() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "last".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(30.0)));
    }

    #[test]
    fn test_collect_list_returns_all_values() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "collect_list".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(
            result,
            Some(Value::Array(vec![
                Value::Number(10.0),
                Value::Number(20.0),
                Value::Number(30.0),
            ]))
        );
    }

    #[test]
    fn test_collect_set_returns_unique_values() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("a".to_string()),
            Value::Str("c".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "collect_set".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Array(arr)) = result {
            assert_eq!(arr.len(), 3); // a, b, c (unique)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_stddev_calculation() {
        let ctx = make_test_event(vec![
            Value::Number(2.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(5.0),
            Value::Number(5.0),
            Value::Number(7.0),
            Value::Number(9.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "stddev".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Number(stddev)) = result {
            // Population stddev of [2,4,4,4,5,5,7,9] = 2.0
            assert!((stddev - 2.0).abs() < 0.01, "Expected ~2.0, got {}", stddev);
        } else {
            panic!("Expected numeric result, got {:?}", result);
        }
    }

    #[test]
    fn test_stddev_returns_zero_for_single_value() {
        let ctx = make_test_event(vec![Value::Number(5.0)]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "stddev".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(0.0)));
    }

    #[test]
    fn test_percentile_calculation() {
        let ctx = make_test_event(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
            Value::Number(4.0),
        ]);
        // percentile(value, 50) should return median-like value.
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(50.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Number(p)) = result {
            // sorted=[1,2,3,4], idx=(3*0.5).round=2, result=3
            assert!((p - 3.0).abs() < 0.01, "Expected ~3.0, got {}", p);
        } else {
            panic!("Expected numeric result, got {:?}", result);
        }
    }

    #[test]
    fn test_percentile_zero_returns_min() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(0.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(10.0)));
    }

    #[test]
    fn test_percentile_one_returns_max() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(100.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(30.0)));
    }

    #[test]
    fn test_nested_l3_in_arithmetic() {
        let ctx = make_test_event(vec![
            Value::Number(2.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(5.0),
            Value::Number(5.0),
            Value::Number(7.0),
            Value::Number(9.0),
        ]);
        let expr = Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "stddev".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "e".to_string(),
                    "value".to_string(),
                ))],
            }),
            right: Box::new(Expr::Number(1.0)),
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Number(v)) = result {
            assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
        } else {
            panic!("Expected numeric result, got {:?}", result);
        }
    }

    #[test]
    fn test_qualified_alias_selects_matching_step() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "_step_0_values".to_string(),
            Value::Array(vec![Value::Number(10.0)]),
        );
        fields.insert("_step_0_source".to_string(), Value::Str("a".to_string()));
        fields.insert(
            "_step_1_values".to_string(),
            Value::Array(vec![Value::Number(99.0)]),
        );
        fields.insert("_step_1_source".to_string(), Value::Str("b".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "first".to_string(),
            args: vec![Expr::Field(FieldRef::Qualified(
                "b".to_string(),
                "value".to_string(),
            ))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(99.0)));
    }

    #[test]
    fn test_qualified_alias_without_match_returns_none_for_first() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "_step_0_values".to_string(),
            Value::Array(vec![Value::Number(10.0)]),
        );
        fields.insert("_step_0_source".to_string(), Value::Str("a".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "first".to_string(),
            args: vec![Expr::Field(FieldRef::Qualified(
                "missing".to_string(),
                "value".to_string(),
            ))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, None);
    }
}
