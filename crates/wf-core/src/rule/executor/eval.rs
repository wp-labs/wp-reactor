use orion_error::prelude::*;

use crate::error::{CoreReason, CoreResult};
use crate::rule::match_engine::{Event, Value, eval_expr, value_to_string};

/// Evaluate a yield/derive expression with L3 function support.
///
/// L3 functions (collect_set, collect_list, first, last, stddev, percentile)
/// need access to the collected values from step execution. These values are
/// stored in `_step_{step_idx}_values` fields in the eval context.
pub(super) fn eval_yield_expr(
    expr: &wf_lang::ast::Expr,
    ctx: &Event,
    step_idx: usize,
) -> Option<Value> {
    use wf_lang::ast::Expr;

    match expr {
        // L3 Collection functions
        Expr::FuncCall { name, args, .. } if name == "collect_set" => {
            if args.len() != 1 {
                return None;
            }
            let values = get_step_values(ctx, step_idx)?;
            // The field argument is for semantic compatibility; we collect all values
            let _field_name = extract_field_name(&args[0])?;
            let collected: std::collections::HashSet<String> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Str(s) => Some(s.clone()),
                    Value::Number(n) => Some(n.to_string()),
                    Value::Bool(b) => Some(b.to_string()),
                    _ => None,
                })
                .collect();
            let result: Vec<Value> = collected.into_iter().map(Value::Str).collect();
            Some(Value::Array(result))
        }
        Expr::FuncCall { name, args, .. } if name == "collect_list" => {
            if args.len() != 1 {
                return None;
            }
            let values = get_step_values(ctx, step_idx)?;
            // Return all collected values as an array
            let result: Vec<Value> = values.clone();
            Some(Value::Array(result))
        }
        Expr::FuncCall { name, args, .. } if name == "first" => {
            if args.len() != 1 {
                return None;
            }
            let values = get_step_values(ctx, step_idx)?;
            values.first().cloned()
        }
        Expr::FuncCall { name, args, .. } if name == "last" => {
            if args.len() != 1 {
                return None;
            }
            let values = get_step_values(ctx, step_idx)?;
            values.last().cloned()
        }
        // L3 Statistical functions
        Expr::FuncCall { name, args, .. } if name == "stddev" => {
            if args.len() != 1 {
                return None;
            }
            let values = get_step_values(ctx, step_idx)?;
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
        Expr::FuncCall { name, args, .. } if name == "percentile" => {
            if args.len() != 2 {
                return None;
            }
            let values = get_step_values(ctx, step_idx)?;
            let p = match eval_expr(&args[1], ctx)? {
                Value::Number(n) => n.clamp(0.0, 1.0),
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
        // Fall back to standard expression evaluation
        _ => eval_expr(expr, ctx),
    }
}

/// Get the collected values for a step from the eval context.
fn get_step_values(ctx: &Event, step_idx: usize) -> Option<&Vec<Value>> {
    let field_name = format!("_step_{}_values", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Array(arr)) => Some(arr),
        _ => None,
    }
}

/// Extract a field name from an expression (for L3 functions).
fn extract_field_name(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Simple(name)) => Some(name.as_str()),
        Expr::Field(FieldRef::Qualified(_, name)) => Some(name.as_str()),
        _ => None,
    }
}

/// Evaluate the score expression and clamp to `[0, 100]`.
///
/// L3 functions (collect_set/list, first/last, stddev/percentile) are evaluated
/// using step 0's collected values, as score expressions typically reference
/// the first (and often only) satisfied step.
pub(super) fn eval_score(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<f64> {
    let val = eval_yield_expr(expr, ctx, 0);
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
/// L3 functions are evaluated using step 0's collected values.
pub(super) fn eval_entity_id(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<String> {
    let val = eval_yield_expr(expr, ctx, 0);
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
    use wf_lang::ast::{Expr, FieldRef};

    fn make_test_event(values: Vec<Value>) -> Event {
        let mut fields = std::collections::HashMap::new();
        fields.insert("_step_0_values".to_string(), Value::Array(values));
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
        let result = eval_yield_expr(&expr, &ctx, 0);
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
        let result = eval_yield_expr(&expr, &ctx, 0);
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
        let result = eval_yield_expr(&expr, &ctx, 0);
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
        let result = eval_yield_expr(&expr, &ctx, 0);
        // collect_set returns unique values as strings
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
        let result = eval_yield_expr(&expr, &ctx, 0);
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
        let result = eval_yield_expr(&expr, &ctx, 0);
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
        // percentile(value, 0.5) should return median (2.0 or 3.0 depending on method)
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(0.5),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx, 0);
        if let Some(Value::Number(p)) = result {
            // With our implementation: sorted=[1,2,3,4], idx=(3*0.5).round=2, result=3
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
        let result = eval_yield_expr(&expr, &ctx, 0);
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
                Expr::Number(1.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx, 0);
        assert_eq!(result, Some(Value::Number(30.0)));
    }
}
