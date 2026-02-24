use orion_error::prelude::*;

use crate::error::{CoreReason, CoreResult};
use crate::rule::match_engine::{Event, Value, eval_expr, value_to_string};

/// Evaluate the score expression and clamp to `[0, 100]`.
pub(super) fn eval_score(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<f64> {
    let val = eval_expr(expr, ctx);
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
pub(super) fn eval_entity_id(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<String> {
    let val = eval_expr(expr, ctx);
    match val {
        Some(v) => Ok(value_to_string(&v)),
        None => StructError::from(CoreReason::RuleExec)
            .with_detail("entity_id expression evaluated to None")
            .err(),
    }
}
