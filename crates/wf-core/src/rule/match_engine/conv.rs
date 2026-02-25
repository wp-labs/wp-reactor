use std::collections::HashMap;

use wf_lang::ast::FieldRef;
use wf_lang::plan::{ConvChainPlan, ConvOpPlan, ConvPlan};

use super::eval::eval_expr;
use super::key::{field_ref_name, value_to_string};
use super::types::{CloseOutput, Event, Value};

/// Apply conv transformations to a batch of close outputs.
///
/// Iterates through each chain sequentially; within a chain, each operation
/// is applied left-to-right (pipeline). This produces the final transformed
/// batch, e.g. `sort(-count) | top(10)` sorts descending then truncates.
pub(crate) fn apply_conv(
    plan: &ConvPlan,
    keys: &[FieldRef],
    mut outputs: Vec<CloseOutput>,
) -> Vec<CloseOutput> {
    for chain in &plan.chains {
        outputs = apply_chain(chain, keys, outputs);
    }
    outputs
}

fn apply_chain(
    chain: &ConvChainPlan,
    keys: &[FieldRef],
    mut outputs: Vec<CloseOutput>,
) -> Vec<CloseOutput> {
    for op in &chain.ops {
        outputs = apply_op(op, keys, outputs);
    }
    outputs
}

fn apply_op(op: &ConvOpPlan, keys: &[FieldRef], mut outputs: Vec<CloseOutput>) -> Vec<CloseOutput> {
    match op {
        ConvOpPlan::Sort(sort_keys) => {
            outputs.sort_by(|a, b| {
                let ctx_a = build_eval_context(a, keys);
                let ctx_b = build_eval_context(b, keys);
                for sk in sort_keys {
                    let va = eval_expr(&sk.expr, &ctx_a);
                    let vb = eval_expr(&sk.expr, &ctx_b);
                    let ord = compare_option_values(&va, &vb);
                    let ord = if sk.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            outputs
        }
        ConvOpPlan::Top(n) => {
            outputs.truncate(*n as usize);
            outputs
        }
        ConvOpPlan::Dedup(expr) => {
            let mut seen = Vec::<String>::new();
            outputs.retain(|output| {
                let ctx = build_eval_context(output, keys);
                let val = eval_expr(expr, &ctx);
                let key = match val {
                    Some(v) => value_to_string(&v),
                    None => "__none__".to_string(),
                };
                if seen.contains(&key) {
                    false
                } else {
                    seen.push(key);
                    true
                }
            });
            outputs
        }
        ConvOpPlan::Where(expr) => {
            outputs.retain(|output| {
                let ctx = build_eval_context(output, keys);
                matches!(eval_expr(expr, &ctx), Some(Value::Bool(true)))
            });
            outputs
        }
    }
}

/// Build an `Event` context from a `CloseOutput` for expression evaluation.
///
/// The context includes:
/// - Scope key fields: key names mapped to their values
/// - Step labels: label names mapped to their measure values (from both event and close steps)
fn build_eval_context(output: &CloseOutput, keys: &[FieldRef]) -> Event {
    let mut fields = HashMap::new();

    // Map scope key values to their field names
    for (i, key) in keys.iter().enumerate() {
        if let Some(val) = output.scope_key.get(i) {
            let name = field_ref_name(key).to_string();
            fields.insert(name, val.clone());
        }
    }

    // Map step labels to their measure values (event steps first, then close steps)
    for step in output
        .event_step_data
        .iter()
        .chain(output.close_step_data.iter())
    {
        if let Some(ref label) = step.label {
            fields.insert(label.clone(), Value::Number(step.measure_value));
        }
    }

    Event { fields }
}

/// Compare two optional values for sorting purposes.
fn compare_option_values(a: &Option<Value>, b: &Option<Value>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(va), Some(vb)) => compare_values(va, vb),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

/// Compare two values for sorting: numbers numerically, strings lexicographically.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // Mixed types: Number < Str < Bool
        (Value::Number(_), _) => std::cmp::Ordering::Less,
        (_, Value::Number(_)) => std::cmp::Ordering::Greater,
        (Value::Str(_), Value::Bool(_)) => std::cmp::Ordering::Less,
        (Value::Bool(_), Value::Str(_)) => std::cmp::Ordering::Greater,
    }
}
