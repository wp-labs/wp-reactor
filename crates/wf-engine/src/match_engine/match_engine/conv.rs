use wf_lang::ast::FieldRef;
use wf_lang::plan::{ConvChainPlan, ConvOpPlan, ConvPlan};

use super::eval::eval_expr;
use super::key::{field_ref_name, value_to_string};
use super::types::{CloseOutput, EngineHashMap, Event, Value};

/// Apply conv transformations to a batch of close outputs.
///
/// Iterates through each chain sequentially; within a chain, each operation
/// is applied left-to-right (pipeline). This produces the final transformed
/// batch, e.g. `sort(-count) | top(10)` sorts descending then truncates.
pub fn apply_conv(
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
            // 预提取每个 output 的排序键值（每元素每 sort key 1 次 eval；旧实现
            // 每次比较构建 2 次 context + eval 2 次 → O(n log n) 次 HashMap 分配
            // 与求值，是 Q5/Q7 收口批 conv `sort(-m)|top(1)` 的主成本（~2k 行批
            // → 数万次分配；2026-08 nexmark hotpath 审查）。预提取后比较阶段
            // 零分配零求值。语义不变：eval/compare 与旧实现逐元素相同，且
            // `sort_by` 为稳定排序（相同键保持原顺序）。
            let key_rows: Vec<Vec<Option<Value>>> = outputs
                .iter()
                .map(|o| {
                    let ctx = build_eval_context(o, keys);
                    sort_keys
                        .iter()
                        .map(|sk| eval_expr(&sk.expr, &ctx))
                        .collect()
                })
                .collect();
            let mut idx: Vec<usize> = (0..outputs.len()).collect();
            idx.sort_by(|&i, &j| {
                let row_i = &key_rows[i];
                let row_j = &key_rows[j];
                for (sk, (va, vb)) in sort_keys.iter().zip(row_i.iter().zip(row_j.iter())) {
                    let ord = compare_option_values(va, vb);
                    let ord = if sk.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            let mut buf: Vec<Option<CloseOutput>> = outputs.into_iter().map(Some).collect();
            outputs = idx
                .into_iter()
                .map(|i| buf[i].take().expect("each output moved exactly once"))
                .collect();
            outputs
        }
        ConvOpPlan::Top(n) => {
            outputs.truncate(*n as usize);
            outputs
        }
        ConvOpPlan::Dedup(expr) => {
            // HashSet 替代 Vec::contains 线性扫描（旧 O(n²)，大收口批下
            // dedup 成本与 sort 同级；语义不变——只判存在性，序无关）。
            let mut seen = std::collections::HashSet::<String>::new();
            outputs.retain(|output| {
                let ctx = build_eval_context(output, keys);
                let val = eval_expr(expr, &ctx);
                let key = match val {
                    Some(v) => value_to_string(&v),
                    None => "__none__".to_string(),
                };
                seen.insert(key)
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
    let mut fields = EngineHashMap::default();

    // Map scope key values to their field names
    for (i, key) in keys.iter().enumerate() {
        if let Some(val) = output.scope_key.get(i) {
            let name = field_ref_name(key).to_string();
            fields.insert(name.into(), val.clone());
        }
    }

    // Map step labels to their measure values (event steps first, then close steps)
    for step in output
        .event_step_data
        .iter()
        .chain(output.close_step_data.iter())
    {
        if let Some(ref label) = step.label {
            fields.insert(label.clone().into(), Value::Number(step.measure_value));
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
        (Value::Array(x), Value::Array(y)) => x.len().cmp(&y.len()),
        (Value::Object(x), Value::Object(y)) => x.len().cmp(&y.len()),
        // Mixed types: Number < Str < Bool < Array < Object
        (Value::Number(_), _) => std::cmp::Ordering::Less,
        (_, Value::Number(_)) => std::cmp::Ordering::Greater,
        (Value::Str(_), Value::Bool(_) | Value::Array(_) | Value::Object(_)) => {
            std::cmp::Ordering::Less
        }
        (Value::Bool(_) | Value::Array(_) | Value::Object(_), Value::Str(_)) => {
            std::cmp::Ordering::Greater
        }
        (Value::Bool(_), Value::Array(_) | Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Array(_) | Value::Object(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::Array(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Object(_), Value::Array(_)) => std::cmp::Ordering::Greater,
    }
}
