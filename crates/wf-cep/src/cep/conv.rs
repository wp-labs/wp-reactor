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
    // `sort | top_ties` 相邻对合并（Q5/Q7 形状）：排序键只预提取一遍。
    // 旧实现 Sort 与 TopTies 各自 eval（apply_op 间无状态共享）——2026-08-24
    // bench：top_ties 相对 top +46~55% 的主因就是双倍 eval。语义不变：
    // eval 无副作用、同输入同结果；仅当 sort_keys 完全一致时合并，否则
    // 逐 op 独立执行（apply_op 原逻辑）。
    let mut i = 0;
    while i < chain.ops.len() {
        if let [
            ConvOpPlan::Sort(sort_keys),
            ConvOpPlan::TopTies {
                n,
                sort_keys: ties_keys,
            },
        ] = &chain.ops[i..(i + 2).min(chain.ops.len())]
            && *sort_keys == *ties_keys
        {
            // 预提取 + 排序（重排 outputs）+ 缓存排序后顺序的键行。
            let key_rows = precompute_sort_keys(&outputs, keys, sort_keys);
            let (idx, sorted_rows) = sort_by_key_rows(&outputs, sort_keys, key_rows);
            let mut buf: Vec<Option<CloseOutput>> = outputs.into_iter().map(Some).collect();
            outputs = idx
                .into_iter()
                .map(|i| buf[i].take().expect("each output moved exactly once"))
                .collect();
            outputs = top_ties_with_keys(outputs, *n as usize, sorted_rows);
            i += 2;
        } else {
            outputs = apply_op(&chain.ops[i], keys, outputs);
            i += 1;
        }
    }
    outputs
}

/// 预提取每个 output 的排序键值（每元素每 sort key 1 次 eval；旧实现每次比较
/// 构建 2 次 context + eval 2 次 → O(n log n) 次 HashMap 分配与求值，是 Q5/Q7
/// 收口批 conv `sort(-m)|top(1)` 的主成本（~2k 行批 → 数万次分配；2026-08
/// nexmark hotpath 审查）。预提取后比较阶段零分配零求值。
fn precompute_sort_keys(
    outputs: &[CloseOutput],
    keys: &[FieldRef],
    sort_keys: &[wf_lang::plan::SortKeyPlan],
) -> Vec<Vec<Option<Value>>> {
    outputs
        .iter()
        .map(|o| {
            let ctx = build_eval_context(o, keys);
            sort_keys
                .iter()
                .map(|sk| eval_expr(&sk.expr, &ctx))
                .collect()
        })
        .collect()
}

/// 按预提取键稳定排序，返回 (索引排列, 排序后顺序的键行)。
/// `sorted` 与排序后的 `outputs` 索引对齐——后续 `top_ties` 直接读，不再 eval。
fn sort_by_key_rows(
    outputs: &[CloseOutput],
    sort_keys: &[wf_lang::plan::SortKeyPlan],
    key_rows: Vec<Vec<Option<Value>>>,
) -> (Vec<usize>, Vec<Vec<Option<Value>>>) {
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
    let sorted_rows = idx.iter().map(|&i| key_rows[i].clone()).collect();
    (idx, sorted_rows)
}

/// `sort | top_ties` 共享 key 的并列判定：`key_rows` 已是排序后顺序。
fn top_ties_with_keys(
    mut outputs: Vec<CloseOutput>,
    n: usize,
    key_rows: Vec<Vec<Option<Value>>>,
) -> Vec<CloseOutput> {
    let count = outputs.len().min(n);
    if count == 0 || outputs.len() <= count || key_rows.is_empty() {
        outputs.truncate(count);
        return outputs;
    }
    // 第 N 条（0 基 count-1）的排序键值为并列基准：后续全部等值条目保留。
    let tie = &key_rows[count - 1];
    let mut i = count;
    while i < outputs.len() {
        let row = &key_rows[i];
        let all_equal = row
            .iter()
            .zip(tie.iter())
            .all(|(a, b)| compare_option_values(a, b) == std::cmp::Ordering::Equal);
        if !all_equal {
            break;
        }
        i += 1;
    }
    outputs.truncate(i);
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
            // 注：`apply_chain` 对紧随的 `top_ties`（同 sort_keys）合并共享 key；
            // 此处为独立/其他位置的 Sort（含 apply_conv_filtered 路径）。
            let key_rows = precompute_sort_keys(&outputs, keys, sort_keys);
            let (idx, _) = sort_by_key_rows(&outputs, sort_keys, key_rows);
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
        ConvOpPlan::TopTies { n, sort_keys } => {
            let count = outputs.len().min(*n as usize);
            // top_ties(0) / 空输入 / 无前导 sort（checker 应拒绝）：退化为普通
            // top 截断，绝不 panic（第 N 条索引要求 count >= 1）。
            if count == 0 || outputs.len() <= count || sort_keys.is_empty() {
                outputs.truncate(count);
                return outputs;
            }
            // 预提取排序键值（与 Sort 同路径；稳定排序保证并列条目相邻）。
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
            // 第 N 条（0 基 count-1）的排序键值为并列基准：后续全部等值条目保留。
            let tie = &key_rows[count - 1];
            let mut i = count;
            while i < outputs.len() {
                let row = &key_rows[i];
                let all_equal = row
                    .iter()
                    .zip(tie.iter())
                    .all(|(a, b)| compare_option_values(a, b) == std::cmp::Ordering::Equal);
                if !all_equal {
                    break;
                }
                i += 1;
            }
            outputs.truncate(i);
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
