use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use wf_lang::ast::{BinOp, CmpOp, Expr};

use super::key::{field_ref_name, value_to_string};
use super::types::{Event, RollingStats, Value, WindowLookup};

// ---------------------------------------------------------------------------
// Expression evaluator (L1)
// ---------------------------------------------------------------------------

/// Evaluate an expression against an event, returning a [`Value`].
///
/// Supports: literals, field refs, BinOp (And/Or/comparisons/arithmetic),
/// Neg, InList, and basic FuncCall (contains, startswith, endswith, substr, replace, trim, lower, upper, len, mvcount, mvjoin, mvindex, mvappend, split, mvdedup, abs, round, ceil, floor, sqrt, pow, log, exp, clamp, sign, trunc, is_finite, ltrim, rtrim, concat, indexof, replace_plain, startswith_any, endswith_any, coalesce, isnull, isnotnull, mvsort, mvreverse, strftime, strptime, has, baseline).
pub(crate) fn eval_expr(expr: &Expr, event: &Event) -> Option<Value> {
    let mut empty = HashMap::new();
    eval_expr_ext(expr, event, None, &mut empty)
}

/// Extended expression evaluator with window lookup and baseline store access.
///
/// All recursive calls go through this function (not `eval_expr`) to preserve
/// the `windows` and `baselines` context through compound expressions.
pub(crate) fn eval_expr_ext(
    expr: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Field(fr) => {
            let name = field_ref_name(fr);
            event.fields.get(name).cloned()
        }
        Expr::Neg(inner) => {
            let v = eval_expr_ext(inner, event, windows, baselines)?;
            match v {
                Value::Number(n) => Some(Value::Number(-n)),
                _ => None,
            }
        }
        Expr::BinOp { op, left, right } => eval_binop(*op, left, right, event, windows, baselines),
        Expr::InList {
            expr: target,
            list,
            negated,
        } => {
            let target_val = eval_expr_ext(target, event, windows, baselines)?;
            // InList items are typically literals — context not needed, but
            // we pass it for correctness in case of field refs / func calls.
            let found = list.iter().any(|item| {
                eval_expr_ext(item, event, windows, baselines)
                    .map(|v| values_equal(&target_val, &v))
                    .unwrap_or(false)
            });
            Some(Value::Bool(if *negated { !found } else { found }))
        }
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            // Handle window.has()
            if let Some(window_name) = qualifier
                && name == "has"
            {
                return eval_window_has(window_name, args, event, windows);
            }
            // Handle baseline()
            if name == "baseline" && (args.len() == 2 || args.len() == 3) {
                return eval_baseline(args, event, baselines);
            }
            eval_func_call(name, args, event, windows, baselines)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            let cond_val = eval_expr_ext(cond, event, windows, baselines);
            match cond_val {
                Some(Value::Bool(true)) => eval_expr_ext(then_expr, event, windows, baselines),
                Some(Value::Bool(false)) => eval_expr_ext(else_expr, event, windows, baselines),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Evaluate `window.has(expr [, "field"])`.
fn eval_window_has(
    window_name: &str,
    args: &[Expr],
    event: &Event,
    windows: Option<&dyn WindowLookup>,
) -> Option<Value> {
    let windows = windows?;
    let lookup_val = eval_expr(&args[0], event)?;
    let lookup_str = value_to_string(&lookup_val);

    // Explicit field name from 2nd arg, or infer from the field ref in 1st arg
    let field_name = match args.get(1) {
        Some(Expr::StringLit(f)) => f.clone(),
        Some(_) => return None,
        None => match &args[0] {
            Expr::Field(fr) => field_ref_name(fr).to_string(),
            _ => return None,
        },
    };

    let values = windows.snapshot_field_values(window_name, &field_name)?;
    Some(Value::Bool(values.contains(&lookup_str)))
}

/// Evaluate `baseline(expr, duration_seconds [, method])`.
///
/// Computes the z-score (number of standard deviations from the running mean)
/// of the current value, then updates the running statistics.
///
/// Supported methods: "mean" (default), "ewma", "median"
fn eval_baseline(
    args: &[Expr],
    event: &Event,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    let current_val = match eval_expr(&args[0], event)? {
        Value::Number(n) => n,
        _ => return None,
    };

    // Parse optional method argument (default to "mean")
    let method = args
        .get(2)
        .and_then(|arg| match arg {
            Expr::StringLit(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("mean");

    // Build a key to identify this baseline expression (including method)
    let key = format!("{:?}:{}", args[0], method);

    let stats = baselines
        .entry(key)
        .or_insert_with(|| RollingStats::new_with_method(method));
    let deviation = stats.deviation(current_val);
    stats.update(current_val);
    Some(Value::Number(deviation))
}

fn eval_binop(
    op: BinOp,
    left: &Expr,
    right: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    match op {
        BinOp::And => eval_logic_and(left, right, event, windows, baselines),
        BinOp::Or => eval_logic_or(left, right, event, windows, baselines),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            let lv = eval_expr_ext(left, event, windows, baselines)?;
            let rv = eval_expr_ext(right, event, windows, baselines)?;
            Some(Value::Bool(compare_values(op, &lv, &rv)))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let lv = eval_expr_ext(left, event, windows, baselines)?;
            let rv = eval_expr_ext(right, event, windows, baselines)?;
            let ln = coerce_to_f64(&lv)?;
            let rn = coerce_to_f64(&rv)?;
            eval_arithmetic(op, ln, rn)
        }
        _ => None,
    }
}

/// Three-valued (SQL NULL) logical AND.
///
/// Both sides are always evaluated so that partial information is preserved.
/// This is essential for close-step guards where one side references an
/// event field (missing at close time) and the other references
/// close_reason (missing during accumulation).
fn eval_logic_and(
    left: &Expr,
    right: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    let lv = eval_expr_ext(left, event, windows, baselines);
    let rv = eval_expr_ext(right, event, windows, baselines);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => Some(Value::Bool(false)),
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => Some(Value::Bool(true)),
        _ => None,
    }
}

/// Three-valued (SQL NULL) logical OR.
fn eval_logic_or(
    left: &Expr,
    right: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    let lv = eval_expr_ext(left, event, windows, baselines);
    let rv = eval_expr_ext(right, event, windows, baselines);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(Value::Bool(true)),
        (Some(Value::Bool(false)), Some(Value::Bool(false))) => Some(Value::Bool(false)),
        _ => None,
    }
}

/// Arithmetic on two numeric values: +, -, *, /, %.
fn eval_arithmetic(op: BinOp, lv: f64, rv: f64) -> Option<Value> {
    let result = match op {
        BinOp::Add => lv + rv,
        BinOp::Sub => lv - rv,
        BinOp::Mul => lv * rv,
        BinOp::Div => {
            if rv == 0.0 {
                return None;
            }
            lv / rv
        }
        BinOp::Mod => {
            if rv == 0.0 {
                return None;
            }
            lv % rv
        }
        _ => return None,
    };
    Some(Value::Number(result))
}

/// Equality check for InList membership.
pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => (x - y).abs() < f64::EPSILON,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        _ => false,
    }
}

/// Evaluate basic function calls in guard context.
///
/// Supported functions:
/// - `contains(haystack, needle)` → Bool
/// - `startswith(text, prefix)` → Bool
/// - `endswith(text, suffix)` → Bool
/// - `substr(text, start [, length])` → Str
/// - `replace(text, pattern, replacement)` → Str
/// - `trim(s)` → Str
/// - `lower(s)` → Str
/// - `upper(s)` → Str
/// - `len(s)` → Number
/// - `mvcount(arr)` → Number
/// - `mvjoin(arr, sep)` → Str
/// - `mvindex(arr, idx [, end])` → scalar or Array
/// - `mvappend(v1, v2, ...)` → Array
/// - `split(text, sep)` → Array<Str>
/// - `mvdedup(arr)` → Array
/// - `abs(x)` → Number
/// - `round(x [, precision])` → Number
/// - `ceil(x)` → Number
/// - `floor(x)` → Number
/// - `sqrt(x)` → Number
/// - `pow(x, y)` → Number
/// - `log(x [, base])` → Number
/// - `exp(x)` → Number
/// - `clamp(x, min, max)` → Number
/// - `sign(x)` → Number
/// - `trunc(x)` → Number
/// - `is_finite(x)` → Bool
/// - `ltrim(s)` → Str
/// - `rtrim(s)` → Str
/// - `concat(v1, v2, ...)` → Str
/// - `indexof(text, needle)` → Number
/// - `replace_plain(text, from, to)` → Str
/// - `startswith_any(text, prefix1, prefix2, ...)` → Bool
/// - `endswith_any(text, suffix1, suffix2, ...)` → Bool
/// - `coalesce(v1, v2, ...)` → first non-null value
/// - `isnull(expr)` → Bool
/// - `isnotnull(expr)` → Bool
/// - `mvsort(arr)` → Array
/// - `mvreverse(arr)` → Array
/// - `strftime(timestamp_nanos, format)` → Str
/// - `strptime(text, format)` → Number (timestamp nanos)
fn eval_func_call(
    name: &str,
    args: &[Expr],
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    match name {
        "contains" => {
            if args.len() != 2 {
                return None;
            }
            let haystack = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let needle = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            Some(Value::Bool(haystack.contains(&*needle)))
        }
        "startswith" => {
            if args.len() != 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let prefix = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            Some(Value::Bool(text.starts_with(&prefix)))
        }
        "endswith" => {
            if args.len() != 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let suffix = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            Some(Value::Bool(text.ends_with(&suffix)))
        }
        "substr" => {
            if args.len() != 2 && args.len() != 3 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let start = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n.trunc() as i64,
                _ => return None,
            };
            let chars: Vec<char> = text.chars().collect();
            let len = chars.len() as i64;
            let mut start_idx = if start > 0 {
                start - 1
            } else if start < 0 {
                len + start
            } else {
                0
            };
            if start_idx < 0 {
                start_idx = 0;
            }
            if start_idx >= len {
                return Some(Value::Str(String::new()));
            }
            let mut end_idx = len;
            if args.len() == 3 {
                let length = match eval_expr_ext(&args[2], event, windows, baselines)? {
                    Value::Number(n) => n.trunc() as i64,
                    _ => return None,
                };
                if length <= 0 {
                    return Some(Value::Str(String::new()));
                }
                end_idx = (start_idx + length).min(len);
            }
            let sub = chars[start_idx as usize..end_idx as usize]
                .iter()
                .collect::<String>();
            Some(Value::Str(sub))
        }
        "replace" => {
            if args.len() != 3 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let pattern = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let replacement = match eval_expr_ext(&args[2], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let re = regex::Regex::new(&pattern).ok()?;
            Some(Value::Str(
                re.replace_all(&text, replacement.as_str()).into_owned(),
            ))
        }
        "trim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.trim().to_string())),
                _ => None,
            }
        }
        "lower" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.to_lowercase())),
                _ => None,
            }
        }
        "upper" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.to_uppercase())),
                _ => None,
            }
        }
        "len" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Number(s.len() as f64)),
                _ => None,
            }
        }
        "mvcount" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Array(arr) => Some(Value::Number(arr.len() as f64)),
                _ => None,
            }
        }
        "mvjoin" => {
            if args.len() != 2 {
                return None;
            }
            let arr = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Array(arr) => arr,
                _ => return None,
            };
            let sep = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let joined = arr
                .into_iter()
                .map(|v| value_to_string(&v))
                .collect::<Vec<_>>()
                .join(&sep);
            Some(Value::Str(joined))
        }
        "mvindex" => {
            if args.len() != 2 && args.len() != 3 {
                return None;
            }
            let arr = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Array(arr) => arr,
                _ => return None,
            };
            if args.len() == 2 {
                let idx = match eval_expr_ext(&args[1], event, windows, baselines)? {
                    Value::Number(n) => normalize_index(n.trunc() as i64, arr.len()),
                    _ => return None,
                }?;
                return arr.get(idx).cloned();
            }
            if arr.is_empty() {
                return Some(Value::Array(Vec::new()));
            }
            let start = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n.trunc() as i64,
                _ => return None,
            };
            let end = match eval_expr_ext(&args[2], event, windows, baselines)? {
                Value::Number(n) => n.trunc() as i64,
                _ => return None,
            };
            let len = arr.len() as i64;
            let mut start_idx = if start < 0 { len + start } else { start };
            let mut end_idx = if end < 0 { len + end } else { end };
            if end_idx < 0 || start_idx >= len {
                return Some(Value::Array(Vec::new()));
            }
            if start_idx < 0 {
                start_idx = 0;
            }
            if end_idx >= len {
                end_idx = len - 1;
            }
            if start_idx > end_idx {
                return Some(Value::Array(Vec::new()));
            }
            Some(Value::Array(
                arr[start_idx as usize..=end_idx as usize].to_vec(),
            ))
        }
        "mvappend" => {
            if args.is_empty() {
                return None;
            }
            let mut out: Vec<Value> = Vec::new();
            for arg in args {
                match eval_expr_ext(arg, event, windows, baselines)? {
                    Value::Array(values) => out.extend(values),
                    value => out.push(value),
                }
            }
            Some(Value::Array(out))
        }
        "split" => {
            if args.len() != 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let sep = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let parts = if sep.is_empty() {
                text.chars().map(|c| Value::Str(c.to_string())).collect()
            } else {
                text.split(&sep)
                    .map(|s| Value::Str(s.to_string()))
                    .collect()
            };
            Some(Value::Array(parts))
        }
        "mvdedup" => {
            if args.len() != 1 {
                return None;
            }
            let arr = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Array(arr) => arr,
                _ => return None,
            };
            let mut deduped: Vec<Value> = Vec::new();
            for v in arr {
                if !deduped.iter().any(|existing| values_equal(existing, &v)) {
                    deduped.push(v);
                }
            }
            Some(Value::Array(deduped))
        }
        "abs" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => Some(Value::Number(n.abs())),
                _ => None,
            }
        }
        "round" => {
            if args.len() != 1 && args.len() != 2 {
                return None;
            }
            let value = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let precision = if args.len() == 2 {
                match eval_expr_ext(&args[1], event, windows, baselines)? {
                    Value::Number(n) => f64_to_i64_trunc(n)?,
                    _ => return None,
                }
            } else {
                0
            };
            let rounded = round_with_precision(value, precision)?;
            Some(Value::Number(rounded))
        }
        "ceil" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => Some(Value::Number(n.ceil())),
                _ => None,
            }
        }
        "floor" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => Some(Value::Number(n.floor())),
                _ => None,
            }
        }
        "sqrt" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) if n >= 0.0 => Some(Value::Number(n.sqrt())),
                _ => None,
            }
        }
        "pow" => {
            if args.len() != 2 {
                return None;
            }
            let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let y = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let out = x.powf(y);
            if out.is_finite() {
                Some(Value::Number(out))
            } else {
                None
            }
        }
        "log" => {
            if args.len() != 1 && args.len() != 2 {
                return None;
            }
            let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            if x <= 0.0 {
                return None;
            }
            let out = if args.len() == 2 {
                let base = match eval_expr_ext(&args[1], event, windows, baselines)? {
                    Value::Number(n) => n,
                    _ => return None,
                };
                if base <= 0.0 || (base - 1.0).abs() < f64::EPSILON {
                    return None;
                }
                x.log(base)
            } else {
                x.ln()
            };
            if out.is_finite() {
                Some(Value::Number(out))
            } else {
                None
            }
        }
        "exp" => {
            if args.len() != 1 {
                return None;
            }
            let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let out = x.exp();
            if out.is_finite() {
                Some(Value::Number(out))
            } else {
                None
            }
        }
        "clamp" => {
            if args.len() != 3 {
                return None;
            }
            let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let min = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let max = match eval_expr_ext(&args[2], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            if min > max {
                return None;
            }
            Some(Value::Number(x.clamp(min, max)))
        }
        "sign" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) if n.is_finite() => Some(Value::Number(n.signum())),
                _ => None,
            }
        }
        "trunc" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => Some(Value::Number(n.trunc())),
                _ => None,
            }
        }
        "is_finite" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => Some(Value::Bool(n.is_finite())),
                _ => None,
            }
        }
        "ltrim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.trim_start().to_string())),
                _ => None,
            }
        }
        "rtrim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.trim_end().to_string())),
                _ => None,
            }
        }
        "concat" => {
            if args.is_empty() {
                return None;
            }
            let mut out = String::new();
            for arg in args {
                let value = eval_expr_ext(arg, event, windows, baselines)?;
                out.push_str(&value_to_string(&value));
            }
            Some(Value::Str(out))
        }
        "indexof" => {
            if args.len() != 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let needle = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let idx = text.find(&needle).map(|x| x as f64).unwrap_or(-1.0);
            Some(Value::Number(idx))
        }
        "replace_plain" => {
            if args.len() != 3 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let from = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let to = match eval_expr_ext(&args[2], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            Some(Value::Str(text.replace(&from, &to)))
        }
        "startswith_any" => {
            if args.len() < 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            for arg in &args[1..] {
                let prefix = match eval_expr_ext(arg, event, windows, baselines)? {
                    Value::Str(s) => s,
                    _ => return None,
                };
                if text.starts_with(&prefix) {
                    return Some(Value::Bool(true));
                }
            }
            Some(Value::Bool(false))
        }
        "endswith_any" => {
            if args.len() < 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            for arg in &args[1..] {
                let suffix = match eval_expr_ext(arg, event, windows, baselines)? {
                    Value::Str(s) => s,
                    _ => return None,
                };
                if text.ends_with(&suffix) {
                    return Some(Value::Bool(true));
                }
            }
            Some(Value::Bool(false))
        }
        "coalesce" => {
            if args.is_empty() {
                return None;
            }
            for arg in args {
                if let Some(v) = eval_expr_ext(arg, event, windows, baselines) {
                    return Some(v);
                }
            }
            None
        }
        "isnull" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Bool(
                eval_expr_ext(&args[0], event, windows, baselines).is_none(),
            ))
        }
        "isnotnull" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Bool(
                eval_expr_ext(&args[0], event, windows, baselines).is_some(),
            ))
        }
        "mvsort" => {
            if args.len() != 1 {
                return None;
            }
            let mut arr = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Array(arr) => arr,
                _ => return None,
            };
            arr.sort_by(compare_sortable_values);
            Some(Value::Array(arr))
        }
        "mvreverse" => {
            if args.len() != 1 {
                return None;
            }
            let mut arr = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Array(arr) => arr,
                _ => return None,
            };
            arr.reverse();
            Some(Value::Array(arr))
        }
        "strftime" => {
            if args.len() != 2 {
                return None;
            }
            let ts_nanos = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => f64_to_i64_trunc(n)?,
                _ => return None,
            };
            let fmt = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let dt = timestamp_nanos_to_utc(ts_nanos)?;
            Some(Value::Str(dt.format(&fmt).to_string()))
        }
        "strptime" => {
            if args.len() != 2 {
                return None;
            }
            let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let fmt = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let ts_nanos = parse_time_to_timestamp_nanos(&text, &fmt)?;
            Some(Value::Number(ts_nanos as f64))
        }
        "regex_match" => {
            if args.len() != 2 {
                return None;
            }
            let hay = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let pat = match eval_expr_ext(&args[1], event, windows, baselines)? {
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
            let t1 = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let t2 = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            Some(Value::Number((t1 - t2).abs() / 1_000_000_000.0))
        }
        "time_bucket" => {
            if args.len() != 2 {
                return None;
            }
            let t = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let interval = match eval_expr_ext(&args[1], event, windows, baselines)? {
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
        // L3 Collection functions - require instance context, not supported in guard context
        "collect_set" | "collect_list" | "first" | "last" => {
            // These functions need access to the instance's collected events
            // They are supported in yield/derive context via StepEvalContext
            None
        }
        // L3 Statistical functions - require instance context
        "stddev" | "percentile" => {
            // These functions need access to the instance's numeric values
            // They are supported in yield/derive context via StepEvalContext
            None
        }
        _ => None, // unsupported function
    }
}

fn compare_values(op: BinOp, lv: &Value, rv: &Value) -> bool {
    match (lv, rv) {
        (Value::Number(a), Value::Number(b)) => {
            let cmp = CmpOp::from_binop(op);
            compare_cmp(cmp, *a, *b)
        }
        (Value::Str(a), Value::Str(b)) => {
            let ord = a.cmp(b);
            match op {
                BinOp::Eq => ord.is_eq(),
                BinOp::Ne => !ord.is_eq(),
                BinOp::Lt => ord.is_lt(),
                BinOp::Gt => ord.is_gt(),
                BinOp::Le => ord.is_le(),
                BinOp::Ge => ord.is_ge(),
                _ => false,
            }
        }
        (Value::Bool(a), Value::Bool(b)) => match op {
            BinOp::Eq => a == b,
            BinOp::Ne => a != b,
            _ => false,
        },
        _ => false, // type mismatch
    }
}

fn compare_cmp(cmp: CmpOp, lhs: f64, rhs: f64) -> bool {
    match cmp {
        CmpOp::Eq => (lhs - rhs).abs() < f64::EPSILON,
        CmpOp::Ne => (lhs - rhs).abs() >= f64::EPSILON,
        CmpOp::Lt => lhs < rhs,
        CmpOp::Gt => lhs > rhs,
        CmpOp::Le => lhs <= rhs,
        CmpOp::Ge => lhs >= rhs,
        _ => false,
    }
}

/// Helper trait to convert BinOp comparison variants to CmpOp.
trait FromBinOp {
    fn from_binop(op: BinOp) -> Self;
}

impl FromBinOp for CmpOp {
    fn from_binop(op: BinOp) -> Self {
        match op {
            BinOp::Eq => CmpOp::Eq,
            BinOp::Ne => CmpOp::Ne,
            BinOp::Lt => CmpOp::Lt,
            BinOp::Gt => CmpOp::Gt,
            BinOp::Le => CmpOp::Le,
            BinOp::Ge => CmpOp::Ge,
            _ => CmpOp::Eq, // fallback (should not be reached for comparison ops)
        }
    }
}

// ---------------------------------------------------------------------------
// Threshold expression evaluation
// ---------------------------------------------------------------------------

/// Try to evaluate a threshold expression to f64.
/// Returns `Some(f64)` for Number, Neg, and constant arithmetic (BinOp on
/// numeric literals).  Returns `None` for expressions that cannot be
/// statically resolved to a number (field refs, function calls, etc.)
/// — callers must fall back to value-based comparison.
pub(super) fn try_eval_expr_to_f64(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Number(n) => Some(*n),
        Expr::Neg(inner) => try_eval_expr_to_f64(inner).map(|v| -v),
        Expr::BinOp { op, left, right } => {
            let l = try_eval_expr_to_f64(left)?;
            let r = try_eval_expr_to_f64(right)?;
            match op {
                BinOp::Add => Some(l + r),
                BinOp::Sub => Some(l - r),
                BinOp::Mul => Some(l * r),
                BinOp::Div => {
                    if r == 0.0 {
                        None
                    } else {
                        Some(l / r)
                    }
                }
                BinOp::Mod => {
                    if r == 0.0 {
                        None
                    } else {
                        Some(l % r)
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Try to evaluate a threshold expression to a [`Value`].
/// Returns `Some` for literal constants (Number, String, Bool) and
/// constant arithmetic (Neg, BinOp on numeric literals).
/// Returns `None` for non-constant expressions (field refs, func calls, etc.).
pub(super) fn try_eval_expr_to_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => try_eval_expr_to_f64(expr).map(Value::Number),
    }
}

fn coerce_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
}

fn compare_sortable_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => value_to_string(a).cmp(&value_to_string(b)),
    }
}

fn f64_to_i64_trunc(v: f64) -> Option<i64> {
    if !v.is_finite() {
        return None;
    }
    let truncated = v.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return None;
    }
    Some(truncated as i64)
}

fn round_with_precision(value: f64, precision: i64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }
    if precision >= 0 {
        let p = i32::try_from(precision).ok()?;
        let factor = 10_f64.powi(p);
        if !factor.is_finite() || factor == 0.0 {
            return None;
        }
        Some((value * factor).round() / factor)
    } else {
        let p = i32::try_from(-precision).ok()?;
        let factor = 10_f64.powi(p);
        if !factor.is_finite() || factor == 0.0 {
            return None;
        }
        Some((value / factor).round() * factor)
    }
}

fn timestamp_nanos_to_utc(timestamp_nanos: i64) -> Option<DateTime<Utc>> {
    let secs = timestamp_nanos.div_euclid(1_000_000_000);
    let nanos = timestamp_nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

fn parse_time_to_timestamp_nanos(text: &str, fmt: &str) -> Option<i64> {
    if let Ok(dt) = DateTime::parse_from_str(text, fmt) {
        return dt.timestamp_nanos_opt();
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, fmt) {
        return dt.and_utc().timestamp_nanos_opt();
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, fmt) {
        return date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_nanos_opt();
    }
    None
}
