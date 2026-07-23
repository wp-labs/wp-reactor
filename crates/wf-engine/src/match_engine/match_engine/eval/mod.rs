use std::{cell::Cell, collections::HashMap};

use wf_lang::ast::{BinOp, Expr};

use super::key::{field_ref_name, value_to_string};
use super::types::{Event, RollingStats, Value, WindowLookup};

mod cmp;
mod funcs;

use cmp::{coerce_to_f64, compare_values};
use funcs::eval_func_call;

// Re-export items from sub-modules that sibling modules (step, close) need.
pub(super) use cmp::{try_eval_expr_to_f64, try_eval_expr_to_value};

thread_local! {
    pub(super) static EVAL_TIME_NANOS: Cell<Option<i64>> = const { Cell::new(None) };
    static EVAL_TIME_SCOPE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct EvalTimeScope;

impl EvalTimeScope {
    fn enter() -> Self {
        EVAL_TIME_SCOPE_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

impl Drop for EvalTimeScope {
    fn drop(&mut self) {
        EVAL_TIME_SCOPE_DEPTH.with(|depth| {
            let next_depth = depth.get().saturating_sub(1);
            depth.set(next_depth);
            if next_depth == 0 {
                EVAL_TIME_NANOS.with(|time| time.set(None));
            }
        });
    }
}

fn with_eval_time_scope<T>(f: impl FnOnce() -> T) -> T {
    let _scope = EvalTimeScope::enter();
    f()
}

// ---------------------------------------------------------------------------
// Expression evaluator (L1)
// ---------------------------------------------------------------------------

/// Evaluate an expression against an event, returning a [`Value`].
///
/// Supports: literals, field refs, BinOp (And/Or/comparisons/arithmetic),
/// Neg, InList, and basic FuncCall (contains, startswith, endswith, substr, replace, trim, lower, upper, len, mvcount, mvjoin, mvindex, mvappend, split, mvdedup, abs, round, ceil, floor, sqrt, pow, log, exp, clamp, sign, trunc, is_finite, ltrim, rtrim, concat, join, join_by, indexof, replace_plain, startswith_any, endswith_any, coalesce, merge, isnull, isnotnull, is_blank, null_if_blank, default_if_blank, md5, sha1, sha1_n, sha256, hex, stable_id, mvsort, mvreverse, now, now_s, now_ms, now_us, now_ns, strftime, strptime, has, baseline).
pub(crate) fn eval_expr(expr: &Expr, event: &Event) -> Option<Value> {
    with_eval_time_scope(|| {
        let mut empty = HashMap::new();
        eval_expr_ext(expr, event, None, &mut empty)
    })
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
    let _time_scope = EvalTimeScope::enter();
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Field(fr) => {
            let name = field_ref_name(fr);
            event.fields.get(name).cloned()
        }
        Expr::Object(items) => {
            let mut map = HashMap::new();
            for item in items {
                let value = eval_expr_ext(&item.value, event, windows, baselines)?;
                for target in &item.targets {
                    map.insert(target.clone(), value.clone());
                }
            }
            Some(Value::Object(map))
        }
        Expr::Array(items) => items
            .iter()
            .map(|item| eval_expr_ext(item, event, windows, baselines))
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
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
