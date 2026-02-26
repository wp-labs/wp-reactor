use std::collections::HashMap;

use wf_lang::ast::{BinOp, CmpOp, Expr};

use super::key::{field_ref_name, value_to_string};
use super::types::{Event, RollingStats, Value, WindowLookup};

// ---------------------------------------------------------------------------
// Expression evaluator (L1)
// ---------------------------------------------------------------------------

/// Evaluate an expression against an event, returning a [`Value`].
///
/// Supports: literals, field refs, BinOp (And/Or/comparisons/arithmetic),
/// Neg, InList, and basic FuncCall (contains, lower, upper, len, has, baseline).
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
/// - `lower(s)` → Str
/// - `upper(s)` → Str
/// - `len(s)` → Number
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
