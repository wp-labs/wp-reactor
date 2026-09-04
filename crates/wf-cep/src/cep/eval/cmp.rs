use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sha2::{Digest, Sha256};
use wf_lang::ast::{BinOp, CmpOp, Expr};

use crate::time::epoch_nanos_to_millis;

use super::super::key::value_to_string;
use super::super::types::{EngineHashMap, FieldSource, RollingStats, Value, WindowLookup};
use super::eval_expr_ext;

pub(super) fn compare_values(op: BinOp, lv: &Value, rv: &Value) -> bool {
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
pub fn try_eval_expr_to_f64(expr: &Expr) -> Option<f64> {
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
pub fn try_eval_expr_to_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => try_eval_expr_to_f64(expr).map(Value::Number),
    }
}

pub(super) fn coerce_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

pub(super) fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
}

pub(super) fn compare_sortable_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => value_to_string(a).cmp(&value_to_string(b)),
    }
}

pub(super) fn f64_to_i64_trunc(v: f64) -> Option<i64> {
    if !v.is_finite() {
        return None;
    }
    let truncated = v.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return None;
    }
    Some(truncated as i64)
}

pub(super) fn round_with_precision(value: f64, precision: i64) -> Option<f64> {
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

pub fn timestamp_nanos_to_utc(timestamp_nanos: i64) -> Option<DateTime<Utc>> {
    let secs = timestamp_nanos.div_euclid(1_000_000_000);
    let nanos = timestamp_nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

pub(super) fn time_nanos_to_value(nanos: i64) -> Value {
    Value::Number(epoch_nanos_to_millis(nanos) as f64)
}

pub(super) fn parse_time_to_timestamp_nanos(text: &str, fmt: &str) -> Option<i64> {
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

pub(super) fn current_time_nanos() -> Option<i64> {
    super::EVAL_TIME_NANOS.with(|time| {
        if let Some(nanos) = time.get() {
            return Some(nanos);
        }
        let nanos = Utc::now().timestamp_nanos_opt()?;
        time.set(Some(nanos));
        Some(nanos)
    })
}

pub(super) fn is_blank_str(value: &str) -> bool {
    value.trim().is_empty()
}

pub(super) fn eval_single_string_arg(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

pub(super) fn update_stable_id_hash(hasher: &mut Sha256, value: &Value) -> Option<()> {
    let (tag, text) = match value {
        Value::Number(_) => ("n", value_to_string(value)),
        Value::Str(s) => ("s", s.to_string()),
        Value::Bool(_) => ("b", value_to_string(value)),
        Value::Array(_) | Value::Object(_) => return None,
    };
    hasher.update(tag.as_bytes());
    hasher.update(b":");
    hasher.update(text.len().to_string().as_bytes());
    hasher.update(b":");
    hasher.update(text.as_bytes());
    hasher.update(b";");
    Some(())
}

pub fn apply_fmt_template(template: &str, values: &[Value]) -> Option<String> {
    let placeholders = template.matches("{}").count();
    if placeholders != values.len() {
        return None;
    }
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    for value in values {
        let (head, tail) = rest.split_once("{}")?;
        rendered.push_str(head);
        rendered.push_str(&value_to_string(value));
        rest = tail;
    }
    rendered.push_str(rest);
    Some(rendered)
}
