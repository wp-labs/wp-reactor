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
        (Value::Str(a), Value::Str(b)) => compare_strs(op, a, b),
        (Value::Bool(a), Value::Bool(b)) => compare_bools(op, *a, *b),
        _ => false, // type mismatch
    }
}

/// 字符串按字典序比较六种关系。
fn compare_strs(op: BinOp, a: &str, b: &str) -> bool {
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

/// 布尔仅支持等/不等。
fn compare_bools(op: BinOp, a: bool, b: bool) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Ne => a != b,
        _ => false,
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
            fold_f64_binop(op, l, r)
        }
        _ => None,
    }
}

/// 常量折叠的 f64 算术（除/模零 → None; 非算术算子 → None）。
fn fold_f64_binop(op: &BinOp, l: f64, r: f64) -> Option<f64> {
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
    let p = i32::try_from(precision.unsigned_abs()).ok()?;
    let factor = 10_f64.powi(p);
    if !factor.is_finite() || factor == 0.0 {
        return None;
    }
    if precision >= 0 {
        Some((value * factor).round() / factor)
    } else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use wf_lang::ast::FieldRef;

    fn num(v: f64) -> Value {
        Value::Number(v)
    }

    fn strv(v: &str) -> Value {
        Value::Str(v.into())
    }

    fn b(v: bool) -> Value {
        Value::Bool(v)
    }

    fn en(v: f64) -> Expr {
        Expr::Number(v)
    }

    fn bin(op: BinOp, l: Expr, r: Expr) -> Expr {
        Expr::BinOp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        }
    }

    #[test]
    fn compare_values_strings_bools_and_mismatch() {
        assert!(compare_values(BinOp::Eq, &strv("a"), &strv("a")));
        assert!(compare_values(BinOp::Ne, &strv("a"), &strv("b")));
        assert!(compare_values(BinOp::Lt, &strv("a"), &strv("b")));
        assert!(compare_values(BinOp::Le, &strv("a"), &strv("a")));
        assert!(compare_values(BinOp::Gt, &strv("b"), &strv("a")));
        assert!(compare_values(BinOp::Ge, &strv("a"), &strv("a")));
        assert!(compare_values(BinOp::Eq, &num(1.0), &num(1.0 + 1e-16))); // epsilon 内相等
        assert!(compare_values(BinOp::Ne, &num(1.0), &num(1.0 + 1e-6)));
        assert!(compare_values(BinOp::Lt, &num(1.0), &num(1.000001)));
        assert!(compare_values(BinOp::Eq, &b(true), &b(true)));
        assert!(compare_values(BinOp::Ne, &b(true), &b(false)));
        // 类型不匹配 / 非比较 op → false
        assert!(!compare_values(BinOp::Eq, &num(1.0), &strv("1")));
        assert!(!compare_values(BinOp::Eq, &strv("a"), &b(true)));
        assert!(!compare_values(BinOp::Add, &strv("a"), &strv("b")));
        assert!(!compare_values(BinOp::Le, &b(false), &b(true)));
    }

    #[test]
    fn try_const_fold_arithmetic_and_neg() {
        assert_eq!(try_eval_expr_to_f64(&en(3.5)), Some(3.5));
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Neg(Box::new(en(2.0)))),
            Some(-2.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Add, en(1.0), bin(BinOp::Mul, en(2.0), en(3.0)))),
            Some(7.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Div, en(1.0), en(0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Mod, en(1.0), en(0.0))),
            None
        );
        // 非数字常量表达式 → None
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Field(FieldRef::Simple("x".into()))),
            None
        );
        assert_eq!(try_eval_expr_to_f64(&Expr::StringLit("1".into())), None);
    }

    #[test]
    fn try_eval_expr_to_value_literals() {
        assert_eq!(
            try_eval_expr_to_value(&Expr::StringLit("hi".into())),
            Some(strv("hi"))
        );
        assert_eq!(try_eval_expr_to_value(&Expr::Bool(true)), Some(b(true)));
        assert_eq!(
            try_eval_expr_to_value(&bin(BinOp::Mul, en(2.0), en(3.0))),
            Some(num(6.0))
        );
        assert_eq!(
            try_eval_expr_to_value(&Expr::Field(FieldRef::Simple("x".into()))),
            None
        );
    }

    #[test]
    fn index_and_f64_truncation_helpers() {
        assert_eq!(normalize_index(-1, 5), Some(4));
        assert_eq!(normalize_index(0, 5), Some(0));
        assert_eq!(normalize_index(4, 5), Some(4));
        assert_eq!(normalize_index(5, 5), None);
        assert_eq!(normalize_index(-6, 5), None);
        assert_eq!(f64_to_i64_trunc(2.9), Some(2));
        assert_eq!(f64_to_i64_trunc(-2.9), Some(-2));
        assert_eq!(f64_to_i64_trunc(f64::INFINITY), None);
        assert_eq!(f64_to_i64_trunc(1e300), None);
        assert_eq!(coerce_to_f64(&num(1.5)), Some(1.5));
        assert_eq!(coerce_to_f64(&strv("1.5")), None);
    }

    #[test]
    fn round_with_precision_both_directions() {
        let close = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert!(close(round_with_precision(2.345, 2).unwrap(), 2.35));
        assert!(close(round_with_precision(2.5, 0).unwrap(), 3.0));
        assert!(close(round_with_precision(-2.5, 0).unwrap(), -3.0));
        assert!(close(round_with_precision(1234.0, -2).unwrap(), 1200.0));
        assert!(close(round_with_precision(1250.0, -2).unwrap(), 1300.0));
        assert_eq!(round_with_precision(f64::NAN, 2), None);
        assert_eq!(round_with_precision(1.0, 400), None); // 10^400 溢出 → None
    }

    #[test]
    fn ordering_and_format_and_time_helpers() {
        assert_eq!(
            compare_sortable_values(&num(1.0), &num(2.0)),
            Ordering::Less
        );
        assert_eq!(
            compare_sortable_values(&strv("b"), &strv("a")),
            Ordering::Greater
        );
        assert_eq!(compare_sortable_values(&b(false), &b(true)), Ordering::Less);
        assert_eq!(
            apply_fmt_template("a={} b={}", &[num(1.0), strv("x")]),
            Some("a=1 b=x".to_string())
        );
        assert_eq!(
            apply_fmt_template("no placeholders", &[]),
            Some("no placeholders".to_string())
        );
        assert_eq!(apply_fmt_template("{}", &[]), None);
        assert_eq!(apply_fmt_template("{}", &[num(1.0), num(2.0)]), None);
        assert!(is_blank_str("  \t "));
        assert!(!is_blank_str("x"));
        assert_eq!(
            parse_time_to_timestamp_nanos("2023-11-14 22:13:20", "%Y-%m-%d %H:%M:%S"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            timestamp_nanos_to_utc(1_700_000_000_000_000_000).map(|dt| dt.timestamp()),
            Some(1_700_000_000)
        );
        let now = current_time_nanos().expect("now");
        assert!(now > 1_700_000_000_000_000_000);
        assert_eq!(current_time_nanos(), Some(now)); // 同一次求值内缓存
    }

    #[test]
    fn stable_id_hash_is_tagged_and_deterministic() {
        let hash_text = |v: &Value| {
            let mut h = Sha256::new();
            update_stable_id_hash(&mut h, v).unwrap();
            h.finalize().to_vec()
        };
        let a = hash_text(&strv("ab"));
        assert_eq!(a, hash_text(&strv("ab")));
        assert_ne!(a, hash_text(&strv("ac")));
        assert_ne!(hash_text(&num(1.0)), hash_text(&strv("1"))); // 类型标签参与
        // 容器类型不参与
        let mut h = Sha256::new();
        assert!(update_stable_id_hash(&mut h, &Value::Array(vec![num(1.0)])).is_none());
    }
    #[test]
    fn const_fold_zero_guards_and_non_arithmetic_ops() {
        // 除/模零（含 -0.0）→ None; 非算术算子 → None（fold_f64_binop 提取回归）
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Div, en(1.0), en(-0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Mod, en(7.0), en(-0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Sub, en(5.0), en(2.0))),
            Some(3.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Eq, en(1.0), en(1.0))),
            None
        );
        // 未知名路径: 与既有的字面折叠一致
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Bool(true)),
            None,
            "Bool 字面量非数值"
        );
    }
}
