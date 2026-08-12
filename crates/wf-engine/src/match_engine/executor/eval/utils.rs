use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sha2::{Digest, Sha256};

use super::{Event, Value, YieldMeta, eval_expr_with_l3, get_or_init_eval_time_nanos};
use crate::match_engine::match_engine::value_to_string;
use crate::time::epoch_nanos_to_millis;

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

pub(super) fn apply_fmt_template(template: &str, values: &[Value]) -> Option<String> {
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

pub(super) fn timestamp_nanos_to_utc(timestamp_nanos: i64) -> Option<DateTime<Utc>> {
    let secs = timestamp_nanos.div_euclid(1_000_000_000);
    let nanos = timestamp_nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

pub(super) fn time_nanos_to_value(nanos: i64) -> Value {
    Value::Number(epoch_nanos_to_millis(nanos) as f64)
}

pub(super) fn time_nanos_to_expr(nanos: i64) -> wf_lang::ast::Expr {
    wf_lang::ast::Expr::Number(epoch_nanos_to_millis(nanos) as f64)
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

pub(super) fn is_blank_str(value: &str) -> bool {
    value.trim().is_empty()
}

pub(super) fn current_time_nanos() -> Option<i64> {
    get_or_init_eval_time_nanos()
}

pub(super) fn eval_single_string_arg_with_l3(
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
    score: YieldMeta,
) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
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
