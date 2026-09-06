use md5::Digest as Md5Digest;
use md5::Md5;
use sha1::Digest as Sha1Digest;
use sha1::Sha1;
use sha2::Sha256;
use wf_lang::ast::Expr;

use crate::time::{normalize_epoch_timestamp_float_nanos, positive_interval_seconds_to_nanos};

use super::super::key::value_to_string;
use super::super::types::{EngineHashMap, FieldSource, RollingStats, Value, WindowLookup};
use super::cmp::{
    apply_fmt_template, compare_sortable_values, current_time_nanos, eval_single_string_arg,
    f64_to_i64_trunc, is_blank_str, normalize_index, parse_time_to_timestamp_nanos,
    round_with_precision, time_nanos_to_value, timestamp_nanos_to_utc, update_stable_id_hash,
};
use super::{eval_expr_ext, values_equal};

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
/// - `fmt(template, v1, v2, ...)` → Str
/// - `concat(v1, v2, ...)` → Str
/// - `join(v1, v2, ...)` → Str
/// - `join_by(separator, v1, v2, ...)` → Str
/// - `indexof(text, needle)` → Number
/// - `replace_plain(text, from, to)` → Str
/// - `startswith_any(text, prefix1, prefix2, ...)` → Bool
/// - `endswith_any(text, suffix1, suffix2, ...)` → Bool
/// - `coalesce(v1, v2, ...)` → first non-null and non-blank string value
/// - `merge(obj1, obj2, ...)` → shallow left-to-right object merge
/// - `isnull(expr)` → Bool
/// - `isnotnull(expr)` → Bool
/// - `is_blank(expr)` → Bool
/// - `null_if_blank(expr)` → Str or null
/// - `default_if_blank(expr, default)` → Str
/// - `md5(text)` / `sha1(text)` / `sha1_n(text, length)` / `sha256(text)` → lowercase hex string
/// - `hex(text)` → lowercase hex string
/// - `stable_id(prefix, value, ...)` → `prefix` + first 16 chars of SHA-256 over typed, length-prefixed values
/// - `mvsort(arr)` → Array
/// - `mvreverse(arr)` → Array
/// - `now()` → Number (timestamp millis)
/// - `now_s()` → Number (timestamp seconds)
/// - `now_ms()` → Number (timestamp millis)
/// - `now_us()` → Number (timestamp micros)
/// - `now_ns()` → Number (timestamp nanos)
/// - `strftime(timestamp, format)` → Str
/// - `strptime(text, format)` → Number (timestamp millis)
/// - `regex_match(text, pattern)` → Bool
/// - `cidr_match(ip, subnet)` → Bool（Sigma `|cidr` 等效，subnet 形如 `"10.0.0.0/8"`）
/// - `time_diff(t1, t2)` → Number (seconds)
/// - `time_bucket(t, interval_seconds)` → Number (timestamp millis)
/// - `external("service", arg1, ...)` → dispatched
pub(super) fn eval_func_call(
    name: &str,
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let handler = eval_handler(name)?;
    handler(args, event, windows, baselines)
}

type FuncHandler = fn(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value>;

fn eval_func_count_char(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // Flink q14 UDF 同款：count_char(text, ch) = ch 在 text 中的出现次数。
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
    if needle.is_empty() {
        return Some(Value::Number(0.0));
    }
    let ch = needle.chars().next().unwrap();
    Some(Value::Number(
        text.chars().filter(|&c| c == ch).count() as f64
    ))
}

fn eval_func_contains(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_startswith(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    Some(Value::Bool(text.starts_with(prefix.as_str())))
}

fn eval_func_endswith(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    Some(Value::Bool(text.ends_with(suffix.as_str())))
}

fn eval_func_substr(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
        return Some(Value::Str(String::new().into()));
    }
    let mut end_idx = len;
    if args.len() == 3 {
        let length = match eval_expr_ext(&args[2], event, windows, baselines)? {
            Value::Number(n) => n.trunc() as i64,
            _ => return None,
        };
        if length <= 0 {
            return Some(Value::Str(String::new().into()));
        }
        end_idx = (start_idx + length).min(len);
    }
    let sub = chars[start_idx as usize..end_idx as usize]
        .iter()
        .collect::<String>();
    Some(Value::Str(sub.into()))
}

fn eval_func_replace(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
        re.replace_all(text.as_str(), replacement.as_str())
            .into_owned()
            .into(),
    ))
}

fn eval_func_trim(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(Value::Str(s.trim().to_string().into())),
        _ => None,
    }
}

fn eval_func_lower(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(Value::Str(s.to_lowercase().into())),
        _ => None,
    }
}

fn eval_func_upper(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(Value::Str(s.to_uppercase().into())),
        _ => None,
    }
}

fn eval_func_len(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(Value::Number(s.len() as f64)),
        _ => None,
    }
}

fn eval_func_mvcount(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Array(arr) => Some(Value::Number(arr.len() as f64)),
        _ => None,
    }
}

fn eval_func_mvjoin(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    Some(Value::Str(joined.into()))
}

fn eval_func_mvindex(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    let start_idx = if start < 0 { len + start } else { start };
    let end_idx = if end < 0 { len + end } else { end };
    let Some((from, to)) = inclusive_slice_bounds(start_idx, end_idx, len) else {
        return Some(Value::Array(Vec::new()));
    };
    Some(Value::Array(arr[from..=to].to_vec()))
}

/// mvindex 闭区间切片边界：负下标按长度换算后 clamp，非法区间返回 `None`。
fn inclusive_slice_bounds(start: i64, end: i64, len: i64) -> Option<(usize, usize)> {
    let start = start.max(0);
    if end < 0 || start >= len {
        return None;
    }
    let end = end.min(len - 1);
    (start <= end).then_some((start as usize, end as usize))
}

fn eval_func_mvappend(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_split(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
        text.chars()
            .map(|c| Value::Str(c.to_string().into()))
            .collect()
    } else {
        text.split(sep.as_str())
            .map(|s| Value::Str(s.to_string().into()))
            .collect()
    };
    Some(Value::Array(parts))
}

fn eval_func_mvdedup(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_abs(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.abs())),
        _ => None,
    }
}

fn eval_func_round(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_ceil(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.ceil())),
        _ => None,
    }
}

fn eval_func_floor(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.floor())),
        _ => None,
    }
}

fn eval_func_sqrt(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) if n >= 0.0 => Some(Value::Number(n.sqrt())),
        _ => None,
    }
}

fn eval_func_pow(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_log(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_exp(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_clamp(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_sign(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) if n.is_finite() => Some(Value::Number(n.signum())),
        _ => None,
    }
}

fn eval_func_trunc(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.trunc())),
        _ => None,
    }
}

fn eval_func_is_finite(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Bool(n.is_finite())),
        _ => None,
    }
}

fn eval_func_ltrim(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(Value::Str(s.trim_start().to_string().into())),
        _ => None,
    }
}

fn eval_func_rtrim(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(Value::Str(s.trim_end().to_string().into())),
        _ => None,
    }
}

fn eval_func_fmt(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let template = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let values = args[1..]
        .iter()
        .map(|arg| eval_expr_ext(arg, event, windows, baselines))
        .collect::<Option<Vec<_>>>()?;
    Some(Value::Str(
        apply_fmt_template(template.as_str(), &values)?.into(),
    ))
}

fn eval_func_concat(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut out = String::new();
    for arg in args {
        let value = eval_expr_ext(arg, event, windows, baselines)?;
        out.push_str(&value_to_string(&value));
    }
    Some(Value::Str(out.into()))
}

fn eval_func_join(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut out = String::new();
    for arg in args {
        out.push_str(&eval_join_arg(arg, event, windows, baselines)?);
    }
    Some(Value::Str(out.into()))
}

fn eval_func_join_by(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let sep = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let mut parts = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        parts.push(eval_join_arg(arg, event, windows, baselines)?);
    }
    Some(Value::Str(parts.join(&sep).into()))
}

fn eval_func_indexof(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    let idx = text.find(needle.as_str()).map(|x| x as f64).unwrap_or(-1.0);
    Some(Value::Number(idx))
}

fn eval_func_replace_plain(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    Some(Value::Str(text.replace(from.as_str(), to.as_str()).into()))
}

fn eval_func_startswith_any(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
        if text.starts_with(prefix.as_str()) {
            return Some(Value::Bool(true));
        }
    }
    Some(Value::Bool(false))
}

fn eval_func_endswith_any(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
        if text.ends_with(suffix.as_str()) {
            return Some(Value::Bool(true));
        }
    }
    Some(Value::Bool(false))
}

fn eval_func_coalesce(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    for arg in args {
        if let Some(v) = eval_expr_ext(arg, event, windows, baselines) {
            if matches!(&v, Value::Str(s) if is_blank_str(s)) {
                continue;
            }
            return Some(v);
        }
    }
    None
}

fn eval_func_merge(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut merged = EngineHashMap::default();
    for arg in args {
        match eval_merge_arg(arg, event, windows, baselines) {
            Some(Value::Object(fields)) => merged.extend(fields),
            None if matches!(arg, Expr::Field(_)) => {}
            None => return None,
            Some(_) => return None,
        }
    }
    Some(Value::Object(merged))
}

fn eval_func_isnull(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    Some(Value::Bool(
        eval_expr_ext(&args[0], event, windows, baselines).is_none(),
    ))
}

fn eval_func_isnotnull(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    Some(Value::Bool(
        eval_expr_ext(&args[0], event, windows, baselines).is_some(),
    ))
}

fn eval_func_is_blank(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines) {
        Some(Value::Str(s)) => Some(Value::Bool(is_blank_str(&s))),
        None => Some(Value::Bool(true)),
        Some(_) => None,
    }
}

fn eval_func_null_if_blank(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) if is_blank_str(&s) => None,
        Value::Str(s) => Some(Value::Str(s)),
        _ => None,
    }
}

fn eval_func_default_if_blank(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines) {
        Some(Value::Str(s)) if !is_blank_str(&s) => Some(Value::Str(s)),
        Some(Value::Str(_)) | None => match eval_expr_ext(&args[1], event, windows, baselines)? {
            Value::Str(s) => Some(Value::Str(s)),
            _ => None,
        },
        Some(_) => None,
    }
}

fn eval_func_md5(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let text = eval_single_string_arg(args, event, windows, baselines)?;
    Some(Value::Str(
        hex::encode(<Md5 as Md5Digest>::digest(text.as_bytes())).into(),
    ))
}

fn eval_func_sha1(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let text = eval_single_string_arg(args, event, windows, baselines)?;
    Some(Value::Str(
        hex::encode(<Sha1 as Sha1Digest>::digest(text.as_bytes())).into(),
    ))
}

fn eval_func_sha1_n(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let len = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 => n as usize,
        _ => return None,
    };
    if !(1..=40).contains(&len) {
        return None;
    }
    let digest = hex::encode(<Sha1 as Sha1Digest>::digest(text.as_bytes()));
    Some(Value::Str(digest[..len].to_string().into()))
}

fn eval_func_sha256(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let text = eval_single_string_arg(args, event, windows, baselines)?;
    Some(Value::Str(
        hex::encode(Sha256::digest(text.as_bytes())).into(),
    ))
}

fn eval_func_hex(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let text = eval_single_string_arg(args, event, windows, baselines)?;
    Some(Value::Str(hex::encode(text.as_bytes()).into()))
}

fn eval_func_stable_id(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let prefix = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let mut hasher = Sha256::new();
    for arg in &args[1..] {
        let value = eval_expr_ext(arg, event, windows, baselines)?;
        update_stable_id_hash(&mut hasher, &value)?;
    }
    let digest = hex::encode(hasher.finalize());
    Some(Value::Str(format!("{}{}", prefix, &digest[..16]).into()))
}

fn eval_func_mvsort(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_mvreverse(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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

fn eval_func_now(
    args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(time_nanos_to_value(current_time_nanos()?))
}

fn eval_func_now_s(
    args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(Value::Number(
        (current_time_nanos()? / 1_000_000_000) as f64,
    ))
}

fn eval_func_now_us(
    args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(Value::Number((current_time_nanos()? / 1_000) as f64))
}

fn eval_func_now_ns(
    args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(Value::Number(current_time_nanos()? as f64))
}

fn eval_func_strftime(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 && args.len() != 2 {
        return None;
    }
    let ts_nanos = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let fmt = if let Some(fmt_expr) = args.get(1) {
        match eval_expr_ext(fmt_expr, event, windows, baselines)? {
            Value::Str(s) => s.to_string(),
            _ => return None,
        }
    } else {
        wf_lang::DEFAULT_OUTPUT_TIME_FORMAT.to_string()
    };
    let dt = timestamp_nanos_to_utc(ts_nanos)?;
    Some(Value::Str(dt.format(&fmt).to_string().into()))
}

fn eval_func_strptime(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    Some(time_nanos_to_value(ts_nanos))
}

fn eval_func_regex_match(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
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
    let re = crate::regex_cache::cached_regex(&pat)?;
    Some(Value::Bool(re.is_match(&hay)))
}

fn eval_func_cidr_match(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let ip = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let cidr = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let net = crate::cidr_cache::cached_cidr(&cidr)?;
    Some(Value::Bool(net.contains(&ip)))
}

fn eval_func_time_diff(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let t1 = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let t2 = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    Some(Value::Number((t1 - t2).abs() as f64 / 1_000_000_000.0))
}

fn eval_func_time_bucket(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let t = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let interval = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let interval_nanos = positive_interval_seconds_to_nanos(interval)?;
    let bucketed = t.div_euclid(interval_nanos) * interval_nanos;
    Some(time_nanos_to_value(bucketed))
}

fn eval_func_bucket_end(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // 桶末：`bucket_end(t, interval) = time_bucket(t, interval) + interval`
    //（Q8 形态：`within [p.dateTime, <bucket_end(p.dateTime, 10s)]` 表达上开桶）
    if args.len() != 2 {
        return None;
    }
    let t = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let interval = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let interval_nanos = positive_interval_seconds_to_nanos(interval)?;
    let bucketed = t.div_euclid(interval_nanos) * interval_nanos;
    Some(time_nanos_to_value(bucketed.checked_add(interval_nanos)?))
}

// L3 Collection functions - require instance context, not supported in guard context
fn eval_func_collect_set(
    _args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // These functions need access to the instance's collected events
    // They are supported in yield/derive context via StepEvalContext
    None
}

// L3 Statistical functions - require instance context
fn eval_func_stddev(
    _args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // These functions need access to the instance's numeric values
    // They are supported in yield/derive context via StepEvalContext
    None
}

fn eval_func_external(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // external("service", arg1, ...) — dispatch to the global
    // ExternalCallHandler (wp_knowledge facade). Uses the shared
    // `eval_external` helper so the arg-parsing logic is identical
    // to the executor/eval.rs path.
    crate::external::eval_external(&args[0], &args[1..], |a| {
        eval_expr_ext(a, event, windows, baselines)
    })
}

/// name → handler 注册表（OnceLock 惰性构建，单线程只写一次）。
fn eval_handler(name: &str) -> Option<FuncHandler> {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    static HANDLERS: OnceLock<HashMap<&'static str, FuncHandler>> = OnceLock::new();
    let map = HANDLERS.get_or_init(|| {
        let mut map: HashMap<&'static str, FuncHandler> = HashMap::new();
        map.insert("count_char", eval_func_count_char);
        map.insert("contains", eval_func_contains);
        map.insert("startswith", eval_func_startswith);
        map.insert("endswith", eval_func_endswith);
        map.insert("substr", eval_func_substr);
        map.insert("replace", eval_func_replace);
        map.insert("trim", eval_func_trim);
        map.insert("lower", eval_func_lower);
        map.insert("upper", eval_func_upper);
        map.insert("len", eval_func_len);
        map.insert("mvcount", eval_func_mvcount);
        map.insert("mvjoin", eval_func_mvjoin);
        map.insert("mvindex", eval_func_mvindex);
        map.insert("mvappend", eval_func_mvappend);
        map.insert("split", eval_func_split);
        map.insert("mvdedup", eval_func_mvdedup);
        map.insert("abs", eval_func_abs);
        map.insert("round", eval_func_round);
        map.insert("ceil", eval_func_ceil);
        map.insert("floor", eval_func_floor);
        map.insert("sqrt", eval_func_sqrt);
        map.insert("pow", eval_func_pow);
        map.insert("log", eval_func_log);
        map.insert("exp", eval_func_exp);
        map.insert("clamp", eval_func_clamp);
        map.insert("sign", eval_func_sign);
        map.insert("trunc", eval_func_trunc);
        map.insert("is_finite", eval_func_is_finite);
        map.insert("ltrim", eval_func_ltrim);
        map.insert("rtrim", eval_func_rtrim);
        map.insert("fmt", eval_func_fmt);
        map.insert("concat", eval_func_concat);
        map.insert("join", eval_func_join);
        map.insert("join_by", eval_func_join_by);
        map.insert("indexof", eval_func_indexof);
        map.insert("replace_plain", eval_func_replace_plain);
        map.insert("startswith_any", eval_func_startswith_any);
        map.insert("endswith_any", eval_func_endswith_any);
        map.insert("coalesce", eval_func_coalesce);
        map.insert("merge", eval_func_merge);
        map.insert("isnull", eval_func_isnull);
        map.insert("isnotnull", eval_func_isnotnull);
        map.insert("is_blank", eval_func_is_blank);
        map.insert("null_if_blank", eval_func_null_if_blank);
        map.insert("default_if_blank", eval_func_default_if_blank);
        map.insert("md5", eval_func_md5);
        map.insert("sha1", eval_func_sha1);
        map.insert("sha1_n", eval_func_sha1_n);
        map.insert("sha256", eval_func_sha256);
        map.insert("hex", eval_func_hex);
        map.insert("stable_id", eval_func_stable_id);
        map.insert("mvsort", eval_func_mvsort);
        map.insert("mvreverse", eval_func_mvreverse);
        map.insert("now", eval_func_now);
        map.insert("now_ms", eval_func_now);
        map.insert("now_s", eval_func_now_s);
        map.insert("now_us", eval_func_now_us);
        map.insert("now_ns", eval_func_now_ns);
        map.insert("strftime", eval_func_strftime);
        map.insert("strptime", eval_func_strptime);
        map.insert("regex_match", eval_func_regex_match);
        map.insert("cidr_match", eval_func_cidr_match);
        map.insert("time_diff", eval_func_time_diff);
        map.insert("time_bucket", eval_func_time_bucket);
        map.insert("bucket_end", eval_func_bucket_end);
        map.insert("collect_set", eval_func_collect_set);
        map.insert("collect_list", eval_func_collect_set);
        map.insert("first", eval_func_collect_set);
        map.insert("last", eval_func_collect_set);
        map.insert("stddev", eval_func_stddev);
        map.insert("percentile", eval_func_stddev);
        map.insert("external", eval_func_external);
        map
    });
    map.get(name).copied()
}

fn scalar_value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Array(_) | Value::Object(_) => None,
        _ => Some(value_to_string(value)),
    }
}

fn eval_join_arg(
    arg: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<String> {
    match eval_expr_ext(arg, event, windows, baselines) {
        Some(value) => scalar_value_to_string(&value),
        None if matches!(arg, Expr::Field(_)) => Some(String::new()),
        None => None,
    }
}

fn eval_merge_arg(
    arg: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    eval_expr_ext(arg, event, windows, baselines)
}

#[cfg(test)]
mod tests {
    use super::inclusive_slice_bounds;

    #[test]
    fn mvindex_slice_bounds_clamps_and_rejects() {
        // 合法闭区间
        assert_eq!(inclusive_slice_bounds(0, 1, 5), Some((0, 1)));
        assert_eq!(inclusive_slice_bounds(3, 9, 5), Some((3, 4))); // end 越界 → 收拢到 len-1
        assert_eq!(inclusive_slice_bounds(2, 2, 5), Some((2, 2)));
        // 负下标超范围 → 归一化后仍为负：clamp 起点 / 拒绝
        assert_eq!(inclusive_slice_bounds(-3, 2, 5), Some((0, 2)));
        assert_eq!(inclusive_slice_bounds(0, -1, 5), None); // end 为负 → 空
        // 起点越过末尾或起点>终点 → 空
        assert_eq!(inclusive_slice_bounds(7, 4, 5), None);
        assert_eq!(inclusive_slice_bounds(2, 1, 5), None);
        // 空数组保护（正常路径早退，函数自身也安全）
        assert_eq!(inclusive_slice_bounds(0, 0, 0), None);
    }
}
