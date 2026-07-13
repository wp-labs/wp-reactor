use std::collections::HashMap;

use md5::Digest as Md5Digest;
use md5::Md5;
use sha1::Digest as Sha1Digest;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use wf_lang::ast::Expr;

use crate::time::{normalize_epoch_timestamp_float_nanos, positive_interval_seconds_to_nanos};

use super::super::key::value_to_string;
use super::super::types::{Event, RollingStats, Value, WindowLookup};
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
/// - `indexof(text, needle)` → Number
/// - `replace_plain(text, from, to)` → Str
/// - `startswith_any(text, prefix1, prefix2, ...)` → Bool
/// - `endswith_any(text, suffix1, suffix2, ...)` → Bool
/// - `coalesce(v1, v2, ...)` → first non-null value
/// - `isnull(expr)` → Bool
/// - `isnotnull(expr)` → Bool
/// - `is_blank(expr)` → Bool
/// - `null_if_blank(expr)` → Str or null
/// - `default_if_blank(expr, default)` → Str
/// - `md5(text)` / `sha1(text)` / `sha256(text)` → lowercase hex string
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
/// - `time_diff(t1, t2)` → Number (seconds)
/// - `time_bucket(t, interval_seconds)` → Number (timestamp millis)
/// - `external("service", arg1, ...)` → dispatched
pub(super) fn eval_func_call(
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
        "fmt" => {
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
            Some(Value::Str(apply_fmt_template(&template, &values)?))
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
        "is_blank" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines) {
                Some(Value::Str(s)) => Some(Value::Bool(is_blank_str(&s))),
                None => Some(Value::Bool(true)),
                Some(_) => None,
            }
        }
        "null_if_blank" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) if is_blank_str(&s) => None,
                Value::Str(s) => Some(Value::Str(s)),
                _ => None,
            }
        }
        "default_if_blank" => {
            if args.len() != 2 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines) {
                Some(Value::Str(s)) if !is_blank_str(&s) => Some(Value::Str(s)),
                Some(Value::Str(_)) | None => {
                    match eval_expr_ext(&args[1], event, windows, baselines)? {
                        Value::Str(s) => Some(Value::Str(s)),
                        _ => None,
                    }
                }
                Some(_) => None,
            }
        }
        "md5" => {
            let text = eval_single_string_arg(args, event, windows, baselines)?;
            Some(Value::Str(hex::encode(<Md5 as Md5Digest>::digest(
                text.as_bytes(),
            ))))
        }
        "sha1" => {
            let text = eval_single_string_arg(args, event, windows, baselines)?;
            Some(Value::Str(hex::encode(<Sha1 as Sha1Digest>::digest(
                text.as_bytes(),
            ))))
        }
        "sha256" => {
            let text = eval_single_string_arg(args, event, windows, baselines)?;
            Some(Value::Str(hex::encode(Sha256::digest(text.as_bytes()))))
        }
        "hex" => {
            let text = eval_single_string_arg(args, event, windows, baselines)?;
            Some(Value::Str(hex::encode(text.as_bytes())))
        }
        "stable_id" => {
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
            Some(Value::Str(format!("{}{}", prefix, &digest[..16])))
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
        "now" | "now_ms" => {
            if !args.is_empty() {
                return None;
            }
            Some(time_nanos_to_value(current_time_nanos()?))
        }
        "now_s" => {
            if !args.is_empty() {
                return None;
            }
            Some(Value::Number(
                (current_time_nanos()? / 1_000_000_000) as f64,
            ))
        }
        "now_us" => {
            if !args.is_empty() {
                return None;
            }
            Some(Value::Number((current_time_nanos()? / 1_000) as f64))
        }
        "now_ns" => {
            if !args.is_empty() {
                return None;
            }
            Some(Value::Number(current_time_nanos()? as f64))
        }
        "strftime" => {
            if args.len() != 2 {
                return None;
            }
            let ts_nanos = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
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
            Some(time_nanos_to_value(ts_nanos))
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
                Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
                _ => return None,
            };
            let t2 = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
                _ => return None,
            };
            Some(Value::Number((t1 - t2).abs() as f64 / 1_000_000_000.0))
        }
        "time_bucket" => {
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
        "external" => {
            // external("service", arg1, ...) — dispatch to the global
            // ExternalCallHandler (wp_knowledge facade). Uses the shared
            // `eval_external` helper so the arg-parsing logic is identical
            // to the executor/eval.rs path.
            crate::external::eval_external(&args[0], &args[1..], |a| {
                eval_expr_ext(a, event, windows, baselines)
            })
        }
        _ => None, // unsupported function
    }
}
