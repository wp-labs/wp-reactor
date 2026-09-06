//! now 族内置函数（eval/funcs.rs 拆分; 见 eval_func_call）

use super::super::types::EngineHashMap;
use super::super::types::FieldSource;
use super::super::types::RollingStats;
use super::super::types::Value;
use super::super::types::WindowLookup;
use super::cmp::compare_sortable_values;
use super::cmp::current_time_nanos;
use super::cmp::eval_single_string_arg;
use super::cmp::parse_time_to_timestamp_nanos;
use super::cmp::time_nanos_to_value;
use super::cmp::timestamp_nanos_to_utc;
use super::cmp::update_stable_id_hash;
use super::eval_expr_ext;
use crate::time::normalize_epoch_timestamp_float_nanos;
use crate::time::positive_interval_seconds_to_nanos;
use md5::Digest as Md5Digest;
use md5::Md5;
use sha1::Digest as Sha1Digest;
use sha1::Sha1;
use sha2::Sha256;
use wf_lang::ast::Expr;

pub(super) fn eval_func_md5(
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
pub(super) fn eval_func_sha1(
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
pub(super) fn eval_func_sha1_n(
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
pub(super) fn eval_func_sha256(
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
pub(super) fn eval_func_hex(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let text = eval_single_string_arg(args, event, windows, baselines)?;
    Some(Value::Str(hex::encode(text.as_bytes()).into()))
}
pub(super) fn eval_func_stable_id(
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
pub(super) fn eval_func_mvsort(
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
pub(super) fn eval_func_mvreverse(
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
pub(super) fn eval_func_now(
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
pub(super) fn eval_func_now_s(
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
pub(super) fn eval_func_now_us(
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
pub(super) fn eval_func_now_ns(
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
pub(super) fn eval_func_strftime(
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
pub(super) fn eval_func_strptime(
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
pub(super) fn eval_func_regex_match(
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
pub(super) fn eval_func_cidr_match(
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
pub(super) fn eval_func_time_diff(
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
pub(super) fn eval_func_time_bucket(
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
pub(super) fn eval_func_bucket_end(
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
pub(super) fn eval_func_collect_set(
    _args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // These functions need access to the instance's collected events
    // They are supported in yield/derive context via StepEvalContext
    None
}
pub(super) fn eval_func_stddev(
    _args: &[Expr],
    _event: &dyn FieldSource,
    _windows: Option<&dyn WindowLookup>,
    _baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    // These functions need access to the instance's numeric values
    // They are supported in yield/derive context via StepEvalContext
    None
}
pub(super) fn eval_func_external(
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
