//! eval_builtin_func_with_l3 的 per-name handler（issue: builtins.rs 拆分，
//! 2026-09-06 code.split_reduce_complexity 机械搬运；行为与源 arm 一致）。
use super::*;
pub(super) fn builtin_contains(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let haystack = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let needle = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    Some(Value::Bool(haystack.contains(needle.as_str())))
}
pub(super) fn builtin_startswith(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let prefix = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    Some(Value::Bool(text.starts_with(prefix.as_str())))
}
pub(super) fn builtin_endswith(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let suffix = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    Some(Value::Bool(text.ends_with(suffix.as_str())))
}
pub(super) fn builtin_merge(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut merged = EngineHashMap::default();
    for arg in args {
        match eval_merge_arg(arg, ctx, score) {
            Some(Value::Object(fields)) => merged.extend(fields),
            None if matches!(arg, wf_lang::ast::Expr::Field(_)) => {}
            None => return None,
            Some(_) => return None,
        }
    }
    Some(Value::Object(merged))
}
pub(super) fn builtin_substr(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 && args.len() != 3 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let start = match eval_expr_with_l3(&args[1], ctx, score)? {
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
        let length = match eval_expr_with_l3(&args[2], ctx, score)? {
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
pub(super) fn builtin_replace(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 3 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let pattern = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let replacement = match eval_expr_with_l3(&args[2], ctx, score)? {
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
pub(super) fn builtin_trim(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(Value::Str(s.trim().to_string().into())),
        _ => None,
    }
}
pub(super) fn builtin_lower(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(Value::Str(s.to_lowercase().into())),
        _ => None,
    }
}
pub(super) fn builtin_upper(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(Value::Str(s.to_uppercase().into())),
        _ => None,
    }
}
pub(super) fn builtin_len(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(Value::Number(s.len() as f64)),
        _ => None,
    }
}
pub(super) fn builtin_mvcount(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Array(arr) => Some(Value::Number(arr.len() as f64)),
        _ => None,
    }
}
pub(super) fn builtin_mvjoin(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let arr = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Array(arr) => arr,
        _ => return None,
    };
    let sep = match eval_expr_with_l3(&args[1], ctx, score)? {
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
pub(super) fn builtin_mvindex(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 && args.len() != 3 {
        return None;
    }
    let arr = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Array(arr) => arr,
        _ => return None,
    };
    if args.len() == 2 {
        let idx = match eval_expr_with_l3(&args[1], ctx, score)? {
            Value::Number(n) => utils::normalize_index(n.trunc() as i64, arr.len()),
            _ => return None,
        }?;
        return arr.get(idx).cloned();
    }
    if arr.is_empty() {
        return Some(Value::Array(Vec::new()));
    }
    let start = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Number(n) => n.trunc() as i64,
        _ => return None,
    };
    let end = match eval_expr_with_l3(&args[2], ctx, score)? {
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
pub(super) fn builtin_mvappend(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut out: Vec<Value> = Vec::new();
    for arg in args {
        match eval_expr_with_l3(arg, ctx, score)? {
            Value::Array(values) => out.extend(values),
            value => out.push(value),
        }
    }
    Some(Value::Array(out))
}
pub(super) fn builtin_split(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let sep = match eval_expr_with_l3(&args[1], ctx, score)? {
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
pub(super) fn builtin_mvdedup(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let arr = match eval_expr_with_l3(&args[0], ctx, score)? {
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
pub(super) fn builtin_abs(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => Some(Value::Number(n.abs())),
        _ => None,
    }
}
pub(super) fn builtin_round(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 && args.len() != 2 {
        return None;
    }
    let value = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let precision = if args.len() == 2 {
        match eval_expr_with_l3(&args[1], ctx, score)? {
            Value::Number(n) => utils::f64_to_i64_trunc(n)?,
            _ => return None,
        }
    } else {
        0
    };
    let rounded = utils::round_with_precision(value, precision)?;
    Some(Value::Number(rounded))
}
pub(super) fn builtin_ceil(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => Some(Value::Number(n.ceil())),
        _ => None,
    }
}
pub(super) fn builtin_floor(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => Some(Value::Number(n.floor())),
        _ => None,
    }
}
pub(super) fn builtin_sqrt(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) if n >= 0.0 => Some(Value::Number(n.sqrt())),
        _ => None,
    }
}
pub(super) fn builtin_pow(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let x = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let y = match eval_expr_with_l3(&args[1], ctx, score)? {
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
pub(super) fn builtin_log(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 && args.len() != 2 {
        return None;
    }
    let x = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    if x <= 0.0 {
        return None;
    }
    let out = if args.len() == 2 {
        let base = match eval_expr_with_l3(&args[1], ctx, score)? {
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
pub(super) fn builtin_exp(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let x = match eval_expr_with_l3(&args[0], ctx, score)? {
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
pub(super) fn builtin_clamp(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 3 {
        return None;
    }
    let x = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let min = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let max = match eval_expr_with_l3(&args[2], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    if min > max {
        return None;
    }
    Some(Value::Number(x.clamp(min, max)))
}
pub(super) fn builtin_sign(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) if n.is_finite() => Some(Value::Number(n.signum())),
        _ => None,
    }
}
pub(super) fn builtin_trunc(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => Some(Value::Number(n.trunc())),
        _ => None,
    }
}
pub(super) fn builtin_is_finite(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => Some(Value::Bool(n.is_finite())),
        _ => None,
    }
}
pub(super) fn builtin_ltrim(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(Value::Str(s.trim_start().to_string().into())),
        _ => None,
    }
}
pub(super) fn builtin_rtrim(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(Value::Str(s.trim_end().to_string().into())),
        _ => None,
    }
}
pub(super) fn builtin_fmt(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let template = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let values = args[1..]
        .iter()
        .map(|arg| eval_expr_with_l3(arg, ctx, score))
        .collect::<Option<Vec<_>>>()?;
    Some(Value::Str(
        utils::apply_fmt_template(template.as_str(), &values)?.into(),
    ))
}
pub(super) fn builtin_concat(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut out = String::new();
    for arg in args {
        let value = eval_expr_with_l3(arg, ctx, score)?;
        out.push_str(&value_to_string(&value));
    }
    Some(Value::Str(out.into()))
}
pub(super) fn builtin_join(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let mut out = String::new();
    for arg in args {
        out.push_str(&eval_join_arg_with_l3(arg, ctx, score)?);
    }
    Some(Value::Str(out.into()))
}
pub(super) fn builtin_join_by(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let sep = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let mut parts = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        parts.push(eval_join_arg_with_l3(arg, ctx, score)?);
    }
    Some(Value::Str(parts.join(&sep).into()))
}
pub(super) fn builtin_indexof(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let needle = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let idx = text.find(needle.as_str()).map(|x| x as f64).unwrap_or(-1.0);
    Some(Value::Number(idx))
}
pub(super) fn builtin_replace_plain(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 3 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let from = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let to = match eval_expr_with_l3(&args[2], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    Some(Value::Str(text.replace(from.as_str(), to.as_str()).into()))
}
pub(super) fn builtin_startswith_any(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    for arg in &args[1..] {
        let prefix = match eval_expr_with_l3(arg, ctx, score)? {
            Value::Str(s) => s,
            _ => return None,
        };
        if text.starts_with(prefix.as_str()) {
            return Some(Value::Bool(true));
        }
    }
    Some(Value::Bool(false))
}
pub(super) fn builtin_endswith_any(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    for arg in &args[1..] {
        let suffix = match eval_expr_with_l3(arg, ctx, score)? {
            Value::Str(s) => s,
            _ => return None,
        };
        if text.ends_with(suffix.as_str()) {
            return Some(Value::Bool(true));
        }
    }
    Some(Value::Bool(false))
}
pub(super) fn builtin_coalesce(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    for arg in args {
        if let Some(v) = eval_expr_with_l3(arg, ctx, score) {
            if matches!(&v, Value::Str(s) if utils::is_blank_str(s)) {
                continue;
            }
            return Some(v);
        }
    }
    None
}
pub(super) fn builtin_isnull(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    Some(Value::Bool(
        eval_expr_with_l3(&args[0], ctx, score).is_none(),
    ))
}
pub(super) fn builtin_isnotnull(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    Some(Value::Bool(
        eval_expr_with_l3(&args[0], ctx, score).is_some(),
    ))
}
pub(super) fn builtin_is_blank(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score) {
        Some(Value::Str(s)) => Some(Value::Bool(utils::is_blank_str(&s))),
        None => Some(Value::Bool(true)),
        Some(_) => None,
    }
}
pub(super) fn builtin_null_if_blank(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) if utils::is_blank_str(&s) => None,
        Value::Str(s) => Some(Value::Str(s)),
        _ => None,
    }
}
pub(super) fn builtin_default_if_blank(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score) {
        Some(Value::Str(s)) if !utils::is_blank_str(&s) => Some(Value::Str(s)),
        Some(Value::Str(_)) | None => match eval_expr_with_l3(&args[1], ctx, score)? {
            Value::Str(s) => Some(Value::Str(s)),
            _ => None,
        },
        Some(_) => None,
    }
}
pub(super) fn builtin_md5(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
    Some(Value::Str(
        hex::encode(<Md5 as Md5Digest>::digest(text.as_bytes())).into(),
    ))
}
pub(super) fn builtin_sha1(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
    Some(Value::Str(
        hex::encode(<Sha1 as Sha1Digest>::digest(text.as_bytes())).into(),
    ))
}
pub(super) fn builtin_sha1_n(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let len = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 => n as usize,
        _ => return None,
    };
    if !(1..=40).contains(&len) {
        return None;
    }
    let digest = hex::encode(<Sha1 as Sha1Digest>::digest(text.as_bytes()));
    Some(Value::Str(digest[..len].to_string().into()))
}
pub(super) fn builtin_sha256(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
    Some(Value::Str(
        hex::encode(Sha256::digest(text.as_bytes())).into(),
    ))
}
pub(super) fn builtin_hex(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
    Some(Value::Str(hex::encode(text.as_bytes()).into()))
}
pub(super) fn builtin_stable_id(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    let prefix = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let mut hasher = Sha256::new();
    for arg in &args[1..] {
        let value = eval_expr_with_l3(arg, ctx, score)?;
        utils::update_stable_id_hash(&mut hasher, &value)?;
    }
    let digest = hex::encode(hasher.finalize());
    Some(Value::Str(format!("{}{}", prefix, &digest[..16]).into()))
}
pub(super) fn builtin_mvsort(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let mut arr = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Array(arr) => arr,
        _ => return None,
    };
    arr.sort_by(utils::compare_sortable_values);
    Some(Value::Array(arr))
}
pub(super) fn builtin_mvreverse(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let mut arr = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Array(arr) => arr,
        _ => return None,
    };
    arr.reverse();
    Some(Value::Array(arr))
}
pub(super) fn builtin_now(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    _ctx: &dyn FieldSource,
    _score: YieldMeta<'_>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(utils::time_nanos_to_value(utils::current_time_nanos()?))
}
pub(super) fn builtin_now_s(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    _ctx: &dyn FieldSource,
    _score: YieldMeta<'_>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(Value::Number(
        (utils::current_time_nanos()? / 1_000_000_000) as f64,
    ))
}
pub(super) fn builtin_now_us(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    _ctx: &dyn FieldSource,
    _score: YieldMeta<'_>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(Value::Number((utils::current_time_nanos()? / 1_000) as f64))
}
pub(super) fn builtin_now_ns(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    _ctx: &dyn FieldSource,
    _score: YieldMeta<'_>,
) -> Option<Value> {
    if !args.is_empty() {
        return None;
    }
    Some(Value::Number(utils::current_time_nanos()? as f64))
}
pub(super) fn builtin_time_to_s(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    // time 值（系统变量毫秒 / 输入字段纳秒 / 聚合保持原单位）统一按
    // 数量级归一化到纳秒后转目标单位——两种来源都正确（issue #69）。
    if args.len() != 1 {
        return None;
    }
    let ts_nanos = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let divisor = if name == "time_to_s" {
        1_000_000_000
    } else {
        1_000_000
    };
    Some(Value::Number((ts_nanos / divisor) as f64))
}
pub(super) fn builtin_strftime(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 1 && args.len() != 2 {
        return None;
    }
    let ts_nanos = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let fmt = if let Some(fmt_expr) = args.get(1) {
        match eval_expr_with_l3(fmt_expr, ctx, score)? {
            Value::Str(s) => s.to_string(),
            _ => return None,
        }
    } else {
        score
            .time_format
            .unwrap_or(wf_config::DEFAULT_OUTPUT_TIME_FORMAT)
            .to_string()
    };
    let dt = utils::timestamp_nanos_to_utc(ts_nanos)?;
    Some(Value::Str(dt.format(&fmt).to_string().into()))
}
pub(super) fn builtin_strptime(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let text = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let fmt = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let ts_nanos = utils::parse_time_to_timestamp_nanos(&text, &fmt)?;
    Some(utils::time_nanos_to_value(ts_nanos))
}
pub(super) fn builtin_regex_match(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let hay = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let pat = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let re = crate::match_engine::regex_cache::cached_regex(&pat)?;
    Some(Value::Bool(re.is_match(&hay)))
}
pub(super) fn builtin_cidr_match(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let ip = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let cidr = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let net = crate::match_engine::cidr_cache::cached_cidr(&cidr)?;
    Some(Value::Bool(net.contains(&ip)))
}
pub(super) fn builtin_time_diff(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let t1 = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let t2 = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    Some(Value::Number((t1 - t2).abs() as f64 / 1_000_000_000.0))
}
pub(super) fn builtin_time_bucket(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let t = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let interval = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let interval_nanos = positive_interval_seconds_to_nanos(interval)?;
    let bucketed = t.div_euclid(interval_nanos) * interval_nanos;
    Some(utils::time_nanos_to_value(bucketed))
}
pub(super) fn builtin_bucket_end(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    // 桶末：`bucket_end(t, interval) = time_bucket(t, interval) + interval`
    if args.len() != 2 {
        return None;
    }
    let t = match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
        _ => return None,
    };
    let interval = match eval_expr_with_l3(&args[1], ctx, score)? {
        Value::Number(n) => n,
        _ => return None,
    };
    let interval_nanos = positive_interval_seconds_to_nanos(interval)?;
    let bucketed = t.div_euclid(interval_nanos) * interval_nanos;
    Some(utils::time_nanos_to_value(
        bucketed.checked_add(interval_nanos)?,
    ))
}
pub(super) fn builtin_external(
    _name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
    crate::external::eval_external(&args[0], &args[1..], |a| eval_expr_with_l3(a, ctx, score))
}
