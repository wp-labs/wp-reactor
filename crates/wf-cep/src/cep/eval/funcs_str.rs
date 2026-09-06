//! count_char 族内置函数（eval/funcs.rs 拆分; 见 eval_func_call）

use wf_lang::ast::Expr;
use super::super::types::Value;
use super::super::types::EngineHashMap;
use super::super::types::FieldSource;
use super::super::types::RollingStats;
use super::super::types::WindowLookup;
use super::super::key::value_to_string;
use super::eval_expr_ext;
use super::values_equal;
use super::cmp::normalize_index;

pub(super) fn eval_func_count_char(
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
pub(super) fn eval_func_contains(
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
pub(super) fn eval_func_startswith(
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
pub(super) fn eval_func_endswith(
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
pub(super) fn eval_func_substr(
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
pub(super) fn eval_func_replace(
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
pub(super) fn eval_func_trim(
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
pub(super) fn eval_func_lower(
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
pub(super) fn eval_func_upper(
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
pub(super) fn eval_func_len(
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
pub(super) fn eval_func_mvcount(
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
pub(super) fn eval_func_mvjoin(
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
pub(super) fn eval_func_mvindex(
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
fn inclusive_slice_bounds(start: i64, end: i64, len: i64) -> Option<(usize, usize)> {
    let start = start.max(0);
    if end < 0 || start >= len {
        return None;
    }
    let end = end.min(len - 1);
    (start <= end).then_some((start as usize, end as usize))
}
pub(super) fn eval_func_mvappend(
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
pub(super) fn eval_func_split(
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
pub(super) fn eval_func_mvdedup(
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
pub(super) fn eval_func_ltrim(
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
pub(super) fn eval_func_rtrim(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvindex_slice_bounds_clamps_and_rejects() {
        assert_eq!(inclusive_slice_bounds(0, 1, 5), Some((0, 1)));
        assert_eq!(inclusive_slice_bounds(3, 9, 5), Some((3, 4))); // end 越界 → 收拢到 len-1
        assert_eq!(inclusive_slice_bounds(2, 2, 5), Some((2, 2)));
        assert_eq!(inclusive_slice_bounds(-3, 2, 5), Some((0, 2)));
        assert_eq!(inclusive_slice_bounds(0, -1, 5), None); // end 为负 → 空
        assert_eq!(inclusive_slice_bounds(7, 4, 5), None);
        assert_eq!(inclusive_slice_bounds(2, 1, 5), None);
        assert_eq!(inclusive_slice_bounds(0, 0, 0), None);
    }
}
