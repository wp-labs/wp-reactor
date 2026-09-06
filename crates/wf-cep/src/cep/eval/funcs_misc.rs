//! fmt 族内置函数（eval/funcs.rs 拆分; 见 eval_func_call）

use super::super::key::value_to_string;
use super::super::types::EngineHashMap;
use super::super::types::FieldSource;
use super::super::types::RollingStats;
use super::super::types::Value;
use super::super::types::WindowLookup;
use super::cmp::apply_fmt_template;
use super::cmp::is_blank_str;
use super::eval_expr_ext;
use wf_lang::ast::Expr;

pub(super) fn eval_func_fmt(
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
pub(super) fn eval_func_concat(
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
pub(super) fn eval_func_join(
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
pub(super) fn eval_func_join_by(
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
pub(super) fn eval_func_indexof(
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
pub(super) fn eval_func_replace_plain(
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
pub(super) fn eval_func_startswith_any(
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
pub(super) fn eval_func_endswith_any(
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
pub(super) fn eval_func_coalesce(
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
pub(super) fn eval_func_merge(
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
pub(super) fn eval_func_isnull(
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
pub(super) fn eval_func_isnotnull(
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
pub(super) fn eval_func_is_blank(
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
pub(super) fn eval_func_null_if_blank(
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
pub(super) fn eval_func_default_if_blank(
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
