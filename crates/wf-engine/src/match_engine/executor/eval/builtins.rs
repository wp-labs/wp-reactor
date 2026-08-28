use md5::Digest as Md5Digest;
use md5::Md5;
use sha1::Digest as Sha1Digest;
use sha1::Sha1;
use sha2::{Digest, Sha256};

use super::{Event, Value, YieldMeta, eval_expr_with_l3, step_data, utils};
use crate::match_engine::match_engine::{EngineHashMap, value_to_string, values_equal};
use crate::time::{normalize_epoch_timestamp_float_nanos, positive_interval_seconds_to_nanos};

pub(super) fn contains_system_var(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::SystemVar(_) | Expr::WfuMeta(_) => true,
        Expr::BinOp { left, right, .. } => contains_system_var(left) || contains_system_var(right),
        Expr::Neg(inner) => contains_system_var(inner),
        Expr::Not(inner) => contains_system_var(inner),
        Expr::FuncCall { args, .. } => args.iter().any(contains_system_var),
        Expr::Object(items) => items.iter().any(|item| contains_system_var(&item.value)),
        Expr::Array(items) => items.iter().any(contains_system_var),
        Expr::InList { expr, list, .. } => {
            contains_system_var(expr) || list.iter().any(contains_system_var)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            contains_system_var(cond)
                || contains_system_var(then_expr)
                || contains_system_var(else_expr)
        }
        _ => false,
    }
}

pub(super) fn materialize_system_vars(
    expr: &wf_lang::ast::Expr,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::{Expr, SystemVar};

    match expr {
        Expr::Number(n) => Some(Expr::Number(*n)),
        Expr::StringLit(s) => Some(Expr::StringLit(s.clone())),
        Expr::Bool(b) => Some(Expr::Bool(*b)),
        Expr::SystemVar(SystemVar::Score) => Some(Expr::Number(score.score?)),
        Expr::SystemVar(SystemVar::EventFirstTime | SystemVar::EvidenceStartTime) => {
            Some(utils::time_nanos_to_expr(score.event_first_time_nanos?))
        }
        Expr::SystemVar(SystemVar::EventLastTime | SystemVar::EvidenceEndTime) => {
            Some(utils::time_nanos_to_expr(score.event_last_time_nanos?))
        }
        Expr::SystemVar(SystemVar::WindowStartTime) => {
            Some(utils::time_nanos_to_expr(score.window_start_time_nanos?))
        }
        Expr::SystemVar(SystemVar::WindowEndTime) => {
            Some(utils::time_nanos_to_expr(score.window_end_time_nanos?))
        }
        Expr::SystemVar(SystemVar::EmitTime) => {
            Some(utils::time_nanos_to_expr(score.emit_time_nanos?))
        }
        Expr::WfuMeta(field) => match score.resolve_wfu_meta(*field)? {
            Value::Number(n) => Some(Expr::Number(n)),
            Value::Str(s) => Some(Expr::StringLit(s.to_string())),
            Value::Bool(b) => Some(Expr::Bool(b)),
            _ => None,
        },
        Expr::Field(fr) => Some(Expr::Field(fr.clone())),
        Expr::BinOp { op, left, right } => Some(Expr::BinOp {
            op: *op,
            left: Box::new(materialize_system_vars(left, score)?),
            right: Box::new(materialize_system_vars(right, score)?),
        }),
        Expr::Neg(inner) => Some(Expr::Neg(Box::new(materialize_system_vars(inner, score)?))),
        Expr::Not(inner) => Some(Expr::Not(Box::new(materialize_system_vars(inner, score)?))),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => Some(Expr::FuncCall {
            qualifier: qualifier.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| materialize_system_vars(arg, score))
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::Object(items) => Some(Expr::Object(
            items
                .iter()
                .map(|item| {
                    Some(wf_lang::ast::ObjectItem {
                        targets: item.targets.clone(),
                        type_hint: item.type_hint.clone(),
                        value: materialize_system_vars(&item.value, score)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::Array(items) => Some(Expr::Array(
            items
                .iter()
                .map(|item| materialize_system_vars(item, score))
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::InList {
            expr,
            list,
            negated,
        } => Some(Expr::InList {
            expr: Box::new(materialize_system_vars(expr, score)?),
            list: list
                .iter()
                .map(|item| materialize_system_vars(item, score))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
        }),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Some(Expr::IfThenElse {
            cond: Box::new(materialize_system_vars(cond, score)?),
            then_expr: Box::new(materialize_system_vars(then_expr, score)?),
            else_expr: Box::new(materialize_system_vars(else_expr, score)?),
        }),
        _ => None,
    }
}

pub(super) fn eval_builtin_func_with_l3(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
    score: YieldMeta,
) -> Option<Value> {
    match name {
        "contains" => {
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
        "startswith" => {
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
        "endswith" => {
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
        "merge" => {
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
        "substr" => {
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
        "replace" => {
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
        "trim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.trim().to_string().into())),
                _ => None,
            }
        }
        "lower" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.to_lowercase().into())),
                _ => None,
            }
        }
        "upper" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.to_uppercase().into())),
                _ => None,
            }
        }
        "len" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Number(s.len() as f64)),
                _ => None,
            }
        }
        "mvcount" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Array(arr) => Some(Value::Number(arr.len() as f64)),
                _ => None,
            }
        }
        "mvjoin" => {
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
        "mvindex" => {
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
                match eval_expr_with_l3(arg, ctx, score)? {
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
        "mvdedup" => {
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
        "abs" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) => Some(Value::Number(n.abs())),
                _ => None,
            }
        }
        "round" => {
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
        "ceil" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) => Some(Value::Number(n.ceil())),
                _ => None,
            }
        }
        "floor" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) => Some(Value::Number(n.floor())),
                _ => None,
            }
        }
        "sqrt" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) if n >= 0.0 => Some(Value::Number(n.sqrt())),
                _ => None,
            }
        }
        "pow" => {
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
        "log" => {
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
        "exp" => {
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
        "clamp" => {
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
        "sign" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) if n.is_finite() => Some(Value::Number(n.signum())),
                _ => None,
            }
        }
        "trunc" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) => Some(Value::Number(n.trunc())),
                _ => None,
            }
        }
        "is_finite" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) => Some(Value::Bool(n.is_finite())),
                _ => None,
            }
        }
        "ltrim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.trim_start().to_string().into())),
                _ => None,
            }
        }
        "rtrim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.trim_end().to_string().into())),
                _ => None,
            }
        }
        "fmt" => {
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
        "concat" => {
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
        "join" => {
            if args.is_empty() {
                return None;
            }
            let mut out = String::new();
            for arg in args {
                out.push_str(&eval_join_arg_with_l3(arg, ctx, score)?);
            }
            Some(Value::Str(out.into()))
        }
        "join_by" => {
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
        "indexof" => {
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
        "replace_plain" => {
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
        "startswith_any" => {
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
        "endswith_any" => {
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
        "coalesce" => {
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
        "isnull" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Bool(
                eval_expr_with_l3(&args[0], ctx, score).is_none(),
            ))
        }
        "isnotnull" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Bool(
                eval_expr_with_l3(&args[0], ctx, score).is_some(),
            ))
        }
        "is_blank" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score) {
                Some(Value::Str(s)) => Some(Value::Bool(utils::is_blank_str(&s))),
                None => Some(Value::Bool(true)),
                Some(_) => None,
            }
        }
        "null_if_blank" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) if utils::is_blank_str(&s) => None,
                Value::Str(s) => Some(Value::Str(s)),
                _ => None,
            }
        }
        "default_if_blank" => {
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
        "md5" => {
            let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(
                hex::encode(<Md5 as Md5Digest>::digest(text.as_bytes())).into(),
            ))
        }
        "sha1" => {
            let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(
                hex::encode(<Sha1 as Sha1Digest>::digest(text.as_bytes())).into(),
            ))
        }
        "sha1_n" => {
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
        "sha256" => {
            let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(
                hex::encode(Sha256::digest(text.as_bytes())).into(),
            ))
        }
        "hex" => {
            let text = utils::eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(hex::encode(text.as_bytes()).into()))
        }
        "stable_id" => {
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
        "mvsort" => {
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
        "mvreverse" => {
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
        "now" | "now_ms" => {
            if !args.is_empty() {
                return None;
            }
            Some(utils::time_nanos_to_value(utils::current_time_nanos()?))
        }
        "now_s" => {
            if !args.is_empty() {
                return None;
            }
            Some(Value::Number(
                (utils::current_time_nanos()? / 1_000_000_000) as f64,
            ))
        }
        "now_us" => {
            if !args.is_empty() {
                return None;
            }
            Some(Value::Number((utils::current_time_nanos()? / 1_000) as f64))
        }
        "now_ns" => {
            if !args.is_empty() {
                return None;
            }
            Some(Value::Number(utils::current_time_nanos()? as f64))
        }
        "time_to_s" | "time_to_ms" => {
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
        "strftime" => {
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
        "strptime" => {
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
        "regex_match" => {
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
        "cidr_match" => {
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
        "time_diff" => {
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
        "time_bucket" => {
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
        "bucket_end" => {
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
        "external" => crate::external::eval_external(&args[0], &args[1..], |a| {
            eval_expr_with_l3(a, ctx, score)
        }),
        _ => None,
    }
}

fn eval_merge_arg(arg: &wf_lang::ast::Expr, ctx: &Event, score: YieldMeta<'_>) -> Option<Value> {
    eval_expr_with_l3(arg, ctx, score)
}

enum StatSelector<'a> {
    WindowEvent(&'a str),
    MatchEvent(&'a str),
    MatchDistinct(&'a str),
    Trigger(&'a str),
    Final(&'a str),
}

pub(super) fn is_stat_selector_func(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger" | "final"
    )
}

pub(super) fn eval_stat_func(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let selector = parse_stat_selector(&args[0])?;
    match (name, selector) {
        ("count", StatSelector::WindowEvent(alias)) => ctx
            .fields
            .get(format!("_bind_{alias}_count").as_str())
            .and_then(number_value),
        ("count", StatSelector::MatchEvent(label) | StatSelector::MatchDistinct(label)) => {
            ctx.fields.get(label).and_then(number_value)
        }
        ("value", StatSelector::Trigger(label) | StatSelector::Final(label)) => {
            ctx.fields.get(label).and_then(number_value)
        }
        _ => None,
    }
}

fn parse_stat_selector(expr: &wf_lang::ast::Expr) -> Option<StatSelector<'_>> {
    let wf_lang::ast::Expr::FuncCall {
        qualifier: None,
        name,
        args,
    } = expr
    else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    let wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Simple(symbol)) = &args[0] else {
        return None;
    };
    match name.as_str() {
        "window_event" => Some(StatSelector::WindowEvent(symbol)),
        "match_event" => Some(StatSelector::MatchEvent(symbol)),
        "match_distinct" => Some(StatSelector::MatchDistinct(symbol)),
        "trigger" => Some(StatSelector::Trigger(symbol)),
        "final" => Some(StatSelector::Final(symbol)),
        _ => None,
    }
}

fn number_value(value: &Value) -> Option<Value> {
    match value {
        Value::Number(n) => Some(Value::Number(*n)),
        _ => None,
    }
}

pub(super) fn eval_l3_func(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
    score: YieldMeta,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let step_indices = step_data::resolve_step_indices(ctx, args.first());
    let values = if let Some((alias, _)) = args.first().and_then(step_data::extract_bind_field_ref)
        && step_data::get_bind_count(ctx, alias).is_some()
    {
        step_data::flatten_bind_series(ctx, args.first())
    } else {
        step_data::flatten_step_series(ctx, &step_indices, args.first())
    };
    match name {
        "collect_set" => {
            if args.len() != 1 {
                return None;
            }
            let mut out: Vec<Value> = Vec::new();
            for v in values {
                if !out.iter().any(|seen| values_equal(seen, &v)) {
                    out.push(v);
                }
            }
            Some(Value::Array(out))
        }
        "collect_list" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Array(values))
        }
        "first" => {
            if args.len() != 1 {
                return None;
            }
            values.first().cloned()
        }
        "last" => {
            if args.len() != 1 {
                return None;
            }
            values.last().cloned()
        }
        "stddev" => {
            if args.len() != 1 {
                return None;
            }
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => Some(*n),
                    _ => None,
                })
                .collect();
            if nums.len() < 2 {
                return Some(Value::Number(0.0));
            }
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let variance = nums.iter().map(|n| (n - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            Some(Value::Number(variance.sqrt()))
        }
        "percentile" => {
            if args.len() != 2 {
                return None;
            }
            let p = match eval_expr_with_l3(&args[1], ctx, score)? {
                Value::Number(n) => n.clamp(0.0, 100.0) / 100.0,
                _ => return None,
            };
            let mut nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => Some(*n),
                    _ => None,
                })
                .collect();
            if nums.is_empty() {
                return Some(Value::Number(0.0));
            }
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((nums.len() - 1) as f64 * p).round() as usize;
            Some(Value::Number(nums[idx.min(nums.len() - 1)]))
        }
        _ => None,
    }
}

pub(super) fn eval_aggregate_func(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match &args[0] {
        wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Qualified(_, _))
        | wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Bracketed(_, _)) => {
            let step_indices = step_data::resolve_aggregate_step_indices(ctx, args.first());
            let step_values = step_data::flatten_step_series(ctx, &step_indices, args.first());
            if !step_values.is_empty() {
                return eval_aggregate_over_values(name, &step_values);
            }
            let bind_values = step_data::flatten_bind_series(ctx, args.first());
            if !bind_values.is_empty() {
                return eval_aggregate_over_values(name, &bind_values);
            }
            None
        }
        wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Simple(_)) => {
            let step_indices = step_data::resolve_aggregate_step_indices(ctx, args.first());
            let measures: Vec<f64> = step_indices
                .iter()
                .filter_map(|idx| step_data::get_step_measure(ctx, *idx))
                .collect();
            if !measures.is_empty() {
                return eval_aggregate_over_numbers(name, &measures);
            }
            if name == "count"
                && let Some(alias) = args.first().and_then(step_data::extract_bind_ref)
                && let Some(count) = step_data::get_bind_count(ctx, alias)
            {
                return Some(Value::Number(count));
            }
            let step_values = step_data::flatten_step_series(ctx, &step_indices, args.first());
            if !step_values.is_empty() {
                return eval_aggregate_over_values(name, &step_values);
            }
            None
        }
        _ => None,
    }
}

fn scalar_value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Array(_) | Value::Object(_) => None,
        _ => Some(value_to_string(value)),
    }
}

fn eval_join_arg_with_l3(
    arg: &wf_lang::ast::Expr,
    ctx: &Event,
    score: YieldMeta,
) -> Option<String> {
    match eval_expr_with_l3(arg, ctx, score) {
        Some(value) => scalar_value_to_string(&value),
        None if matches!(arg, wf_lang::ast::Expr::Field(_)) => Some(String::new()),
        None => None,
    }
}

pub(super) fn eval_aggregate_over_numbers(name: &str, values: &[f64]) -> Option<Value> {
    match name {
        "count" => Some(Value::Number(values.iter().sum())),
        "sum" => Some(Value::Number(values.iter().sum())),
        "avg" => {
            if values.is_empty() {
                Some(Value::Number(0.0))
            } else {
                Some(Value::Number(
                    values.iter().sum::<f64>() / values.len() as f64,
                ))
            }
        }
        "min" => values
            .iter()
            .copied()
            .reduce(f64::min)
            .map(Value::Number)
            .or(Some(Value::Number(0.0))),
        "max" => values
            .iter()
            .copied()
            .reduce(f64::max)
            .map(Value::Number)
            .or(Some(Value::Number(0.0))),
        _ => None,
    }
}

pub(super) fn eval_aggregate_over_values(name: &str, values: &[Value]) -> Option<Value> {
    match name {
        "count" => Some(Value::Number(values.len() as f64)),
        "sum" => Some(Value::Number(sum_numeric_values(values))),
        "avg" => {
            let nums = numeric_values(values);
            if nums.is_empty() {
                Some(Value::Number(0.0))
            } else {
                Some(Value::Number(nums.iter().sum::<f64>() / nums.len() as f64))
            }
        }
        "min" => values
            .iter()
            .cloned()
            .min_by(utils::compare_sortable_values),
        "max" => values
            .iter()
            .cloned()
            .max_by(utils::compare_sortable_values),
        _ => None,
    }
}

pub(super) fn numeric_values(values: &[Value]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|value| match value {
            Value::Number(n) => Some(*n),
            _ => None,
        })
        .collect()
}

pub(super) fn sum_numeric_values(values: &[Value]) -> f64 {
    numeric_values(values).iter().sum()
}
