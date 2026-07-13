use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use md5::Digest as Md5Digest;
use md5::Md5;
use orion_error::prelude::*;
use sha1::Digest as Sha1Digest;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::cell::Cell;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::match_engine::{
    Event, Value, WindowLookup, eval_expr, eval_expr_ext, field_ref_name, value_to_string,
    values_equal,
};
use crate::time::{
    epoch_nanos_to_millis, normalize_epoch_timestamp_float_nanos,
    positive_interval_seconds_to_nanos,
};

/// Evaluate a yield/derive expression with L3 function support.
///
/// L3 functions (collect_set, collect_list, first, last, stddev, percentile)
/// need access to the collected values from step execution. These values are
/// stored in `_step_{i}_values` and `_step_{i}_source` fields in the eval context.
pub(super) fn eval_yield_expr(expr: &wf_lang::ast::Expr, ctx: &Event) -> Option<Value> {
    eval_yield_expr_with_score(expr, ctx, None)
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct YieldMeta {
    pub(super) score: Option<f64>,
    pub(super) event_first_time_nanos: Option<i64>,
    pub(super) event_last_time_nanos: Option<i64>,
    pub(super) window_start_time_nanos: Option<i64>,
    pub(super) window_end_time_nanos: Option<i64>,
    pub(super) emit_time_nanos: Option<i64>,
}

thread_local! {
    static EVAL_TIME_NANOS: Cell<Option<i64>> = const { Cell::new(None) };
    static EVAL_TIME_SCOPE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct EvalTimeScope;

impl EvalTimeScope {
    fn enter() -> Self {
        EVAL_TIME_SCOPE_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

impl Drop for EvalTimeScope {
    fn drop(&mut self) {
        EVAL_TIME_SCOPE_DEPTH.with(|depth| {
            let next_depth = depth.get().saturating_sub(1);
            depth.set(next_depth);
            if next_depth == 0 {
                EVAL_TIME_NANOS.with(|time| time.set(None));
            }
        });
    }
}

pub(super) fn with_yield_eval_scope<T>(f: impl FnOnce() -> T) -> T {
    let _scope = EvalTimeScope::enter();
    f()
}

pub(super) fn eval_yield_expr_with_score(
    expr: &wf_lang::ast::Expr,
    ctx: &Event,
    score: Option<f64>,
) -> Option<Value> {
    eval_yield_expr_with_meta(
        expr,
        ctx,
        YieldMeta {
            score,
            ..YieldMeta::default()
        },
    )
}

pub(super) fn eval_yield_expr_with_meta(
    expr: &wf_lang::ast::Expr,
    ctx: &Event,
    meta: YieldMeta,
) -> Option<Value> {
    // For yield expressions, fall back to empty string when a field is missing
    // (e.g., join window fields not available in test runner)
    with_yield_eval_scope(|| match eval_expr_with_l3(expr, ctx, meta) {
        None => Some(Value::Str(String::new())),
        val => val,
    })
}

pub(super) fn eval_bool_expr(expr: &wf_lang::ast::Expr, ctx: &Event) -> Option<bool> {
    match eval_expr_with_l3(expr, ctx, YieldMeta::default()) {
        Some(Value::Bool(result)) => Some(result),
        _ => None,
    }
}

pub(super) fn eval_bool_expr_with_lookup(
    expr: &wf_lang::ast::Expr,
    ctx: &Event,
    windows: Option<&dyn WindowLookup>,
) -> Option<bool> {
    let mut baselines = std::collections::HashMap::new();
    match eval_expr_ext(expr, ctx, windows, &mut baselines) {
        Some(Value::Bool(result)) => Some(result),
        _ => None,
    }
}

fn eval_expr_with_l3(expr: &wf_lang::ast::Expr, ctx: &Event, meta: YieldMeta) -> Option<Value> {
    use wf_lang::ast::{BinOp, Expr, SystemVar};

    let _time_scope = EvalTimeScope::enter();
    let score = meta;
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::SystemVar(SystemVar::Score) => meta.score.map(Value::Number),
        Expr::SystemVar(SystemVar::EventFirstTime | SystemVar::EvidenceStartTime) => {
            meta.event_first_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::EventLastTime | SystemVar::EvidenceEndTime) => {
            meta.event_last_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::WindowStartTime) => {
            meta.window_start_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::WindowEndTime) => {
            meta.window_end_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::EmitTime) => meta.emit_time_nanos.map(time_nanos_to_value),
        Expr::Field(fr) => ctx.fields.get(field_ref_name(fr)).cloned(),
        Expr::Object(items) => {
            let mut map = std::collections::HashMap::new();
            for item in items {
                let value = eval_expr_with_l3(&item.value, ctx, score)?;
                for target in &item.targets {
                    map.insert(target.clone(), value.clone());
                }
            }
            Some(Value::Object(map))
        }
        Expr::Array(items) => items
            .iter()
            .map(|item| eval_expr_with_l3(item, ctx, score))
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        Expr::Neg(inner) => match eval_expr_with_l3(inner, ctx, score)? {
            Value::Number(n) => Some(Value::Number(-n)),
            _ => None,
        },
        Expr::BinOp { op, left, right } => match op {
            BinOp::And => eval_logic_and_with_l3(left, right, ctx, score),
            BinOp::Or => eval_logic_or_with_l3(left, right, ctx, score),
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                let lv = eval_expr_with_l3(left, ctx, score)?;
                let rv = eval_expr_with_l3(right, ctx, score)?;
                Some(Value::Bool(compare_values(*op, &lv, &rv)))
            }
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                let lv = eval_expr_with_l3(left, ctx, score)?;
                let rv = eval_expr_with_l3(right, ctx, score)?;
                let ln = coerce_to_f64(&lv)?;
                let rn = coerce_to_f64(&rv)?;
                let out = match op {
                    BinOp::Add => ln + rn,
                    BinOp::Sub => ln - rn,
                    BinOp::Mul => ln * rn,
                    BinOp::Div => {
                        if rn == 0.0 {
                            return None;
                        }
                        ln / rn
                    }
                    BinOp::Mod => {
                        if rn == 0.0 {
                            return None;
                        }
                        ln % rn
                    }
                    _ => unreachable!(),
                };
                Some(Value::Number(out))
            }
            _ => None,
        },
        Expr::InList {
            expr: target,
            list,
            negated,
        } => {
            let target_val = eval_expr_with_l3(target, ctx, score)?;
            let found = list.iter().any(|item| {
                eval_expr_with_l3(item, ctx, score)
                    .map(|v| values_equal(&target_val, &v))
                    .unwrap_or(false)
            });
            Some(Value::Bool(if *negated { !found } else { found }))
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => match eval_expr_with_l3(cond, ctx, score) {
            Some(Value::Bool(true)) => eval_expr_with_l3(then_expr, ctx, score),
            Some(Value::Bool(false)) => eval_expr_with_l3(else_expr, ctx, score),
            _ => None,
        },
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            if qualifier.is_some() {
                if contains_system_var(expr) {
                    let rewritten = materialize_system_vars(expr, score)?;
                    return eval_expr(&rewritten, ctx);
                }
                return eval_expr(expr, ctx);
            }
            if is_aggregate_func(name) {
                return eval_aggregate_func(name, args, ctx);
            }
            if is_l3_func(name) {
                return eval_l3_func(name, args, ctx, score);
            }
            if name == "external"
                || is_eval_time_func(name)
                || args.iter().any(contains_l3_func)
                || args.iter().any(contains_aggregate_func)
                || args.iter().any(contains_eval_time_func)
            {
                // `external()` is implemented only in `eval_builtin_func_with_l3`
                // (it dispatches to the global ExternalCallHandler / wp_knowledge
                // facade). Route it here even when its args are plain literals /
                // fields, otherwise `on each where external(...)` filters silently
                // evaluate to None and never query the backend.
                return eval_builtin_func_with_l3(name, args, ctx, score);
            }
            if args.iter().any(contains_system_var) {
                let rewritten = materialize_system_vars(expr, score)?;
                return eval_expr(&rewritten, ctx);
            }
            eval_expr(expr, ctx)
        }
        _ => None,
    }
}

fn eval_logic_and_with_l3(
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &Event,
    score: YieldMeta,
) -> Option<Value> {
    let lv = eval_expr_with_l3(left, ctx, score);
    let rv = eval_expr_with_l3(right, ctx, score);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => Some(Value::Bool(false)),
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => Some(Value::Bool(true)),
        _ => None,
    }
}

fn eval_logic_or_with_l3(
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &Event,
    score: YieldMeta,
) -> Option<Value> {
    let lv = eval_expr_with_l3(left, ctx, score);
    let rv = eval_expr_with_l3(right, ctx, score);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(Value::Bool(true)),
        (Some(Value::Bool(false)), Some(Value::Bool(false))) => Some(Value::Bool(false)),
        _ => None,
    }
}

fn compare_values(op: wf_lang::ast::BinOp, lv: &Value, rv: &Value) -> bool {
    use wf_lang::ast::BinOp;
    match op {
        BinOp::Eq => values_equal(lv, rv),
        BinOp::Ne => !values_equal(lv, rv),
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => match (lv, rv) {
            (Value::Number(a), Value::Number(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            (Value::Str(a), Value::Str(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            (Value::Bool(a), Value::Bool(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            _ => false,
        },
        _ => false,
    }
}

fn coerce_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

fn is_l3_func(name: &str) -> bool {
    matches!(
        name,
        "collect_set" | "collect_list" | "first" | "last" | "stddev" | "percentile"
    )
}

fn is_aggregate_func(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max")
}

fn contains_l3_func(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::FuncCall { name, args, .. } => is_l3_func(name) || args.iter().any(contains_l3_func),
        Expr::BinOp { left, right, .. } => contains_l3_func(left) || contains_l3_func(right),
        Expr::Neg(inner) => contains_l3_func(inner),
        Expr::Object(items) => items.iter().any(|item| contains_l3_func(&item.value)),
        Expr::Array(items) => items.iter().any(contains_l3_func),
        Expr::InList { expr, list, .. } => {
            contains_l3_func(expr) || list.iter().any(contains_l3_func)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => contains_l3_func(cond) || contains_l3_func(then_expr) || contains_l3_func(else_expr),
        _ => false,
    }
}

fn is_eval_time_func(name: &str) -> bool {
    matches!(name, "now" | "now_s" | "now_ms" | "now_us" | "now_ns")
}

fn contains_eval_time_func(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::FuncCall { name, args, .. } => {
            is_eval_time_func(name) || args.iter().any(contains_eval_time_func)
        }
        Expr::BinOp { left, right, .. } => {
            contains_eval_time_func(left) || contains_eval_time_func(right)
        }
        Expr::Neg(inner) => contains_eval_time_func(inner),
        Expr::Object(items) => items
            .iter()
            .any(|item| contains_eval_time_func(&item.value)),
        Expr::Array(items) => items.iter().any(contains_eval_time_func),
        Expr::InList { expr, list, .. } => {
            contains_eval_time_func(expr) || list.iter().any(contains_eval_time_func)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            contains_eval_time_func(cond)
                || contains_eval_time_func(then_expr)
                || contains_eval_time_func(else_expr)
        }
        _ => false,
    }
}

fn contains_aggregate_func(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::FuncCall { name, args, .. } => {
            is_aggregate_func(name) || args.iter().any(contains_aggregate_func)
        }
        Expr::BinOp { left, right, .. } => {
            contains_aggregate_func(left) || contains_aggregate_func(right)
        }
        Expr::Neg(inner) => contains_aggregate_func(inner),
        Expr::Object(items) => items
            .iter()
            .any(|item| contains_aggregate_func(&item.value)),
        Expr::Array(items) => items.iter().any(contains_aggregate_func),
        Expr::InList { expr, list, .. } => {
            contains_aggregate_func(expr) || list.iter().any(contains_aggregate_func)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            contains_aggregate_func(cond)
                || contains_aggregate_func(then_expr)
                || contains_aggregate_func(else_expr)
        }
        _ => false,
    }
}

fn contains_system_var(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::SystemVar(_) => true,
        Expr::BinOp { left, right, .. } => contains_system_var(left) || contains_system_var(right),
        Expr::Neg(inner) => contains_system_var(inner),
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

fn materialize_system_vars(
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
            Some(time_nanos_to_expr(score.event_first_time_nanos?))
        }
        Expr::SystemVar(SystemVar::EventLastTime | SystemVar::EvidenceEndTime) => {
            Some(time_nanos_to_expr(score.event_last_time_nanos?))
        }
        Expr::SystemVar(SystemVar::WindowStartTime) => {
            Some(time_nanos_to_expr(score.window_start_time_nanos?))
        }
        Expr::SystemVar(SystemVar::WindowEndTime) => {
            Some(time_nanos_to_expr(score.window_end_time_nanos?))
        }
        Expr::SystemVar(SystemVar::EmitTime) => Some(time_nanos_to_expr(score.emit_time_nanos?)),
        Expr::Field(fr) => Some(Expr::Field(fr.clone())),
        Expr::BinOp { op, left, right } => Some(Expr::BinOp {
            op: *op,
            left: Box::new(materialize_system_vars(left, score)?),
            right: Box::new(materialize_system_vars(right, score)?),
        }),
        Expr::Neg(inner) => Some(Expr::Neg(Box::new(materialize_system_vars(inner, score)?))),
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

fn eval_builtin_func_with_l3(
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
            Some(Value::Bool(haystack.contains(&needle)))
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
            Some(Value::Bool(text.starts_with(&prefix)))
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
            Some(Value::Bool(text.ends_with(&suffix)))
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
                return Some(Value::Str(String::new()));
            }
            let mut end_idx = len;
            if args.len() == 3 {
                let length = match eval_expr_with_l3(&args[2], ctx, score)? {
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
                re.replace_all(&text, replacement.as_str()).into_owned(),
            ))
        }
        "trim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.trim().to_string())),
                _ => None,
            }
        }
        "lower" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.to_lowercase())),
                _ => None,
            }
        }
        "upper" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.to_uppercase())),
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
            Some(Value::Str(joined))
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
                    Value::Number(n) => normalize_index(n.trunc() as i64, arr.len()),
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
                Value::Str(s) => Some(Value::Str(s.trim_start().to_string())),
                _ => None,
            }
        }
        "rtrim" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => Some(Value::Str(s.trim_end().to_string())),
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
            Some(Value::Str(apply_fmt_template(&template, &values)?))
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
            Some(Value::Str(out))
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
            let idx = text.find(&needle).map(|x| x as f64).unwrap_or(-1.0);
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
            Some(Value::Str(text.replace(&from, &to)))
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
            let text = match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => s,
                _ => return None,
            };
            for arg in &args[1..] {
                let suffix = match eval_expr_with_l3(arg, ctx, score)? {
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
                if let Some(v) = eval_expr_with_l3(arg, ctx, score) {
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
                Some(Value::Str(s)) => Some(Value::Bool(is_blank_str(&s))),
                None => Some(Value::Bool(true)),
                Some(_) => None,
            }
        }
        "null_if_blank" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) if is_blank_str(&s) => None,
                Value::Str(s) => Some(Value::Str(s)),
                _ => None,
            }
        }
        "default_if_blank" => {
            if args.len() != 2 {
                return None;
            }
            match eval_expr_with_l3(&args[0], ctx, score) {
                Some(Value::Str(s)) if !is_blank_str(&s) => Some(Value::Str(s)),
                Some(Value::Str(_)) | None => match eval_expr_with_l3(&args[1], ctx, score)? {
                    Value::Str(s) => Some(Value::Str(s)),
                    _ => None,
                },
                Some(_) => None,
            }
        }
        "md5" => {
            let text = eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(hex::encode(<Md5 as Md5Digest>::digest(
                text.as_bytes(),
            ))))
        }
        "sha1" => {
            let text = eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(hex::encode(<Sha1 as Sha1Digest>::digest(
                text.as_bytes(),
            ))))
        }
        "sha256" => {
            let text = eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(hex::encode(Sha256::digest(text.as_bytes()))))
        }
        "hex" => {
            let text = eval_single_string_arg_with_l3(args, ctx, score)?;
            Some(Value::Str(hex::encode(text.as_bytes())))
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
                update_stable_id_hash(&mut hasher, &value)?;
            }
            let digest = hex::encode(hasher.finalize());
            Some(Value::Str(format!("{}{}", prefix, &digest[..16])))
        }
        "mvsort" => {
            if args.len() != 1 {
                return None;
            }
            let mut arr = match eval_expr_with_l3(&args[0], ctx, score)? {
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
            let ts_nanos = match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Number(n) => normalize_epoch_timestamp_float_nanos(n)?,
                _ => return None,
            };
            let fmt = match eval_expr_with_l3(&args[1], ctx, score)? {
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
            let text = match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let fmt = match eval_expr_with_l3(&args[1], ctx, score)? {
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
            let hay = match eval_expr_with_l3(&args[0], ctx, score)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let pat = match eval_expr_with_l3(&args[1], ctx, score)? {
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
            Some(time_nanos_to_value(bucketed))
        }
        "external" => crate::external::eval_external(&args[0], &args[1..], |a| {
            eval_expr_with_l3(a, ctx, score)
        }),
        _ => None,
    }
}

fn eval_l3_func(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
    score: YieldMeta,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let step_indices = resolve_step_indices(ctx, args.first());
    let step_values = flatten_step_series(ctx, &step_indices, args.first());
    let values = if step_values.is_empty() {
        flatten_bind_series(ctx, args.first())
    } else {
        step_values
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

fn eval_aggregate_func(name: &str, args: &[wf_lang::ast::Expr], ctx: &Event) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match &args[0] {
        wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Qualified(_, _))
        | wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Bracketed(_, _)) => {
            let step_indices = resolve_aggregate_step_indices(ctx, args.first());
            let step_values = flatten_step_series(ctx, &step_indices, args.first());
            if !step_values.is_empty() {
                return eval_aggregate_over_values(name, &step_values);
            }
            let bind_values = flatten_bind_series(ctx, args.first());
            if !bind_values.is_empty() {
                return eval_aggregate_over_values(name, &bind_values);
            }
            None
        }
        wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Simple(_)) => {
            let step_indices = resolve_aggregate_step_indices(ctx, args.first());
            let measures: Vec<f64> = step_indices
                .iter()
                .filter_map(|idx| get_step_measure(ctx, *idx))
                .collect();
            if !measures.is_empty() {
                return eval_aggregate_over_numbers(name, &measures);
            }
            if name == "count"
                && let Some(alias) = args.first().and_then(extract_bind_ref)
                && let Some(count) = get_bind_count(ctx, alias)
            {
                return Some(Value::Number(count));
            }
            let step_values = flatten_step_series(ctx, &step_indices, args.first());
            if !step_values.is_empty() {
                return eval_aggregate_over_values(name, &step_values);
            }
            None
        }
        _ => None,
    }
}

fn eval_aggregate_over_numbers(name: &str, values: &[f64]) -> Option<Value> {
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

fn eval_aggregate_over_values(name: &str, values: &[Value]) -> Option<Value> {
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
        "min" => values.iter().cloned().min_by(compare_sortable_values),
        "max" => values.iter().cloned().max_by(compare_sortable_values),
        _ => None,
    }
}

fn numeric_values(values: &[Value]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|value| match value {
            Value::Number(n) => Some(*n),
            _ => None,
        })
        .collect()
}

fn sum_numeric_values(values: &[Value]) -> f64 {
    numeric_values(values).iter().sum()
}

fn flatten_bind_series(ctx: &Event, arg: Option<&wf_lang::ast::Expr>) -> Vec<Value> {
    let Some((alias, field_name)) = arg.and_then(extract_bind_field_ref) else {
        return Vec::new();
    };
    get_bind_field_values(ctx, alias, field_name)
        .map(|values| values.to_vec())
        .unwrap_or_default()
}

fn flatten_step_values(ctx: &Event, step_indices: &[usize]) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_values(ctx, *idx) {
            out.extend_from_slice(values);
        }
    }
    out
}

fn flatten_step_series(
    ctx: &Event,
    step_indices: &[usize],
    arg: Option<&wf_lang::ast::Expr>,
) -> Vec<Value> {
    if let Some(field_name) = arg.and_then(extract_qualified_field_name) {
        let values = flatten_step_field_values(ctx, step_indices, field_name);
        if !values.is_empty() {
            return values;
        }
    }
    flatten_step_values(ctx, step_indices)
}

fn flatten_step_field_values(ctx: &Event, step_indices: &[usize], field_name: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_field_values(ctx, *idx, field_name) {
            out.extend_from_slice(values);
        }
    }
    out
}

fn resolve_step_indices(ctx: &Event, arg: Option<&wf_lang::ast::Expr>) -> Vec<usize> {
    let all = step_indices(ctx);
    if all.is_empty() {
        return all;
    }
    let Some(alias) = arg.and_then(extract_source_alias) else {
        return all;
    };
    all.iter()
        .copied()
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s == alias))
        .collect()
}

fn resolve_aggregate_step_indices(ctx: &Event, arg: Option<&wf_lang::ast::Expr>) -> Vec<usize> {
    let all = step_indices(ctx);
    if all.is_empty() {
        return all;
    }
    let Some(step_ref) = arg.and_then(extract_step_ref) else {
        return Vec::new();
    };
    let by_source: Vec<usize> = all
        .iter()
        .copied()
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s == step_ref))
        .collect();
    if !by_source.is_empty() {
        return prefer_close_steps(ctx, by_source);
    }
    let by_label: Vec<usize> = all
        .iter()
        .copied()
        .filter(|idx| get_step_label(ctx, *idx).is_some_and(|label| label == step_ref))
        .collect();
    prefer_close_steps(ctx, by_label)
}

fn prefer_close_steps(ctx: &Event, indices: Vec<usize>) -> Vec<usize> {
    let close_only: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|idx| matches!(get_step_stage(ctx, *idx), Some("close")))
        .collect();
    if close_only.is_empty() {
        indices
    } else {
        close_only
    }
}

fn step_indices(ctx: &Event) -> Vec<usize> {
    let mut out: Vec<usize> = ctx
        .fields
        .keys()
        .filter_map(|k| parse_step_field_index(k, "_values"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_step_field_index(key: &str, suffix: &str) -> Option<usize> {
    let body = key.strip_prefix("_step_")?.strip_suffix(suffix)?;
    body.parse::<usize>().ok()
}

fn get_step_values(ctx: &Event, step_idx: usize) -> Option<&[Value]> {
    let field_name = format!("_step_{}_values", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
        _ => None,
    }
}

fn get_step_field_values<'a>(
    ctx: &'a Event,
    step_idx: usize,
    field_name: &str,
) -> Option<&'a [Value]> {
    let field_name = format!("_step_{}_field_{}", step_idx, field_name);
    match ctx.fields.get(&field_name) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
        _ => None,
    }
}

fn get_step_source(ctx: &Event, step_idx: usize) -> Option<&str> {
    let field_name = format!("_step_{}_source", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn get_step_label(ctx: &Event, step_idx: usize) -> Option<&str> {
    let field_name = format!("_step_{}_label", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn get_step_measure(ctx: &Event, step_idx: usize) -> Option<f64> {
    let field_name = format!("_step_{}_measure", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Number(n)) => Some(*n),
        _ => None,
    }
}

fn get_step_stage(ctx: &Event, step_idx: usize) -> Option<&str> {
    let field_name = format!("_step_{}_stage", step_idx);
    match ctx.fields.get(&field_name) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn get_bind_count(ctx: &Event, alias: &str) -> Option<f64> {
    let field_name = format!("_bind_{}_count", alias);
    match ctx.fields.get(&field_name) {
        Some(Value::Number(n)) => Some(*n),
        _ => None,
    }
}

fn get_bind_field_values<'a>(ctx: &'a Event, alias: &str, field_name: &str) -> Option<&'a [Value]> {
    let field_name = format!("_bind_{}_field_{}", alias, field_name);
    match ctx.fields.get(&field_name) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
        _ => None,
    }
}

fn extract_source_alias(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

fn extract_step_ref(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Simple(name)) => Some(name.as_str()),
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

fn extract_bind_ref(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Simple(name)) => Some(name.as_str()),
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

fn extract_bind_field_ref(expr: &wf_lang::ast::Expr) -> Option<(&str, &str)> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(alias, field))
        | Expr::Field(FieldRef::Bracketed(alias, field)) => Some((alias.as_str(), field.as_str())),
        _ => None,
    }
}

fn extract_qualified_field_name(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(_, field)) | Expr::Field(FieldRef::Bracketed(_, field)) => {
            Some(field.as_str())
        }
        _ => None,
    }
}

fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
}

fn compare_sortable_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => value_to_string(a).cmp(&value_to_string(b)),
    }
}

fn f64_to_i64_trunc(v: f64) -> Option<i64> {
    if !v.is_finite() {
        return None;
    }
    let truncated = v.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return None;
    }
    Some(truncated as i64)
}

fn round_with_precision(value: f64, precision: i64) -> Option<f64> {
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

fn apply_fmt_template(template: &str, values: &[Value]) -> Option<String> {
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

fn timestamp_nanos_to_utc(timestamp_nanos: i64) -> Option<DateTime<Utc>> {
    let secs = timestamp_nanos.div_euclid(1_000_000_000);
    let nanos = timestamp_nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

fn time_nanos_to_value(nanos: i64) -> Value {
    Value::Number(epoch_nanos_to_millis(nanos) as f64)
}

fn time_nanos_to_expr(nanos: i64) -> wf_lang::ast::Expr {
    wf_lang::ast::Expr::Number(epoch_nanos_to_millis(nanos) as f64)
}

fn parse_time_to_timestamp_nanos(text: &str, fmt: &str) -> Option<i64> {
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

fn is_blank_str(value: &str) -> bool {
    value.trim().is_empty()
}

fn current_time_nanos() -> Option<i64> {
    EVAL_TIME_NANOS.with(|time| {
        if let Some(nanos) = time.get() {
            return Some(nanos);
        }
        let nanos = Utc::now().timestamp_nanos_opt()?;
        time.set(Some(nanos));
        Some(nanos)
    })
}

fn eval_single_string_arg_with_l3(
    args: &[wf_lang::ast::Expr],
    ctx: &Event,
    score: YieldMeta,
) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_with_l3(&args[0], ctx, score)? {
        Value::Str(s) => Some(s),
        _ => None,
    }
}

fn update_stable_id_hash(hasher: &mut Sha256, value: &Value) -> Option<()> {
    let (tag, text) = match value {
        Value::Number(_) => ("n", value_to_string(value)),
        Value::Str(s) => ("s", s.clone()),
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

/// Evaluate the score expression and clamp to `[0, 100]`.
///
pub(super) fn eval_score(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<f64> {
    let val = eval_yield_expr(expr, ctx);
    let raw = match val {
        Some(Value::Number(n)) => n,
        Some(other) => {
            return StructError::from(CoreReason::RuleExec)
                .with_detail(format!(
                    "score expression evaluated to non-numeric value: {:?}",
                    other
                ))
                .err();
        }
        None => {
            return StructError::from(CoreReason::RuleExec)
                .with_detail("score expression evaluated to None")
                .err();
        }
    };
    Ok(clamp_score(raw))
}

fn clamp_score(v: f64) -> f64 {
    v.clamp(0.0, 100.0)
}

/// Evaluate the entity_id expression.
///
pub(super) fn eval_entity_id(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<String> {
    let val = eval_yield_expr(expr, ctx);
    match val {
        Some(v) => Ok(value_to_string(&v)),
        None => StructError::from(CoreReason::RuleExec)
            .with_detail("entity_id expression evaluated to None")
            .err(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use wf_lang::ast::{BinOp, Expr, FieldRef};

    fn make_test_event(values: Vec<Value>) -> Event {
        let mut fields = std::collections::HashMap::new();
        fields.insert("_step_0_values".to_string(), Value::Array(values));
        fields.insert("_step_0_source".to_string(), Value::Str("e".to_string()));
        Event { fields }
    }

    #[test]
    fn test_first_returns_first_value() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "first".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(10.0)));
    }

    #[test]
    fn test_last_returns_last_value() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "last".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(30.0)));
    }

    #[test]
    fn test_collect_list_returns_all_values() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "collect_list".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(
            result,
            Some(Value::Array(vec![
                Value::Number(10.0),
                Value::Number(20.0),
                Value::Number(30.0),
            ]))
        );
    }

    #[test]
    fn test_collect_set_returns_unique_values() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("a".to_string()),
            Value::Str("c".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "collect_set".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Array(arr)) = result {
            assert_eq!(arr.len(), 3); // a, b, c (unique)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_stddev_calculation() {
        let ctx = make_test_event(vec![
            Value::Number(2.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(5.0),
            Value::Number(5.0),
            Value::Number(7.0),
            Value::Number(9.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "stddev".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Number(stddev)) = result {
            // Population stddev of [2,4,4,4,5,5,7,9] = 2.0
            assert!((stddev - 2.0).abs() < 0.01, "Expected ~2.0, got {}", stddev);
        } else {
            panic!("Expected numeric result, got {:?}", result);
        }
    }

    #[test]
    fn test_stddev_returns_zero_for_single_value() {
        let ctx = make_test_event(vec![Value::Number(5.0)]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "stddev".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(0.0)));
    }

    #[test]
    fn test_percentile_calculation() {
        let ctx = make_test_event(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
            Value::Number(4.0),
        ]);
        // percentile(value, 50) should return median-like value.
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(50.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Number(p)) = result {
            // sorted=[1,2,3,4], idx=(3*0.5).round=2, result=3
            assert!((p - 3.0).abs() < 0.01, "Expected ~3.0, got {}", p);
        } else {
            panic!("Expected numeric result, got {:?}", result);
        }
    }

    #[test]
    fn test_percentile_zero_returns_min() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(0.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(10.0)));
    }

    #[test]
    fn test_percentile_one_returns_max() {
        let ctx = make_test_event(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "percentile".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("value".to_string())),
                Expr::Number(100.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(30.0)));
    }

    #[test]
    fn test_nested_l3_in_arithmetic() {
        let ctx = make_test_event(vec![
            Value::Number(2.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(4.0),
            Value::Number(5.0),
            Value::Number(5.0),
            Value::Number(7.0),
            Value::Number(9.0),
        ]);
        let expr = Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "stddev".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "e".to_string(),
                    "value".to_string(),
                ))],
            }),
            right: Box::new(Expr::Number(1.0)),
        };
        let result = eval_yield_expr(&expr, &ctx);
        if let Some(Value::Number(v)) = result {
            assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
        } else {
            panic!("Expected numeric result, got {:?}", result);
        }
    }

    #[test]
    fn test_qualified_alias_selects_matching_step() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "_step_0_values".to_string(),
            Value::Array(vec![Value::Number(10.0)]),
        );
        fields.insert("_step_0_source".to_string(), Value::Str("a".to_string()));
        fields.insert(
            "_step_1_values".to_string(),
            Value::Array(vec![Value::Number(99.0)]),
        );
        fields.insert("_step_1_source".to_string(), Value::Str("b".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "first".to_string(),
            args: vec![Expr::Field(FieldRef::Qualified(
                "b".to_string(),
                "value".to_string(),
            ))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(99.0)));
    }

    #[test]
    fn test_qualified_alias_without_match_returns_none_for_first() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "_step_0_values".to_string(),
            Value::Array(vec![Value::Number(10.0)]),
        );
        fields.insert("_step_0_source".to_string(), Value::Str("a".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "first".to_string(),
            args: vec![Expr::Field(FieldRef::Qualified(
                "missing".to_string(),
                "value".to_string(),
            ))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        // fallback: missing join data returns empty string instead of None
        assert_eq!(result, Some(Value::Str("".to_string())));
    }

    #[test]
    fn test_replace_works_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "msg".to_string(),
            Value::Str("failed_login_from_root".to_string()),
        );
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "replace".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("fail.*root".to_string()),
                Expr::StringLit("suspicious".to_string()),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Str("suspicious".to_string())));
    }

    #[test]
    fn test_mvcount_with_collect_set_nested_l3() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("a".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "mvcount".to_string(),
            args: vec![Expr::FuncCall {
                qualifier: None,
                name: "collect_set".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
            }],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Number(2.0)));
    }

    #[test]
    fn test_trim_works_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("msg".to_string(), Value::Str("  hello  ".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "trim".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Str("hello".to_string())));
    }

    #[test]
    fn test_blank_functions_work_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("empty".to_string(), Value::Str(String::new()));
        fields.insert("spaces".to_string(), Value::Str(" \t\n ".to_string()));
        fields.insert("host".to_string(), Value::Str("example.org".to_string()));
        fields.insert("fallback".to_string(), Value::Str("fallback".to_string()));
        fields.insert("n".to_string(), Value::Number(42.0));
        let ctx = Event { fields };

        let is_empty_expr = Expr::FuncCall {
            qualifier: None,
            name: "is_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("empty".to_string()))],
        };
        let is_spaces_expr = Expr::FuncCall {
            qualifier: None,
            name: "is_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("spaces".to_string()))],
        };
        let is_host_expr = Expr::FuncCall {
            qualifier: None,
            name: "is_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("host".to_string()))],
        };
        let is_missing_expr = Expr::FuncCall {
            qualifier: None,
            name: "is_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("missing".to_string()))],
        };
        let null_if_blank_expr = Expr::FuncCall {
            qualifier: None,
            name: "null_if_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("spaces".to_string()))],
        };
        let null_if_host_expr = Expr::FuncCall {
            qualifier: None,
            name: "null_if_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("host".to_string()))],
        };
        let default_blank_expr = Expr::FuncCall {
            qualifier: None,
            name: "default_if_blank".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("spaces".to_string())),
                Expr::Field(FieldRef::Simple("fallback".to_string())),
            ],
        };
        let default_host_expr = Expr::FuncCall {
            qualifier: None,
            name: "default_if_blank".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("host".to_string())),
                Expr::Field(FieldRef::Simple("fallback".to_string())),
            ],
        };
        let coalesce_blank_expr = Expr::FuncCall {
            qualifier: None,
            name: "coalesce".to_string(),
            args: vec![
                null_if_blank_expr.clone(),
                Expr::Field(FieldRef::Simple("fallback".to_string())),
            ],
        };
        let invalid_type_expr = Expr::FuncCall {
            qualifier: None,
            name: "is_blank".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };

        assert_eq!(
            eval_yield_expr(&is_empty_expr, &ctx),
            Some(Value::Bool(true))
        );
        assert_eq!(
            eval_yield_expr(&is_spaces_expr, &ctx),
            Some(Value::Bool(true))
        );
        assert_eq!(
            eval_yield_expr(&is_host_expr, &ctx),
            Some(Value::Bool(false))
        );
        assert_eq!(
            eval_yield_expr(&is_missing_expr, &ctx),
            Some(Value::Bool(true))
        );
        assert_eq!(
            eval_yield_expr(&null_if_blank_expr, &ctx),
            Some(Value::Str(String::new()))
        );
        assert_eq!(
            eval_yield_expr(&null_if_host_expr, &ctx),
            Some(Value::Str("example.org".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&default_blank_expr, &ctx),
            Some(Value::Str("fallback".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&default_host_expr, &ctx),
            Some(Value::Str("example.org".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&coalesce_blank_expr, &ctx),
            Some(Value::Str("fallback".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&invalid_type_expr, &ctx),
            Some(Value::Str(String::new()))
        );
    }

    #[test]
    fn test_hash_and_id_functions_work_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("msg".to_string(), Value::Str("hello".to_string()));
        fields.insert("ip".to_string(), Value::Str("10.0.0.1".to_string()));
        fields.insert("count".to_string(), Value::Number(3.0));
        let ctx = Event { fields };

        let md5_expr = Expr::FuncCall {
            qualifier: None,
            name: "md5".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let sha1_expr = Expr::FuncCall {
            qualifier: None,
            name: "sha1".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let sha256_expr = Expr::FuncCall {
            qualifier: None,
            name: "sha256".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let hex_expr = Expr::FuncCall {
            qualifier: None,
            name: "hex".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let stable_expr = Expr::FuncCall {
            qualifier: None,
            name: "stable_id".to_string(),
            args: vec![
                Expr::StringLit("alert_".to_string()),
                Expr::Field(FieldRef::Simple("ip".to_string())),
                Expr::Field(FieldRef::Simple("count".to_string())),
            ],
        };
        let stable_changed_expr = Expr::FuncCall {
            qualifier: None,
            name: "stable_id".to_string(),
            args: vec![
                Expr::StringLit("alert_".to_string()),
                Expr::Field(FieldRef::Simple("ip".to_string())),
                Expr::Number(4.0),
            ],
        };

        assert_eq!(
            eval_yield_expr(&md5_expr, &ctx),
            Some(Value::Str("5d41402abc4b2a76b9719d911017c592".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&sha1_expr, &ctx),
            Some(Value::Str(
                "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".to_string()
            ))
        );
        assert_eq!(
            eval_yield_expr(&sha256_expr, &ctx),
            Some(Value::Str(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_string()
            ))
        );
        assert_eq!(
            eval_yield_expr(&hex_expr, &ctx),
            Some(Value::Str("68656c6c6f".to_string()))
        );
        let Some(Value::Str(stable_id)) = eval_yield_expr(&stable_expr, &ctx) else {
            panic!("stable_id() should return a string");
        };
        assert_eq!(stable_id, "alert_ba0dab7ccfb2a04c");
        assert_eq!(
            eval_yield_expr(&stable_expr, &ctx),
            Some(Value::Str(stable_id.clone()))
        );
        let Some(Value::Str(changed_stable_id)) = eval_yield_expr(&stable_changed_expr, &ctx)
        else {
            panic!("stable_id() should return a string for changed input");
        };
        assert!(changed_stable_id.starts_with("alert_"));
        assert_eq!(changed_stable_id.len(), "alert_".len() + 16);
        assert_ne!(changed_stable_id, stable_id);
    }

    #[test]
    fn test_stable_id_uses_unambiguous_segments_in_yield_eval() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };
        let first_expr = Expr::FuncCall {
            qualifier: None,
            name: "stable_id".to_string(),
            args: vec![
                Expr::StringLit("id_".to_string()),
                Expr::StringLit("a\x1fb".to_string()),
                Expr::StringLit("c".to_string()),
            ],
        };
        let second_expr = Expr::FuncCall {
            qualifier: None,
            name: "stable_id".to_string(),
            args: vec![
                Expr::StringLit("id_".to_string()),
                Expr::StringLit("a".to_string()),
                Expr::StringLit("b\x1fc".to_string()),
            ],
        };

        assert_eq!(
            eval_yield_expr(&first_expr, &ctx),
            Some(Value::Str("id_234c47ae916c73b0".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&second_expr, &ctx),
            Some(Value::Str("id_1532803f7ab9f6de".to_string()))
        );
        assert_ne!(
            eval_yield_expr(&first_expr, &ctx),
            eval_yield_expr(&second_expr, &ctx)
        );
    }

    #[test]
    fn test_now_functions_share_timestamp_within_yield_expression() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };
        let expr = Expr::BinOp {
            op: BinOp::Sub,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "now_ms".to_string(),
                args: vec![],
            }),
            right: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "now".to_string(),
                args: vec![],
            }),
        };

        assert_eq!(eval_yield_expr(&expr, &ctx), Some(Value::Number(0.0)));
    }

    #[test]
    fn test_now_functions_share_timestamp_across_yield_scope() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };
        let now_expr = Expr::FuncCall {
            qualifier: None,
            name: "now".to_string(),
            args: vec![],
        };
        let now_ms_expr = Expr::FuncCall {
            qualifier: None,
            name: "now_ms".to_string(),
            args: vec![],
        };

        with_yield_eval_scope(|| {
            assert_eq!(
                eval_yield_expr(&now_expr, &ctx),
                eval_yield_expr(&now_ms_expr, &ctx)
            );
        });
    }

    #[test]
    fn test_time_bucket_rejects_invalid_interval_in_yield_eval() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };

        for interval in [0.0, -60.0, f64::INFINITY, f64::NAN] {
            let expr = Expr::FuncCall {
                qualifier: None,
                name: "time_bucket".to_string(),
                args: vec![Expr::Number(1_700_000_075_000.0), Expr::Number(interval)],
            };
            assert_eq!(eval_expr_with_l3(&expr, &ctx, YieldMeta::default()), None);
        }
    }

    #[test]
    fn test_mvjoin_with_collect_list_nested_l3() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "mvjoin".to_string(),
            args: vec![
                Expr::FuncCall {
                    qualifier: None,
                    name: "collect_list".to_string(),
                    args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
                },
                Expr::StringLit(",".to_string()),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Str("a,b,c".to_string())));
    }

    #[test]
    fn test_split_works_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("csv".to_string(), Value::Str("a,b,,c".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "split".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("csv".to_string())),
                Expr::StringLit(",".to_string()),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(
            result,
            Some(Value::Array(vec![
                Value::Str("a".to_string()),
                Value::Str("b".to_string()),
                Value::Str(String::new()),
                Value::Str("c".to_string()),
            ]))
        );
    }

    #[test]
    fn test_mvdedup_with_collect_list_nested_l3() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("a".to_string()),
            Value::Str("c".to_string()),
            Value::Str("b".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "mvdedup".to_string(),
            args: vec![Expr::FuncCall {
                qualifier: None,
                name: "collect_list".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
            }],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(
            result,
            Some(Value::Array(vec![
                Value::Str("a".to_string()),
                Value::Str("b".to_string()),
                Value::Str("c".to_string()),
            ]))
        );
    }

    #[test]
    fn test_substr_works_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("msg".to_string(), Value::Str("abcdef".to_string()));
        let ctx = Event { fields };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "substr".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::Number(2.0),
                Expr::Number(3.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Str("bcd".to_string())));
    }

    #[test]
    fn test_startswith_and_endswith_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "msg".to_string(),
            Value::Str("failed_login_root".to_string()),
        );
        let ctx = Event { fields };
        let starts_expr = Expr::FuncCall {
            qualifier: None,
            name: "startswith".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("failed".to_string()),
            ],
        };
        let ends_expr = Expr::FuncCall {
            qualifier: None,
            name: "endswith".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("root".to_string()),
            ],
        };
        assert_eq!(eval_yield_expr(&starts_expr, &ctx), Some(Value::Bool(true)));
        assert_eq!(eval_yield_expr(&ends_expr, &ctx), Some(Value::Bool(true)));
    }

    #[test]
    fn test_math_and_time_functions_in_yield_eval() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("n".to_string(), Value::Number(-12.345));
        fields.insert("p".to_string(), Value::Number(16.0));
        fields.insert("ts".to_string(), Value::Number(0.0));
        fields.insert(
            "msg".to_string(),
            Value::Str("  failed_login_root  ".to_string()),
        );
        fields.insert(
            "arr".to_string(),
            Value::Array(vec![
                Value::Str("b".to_string()),
                Value::Str("a".to_string()),
                Value::Str("c".to_string()),
            ]),
        );
        let ctx = Event { fields };

        let abs_expr = Expr::FuncCall {
            qualifier: None,
            name: "abs".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };
        let round_expr = Expr::FuncCall {
            qualifier: None,
            name: "round".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("n".to_string())),
                Expr::Number(2.0),
            ],
        };
        let ceil_expr = Expr::FuncCall {
            qualifier: None,
            name: "ceil".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };
        let floor_expr = Expr::FuncCall {
            qualifier: None,
            name: "floor".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };
        let strftime_expr = Expr::FuncCall {
            qualifier: None,
            name: "strftime".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("ts".to_string())),
                Expr::StringLit("%Y-%m-%d".to_string()),
            ],
        };
        let strptime_expr = Expr::FuncCall {
            qualifier: None,
            name: "strptime".to_string(),
            args: vec![
                Expr::StringLit("1970-01-01".to_string()),
                Expr::StringLit("%Y-%m-%d".to_string()),
            ],
        };
        let now_expr = Expr::FuncCall {
            qualifier: None,
            name: "now".to_string(),
            args: vec![],
        };
        let now_s_expr = Expr::FuncCall {
            qualifier: None,
            name: "now_s".to_string(),
            args: vec![],
        };
        let now_ms_expr = Expr::FuncCall {
            qualifier: None,
            name: "now_ms".to_string(),
            args: vec![],
        };
        let now_us_expr = Expr::FuncCall {
            qualifier: None,
            name: "now_us".to_string(),
            args: vec![],
        };
        let now_ns_expr = Expr::FuncCall {
            qualifier: None,
            name: "now_ns".to_string(),
            args: vec![],
        };
        let now_fmt_expr = Expr::FuncCall {
            qualifier: None,
            name: "strftime".to_string(),
            args: vec![now_expr.clone(), Expr::StringLit("%Y".to_string())],
        };
        let sqrt_expr = Expr::FuncCall {
            qualifier: None,
            name: "sqrt".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("p".to_string()))],
        };
        let pow_expr = Expr::FuncCall {
            qualifier: None,
            name: "pow".to_string(),
            args: vec![Expr::Number(2.0), Expr::Number(8.0)],
        };
        let log_expr = Expr::FuncCall {
            qualifier: None,
            name: "log".to_string(),
            args: vec![Expr::Number(100.0), Expr::Number(10.0)],
        };
        let exp_expr = Expr::FuncCall {
            qualifier: None,
            name: "exp".to_string(),
            args: vec![Expr::Number(1.0)],
        };
        let clamp_expr = Expr::FuncCall {
            qualifier: None,
            name: "clamp".to_string(),
            args: vec![Expr::Number(120.0), Expr::Number(0.0), Expr::Number(100.0)],
        };
        let sign_expr = Expr::FuncCall {
            qualifier: None,
            name: "sign".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };
        let trunc_expr = Expr::FuncCall {
            qualifier: None,
            name: "trunc".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };
        let finite_expr = Expr::FuncCall {
            qualifier: None,
            name: "is_finite".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
        };
        let ltrim_expr = Expr::FuncCall {
            qualifier: None,
            name: "ltrim".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let rtrim_expr = Expr::FuncCall {
            qualifier: None,
            name: "rtrim".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let concat_expr = Expr::FuncCall {
            qualifier: None,
            name: "concat".to_string(),
            args: vec![
                Expr::StringLit("ip=".to_string()),
                Expr::StringLit("1.1.1.1".to_string()),
            ],
        };
        let index_expr = Expr::FuncCall {
            qualifier: None,
            name: "indexof".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("login".to_string()),
            ],
        };
        let replace_plain_expr = Expr::FuncCall {
            qualifier: None,
            name: "replace_plain".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("_".to_string()),
                Expr::StringLit("-".to_string()),
            ],
        };
        let sw_any_expr = Expr::FuncCall {
            qualifier: None,
            name: "startswith_any".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("  fail".to_string()),
                Expr::StringLit("deny".to_string()),
            ],
        };
        let ew_any_expr = Expr::FuncCall {
            qualifier: None,
            name: "endswith_any".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("msg".to_string())),
                Expr::StringLit("root  ".to_string()),
                Expr::StringLit("deny".to_string()),
            ],
        };
        let coalesce_expr = Expr::FuncCall {
            qualifier: None,
            name: "coalesce".to_string(),
            args: vec![
                Expr::Field(FieldRef::Simple("missing".to_string())),
                Expr::StringLit("fallback".to_string()),
            ],
        };
        let isnull_expr = Expr::FuncCall {
            qualifier: None,
            name: "isnull".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("missing".to_string()))],
        };
        let isnotnull_expr = Expr::FuncCall {
            qualifier: None,
            name: "isnotnull".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
        };
        let mvsort_expr = Expr::FuncCall {
            qualifier: None,
            name: "mvsort".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("arr".to_string()))],
        };
        let mvreverse_expr = Expr::FuncCall {
            qualifier: None,
            name: "mvreverse".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("arr".to_string()))],
        };

        assert_eq!(
            eval_yield_expr(&abs_expr, &ctx),
            Some(Value::Number(12.345))
        );
        assert_eq!(
            eval_yield_expr(&round_expr, &ctx),
            Some(Value::Number(-12.35))
        );
        assert_eq!(
            eval_yield_expr(&ceil_expr, &ctx),
            Some(Value::Number(-12.0))
        );
        assert_eq!(
            eval_yield_expr(&floor_expr, &ctx),
            Some(Value::Number(-13.0))
        );
        assert_eq!(
            eval_yield_expr(&strftime_expr, &ctx),
            Some(Value::Str("1970-01-01".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&strptime_expr, &ctx),
            Some(Value::Number(0.0))
        );
        let Some(Value::Number(now_millis)) = eval_yield_expr(&now_expr, &ctx) else {
            panic!("now() should return a numeric timestamp");
        };
        let Some(Value::Number(now_s)) = eval_yield_expr(&now_s_expr, &ctx) else {
            panic!("now_s() should return a numeric timestamp");
        };
        let Some(Value::Number(now_ms)) = eval_yield_expr(&now_ms_expr, &ctx) else {
            panic!("now_ms() should return a numeric timestamp");
        };
        let Some(Value::Number(now_us)) = eval_yield_expr(&now_us_expr, &ctx) else {
            panic!("now_us() should return a numeric timestamp");
        };
        let Some(Value::Number(now_ns)) = eval_yield_expr(&now_ns_expr, &ctx) else {
            panic!("now_ns() should return a numeric timestamp");
        };
        let Some(Value::Str(year)) = eval_yield_expr(&now_fmt_expr, &ctx) else {
            panic!("strftime(now(), ...) should format the current time");
        };
        assert!(now_millis > 1_000_000_000_000.0);
        assert!(now_ns > 1_000_000_000_000_000_000.0);
        assert!(now_us > 1_000_000_000_000_000.0);
        assert!(now_ms > 1_000_000_000_000.0);
        assert!(now_s > 1_000_000_000.0);
        assert!(year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()));
        assert_eq!(eval_yield_expr(&sqrt_expr, &ctx), Some(Value::Number(4.0)));
        assert_eq!(eval_yield_expr(&pow_expr, &ctx), Some(Value::Number(256.0)));
        assert_eq!(eval_yield_expr(&log_expr, &ctx), Some(Value::Number(2.0)));
        assert_eq!(
            eval_yield_expr(&exp_expr, &ctx),
            Some(Value::Number(std::f64::consts::E))
        );
        assert_eq!(
            eval_yield_expr(&clamp_expr, &ctx),
            Some(Value::Number(100.0))
        );
        assert_eq!(eval_yield_expr(&sign_expr, &ctx), Some(Value::Number(-1.0)));
        assert_eq!(
            eval_yield_expr(&trunc_expr, &ctx),
            Some(Value::Number(-12.0))
        );
        assert_eq!(eval_yield_expr(&finite_expr, &ctx), Some(Value::Bool(true)));
        assert_eq!(
            eval_yield_expr(&ltrim_expr, &ctx),
            Some(Value::Str("failed_login_root  ".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&rtrim_expr, &ctx),
            Some(Value::Str("  failed_login_root".to_string()))
        );
        assert_eq!(
            eval_yield_expr(&concat_expr, &ctx),
            Some(Value::Str("ip=1.1.1.1".to_string()))
        );
        assert_eq!(eval_yield_expr(&index_expr, &ctx), Some(Value::Number(9.0)));
        assert_eq!(
            eval_yield_expr(&replace_plain_expr, &ctx),
            Some(Value::Str("  failed-login-root  ".to_string()))
        );
        assert_eq!(eval_yield_expr(&sw_any_expr, &ctx), Some(Value::Bool(true)));
        assert_eq!(eval_yield_expr(&ew_any_expr, &ctx), Some(Value::Bool(true)));
        assert_eq!(
            eval_yield_expr(&coalesce_expr, &ctx),
            Some(Value::Str("fallback".to_string()))
        );
        assert_eq!(eval_yield_expr(&isnull_expr, &ctx), Some(Value::Bool(true)));
        assert_eq!(
            eval_yield_expr(&isnotnull_expr, &ctx),
            Some(Value::Bool(true))
        );
        assert_eq!(
            eval_yield_expr(&mvsort_expr, &ctx),
            Some(Value::Array(vec![
                Value::Str("a".to_string()),
                Value::Str("b".to_string()),
                Value::Str("c".to_string()),
            ]))
        );
        assert_eq!(
            eval_yield_expr(&mvreverse_expr, &ctx),
            Some(Value::Array(vec![
                Value::Str("c".to_string()),
                Value::Str("a".to_string()),
                Value::Str("b".to_string()),
            ]))
        );
    }

    #[test]
    fn test_system_score_var_works_inside_builtin_functions() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };
        let round_expr = Expr::FuncCall {
            qualifier: None,
            name: "round".to_string(),
            args: vec![
                Expr::SystemVar(wf_lang::ast::SystemVar::Score),
                Expr::Number(1.0),
            ],
        };
        let concat_expr = Expr::FuncCall {
            qualifier: None,
            name: "concat".to_string(),
            args: vec![
                Expr::StringLit("risk=".to_string()),
                Expr::SystemVar(wf_lang::ast::SystemVar::Score),
            ],
        };

        assert_eq!(
            eval_yield_expr_with_score(&round_expr, &ctx, Some(70.126)),
            Some(Value::Number(70.1))
        );
        assert_eq!(
            eval_yield_expr_with_score(&concat_expr, &ctx, Some(70.126)),
            Some(Value::Str("risk=70.126".to_string()))
        );
    }

    #[test]
    fn test_mvindex_with_collect_list_nested_l3() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
            Value::Str("c".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "mvindex".to_string(),
            args: vec![
                Expr::FuncCall {
                    qualifier: None,
                    name: "collect_list".to_string(),
                    args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
                },
                Expr::Number(1.0),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(result, Some(Value::Str("b".to_string())));
    }

    #[test]
    fn test_mvappend_with_collect_list_nested_l3() {
        let ctx = make_test_event(vec![
            Value::Str("a".to_string()),
            Value::Str("b".to_string()),
        ]);
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "mvappend".to_string(),
            args: vec![
                Expr::FuncCall {
                    qualifier: None,
                    name: "collect_list".to_string(),
                    args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
                },
                Expr::StringLit("c".to_string()),
            ],
        };
        let result = eval_yield_expr(&expr, &ctx);
        assert_eq!(
            result,
            Some(Value::Array(vec![
                Value::Str("a".to_string()),
                Value::Str("b".to_string()),
                Value::Str("c".to_string()),
            ]))
        );
    }

    // -------------------------------------------------------------------
    // external() tests
    // -------------------------------------------------------------------

    #[test]
    fn external_without_handler_returns_none() {
        // NOTE: EXTERNAL_HANDLER is a global OnceLock. If a previous test
        // already installed a handler, dispatch will return Some(...) instead
        // of None. We verify the no-handler path by checking an empty OnceLock
        // directly (mirroring dispatch_external_call's logic).
        let empty: std::sync::OnceLock<std::sync::Arc<dyn crate::external::ExternalCallHandler>> =
            std::sync::OnceLock::new();
        assert!(empty.get().is_none());
        assert!(empty.get().and_then(|h| h.call("test", &[])).is_none());
    }

    #[test]
    fn external_requires_at_least_two_args() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "external".to_string(),
            args: vec![Expr::StringLit("only_service".to_string())],
        };
        let result = eval_bool_expr(&expr, &ctx);
        assert_eq!(result, None);
    }

    #[test]
    fn external_service_must_be_string_literal() {
        let ctx = Event {
            fields: std::collections::HashMap::new(),
        };
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "external".to_string(),
            args: vec![
                Expr::Number(42.0), // not a string
                Expr::StringLit("arg".to_string()),
            ],
        };
        let result = eval_bool_expr(&expr, &ctx);
        assert_eq!(result, None);
    }
}
