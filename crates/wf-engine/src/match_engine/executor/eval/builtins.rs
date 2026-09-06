use md5::Digest as Md5Digest;
use md5::Md5;
use sha1::Digest as Sha1Digest;
use sha1::Sha1;
use sha2::Sha256;

use super::{Value, YieldMeta, eval_expr_with_l3, step_data, utils};
use crate::match_engine::cep::{EngineHashMap, FieldSource, value_to_string, values_equal};
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
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            contains_system_var(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(contains_system_var) || contains_system_var(&arm.value)
                })
                || default.as_ref().is_some_and(|d| contains_system_var(d))
        }
        _ => false,
    }
}

pub(super) fn materialize_system_vars(
    expr: &wf_lang::ast::Expr,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    match expr {
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) | Expr::Field(_) => {
            materialize_simple_leaf(expr)
        }
        Expr::SystemVar(sv) => materialize_system_var(sv, score),
        Expr::WfuMeta(field) => materialize_wfu_meta(*field, score),
        Expr::BinOp { op, left, right } => materialize_binop(*op, left, right, score),
        Expr::Neg(inner) => map_materialize_boxed(inner, score, Expr::Neg),
        Expr::Not(inner) => map_materialize_boxed(inner, score, Expr::Not),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => materialize_func_call(qualifier, name, args, score),
        Expr::Object(items) => materialize_object(items, score),
        Expr::Array(items) => materialize_array(items, score),
        Expr::InList {
            expr,
            list,
            negated,
        } => materialize_in_list(expr, list, *negated, score),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => materialize_if_then_else(cond, then_expr, else_expr, score),
        Expr::Match {
            expr,
            arms,
            default,
        } => materialize_match(expr, arms, default, score),
        _ => None,
    }
}

/// 无需递归的叶子/透传节点。
fn materialize_simple_leaf(expr: &wf_lang::ast::Expr) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    match expr {
        Expr::Number(n) => Some(Expr::Number(*n)),
        Expr::StringLit(s) => Some(Expr::StringLit(s.clone())),
        Expr::Bool(b) => Some(Expr::Bool(*b)),
        Expr::Field(fr) => Some(Expr::Field(fr.clone())),
        _ => None,
    }
}

/// 系统变量 → 字面量表达式。
fn materialize_system_var(
    sv: &wf_lang::ast::SystemVar,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::{Expr, SystemVar};
    match *sv {
        SystemVar::Score => Some(Expr::Number(score.score?)),
        SystemVar::EventFirstTime => Some(utils::time_nanos_to_expr(score.event_first_time_nanos?)),
        SystemVar::EventLastTime => Some(utils::time_nanos_to_expr(score.event_last_time_nanos?)),
        SystemVar::EvidenceStartTime => {
            Some(utils::time_nanos_to_expr(score.evidence_first_time_nanos?))
        }
        SystemVar::EvidenceEndTime => {
            Some(utils::time_nanos_to_expr(score.evidence_last_time_nanos?))
        }
        SystemVar::WindowStartTime => {
            Some(utils::time_nanos_to_expr(score.window_start_time_nanos?))
        }
        SystemVar::WindowEndTime => Some(utils::time_nanos_to_expr(score.window_end_time_nanos?)),
        SystemVar::EmitTime => Some(utils::time_nanos_to_expr(score.emit_time_nanos?)),
        SystemVar::FirstMatchTime => Some(utils::time_nanos_to_expr(score.first_match_time_nanos?)),
        _ => None,
    }
}

/// wfu 元字段 → 对应字面量表达式（容器值不支持 → None）。
fn materialize_wfu_meta(
    field: wf_lang::wfu_meta::WfuMetaField,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    match score.resolve_wfu_meta(field)? {
        Value::Number(n) => Some(Expr::Number(n)),
        Value::Str(s) => Some(Expr::StringLit(s.to_string())),
        Value::Bool(b) => Some(Expr::Bool(b)),
        _ => None,
    }
}

/// 单个子节点的递归包装（Neg / Not）。
fn map_materialize_boxed(
    inner: &wf_lang::ast::Expr,
    score: YieldMeta,
    wrap: fn(Box<wf_lang::ast::Expr>) -> wf_lang::ast::Expr,
) -> Option<wf_lang::ast::Expr> {
    Some(wrap(Box::new(materialize_system_vars(inner, score)?)))
}

fn materialize_binop(
    op: wf_lang::ast::BinOp,
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::BinOp {
        op,
        left: Box::new(materialize_system_vars(left, score)?),
        right: Box::new(materialize_system_vars(right, score)?),
    })
}

fn materialize_func_call(
    qualifier: &Option<String>,
    name: &str,
    args: &[wf_lang::ast::Expr],
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::FuncCall {
        qualifier: qualifier.clone(),
        name: name.to_string(),
        args: args
            .iter()
            .map(|arg| materialize_system_vars(arg, score))
            .collect::<Option<Vec<_>>>()?,
    })
}

fn materialize_object(
    items: &[wf_lang::ast::ObjectItem],
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::Object(
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
    ))
}

fn materialize_array(items: &[wf_lang::ast::Expr], score: YieldMeta) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::Array(
        items
            .iter()
            .map(|item| materialize_system_vars(item, score))
            .collect::<Option<Vec<_>>>()?,
    ))
}

fn materialize_in_list(
    expr: &wf_lang::ast::Expr,
    list: &[wf_lang::ast::Expr],
    negated: bool,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::InList {
        expr: Box::new(materialize_system_vars(expr, score)?),
        list: list
            .iter()
            .map(|item| materialize_system_vars(item, score))
            .collect::<Option<Vec<_>>>()?,
        negated,
    })
}

fn materialize_if_then_else(
    cond: &wf_lang::ast::Expr,
    then_expr: &wf_lang::ast::Expr,
    else_expr: &wf_lang::ast::Expr,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::IfThenElse {
        cond: Box::new(materialize_system_vars(cond, score)?),
        then_expr: Box::new(materialize_system_vars(then_expr, score)?),
        else_expr: Box::new(materialize_system_vars(else_expr, score)?),
    })
}

fn materialize_match(
    expr: &wf_lang::ast::Expr,
    arms: &[wf_lang::ast::MatchArm],
    default: &Option<Box<wf_lang::ast::Expr>>,
    score: YieldMeta,
) -> Option<wf_lang::ast::Expr> {
    use wf_lang::ast::Expr;
    Some(Expr::Match {
        expr: Box::new(materialize_system_vars(expr, score)?),
        arms: arms
            .iter()
            .map(|arm| {
                Some(wf_lang::ast::MatchArm {
                    patterns: arm
                        .patterns
                        .iter()
                        .map(|p| materialize_system_vars(p, score))
                        .collect::<Option<Vec<_>>>()?,
                    value: materialize_system_vars(&arm.value, score)?,
                })
            })
            .collect::<Option<Vec<_>>>()?,
        default: default
            .as_ref()
            .and_then(|d| materialize_system_vars(d, score))
            .map(Box::new),
    })
}

#[path = "builtins_handlers.rs"]
mod handlers;

type BuiltinHandler = fn(&str, &[wf_lang::ast::Expr], &dyn FieldSource, YieldMeta) -> Option<Value>;

fn builtin_handler(name: &str) -> Option<BuiltinHandler> {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    static HANDLERS: OnceLock<HashMap<&'static str, BuiltinHandler>> = OnceLock::new();
    let map = HANDLERS.get_or_init(|| {
        let mut map: HashMap<&'static str, BuiltinHandler> = HashMap::new();
        map.insert("contains", handlers::builtin_contains);
        map.insert("startswith", handlers::builtin_startswith);
        map.insert("endswith", handlers::builtin_endswith);
        map.insert("merge", handlers::builtin_merge);
        map.insert("substr", handlers::builtin_substr);
        map.insert("replace", handlers::builtin_replace);
        map.insert("trim", handlers::builtin_trim);
        map.insert("lower", handlers::builtin_lower);
        map.insert("upper", handlers::builtin_upper);
        map.insert("len", handlers::builtin_len);
        map.insert("mvcount", handlers::builtin_mvcount);
        map.insert("mvjoin", handlers::builtin_mvjoin);
        map.insert("mvindex", handlers::builtin_mvindex);
        map.insert("mvappend", handlers::builtin_mvappend);
        map.insert("split", handlers::builtin_split);
        map.insert("mvdedup", handlers::builtin_mvdedup);
        map.insert("abs", handlers::builtin_abs);
        map.insert("round", handlers::builtin_round);
        map.insert("ceil", handlers::builtin_ceil);
        map.insert("floor", handlers::builtin_floor);
        map.insert("sqrt", handlers::builtin_sqrt);
        map.insert("pow", handlers::builtin_pow);
        map.insert("log", handlers::builtin_log);
        map.insert("exp", handlers::builtin_exp);
        map.insert("clamp", handlers::builtin_clamp);
        map.insert("sign", handlers::builtin_sign);
        map.insert("trunc", handlers::builtin_trunc);
        map.insert("is_finite", handlers::builtin_is_finite);
        map.insert("ltrim", handlers::builtin_ltrim);
        map.insert("rtrim", handlers::builtin_rtrim);
        map.insert("fmt", handlers::builtin_fmt);
        map.insert("concat", handlers::builtin_concat);
        map.insert("join", handlers::builtin_join);
        map.insert("join_by", handlers::builtin_join_by);
        map.insert("indexof", handlers::builtin_indexof);
        map.insert("replace_plain", handlers::builtin_replace_plain);
        map.insert("startswith_any", handlers::builtin_startswith_any);
        map.insert("endswith_any", handlers::builtin_endswith_any);
        map.insert("coalesce", handlers::builtin_coalesce);
        map.insert("isnull", handlers::builtin_isnull);
        map.insert("isnotnull", handlers::builtin_isnotnull);
        map.insert("is_blank", handlers::builtin_is_blank);
        map.insert("null_if_blank", handlers::builtin_null_if_blank);
        map.insert("default_if_blank", handlers::builtin_default_if_blank);
        map.insert("md5", handlers::builtin_md5);
        map.insert("sha1", handlers::builtin_sha1);
        map.insert("sha1_n", handlers::builtin_sha1_n);
        map.insert("sha256", handlers::builtin_sha256);
        map.insert("hex", handlers::builtin_hex);
        map.insert("stable_id", handlers::builtin_stable_id);
        map.insert("mvsort", handlers::builtin_mvsort);
        map.insert("mvreverse", handlers::builtin_mvreverse);
        map.insert("now", handlers::builtin_now);
        map.insert("now_ms", handlers::builtin_now);
        map.insert("now_s", handlers::builtin_now_s);
        map.insert("now_us", handlers::builtin_now_us);
        map.insert("now_ns", handlers::builtin_now_ns);
        map.insert("time_to_s", handlers::builtin_time_to_s);
        map.insert("time_to_ms", handlers::builtin_time_to_s);
        map.insert("strftime", handlers::builtin_strftime);
        map.insert("strptime", handlers::builtin_strptime);
        map.insert("regex_match", handlers::builtin_regex_match);
        map.insert("cidr_match", handlers::builtin_cidr_match);
        map.insert("time_diff", handlers::builtin_time_diff);
        map.insert("time_bucket", handlers::builtin_time_bucket);
        map.insert("bucket_end", handlers::builtin_bucket_end);
        map.insert("external", handlers::builtin_external);
        map
    });
    map.get(name).copied()
}

pub(super) fn eval_builtin_func_with_l3(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    let handler = builtin_handler(name)?;
    handler(name, args, ctx, score)
}

fn eval_merge_arg(
    arg: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta<'_>,
) -> Option<Value> {
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
    ctx: &dyn FieldSource,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let selector = parse_stat_selector(&args[0])?;
    match (name, selector) {
        ("count", StatSelector::WindowEvent(alias)) => {
            let field = format!("_bind_{alias}_count");
            ctx.field_value(&field).as_ref().and_then(number_value)
        }
        ("count", StatSelector::MatchEvent(label) | StatSelector::MatchDistinct(label)) => {
            ctx.field_value(label).as_ref().and_then(number_value)
        }
        ("value", StatSelector::Trigger(label) | StatSelector::Final(label)) => {
            ctx.field_value(label).as_ref().and_then(number_value)
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
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    if args.is_empty() {
        return None;
    }
    let values = l3_series_values(ctx, args.first());
    match name {
        "collect_set" => {
            if args.len() != 1 {
                return None;
            }
            Some(Value::Array(dedup_values(values)))
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
            Some(Value::Number(series_stddev(&values)))
        }
        "percentile" => {
            if args.len() != 2 {
                return None;
            }
            let p = match eval_expr_with_l3(&args[1], ctx, score)? {
                Value::Number(n) => n.clamp(0.0, 100.0) / 100.0,
                _ => return None,
            };
            Some(percentile_value(&values, p))
        }
        _ => None,
    }
}

/// 解析 L3 系列值：bind 字段走 bind 序列，否则按 step 索引展开。
fn l3_series_values(ctx: &dyn FieldSource, first: Option<&wf_lang::ast::Expr>) -> Vec<Value> {
    let step_indices = step_data::resolve_step_indices(ctx, first);
    if let Some((alias, _)) = first.and_then(step_data::extract_bind_field_ref)
        && step_data::get_bind_count(ctx, alias).is_some()
    {
        step_data::flatten_bind_series(ctx, first)
    } else {
        step_data::flatten_step_series(ctx, &step_indices, first)
    }
}

/// collect_set 去重（同原实现等值语义）。
fn dedup_values(values: Vec<Value>) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    for v in values {
        if !out.iter().any(|seen| values_equal(seen, &v)) {
            out.push(v);
        }
    }
    out
}

/// 样本标准差；不足 2 个数值 → 0.0（与原实现一致）。
fn series_stddev(values: &[Value]) -> f64 {
    let nums: Vec<f64> = values
        .iter()
        .filter_map(|v| match v {
            Value::Number(n) => Some(*n),
            _ => None,
        })
        .collect();
    if nums.len() < 2 {
        return 0.0;
    }
    let mean = nums.iter().sum::<f64>() / nums.len() as f64;
    let variance = nums.iter().map(|n| (n - mean).powi(2)).sum::<f64>() / nums.len() as f64;
    variance.sqrt()
}

/// p 分位（最近秩取整）；空序列 → 0.0（与原实现一致）。
fn percentile_value(values: &[Value], p: f64) -> Value {
    let mut nums: Vec<f64> = values
        .iter()
        .filter_map(|v| match v {
            Value::Number(n) => Some(*n),
            _ => None,
        })
        .collect();
    if nums.is_empty() {
        return Value::Number(0.0);
    }
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((nums.len() - 1) as f64 * p).round() as usize;
    Value::Number(nums[idx.min(nums.len() - 1)])
}

pub(super) fn eval_aggregate_func(
    name: &str,
    args: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
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
    ctx: &dyn FieldSource,
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

#[cfg(test)]
mod split_tests {
    use super::*;

    fn nums(values: &[f64]) -> Vec<Value> {
        values.iter().map(|n| Value::Number(*n)).collect()
    }

    #[test]
    fn series_stddev_matches_sample_formula() {
        assert_eq!(series_stddev(&nums(&[])), 0.0);
        assert_eq!(series_stddev(&nums(&[3.0])), 0.0); // 不足 2 个 → 0
        let sd = series_stddev(&nums(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]));
        assert!((sd - 2.0).abs() < 1e-9);
        // 非数值成员被忽略
        let mixed = vec![
            Value::Number(2.0),
            Value::Number(4.0),
            Value::Str("x".into()),
            Value::Number(4.0),
        ];
        let sd = series_stddev(&mixed);
        assert!(sd >= 0.0);
        assert_eq!(sd, series_stddev(&nums(&[2.0, 4.0, 4.0])));
    }

    #[test]
    fn percentile_uses_nearest_rank_and_empty_guard() {
        assert_eq!(percentile_value(&nums(&[]), 0.5), Value::Number(0.0));
        assert_eq!(
            percentile_value(&nums(&[1.0, 2.0, 3.0, 4.0]), 0.5),
            Value::Number(3.0) // idx = round(3 * 0.5) = 2
        );
        assert_eq!(
            percentile_value(&nums(&[10.0, 20.0, 30.0]), 0.0),
            Value::Number(10.0)
        );
        assert_eq!(
            percentile_value(&nums(&[10.0, 20.0, 30.0]), 1.0),
            Value::Number(30.0)
        );
    }

    #[test]
    fn dedup_values_uses_value_equality() {
        let out = dedup_values(vec![
            Value::Number(1.0),
            Value::Str("a".into()),
            Value::Number(1.0),
        ]);
        assert_eq!(out.len(), 2);
        // 近等数值按 epsilon 语义去重
        let near = dedup_values(vec![Value::Number(1.0), Value::Number(1.0 + 1e-16)]);
        assert_eq!(near.len(), 1);
    }
}
