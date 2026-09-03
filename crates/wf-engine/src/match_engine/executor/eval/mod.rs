//! 列式表达式后端：在 Arrow 批上求值内建函数（`builtins*` / `coverage_*`），
//! 供列式执行路径使用；逐事件标量等价物在 `cep/eval/`。
//! 两套实现语义一致由 `match_engine/tests/` 对拍守护（详见 `cep/eval` 模块文档）。

use std::cell::Cell;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::cep::{
    EngineHashMap, FieldSource, Value, WindowLookup, eval_expr, eval_expr_ext,
    eval_field_value_src, value_to_string, values_equal,
};

#[cfg(test)]
pub(super) use crate::match_engine::cep::Event;

mod builtins;
#[cfg(test)]
mod builtins_more;
#[cfg(test)]
mod builtins_r4;
#[cfg(test)]
mod coverage_extra;
#[cfg(test)]
mod coverage_m2;
#[cfg(test)]
mod coverage_more;
#[cfg(test)]
mod coverage_r4;
mod step_data;
#[cfg(test)]
mod tests;
mod utils;

use self::builtins::{
    contains_system_var, eval_aggregate_func, eval_builtin_func_with_l3, eval_l3_func,
    eval_stat_func, is_stat_selector_func, materialize_system_vars,
};
use self::utils::time_nanos_to_value;

/// Evaluate a yield/derive expression with L3 function support.
///
/// L3 functions (collect_set, collect_list, first, last, stddev, percentile)
/// need access to the collected values from step execution. These values are
/// stored in `_step_{i}_values` and `_step_{i}_source` fields in the eval context.
pub(super) fn eval_yield_expr(expr: &wf_lang::ast::Expr, ctx: &dyn FieldSource) -> Option<Value> {
    eval_yield_expr_with_score(expr, ctx, None)
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct YieldMeta<'a> {
    pub(super) score: Option<f64>,
    pub(super) wfx_id: Option<&'a str>,
    pub(super) rule_name: Option<&'a str>,
    pub(super) entity_type: Option<&'a str>,
    pub(super) entity_id: Option<&'a str>,
    pub(super) origin: Option<&'a str>,
    pub(super) close_reason: Option<&'a str>,
    pub(super) fired_at: Option<&'a str>,
    pub(super) emit_time: Option<&'a str>,
    pub(super) summary: Option<&'a str>,
    pub(super) event_first_time_nanos: Option<i64>,
    pub(super) event_last_time_nanos: Option<i64>,
    pub(super) evidence_first_time_nanos: Option<i64>,
    pub(super) evidence_last_time_nanos: Option<i64>,
    pub(super) window_start_time_nanos: Option<i64>,
    pub(super) window_end_time_nanos: Option<i64>,
    pub(super) emit_time_nanos: Option<i64>,
    pub(super) first_match_time_nanos: Option<i64>,
    pub(super) time_format: Option<&'a str>,
}

impl YieldMeta<'_> {
    fn resolve_wfu_meta(self, field: wf_lang::wfu_meta::WfuMetaField) -> Option<Value> {
        use wf_lang::wfu_meta::WfuMetaField;

        match field {
            WfuMetaField::Id => self.wfx_id.map(|value| Value::Str(value.into())),
            WfuMetaField::RuleName => self.rule_name.map(|value| Value::Str(value.into())),
            WfuMetaField::Score => self.score.map(Value::Number),
            WfuMetaField::EntityType => self.entity_type.map(|value| Value::Str(value.into())),
            WfuMetaField::EntityId => self.entity_id.map(|value| Value::Str(value.into())),
            WfuMetaField::Origin => self.origin.map(|value| Value::Str(value.into())),
            WfuMetaField::CloseReason => self.close_reason.map(|value| Value::Str(value.into())),
            WfuMetaField::FiredAt => self.fired_at.map(|value| Value::Str(value.into())),
            WfuMetaField::EmitTime => self.emit_time.map(|value| Value::Str(value.into())),
            WfuMetaField::Summary => self.summary.map(|value| Value::Str(value.into())),
        }
    }
}

thread_local! {
    static EVAL_TIME_NANOS: Cell<Option<i64>> = const { Cell::new(None) };
    static EVAL_TIME_SCOPE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn get_or_init_eval_time_nanos() -> Option<i64> {
    EVAL_TIME_NANOS.with(|time| {
        if let Some(nanos) = time.get() {
            return Some(nanos);
        }
        let nanos = chrono::Utc::now().timestamp_nanos_opt()?;
        time.set(Some(nanos));
        Some(nanos)
    })
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
    ctx: &dyn FieldSource,
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
    ctx: &dyn FieldSource,
    meta: YieldMeta,
) -> Option<Value> {
    // For yield expressions, fall back to empty string when a field is missing
    // (e.g., join window fields not available in test runner)
    with_yield_eval_scope(|| match eval_expr_with_l3(expr, ctx, meta) {
        None => Some(Value::Str(String::new().into())),
        val => val,
    })
}

pub(super) fn eval_bool_expr(expr: &wf_lang::ast::Expr, ctx: &dyn FieldSource) -> Option<bool> {
    match eval_expr_with_l3(expr, ctx, YieldMeta::default()) {
        Some(Value::Bool(result)) => Some(result),
        _ => None,
    }
}

pub(super) fn eval_bool_expr_with_lookup(
    expr: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
) -> Option<bool> {
    let mut baselines = EngineHashMap::default();
    match eval_expr_ext(expr, ctx, windows, &mut baselines) {
        Some(Value::Bool(result)) => Some(result),
        _ => None,
    }
}

pub(super) fn eval_expr_with_l3(
    expr: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    meta: YieldMeta<'_>,
) -> Option<Value> {
    use wf_lang::ast::{BinOp, Expr, SystemVar};

    let _time_scope = EvalTimeScope::enter();
    let score = meta;
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::SystemVar(SystemVar::Score) => meta.score.map(Value::Number),
        Expr::SystemVar(SystemVar::EventFirstTime) => {
            meta.event_first_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::EventLastTime) => {
            meta.event_last_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::EvidenceStartTime) => {
            meta.evidence_first_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::EvidenceEndTime) => {
            meta.evidence_last_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::WindowStartTime) => {
            meta.window_start_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::WindowEndTime) => {
            meta.window_end_time_nanos.map(time_nanos_to_value)
        }
        Expr::SystemVar(SystemVar::EmitTime) => meta.emit_time_nanos.map(time_nanos_to_value),
        Expr::SystemVar(SystemVar::FirstMatchTime) => {
            meta.first_match_time_nanos.map(time_nanos_to_value)
        }
        Expr::WfuMeta(field) => meta.resolve_wfu_meta(*field),
        Expr::Field(fr) => eval_field_value_src(ctx, fr),
        Expr::Object(items) => {
            let mut map = EngineHashMap::default();
            for item in items {
                let value = eval_expr_with_l3(&item.value, ctx, score)?;
                for target in &item.targets {
                    map.insert(target.clone().into(), value.clone());
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
        Expr::Not(inner) => match eval_expr_with_l3(inner, ctx, score)? {
            Value::Bool(b) => Some(Value::Bool(!b)),
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
        // 模式匹配（issue #79 Issue 2）：subject 求值后逐分支比较（`in` 同款
        // values_equal），命中短路返回；`_` 默认兜底；无默认且未命中 → None。
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            let subject = eval_expr_with_l3(expr, ctx, score)?;
            for arm in arms {
                let hit = arm.patterns.iter().any(|pattern| {
                    eval_expr_with_l3(pattern, ctx, score)
                        .map(|v| values_equal(&subject, &v))
                        .unwrap_or(false)
                });
                if hit {
                    return eval_expr_with_l3(&arm.value, ctx, score);
                }
            }
            match default {
                Some(d) => eval_expr_with_l3(d, ctx, score),
                None => None,
            }
        }
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            if qualifier.as_deref() == Some("stat") && matches!(name.as_str(), "count" | "value") {
                return eval_stat_func(name, args, ctx);
            }
            if qualifier.is_none() && is_stat_selector_func(name) {
                return None;
            }
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
                || name == "strftime"
                || is_eval_time_func(name)
                || args.iter().any(contains_l3_func)
                || args.iter().any(contains_aggregate_func)
                || args.iter().any(contains_eval_time_func)
                || args.iter().any(contains_stat_selector)
            {
                // `external()` is implemented only in `eval_builtin_func_with_l3`
                // (it dispatches to the global ExternalCallHandler / wp_knowledge
                // facade). Route it here even when its args are plain literals /
                // fields, otherwise `on each where external(...)` filters silently
                // evaluate to None and never query the backend.
                // 含 stat.* 参数时同样必须走 L3 路径：fmt("{}", stat.value(final(x)))
                // 等包装函数若回退到 match_engine 的 eval（无 stat 支持），stat 求值
                // 为 None 使整个表达式返回 None（q15/q16/q17 统计输出为空）。
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
    ctx: &dyn FieldSource,
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
    ctx: &dyn FieldSource,
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
        Expr::Not(inner) => contains_l3_func(inner),
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
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            contains_l3_func(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(contains_l3_func) || contains_l3_func(&arm.value)
                })
                || default.as_ref().is_some_and(|d| contains_l3_func(d))
        }
        _ => false,
    }
}

fn is_eval_time_func(name: &str) -> bool {
    // now 系列 + 时间转换（time_to_s/time_to_ms，issue #69）都需要 L3 时间
    // 工具（current_time_nanos / normalize_epoch_timestamp_float_nanos）。
    matches!(
        name,
        "now" | "now_s" | "now_ms" | "now_us" | "now_ns" | "time_to_s" | "time_to_ms"
    )
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
        Expr::Not(inner) => contains_eval_time_func(inner),
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
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            contains_eval_time_func(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(contains_eval_time_func)
                        || contains_eval_time_func(&arm.value)
                })
                || default.as_ref().is_some_and(|d| contains_eval_time_func(d))
        }
        _ => false,
    }
}

/// Whether `expr` contains a `stat.count/stat.value` selector call.
///
/// Wrapper functions like `fmt("{}", stat.value(final(x)))` must stay on the
/// L3 eval path when any argument references stat selectors — the plain
/// match-engine eval has no stat support and would evaluate them to `None`,
/// making the whole expression `None` (q15/q16/q17 close-path stats output).
fn contains_stat_selector(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::FuncCall {
            qualifier, args, ..
        } => qualifier.as_deref() == Some("stat") || args.iter().any(contains_stat_selector),
        Expr::BinOp { left, right, .. } => {
            contains_stat_selector(left) || contains_stat_selector(right)
        }
        Expr::Neg(inner) => contains_stat_selector(inner),
        Expr::Not(inner) => contains_stat_selector(inner),
        Expr::Object(items) => items.iter().any(|item| contains_stat_selector(&item.value)),
        Expr::Array(items) => items.iter().any(contains_stat_selector),
        Expr::InList { expr, list, .. } => {
            contains_stat_selector(expr) || list.iter().any(contains_stat_selector)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            contains_stat_selector(cond)
                || contains_stat_selector(then_expr)
                || contains_stat_selector(else_expr)
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            contains_stat_selector(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(contains_stat_selector)
                        || contains_stat_selector(&arm.value)
                })
                || default.as_ref().is_some_and(|d| contains_stat_selector(d))
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
        Expr::Not(inner) => contains_aggregate_func(inner),
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
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            contains_aggregate_func(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(contains_aggregate_func)
                        || contains_aggregate_func(&arm.value)
                })
                || default.as_ref().is_some_and(|d| contains_aggregate_func(d))
        }
        _ => false,
    }
}

/// Evaluate the score expression and clamp to `[0, 100]`.
///
pub(super) fn eval_score(expr: &wf_lang::ast::Expr, ctx: &dyn FieldSource) -> CoreResult<f64> {
    let val = eval_yield_expr(expr, ctx);
    let raw = match val {
        Some(Value::Number(n)) => n,
        Some(other) => {
            return orion_error::prelude::StructError::from(CoreReason::RuleExec)
                .with_detail(format!(
                    "score expression evaluated to non-numeric value: {:?}",
                    other
                ))
                .err();
        }
        None => {
            return orion_error::prelude::StructError::from(CoreReason::RuleExec)
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
pub(super) fn eval_entity_id(
    expr: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
) -> CoreResult<String> {
    let val = eval_yield_expr(expr, ctx);
    match val {
        Some(v) => Ok(value_to_string(&v)),
        None => orion_error::prelude::StructError::from(CoreReason::RuleExec)
            .with_detail("entity_id expression evaluated to None")
            .err(),
    }
}
