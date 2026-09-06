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
mod walkers;

use walkers::{
    contains_aggregate_func, contains_eval_time_func, contains_l3_func, contains_stat_selector,
    is_aggregate_func, is_eval_time_func, is_l3_func,
};

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
    use wf_lang::ast::Expr;

    let _time_scope = EvalTimeScope::enter();
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::SystemVar(sv) => eval_system_var_value(sv, meta),
        Expr::WfuMeta(field) => meta.resolve_wfu_meta(*field),
        Expr::Field(fr) => eval_field_value_src(ctx, fr),
        Expr::Object(items) => eval_object_literal_with_l3(items, ctx, meta),
        Expr::Array(items) => eval_array_literal_with_l3(items, ctx, meta),
        Expr::Neg(inner) => eval_neg_with_l3(inner, ctx, meta),
        Expr::Not(inner) => eval_not_with_l3(inner, ctx, meta),
        Expr::BinOp { op, left, right } => eval_binop_with_l3(*op, left, right, ctx, meta),
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list_with_l3(expr, list, *negated, ctx, meta),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => eval_if_then_else_with_l3(cond, then_expr, else_expr, ctx, meta),
        Expr::Match {
            expr,
            arms,
            default,
        } => eval_match_expr_with_l3(expr, arms, default.as_deref(), ctx, meta),
        Expr::FuncCall { .. } => eval_func_call_with_l3(expr, ctx, meta),
        _ => None,
    }
}

/// @系统变量 → 值（与行内原实现一致）。
fn eval_system_var_value(sv: &wf_lang::ast::SystemVar, meta: YieldMeta<'_>) -> Option<Value> {
    use wf_lang::ast::SystemVar;
    match *sv {
        SystemVar::Score => meta.score.map(Value::Number),
        SystemVar::EventFirstTime => meta.event_first_time_nanos.map(time_nanos_to_value),
        SystemVar::EventLastTime => meta.event_last_time_nanos.map(time_nanos_to_value),
        SystemVar::EvidenceStartTime => meta.evidence_first_time_nanos.map(time_nanos_to_value),
        SystemVar::EvidenceEndTime => meta.evidence_last_time_nanos.map(time_nanos_to_value),
        SystemVar::WindowStartTime => meta.window_start_time_nanos.map(time_nanos_to_value),
        SystemVar::WindowEndTime => meta.window_end_time_nanos.map(time_nanos_to_value),
        SystemVar::EmitTime => meta.emit_time_nanos.map(time_nanos_to_value),
        SystemVar::FirstMatchTime => meta.first_match_time_nanos.map(time_nanos_to_value),
        _ => None,
    }
}

/// `object { k1 = e1; ... }` 字面量。
fn eval_object_literal_with_l3(
    items: &[wf_lang::ast::ObjectItem],
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    let mut map = EngineHashMap::default();
    for item in items {
        let value = eval_expr_with_l3(&item.value, ctx, score)?;
        for target in &item.targets {
            map.insert(target.clone().into(), value.clone());
        }
    }
    Some(Value::Object(map))
}

/// `array [e1, ...]` 字面量。
fn eval_array_literal_with_l3(
    items: &[wf_lang::ast::Expr],
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    items
        .iter()
        .map(|item| eval_expr_with_l3(item, ctx, score))
        .collect::<Option<Vec<_>>>()
        .map(Value::Array)
}

/// 一元负号：仅数值可取反。
fn eval_neg_with_l3(
    inner: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    match eval_expr_with_l3(inner, ctx, score)? {
        Value::Number(n) => Some(Value::Number(-n)),
        _ => None,
    }
}

/// 逻辑非：仅布尔可取反。
fn eval_not_with_l3(
    inner: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    match eval_expr_with_l3(inner, ctx, score)? {
        Value::Bool(b) => Some(Value::Bool(!b)),
        _ => None,
    }
}

/// 二元运算分派：逻辑与/或、比较、算术。
fn eval_binop_with_l3(
    op: wf_lang::ast::BinOp,
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    use wf_lang::ast::BinOp;
    match op {
        BinOp::And => eval_logic_and_with_l3(left, right, ctx, score),
        BinOp::Or => eval_logic_or_with_l3(left, right, ctx, score),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            eval_compare_binop(op, left, right, ctx, score)
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            eval_arith_binop(op, left, right, ctx, score)
        }
        _ => None,
    }
}

/// 比较类二元运算。
fn eval_compare_binop(
    op: wf_lang::ast::BinOp,
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    let lv = eval_expr_with_l3(left, ctx, score)?;
    let rv = eval_expr_with_l3(right, ctx, score)?;
    Some(Value::Bool(compare_values(op, &lv, &rv)))
}

/// 算术类二元运算（除/模零 → None）。
fn eval_arith_binop(
    op: wf_lang::ast::BinOp,
    left: &wf_lang::ast::Expr,
    right: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    use wf_lang::ast::BinOp;
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

/// `expr in (...)` / `expr not in (...)`。
fn eval_in_list_with_l3(
    target: &wf_lang::ast::Expr,
    list: &[wf_lang::ast::Expr],
    negated: bool,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    let target_val = eval_expr_with_l3(target, ctx, score)?;
    let found = list.iter().any(|item| {
        eval_expr_with_l3(item, ctx, score)
            .map(|v| values_equal(&target_val, &v))
            .unwrap_or(false)
    });
    Some(Value::Bool(if negated { !found } else { found }))
}

/// `if cond then yes else no`。
fn eval_if_then_else_with_l3(
    cond: &wf_lang::ast::Expr,
    then_expr: &wf_lang::ast::Expr,
    else_expr: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    match eval_expr_with_l3(cond, ctx, score) {
        Some(Value::Bool(true)) => eval_expr_with_l3(then_expr, ctx, score),
        Some(Value::Bool(false)) => eval_expr_with_l3(else_expr, ctx, score),
        _ => None,
    }
}

/// `match <subject> { pat => arm, ..., _ => default }`（短路）。
fn eval_match_expr_with_l3(
    expr: &wf_lang::ast::Expr,
    arms: &[wf_lang::ast::MatchArm],
    default: Option<&wf_lang::ast::Expr>,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
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

/// FuncCall 路由：stat 选择器 / 聚合 / L3 / 时间 / external 与降级路径
/// （原 `eval_expr_with_l3` 的 FuncCall 臂，逻辑照搬）。
fn eval_func_call_with_l3(
    expr: &wf_lang::ast::Expr,
    ctx: &dyn FieldSource,
    score: YieldMeta,
) -> Option<Value> {
    use wf_lang::ast::Expr;
    let Expr::FuncCall {
        qualifier,
        name,
        args,
    } = expr
    else {
        return None;
    };
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
