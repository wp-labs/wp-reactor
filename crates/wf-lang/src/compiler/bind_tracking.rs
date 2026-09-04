//! 绑定跟踪与字段历史/触发器需求分析（compiler/mod.rs 拆件，2026-09-04）：
//! `BindTracking`/`collect_*` 决定 match/close 求值上下文须保留的绑定别名与字段；
//! `compute_needs_field_history`/`compute_trigger_event_needed` 决定是否保留每事件
//! 字段历史与触发事件（引擎热路径开关，见 q12 / F3 / nexmark 注释）。

use super::*;

#[derive(Default)]
pub(crate) struct BindTracking {
    pub aliases: HashSet<String>,
    pub fields: std::collections::HashMap<String, HashSet<String>>,
    pub plain_fields: HashSet<String>,
}

#[cfg(test)]
pub(crate) fn collect_rule_bind_tracking_aliases(
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> HashSet<String> {
    collect_rule_bind_tracking(score_expr, entity_expr, yield_fields).aliases
}

pub(crate) fn collect_rule_bind_tracking(
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> BindTracking {
    let mut tracking = BindTracking::default();
    collect_bind_tracking(score_expr, &mut tracking);
    collect_bind_tracking(entity_expr, &mut tracking);
    for field in yield_fields {
        collect_bind_tracking(&field.value, &mut tracking);
    }
    tracking
}

#[cfg(test)]
pub(crate) fn collect_bind_tracking_aliases(expr: &Expr, aliases: &mut HashSet<String>) {
    let mut tracking = BindTracking {
        aliases: std::mem::take(aliases),
        fields: std::collections::HashMap::new(),
        plain_fields: HashSet::new(),
    };
    collect_bind_tracking(expr, &mut tracking);
    *aliases = tracking.aliases;
}

pub(crate) fn collect_bind_tracking(expr: &Expr, tracking: &mut BindTracking) {
    match expr {
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            if qualifier.as_deref() == Some("stat") {
                collect_stat_bind_tracking(name, args, tracking);
                return;
            }
            if qualifier.is_none() && is_stat_selector(name) {
                return;
            }
            if qualifier.is_none()
                && is_series_func(name)
                && let Some(Expr::Field(
                    FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field),
                )) = args.first()
            {
                track_bind_field(tracking, alias, field);
            }
            for arg in args {
                collect_bind_tracking(arg, tracking);
            }
        }
        Expr::BinOp { left, right, .. } => {
            collect_bind_tracking(left, tracking);
            collect_bind_tracking(right, tracking);
        }
        Expr::Neg(inner) => collect_bind_tracking(inner, tracking),
        Expr::Not(inner) => collect_bind_tracking(inner, tracking),
        Expr::InList { expr, list, .. } => {
            collect_bind_tracking(expr, tracking);
            for item in list {
                collect_bind_tracking(item, tracking);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_bind_tracking(cond, tracking);
            collect_bind_tracking(then_expr, tracking);
            collect_bind_tracking(else_expr, tracking);
        }
        Expr::Field(FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field)) => {
            track_bind_field(tracking, alias, field);
        }
        Expr::Field(FieldRef::Path { alias, segments }) => {
            // Track the root object/array field so it reaches the match/close
            // yield context; nested traversal happens at evaluation time.
            if let Some(PathSegment::Field(root)) = segments.first() {
                track_bind_field(tracking, alias, root);
            }
        }
        Expr::Field(FieldRef::Simple(field)) => {
            tracking.plain_fields.insert(field.clone());
        }
        Expr::Object(items) => {
            // Structured yields recurse so that field references inside
            // `object { ... }` members reach the match/close eval context too.
            for item in items {
                collect_bind_tracking(&item.value, tracking);
            }
        }
        Expr::Array(items) => {
            for item in items {
                collect_bind_tracking(item, tracking);
            }
        }
        _ => {}
    }
}

fn collect_stat_bind_tracking(name: &str, args: &[Expr], tracking: &mut BindTracking) {
    if name != "count" || args.len() != 1 {
        return;
    }
    let Expr::FuncCall {
        qualifier: None,
        name: selector,
        args: selector_args,
    } = &args[0]
    else {
        return;
    };
    if selector != "window_event" || selector_args.len() != 1 {
        return;
    }
    if let Expr::Field(FieldRef::Simple(alias)) = &selector_args[0] {
        tracking.aliases.insert(alias.clone());
    }
}

fn is_stat_selector(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger" | "final"
    )
}

fn track_bind_field(tracking: &mut BindTracking, alias: &str, field: &str) {
    tracking.aliases.insert(alias.to_string());
    tracking
        .fields
        .entry(alias.to_string())
        .or_default()
        .insert(field.to_string());
}

fn is_series_func(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect_set"
            | "collect_list"
            | "first"
            | "last"
            | "stddev"
            | "percentile"
    )
}

/// L3 series functions that read the `_step_field` array of collected values —
/// they require the per-field value *history*, not just the triggering event.
fn is_l3_series_func(name: &str) -> bool {
    matches!(
        name,
        "collect_set" | "collect_list" | "first" | "last" | "stddev" | "percentile"
    )
}

/// Event-accessor stat functions (`match_event`, `window_event`, `trigger`, …)
/// that read the step's collected event data — also require the history.
///
/// `final(label)` is deliberately NOT here: the eval side resolves it to the
/// close-accumulated measure value injected by `build_eval_context` (`ctx.fields
/// .get(label)`), never the per-event field history. Including it forces
/// empty-key close rules (q15: 12 measures × `stat.value(final(...))`) to keep
/// the per-event `collect_event_fields` on for nothing.
fn is_event_accessor(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger"
    )
}

/// Whether an expression references an L3 series or event-accessor function
/// anywhere (recursive) — either way the per-field value *history* is needed.
fn expr_uses_l3_series(e: &Expr) -> bool {
    match e {
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::Field(_)
        | Expr::PresetParam(_)
        | Expr::ListRef(_) => false,
        Expr::BinOp { left, right, .. } => expr_uses_l3_series(left) || expr_uses_l3_series(right),
        Expr::Neg(inner) => expr_uses_l3_series(inner),
        Expr::Not(inner) => expr_uses_l3_series(inner),
        Expr::FuncCall { name, args, .. } => {
            is_l3_series_func(name)
                || is_event_accessor(name)
                || args.iter().any(expr_uses_l3_series)
        }
        Expr::Object(items) => items.iter().any(|i| expr_uses_l3_series(&i.value)),
        Expr::Array(items) => items.iter().any(expr_uses_l3_series),
        Expr::InList { expr, list, .. } => {
            expr_uses_l3_series(expr) || list.iter().any(expr_uses_l3_series)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            expr_uses_l3_series(cond)
                || expr_uses_l3_series(then_expr)
                || expr_uses_l3_series(else_expr)
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            expr_uses_l3_series(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(expr_uses_l3_series) || expr_uses_l3_series(&arm.value)
                })
                || default.as_ref().is_some_and(|d| expr_uses_l3_series(d))
        }
    }
}

/// Whether the rule needs the per-field value *history* (`field_values`).
///
/// A single-bind on-event rule whose yield reads scalar fields can instead read
/// them from the triggering event (passed through `MatchedContext`), so it needs
/// no history and `collect_alias_event` can be skipped. Everything else — close
/// steps (fire with no event), multi-bind (yield may read a non-trigger alias),
/// joins, or L3 series in yield/score/entity — keeps collecting the history.
pub(crate) fn compute_needs_field_history(
    match_plan: &MatchPlan,
    binds: &[BindPlan],
    joins: &[JoinPlan],
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> bool {
    if !match_plan.close_steps.is_empty() {
        // Close steps accumulate per event, but the field *history* (the
        // per-event `field_values` collection feeding close-time `Field`
        // resolution) is only needed when the close-path outputs reference a
        // field that resolves outside the match keys — `build_eval_context`
        // serves keys from `scope_key` with precedence over the history.
        // q12-style count rules whose yields read only the key (or literals /
        // system vars) skip the per-event `collect_alias_event` entirely.
        return close_path_reads_non_key_fields(match_plan, score_expr, entity_expr, yield_fields);
    }
    if binds.len() > 1 || !joins.is_empty() {
        return true;
    }
    if expr_uses_l3_series(score_expr)
        || expr_uses_l3_series(entity_expr)
        || yield_fields.iter().any(|f| expr_uses_l3_series(&f.value))
    {
        return true;
    }
    false
}

/// Close path needs the per-event field history iff the score/entity/yield
/// expressions read a field that is not one of the match keys (which the eval
/// ctx serves from `scope_key`) or call a function that consumes collected
/// values / alias counts (L3 series, `stat.*`, event accessors).
fn close_path_reads_non_key_fields(
    match_plan: &MatchPlan,
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> bool {
    if expr_uses_l3_series(score_expr)
        || expr_uses_l3_series(entity_expr)
        || yield_fields.iter().any(|f| expr_uses_l3_series(&f.value))
    {
        return true;
    }
    let key_names: HashSet<&str> = match_plan
        .keys
        .iter()
        .map(crate::field_usage::field_ref_name)
        .collect();
    let mut refs = HashSet::new();
    crate::field_usage::collect_expr_fields(score_expr, &mut refs);
    crate::field_usage::collect_expr_fields(entity_expr, &mut refs);
    for f in yield_fields {
        crate::field_usage::collect_expr_fields(&f.value, &mut refs);
    }
    refs.into_iter()
        .any(|name| name.is_empty() || name.starts_with('_') || !key_names.contains(name.as_str()))
}

/// Whether on-event fires need the triggering event materialized
/// (`MatchedContext.trigger_event`).
///
/// `build_eval_context` serves match keys from `scope_key` (with precedence over
/// the history / trigger event), so an on-event fire needs the trigger event
/// only when score/entity/yield, a join condition **left field**
/// (`first_join_key` reads it from the ctx — missing ⇒ join miss ⇒ all skips,
/// the F3 lesson), or the post-join `where` reads a field that is not one of
/// the match keys. When false, the fire path skips `event.to_event()` — a
/// per-event full HashMap clone on every-event-fire rules (Q5/Q7/Q12/Q13,
/// 2026-08 nexmark hotpath bench: match+join emit 1690 → 1553 ns/evt after the
/// F3 ctx-narrowing; the clone remains and is this field's target).
pub(super) fn compute_trigger_event_needed(
    match_plan: &MatchPlan,
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
    joins: &[JoinPlan],
    r#where: Option<&Expr>,
    lets: &[LetDecl],
) -> bool {
    let key_names: HashSet<&str> = match_plan
        .keys
        .iter()
        .map(crate::field_usage::field_ref_name)
        .collect();
    let mut refs = HashSet::new();
    crate::field_usage::collect_expr_fields(score_expr, &mut refs);
    crate::field_usage::collect_expr_fields(entity_expr, &mut refs);
    for f in yield_fields {
        crate::field_usage::collect_expr_fields(&f.value, &mut refs);
    }
    // let 派生字段（2026-08-31，issue #79）：let RHS 引用的非键字段必须保留
    // trigger_event——否则 rule_task 丢弃事件，match 路径 apply_lets 读不到。
    for l in lets {
        crate::field_usage::collect_expr_fields(&l.expr, &mut refs);
    }
    for join in joins {
        for cond in &join.conds {
            crate::field_usage::collect_expr_fields(&Expr::Field(cond.left.clone()), &mut refs);
        }
    }
    if let Some(w) = r#where {
        crate::field_usage::collect_expr_fields(w, &mut refs);
    }
    refs.into_iter()
        .any(|name| name.is_empty() || name.starts_with('_') || !key_names.contains(name.as_str()))
}
