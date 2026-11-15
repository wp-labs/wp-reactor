//! 绑定跟踪与字段历史/触发器需求分析（compiler/mod.rs 拆件，2026-09-04）：
//! `BindTracking`/`collect_*` 决定 match/close 求值上下文须保留的绑定别名与字段；
//! `compute_needs_field_history`/`compute_trigger_event_needed` 决定是否保留每事件
//! 字段历史与触发事件（引擎热路径开关，见 q12 / F3 / nexmark 注释）。

use super::*;

use crate::plan::FieldMode;

/// 字段历史用法位：决定 [`FieldMode`]（合并规则见 [`BindTracking::field_modes`]）。
const USAGE_FIRST: u8 = 1;
const USAGE_LAST: u8 = 2;
const USAGE_SERIES: u8 = 4;

#[derive(Default)]
pub(crate) struct BindTracking {
    pub aliases: HashSet<String>,
    pub fields: std::collections::HashMap<String, HashSet<String>>,
    pub plain_fields: HashSet<String>,
    /// alias → field → usage bits（内部；对外经 [`Self::field_modes`] 投影）。
    usage: std::collections::HashMap<String, std::collections::HashMap<String, u8>>,
}

impl BindTracking {
    /// 把按用法累积的位投影为每个字段的存储模式。
    ///
    /// 合并规则（宁可多存，不可丢语义）：
    /// - 任何 `Series` 用法 → `Series`（`collect_*` / `stddev` / `percentile` /
    ///   对字段求值的 `min/max/sum/avg/count` 都需完整序列）；
    /// - 同时被 `first()` 与普通读取/`last()` 引用 → `Series`；
    /// - 仅 `first()` → `First`（只写首个值）；
    /// - 其余（普通投影 / `last()`）→ `Last`（只保留最新值）。
    pub(crate) fn field_modes(
        &self,
    ) -> std::collections::HashMap<String, std::collections::HashMap<String, FieldMode>> {
        self.usage
            .iter()
            .map(|(alias, fields)| {
                let modes = fields
                    .iter()
                    .map(|(field, bits)| {
                        let mode = if *bits & USAGE_SERIES != 0 {
                            FieldMode::Series
                        } else if *bits & USAGE_FIRST != 0 && *bits & USAGE_LAST != 0 {
                            FieldMode::Series
                        } else if *bits & USAGE_FIRST != 0 {
                            FieldMode::First
                        } else {
                            FieldMode::Last
                        };
                        (field.clone(), mode)
                    })
                    .collect();
                (alias.clone(), modes)
            })
            .collect()
    }

    fn mark_field(&mut self, alias: &str, field: &str, usage: u8) {
        let entry = self
            .usage
            .entry(alias.to_string())
            .or_default()
            .entry(field.to_string())
            .or_insert(0);
        *entry |= usage;
    }
}

#[cfg(test)]
pub(crate) fn collect_rule_bind_tracking_aliases(
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
    lets: &[LetDecl],
) -> HashSet<String> {
    collect_rule_bind_tracking(score_expr, entity_expr, yield_fields, lets).aliases
}

/// 收集规则级表达式（score/entity/yield **以及 let 绑定**）对绑定别名与字段的
/// 引用。let 必须纳入：L3 序列函数（`first`/`last`/`collect_*`）可能只写在
/// `let` 中、由 yield 间接引用（warp-fusion#99）——遗漏会使引擎不物化对应
/// 事件历史，let 求值得到空值。
pub(crate) fn collect_rule_bind_tracking(
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
    lets: &[LetDecl],
) -> BindTracking {
    let mut tracking = BindTracking::default();
    collect_bind_tracking(score_expr, &mut tracking);
    collect_bind_tracking(entity_expr, &mut tracking);
    for field in yield_fields {
        collect_bind_tracking(&field.value, &mut tracking);
    }
    for l in lets {
        collect_bind_tracking(&l.expr, &mut tracking);
    }
    tracking
}

#[cfg(test)]
pub(crate) fn collect_bind_tracking_aliases(expr: &Expr, aliases: &mut HashSet<String>) {
    let mut tracking = BindTracking {
        aliases: std::mem::take(aliases),
        fields: std::collections::HashMap::new(),
        plain_fields: HashSet::new(),
        usage: std::collections::HashMap::new(),
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
            // 字段用法分类（内存）：决定该字段在实例里存整条序列 / 只存首值 /
            // 只存最后值（见 `plan::FieldMode`）。`first`/`last` 必须先于
            // `is_series_func` 判断——后者包含它们。
            let usage = if qualifier.is_none() && name == "first" {
                Some(USAGE_FIRST)
            } else if qualifier.is_none() && name == "last" {
                Some(USAGE_LAST)
            } else if qualifier.is_none() && is_series_func(name) {
                // collect_* / stddev / percentile / 对字段求值的 min·max·sum·avg·count
                // 都会读完整序列（eval 侧 flatten_bind_series）。
                Some(USAGE_SERIES)
            } else {
                None
            };
            let mut first_arg_classified = false;
            if let Some(usage) = usage {
                match args.first() {
                    Some(Expr::Field(
                        FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field),
                    )) => {
                        track_bind_field(tracking, alias, field);
                        tracking.mark_field(alias, field, usage);
                        first_arg_classified = true;
                    }
                    // 嵌套路径（`first(s.meta.body)`）：按根字段归类，否则会被
                    // 递归当成普通读取记为 Last（first 语义丢失）。
                    Some(Expr::Field(FieldRef::Path { alias, segments })) => {
                        if let Some(PathSegment::Field(root)) = segments.first() {
                            track_bind_field(tracking, alias, root);
                            tracking.mark_field(alias, root, usage);
                            first_arg_classified = true;
                        }
                    }
                    _ => {}
                }
            }
            for (idx, arg) in args.iter().enumerate() {
                if idx == 0 && first_arg_classified {
                    // 已按函数语义标记，不能再按普通读取记为 Last（否则
                    // `first(x)` 会退化成 First+Last → Series）。
                    continue;
                }
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
            tracking.mark_field(alias, field, USAGE_LAST);
        }
        Expr::Field(FieldRef::Path { alias, segments }) => {
            // Track the root object/array field so it reaches the match/close
            // yield context; nested traversal happens at evaluation time.
            if let Some(PathSegment::Field(root)) = segments.first() {
                track_bind_field(tracking, alias, root);
                tracking.mark_field(alias, root, USAGE_LAST);
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
    lets: &[LetDecl],
) -> bool {
    if !match_plan.close_steps.is_empty() {
        // Close steps accumulate per event, but the field *history* (the
        // per-event `field_values` collection feeding close-time `Field`
        // resolution) is only needed when the close-path outputs reference a
        // field that resolves outside the match keys — `build_eval_context`
        // serves keys from `scope_key` with precedence over the history.
        // q12-style count rules whose yields read only the key (or literals /
        // system vars) skip the per-event `collect_alias_event` entirely.
        return close_path_reads_non_key_fields(
            match_plan,
            score_expr,
            entity_expr,
            yield_fields,
            lets,
        );
    }
    if binds.len() > 1 || !joins.is_empty() {
        return true;
    }
    if expr_uses_l3_series(score_expr)
        || expr_uses_l3_series(entity_expr)
        || yield_fields.iter().any(|f| expr_uses_l3_series(&f.value))
        || lets.iter().any(|l| expr_uses_l3_series(&l.expr))
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
    lets: &[LetDecl],
) -> bool {
    if expr_uses_l3_series(score_expr)
        || expr_uses_l3_series(entity_expr)
        || yield_fields.iter().any(|f| expr_uses_l3_series(&f.value))
        || lets.iter().any(|l| expr_uses_l3_series(&l.expr))
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
    // let 绑定引用的字段同样算入（issue #99）：L3/非键字段可能只出现在 let 中。
    for l in lets {
        crate::field_usage::collect_expr_fields(&l.expr, &mut refs);
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

#[cfg(test)]
mod field_mode_tests {
    use super::*;
    use crate::plan::FieldMode;

    fn field(alias: &str, name: &str) -> Expr {
        Expr::Field(FieldRef::Qualified(alias.to_string(), name.to_string()))
    }

    fn call(name: &str, arg: Expr) -> Expr {
        Expr::FuncCall {
            qualifier: None,
            name: name.to_string(),
            args: vec![arg],
        }
    }

    fn modes(expr: &Expr) -> std::collections::HashMap<String, FieldMode> {
        let mut t = BindTracking::default();
        collect_bind_tracking(expr, &mut t);
        t.field_modes().remove("a").unwrap_or_default()
    }

    /// `first(x)` 只要首值 → 单槽（2026-11 内存优化的核心判定）。
    #[test]
    fn first_only_maps_to_single_slot() {
        let m = modes(&call("first", field("a", "body")));
        assert_eq!(m.get("body"), Some(&FieldMode::First));
    }

    /// 普通投影只读最后值 → 单槽覆盖。
    #[test]
    fn plain_read_maps_to_last() {
        let m = modes(&field("a", "url"));
        assert_eq!(m.get("url"), Some(&FieldMode::Last));
    }

    /// `collect_list` / `stddev` 等需要完整序列。
    #[test]
    fn collect_list_keeps_series() {
        let m = modes(&call("collect_list", field("a", "url")));
        assert_eq!(m.get("url"), Some(&FieldMode::Series));
    }

    /// 对字段求值的聚合（eval 读 bind 序列）同样需要完整序列。
    #[test]
    fn aggregate_over_field_keeps_series() {
        for name in ["max", "min", "sum", "avg", "count"] {
            let m = modes(&call(name, field("a", "v")));
            assert_eq!(m.get("v"), Some(&FieldMode::Series), "aggregate {name}");
        }
    }

    /// `first()` 与普通读取同时出现 → 两端都要，退化为完整序列。
    #[test]
    fn first_plus_plain_degrades_to_series() {
        let m = modes(&Expr::Array(vec![
            call("first", field("a", "x")),
            field("a", "x"),
        ]));
        assert_eq!(m.get("x"), Some(&FieldMode::Series));
    }

    /// `last()` 只要最新值 → 单槽覆盖。
    #[test]
    fn last_only_maps_to_single_slot() {
        let m = modes(&call("last", field("a", "y")));
        assert_eq!(m.get("y"), Some(&FieldMode::Last));
    }

    /// `first()` 与 `last()` 同时使用 → 两端都要，退化为完整序列。
    #[test]
    fn first_plus_last_degrades_to_series() {
        let m = modes(&Expr::Array(vec![
            call("first", field("a", "z")),
            call("last", field("a", "z")),
        ]));
        assert_eq!(m.get("z"), Some(&FieldMode::Series));
    }

    /// 嵌套路径在 `first()` 内按根字段归类（否则会被递归记成 Last）。
    #[test]
    fn nested_path_inside_first_uses_root_field() {
        use crate::ast::PathSegment;
        let expr = call(
            "first",
            Expr::Field(FieldRef::Path {
                alias: "a".to_string(),
                segments: vec![PathSegment::Field("meta".to_string())],
            }),
        );
        let m = modes(&expr);
        assert_eq!(m.get("meta"), Some(&FieldMode::First));
    }
}
