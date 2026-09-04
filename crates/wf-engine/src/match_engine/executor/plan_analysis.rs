//! RuleExecutor 构造期（`new_with_options`）的 RulePlan 静态分析组：
//! close/match ctx 字段窄化（plan_close_ctx_fields）、`reduce ... as label`
//! 按需物化门控（plan_reduce_label_reads）、dead-join 消除（compute_live_joins）
//! 与 ctx-free match emit 门控（compute_match_ctx_free）。
//! 类型（`YieldKind`/`ReduceLabelReads`/`OutputStatic`）与 `RuleExecutor`
//! struct 留在 `super`（executor/mod.rs）；四项入口 fn 提 `pub(crate)` 供
//! `rule_exec` 构造路径引用，`plan_close_ctx_fields` 另经 executor 层
//! cfg(test) 转发给 executor 测试。

use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{JoinPlan, RulePlan};

use super::{CloseCtxFields, ReduceLabelReads, YieldKind};
use crate::match_engine::cep::field_ref_name;

/// Narrow the synthetic ctx fields built for close/match alert construction
/// to the names the rule's score/entity/yield expressions can actually read.
/// Any function call (L3 aggregation, window access) or a reference to a
/// reserved synthetic field name forces the conservative all-fields build.
pub(crate) fn plan_close_ctx_fields(plan: &RulePlan) -> CloseCtxFields {
    let mut names = std::collections::HashSet::new();
    let mut force_all = false;
    // let 派生字段（2026-08-31，issue #79）：let RHS 引用的字段必须进入 Named
    // 窄化集合——build_eval_context 只注入 needed 字段，否则 apply_lets 读不到
    // 事件/聚合字段。引用更早 let 名（链式）收集为 ctx 中不存在的名字，无害。
    for l in &plan.lets {
        visit_expr_fields(&l.expr, &mut names, &mut force_all);
    }
    visit_expr_fields(&plan.score_plan.expr, &mut names, &mut force_all);
    visit_expr_fields(&plan.entity_plan.entity_id_expr, &mut names, &mut force_all);
    for field in &plan.yield_plan.fields {
        visit_expr_fields(&field.value, &mut names, &mut force_all);
    }
    // build_eval_context 的 trigger_event 注入按 `needed` 窄化后，Named 集合
    // 必须覆盖所有从 ctx 读取的字段，否则静默失真：
    // - join 条件**左字段**（`first_join_key` 从 ctx 读，缺字段 → join miss →
    //   全 skip；Q4/Q6 的 b.auction 不在 yield 里，窄化前靠全量注入才有值）；
    // - `where` 表达式字段（`where_ok` 从 ctx 读；可能引用 trigger_event 字段）。
    // 右字段/富化字段由 execute_joins 的 enrich_join_row 注入，不依赖这里。
    for join in &plan.joins {
        for cond in &join.conds {
            visit_expr_fields(&Expr::Field(cond.left.clone()), &mut names, &mut force_all);
        }
    }
    if let Some(w) = &plan.r#where {
        visit_expr_fields(w, &mut names, &mut force_all);
    }

    if force_all {
        CloseCtxFields::All
    } else {
        CloseCtxFields::Named(names)
    }
}

fn visit_expr_fields(
    expr: &Expr,
    names: &mut std::collections::HashSet<String>,
    force_all: &mut bool,
) {
    match expr {
        Expr::Field(fr) => match field_ref_name(fr) {
            "" => *force_all = true,
            name if name.starts_with('_') => *force_all = true,
            name => {
                names.insert(name.to_string());
            }
        },
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } if wf_lang::columnar::columnar_output_func(name).is_some() => {
            // 列式输出函数（fmt/strftime/count_char/split/mvindex/concat）：纯
            // 参数函数——只读参数里的字段，不读 `_step_*`/`_bind_*` 合成字段 →
            // 递归收集参数，不 force_all（2026-08-25 层 2 收口 review：q15-q19
            // 的 fmt detail 因此从 All 降为 Named 窄化，行式/回退 ctx 构建省
            // 全量注入）。引用合成字段的表达式仍由 Field 的 `_` 前缀检查
            // force_all，安全。
            for arg in args {
                visit_expr_fields(arg, names, force_all);
            }
        }
        Expr::FuncCall { .. } | Expr::PresetParam(_) => *force_all = true,
        Expr::BinOp { left, right, .. } => {
            visit_expr_fields(left, names, force_all);
            visit_expr_fields(right, names, force_all);
        }
        Expr::Neg(inner) => visit_expr_fields(inner, names, force_all),
        Expr::Not(inner) => visit_expr_fields(inner, names, force_all),
        Expr::Array(items) => {
            for item in items {
                visit_expr_fields(item, names, force_all);
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            visit_expr_fields(inner, names, force_all);
            for item in list {
                visit_expr_fields(item, names, force_all);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            visit_expr_fields(cond, names, force_all);
            visit_expr_fields(then_expr, names, force_all);
            visit_expr_fields(else_expr, names, force_all);
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            visit_expr_fields(expr, names, force_all);
            for arm in arms {
                for pattern in &arm.patterns {
                    visit_expr_fields(pattern, names, force_all);
                }
                visit_expr_fields(&arm.value, names, force_all);
            }
            if let Some(d) = default {
                visit_expr_fields(d, names, force_all);
            }
        }
        Expr::Object(items) => {
            for item in items {
                visit_expr_fields(&item.value, names, force_all);
            }
        }
        // Number/StringLit/Bool/SystemVar/WfuMeta read literals or
        // YieldMeta — no ctx field access.
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_) => {}
        // Non-exhaustive Expr: unknown variants conservatively force the
        // all-fields build (the eval may read anything from the ctx).
        _ => *force_all = true,
    }
}

/// Which `reduce ... as label` objects the rule's expressions actually read
/// (2026-08-24, q9 deferred hot path).
///
/// `as label` injects the winning row as a `Value::Object` under `label`, but
/// the documented access shape `label.field` compiles to `FieldRef::Path {
/// alias: label, segments: [field] }` and [`eval_field_value`] **drops the
/// alias** — it reads `segments[0]` as a root ctx field, i.e. the bare column
/// name that `enrich_join_row` already injected. So for `winner.bidder` the
/// object is pure redundant materialization (one `EngineHashMap` + a clone per
/// row column, per emitted row: `deferred_bench` eval-maxrow 1353 → 1113 ns/op).
///
/// The object *is* observable when some reference resolves to the label name
/// itself — `FieldRef::Simple(label)` (bare `winner`, explicitly allowed by the
/// checker's `resolve_simple`) or any shape whose [`field_ref_name`] equals the
/// label (`Qualified`/`Bracketed` second component). That is exactly the
/// name-set this analysis collects, so gating on it is behavior-preserving.
pub(crate) fn plan_reduce_label_reads(plan: &RulePlan) -> ReduceLabelReads {
    // No labels → nothing to gate (skip the walk entirely).
    if !plan
        .joins
        .iter()
        .any(|j| j.reduce.as_ref().is_some_and(|rc| rc.label.is_some()))
    {
        return ReduceLabelReads::Named(Default::default());
    }
    let mut names = std::collections::HashSet::new();
    let mut force_all = false;
    // Every expression evaluated against the *post-injection* ctx: `where`
    // (`where_ok`), score / entity / yield (`build_each_alert_with`), plus
    // `lets` (conservative — not evaluated on this path today).
    for let_plan in &plan.lets {
        visit_ctx_field_reads(&let_plan.expr, &mut names, &mut force_all);
    }
    if let Some(w) = &plan.r#where {
        visit_ctx_field_reads(w, &mut names, &mut force_all);
    }
    visit_ctx_field_reads(&plan.score_plan.expr, &mut names, &mut force_all);
    visit_ctx_field_reads(&plan.entity_plan.entity_id_expr, &mut names, &mut force_all);
    for field in &plan.yield_plan.fields {
        visit_ctx_field_reads(&field.value, &mut names, &mut force_all);
    }
    if force_all {
        ReduceLabelReads::All
    } else {
        ReduceLabelReads::Named(names)
    }
}

/// Collect the ctx field names an expression can read by name.
///
/// Unlike [`visit_expr_fields`] (which force-alls on any call because the
/// close-ctx build must also cover synthetic `_step_*` fields), plain function
/// calls recurse into their arguments: a function can only reach a ctx field
/// through a field reference in its own arguments. Qualified calls (`stat.*`)
/// and preset params can resolve names we cannot see, so they stay
/// conservative.
fn visit_ctx_field_reads(
    expr: &Expr,
    names: &mut std::collections::HashSet<String>,
    force_all: &mut bool,
) {
    match expr {
        Expr::Field(fr) => {
            let name = field_ref_name(fr);
            // "" = a path starting with an index — cannot name a label.
            if !name.is_empty() {
                names.insert(name.to_string());
            }
        }
        Expr::FuncCall {
            qualifier, args, ..
        } => {
            if qualifier.is_some() {
                *force_all = true;
                return;
            }
            for arg in args {
                visit_ctx_field_reads(arg, names, force_all);
            }
        }
        Expr::BinOp { left, right, .. } => {
            visit_ctx_field_reads(left, names, force_all);
            visit_ctx_field_reads(right, names, force_all);
        }
        Expr::Neg(inner) => visit_ctx_field_reads(inner, names, force_all),
        Expr::Not(inner) => visit_ctx_field_reads(inner, names, force_all),
        Expr::Array(items) => {
            for item in items {
                visit_ctx_field_reads(item, names, force_all);
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            visit_ctx_field_reads(inner, names, force_all);
            for item in list {
                visit_ctx_field_reads(item, names, force_all);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            visit_ctx_field_reads(cond, names, force_all);
            visit_ctx_field_reads(then_expr, names, force_all);
            visit_ctx_field_reads(else_expr, names, force_all);
        }
        Expr::Object(items) => {
            for item in items {
                visit_ctx_field_reads(&item.value, names, force_all);
            }
        }
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_) => {}
        // Preset params expand to unknown expressions; unknown variants may
        // read anything.
        Expr::PresetParam(_) => *force_all = true,
        _ => *force_all = true,
    }
}

/// Compute the subset of [`JoinPlan`]s whose enrichment is actually consumed
/// by the rule's output expressions (lets / `where` / score / entity / yield).
///
/// **Dead-join elimination** (2026-08-23, q13 RSS/EPS): a join whose added
/// fields are never read is pure per-event overhead — for `Snapshot`/`Asof`
/// modes a miss merely skips enrichment and keeps the event, so dropping the
/// join is byte-identical. Other modes have output semantics and are never
/// dropped:
/// - `Inner` / `Anti`: filter (miss/hit drops the event);
/// - `within` interval: `execute_interval_join` drops on miss;
/// - `reduce` / `emit at`: produce output (label value / deferred driver).
///
/// The reference check is deliberately conservative: if any output expression
/// reads a **plain** (unqualified) field, or contains an expression shape we
/// cannot fully analyze, every dead-eligible join stays live (a plain field
/// could be join-provided; the checker would have resolved it so). Only when
/// every output field reference is **qualified** (or a literal) is a join
/// provably dead — its `right_window` then cannot appear in the qualified
/// windows the output reads.
pub(crate) fn compute_live_joins(plan: &RulePlan) -> Vec<JoinPlan> {
    let mut plain_ref = false;
    let mut qualified_windows: std::collections::HashSet<String> = Default::default();
    let mut force_all = false;

    // Output expressions that read the (possibly join-enriched) ctx. The each
    // bind filter runs *before* joins and is excluded (it cannot see enriched
    // fields). The join conditions' own field refs are excluded too — they are
    // read from the *driving* event, not the enriched output.
    for let_plan in &plan.lets {
        visit_output_expr(
            &let_plan.expr,
            &mut plain_ref,
            &mut qualified_windows,
            &mut force_all,
        );
    }
    if let Some(w) = &plan.r#where {
        visit_output_expr(w, &mut plain_ref, &mut qualified_windows, &mut force_all);
    }
    visit_output_expr(
        &plan.score_plan.expr,
        &mut plain_ref,
        &mut qualified_windows,
        &mut force_all,
    );
    visit_output_expr(
        &plan.entity_plan.entity_id_expr,
        &mut plain_ref,
        &mut qualified_windows,
        &mut force_all,
    );
    for field in &plan.yield_plan.fields {
        visit_output_expr(
            &field.value,
            &mut plain_ref,
            &mut qualified_windows,
            &mut force_all,
        );
    }

    plan.joins
        .iter()
        .filter(|join| {
            // Non-eliminable (output semantics): keep.
            let eligible = matches!(join.mode, JoinMode::Snapshot | JoinMode::Asof { .. })
                && join.within.is_none()
                && join.reduce.is_none()
                && join.emit_at.is_none();
            if !eligible || plain_ref || force_all {
                return true;
            }
            // Keep iff the output reads this window's fields (provably dead
            // otherwise: no output expression can see the dropped enrichment).
            qualified_windows.contains(join.right_window.as_str())
        })
        .cloned()
        .collect()
}

/// ctx-free match emit gate（F8.5，2026-08-23，q6 每事件 emit）：
/// 规则输出（score/entity/yield）只读「scope_key 键 ∪ 触发事件字段」时，
/// emit 免 `build_eval_context` 的 HashMap 构建，字段直读。条件：
/// - score 常量（`score_const` 同判定：Number 字面量）；
/// - entity 为 `Expr::Field`；yield 字段全 Lit/Field（无 General 表达式）；
/// - 无 `where`（execute_match_with_joins 的 where 需要完整 ctx）；
/// - live_joins 空（join 富化字段读不到——match 形态 join 若存活则禁用）；
/// - 输出 Field 字段名不命中 step branch labels / tracked 字段集合 /
///   `_step_*`、`_bind_*` 合成字段（这些只能从 ctx 读取）。
pub(crate) fn compute_match_ctx_free(
    plan: &RulePlan,
    live_joins: &[JoinPlan],
    yield_kinds: &[YieldKind],
) -> bool {
    if plan.r#where.is_some()
        || !live_joins.is_empty()
        // let 派生字段（2026-08-31，issue #79）：需 build_eval_context 注入，
        // Free 模式（字段直读 scope_key + trigger_event）读不到 let 值。
        || !plan.lets.is_empty()
        || !matches!(plan.score_plan.expr, Expr::Number(_))
        || !matches!(plan.entity_plan.entity_id_expr, Expr::Field(_))
        || yield_kinds.iter().any(|k| matches!(k, YieldKind::General))
    {
        return false;
    }

    // step branch labels：ctx 注入 label → measure_value，free 模式读不到。
    let labels: std::collections::HashSet<&str> = plan
        .match_plan
        .event_steps
        .iter()
        .flat_map(|step| step.branches.iter())
        .filter_map(|b| b.label.as_deref())
        .collect();
    // tracked 字段：ctx 的 field_values 注入 last_val（bare 字段名）。
    let tracked: std::collections::HashSet<&str> = plan
        .match_plan
        .tracked_plain_fields
        .iter()
        .map(|s| s.as_str())
        .chain(
            plan.match_plan
                .tracked_bind_fields
                .values()
                .flatten()
                .map(|s| s.as_str()),
        )
        .collect();

    // 输出字段引用集合。
    let mut out_fields = Vec::new();
    if let Expr::Field(fr) = &plan.entity_plan.entity_id_expr {
        out_fields.push(fr);
    }
    out_fields.extend(
        plan.yield_plan
            .fields
            .iter()
            .filter_map(|f| match &f.value {
                Expr::Field(fr) => Some(fr),
                _ => None,
            }),
    );

    out_fields.into_iter().all(|fr| {
        let name = field_ref_name(fr);
        !labels.contains(name)
            && !tracked.contains(name)
            && !name.starts_with("_step_")
            && !name.starts_with("_bind_")
    })
}

/// Collect field refs from an output expression: `Qualified(window, _)` records
/// the window; `Simple`/`Bracketed`/`Path` (plain reads) set `plain_ref`; shapes
/// we cannot fully inspect set `force_all` (keep every join live).
fn visit_output_expr(
    expr: &Expr,
    plain_ref: &mut bool,
    qualified_windows: &mut std::collections::HashSet<String>,
    force_all: &mut bool,
) {
    match expr {
        Expr::Field(fr) => match fr {
            FieldRef::Qualified(window, _) => {
                qualified_windows.insert(window.clone());
            }
            FieldRef::Simple(_) | FieldRef::Bracketed(_, _) | FieldRef::Path { .. } => {
                *plain_ref = true;
            }
            _ => *force_all = true,
        },
        Expr::FuncCall { args, .. } => {
            for arg in args {
                visit_output_expr(arg, plain_ref, qualified_windows, force_all);
            }
        }
        Expr::BinOp { left, right, .. } => {
            visit_output_expr(left, plain_ref, qualified_windows, force_all);
            visit_output_expr(right, plain_ref, qualified_windows, force_all);
        }
        Expr::Neg(inner) => visit_output_expr(inner, plain_ref, qualified_windows, force_all),
        Expr::Not(inner) => visit_output_expr(inner, plain_ref, qualified_windows, force_all),
        Expr::Array(items) => {
            for item in items {
                visit_output_expr(item, plain_ref, qualified_windows, force_all);
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            visit_output_expr(inner, plain_ref, qualified_windows, force_all);
            for item in list {
                visit_output_expr(item, plain_ref, qualified_windows, force_all);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            visit_output_expr(cond, plain_ref, qualified_windows, force_all);
            visit_output_expr(then_expr, plain_ref, qualified_windows, force_all);
            visit_output_expr(else_expr, plain_ref, qualified_windows, force_all);
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            visit_output_expr(expr, plain_ref, qualified_windows, force_all);
            for arm in arms {
                for pattern in &arm.patterns {
                    visit_output_expr(pattern, plain_ref, qualified_windows, force_all);
                }
                visit_output_expr(&arm.value, plain_ref, qualified_windows, force_all);
            }
            if let Some(d) = default {
                visit_output_expr(d, plain_ref, qualified_windows, force_all);
            }
        }
        Expr::Object(items) => {
            for item in items {
                visit_output_expr(&item.value, plain_ref, qualified_windows, force_all);
            }
        }
        // Literals / system vars read no ctx fields.
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::PresetParam(_) => {}
        // Unknown/forward-compatible Expr variants: cannot prove field reads —
        // conservatively keep every join live.
        _ => *force_all = true,
    }
}
