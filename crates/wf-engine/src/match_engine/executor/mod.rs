mod alert;
#[cfg(test)]
pub(crate) use alert::{EachWfxPrefix, format_nanos_utc};
use each_exec::parse_each_join_columnar;
mod close_exec;
mod context;
#[cfg(test)]
pub(crate) use context::{build_eval_context, execute_joins};
#[cfg(test)]
pub(crate) use context::{enrich_join_row, enrich_join_row_bare, in_interval, row_matches_conds};
#[cfg(test)]
pub(crate) use deferred_exec::select_reduce_row;
mod deferred_exec;
pub use deferred_exec::DeferredPending;
mod each_exec;
mod eval;
mod match_exec;
mod stats_exec;

pub use each_exec::{EachDirectBatchStats, PipeEachRow, PipeRowSink};
// 供 `match_engine::pub use executor::DistinctKey` 转发（stats distinct 键类型）。
pub use stats_exec::{
    DistinctKey, DistinctSet, NumericAccum, RowFieldLayout, RowFields, StatsAccum, StatsBucketAccs,
    StatsCloseBucket, StatsExecutor, StatsMaskCache, StatsWindowState, TopEntry,
};

// 供 crate 内 SoA 对照 bench（tests/）访问私有热路径函数。
#[cfg(test)]
pub(crate) use stats_exec::{
    NumericSoALayout, StatsBucket, accumulate_column_row, accumulate_soa, comps_hash,
    measure_values_soa,
};
// spill 序列化用（同 crate：executor::stats_exec 为私有模块，需经此转发）。
pub(crate) use stats_exec::scope_key_hash;

#[cfg(test)]
mod close_coverage_more;
#[cfg(test)]
mod context_coverage_more;
#[cfg(test)]
mod coverage_extra;
#[cfg(test)]
mod coverage_more;
#[cfg(test)]
mod coverage_r4;
#[cfg(test)]
mod stats_coverage_extra;
#[cfg(test)]
mod stats_exec_test;
#[cfg(test)]
mod stats_exec_wiring_test;
#[cfg(test)]
mod stats_spill_test;

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use orion_error::conversion::{SourceRawErr, ToStructError};
use wf_config::OutputConfig;
use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{JoinPlan, RulePlan};
use wf_lang::{BaseType, FieldType};

use self::alert::build_summary;
pub(crate) use self::context::CloseCtxFields;
use self::eval::eval_bool_expr_with_lookup;
use crate::alert::AlertOrigin;
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::columnar::{
    CVec, ColumnExpr, ColumnarBatch, GuardMasks, compile_guard, compile_yield_cvec,
    eval_compiled_guard,
};
use crate::match_engine::match_engine::{Event, FieldSource, Value, WindowLookup, field_ref_name};
use crate::time::normalize_epoch_timestamp_float_nanos;
use arrow::array::BooleanArray;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

/// Per-yield-field specialization, precomputed once at executor construction.
///
/// - `Lit`: a literal expression — its `Value` is built once and cloned per
///   record (no interpreter dispatch).
/// - `Field`: an `Expr::Field` — a direct flat field lookup, skipping the
///   interpreter and its per-record eval-time scope.
/// - `General`: anything else (system vars, `$wfu.*`, functions) — full
///   interpreter evaluation with the per-record meta.
#[derive(Clone)]
pub(crate) enum YieldKind {
    Lit(Value),
    Field,
    General,
}

/// Plan-level output constants, precomputed once at executor construction.
///
/// These are identical for every event/match a rule produces. The hot path
/// previously re-derived them per event — `String` clones of rule/entity/
/// target names, per-field `HashMap` type lookups, per-event summary
/// formatting — roughly a dozen heap allocations per output record that
/// existed only to reproduce plan constants.
#[derive(Clone)]
pub(crate) struct OutputStatic {
    pub(crate) rule_name: Arc<str>,
    pub(crate) entity_type: Arc<str>,
    pub(crate) yield_target: Arc<str>,
    /// `(field name, resolved type)` aligned by index with
    /// `plan.yield_plan.fields` — kills the per-field type lookup + name
    /// clone on every output.
    pub(crate) yield_specs: Arc<[(Arc<str>, Option<FieldType>)]>,
    /// Typed field list carried by every `OutputRecord` (plan constant).
    pub(crate) yield_field_types: Arc<[(Arc<str>, FieldType)]>,
    /// Per-yield-field specialization, index-aligned with `yield_specs`.
    pub(crate) yield_kinds: Arc<[YieldKind]>,
    /// `Some(clamp(n))` when the score expression is a numeric literal — lets
    /// the hot path skip `eval_score` for constant-score rules.
    pub(crate) score_const: Option<f64>,
    /// `on each` constant summary — scope key and step data are always empty
    /// on that path, so the whole summary string is a plan constant.
    pub(crate) each_summary: Option<Arc<str>>,
    /// `on each` constant origin string (`"event"`) — direct-write emit
    /// (plan C2) shares this `Arc` instead of copying per record.
    pub(crate) each_origin: Arc<str>,
    /// `on each` constant close reason (`""` — the event origin never has a
    /// close reason).
    pub(crate) each_close_reason: Arc<str>,
    /// ctx-free match emit gate（F8.5）：true 时 `build_match_alert_free` 免
    /// `build_eval_context` HashMap 构建，字段直读 scope_key + trigger_event。
    /// 条件：score 常量、entity/yield 全 Field/Lit、无 where、live_joins 空、
    /// 输出字段不依赖 step label/tracked/_step_/_bind_ 合成字段。
    pub(crate) match_ctx_free: bool,
}

/// Narrow the synthetic ctx fields built for close/match alert construction
/// to the names the rule's score/entity/yield expressions can actually read.
/// Any function call (L3 aggregation, window access) or a reference to a
/// reserved synthetic field name forces the conservative all-fields build.
fn plan_close_ctx_fields(plan: &RulePlan) -> CloseCtxFields {
    let mut names = std::collections::HashSet::new();
    let mut force_all = false;
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

/// Ctx field names the rule's post-join expressions can read, used to gate the
/// `reduce ... as label` object materialization (see [`ReduceLabelReads`]).
#[derive(Debug, Clone)]
pub(crate) enum ReduceLabelReads {
    /// An expression shape we cannot fully analyze — assume every name is read.
    All,
    /// `field_ref_name` of every field reference in the post-join expressions.
    Named(std::collections::HashSet<String>),
}

impl ReduceLabelReads {
    /// True when the rule can read `label` as a whole object, i.e. the `as
    /// label` injection is observable.
    pub(crate) fn needs(&self, label: &str) -> bool {
        match self {
            ReduceLabelReads::All => true,
            ReduceLabelReads::Named(names) => names.contains(label),
        }
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
fn plan_reduce_label_reads(plan: &RulePlan) -> ReduceLabelReads {
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
fn compute_live_joins(plan: &RulePlan) -> Vec<JoinPlan> {
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
fn compute_match_ctx_free(
    plan: &RulePlan,
    live_joins: &[JoinPlan],
    yield_kinds: &[YieldKind],
) -> bool {
    if plan.r#where.is_some()
        || !live_joins.is_empty()
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

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`OutputRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct RuleExecutor {
    plan: RulePlan,
    /// 列式 join 富化计划（each + 单 Snapshot join 的列式执行描述）；`None` =
    /// 形状不支持 → 行式 each+join 路径。见 [`parse_each_join_columnar`]。
    each_join_plan: Option<crate::match_engine::executor::each_exec::EachJoinPlan>,
    /// Joins whose enrichment the rule's output expressions actually read.
    /// Dead joins (Snapshot/Asof, enrichment unreferenced) are dropped here so
    /// the per-event join cost (ctx clone + lookup + `find_matching_row` +
    /// enrichment) disappears and rules can use the columnar each path — the
    /// q13 RSS/EPS fix (2026-08-23: q13 声明了 person 快照 join 但 yield/score/
    /// entity 全读 bid 字段 → join 纯开销，消除后 1.7M→7.6M/s 量级)。
    /// 语义安全：Snapshot/Asof miss 保留事件（无过滤作用），富化字段无人读 →
    /// 输出字节不变。Inner/Anti/within/reduce/emit_at 有过滤/输出语义 → 永不消除。
    live_joins: Vec<JoinPlan>,
    yield_field_types: HashMap<String, FieldType>,
    output: OutputConfig,
    /// alias → bind filter, precomputed so per-event alias matching is O(1)
    /// instead of a linear scan of `plan.binds` on every (event × alias).
    bind_filters: HashMap<String, Option<Expr>>,
    output_static: OutputStatic,
    /// Last (nanos, formatted) emit time. The runtime feeds a batch-level
    /// cached wall clock into the on-each path, so all events in a batch
    /// share one timestamp — format it once and Arc-share it instead of one
    /// String per event.
    ///
    /// The cache is a pure memo (value fully determined by the nanos key),
    /// so clones get their OWN cache (reset to empty). It must NOT be
    /// shared behind an `Arc`: sharded on-each workers lock it per event,
    /// and a shared `Mutex` ping-pongs a cache line across worker threads —
    /// 6 workers on one lock dropped per-worker throughput ~20x (nexmark
    /// q1 30M, 2026-08-16).
    emit_time_cache: Mutex<(i64, Arc<str>)>,
    /// Narrowed synthetic ctx field set for the close/match alert builders
    /// (see [`plan_close_ctx_fields`] — `All` for rules whose expressions
    /// can't be statically narrowed).
    close_ctx_fields: CloseCtxFields,
    /// Whether the rule can observe a `reduce ... as label` object, gating the
    /// deferred-join label materialization (see [`plan_reduce_label_reads`]).
    reduce_label_reads: ReduceLabelReads,
    /// Compiled columnar guard trees, cached across batches (review #5): a
    /// [`wf_engine::match_engine::columnar::ColumnExpr`] tree is batch-
    /// independent (leaf [`ColRef`]s are projection-slot + type tags), so the
    /// expensive per-batch work — expression-tree build, `Cidr::parse` /
    /// `regex::Regex::new` for literal constants — happens once per
    /// (site, schema) instead of once per batch. The key carries a schema
    /// fingerprint so a reused tree is never applied to a mismatched schema
    /// (a batch whose schema drifted recompiles, then re-caches).
    ///
    /// The cache is a pure memo, so clones get their OWN cache (reset empty,
    /// mirroring `emit_time_cache`). `Mutex` keeps the executor `Send + Sync`;
    /// lookups happen once per (batch, site) — never in the per-row hot loop.
    compiled_guards: Mutex<std::collections::HashMap<(String, u64), ColumnExpr>>,
}

/// Schema fingerprint for the compiled-guard cache: field name + data type +
/// metadata (the metadata marks structured JSON-array columns, which change
/// the compiled [`ColKind`]). Two batches of the same window share a schema, so
/// the fingerprint is stable across them; any drift recompiles.
fn guard_schema_fingerprint(batch: &RecordBatch) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for f in batch.schema().fields() {
        f.name().hash(&mut h);
        format!("{:?}", f.data_type()).hash(&mut h);
        for (k, v) in f.metadata() {
            k.hash(&mut h);
            v.hash(&mut h);
        }
    }
    h.finish()
}

// Manual impl: `Mutex` is not `Clone`. `emit_time_cache` is a pure memo
// keyed by nanos, so the clone simply starts with an empty cache instead of
// sharing the lock — each sharded on-each worker locks only its own cache.
impl Clone for RuleExecutor {
    fn clone(&self) -> Self {
        Self {
            plan: self.plan.clone(),
            each_join_plan: self.each_join_plan.clone(),
            live_joins: self.live_joins.clone(),
            yield_field_types: self.yield_field_types.clone(),
            output: self.output.clone(),
            bind_filters: self.bind_filters.clone(),
            output_static: self.output_static.clone(),
            emit_time_cache: Mutex::new((0, Arc::from(""))),
            close_ctx_fields: self.close_ctx_fields.clone(),
            reduce_label_reads: self.reduce_label_reads.clone(),
            compiled_guards: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuleExecutorOptions {
    pub yield_field_types: HashMap<String, FieldType>,
    pub output: OutputConfig,
}

impl RuleExecutor {
    pub fn new(plan: RulePlan) -> Self {
        Self::new_with_options(plan, RuleExecutorOptions::default())
    }

    pub fn new_with_yield_field_types(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
    ) -> Self {
        Self::new_with_options(
            plan,
            RuleExecutorOptions {
                yield_field_types,
                output: OutputConfig::default(),
            },
        )
    }

    pub fn new_with_yield_field_types_and_output(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
        output: OutputConfig,
    ) -> Self {
        Self::new_with_options(
            plan,
            RuleExecutorOptions {
                yield_field_types,
                output,
            },
        )
    }

    pub fn new_with_options(plan: RulePlan, options: RuleExecutorOptions) -> Self {
        let live_joins = compute_live_joins(&plan);
        let each_join_plan = parse_each_join_columnar(&plan, &live_joins);
        let bind_filters = plan
            .binds
            .iter()
            .map(|b| (b.alias.clone(), b.filter.clone()))
            .collect();
        // Precompute plan-level output constants (see `OutputStatic`). The
        // yield field types map comes from runtime schema knowledge, which is
        // exactly what `new_with_options` receives.
        let yield_specs: Vec<(Arc<str>, Option<FieldType>)> = plan
            .yield_plan
            .fields
            .iter()
            .map(|field| {
                (
                    Arc::from(field.name.as_str()),
                    options.yield_field_types.get(&field.name).cloned(),
                )
            })
            .collect();
        let typed_fields: Vec<(Arc<str>, FieldType)> = yield_specs
            .iter()
            .filter_map(|(name, field_type)| {
                field_type
                    .clone()
                    .map(|field_type| (Arc::clone(name), field_type))
            })
            .collect();
        let each_summary = plan.each_plan.as_ref().map(|_| {
            Arc::from(build_summary(
                &plan.name,
                &[],
                &[],
                &[],
                &AlertOrigin::Event,
            ))
        });
        // Precompute per-field yield specialization (literal value / direct
        // field lookup / full interpreter) and the constant score, so the
        // per-record hot path never re-classifies expressions.
        let yield_kinds: Vec<YieldKind> = plan
            .yield_plan
            .fields
            .iter()
            .map(|field| match &field.value {
                Expr::Number(n) => YieldKind::Lit(Value::Number(*n)),
                Expr::StringLit(s) => YieldKind::Lit(Value::Str(s.clone().into())),
                Expr::Bool(b) => YieldKind::Lit(Value::Bool(*b)),
                Expr::Field(_) => YieldKind::Field,
                _ => YieldKind::General,
            })
            .collect();
        let score_const = match &plan.score_plan.expr {
            Expr::Number(n) => Some(n.clamp(0.0, 100.0)),
            _ => None,
        };
        let close_ctx_fields = plan_close_ctx_fields(&plan);
        let reduce_label_reads = plan_reduce_label_reads(&plan);
        let match_ctx_free = compute_match_ctx_free(&plan, &live_joins, &yield_kinds);
        Self {
            output_static: OutputStatic {
                rule_name: Arc::from(plan.name.as_str()),
                entity_type: Arc::from(plan.entity_plan.entity_type.as_str()),
                yield_target: Arc::from(plan.yield_plan.target.as_str()),
                yield_specs: Arc::from(yield_specs),
                yield_field_types: Arc::from(typed_fields),
                yield_kinds: Arc::from(yield_kinds),
                score_const,
                each_summary,
                each_origin: Arc::from(AlertOrigin::Event.as_str()),
                each_close_reason: Arc::from(""),
                match_ctx_free,
            },
            plan,
            each_join_plan,
            live_joins,
            yield_field_types: options.yield_field_types,
            output: options.output,
            bind_filters,
            emit_time_cache: Mutex::new((0, Arc::from(""))),
            close_ctx_fields,
            reduce_label_reads,
            compiled_guards: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Formatted emit time for `nanos`, cached: consecutive calls with the
    /// same nanos (the batch-shared wall clock) return the same `Arc<str>`
    /// with no re-formatting.
    pub(crate) fn cached_emit_time(&self, nanos: i64) -> Arc<str> {
        let mut cache = self.emit_time_cache.lock().unwrap();
        if cache.0 != nanos || cache.1.is_empty() {
            *cache = (nanos, Arc::from(alert::format_nanos_utc(nanos)));
        }
        Arc::clone(&cache.1)
    }

    pub fn plan(&self) -> &RulePlan {
        &self.plan
    }

    /// Live (output-referenced) joins after dead-join elimination. The runtime
    /// uses this to route between the join-free and join columnar each paths.
    pub fn live_joins(&self) -> &[JoinPlan] {
        &self.live_joins
    }

    /// Whether this rule can run the columnar join-enrichment each path
    /// (q20 等：each + 单 Snapshot join + 受限 where/输出形状)。
    pub fn each_join_columnar_ready(&self) -> bool {
        self.each_join_plan.is_some()
    }

    /// Post-join `where` filter check: evaluated after joins enrich the event
    /// context, before alert construction. Strict semantics — `false` or a
    /// missing field (`None`) suppresses the output, aligning INNER JOIN
    /// miss-drop (q3 state filter / q20 category filter).
    pub(crate) fn where_ok(&self, ctx: &Event) -> bool {
        match &self.plan.r#where {
            None => true,
            Some(expr) => matches!(self::eval::eval_bool_expr(expr, ctx), Some(true)),
        }
    }

    /// Precomputed plan-level output constants (see [`OutputStatic`]).
    pub(crate) fn output_static(&self) -> &OutputStatic {
        &self.output_static
    }

    /// Plan-constant yield target as the precomputed `Arc` — used by the
    /// runtime's direct-write on-each emit (plan C2) to locate the columnar
    /// builder without re-deriving or re-allocating the target string.
    pub fn static_yield_target(&self) -> &Arc<str> {
        &self.output_static().yield_target
    }

    /// close ctx 是否 All（保守全字段构建——L3 聚合/窗口访问表达式用）。
    /// stats close 据此决定行字段注入方式（Named 窄化下多 last 度量共享同一
    /// [`RowFields`] Arc, 只需首个度量注入; All 下每度量独立注入, 保
    /// `_step_i_field_*` 完整性）。
    pub fn close_ctx_is_all(&self) -> bool {
        matches!(self.close_ctx_fields, CloseCtxFields::All)
    }

    pub(crate) fn output_config(&self) -> &OutputConfig {
        &self.output
    }

    /// Coerce a yield field value against a precomputed type (from
    /// `output_static().yield_specs`) — avoids the per-field `HashMap`
    /// lookup on the hot path.
    pub(crate) fn coerce_yield_field_value_with(
        name: &str,
        field_type: Option<&FieldType>,
        value: Value,
    ) -> CoreResult<Option<Value>> {
        let Some(field_type) = field_type else {
            return Ok(Some(value));
        };
        coerce_yield_value(name, field_type, value)
    }

    pub(crate) fn build_machine_id(&self, machine_id: &str) -> Arc<str> {
        if machine_id.is_empty() {
            // 热路径（q6 等无自定义 machine_id）：直接 Arc 复用 rule 名——
            // OutputRecord.machine_id 由 String 改 Arc<str> 后免每事件 String
            // clone + 堆分配（sample: String::clone 9+7+5）。
            Arc::clone(&self.output_static().rule_name)
        } else {
            Arc::from(machine_id)
        }
    }

    pub(crate) fn build_scope_key(
        &self,
        keys: &[wf_lang::ast::FieldRef],
        scope_values: &[crate::match_engine::match_engine::Value],
    ) -> Arc<str> {
        use std::fmt::Write as _;
        // 单 String 一次写入（旧实现每 key 一个 format! String + Vec + join
        // → 每事件 2 次分配 + Vec 分配；q6 每事件 emit 路径的分配热点之一）。
        let mut out = String::with_capacity(scope_values.len() * 16);
        for (i, (k, v)) in keys.iter().zip(scope_values.iter()).enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(
                out,
                "{}={}",
                crate::match_engine::match_engine::field_ref_name(k),
                crate::match_engine::match_engine::value_to_string(v)
            );
        }
        Arc::from(out)
    }

    pub fn event_matches_alias(
        &self,
        alias: &str,
        event: &dyn FieldSource,
        windows: Option<&dyn WindowLookup>,
    ) -> bool {
        passes_bind_filter(self.bind_filter(alias), event, windows)
    }

    /// The bind filter for `alias`, if any.
    ///
    /// Few binds: a linear scan is cheaper than hashing the alias. Many binds:
    /// the precomputed map keeps this O(1) instead of O(binds) per event.
    /// Measured crossover: the map wins from ~24 binds (24: 5.1M vs 5.8M q/s;
    /// 16: linear still 1.3x faster).
    fn bind_filter(&self, alias: &str) -> Option<&Expr> {
        if self.plan.binds.len() <= 24 {
            self.plan
                .binds
                .iter()
                .find(|b| b.alias == alias)
                .and_then(|b| b.filter.as_ref())
        } else {
            self.bind_filters.get(alias).and_then(|f| f.as_ref())
        }
    }

    /// Columnar evaluation of `alias`'s bind filter over a whole batch.
    ///
    /// Returns `None` when there is no filter (nothing to reject) or the filter
    /// is not columnar (caller falls back to per-event [`Self::event_matches_alias`]).
    /// `Some(mask)` has one boolean per row; `false` = bind filter rejected that
    /// row, matching `event_matches_alias`'s `false`.
    pub fn bind_filter_columnar_mask(
        &self,
        alias: &str,
        batch: &RecordBatch,
    ) -> Option<BooleanArray> {
        let filter = self.bind_filter(alias)?;
        if !wf_lang::columnar::expr_is_columnar(filter) {
            return None;
        }
        let view = ColumnarBatch::from_all_fields(batch);
        Some(self.guard_mask(&format!("bind:{alias}"), filter, &view, batch))
    }

    /// Compiled-guard cache lookup-or-compile: the compiled [`ColumnExpr`] tree
    /// is batch-independent (leaf [`ColRef`]s are projection slot + type tag),
    /// so identical `(site, schema)` calls reuse the tree instead of rebuilding
    /// it — and re-parsing / recompiling its literal constants — per batch.
    /// The schema fingerprint in the key means a schema-drifted batch recompiles
    /// rather than evaluating a stale tree.
    fn guard_mask(
        &self,
        site: &str,
        filter: &Expr,
        view: &ColumnarBatch<'_>,
        batch: &RecordBatch,
    ) -> BooleanArray {
        let key = (site.to_string(), guard_schema_fingerprint(batch));
        let mut cache = self
            .compiled_guards
            .lock()
            .expect("compiled-guard cache poisoned");
        if let Some(plan) = cache.get(&key) {
            return eval_compiled_guard(plan, view);
        }
        let Some(plan) = compile_guard(filter, view) else {
            // Uncompilable (a shape outside the gate, or an invalid literal
            // constant) → all rows miss, matching `eval_guard_columnar`'s
            // fallback; not cached (the failure is per-call cheap).
            return BooleanArray::from(vec![false; view.num_rows()]);
        };
        let plan_ref = cache.entry(key).or_insert(plan);
        eval_compiled_guard(plan_ref, view)
    }

    /// Whether every bind of `window` can be evaluated columnarly — every
    /// filter is absent (nothing to reject) or [`expr_is_columnar`]. This is
    /// the rule-task's local check for safe deferred (columnar) materialization
    /// when a raw batch is present: a non-columnar filter would otherwise be
    /// skipped entirely (the deferred path's missing-mask fallback accepts all
    /// rows), silently corrupting the filtered subset.
    pub fn bind_filters_columnar_safe(&self, window: &str) -> bool {
        self.plan
            .binds
            .iter()
            .filter(|b| b.window == window)
            .all(|b| {
                b.filter
                    .as_ref()
                    .is_none_or(wf_lang::columnar::expr_is_columnar)
            })
    }

    /// Columnar evaluation of the `on each` filter over a whole batch.
    ///
    /// Same `None` / `Some(mask)` contract as [`Self::bind_filter_columnar_mask`],
    /// but for `plan.each_plan.filter`.
    pub fn each_filter_columnar_mask(&self, batch: &RecordBatch) -> Option<BooleanArray> {
        let filter = self.plan.each_plan.as_ref()?.filter.as_ref()?;
        if !wf_lang::columnar::expr_is_columnar(filter) {
            return None;
        }
        let view = ColumnarBatch::from_all_fields(batch);
        Some(self.guard_mask("each", filter, &view, batch))
    }

    /// Columnar branch-guard masks for the three per-event guard sites:
    ///
    /// - event steps, keyed `(event_step_idx, branch_idx)`;
    /// - close-step accumulation guards, keyed `(close_step_idx, branch_idx)`;
    /// - seq negation guards, keyed `(neg_idx, 0)`.
    ///
    /// Non-columnar / absent guards are simply not inserted, so the state
    /// machine falls back to interpreted evaluation for those branches.
    pub fn branch_guard_masks(&self, batch: &RecordBatch) -> GuardMasks {
        let view = ColumnarBatch::from_all_fields(batch);
        let mut masks = GuardMasks::default();
        for (step_idx, step) in self.plan.match_plan.event_steps.iter().enumerate() {
            for (branch_idx, branch) in step.branches.iter().enumerate() {
                if let Some(guard) = &branch.guard
                    && wf_lang::columnar::expr_is_columnar(guard)
                {
                    let site = format!("event:{step_idx}:{branch_idx}");
                    masks.insert_event(
                        step_idx,
                        branch_idx,
                        self.guard_mask(&site, guard, &view, batch),
                    );
                }
            }
        }
        for (step_idx, step) in self.plan.match_plan.close_steps.iter().enumerate() {
            for (branch_idx, branch) in step.branches.iter().enumerate() {
                if let Some(guard) = &branch.guard
                    && wf_lang::columnar::expr_is_columnar(guard)
                {
                    let site = format!("close:{step_idx}:{branch_idx}");
                    masks.insert_close(
                        step_idx,
                        branch_idx,
                        self.guard_mask(&site, guard, &view, batch),
                    );
                }
            }
        }
        if let Some(seq) = &self.plan.match_plan.seq {
            // Negation steps only, in the same order `SeqRuntime::build` emits
            // them (so `neg_idx` lines up with `meta.negs`).
            let mut neg_idx = 0usize;
            for step in &seq.steps {
                if step.neg {
                    if let Some(guard) = &step.branch.guard
                        && wf_lang::columnar::expr_is_columnar(guard)
                    {
                        let site = format!("neg:{neg_idx}");
                        masks.insert_neg(neg_idx, 0, self.guard_mask(&site, guard, &view, batch));
                    }
                    neg_idx += 1;
                }
            }
        }
        masks
    }

    pub fn is_aux_bind_alias(&self, alias: &str) -> bool {
        !self
            .plan
            .match_plan
            .event_steps
            .iter()
            .chain(self.plan.match_plan.close_steps.iter())
            .flat_map(|step| step.branches.iter())
            .any(|branch| branch.source == alias)
    }

    /// 引用字段集 = 键名（ctx 恒注入）∪ General yield **内联 let 后**引用的普通
    /// 字段（层 2 收口，close/match/行式批共用；去重保序）。内联展开保证物化
    /// 视图包含 let RHS 引用的 schema 字段（q22：`let parts = split(url)` → 需
    /// 物化 url，而非 let 名 parts）。
    ///
    /// `inline` 必须与 `compile_general_slots` 的 `lets` 参数一致：each/行式批
    /// 传 true（编译也内联）；close/match 传 false（编译传空 lets——解释路径
    /// 无 let 视图，内联会产生值 vs 解释空串的分叉；此时 let RHS 字段不收集，
    /// 物化只含 yield 直接引用的字段，编译对 let 名读 Null → 空串，一致）。
    pub(crate) fn yield_ref_fields(&self, inline: bool) -> Vec<String> {
        let mut ref_fields: Vec<String> = Vec::new();
        for k in &self.plan.match_plan.keys {
            push_uniq(&mut ref_fields, field_ref_name(k));
        }
        for field in &self.plan.yield_plan.fields {
            if !matches!(
                field.value,
                Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) | Expr::Field(_)
            ) {
                if inline && !self.plan.lets.is_empty() {
                    let inlined = crate::match_engine::columnar::inline_lets(
                        &field.value,
                        &self.plan.lets,
                        &mut Vec::new(),
                    );
                    collect_general_plain_fields(&inlined, &mut ref_fields);
                } else {
                    collect_general_plain_fields(&field.value, &mut ref_fields);
                }
            }
        }
        ref_fields
    }

    /// 统一列式 General yield 槽位编译（层 2 收口）：引用字段物化（统一
    /// `materialize_fields`）→ `ColumnarBatch` 视图 → 逐 yield 字段
    /// `compile_yield_cvec`。槽位按 yield 字段位置索引（Lit/Field → None）；
    /// 物化失败（类型不一致/结构化）/ 编译失败 → None → 调用方逐行回退。
    pub(crate) fn compile_general_slots<F>(
        &self,
        ref_fields: &[String],
        n: usize,
        resolve: F,
        lets: &[wf_lang::plan::LetPlan],
    ) -> Vec<Option<CVec>>
    where
        F: FnMut(usize, &str) -> Option<Value>,
    {
        let slots = self.plan.yield_plan.fields.len();
        if n == 0 || ref_fields.is_empty() {
            return (0..slots).map(|_| None).collect();
        }
        let Some((schema_fields, arrays)) =
            crate::match_engine::columnar::materialize_fields(ref_fields, n, resolve)
        else {
            return (0..slots).map(|_| None).collect();
        };
        match RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), arrays) {
            Ok(batch) => {
                let view = ColumnarBatch::from_all_fields(&batch);
                self.plan
                    .yield_plan
                    .fields
                    .iter()
                    .map(|field| compile_yield_cvec(field, &view, n, lets))
                    .collect()
            }
            Err(_) => (0..slots).map(|_| None).collect(),
        }
    }
}

/// General yield 表达式引用的普通字段名（非空、非 `_` 合成、非 Path）——与
/// `yield_general_columnar_safe` 的门控形状一致（门控已保证只引用这些字段，
/// 这里静态收集供物化用；去重保序）。close/match/行式批共用（层 2 收口）。
pub(crate) fn collect_general_plain_fields(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Field(fr) => push_uniq(out, field_ref_name(fr)),
        Expr::BinOp { left, right, .. } => {
            collect_general_plain_fields(left, out);
            collect_general_plain_fields(right, out);
        }
        Expr::Neg(inner) | Expr::Not(inner) => collect_general_plain_fields(inner, out),
        Expr::Array(items) => {
            for item in items {
                collect_general_plain_fields(item, out);
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            collect_general_plain_fields(inner, out);
            for item in list {
                collect_general_plain_fields(item, out);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_general_plain_fields(cond, out);
            collect_general_plain_fields(then_expr, out);
            collect_general_plain_fields(else_expr, out);
        }
        Expr::Object(items) => {
            for it in items {
                collect_general_plain_fields(&it.value, out);
            }
        }
        Expr::FuncCall { args, .. } => {
            for a in args {
                collect_general_plain_fields(a, out);
            }
        }
        _ => {}
    }
}

pub(crate) fn push_uniq(v: &mut Vec<String>, name: &str) {
    if !name.is_empty() && !v.iter().any(|f| f == name) {
        v.push(name.to_string());
    }
}

/// General yield（fmt/strftime/count_char 等）在列式 close/match 路径可安全求值:
/// 表达式引用的全部字段都是普通字段（非 `_step_*`/`_bind_*` 合成字段、非 Path、
/// 非空名）——Named 窄化的 `build_eval_context` 才会注入这些字段。合成字段只
/// 在 all 分支注入, 求值会读到空 → 输出失真, 门控拒绝。close/match 门控共用
/// （层 2 收口）。
pub(crate) fn yield_general_columnar_safe(expr: &Expr) -> bool {
    match expr {
        Expr::Field(fr) => {
            let n = field_ref_name(fr);
            !n.is_empty() && !n.starts_with('_') && !matches!(fr, FieldRef::Path { .. })
        }
        Expr::BinOp { left, right, .. } => {
            yield_general_columnar_safe(left) && yield_general_columnar_safe(right)
        }
        Expr::Neg(inner) | Expr::Not(inner) => yield_general_columnar_safe(inner),
        Expr::Array(items) => items.iter().all(yield_general_columnar_safe),
        Expr::InList {
            expr: inner, list, ..
        } => yield_general_columnar_safe(inner) && list.iter().all(yield_general_columnar_safe),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            yield_general_columnar_safe(cond)
                && yield_general_columnar_safe(then_expr)
                && yield_general_columnar_safe(else_expr)
        }
        Expr::Object(items) => items
            .iter()
            .all(|it| yield_general_columnar_safe(&it.value)),
        Expr::FuncCall { args, .. } => args.iter().all(yield_general_columnar_safe),
        // Number/StringLit/Bool/SystemVar/WfuMeta/PresetParam: 读字面量/
        // YieldMeta/参数体, 无 ctx 字段访问。
        _ => true,
    }
}

fn coerce_yield_value(
    name: &str,
    field_type: &FieldType,
    value: Value,
) -> CoreResult<Option<Value>> {
    // A yield expression referencing a missing input field evaluates to the
    // empty-string fallback (see `eval_yield_expr_with_meta`). For targets that
    // can never be a valid empty string, treat it as an absent/optional field:
    // omit it from the output instead of failing the whole record
    // (wp-labs/warp-fusion#62). Explicit NaN/Infinity values still fail below.
    if matches!(&value, Value::Str(s) if s.is_empty())
        && !matches!(field_type, FieldType::Base(BaseType::Chars))
    {
        return Ok(None);
    }
    match field_type {
        FieldType::Base(base_type) => coerce_yield_base_value(name, base_type, value).map(Some),
        FieldType::Array(_) | FieldType::ArrayAny => match value {
            Value::Array(_) => Ok(Some(value)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an array value"))
                .err(),
        },
        FieldType::Object => match value {
            Value::Object(_) => Ok(Some(value)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an object value"))
                .err(),
        },
    }
}

fn coerce_yield_base_value(name: &str, base_type: &BaseType, value: Value) -> CoreResult<Value> {
    match base_type {
        BaseType::Chars => match value {
            // Already a string: pass through without re-rendering — saves one
            // `String` allocation per cell on the hot path and is
            // byte-identical to `render_yield_value_as_string`.
            Value::Str(_) => Ok(value),
            _ => render_yield_value_as_string(value).map(|s| Value::Str(s.into())),
        },
        BaseType::Digit => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 => Ok(Value::Number(n)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} expects an integer-compatible number"
                ))
                .err(),
        },
        BaseType::Float => match value {
            Value::Number(n) if n.is_finite() => Ok(Value::Number(n)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects a finite number"))
                .err(),
        },
        BaseType::Bool => match value {
            Value::Bool(_) => Ok(value),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects a boolean value"))
                .err(),
        },
        BaseType::Time => coerce_yield_time_value(name, value),
        BaseType::Ip => match value {
            Value::Str(text) => {
                IpAddr::from_str(&text).source_raw_err(
                    CoreReason::DataFormat,
                    format!("yield field {name:?} has invalid ip literal {text:?}"),
                )?;
                Ok(Value::Str(text))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an ip string"))
                .err(),
        },
        BaseType::Hex => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 && n >= 0.0 => {
                Ok(Value::Number(n))
            }
            Value::Str(text) => {
                let normalized = text
                    .strip_prefix("0x")
                    .or_else(|| text.strip_prefix("0X"))
                    .unwrap_or(&text);
                u128::from_str_radix(normalized, 16).source_raw_err(
                    CoreReason::DataFormat,
                    format!("yield field {name:?} has invalid hex literal {text:?}"),
                )?;
                Ok(Value::Str(text))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} expects a hex string or non-negative integer"
                ))
                .err(),
        },
    }
}

fn coerce_yield_time_value(name: &str, value: Value) -> CoreResult<Value> {
    match value {
        Value::Number(n) => {
            normalize_epoch_timestamp_float_nanos(n).ok_or_else(|| {
                orion_error::StructError::from(CoreReason::DataFormat).with_detail(format!(
                    "yield field {name:?} expects a valid epoch timestamp"
                ))
            })?;
            Ok(Value::Number(n))
        }
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail(format!(
                "yield field {name:?} expects an explicit time expression or epoch timestamp"
            ))
            .err(),
    }
}

fn render_yield_value_as_string(value: Value) -> CoreResult<String> {
    match value {
        Value::Str(s) => Ok(s.to_string()),
        Value::Number(n) if n.is_finite() => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(&yield_value_to_json(&value)?)
            .source_raw_err(CoreReason::DataFormat, "serialize structured yield value"),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("yield string conversion requires finite numeric values")
            .err(),
    }
}

fn yield_value_to_json(value: &Value) -> CoreResult<serde_json::Value> {
    match value {
        Value::Number(n) if n.is_finite() => Ok(serde_json::Value::from(*n)),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
        Value::Str(s) => Ok(serde_json::Value::from(s.as_str())),
        Value::Bool(b) => Ok(serde_json::Value::from(*b)),
        Value::Array(items) => Ok(serde_json::Value::Array(
            items
                .iter()
                .map(yield_value_to_json)
                .collect::<CoreResult<Vec<_>>>()?,
        )),
        Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), yield_value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

fn passes_bind_filter(
    filter: Option<&Expr>,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
) -> bool {
    match filter.and_then(|expr| eval_bool_expr_with_lookup(expr, event, windows)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}
