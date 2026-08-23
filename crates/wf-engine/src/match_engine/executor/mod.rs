mod alert;
#[cfg(test)]
pub(crate) use alert::{EachWfxPrefix, format_nanos_utc};
use each_exec::parse_each_join_columnar;
mod close_exec;
mod context;
#[cfg(test)]
pub(crate) use context::{build_eval_context, execute_joins};
mod deferred_exec;
pub use deferred_exec::DeferredPending;
mod each_exec;
mod eval;
mod match_exec;
mod stats_exec;

pub use each_exec::EachDirectBatchStats;
pub use stats_exec::{DistinctKey, StatsAccum, StatsExecutor, StatsWindowState};

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
use crate::match_engine::columnar::{ColumnarBatch, GuardMasks, eval_guard_columnar};
use crate::match_engine::match_engine::{Event, FieldSource, Value, WindowLookup, field_ref_name};
use crate::time::normalize_epoch_timestamp_float_nanos;
use arrow::array::BooleanArray;
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
        Expr::FuncCall { .. } | Expr::PresetParam(_) => *force_all = true,
        Expr::BinOp { left, right, .. } => {
            visit_expr_fields(left, names, force_all);
            visit_expr_fields(right, names, force_all);
        }
        Expr::Neg(inner) => visit_expr_fields(inner, names, force_all),
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
        let each_join_plan = parse_each_join_columnar(&plan);
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
            },
            plan,
            each_join_plan,
            live_joins,
            yield_field_types: options.yield_field_types,
            output: options.output,
            bind_filters,
            emit_time_cache: Mutex::new((0, Arc::from(""))),
            close_ctx_fields,
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

    pub(crate) fn build_machine_id(&self, machine_id: &str) -> String {
        if machine_id.is_empty() {
            self.plan.name.clone()
        } else {
            machine_id.to_string()
        }
    }

    pub(crate) fn build_scope_key(
        &self,
        keys: &[wf_lang::ast::FieldRef],
        scope_values: &[crate::match_engine::match_engine::Value],
    ) -> String {
        keys.iter()
            .zip(scope_values.iter())
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    crate::match_engine::match_engine::field_ref_name(k),
                    crate::match_engine::match_engine::value_to_string(v)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
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
        Some(eval_guard_columnar(filter, &view))
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
                    .is_none_or(|f| wf_lang::columnar::expr_is_columnar(f))
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
        Some(eval_guard_columnar(filter, &view))
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
                    masks.insert_event(step_idx, branch_idx, eval_guard_columnar(guard, &view));
                }
            }
        }
        for (step_idx, step) in self.plan.match_plan.close_steps.iter().enumerate() {
            for (branch_idx, branch) in step.branches.iter().enumerate() {
                if let Some(guard) = &branch.guard
                    && wf_lang::columnar::expr_is_columnar(guard)
                {
                    masks.insert_close(step_idx, branch_idx, eval_guard_columnar(guard, &view));
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
                        masks.insert_neg(neg_idx, 0, eval_guard_columnar(guard, &view));
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
