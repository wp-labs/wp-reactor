mod alert;
#[cfg(test)]
pub(crate) use alert::{EachWfxPrefix, format_nanos_utc};
mod close_exec;
mod context;
#[cfg(test)]
pub(crate) use context::{build_eval_context, execute_joins};
#[cfg(test)]
pub(crate) use context::{enrich_join_row, enrich_join_row_bare, in_interval, row_matches_conds};
#[cfg(test)]
pub(crate) use deferred_exec::select_reduce_row;
mod deferred_exec;
pub use deferred_exec::{DeferredLeft, DeferredPending};
mod each_exec;
mod eval;
mod execution_path;
pub use execution_path::{ExecutionPath, ExecutionPathContext};
mod match_exec;
mod plan_analysis;
mod rule_exec;
mod stats_exec;

pub use each_exec::{EachDirectBatchStats, PipeEachRow, PipeRowSink};
// 供 `match_engine::pub use executor::DistinctKey` 转发（stats distinct 键类型）。
pub use stats_exec::{
    DistinctKey, DistinctSet, StatsAccum, StatsBucketAccs, StatsExecutor, StatsMaskCache,
    StatsWindowState,
};
// RowFields/RowFieldLayout 纯值类型已下沉 wf-cep::rows（P4-A 片 3）。
pub use wf_cep::rows::{RowFieldLayout, RowFields};
// 仅 crate 内消费（spill 序列化 / stats 测试），不构成对外契约。
pub(crate) use stats_exec::{NumericAccum, StatsCloseBucket, TopEntry};

// 供 crate 内 SoA 对照 bench（tests/）访问私有热路径函数。
#[cfg(test)]
pub(crate) use stats_exec::{
    NumericSoALayout, StatsBucket, accumulate_column_row, accumulate_soa, comps_hash,
    measure_values_soa,
};
// spill 序列化用（同 crate：executor::stats_exec 为私有模块，需经此转发）。
pub(crate) use stats_exec::scope_key_hash;
// 拆件后子模块面：`match_exec` 行式列式门控经 `super::yield_general_columnar_safe`
// 引用（定义于 rule_exec，转发保持 executor 层名）；`plan_close_ctx_fields` 被
// executor 测试经 crate::match_engine::executor 路径引用（cfg(test) 下转发）。
#[cfg(test)]
pub(crate) use plan_analysis::plan_close_ctx_fields;
pub(crate) use rule_exec::yield_general_columnar_safe;

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
use std::sync::{Arc, Mutex};

use wf_config::OutputConfig;
use wf_lang::FieldType;
use wf_lang::ast::Expr;
use wf_lang::plan::{JoinPlan, RulePlan};

use crate::match_engine::cep::Value;
use crate::match_engine::columnar::ColumnExpr;

pub(crate) use self::context::CloseCtxFields;

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

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`OutputRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.RuleExecution")]
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
    /// M1（P4 终态机制，2026-09-02）：规则级 fire 投影——match 行 ColumnarEvent
    /// 的 `to_event` 只物化本规则输出 ctx 实际读取的字段（= `close_ctx_fields`
    /// 的 Named 集；ctx 只读该集，字节一致由构造保证）。消除窗口并集/全列
    /// 物化里的结构化列 JSON 解析（qradar 40% trigger fires 每发都解析
    /// conn_info/tags 的问题）。`None` = 非 match / All（无法静态窄化）→ 回退
    /// 窗口 materialize_fields（现状）。
    fire_trigger_projection: Option<std::sync::Arc<std::collections::HashSet<String>>>,
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
            fire_trigger_projection: self.fire_trigger_projection.clone(),
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
