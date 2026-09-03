use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::ast::{
    CloseMode, CmpOp, Expr, FieldRef, FieldSelector, JoinMode, MatchMode, Measure, ReduceClause,
    Transform, WithinSpec,
};

// ---------------------------------------------------------------------------
// ExprPlan — L1 alias for ast::Expr
// ---------------------------------------------------------------------------

/// Expression in the execution plan.
///
/// For L1 this is a zero-cost alias of `ast::Expr`. When L2/L3 introduces
/// expression lowering (e.g. resolving field refs, inlining conv lookups),
/// this will become a distinct type.
pub type ExprPlan = Expr;

// ---------------------------------------------------------------------------
// RulePlan — top-level compiled rule
// ---------------------------------------------------------------------------

/// Compiled rule — the executable representation consumed by MatchEngine.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct RulePlan {
    pub name: String,
    pub binds: Vec<BindPlan>,
    /// Per-event `let` bindings: evaluated once per event (on-each path), the
    /// bound value injected into the event's field map under `name`.
    pub lets: Vec<LetPlan>,
    pub match_plan: MatchPlan,
    pub each_plan: Option<EachPlan>,
    /// 声明式窗口统计计划（stats 形态, 与 match_plan 互斥——规则体二选一）。
    pub stats_plan: Option<StatsPlan>,
    pub joins: Vec<JoinPlan>,
    /// `where <expr>` — post-join filter (strict: false/None suppresses output).
    pub r#where: Option<ExprPlan>,
    pub entity_plan: EntityPlan,
    pub yield_plan: YieldPlan,
    pub score_plan: ScorePlan,
    pub pattern_origin: Option<PatternOriginPlan>,
    pub conv_plan: Option<ConvPlan>,
    pub limits_plan: Option<LimitsPlan>,
    /// P2c: when a fixed-window rule carries `conv`, the compiler auto-generates
    /// a conv aggregation window + conv stage. `Some` only for such rules; the
    /// rule is then shardable and closes are aggregated cross-shard. Sliding /
    /// session conv rules keep `None` and stay on the legacy inline path.
    pub conv_window: Option<ConvWindowPlan>,
}

/// One compiled `let <name> = <expr>` binding (on-each per-event evaluation).
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct LetPlan {
    pub name: String,
    pub expr: ExprPlan,
}

/// Auto-generated conv aggregation descriptor (P2c).
///
/// Marks a conv rule as shardable and carries the runtime bucketing parameters
/// for the conv stage. `over` = **seal length** (bucket start + over must be
/// passed by every shard's watermark before the bucket is sealed); `keys` =
/// scope keys. (The runtime aggregates closes inside the conv stage — a
/// dedicated aggregation window is not materialized.)
///
/// Bucket alignment differs by window shape (2026-08-24 hop extension):
/// - `slide == None`（fixed）：bucket 对齐 = `over`（每 over 一个桶，现状）；
/// - `slide == Some(slide)`（hop）：bucket 对齐 = `slide`（每 slide 一个桶，
///   实例在 window_start + size 收口），`over` = size（封口长度）。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct ConvWindowPlan {
    /// Seal length：bucket 封口需要 `bucket + over <= min(barrier)`。
    /// fixed = 窗口时长；hop = size。
    pub over: Duration,
    /// Bucket alignment：`None`（fixed）= `over`；`Some(slide)`（hop）= slide。
    pub slide: Option<Duration>,
    /// Scope-key fields (same as `MatchPlan.keys`).
    pub keys: Vec<FieldRef>,
}

/// Stateless per-event trigger: `on each alias [where expr] -> score(...)`.
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct EachPlan {
    pub alias: String,
    pub filter: Option<ExprPlan>,
}

// ---------------------------------------------------------------------------
// PatternOriginPlan — tracks pattern origin for explain
// ---------------------------------------------------------------------------

/// Tracks the pattern origin for `wf explain` display.
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct PatternOriginPlan {
    pub pattern_name: String,
    pub args: Vec<String>,
}

// ---------------------------------------------------------------------------
// BindPlan — event source binding
// ---------------------------------------------------------------------------

/// A bound event source: alias + window + optional filter.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct BindPlan {
    pub alias: String,
    pub window: String,
    pub filter: Option<ExprPlan>,
}

// ---------------------------------------------------------------------------
// MatchPlan — temporal matching
// ---------------------------------------------------------------------------

/// The match plan: keys, window spec, event steps, close steps, key mapping, and close mode.
///
/// 构造约定（2026-09 P3）：本类型已实现 `Default`（空计划占位）。**新增字段时**
/// 给**空安全默认类型**（Vec/Option/bool/…），既有构造点（引擎测试 / wfgen
/// datagen/oracle，两仓库 60+ 字面量）即可免于逐点改——需要全字段字面量的地方
/// 在加字段当次按需更新即可。注意：clippy `needless_struct_update` 禁止
/// 「全字段字面量 + `..Default::default()`」写法，勿用该模式规避。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Default)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct MatchPlan {
    pub keys: Vec<FieldRef>,
    /// 派生 key 表达式槽（issue #80）：与 `keys` 逐位对齐——`Some(expr)` 表示
    /// 该 key 不是事件字段，而是要对**触发事件**求值的派生表达式（`let` 定义
    /// 编译期展开为纯事件字段表达式）。`None` = 普通字段/嵌套路径 key（按
    /// `keys[i]` 从事件提取）。
    ///
    /// 与 #83 纯字段 let key（编译期内联成等值 FieldRef、此处为 None）不同，
    /// 函数/字面量派生 let 无法内联成 FieldRef，故保留 `keys[i] = Simple(let 名)`
    /// 作逻辑名（ctx 注入/输出/摘要按此名配对 scope_key 值），引擎按本槽求值。
    pub key_exprs: Vec<Option<ExprPlan>>,
    pub key_map: Option<Vec<KeyMapPlan>>,
    /// join-then-key (Path A): the match key comes from a snapshot join's right
    /// window (e.g. `match<category:10m>` where `category` is on auction_events
    /// and the driver is bid). `None` = all keys are read from the driver event.
    pub key_join: Option<JoinKeyPlan>,
    pub window_spec: WindowSpec,
    pub event_steps: Vec<StepPlan>,
    pub close_steps: Vec<StepPlan>,
    pub close_mode: CloseMode,
    pub tracked_bind_aliases: HashSet<String>,
    pub tracked_bind_fields: HashMap<String, HashSet<String>>,
    pub tracked_plain_fields: HashSet<String>,
    /// Ordering mode of the `on event` block (`Seq` ordered, `Any` unordered).
    pub match_mode: MatchMode,
    /// Ordered sequence constraints (`on event seq { ... }`). When present, event/close steps are empty.
    pub seq: Option<SeqPlan>,
    /// `on event<accu>` — within-window accumulation: after firing, count and
    /// evidence keep accumulating without reset and each subsequent qualifying
    /// event re-fires with the running cumulative values until the window expires.
    pub accu: bool,
    /// Whether any alias needs the per-field value **history** (`field_values`).
    ///
    /// True when a close step fires (no triggering event to read fields from),
    /// the rule binds multiple events (yield may read a non-trigger alias's
    /// field), joins are present, or a yield/score/entity expression uses an L3
    /// series function (collect_set/list, first/last, stddev, percentile) that
    /// reads the `_step_field` array. False for single-bind on-event rules whose
    /// yield reads scalar fields — those read the scalar from the triggering
    /// event instead, so `collect_alias_event` can be skipped entirely (avoids
    /// per-instance field_values allocation under churn → RSS growth).
    pub needs_field_history: bool,
    /// Whether on-event fires need the triggering event materialized
    /// (`MatchedContext.trigger_event`). `false` = score/entity/yield + join
    /// condition left fields + `where` read only match keys (served from
    /// `scope_key`) or literals — the fire path can skip `event.to_event()`
    /// per-event clone (Q5/Q7/Q12/Q13 every-event-fire hot path, 2026-08).
    pub trigger_event_needed: bool,
}

// ---------------------------------------------------------------------------
// StatsPlan — 声明式窗口统计（stats 形态, 与 MatchPlan 平级）
// ---------------------------------------------------------------------------

/// Compiled stats rule — 桶键表达式 + 度量 + 输出形状, 由 StatsExecutor 消费。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct StatsPlan {
    pub window_spec: WindowSpec,
    /// 桶键表达式列表（group by + tier 桶键函数统一; 空 = 空键全局）。
    pub keys: Vec<ExprPlan>,
    /// 输出形状: 行展开（每桶一行）或列展开（每桶一列, pivot 转置）。
    pub output_shape: StatsOutputShapePlan,
    pub measures: Vec<StatsMeasurePlan>,
    /// 物化字段投影（同 MatchPlan.tracked_bind_fields 语义）。
    pub tracked_bind_fields: HashMap<String, HashSet<String>>,
}

/// 输出形状（对应 ast::StatsOutputShape）。
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum StatsOutputShapePlan {
    Rows,
    Columns,
}

/// 编译后的统计度量。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct StatsMeasurePlan {
    pub label: String,
    pub source_alias: String,
    pub where_expr: Option<ExprPlan>,
    pub agg: StatsAggPlan,
    pub field: Option<FieldRef>,
    pub arg: Option<u64>,
}

/// 统计聚合函数（对应 ast::StatsAgg）。
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum StatsAggPlan {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    DistinctCount,
    Last,
    Top,
}

/// Explicit key mapping entry: logical name → source alias + field.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct KeyMapPlan {
    pub logical_name: String,
    pub source_alias: String,
    pub source_field: String,
}

/// join-then-key descriptor: the match key's value is resolved by looking the
/// event's join-left key up in the join's right window, then reading
/// `right_field` from the joined row (e.g. bid.auction → auction_events.id →
/// auction_events.category). Carries everything the runtime needs so the state
/// machine can do the lookup without reaching into `RulePlan.joins`.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct JoinKeyPlan {
    /// Index into `RulePlan.joins` whose right window provides the key value
    /// (kept for explain / match-time join enrichment reuse).
    pub join_idx: usize,
    /// Right window name — the join target that holds the key field.
    pub right_window: String,
    /// Driver-side join key field (e.g. `b.auction`) — extracted from the
    /// event to drive `WindowLookup::join_lookup`.
    pub left_field: FieldRef,
    /// Join condition's right-side key field (e.g. `auction_events.id`) — the
    /// lookup index key on the right window.
    pub right_key_field: String,
    /// Right-row field read as the window key (e.g. `category`).
    pub right_field: String,
    /// RESERVED — logical key name (defaults to `right_field`). No consumer in
    /// the engine today (v1 forbids key_mapping, so the logical name is always
    /// the right field); kept for moju/explain compatibility. Do not rely on it.
    pub key_name: String,
}

/// Window specification for the match clause.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum WindowSpec {
    /// Sliding window with a fixed duration.
    Sliding(Duration),
    /// Fixed window with a fixed duration (non-overlapping buckets).
    Fixed(Duration),
    /// Session window with gap duration (L3 behavior analysis).
    Session(Duration),
    /// HOP sliding window: `size` window duration advancing every `slide`
    /// (size % slide == 0). Each event belongs to `size/slide` overlapping
    /// windows aligned to epoch slide boundaries.
    Hop { size: Duration, slide: Duration },
}

// 构造占位默认（Duration::ZERO 无窗口语义）：仅供测试/扩展用 Default；
// 真实运行必须显式给 window_spec。tuple variant 无法用 #[default]，手写。
impl Default for WindowSpec {
    fn default() -> Self {
        WindowSpec::Sliding(Duration::ZERO)
    }
}

/// One match step containing one or more OR branches.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct StepPlan {
    pub branches: Vec<BranchPlan>,
}

/// A single branch within a match step.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct BranchPlan {
    pub label: Option<String>,
    pub source: String,
    pub field: Option<FieldSelector>,
    pub guard: Option<ExprPlan>,
    pub agg: AggPlan,
}

/// Aggregation pipeline: transforms → measure → cmp → threshold.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct AggPlan {
    pub transforms: Vec<Transform>,
    pub measure: Measure,
    pub cmp: CmpOp,
    pub threshold: ExprPlan,
}

// ---------------------------------------------------------------------------
// SeqPlan — ordered sequence matching (L1/L2)
// ---------------------------------------------------------------------------

/// Ordered sequence plan: steps complete in order within the match window.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct SeqPlan {
    /// `consec` — strict adjacency; default: gap.
    pub consec: bool,
    /// After-match skip policy.
    pub skip: SeqSkipPlan,
    /// Ordered steps.
    pub steps: Vec<SeqStepPlan>,
}

/// After-match skip policy.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum SeqSkipPlan {
    /// Reset all step state after firing (default).
    PastLast,
    /// Keep non-first steps for overlapping matches (L3).
    ToNext,
}

/// One ordered chain step: `[not] <branch> [within]`.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct SeqStepPlan {
    /// `not` negation prefix.
    pub neg: bool,
    /// Time gap relative to the previous step's completion.
    pub within: Option<Duration>,
    /// Step body. For `has <alias>` steps, `agg` is `count >= 1`.
    pub branch: BranchPlan,
}

// ---------------------------------------------------------------------------
// JoinPlan — cross-source joins
// ---------------------------------------------------------------------------

/// Cross-source join plan.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct JoinPlan {
    pub right_window: String,
    pub mode: JoinMode,
    pub conds: Vec<JoinCondPlan>,
    /// `within` 时间区间谓词（P1 语法/计划；P2/P3 执行）。
    pub within: Option<WithinSpec>,
    /// `reduce` 归约 + `as label`（P1 语法/计划；P3 执行）。
    pub reduce: Option<ReduceClause>,
    /// `emit at` deferred 触发点（P1 语法/计划；P3 执行）。
    pub emit_at: Option<ExprPlan>,
}

/// A single join condition: left field == right field.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct JoinCondPlan {
    pub left: FieldRef,
    pub right: FieldRef,
}

impl JoinCondPlan {
    /// The right-side (target-window) key field name, if it is a flat
    /// (non-nested) reference. Join keys are validated as flat scalars by the
    /// checker, so this is `Some` for compiled joins.
    pub fn right_field_name(&self) -> Option<&str> {
        match &self.right {
            FieldRef::Simple(f) | FieldRef::Qualified(_, f) | FieldRef::Bracketed(_, f) => {
                Some(f.as_str())
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// LimitsPlan — resource budget enforcement
// ---------------------------------------------------------------------------

/// Compiled limits for runtime enforcement.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct LimitsPlan {
    pub max_memory_bytes: Option<usize>,
    pub max_instances: Option<usize>,
    pub max_throttle: Option<RateSpec>,
    pub on_exceed: ExceedAction,
    /// 磁盘提供者（状态落盘后端, 2026-08-27 改名自 `spill`, 键 `disk_provider`;
    /// 旧键 `spill` 保留为兼容别名）:
    /// None = 不落盘（默认, 超 `max_memory` 拒收新键）; Redb = 状态落盘、
    /// 内存只留活跃子集（`docs/design/stats-state-spill-redb.md`）。
    pub disk_provider: Option<SpillMode>,
    /// 规则级磁盘占用上限（2026-08-27 改名自 `max_spill_bytes`, 键 `max_disk`;
    /// 旧键保留为兼容别名）。用户配的是规则总量（分片数是引擎内部细节）。
    pub max_disk_bytes: Option<usize>,
}

/// 状态外溢存储模式。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum SpillMode {
    /// redb 持久化（B+ 树单文件库）。
    Redb,
}

/// What to do when a limit is exceeded.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum ExceedAction {
    Throttle,
    DropOldest,
    FailRule,
}

/// Emit rate specification: count per duration.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct RateSpec {
    pub count: u64,
    pub per: Duration,
}

// ---------------------------------------------------------------------------
// EntityPlan
// ---------------------------------------------------------------------------

/// Entity identification: lowercase-normalized type string + id expression.
///
/// Both `entity(IP, ...)` and `entity("ip", ...)` compile to `entity_type = "ip"`.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct EntityPlan {
    pub entity_type: String,
    pub entity_id_expr: ExprPlan,
}

// ---------------------------------------------------------------------------
// ScorePlan
// ---------------------------------------------------------------------------

/// Score computation expression.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct ScorePlan {
    pub expr: ExprPlan,
}

// ---------------------------------------------------------------------------
// YieldPlan
// ---------------------------------------------------------------------------

/// Output yield: target window + optional version + fields.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct YieldPlan {
    pub target: String,
    pub version: Option<u32>,
    pub fields: Vec<YieldField>,
}

/// A single yield field: name = expression.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct YieldField {
    pub name: String,
    pub value: ExprPlan,
}

// ---------------------------------------------------------------------------
// ConvPlan — result set transformations for fixed windows (L3)
// ---------------------------------------------------------------------------

/// Compiled conv plan — post-close result set transformations.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct ConvPlan {
    pub chains: Vec<ConvChainPlan>,
}

/// One semicolon-separated chain of piped operations.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct ConvChainPlan {
    pub ops: Vec<ConvOpPlan>,
}

/// A single conv operation.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum ConvOpPlan {
    Sort(Vec<SortKeyPlan>),
    Top(u64),
    /// RANK 语义 top-N：前 N 条 + 与第 N 条排序键等值的全部条目。
    /// `sort_keys` 为前导 sort 的键（编译期复制；空 = 退化为普通 top）。
    TopTies {
        n: u64,
        sort_keys: Vec<SortKeyPlan>,
    },
    Dedup(ExprPlan),
    Where(ExprPlan),
}

/// Sort key with direction.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct SortKeyPlan {
    pub expr: ExprPlan,
    pub descending: bool,
}
