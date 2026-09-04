// 文件组织（hub）：`CepStateMachine` 结构面/构造/共享簿记留在本文件，事件
// 推进编排在 `advance.rs`，单窗实例推进在 `window.rs`，到期收口在 `expiry.rs`；
// 叶子实现在 close/conv/eval/key/limits/seq/state/step/types。
mod advance;
mod close;
mod conv;
pub mod eval; // 2026-09-04 P4-B1：engine 剩码经 shim 跨 crate 消费

mod expiry;
mod join_then_key;
pub mod key; // 同上

mod limits;
mod seq;
mod state;
mod step;
mod types;
mod window;

// Re-export public types
pub use limits::SharedLimits;
pub use types::{
    AsofLookup, BindData, CloseOutput, CloseReason, Event, FieldSource, JoinKey, MACHINE_ID,
    MatchedContext, StepData, StepOutcome, StepProgress, StepResult, Value, WindowLookup,
};
pub use types::{EngineHashMap, EngineHashSet};

// Re-export pub(crate) items
pub use eval::eval_expr;
pub use eval::values_equal;
pub use key::{ScopeKey, field_ref_name};
#[allow(unused_imports)] // key.rs 内部用全限定路径；重导出由 executor::eval 等模块消费
pub use key::{
    eval_field_value, eval_field_value_src, extract_key_simple, extract_scope_key_from_row,
    extract_scope_key_mixed, push_i64_exact_decimal, scope_key_from_values, scope_key_shard_index,
    value_to_string,
};

pub use conv::apply_conv;
pub use join_then_key::precompute_join_then_keys;

pub use eval::eval_expr_ext;

// Test-only re-exports: the `tests` sibling module sits outside `match_engine`,
// so private submodules (close/key/state/types) are not directly reachable.
// Benchmarks measure the production hot path without widening production APIs.
// engine 剩测/bench（close_bench 等）跨 crate 消费——常驻 pub（doc hidden 语义）。
pub use key::ValueKey;
pub use state::StepState;
pub use types::RollingStats;

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use wf_lang::ast::CloseMode;
use wf_lang::plan::{ConvPlan, JoinKeyPlan, LimitsPlan, MatchPlan, RateSpec, WindowSpec};

pub use close::accumulate_close_steps;
use key::InstanceKey;
use seq::SeqRuntime;
use state::Instance;

// ---------------------------------------------------------------------------
// CepStateMachine — public API
// ---------------------------------------------------------------------------

/// Runtime CEP state machine that drives `match<key:dur>` execution.
///
/// Consumes a [`MatchPlan`] (produced by the M13 compiler) and processes
/// events one-at-a-time via [`advance`](Self::advance). Maintains per-key
/// state machine instances that advance through sequential steps with
/// OR-branch semantics and aggregation pipelines.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct CepStateMachine {
    rule_name: String,
    plan: MatchPlan,
    instances: EngineHashMap<InstanceKey, Instance>,
    time_field: Option<String>,
    watermark_nanos: i64,
    limits: Option<LimitsPlan>,
    /// Set to true when `FailRule` limit is exceeded — all future events are
    /// rejected until the machine is reset.
    failed: bool,
    emit_count: u64,
    emit_window_start: i64,
    /// Shared rate-limit / budget atomics across the rule's shards (P2b).
    ///
    /// `Some` only when the machine is a shard of a sharded rule with limits;
    /// the per-machine `emit_count`/`estimated_memory_bytes` fields are then
    /// unused in favor of the shared atomics.
    shared: Option<std::sync::Arc<SharedLimits>>,
    /// When true, close output bypasses inline conv/throttle and is emitted raw
    /// to the conv aggregation window (P2c). Only set on shards of a conv rule.
    raw_conv_mode: bool,
    /// Expiry candidates ordered by `(expire_time, instance_key)`.
    ///
    /// Stale candidates are filtered out when popped by checking the current
    /// instance state in `self.instances`.
    expiry_heap: BinaryHeap<Reverse<(i64, InstanceKey)>>,
    /// Keys with a pending expiry candidate. Prevents per-event/reset
    /// `push_expiry_candidate` from stacking duplicate heap entries (the
    /// dominant leak on high-fire rules like pass-through count).
    pending_expiry: EngineHashSet<InstanceKey>,
    /// Cached estimated memory across active instances.
    ///
    /// This keeps `limits.max_memory` checks O(1) for the common path instead
    /// of re-summing every instance for every incoming event.
    estimated_memory_bytes: usize,
    /// Whether per-event instance state can grow (distinct set / field history /
    /// seq negation). `false` = instance memory only changes on insert/remove
    /// (both accounted exactly), so the per-event `max_memory` enforcement check
    /// can be skipped for non-new events (2026-08-31: qradar/真实规则全部带
    /// limits，逐事件检查是纯浪费——摊还到新实例准入 + 可增长规则)。
    memory_grows_per_event: bool,
    /// Chain semantics (`within` / `not` / `consec`) precomputed from the plan.
    seq_meta: Option<SeqRuntime>,
    /// 引擎处理墙钟（issue #82，`@first_match_time`）：由驱动方（rule_task 每批/
    /// 每次扫收口前）通过 [`Self::set_processing_wall`] 注入，首次命中实例时记入
    /// `Instance::first_hit_wall_nanos`。None = 未注入（单测/测试驱动）→
    /// `@first_match_time` 无值，与 `@emit_time` 未提供时行为一致。
    processing_wall_nanos: Option<i64>,
}

/// 规则实例状态是否**每事件增长**（`estimated_memory_bytes` 只在 insert/remove
/// 精确记账，增长类状态靠周期 recalibrate 修正后由逐事件检查执行驱逐）：
/// - 多步 AND/Any：`completed_steps` 随步骤完成累积；
/// - close 步骤（baselines 等累积状态）；
/// - `accu`（跨发射累积）；
/// - 字段历史（`needs_field_history` → collected_values / alias_state 写入）；
/// - 序列否定（`seq` 否定窗口累积命中）；
/// - Distinct 度量（`distinct_set` 无上限累积）。
///
/// 纯单步 count/sum/min/max/avg 单 bind 规则（无上述任何来源）：实例内存只在
/// insert/remove 变化 → 逐事件 `max_memory` 检查纯冗余（2026-08-31 摊还；
/// qradar c/g/s 家族与真实单步计数规则命中此路径）。
fn plan_memory_grows_per_event(plan: &MatchPlan) -> bool {
    if plan.needs_field_history
        || plan.seq.is_some()
        || plan.accu
        || plan.event_steps.len() > 1
        || !plan.close_steps.is_empty()
    {
        return true;
    }
    plan.event_steps.iter().any(|step| {
        step.branches.iter().any(|b| {
            b.agg
                .transforms
                .iter()
                .any(|t| matches!(t, wf_lang::ast::Transform::Distinct))
        })
    })
}

impl CepStateMachine {
    /// Create a new state machine for the given rule + plan.
    pub fn new(rule_name: String, plan: MatchPlan, time_field: Option<String>) -> Self {
        let seq_meta = plan
            .seq
            .as_ref()
            .map(|c| SeqRuntime::build(&c.steps, c.consec));
        let memory_grows_per_event = plan_memory_grows_per_event(&plan);
        Self {
            rule_name,
            plan,
            instances: EngineHashMap::default(),
            pending_expiry: EngineHashSet::default(),
            time_field,
            watermark_nanos: 0,
            limits: None,
            failed: false,
            emit_count: 0,
            emit_window_start: 0,
            shared: None,
            raw_conv_mode: false,
            expiry_heap: BinaryHeap::new(),
            estimated_memory_bytes: 0,
            memory_grows_per_event,
            seq_meta,
            processing_wall_nanos: None,
        }
    }

    /// Create a new state machine with limits enforcement.
    pub fn with_limits(
        rule_name: String,
        plan: MatchPlan,
        time_field: Option<String>,
        limits: Option<LimitsPlan>,
    ) -> Self {
        let seq_meta = plan
            .seq
            .as_ref()
            .map(|c| SeqRuntime::build(&c.steps, c.consec));
        let memory_grows_per_event = plan_memory_grows_per_event(&plan);
        Self {
            rule_name,
            plan,
            instances: EngineHashMap::default(),
            pending_expiry: EngineHashSet::default(),
            time_field,
            watermark_nanos: 0,
            limits,
            failed: false,
            emit_count: 0,
            emit_window_start: 0,
            shared: None,
            raw_conv_mode: false,
            expiry_heap: BinaryHeap::new(),
            estimated_memory_bytes: 0,
            memory_grows_per_event,
            seq_meta,
            processing_wall_nanos: None,
        }
    }

    /// Create a shard of a sharded rule with limits enforcement that shares
    /// rate-limit / budget atomics across all shards (P2b).
    pub fn with_limits_shared(
        rule_name: String,
        plan: MatchPlan,
        time_field: Option<String>,
        limits: Option<LimitsPlan>,
        shared: std::sync::Arc<SharedLimits>,
    ) -> Self {
        let seq_meta = plan
            .seq
            .as_ref()
            .map(|c| SeqRuntime::build(&c.steps, c.consec));
        let memory_grows_per_event = plan_memory_grows_per_event(&plan);
        Self {
            rule_name,
            plan,
            instances: EngineHashMap::default(),
            pending_expiry: EngineHashSet::default(),
            time_field,
            watermark_nanos: 0,
            limits,
            failed: false,
            emit_count: 0,
            emit_window_start: 0,
            shared: Some(shared),
            raw_conv_mode: false,
            expiry_heap: BinaryHeap::new(),
            estimated_memory_bytes: 0,
            memory_grows_per_event,
            seq_meta,
            processing_wall_nanos: None,
        }
    }

    /// 设置引擎处理墙钟（issue #82，`@first_match_time` 语义）——每次驱动批次/
    /// 扫收口前由驱动方（rule_task）调用，供实例首次完整命中时记录处理时钟。
    pub fn set_processing_wall(&mut self, wall_nanos: i64) {
        self.processing_wall_nanos = Some(wall_nanos);
    }

    /// Switch this shard to raw-conv mode (P2c): close output is emitted raw to
    /// the conv aggregation window instead of inline conv/throttle.
    pub fn set_raw_conv_mode(&mut self) {
        self.raw_conv_mode = true;
    }

    /// Whether this shard emits raw closes for the conv aggregation window.
    pub fn raw_conv_mode(&self) -> bool {
        self.raw_conv_mode
    }

    /// Returns the rule name this state machine was created for.
    pub fn rule_name(&self) -> &str {
        &self.rule_name
    }

    /// Extract event time from the event using the configured time_field.
    fn extract_event_time(&self, event: &Event) -> i64 {
        self.time_field
            .as_ref()
            .and_then(|tf| event.fields.get(tf.as_str()))
            .and_then(|v| match v {
                Value::Number(n) => Some(*n as i64),
                _ => None,
            })
            .unwrap_or(0)
    }

    /// Extract event time from the configured time field.
    ///
    /// Returns 0 if the field is absent or non-numeric.
    pub fn event_time_nanos(&self, event: &Event) -> i64 {
        self.extract_event_time(event)
    }

    /// The configured event-time field name, if any.
    pub fn time_field(&self) -> Option<&str> {
        self.time_field.as_deref()
    }

    /// Extract a string field from an event source, returning empty string if
    /// not found. Generic over [`FieldSource`] so the columnar path reads it
    /// straight from the batch.
    pub fn extract_event_str<E: FieldSource>(event: &E, field: &str) -> String {
        event.field_value_str(field)
    }

    /// Number of active per-key instances.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// Borrow the underlying plan.
    pub fn plan(&self) -> &MatchPlan {
        &self.plan
    }

    /// Current watermark (nanoseconds since epoch).
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos
    }

    fn remove_instance(&mut self, key: &InstanceKey) -> Option<Instance> {
        let instance = self.instances.remove(key)?;
        if self.tracks_memory_bytes() {
            self.estimated_memory_bytes = self
                .estimated_memory_bytes
                .saturating_sub(instance.base_cost);
            if let Some(shared) = &self.shared {
                shared.sub_memory(instance.base_cost);
            }
        }
        Some(instance)
    }

    /// Release this machine's shared instance slot after a *permanent* remove
    /// (close / expiry / eviction). The get-or-create remove in the advance
    /// path is temporary (re-inserted) and must NOT release — it never calls
    /// this.
    fn release_shared_instance(&self) {
        if let Some(shared) = &self.shared {
            shared.release_instance();
        }
    }

    /// Whether the per-instance memory accounting cache is needed.
    ///
    /// `estimated_memory_bytes` is only read when a `max_memory_bytes` limit is
    /// configured; maintaining it otherwise is pure per-event overhead.
    fn tracks_memory_bytes(&self) -> bool {
        self.limits
            .as_ref()
            .and_then(|limits| limits.max_memory_bytes)
            .is_some()
    }

    /// Recompute the exact per-instance memory estimate across all active
    /// instances.
    ///
    /// `insert/remove_instance` track only the fixed per-instance `base_cost`
    /// (O(1)), so the running estimate drifts below true usage as instances
    /// accumulate state (field_values, distinct_set, …). Calling this
    /// periodically (e.g. per timeout scan) re-anchors it to the exact sum.
    ///
    /// Skips the O(instances) sweep when the running estimate is already at or
    /// above `max_memory_bytes`: the exact value can only be larger (instances
    /// accumulate state, never shrink), so it cannot flip the throttle decision
    /// — the rule is already over budget and `Throttle`/`DropOldest` engage.
    /// Without this, a high-instance rule spends ~a second per `scan_timeouts`
    /// tick walking millions of instances (`estimated_bytes`), during which the
    /// rule task is not draining pushes — the 30 rule channels fill, the window
    /// broadcast blocks, the window byte budget exhausts, and the pipeline
    /// stalls (q5 100M: `budget starved` / freeze).
    pub fn recalibrate_memory(&mut self) {
        if !self.tracks_memory_bytes() {
            return;
        }
        let Some(limit) = self
            .limits
            .as_ref()
            .and_then(|limits| limits.max_memory_bytes)
        else {
            return;
        };
        let cur = self
            .shared
            .as_ref()
            .map(|s| s.memory_bytes())
            .unwrap_or(self.estimated_memory_bytes);
        if cur >= limit {
            return;
        }
        let exact: usize = self
            .instances
            .values()
            .map(|instance| instance.estimated_bytes())
            .sum();
        // P2b: adjust the shared total by this shard's recalibration delta, then
        // re-anchor the local cache.
        if let Some(shared) = &self.shared {
            shared.recalibrate_memory(self.estimated_memory_bytes, exact);
        }
        self.estimated_memory_bytes = exact;
    }

    /// Test-only estimate hook（2026-09-04 P4-B1：engine 剩测跨 crate 消费，常驻）。
    pub fn estimated_memory_bytes_for_test(&self) -> usize {
        self.estimated_memory_bytes
    }

    fn expire_time_for(window_spec: &WindowSpec, instance: &Instance) -> i64 {
        match window_spec {
            WindowSpec::Session(d) => instance.last_event_nanos + d.as_nanos() as i64,
            WindowSpec::Sliding(d) | WindowSpec::Fixed(d) => {
                instance.created_at + d.as_nanos() as i64
            }
            WindowSpec::Hop { size, .. } => instance.created_at + size.as_nanos() as i64,
        }
    }
}

/// Merge two per-window outcomes for HOP fan-out: the higher-priority result
/// wins (Matched > Advance > Accumulate); the first window's progress is kept.
fn merge_step_outcome(a: StepOutcome, b: StepOutcome) -> StepOutcome {
    let rank = |r: &StepResult| match r {
        StepResult::Matched(_) => 2,
        StepResult::Advance => 1,
        StepResult::Accumulate => 0,
    };
    if rank(&b.result) > rank(&a.result) {
        b
    } else {
        a
    }
}

fn should_track_bind_alias(plan: &MatchPlan, _alias: &str) -> bool {
    // Collect the per-field value *history* only when the rule needs it (close
    // steps, multi-bind, joins, or L3 series in yield/score/entity). A
    // single-bind on-event rule whose yield reads scalar fields resolves them
    // from the triggering event (`MatchedContext::trigger_event`) instead, so
    // skipping collection here avoids the per-instance field_values allocation
    // under churn that drove RSS unbounded on sustained inject.
    plan.needs_field_history
}

/// Resolve a join-then-key (Path A) scope key for one event: look the event's
/// join-left value up in the joined window, re-verify candidates with
/// `values_equal` (the index key truncates f64 — a fractional driver value
/// would otherwise false-match a truncated row), and read the key field off
/// the first matching row. `None` on any miss (no lookup, missing left field,
/// join miss, key absent) — the caller skips the event.
///
/// Shared by the per-event advance path and the batch pre-resolution helper
/// (`precompute_join_then_keys` in wf-runtime), which calls the same lookup so
/// both produce byte-identical scope keys.
fn resolve_key_join_scope_key<E: FieldSource>(
    kjp: &JoinKeyPlan,
    event: &E,
    windows: Option<&dyn WindowLookup>,
) -> Option<Vec<Value>> {
    let Some(windows) = windows else {
        return None; // no lookup → join miss
    };
    let Some(left_val) = event.field_value(field_ref_name(&kjp.left_field)) else {
        return None; // missing join-left key → skip
    };
    let Some(rows) = windows.join_lookup(&kjp.right_window, &kjp.right_key_field, &left_val) else {
        return None; // window not found → skip
    };
    let Some(row) = rows.iter().find(|r| {
        r.field_value(&kjp.right_key_field)
            .is_some_and(|rv| values_equal(&left_val, &rv))
    }) else {
        return None; // join miss → skip
    };
    let Some(key_val) = row.field_value(&kjp.right_field) else {
        return None; // key absent on joined row → skip
    };
    Some(vec![key_val])
}

fn step_outcome(result: StepResult, progress: Option<StepProgress>) -> StepOutcome {
    StepOutcome { result, progress }
}

// ---------------------------------------------------------------------------
// Conv helper — filter-then-transform
// ---------------------------------------------------------------------------

/// Filter close outputs to only qualifying entries, then apply conv.
///
/// Non-qualifying outputs (`!event_ok || !close_ok`) are separated first so
/// that `top`/`dedup`/`where` operate only on entries that would produce
/// alerts. The non-qualifying outputs are appended back (unchanged) so that
/// callers that iterate the full batch and call `execute_close` still see
/// them (they'll be harmlessly discarded by the `event_ok && close_ok`
/// check inside `execute_close`).
fn apply_conv_filtered(
    outputs: Vec<CloseOutput>,
    conv_plan: Option<&ConvPlan>,
    keys: &[wf_lang::ast::FieldRef],
) -> Vec<CloseOutput> {
    let conv = match conv_plan {
        Some(plan) => plan,
        None => return outputs,
    };

    let (qualifying, non_qualifying): (Vec<_>, Vec<_>) =
        outputs.into_iter().partition(close_is_qualified);

    if qualifying.is_empty() {
        return non_qualifying;
    }

    let mut result = conv::apply_conv(conv, keys, qualifying);
    result.extend(non_qualifying);
    result
}

/// Whether a close output qualifies to produce an alert.
///
/// Exposed for the P2c conv stage: shards emit raw closes and the conv stage
/// filters to qualifying ones before applying conv / emitting.
pub fn close_is_qualified(close: &CloseOutput) -> bool {
    match close.close_mode {
        CloseMode::And => close.event_ok && close.close_ok,
        CloseMode::Or => close.close_ok && !close.close_step_data.is_empty(),
    }
}

// ---------------------------------------------------------------------------
// Rate-limit / fail helpers (free functions)
//
// These take disjoint field refs (`&self.shared`, `&mut self.emit_count`, …)
// instead of `&mut self`, because the advance body holds `let plan = &self.plan`
// for its whole extent — a `&mut self` method call would conflict with that
// immutable borrow of `self`.
// ---------------------------------------------------------------------------

/// Returns `true` if this emit is within the rate budget.
///
/// Uses the shared throttle atomics when the machine is a shard of a sharded
/// rule (P2b), otherwise the legacy per-machine sliding window.
fn throttle_allows(
    shared: &Option<std::sync::Arc<SharedLimits>>,
    emit_count: &mut u64,
    emit_window_start: &mut i64,
    now_nanos: i64,
    rate: &RateSpec,
) -> bool {
    match shared {
        Some(shared) => shared.try_acquire_throttle(now_nanos, rate),
        None => {
            let window = rate.per.as_nanos() as i64;
            if now_nanos - *emit_window_start >= window {
                *emit_count = 0;
                *emit_window_start = now_nanos;
            }
            if *emit_count >= rate.count {
                return false;
            }
            *emit_count += 1;
            true
        }
    }
}

/// Latch the rule as failed (`FailRule`); propagates to shared state.
fn fail_rule(failed: &mut bool, shared: &Option<std::sync::Arc<SharedLimits>>) {
    *failed = true;
    if let Some(shared) = shared {
        shared.fail();
    }
}

#[cfg(test)]
mod tests;
