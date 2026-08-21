mod close;
mod conv;
mod eval;
mod key;
mod limits;
mod seq;
mod state;
mod step;
mod types;

// Re-export public types
pub use limits::SharedLimits;
pub use types::{
    AsofLookup, BindData, CloseOutput, CloseReason, Event, FieldSource, JoinKey, MACHINE_ID,
    MatchedContext, StepData, StepOutcome, StepProgress, StepResult, Value, WindowLookup,
};
pub use types::{EngineHashMap, EngineHashSet};

// Re-export pub(crate) items
pub(crate) use eval::{eval_expr, values_equal};
pub(crate) use key::{
    ScopeKey, eval_field_value, extract_key_simple, field_ref_name, push_i64_exact_decimal,
    scope_key_from_values, scope_key_shard_index, value_to_string,
};

pub use conv::apply_conv;

pub(crate) use eval::eval_expr_ext;

// Test-only re-exports: the `tests` sibling module sits outside `match_engine`,
// so private submodules (close/key/state/types) are not directly reachable.
// Benchmarks measure the production hot path without widening production APIs.
#[cfg(test)]
pub(crate) use key::ValueKey;
#[cfg(test)]
pub(crate) use state::StepState;
#[cfg(test)]
pub(crate) use types::RollingStats;

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use wf_lang::ast::CloseMode;
use wf_lang::plan::{ConvPlan, ExceedAction, LimitsPlan, MatchPlan, RateSpec, WindowSpec};

use crate::match_engine::columnar::GuardMasks;
pub(crate) use close::accumulate_close_steps;
use close::{evaluate_close, evidence_time_range};
use key::{InstanceKey, extract_key};
use seq::{SeqRuntime, consec_broken, scan_negations};
use state::{AliasState, Instance, snapshot_bind_data};
use step::{
    StepEvaluationInput, StepProgressCapture, collect_alias_event, evaluate_step_with_progress,
};

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
    /// Chain semantics (`within` / `not` / `consec`) precomputed from the plan.
    seq_meta: Option<SeqRuntime>,
}

/// Max expiry candidates processed per `scan_expired_at` call (incremental
/// expiry). Bounds each sweep so a far-ahead watermark cannot pop the whole
/// heap in one call and starve the pipeline (see `scan_expired_at`).
const MAX_EXPIRY_SCAN_BUDGET: usize = 1024;

impl CepStateMachine {
    /// Create a new state machine for the given rule + plan.
    pub fn new(rule_name: String, plan: MatchPlan, time_field: Option<String>) -> Self {
        let seq_meta = plan
            .seq
            .as_ref()
            .map(|c| SeqRuntime::build(&c.steps, c.consec));
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
            seq_meta,
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
            seq_meta,
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
            seq_meta,
        }
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

    /// Feed one event (arriving on `alias`) into the state machine.
    ///
    /// Extracts event time from the configured `time_field`, falling back to 0.
    pub fn advance(&mut self, alias: &str, event: &Event) -> StepResult {
        self.advance_with(alias, event, None)
    }

    /// Feed one event with optional window lookup for `window.has()` in guards.
    pub fn advance_with(
        &mut self,
        alias: &str,
        event: &Event,
        windows: Option<&dyn WindowLookup>,
    ) -> StepResult {
        let event_nanos = self.extract_event_time(event);
        self.advance_at_with(alias, event, event_nanos, windows)
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

    /// Feed one event with an explicit event-time timestamp (nanoseconds since epoch).
    pub fn advance_at(&mut self, alias: &str, event: &Event, now_nanos: i64) -> StepResult {
        self.advance_at_with(alias, event, now_nanos, None)
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
    pub(crate) fn extract_event_str<E: FieldSource>(event: &E, field: &str) -> String {
        event.field_value_str(field)
    }

    /// Feed one event with explicit timestamp and optional window lookup.
    pub fn advance_at_with(
        &mut self,
        alias: &str,
        event: &Event,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
    ) -> StepResult {
        self.advance_at_with_masks(alias, event, now_nanos, windows, 0, None)
    }

    /// Like [`Self::advance_at_with`], but with batch-level columnar branch-guard
    /// masks and the row index within the current batch. `masks` may be `None`
    /// (interpreted fallback for every branch).
    ///
    /// Generic over [`FieldSource`]: the eager path passes `&Event`, the
    /// deferred columnar path passes `&ColumnarEvent` (P3 FieldView — hit rows
    /// are fed straight from the batch, no HashMap materialization).
    pub fn advance_at_with_masks<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
    ) -> StepResult {
        self.advance_at_with_diagnostics(alias, event, now_nanos, windows, row, masks, false)
            .result
    }

    /// Feed one event and return both the state-machine result and diagnostic
    /// progress for the evaluated step, when progress can be captured.
    pub fn advance_at_with_progress<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
    ) -> StepOutcome {
        self.advance_at_with_diagnostics(alias, event, now_nanos, windows, 0, None, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn advance_at_with_diagnostics<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
        capture_progress: bool,
    ) -> StepOutcome {
        // FailRule: once the rule has failed, reject all future events.
        // P2b: with shared limits, a FailRule latch on any shard fails the rule.
        if self.failed || self.shared.as_ref().is_some_and(|s| s.is_failed()) {
            return step_outcome(StepResult::Accumulate, None);
        }

        // Update watermark
        if now_nanos > self.watermark_nanos {
            self.watermark_nanos = now_nanos;
        }

        // 1. Extract scope key from event. Join-then-key (Path A): when the key
        //    lives on a snapshot join's right window (plan.key_join), resolve it
        //    by looking the event's join-left value up in the joined window and
        //    reading the key field off the joined row. A miss anywhere (no
        //    lookup, missing left field, join miss, key absent on the row) is
        //    the same as a missing key field: skip the event.
        let scope_key = if let Some(kjp) = &self.plan.key_join {
            let Some(windows) = windows else {
                return step_outcome(StepResult::Accumulate, None); // no lookup → join miss
            };
            let Some(left_val) = event.field_value(field_ref_name(&kjp.left_field)) else {
                return step_outcome(StepResult::Accumulate, None); // missing join-left key → skip
            };
            let Some(rows) =
                windows.join_lookup(&kjp.right_window, &kjp.right_key_field, &left_val)
            else {
                return step_outcome(StepResult::Accumulate, None); // window not found → skip
            };
            // Match-time join re-verifies every candidate with `values_equal`
            // after the index lookup (`find_matching_row`); join-then-key must
            // do the same — the index key truncates f64
            // (`JoinKey::from_value` `as i64`), so a fractional driver value
            // would otherwise false-match a truncated row.
            let Some(row) = rows.iter().find(|r| {
                r.field_value(&kjp.right_key_field)
                    .is_some_and(|rv| values_equal(&left_val, &rv))
            }) else {
                return step_outcome(StepResult::Accumulate, None); // join miss → skip
            };
            let Some(key_val) = row.field_value(&kjp.right_field) else {
                return step_outcome(StepResult::Accumulate, None); // key absent on joined row → skip
            };
            vec![key_val]
        } else {
            match extract_key(event, &self.plan.keys, self.plan.key_map.as_deref(), alias) {
                Some(k) => k,
                None => return step_outcome(StepResult::Accumulate, None), // missing key field → skip
            }
        };

        // Build structured instance key
        let (instance_key, fixed_created_at) = match self.plan.window_spec {
            WindowSpec::Sliding(_) | WindowSpec::Session(_) => {
                // Session windows use sliding-style keys but with gap-based expiration
                (
                    InstanceKey::sliding(&scope_key_from_values(&scope_key)),
                    None,
                )
            }
            WindowSpec::Fixed(dur) => {
                let dur_nanos = dur.as_nanos() as i64;
                let bucket_start = (now_nanos / dur_nanos) * dur_nanos;
                let skey = scope_key_from_values(&scope_key);
                (InstanceKey::fixed(&skey, bucket_start), Some(bucket_start))
            }
        };

        // 2. Get or create instance (with limits check)
        let is_new = !self.instances.contains_key(&instance_key);
        // N1: whether THIS call holds a shared instance-slot reservation for the
        // incoming key. Every early return between the reservation and the
        // actual instance insert below must release it, or the shared budget
        // leaks one slot per throttled/failing new key until it is exhausted.
        let mut shared_slot_reserved = false;
        if is_new
            && let Some(ref limits) = self.limits
            && let Some(max_inst) = limits.max_instances
        {
            // P2b: with shared limits the budget is the cross-shard instance total.
            // Use an exact CAS reservation (`try_reserve_instance`) instead of a
            // read-then-act check — two shards can no longer both pass a stale
            // count and overshoot the cap (P1②).
            let reserved = match &self.shared {
                Some(shared) => {
                    let ok = shared.try_reserve_instance(max_inst);
                    shared_slot_reserved = ok;
                    ok
                }
                None => self.instances.len() < max_inst,
            };
            if !reserved {
                match limits.on_exceed {
                    ExceedAction::Throttle => return step_outcome(StepResult::Accumulate, None),
                    // P3-B: under shared limits this evicts this shard's LOCAL
                    // oldest instance, not the global oldest across shards (a
                    // cross-shard priority queue is out of scope). The shared
                    // count stays exact either way; eviction fairness is
                    // per-shard.
                    ExceedAction::DropOldest => {
                        // Evict the local oldest instance, releasing its shared
                        // slot, then re-reserve so the new instance is counted
                        // exactly. If this shard has no local instance to evict
                        // (budget held by other shards), reject the new key.
                        if let Some(oldest_key) = self
                            .instances
                            .iter()
                            .min_by_key(|(_, inst)| inst.created_at)
                            .map(|(k, _)| k.clone())
                            && self.remove_instance(&oldest_key).is_some()
                            && let Some(shared) = &self.shared
                        {
                            shared.release_instance();
                        }
                        let re_reserved = match &self.shared {
                            Some(shared) => {
                                let ok = shared.try_reserve_instance(max_inst);
                                shared_slot_reserved = ok;
                                ok
                            }
                            None => self.instances.len() < max_inst,
                        };
                        if !re_reserved {
                            return step_outcome(StepResult::Accumulate, None);
                        }
                    }
                    ExceedAction::FailRule => {
                        fail_rule(&mut self.failed, &self.shared);
                        return step_outcome(StepResult::Accumulate, None);
                    }
                }
            }
        }

        // New-instance base cost: reused for the max_memory check below and for
        // O(1) per-instance accounting in insert/remove (exact state growth is
        // corrected by periodic `recalibrate_memory`).
        let new_base = if is_new && self.tracks_memory_bytes() {
            Some(Instance::base_estimated_bytes(
                &self.plan, &scope_key, alias, event,
            ))
        } else {
            None
        };

        // max_memory_bytes: total estimated memory across all instances.
        // Runs on every event to catch both new instance creation and
        // existing instance growth (e.g. distinct_set expansion).
        //
        // P1②: under sharding this is an *approximate* budget — the shared total
        // is a check-then-act read plus per-insert base-cost deltas, so concurrent
        // shards may transiently overshoot by ≤ shard_count-1 new instances, and
        // per-instance state growth (distinct_set etc.) is only corrected by the
        // periodic `recalibrate_memory`. The eviction loop + recalibrate keep it
        // bounded; an exact CAS reserve is impractical for memory (grows
        // non-atomically). `max_instances` above IS exact.
        if let Some(ref limits) = self.limits
            && let Some(max_bytes) = limits.max_memory_bytes
        {
            let new_cost = new_base.unwrap_or(0);
            // P2b: with shared limits the budget is the cross-shard memory total.
            let shared_total = self
                .shared
                .as_ref()
                .map(|s| s.memory_bytes())
                .unwrap_or(self.estimated_memory_bytes);
            let mut total = shared_total + new_cost;
            if total >= max_bytes {
                match limits.on_exceed {
                    ExceedAction::Throttle => {
                        // N1: admission reserved a shared slot for this new key
                        // but we return before inserting — release or it leaks.
                        if shared_slot_reserved {
                            self.release_shared_instance();
                        }
                        return step_outcome(StepResult::Accumulate, None);
                    }
                    ExceedAction::DropOldest => {
                        // Evict oldest instances in a loop until under limit or nothing left.
                        // If the current key is the oldest it gets evicted too — its
                        // accumulated state is lost and entry() re-creates a fresh instance.
                        // We add the re-creation base cost to the budget so the loop
                        // keeps evicting until the fresh instance actually fits.
                        // N2: when the incoming key's own instance is evicted here, the
                        // re-creation below inherits its shared slot — releasing it now
                        // would under-count and over-admit later keys. The flag lets an
                        // early return still give the slot back if the re-creation
                        // never happens.
                        let mut slot_inherited_for_incoming = false;
                        while total >= max_bytes {
                            if let Some(oldest_key) = self
                                .instances
                                .iter()
                                .min_by_key(|(_, inst)| inst.created_at)
                                .map(|(k, _)| k.clone())
                            {
                                let evicting_current = oldest_key == instance_key;
                                if let Some(removed) = self.remove_instance(&oldest_key) {
                                    total = total.saturating_sub(removed.estimated_bytes());
                                    if evicting_current {
                                        // N2: the fresh instance created below takes
                                        // over this slot; shared count stays exact.
                                        slot_inherited_for_incoming = true;
                                    } else {
                                        // P1②: permanent eviction releases the shared slot.
                                        self.release_shared_instance();
                                    }
                                }
                                // Current key will be re-created — account for base cost
                                if evicting_current && !is_new {
                                    total += Instance::base_estimated_bytes(
                                        &self.plan, &scope_key, alias, event,
                                    );
                                }
                            } else {
                                // No instances to evict — cannot make room. The
                                // re-creation will not happen, so a held/inherited
                                // slot must go back (N1/N2).
                                if shared_slot_reserved || slot_inherited_for_incoming {
                                    self.release_shared_instance();
                                }
                                return step_outcome(StepResult::Accumulate, None);
                            }
                        }
                    }
                    ExceedAction::FailRule => {
                        fail_rule(&mut self.failed, &self.shared);
                        // N1: release the un-consumed reservation (see Throttle arm).
                        if shared_slot_reserved {
                            self.release_shared_instance();
                        }
                        return step_outcome(StepResult::Accumulate, None);
                    }
                }
            }
        }

        if is_new {
            self.push_expiry_candidate(&instance_key, fixed_created_at.unwrap_or(now_nanos));
        }
        let mut instance = self.take_instance(&instance_key).unwrap_or_else(|| {
            let created = fixed_created_at.unwrap_or(now_nanos);
            let machine_id = Self::extract_event_str(event, MACHINE_ID);
            let mut inst = Instance::new_at(&self.plan, machine_id, created);
            inst.base_cost = new_base.unwrap_or(0);
            inst
        });
        if is_new {
            // A freshly created instance enters the map here — account its base
            // cost once (the old insert_instance did it per put; put_instance is
            // net-zero and skips the mirror, so the admission must charge it).
            if self.tracks_memory_bytes() {
                self.estimated_memory_bytes = self
                    .estimated_memory_bytes
                    .saturating_add(instance.base_cost);
                if let Some(shared) = &self.shared {
                    shared.add_memory(instance.base_cost);
                }
            }
        }
        let plan = &self.plan;

        instance.observe_seen_event_time(now_nanos);

        if should_track_bind_alias(plan, alias) {
            let tracked_fields = plan.tracked_bind_fields.get(alias);
            collect_alias_event(
                event,
                instance
                    .alias_states
                    .get_or_insert_with(|| Box::new(EngineHashMap::default()))
                    .entry(alias.to_string())
                    .or_insert_with(AliasState::new),
                tracked_fields,
            );
        }

        // 2b. Chain semantics: negation scan + strict adjacency.
        let seq_broken = if let Some(meta) = self.seq_meta.as_ref() {
            scan_negations(
                meta,
                &mut instance,
                alias,
                event,
                now_nanos,
                windows,
                row,
                masks,
            );
            consec_broken(meta, &instance, plan, alias)
        } else {
            false
        };
        if seq_broken {
            let reset_at = fixed_created_at.unwrap_or(now_nanos);
            // A negation violation must persist across a `consec` adjacency break;
            // otherwise an in-window violation could be wiped and the chain re-fire.
            let neg_violated = instance.neg_violated;
            instance.reset(plan, reset_at);
            instance.neg_violated = neg_violated;
            self.push_expiry_candidate(&instance_key, reset_at);
            self.put_instance(instance_key, instance);
            return step_outcome(StepResult::Accumulate, None);
        }

        // 3. Accumulate close steps (if any) — happens on every event
        if !plan.close_steps.is_empty() {
            accumulate_close_steps(
                alias,
                event,
                now_nanos,
                plan,
                &mut instance.close_step_states,
                windows,
                &mut instance.baselines,
                row,
                masks,
            );
        }

        // 3b. Any-mode (unordered co-occurrence): evaluate all steps in parallel and
        // fire once every step has satisfied its threshold, regardless of order.
        if plan.match_mode == wf_lang::ast::MatchMode::Any {
            for step_idx in 0..plan.event_steps.len() {
                if instance.satisfied_flags[step_idx] {
                    continue;
                }
                let step_plan = &plan.event_steps[step_idx];
                let (satisfied, _) = {
                    let step_state = &mut instance.step_states[step_idx];
                    evaluate_step_with_progress(
                        StepEvaluationInput {
                            alias,
                            event,
                            event_time_nanos: now_nanos,
                            windows,
                            progress: None,
                            step_index: step_idx,
                            row,
                            masks,
                        },
                        step_plan,
                        step_state,
                        &mut instance.baselines,
                    )
                };
                if let Some((branch_idx, measure_value)) = satisfied {
                    let label = step_plan.branches[branch_idx].label.clone();
                    let (first, last, collected, field_vals) = {
                        let bs = &instance.step_states[step_idx].branch_states[branch_idx];
                        (
                            bs.event_first_time_nanos,
                            bs.event_last_time_nanos,
                            bs.collected_values
                                .as_deref()
                                .map(|q| q.iter().cloned().collect())
                                .unwrap_or_default(),
                            bs.field_values
                                .as_deref()
                                .map(|m| {
                                    m.iter()
                                        .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                                        .collect()
                                })
                                .unwrap_or_default(),
                        )
                    };
                    instance.completed_steps.push(StepData {
                        satisfied_branch_index: branch_idx,
                        label,
                        measure_value,
                        event_first_time_nanos: first,
                        event_last_time_nanos: last,
                        collected_values: collected,
                        field_values: field_vals,
                    });
                    instance.satisfied_flags[step_idx] = true;
                }
            }

            if instance.satisfied_flags.iter().all(|&f| f) {
                // Rate limiting before emitting (mirror the no-close path).
                if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
                    && !throttle_allows(
                        &self.shared,
                        &mut self.emit_count,
                        &mut self.emit_window_start,
                        now_nanos,
                        &rate,
                    )
                {
                    let on_exceed = self
                        .limits
                        .as_ref()
                        .map(|l| l.on_exceed.clone())
                        .unwrap_or(ExceedAction::Throttle);
                    match on_exceed {
                        ExceedAction::Throttle | ExceedAction::DropOldest => {
                            // `on event<accu>`: a throttled re-fire suppresses the
                            // alert but keeps the running accumulation.
                            if plan.accu {
                                instance.rearm(plan);
                            } else {
                                let reset_at = fixed_created_at.unwrap_or(now_nanos);
                                instance.reset(plan, reset_at);
                                self.push_expiry_candidate(&instance_key, reset_at);
                            }
                            self.put_instance(instance_key, instance);
                            return step_outcome(StepResult::Accumulate, None);
                        }
                        ExceedAction::FailRule => {
                            fail_rule(&mut self.failed, &self.shared);
                            self.put_instance(instance_key, instance);
                            return step_outcome(StepResult::Accumulate, None);
                        }
                    }
                }
                let (evidence_first, evidence_last) =
                    evidence_time_range(instance.completed_steps.iter())
                        .unwrap_or((now_nanos, now_nanos));
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key,
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
                    event_time_nanos: now_nanos,
                    event_first_time_nanos: evidence_first,
                    event_last_time_nanos: evidence_last,
                    window_start_time_nanos: instance.created_at,
                    window_end_time_nanos: Self::expire_time_for(&plan.window_spec, &instance),
                    machine_id: instance.machine_id.clone(),
                    trigger_event: Some(std::sync::Arc::new(event.to_event())),
                };
                if plan.accu {
                    // `on event<accu>` — keep accumulating across fires.
                    instance.rearm(plan);
                } else {
                    let reset_at = fixed_created_at.unwrap_or(now_nanos);
                    instance.reset(plan, reset_at);
                    self.push_expiry_candidate(&instance_key, reset_at);
                }
                self.put_instance(instance_key, instance);
                return step_outcome(StepResult::Matched(ctx), None);
            }

            self.put_instance(instance_key, instance);
            return step_outcome(StepResult::Accumulate, None);
        }

        let mut progress = None;
        let result = 'process: {
            // 4. If event already emitted (OR mode), just accumulate for close
            if instance.event_emitted {
                break 'process StepResult::Accumulate;
            }

            // 5. If event steps already complete (AND mode), just accumulate for close
            if instance.event_ok {
                break 'process StepResult::Accumulate;
            }

            // 6. Current step plan
            if instance.current_step >= plan.event_steps.len() {
                break 'process StepResult::Accumulate;
            }
            let step_idx = instance.current_step;
            let step_plan = &plan.event_steps[step_idx];

            // 6. Evaluate step
            let evaluation = {
                let step_state = &mut instance.step_states[step_idx];
                evaluate_step_with_progress(
                    StepEvaluationInput {
                        alias,
                        event,
                        event_time_nanos: now_nanos,
                        windows,
                        progress: capture_progress.then_some(StepProgressCapture {
                            rule_name: &self.rule_name,
                            scope_key: &scope_key,
                            machine_id: &instance.machine_id,
                            step_index: step_idx,
                        }),
                        step_index: step_idx,
                        row,
                        masks,
                    },
                    step_plan,
                    step_state,
                    &mut instance.baselines,
                )
            };
            let (satisfied, evaluation_progress) = evaluation;
            let Some((branch_idx, measure_value)) = satisfied else {
                progress = evaluation_progress;
                break 'process StepResult::Accumulate;
            };
            progress = evaluation_progress;

            let label = step_plan.branches[branch_idx].label.clone();
            let step_state = &instance.step_states[step_idx];
            // Collect the values from the satisfied branch for L3 functions
            let collected_values = step_state.branch_states[branch_idx]
                .collected_values
                .as_deref()
                .map(|q| q.iter().cloned().collect())
                .unwrap_or_default();
            instance.completed_steps.push(StepData {
                satisfied_branch_index: branch_idx,
                label,
                measure_value,
                event_first_time_nanos: step_state.branch_states[branch_idx].event_first_time_nanos,
                event_last_time_nanos: step_state.branch_states[branch_idx].event_last_time_nanos,
                collected_values,
                field_values: step_state.branch_states[branch_idx]
                    .field_values
                    .as_deref()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                            .collect()
                    })
                    .unwrap_or_default(),
            });

            // Chain `within`: the completing step must land within its gap of the
            // previous step's completion (window start for the first step).
            let within_violated = if let Some(meta) = self.seq_meta.as_ref() {
                meta.within
                    .get(step_idx)
                    .copied()
                    .flatten()
                    .is_some_and(|w| {
                        // Completion time = the event that completed the step
                        // (`event_last_time_nanos`). For aggregate steps this differs
                        // from `event_first_time_nanos` (threshold-met time, not
                        // first-event time).
                        let this_last = step_state.branch_states[branch_idx]
                            .event_last_time_nanos
                            .unwrap_or(now_nanos);
                        let prev_last = if step_idx == 0 {
                            instance.created_at
                        } else {
                            instance
                                .completed_steps
                                .get(step_idx - 1)
                                .and_then(|sd| sd.event_last_time_nanos)
                                .unwrap_or(instance.created_at)
                        };
                        // The gap must be non-negative and within `w`: an
                        // out-of-order completion (this before prev) violates
                        // "within" just as a gap that is too large does.
                        let gap = this_last - prev_last;
                        gap < 0 || gap > w.as_nanos() as i64
                    })
            } else {
                false
            };
            if within_violated {
                let reset_at = fixed_created_at.unwrap_or(now_nanos);
                // Preserve a negation violation across a `within` reset, matching the
                // `consec`-break reset: an in-window violation must not be wiped so the
                // chain can re-fire.
                let neg_violated = instance.neg_violated;
                instance.reset(plan, reset_at);
                instance.neg_violated = neg_violated;
                self.push_expiry_candidate(&instance_key, reset_at);
                break 'process StepResult::Accumulate;
            }
            instance.current_step += 1;

            if instance.current_step < plan.event_steps.len() {
                break 'process StepResult::Advance;
            }

            // Chain negation: a violated negation step must suppress the emit.
            if instance.neg_violated {
                let reset_at = fixed_created_at.unwrap_or(now_nanos);
                instance.reset(plan, reset_at);
                self.push_expiry_candidate(&instance_key, reset_at);
                break 'process StepResult::Accumulate;
            }

            if plan.close_steps.is_empty() {
                // Rate limiting check before emitting
                if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
                    && !throttle_allows(
                        &self.shared,
                        &mut self.emit_count,
                        &mut self.emit_window_start,
                        now_nanos,
                        &rate,
                    )
                {
                    let on_exceed = self
                        .limits
                        .as_ref()
                        .map(|l| l.on_exceed.clone())
                        .unwrap_or(ExceedAction::Throttle);
                    match on_exceed {
                        ExceedAction::Throttle | ExceedAction::DropOldest => {
                            // `on event<accu>`: a throttled re-fire suppresses the
                            // alert but keeps the running accumulation.
                            if plan.accu {
                                instance.rearm(plan);
                            } else {
                                // Suppress the match — reset instance for future use
                                let reset_at = fixed_created_at.unwrap_or(now_nanos);
                                instance.reset(plan, reset_at);
                                self.push_expiry_candidate(&instance_key, reset_at);
                            }
                            break 'process StepResult::Accumulate;
                        }
                        ExceedAction::FailRule => {
                            fail_rule(&mut self.failed, &self.shared);
                            break 'process StepResult::Accumulate;
                        }
                    }
                }

                // No close steps → M14 backward compat: Matched + reset, or
                // `on event<accu>` rearm (keep accumulating across fires).
                let (evidence_first, evidence_last) =
                    evidence_time_range(instance.completed_steps.iter())
                        .unwrap_or((now_nanos, now_nanos));
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key,
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
                    event_time_nanos: now_nanos,
                    event_first_time_nanos: evidence_first,
                    event_last_time_nanos: evidence_last,
                    window_start_time_nanos: instance.created_at,
                    window_end_time_nanos: Self::expire_time_for(&plan.window_spec, &instance),
                    machine_id: instance.machine_id.clone(),
                    trigger_event: Some(std::sync::Arc::new(event.to_event())),
                };
                if plan.accu {
                    // `on event<accu>` — keep accumulating across fires.
                    instance.rearm(plan);
                } else {
                    let reset_at = fixed_created_at.unwrap_or(now_nanos);
                    instance.reset(plan, reset_at);
                    self.push_expiry_candidate(&instance_key, reset_at);
                }
                StepResult::Matched(ctx)
            } else if plan.close_mode == CloseMode::Or {
                // OR mode: emit from event path immediately, keep instance alive for close
                if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
                    && !throttle_allows(
                        &self.shared,
                        &mut self.emit_count,
                        &mut self.emit_window_start,
                        now_nanos,
                        &rate,
                    )
                {
                    let on_exceed = self
                        .limits
                        .as_ref()
                        .map(|l| l.on_exceed.clone())
                        .unwrap_or(ExceedAction::Throttle);
                    match on_exceed {
                        ExceedAction::Throttle | ExceedAction::DropOldest => {
                            instance.event_emitted = true;
                            break 'process StepResult::Accumulate;
                        }
                        ExceedAction::FailRule => {
                            fail_rule(&mut self.failed, &self.shared);
                            break 'process StepResult::Accumulate;
                        }
                    }
                }
                instance.event_emitted = true;
                let (evidence_first, evidence_last) =
                    evidence_time_range(instance.completed_steps.iter())
                        .unwrap_or((now_nanos, now_nanos));
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key,
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
                    event_time_nanos: now_nanos,
                    event_first_time_nanos: evidence_first,
                    event_last_time_nanos: evidence_last,
                    window_start_time_nanos: instance.created_at,
                    window_end_time_nanos: Self::expire_time_for(&plan.window_spec, &instance),
                    machine_id: instance.machine_id.clone(),
                    trigger_event: Some(std::sync::Arc::new(event.to_event())),
                };
                StepResult::Matched(ctx)
            } else {
                // AND mode: mark event_ok, keep accumulating
                instance.event_ok = true;
                StepResult::Advance
            }
        };
        self.put_instance(instance_key, instance);
        if let Some(progress) = &mut progress {
            progress.instances = self.instances.len();
        }
        step_outcome(result, progress)
    }

    /// Number of active per-key instances.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// Borrow the underlying plan.
    pub fn plan(&self) -> &MatchPlan {
        &self.plan
    }

    /// Close a specific instance by scope key, evaluating close_steps.
    ///
    /// Removes the instance from the map and returns the [`CloseOutput`].
    /// Returns `None` if no instance exists for the given scope key.
    ///
    /// For fixed windows, multiple bucket instances may exist for the same
    /// scope key. This method closes the **oldest** bucket instance (by
    /// `created_at`). Call repeatedly to drain all buckets.
    pub fn close(&mut self, scope_key: &[Value], reason: CloseReason) -> Option<CloseOutput> {
        let skey = scope_key_from_values(scope_key);

        let instance_key = match self.plan.window_spec {
            WindowSpec::Sliding(_) | WindowSpec::Session(_) => InstanceKey::sliding(&skey),
            WindowSpec::Fixed(_) => self
                .instances
                .iter()
                .filter(|(k, _)| k.matches_scope(&skey))
                .min_by_key(|(_, inst)| inst.created_at)
                .map(|(k, _)| k.clone())?,
        };

        let instance = self.remove_instance(&instance_key)?;
        // P1②: closing an instance is a permanent remove — release its slot.
        self.release_shared_instance();
        let mut output = evaluate_close(
            &self.rule_name,
            &self.plan,
            instance,
            instance_key.scope_key_values(),
            reason,
            self.watermark_nanos,
        );
        self.rate_limit_close(&mut output, self.watermark_nanos);
        Some(output)
    }

    /// Scan all instances for maxspan expiry using the internal watermark.
    ///
    /// Used by the scheduler on periodic ticks.
    pub fn scan_expired(&mut self) -> Vec<CloseOutput> {
        self.scan_expired_at(self.watermark_nanos)
    }

    /// Scan all instances for maxspan expiry using an explicit watermark,
    /// returning every expired instance's [`CloseOutput`] (qualified or not) —
    /// the full-close contract used by the oracle and tests.
    ///
    /// Each expired instance's close output uses `created_at + maxspan` as its
    /// watermark (the logical expiry time), rather than the detection-time
    /// watermark. This makes `fired_at` deterministic regardless of batch size
    /// or scan frequency.
    pub fn scan_expired_at(&mut self, watermark_nanos: i64) -> Vec<CloseOutput> {
        self.scan_expired_at_impl(watermark_nanos, false, MAX_EXPIRY_SCAN_BUDGET)
    }

    /// Like [`Self::scan_expired_at`], but skips building [`CloseOutput`]s for
    /// instances that can never produce an alert.
    ///
    /// For rules with **no close steps** the qualification is decidable from the
    /// instance alone without building a CloseOutput:
    ///
    ///   - `And` mode: qualifies iff `event_ok` (`close_ok` is always true)
    ///   - `Or` mode: never qualifies (empty `close_step_data`)
    ///
    /// `event_ok` is a cheap bool on the instance. At 100M-scale count rules
    /// (q5) the vast majority of expiring instances never matched, so
    /// `evaluate_close` (close-steps eval + bind snapshot + completed-steps
    /// move) for each of them is pure waste that monopolizes the rule task and
    /// starves push consumption. The instance is removed identically either
    /// way, so skipping neither defers expiry nor holds memory. Callers that
    /// only process qualifying closes (the rule-task hot path, conv stage)
    /// can use this and observe identical output.
    pub fn scan_expired_at_skip_non_alerting(&mut self, watermark_nanos: i64) -> Vec<CloseOutput> {
        self.scan_expired_at_impl(watermark_nanos, true, MAX_EXPIRY_SCAN_BUDGET)
    }

    /// Like [`Self::scan_expired_at_skip_non_alerting`], but with an **unbounded**
    /// expiry budget. Only safe off the event hot path (periodic `scan_timeouts`,
    /// where the push pipeline is idle): a far-ahead watermark here pops the whole
    /// remaining heap in one call instead of deferring — fixed-window rules whose
    /// final bucket expires past the last event time depend on this sweep to close
    /// (q16 30M dropped the final bucket: 1.48M vs 1.89M ideal with a 1024 budget).
    pub fn scan_expired_at_skip_non_alerting_unbounded(
        &mut self,
        watermark_nanos: i64,
    ) -> Vec<CloseOutput> {
        self.scan_expired_at_impl(watermark_nanos, true, usize::MAX)
    }

    fn scan_expired_at_impl(
        &mut self,
        watermark_nanos: i64,
        skip_non_alerting: bool,
        budget: usize,
    ) -> Vec<CloseOutput> {
        let mut results = Vec::new();
        // Incremental expiry: bound each sweep so a far-ahead watermark cannot
        // pop millions of candidates in a single call and starve push
        // consumption (q5/q6/q7 froze at 30M+ — the sweep occupied the rule
        // task, the push channel filled, the pipeline froze). Remaining
        // candidates stay in the heap and are processed on the next scan
        // (per-row in the deferred loop + periodic `scan_timeouts`).
        let mut budget = budget;
        while let Some(Reverse((candidate_expire, key))) = self.expiry_heap.peek().cloned() {
            if candidate_expire > watermark_nanos || budget == 0 {
                break;
            }
            budget -= 1;
            self.expiry_heap.pop();
            self.pending_expiry.remove(&key);

            let current_expire = match self.instances.get(&key) {
                Some(instance) => Self::expire_time_for(&self.plan.window_spec, instance),
                None => continue, // stale candidate for an already-removed instance
            };

            if current_expire > watermark_nanos {
                // Session windows refresh expiry as events arrive. Re-queue
                // this key with the up-to-date expiry and continue.
                self.pending_expiry.insert(key.clone());
                self.expiry_heap.push(Reverse((current_expire, key)));
                continue;
            }

            if let Some(instance) = self.remove_instance(&key) {
                // P1②: expiry is a permanent remove — release its slot.
                self.release_shared_instance();
                let skip_close = skip_non_alerting
                    && self.plan.close_steps.is_empty()
                    && match self.plan.close_mode {
                        CloseMode::And => !instance.event_ok,
                        CloseMode::Or => true,
                    };
                if skip_close {
                    continue;
                }
                let mut output = evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
                    key.scope_key_values(),
                    CloseReason::Timeout,
                    current_expire,
                );
                self.rate_limit_close(&mut output, current_expire);
                results.push(output);
            }
        }
        results
    }

    /// Scan expired instances and apply conv transformations if configured.
    ///
    /// Filters out non-qualifying outputs (`!event_ok || !close_ok`) before
    /// applying conv, so that `top`/`dedup` operate only on entries that
    /// would actually produce alerts.
    pub fn scan_expired_at_with_conv(
        &mut self,
        watermark_nanos: i64,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.scan_expired_at(watermark_nanos);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// [`Self::scan_expired_at_with_conv`] over the skip-non-alerting scan — for
    /// the rule-task hot path where non-qualifying closes are discarded anyway.
    pub fn scan_expired_at_with_conv_skip_non_alerting(
        &mut self,
        watermark_nanos: i64,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.scan_expired_at_skip_non_alerting(watermark_nanos);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// [`Self::scan_expired_at_with_conv_skip_non_alerting`] with the unbounded
    /// expiry budget (off the event hot path only, see
    /// [`Self::scan_expired_at_skip_non_alerting_unbounded`]).
    pub fn scan_expired_at_with_conv_skip_non_alerting_unbounded(
        &mut self,
        watermark_nanos: i64,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.scan_expired_at_skip_non_alerting_unbounded(watermark_nanos);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// Close all active instances with optional conv transformations.
    ///
    /// Like [`close_all`], but applies conv to the qualifying outputs
    /// (where `event_ok && close_ok`) before returning.
    pub fn close_all_with_conv(
        &mut self,
        reason: CloseReason,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.close_all(reason);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// Close all active instances, returning a [`CloseOutput`] for each.
    ///
    /// Used during shutdown to flush all in-flight state.
    pub fn close_all(&mut self, reason: CloseReason) -> Vec<CloseOutput> {
        // Sort by (created_at, key) for fully deterministic rate limiting
        // order, same rationale as scan_expired_at.
        let mut keys: Vec<(InstanceKey, i64)> = self
            .instances
            .iter()
            .map(|(k, inst)| (k.clone(), inst.created_at))
            .collect();
        keys.sort_by(|(k1, t1), (k2, t2)| t1.cmp(t2).then_with(|| k1.cmp(k2)));
        let mut results = Vec::with_capacity(keys.len());
        let wm = self.watermark_nanos;
        for (key, _) in keys {
            if let Some(instance) = self.remove_instance(&key) {
                // P1②: close_all is a permanent remove — release each slot.
                self.release_shared_instance();
                let mut output = evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
                    key.scope_key_values(),
                    reason,
                    wm,
                );
                self.rate_limit_close(&mut output, wm);
                results.push(output);
            }
        }
        self.expiry_heap.clear();
        self.pending_expiry.clear();
        results
    }

    /// Current watermark (nanoseconds since epoch).
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos
    }

    /// Apply max_throttle to a close output that would produce an alert.
    ///
    /// If the output would emit (`event_ok && close_ok`) and the rate limit
    /// is exceeded, suppresses emission by clearing `close_ok`. This shares
    /// the same sliding-window counter used by the match path.
    fn rate_limit_close(&mut self, output: &mut CloseOutput, now_nanos: i64) {
        // P2c: shards in raw-conv mode skip inline throttle — the conv stage
        // applies the (shared) rate limit on the aggregated batch.
        if self.raw_conv_mode {
            return;
        }
        // Check if this output would emit based on close mode
        let would_emit = match output.close_mode {
            CloseMode::And => output.event_ok && output.close_ok,
            CloseMode::Or => output.close_ok && !output.close_step_data.is_empty(),
        };
        if !would_emit {
            return; // won't emit an alert anyway
        }
        if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
            && !throttle_allows(
                &self.shared,
                &mut self.emit_count,
                &mut self.emit_window_start,
                now_nanos,
                &rate,
            )
        {
            let on_exceed = self
                .limits
                .as_ref()
                .map(|l| l.on_exceed.clone())
                .unwrap_or(ExceedAction::Throttle);
            match on_exceed {
                ExceedAction::Throttle | ExceedAction::DropOldest => {
                    output.close_ok = false;
                }
                ExceedAction::FailRule => {
                    fail_rule(&mut self.failed, &self.shared);
                    output.close_ok = false;
                }
            }
        }
    }
    fn push_expiry_candidate(&mut self, key: &InstanceKey, created_at: i64) {
        // Only schedule one pending candidate per key. Per-event/reset pushes
        // (high-fire rules) would otherwise stack duplicate heap entries that
        // never get deduplicated — the dominant memory leak on pass-through
        // rules (wp-reactor leak investigation).
        if !self.pending_expiry.insert(key.clone()) {
            return;
        }
        let expire_time = match self.plan.window_spec {
            WindowSpec::Sliding(d) | WindowSpec::Fixed(d) | WindowSpec::Session(d) => {
                created_at + d.as_nanos() as i64
            }
        };
        self.expiry_heap.push(Reverse((expire_time, key.clone())));
    }

    /// Take an instance out of the map for the in-event processing round-trip
    /// WITHOUT touching the memory mirror — the same instance is put back a few
    /// statements later (net-zero base cost), so the per-event add/sub churn
    /// on the q17-style high-fire path (171 万实例 sliding map, every event
    /// removes + re-inserts) was pure overhead: 4 AtomicU64 ops per event.
    /// New-instance admission accounts its base cost explicitly at the take
    /// site; permanent removes (evict/close/expiry) keep `remove_instance`.
    fn take_instance(&mut self, key: &InstanceKey) -> Option<Instance> {
        self.instances.remove(key)
    }

    /// Put the instance back after the in-event round-trip (no memory
    /// mirroring — see [`Self::take_instance`]).
    fn put_instance(&mut self, key: InstanceKey, instance: Instance) {
        self.instances.insert(key, instance);
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

    #[cfg(test)]
    pub fn estimated_memory_bytes_for_test(&self) -> usize {
        self.estimated_memory_bytes
    }

    fn expire_time_for(window_spec: &WindowSpec, instance: &Instance) -> i64 {
        match window_spec {
            WindowSpec::Session(d) => instance.last_event_nanos + d.as_nanos() as i64,
            WindowSpec::Sliding(d) | WindowSpec::Fixed(d) => {
                instance.created_at + d.as_nanos() as i64
            }
        }
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
mod tests {
    use super::*;

    fn make_event(fields: Vec<(&str, Value)>) -> Event {
        Event {
            fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    #[test]
    fn extract_event_str() {
        let e = make_event(vec![
            ("sip", Value::Str("10.0.0.1".into())),
            ("n", Value::Number(5.0)),
            ("flag", Value::Bool(true)),
        ]);
        assert_eq!(CepStateMachine::extract_event_str(&e, "sip"), "10.0.0.1");
        let empty = make_event(vec![]);
        assert_eq!(CepStateMachine::extract_event_str(&empty, "any"), "");
    }
}
