mod close;
mod conv;
mod eval;
mod key;
mod state;
mod step;
mod types;

// Re-export public types
pub use types::{
    BindData, CloseOutput, CloseReason, Event, MACHINE_ID, MatchedContext, StepData, StepResult,
    Value, WindowLookup,
};

// Re-export pub(crate) items
pub(crate) use eval::{eval_expr, values_equal};
pub(crate) use key::{field_ref_name, value_to_string};

#[cfg(test)]
pub(crate) use conv::apply_conv;

pub(crate) use eval::eval_expr_ext;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use wf_lang::ast::CloseMode;
use wf_lang::plan::{ConvPlan, ExceedAction, LimitsPlan, MatchPlan, WindowSpec};

use close::{accumulate_close_steps, evaluate_close};
use key::{InstanceKey, extract_key, make_scope_key_str};
use state::{AliasState, Instance, snapshot_bind_data};
use step::{collect_alias_event, evaluate_step};

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
    instances: HashMap<InstanceKey, Instance>,
    time_field: Option<String>,
    watermark_nanos: i64,
    limits: Option<LimitsPlan>,
    /// Set to true when `FailRule` limit is exceeded — all future events are
    /// rejected until the machine is reset.
    failed: bool,
    emit_count: u64,
    emit_window_start: i64,
    /// Expiry candidates ordered by `(expire_time, instance_key)`.
    ///
    /// Stale candidates are filtered out when popped by checking the current
    /// instance state in `self.instances`.
    expiry_heap: BinaryHeap<Reverse<(i64, InstanceKey)>>,
    /// Cached estimated memory across active instances.
    ///
    /// This keeps `limits.max_memory` checks O(1) for the common path instead
    /// of re-summing every instance for every incoming event.
    estimated_memory_bytes: usize,
}

impl CepStateMachine {
    /// Create a new state machine for the given rule + plan.
    pub fn new(rule_name: String, plan: MatchPlan, time_field: Option<String>) -> Self {
        Self {
            rule_name,
            plan,
            instances: HashMap::new(),
            time_field,
            watermark_nanos: 0,
            limits: None,
            failed: false,
            emit_count: 0,
            emit_window_start: 0,
            expiry_heap: BinaryHeap::new(),
            estimated_memory_bytes: 0,
        }
    }

    /// Create a new state machine with limits enforcement.
    pub fn with_limits(
        rule_name: String,
        plan: MatchPlan,
        time_field: Option<String>,
        limits: Option<LimitsPlan>,
    ) -> Self {
        Self {
            rule_name,
            plan,
            instances: HashMap::new(),
            time_field,
            watermark_nanos: 0,
            limits,
            failed: false,
            emit_count: 0,
            emit_window_start: 0,
            expiry_heap: BinaryHeap::new(),
            estimated_memory_bytes: 0,
        }
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
            .and_then(|tf| event.fields.get(tf))
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

    /// Extract a string field from an event, returning empty string if not found.
    pub(crate) fn extract_event_str(event: &Event, field: &str) -> String {
        event
            .fields
            .get(field)
            .and_then(|v| match v {
                Value::Str(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Feed one event with explicit timestamp and optional window lookup.
    pub fn advance_at_with(
        &mut self,
        alias: &str,
        event: &Event,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
    ) -> StepResult {
        // FailRule: once the rule has failed, reject all future events
        if self.failed {
            return StepResult::Accumulate;
        }

        // Update watermark
        if now_nanos > self.watermark_nanos {
            self.watermark_nanos = now_nanos;
        }

        // 1. Extract scope key from event
        let scope_key =
            match extract_key(event, &self.plan.keys, self.plan.key_map.as_deref(), alias) {
                Some(k) => k,
                None => return StepResult::Accumulate, // missing key field → skip
            };

        // Build structured instance key
        let (instance_key, fixed_created_at) = match self.plan.window_spec {
            WindowSpec::Sliding(_) | WindowSpec::Session(_) => {
                // Session windows use sliding-style keys but with gap-based expiration
                (InstanceKey::sliding(&scope_key), None)
            }
            WindowSpec::Fixed(dur) => {
                let dur_nanos = dur.as_nanos() as i64;
                let bucket_start = (now_nanos / dur_nanos) * dur_nanos;
                (
                    InstanceKey::fixed(&scope_key, bucket_start),
                    Some(bucket_start),
                )
            }
        };

        // 2. Get or create instance (with limits check)
        let is_new = !self.instances.contains_key(&instance_key);
        if is_new
            && let Some(ref limits) = self.limits
            && let Some(max_inst) = limits.max_instances
            && self.instances.len() >= max_inst
        {
            match limits.on_exceed {
                ExceedAction::Throttle => return StepResult::Accumulate,
                ExceedAction::DropOldest => {
                    // Find and remove the oldest instance
                    if let Some(oldest_key) = self
                        .instances
                        .iter()
                        .min_by_key(|(_, inst)| inst.created_at)
                        .map(|(k, _)| k.clone())
                    {
                        self.remove_instance(&oldest_key);
                    }
                }
                ExceedAction::FailRule => {
                    self.failed = true;
                    return StepResult::Accumulate;
                }
            }
        }

        // max_memory_bytes: total estimated memory across all instances.
        // Runs on every event to catch both new instance creation and
        // existing instance growth (e.g. distinct_set expansion).
        if let Some(ref limits) = self.limits
            && let Some(max_bytes) = limits.max_memory_bytes
        {
            let new_cost = if is_new {
                Instance::base_estimated_bytes(&self.plan, &scope_key, alias, event)
            } else {
                0
            };
            let mut total = self.estimated_memory_bytes + new_cost;
            if total >= max_bytes {
                match limits.on_exceed {
                    ExceedAction::Throttle => return StepResult::Accumulate,
                    ExceedAction::DropOldest => {
                        // Evict oldest instances in a loop until under limit or nothing left.
                        // If the current key is the oldest it gets evicted too — its
                        // accumulated state is lost and entry() re-creates a fresh instance.
                        // We add the re-creation base cost to the budget so the loop
                        // keeps evicting until the fresh instance actually fits.
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
                                }
                                // Current key will be re-created — account for base cost
                                if evicting_current && !is_new {
                                    total += Instance::base_estimated_bytes(
                                        &self.plan, &scope_key, alias, event,
                                    );
                                }
                            } else {
                                // No instances to evict — cannot make room
                                return StepResult::Accumulate;
                            }
                        }
                    }
                    ExceedAction::FailRule => {
                        self.failed = true;
                        return StepResult::Accumulate;
                    }
                }
            }
        }

        if is_new {
            self.push_expiry_candidate(&instance_key, fixed_created_at.unwrap_or(now_nanos));
        }
        let mut instance = self.remove_instance(&instance_key).unwrap_or_else(|| {
            let created = fixed_created_at.unwrap_or(now_nanos);
            let machine_id = Self::extract_event_str(event, MACHINE_ID);
            Instance::new_at(&self.plan, scope_key.clone(), machine_id, created)
        });
        let plan = &self.plan;

        // Track the latest event time for this instance
        if now_nanos > instance.last_event_nanos {
            instance.last_event_nanos = now_nanos;
        }

        if should_track_bind_alias(plan, alias) {
            let tracked_fields = plan.tracked_bind_fields.get(alias);
            collect_alias_event(
                event,
                instance
                    .alias_states
                    .entry(alias.to_string())
                    .or_insert_with(AliasState::new),
                tracked_fields,
            );
        }

        // 3. Accumulate close steps (if any) — happens on every event
        if !plan.close_steps.is_empty() {
            accumulate_close_steps(
                alias,
                event,
                plan,
                &mut instance.close_step_states,
                windows,
                &mut instance.baselines,
            );
        }

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
            let step_state = &mut instance.step_states[step_idx];

            // 6. Evaluate step
            let Some((branch_idx, measure_value)) = evaluate_step(
                alias,
                event,
                step_plan,
                step_state,
                windows,
                &mut instance.baselines,
            ) else {
                break 'process StepResult::Accumulate;
            };

            let label = step_plan.branches[branch_idx].label.clone();
            // Collect the values from the satisfied branch for L3 functions
            let collected_values = step_state.branch_states[branch_idx]
                .collected_values
                .clone();
            instance.completed_steps.push(StepData {
                satisfied_branch_index: branch_idx,
                label,
                measure_value,
                collected_values,
                field_values: step_state.branch_states[branch_idx].field_values.clone(),
            });
            instance.current_step += 1;

            if instance.current_step < plan.event_steps.len() {
                break 'process StepResult::Advance;
            }

            if plan.close_steps.is_empty() {
                // Rate limiting check before emitting
                if let Some(ref limits) = self.limits
                    && let Some(ref rate) = limits.max_throttle
                {
                    let window_nanos = rate.per.as_nanos() as i64;
                    // Rotate window if expired
                    if now_nanos - self.emit_window_start >= window_nanos {
                        self.emit_count = 0;
                        self.emit_window_start = now_nanos;
                    }
                    if self.emit_count >= rate.count {
                        match limits.on_exceed {
                            ExceedAction::Throttle | ExceedAction::DropOldest => {
                                // Suppress the match — reset instance for future use
                                let reset_at = fixed_created_at.unwrap_or(now_nanos);
                                instance.reset(plan, reset_at);
                                self.push_expiry_candidate(&instance_key, reset_at);
                                break 'process StepResult::Accumulate;
                            }
                            ExceedAction::FailRule => {
                                self.failed = true;
                                break 'process StepResult::Accumulate;
                            }
                        }
                    }
                    self.emit_count += 1;
                }

                // No close steps → M14 backward compat: Matched + reset
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key,
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(&instance.alias_states),
                    event_time_nanos: now_nanos,
                    machine_id: instance.machine_id.clone(),
                };
                let reset_at = fixed_created_at.unwrap_or(now_nanos);
                instance.reset(plan, reset_at);
                self.push_expiry_candidate(&instance_key, reset_at);
                StepResult::Matched(ctx)
            } else if plan.close_mode == CloseMode::Or {
                // OR mode: emit from event path immediately, keep instance alive for close
                if let Some(ref limits) = self.limits
                    && let Some(ref rate) = limits.max_throttle
                {
                    let window_nanos = rate.per.as_nanos() as i64;
                    if now_nanos - self.emit_window_start >= window_nanos {
                        self.emit_count = 0;
                        self.emit_window_start = now_nanos;
                    }
                    if self.emit_count >= rate.count {
                        match limits.on_exceed {
                            ExceedAction::Throttle | ExceedAction::DropOldest => {
                                instance.event_emitted = true;
                                break 'process StepResult::Accumulate;
                            }
                            ExceedAction::FailRule => {
                                self.failed = true;
                                break 'process StepResult::Accumulate;
                            }
                        }
                    }
                    self.emit_count += 1;
                }
                instance.event_emitted = true;
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key,
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(&instance.alias_states),
                    event_time_nanos: now_nanos,
                    machine_id: instance.machine_id.clone(),
                };
                StepResult::Matched(ctx)
            } else {
                // AND mode: mark event_ok, keep accumulating
                instance.event_ok = true;
                StepResult::Advance
            }
        };
        self.insert_instance(instance_key, instance);
        result
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
        let scope_key_str = make_scope_key_str(scope_key);

        let instance_key = match self.plan.window_spec {
            WindowSpec::Sliding(_) | WindowSpec::Session(_) => InstanceKey::sliding(scope_key),
            WindowSpec::Fixed(_) => self
                .instances
                .iter()
                .filter(|(k, _)| k.matches_scope(&scope_key_str))
                .min_by_key(|(_, inst)| inst.created_at)
                .map(|(k, _)| k.clone())?,
        };

        let instance = self.remove_instance(&instance_key)?;
        let mut output = evaluate_close(
            &self.rule_name,
            &self.plan,
            instance,
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

    /// Scan all instances for maxspan expiry using an explicit watermark.
    ///
    /// Used by the oracle and tests.
    ///
    /// Each expired instance's close output uses `created_at + maxspan` as its
    /// watermark (the logical expiry time), rather than the detection-time
    /// watermark. This makes `fired_at` deterministic regardless of batch size
    /// or scan frequency.
    pub fn scan_expired_at(&mut self, watermark_nanos: i64) -> Vec<CloseOutput> {
        let mut results = Vec::new();
        while let Some(Reverse((candidate_expire, key))) = self.expiry_heap.peek().cloned() {
            if candidate_expire > watermark_nanos {
                break;
            }
            self.expiry_heap.pop();

            let current_expire = match self.instances.get(&key) {
                Some(instance) => Self::expire_time_for(&self.plan.window_spec, instance),
                None => continue, // stale candidate for an already-removed instance
            };

            if current_expire > watermark_nanos {
                // Session windows refresh expiry as events arrive. Re-queue
                // this key with the up-to-date expiry and continue.
                self.expiry_heap.push(Reverse((current_expire, key)));
                continue;
            }

            if let Some(instance) = self.remove_instance(&key) {
                let mut output = evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
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
                let mut output = evaluate_close(&self.rule_name, &self.plan, instance, reason, wm);
                self.rate_limit_close(&mut output, wm);
                results.push(output);
            }
        }
        self.expiry_heap.clear();
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
        // Check if this output would emit based on close mode
        let would_emit = match output.close_mode {
            CloseMode::And => output.event_ok && output.close_ok,
            CloseMode::Or => output.close_ok && !output.close_step_data.is_empty(),
        };
        if !would_emit {
            return; // won't emit an alert anyway
        }
        if let Some(ref limits) = self.limits
            && let Some(ref rate) = limits.max_throttle
        {
            let window_nanos = rate.per.as_nanos() as i64;
            if now_nanos - self.emit_window_start >= window_nanos {
                self.emit_count = 0;
                self.emit_window_start = now_nanos;
            }
            if self.emit_count >= rate.count {
                match limits.on_exceed {
                    ExceedAction::Throttle | ExceedAction::DropOldest => {
                        output.close_ok = false;
                    }
                    ExceedAction::FailRule => {
                        self.failed = true;
                        output.close_ok = false;
                    }
                }
                return;
            }
            self.emit_count += 1;
        }
    }
    fn push_expiry_candidate(&mut self, key: &InstanceKey, created_at: i64) {
        let expire_time = match self.plan.window_spec {
            WindowSpec::Sliding(d) | WindowSpec::Fixed(d) | WindowSpec::Session(d) => {
                created_at + d.as_nanos() as i64
            }
        };
        self.expiry_heap.push(Reverse((expire_time, key.clone())));
    }

    fn insert_instance(&mut self, key: InstanceKey, instance: Instance) {
        self.estimated_memory_bytes = self
            .estimated_memory_bytes
            .saturating_add(instance.estimated_bytes());
        self.instances.insert(key, instance);
    }

    fn remove_instance(&mut self, key: &InstanceKey) -> Option<Instance> {
        let instance = self.instances.remove(key)?;
        self.estimated_memory_bytes = self
            .estimated_memory_bytes
            .saturating_sub(instance.estimated_bytes());
        Some(instance)
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

fn should_track_bind_alias(plan: &MatchPlan, alias: &str) -> bool {
    plan.tracked_bind_aliases.contains(alias)
        || !plan
            .event_steps
            .iter()
            .chain(plan.close_steps.iter())
            .flat_map(|step| step.branches.iter())
            .any(|branch| branch.source == alias)
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
        outputs.into_iter().partition(|o| match o.close_mode {
            CloseMode::And => o.event_ok && o.close_ok,
            CloseMode::Or => o.close_ok && !o.close_step_data.is_empty(),
        });

    if qualifying.is_empty() {
        return non_qualifying;
    }

    let mut result = conv::apply_conv(conv, keys, qualifying);
    result.extend(non_qualifying);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(fields: Vec<(&str, Value)>) -> Event {
        Event {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
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
