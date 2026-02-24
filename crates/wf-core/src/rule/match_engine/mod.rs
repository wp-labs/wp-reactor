mod close;
mod eval;
mod key;
mod state;
mod step;
mod types;

// Re-export public types
pub use types::{
    CloseOutput, CloseReason, Event, MatchedContext, StepData, StepResult, Value, WindowLookup,
};

// Re-export pub(crate) items
pub(crate) use eval::{eval_expr, values_equal};
pub(crate) use key::{field_ref_name, value_to_string};

#[cfg(test)]
pub(crate) use eval::eval_expr_ext;

use std::collections::HashMap;

use wf_lang::plan::{ExceedAction, LimitsPlan, MatchPlan, WindowSpec};

use close::{accumulate_close_steps, evaluate_close};
use key::{extract_key, make_instance_key};
use state::Instance;
use step::evaluate_step;

// ---------------------------------------------------------------------------
// CepStateMachine — public API
// ---------------------------------------------------------------------------

/// Runtime CEP state machine that drives `match<key:dur>` execution.
///
/// Consumes a [`MatchPlan`] (produced by the M13 compiler) and processes
/// events one-at-a-time via [`advance`](Self::advance). Maintains per-key
/// state machine instances that advance through sequential steps with
/// OR-branch semantics and aggregation pipelines.
pub struct CepStateMachine {
    rule_name: String,
    plan: MatchPlan,
    instances: HashMap<String, Instance>,
    time_field: Option<String>,
    watermark_nanos: i64,
    limits: Option<LimitsPlan>,
    /// Set to true when `FailRule` limit is exceeded — all future events are
    /// rejected until the machine is reset.
    failed: bool,
    #[allow(dead_code)] // Reserved for emit rate limiting (L2)
    emit_count: u64,
    #[allow(dead_code)] // Reserved for emit rate limiting (L2)
    emit_window_start: i64,
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

    /// Feed one event with explicit timestamp and optional window lookup.
    fn advance_at_with(
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
        let instance_key = make_instance_key(&scope_key);

        // 2. Get or create instance (with limits check)
        let plan = &self.plan;
        let is_new = !self.instances.contains_key(&instance_key);
        if is_new
            && let Some(ref limits) = self.limits
            && let Some(max_card) = limits.max_cardinality
            && self.instances.len() >= max_card
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
                        self.instances.remove(&oldest_key);
                    }
                }
                ExceedAction::FailRule => {
                    self.failed = true;
                    return StepResult::Accumulate;
                }
            }
        }

        let instance = self
            .instances
            .entry(instance_key)
            .or_insert_with(|| Instance::new(plan, scope_key.clone(), now_nanos));

        // 3. Accumulate close steps (if any) — happens on every event
        if !plan.close_steps.is_empty() {
            accumulate_close_steps(
                alias,
                event,
                &plan.close_steps,
                &mut instance.close_step_states,
                windows,
                &mut instance.baselines,
            );
        }

        // 4. If event steps already complete, just accumulate for close
        if instance.event_ok {
            return StepResult::Accumulate;
        }

        // 5. Current step plan
        if instance.current_step >= plan.event_steps.len() {
            return StepResult::Accumulate;
        }
        let step_idx = instance.current_step;
        let step_plan = &plan.event_steps[step_idx];
        let step_state = &mut instance.step_states[step_idx];

        // 6. Evaluate step
        match evaluate_step(
            alias,
            event,
            step_plan,
            step_state,
            windows,
            &mut instance.baselines,
        ) {
            None => StepResult::Accumulate,
            Some((branch_idx, measure_value)) => {
                let label = step_plan.branches[branch_idx].label.clone();
                instance.completed_steps.push(StepData {
                    satisfied_branch_index: branch_idx,
                    label,
                    measure_value,
                });
                instance.current_step += 1;

                if instance.current_step >= plan.event_steps.len() {
                    if plan.close_steps.is_empty() {
                        // No close steps → M14 backward compat: Matched + reset
                        let ctx = MatchedContext {
                            rule_name: self.rule_name.clone(),
                            scope_key,
                            step_data: instance.completed_steps.clone(),
                            event_time_nanos: now_nanos,
                        };
                        instance.reset(plan, now_nanos);
                        StepResult::Matched(ctx)
                    } else {
                        // Close steps present → mark event_ok, keep accumulating
                        instance.event_ok = true;
                        StepResult::Advance
                    }
                } else {
                    StepResult::Advance
                }
            }
        }
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
    pub fn close(&mut self, scope_key: &[Value], reason: CloseReason) -> Option<CloseOutput> {
        let instance_key = make_instance_key(scope_key);
        let instance = self.instances.remove(&instance_key)?;
        Some(evaluate_close(
            &self.rule_name,
            &self.plan,
            instance,
            reason,
            self.watermark_nanos,
        ))
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
        let WindowSpec::Sliding(maxspan) = self.plan.window_spec;
        let maxspan_nanos = maxspan.as_nanos() as i64;
        let mut expired_keys = Vec::new();
        for (key, inst) in &self.instances {
            if watermark_nanos.saturating_sub(inst.created_at) >= maxspan_nanos {
                expired_keys.push(key.clone());
            }
        }
        let mut results = Vec::with_capacity(expired_keys.len());
        for key in expired_keys {
            if let Some(instance) = self.instances.remove(&key) {
                // Use the instance's logical expiry time for deterministic fired_at
                let expire_time = instance.created_at + maxspan_nanos;
                results.push(evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
                    CloseReason::Timeout,
                    expire_time,
                ));
            }
        }
        results
    }

    /// Close all active instances, returning a [`CloseOutput`] for each.
    ///
    /// Used during shutdown to flush all in-flight state.
    pub fn close_all(&mut self, reason: CloseReason) -> Vec<CloseOutput> {
        let keys: Vec<String> = self.instances.keys().cloned().collect();
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(instance) = self.instances.remove(&key) {
                results.push(evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
                    reason,
                    self.watermark_nanos,
                ));
            }
        }
        results
    }

    /// Current watermark (nanoseconds since epoch).
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos
    }
}
