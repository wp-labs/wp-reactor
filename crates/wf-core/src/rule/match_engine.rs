use std::collections::{HashMap, HashSet};

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure, Transform};
use wf_lang::plan::{AggPlan, ExceedAction, LimitsPlan, MatchPlan, StepPlan, WindowSpec};

// ---------------------------------------------------------------------------
// Public types — Event & Value
// ---------------------------------------------------------------------------

/// A thin event abstraction: named fields with heterogeneous values.
///
/// M14 works exclusively with this type. Arrow RecordBatch bridging (M16)
/// will provide a zero-copy adapter later.
#[derive(Debug, Clone)]
pub struct Event {
    pub fields: HashMap<String, Value>,
}

/// Scalar value carried inside an [`Event`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Number(f64),
    Str(String),
    Bool(bool),
}

// ---------------------------------------------------------------------------
// Public types — result of advance()
// ---------------------------------------------------------------------------

/// Outcome of feeding one event into the state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum StepResult {
    /// Event was consumed but no step boundary was crossed.
    Accumulate,
    /// A step boundary was crossed (but more steps remain).
    Advance,
    /// All steps satisfied — the match is complete.
    Matched(MatchedContext),
}

/// Context returned when a full match fires.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchedContext {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub step_data: Vec<StepData>,
    pub event_time_nanos: i64,
}

/// Per-step snapshot captured when a step is satisfied.
#[derive(Debug, Clone, PartialEq)]
pub struct StepData {
    pub satisfied_branch_index: usize,
    pub label: Option<String>,
    pub measure_value: f64,
}

// ---------------------------------------------------------------------------
// Public types — close / timeout
// ---------------------------------------------------------------------------

/// Reason why a window instance was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Timeout,
    Flush,
    Eos,
}

impl CloseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseReason::Timeout => "timeout",
            CloseReason::Flush => "flush",
            CloseReason::Eos => "eos",
        }
    }
}

/// Output produced when an instance is closed (by timeout, flush, or eos).
#[derive(Debug, Clone, PartialEq)]
pub struct CloseOutput {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub close_reason: CloseReason,
    pub event_ok: bool,
    pub close_ok: bool,
    pub event_step_data: Vec<StepData>,
    pub close_step_data: Vec<StepData>,
    pub watermark_nanos: i64,
}

// ---------------------------------------------------------------------------
// WindowLookup trait — external window access for has() and join
// ---------------------------------------------------------------------------

/// Trait for accessing external window data at runtime.
/// Used by `window.has()` and join operations.
pub trait WindowLookup: Send + Sync {
    /// Get all distinct values for a field in a static window (for `has()`).
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>>;

    /// Get a full snapshot of a window (for join).
    fn snapshot(&self, window: &str) -> Option<Vec<HashMap<String, Value>>>;
}

// ---------------------------------------------------------------------------
// RollingStats — baseline deviation tracking
// ---------------------------------------------------------------------------

/// Cumulative statistics tracker for `baseline()` function.
#[derive(Debug, Clone)]
pub(crate) struct RollingStats {
    count: u64,
    sum: f64,
    sum_sq: f64,
}

impl RollingStats {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.sum_sq += value * value;
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    fn stddev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq / n) - (self.mean() * self.mean());
        if variance < 0.0 { 0.0 } else { variance.sqrt() }
    }

    /// How many standard deviations the value is from the mean.
    fn deviation(&self, value: f64) -> f64 {
        let std = self.stddev();
        if std == 0.0 {
            0.0
        } else {
            (value - self.mean()) / std
        }
    }
}

// ---------------------------------------------------------------------------
// Internal — per-branch / per-step / per-instance state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct BranchState {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    min_val: Option<Value>,
    max_val: Option<Value>,
    avg_sum: f64,
    avg_count: u64,
    distinct_set: HashSet<String>,
}

impl BranchState {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            min_val: None,
            max_val: None,
            avg_sum: 0.0,
            avg_count: 0,
            distinct_set: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct StepState {
    branch_states: Vec<BranchState>,
}

impl StepState {
    fn new(branch_count: usize) -> Self {
        Self {
            branch_states: (0..branch_count).map(|_| BranchState::new()).collect(),
        }
    }
}

#[derive(Debug, Clone)]
struct Instance {
    scope_key: Vec<Value>,
    created_at: i64,
    current_step: usize,
    event_ok: bool,
    step_states: Vec<StepState>,
    completed_steps: Vec<StepData>,
    close_step_states: Vec<StepState>,
    baselines: HashMap<String, RollingStats>,
}

impl Instance {
    fn new(plan: &MatchPlan, scope_key: Vec<Value>, now_nanos: i64) -> Self {
        let step_states = plan
            .event_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        let close_step_states = plan
            .close_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        Self {
            scope_key,
            created_at: now_nanos,
            current_step: 0,
            event_ok: false,
            step_states,
            completed_steps: Vec::new(),
            close_step_states,
            baselines: HashMap::new(),
        }
    }

    fn reset(&mut self, plan: &MatchPlan, now_nanos: i64) {
        self.created_at = now_nanos;
        self.current_step = 0;
        self.event_ok = false;
        self.step_states = plan
            .event_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        self.completed_steps.clear();
        self.close_step_states = plan
            .close_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        self.baselines.clear();
    }
}

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
        match evaluate_step(alias, event, step_plan, step_state, windows, &mut instance.baselines) {
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

// ---------------------------------------------------------------------------
// Key extraction
// ---------------------------------------------------------------------------

/// Extract the scope key values from an event using the plan's key fields.
///
/// When `key_map` is provided, uses alias-specific field mappings to extract
/// the key from different source fields depending on the event's alias.
///
/// Returns `None` if any key field is missing from the event.
/// Returns `Some(vec![])` if the key list is empty (shared instance).
fn extract_key(
    event: &Event,
    keys: &[FieldRef],
    key_map: Option<&[wf_lang::plan::KeyMapPlan]>,
    alias: &str,
) -> Option<Vec<Value>> {
    let km = match key_map {
        Some(km) => km,
        None => return extract_key_simple(event, keys),
    };

    // Collect unique logical key names (preserving order)
    let mut logical_names = Vec::new();
    for entry in km {
        if !logical_names.contains(&entry.logical_name) {
            logical_names.push(entry.logical_name.clone());
        }
    }

    if logical_names.is_empty() && keys.is_empty() {
        return Some(vec![]);
    }

    // For each logical key, try to extract a value:
    //   1. From this alias's mapped source field
    //   2. Fallback: from the event using the logical name directly
    let mut result = Vec::with_capacity(logical_names.len());
    for logical in &logical_names {
        // Try alias-specific mapping first
        let mapped = km
            .iter()
            .find(|e| e.logical_name == *logical && e.source_alias == alias)
            .and_then(|e| event.fields.get(&e.source_field));

        if let Some(val) = mapped {
            result.push(val.clone());
            continue;
        }

        // Fallback: field named after the logical key
        if let Some(val) = event.fields.get(logical.as_str()) {
            result.push(val.clone());
            continue;
        }
    }

    if result.is_empty() && !keys.is_empty() {
        return extract_key_simple(event, keys);
    }

    // Reject partial keys: all logical keys must be present
    if result.len() != logical_names.len() {
        return None;
    }

    Some(result)
}

fn extract_key_simple(event: &Event, keys: &[FieldRef]) -> Option<Vec<Value>> {
    let mut result = Vec::with_capacity(keys.len());
    for key in keys {
        let field_name = field_ref_name(key);
        let val = event.fields.get(field_name)?;
        result.push(val.clone());
    }
    Some(result)
}

pub(crate) fn field_ref_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        _ => "",
    }
}

fn make_instance_key(scope_key: &[Value]) -> String {
    scope_key
        .iter()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join("\x1f")
}

pub(crate) fn value_to_string(v: &Value) -> String {
    match v {
        Value::Number(n) => n.to_string(),
        Value::Str(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Step evaluation
// ---------------------------------------------------------------------------

/// Evaluate all branches in a step. Returns the first branch that is
/// satisfied: `Some((branch_index, measure_value))`.
fn evaluate_step(
    alias: &str,
    event: &Event,
    step_plan: &StepPlan,
    step_state: &mut StepState,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<(usize, f64)> {
    for (branch_idx, branch) in step_plan.branches.iter().enumerate() {
        // Source must match alias
        if branch.source != alias {
            continue;
        }

        // Guard check
        if let Some(guard) = &branch.guard {
            match eval_expr_ext(guard, event, windows, baselines) {
                Some(Value::Bool(true)) => {} // guard passed
                _ => continue,                // guard failed or non-bool
            }
        }

        // Extract field value (for aggregation)
        let field_value = extract_branch_field(event, &branch.field);

        let bs = &mut step_state.branch_states[branch_idx];

        // Apply transforms (Distinct dedup)
        if !apply_transforms(&branch.agg.transforms, &field_value, bs) {
            continue; // filtered out by transform (e.g. duplicate in distinct)
        }

        // Update measure accumulators
        update_measure(&branch.agg.measure, &field_value, bs);

        // Check threshold
        let satisfied = check_threshold(&branch.agg, bs);

        if satisfied {
            let measure_val = compute_measure(&branch.agg.measure, bs);
            return Some((branch_idx, measure_val));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Branch field extraction
// ---------------------------------------------------------------------------

fn extract_branch_field(event: &Event, field: &Option<FieldSelector>) -> Option<Value> {
    match field {
        Some(FieldSelector::Dot(name)) | Some(FieldSelector::Bracket(name)) => {
            event.fields.get(name).cloned()
        }
        Some(_) => None,
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Transform application
// ---------------------------------------------------------------------------

/// Apply transforms. Returns `false` if the event should be skipped
/// (e.g. duplicate value in a Distinct pipeline).
fn apply_transforms(
    transforms: &[Transform],
    field_value: &Option<Value>,
    bs: &mut BranchState,
) -> bool {
    for t in transforms {
        if t == &Transform::Distinct {
            let key = match field_value {
                Some(v) => value_to_string(v),
                None => return false,
            };
            if !bs.distinct_set.insert(key) {
                return false; // duplicate
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Measure update & computation
// ---------------------------------------------------------------------------

fn update_measure(measure: &Measure, field_value: &Option<Value>, bs: &mut BranchState) {
    let fval = field_value.as_ref().and_then(value_to_f64);

    match measure {
        Measure::Count => {
            bs.count += 1;
        }
        Measure::Sum => {
            if let Some(v) = fval {
                bs.sum += v;
            }
        }
        Measure::Avg => {
            if let Some(v) = fval {
                bs.avg_sum += v;
                bs.avg_count += 1;
            }
        }
        Measure::Min => {
            update_extreme(fval, field_value, &mut bs.min, &mut bs.min_val, true);
        }
        Measure::Max => {
            update_extreme(fval, field_value, &mut bs.max, &mut bs.max_val, false);
        }
        _ => {} // unknown measure — no-op
    }
}

/// Update numeric extreme + Value-based extreme in one shot.
fn update_extreme(
    fval: Option<f64>,
    field_value: &Option<Value>,
    num_acc: &mut f64,
    val_acc: &mut Option<Value>,
    is_min: bool,
) {
    if let Some(v) = fval
        && ((is_min && v < *num_acc) || (!is_min && v > *num_acc))
    {
        *num_acc = v;
    }
    if let Some(val) = field_value {
        let replace = match val_acc.as_ref() {
            None => true,
            Some(cur) => {
                let ord = value_ordering(val, cur);
                if is_min { ord.is_lt() } else { ord.is_gt() }
            }
        };
        if replace {
            *val_acc = Some(val.clone());
        }
    }
}

fn compute_measure(measure: &Measure, bs: &BranchState) -> f64 {
    match measure {
        Measure::Count => bs.count as f64,
        Measure::Sum => bs.sum,
        Measure::Avg => {
            if bs.avg_count == 0 {
                0.0
            } else {
                bs.avg_sum / bs.avg_count as f64
            }
        }
        Measure::Min => bs.min,
        Measure::Max => bs.max,
        _ => 0.0, // unknown measure
    }
}

/// Unified threshold check for a branch's aggregation plan.
///
/// Strategy:
/// 1. Try `try_eval_expr_to_f64` on the threshold expression.
///    - If it succeeds AND the numeric measure value is usable → f64 compare.
/// 2. For min/max where the numeric path gives ±INF (non-numeric field)
///    OR the threshold is non-constant → fall back to Value-based comparison.
/// 3. If neither path resolves, the check returns `false` (not satisfied).
fn check_threshold(agg: &AggPlan, bs: &BranchState) -> bool {
    let measure_f64 = compute_measure(&agg.measure, bs);

    // Fast path: threshold is a constant numeric expression
    if let Some(threshold_f64) = try_eval_expr_to_f64(&agg.threshold) {
        match agg.measure {
            Measure::Min | Measure::Max if !measure_f64.is_finite() => {
                // Numeric accumulator is ±INF → non-numeric field, fall through
                // to value-based path below
            }
            _ => return compare(agg.cmp, measure_f64, threshold_f64),
        }
    }

    // Value-based path: needed for min/max on non-numeric fields,
    // or when threshold expression is non-constant.
    match agg.measure {
        Measure::Min => {
            if let (Some(val), Some(threshold_val)) =
                (&bs.min_val, try_eval_expr_to_value(&agg.threshold))
            {
                compare_value_threshold(agg.cmp, val, &threshold_val)
            } else {
                false
            }
        }
        Measure::Max => {
            if let (Some(val), Some(threshold_val)) =
                (&bs.max_val, try_eval_expr_to_value(&agg.threshold))
            {
                compare_value_threshold(agg.cmp, val, &threshold_val)
            } else {
                false
            }
        }
        _ => {
            // count/sum/avg with a non-constant threshold (e.g. field ref):
            // cannot evaluate — treat as unsatisfied rather than silently
            // comparing against 0.0
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

fn compare(cmp: CmpOp, lhs: f64, rhs: f64) -> bool {
    match cmp {
        CmpOp::Eq => (lhs - rhs).abs() < f64::EPSILON,
        CmpOp::Ne => (lhs - rhs).abs() >= f64::EPSILON,
        CmpOp::Lt => lhs < rhs,
        CmpOp::Gt => lhs > rhs,
        CmpOp::Le => lhs <= rhs,
        CmpOp::Ge => lhs >= rhs,
        _ => false,
    }
}

/// Ordering for Value (used by min/max on orderable fields).
/// Number < Str < Bool for cross-type (shouldn't happen in practice).
fn value_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // Cross-type: shouldn't happen with well-typed rules
        (Value::Number(_), _) => std::cmp::Ordering::Less,
        (_, Value::Number(_)) => std::cmp::Ordering::Greater,
        (Value::Str(_), Value::Bool(_)) => std::cmp::Ordering::Less,
        (Value::Bool(_), Value::Str(_)) => std::cmp::Ordering::Greater,
    }
}

/// Compare a Value against a threshold Value using CmpOp.
/// Returns `false` for cross-type comparisons (e.g. Str vs Number)
/// to prevent false positives from the arbitrary cross-type ordering.
fn compare_value_threshold(cmp: CmpOp, val: &Value, threshold: &Value) -> bool {
    let same_type = matches!(
        (val, threshold),
        (Value::Number(_), Value::Number(_))
            | (Value::Str(_), Value::Str(_))
            | (Value::Bool(_), Value::Bool(_))
    );
    if !same_type {
        return false;
    }
    let ord = value_ordering(val, threshold);
    match cmp {
        CmpOp::Eq => ord.is_eq(),
        CmpOp::Ne => !ord.is_eq(),
        CmpOp::Lt => ord.is_lt(),
        CmpOp::Gt => ord.is_gt(),
        CmpOp::Le => ord.is_le(),
        CmpOp::Ge => ord.is_ge(),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Expression evaluator (L1)
// ---------------------------------------------------------------------------

/// Evaluate an expression against an event, returning a [`Value`].
///
/// Supports: literals, field refs, BinOp (And/Or/comparisons/arithmetic),
/// Neg, InList, and basic FuncCall (contains, lower, upper, len, has, baseline).
pub(crate) fn eval_expr(expr: &Expr, event: &Event) -> Option<Value> {
    let mut empty = HashMap::new();
    eval_expr_ext(expr, event, None, &mut empty)
}

/// Extended expression evaluator with window lookup and baseline store access.
///
/// All recursive calls go through this function (not `eval_expr`) to preserve
/// the `windows` and `baselines` context through compound expressions.
pub(crate) fn eval_expr_ext(
    expr: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Field(fr) => {
            let name = field_ref_name(fr);
            event.fields.get(name).cloned()
        }
        Expr::Neg(inner) => {
            let v = eval_expr_ext(inner, event, windows, baselines)?;
            match v {
                Value::Number(n) => Some(Value::Number(-n)),
                _ => None,
            }
        }
        Expr::BinOp { op, left, right } => {
            eval_binop(*op, left, right, event, windows, baselines)
        }
        Expr::InList {
            expr: target,
            list,
            negated,
        } => {
            let target_val = eval_expr_ext(target, event, windows, baselines)?;
            // InList items are typically literals — context not needed, but
            // we pass it for correctness in case of field refs / func calls.
            let found = list.iter().any(|item| {
                eval_expr_ext(item, event, windows, baselines)
                    .map(|v| values_equal(&target_val, &v))
                    .unwrap_or(false)
            });
            Some(Value::Bool(if *negated { !found } else { found }))
        }
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            // Handle window.has()
            if let Some(window_name) = qualifier
                && name == "has"
            {
                return eval_window_has(window_name, args, event, windows);
            }
            // Handle baseline()
            if name == "baseline" && args.len() == 2 {
                return eval_baseline(args, event, baselines);
            }
            eval_func_call(name, args, event, windows, baselines)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            let cond_val = eval_expr_ext(cond, event, windows, baselines);
            match cond_val {
                Some(Value::Bool(true)) => eval_expr_ext(then_expr, event, windows, baselines),
                Some(Value::Bool(false)) => eval_expr_ext(else_expr, event, windows, baselines),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Evaluate `window.has(expr [, "field"])`.
fn eval_window_has(
    window_name: &str,
    args: &[Expr],
    event: &Event,
    windows: Option<&dyn WindowLookup>,
) -> Option<Value> {
    let windows = windows?;
    let lookup_val = eval_expr(&args[0], event)?;
    let lookup_str = value_to_string(&lookup_val);

    // Explicit field name from 2nd arg, or infer from the field ref in 1st arg
    let field_name = match args.get(1) {
        Some(Expr::StringLit(f)) => f.clone(),
        Some(_) => return None,
        None => match &args[0] {
            Expr::Field(fr) => field_ref_name(fr).to_string(),
            _ => return None,
        },
    };

    let values = windows.snapshot_field_values(window_name, &field_name)?;
    Some(Value::Bool(values.contains(&lookup_str)))
}

/// Evaluate `baseline(expr, duration_seconds)`.
///
/// Computes the z-score (number of standard deviations from the running mean)
/// of the current value, then updates the running statistics.
fn eval_baseline(
    args: &[Expr],
    event: &Event,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    let current_val = match eval_expr(&args[0], event)? {
        Value::Number(n) => n,
        _ => return None,
    };

    // Build a key to identify this baseline expression
    let key = format!("{:?}", args[0]);

    let stats = baselines.entry(key).or_insert_with(RollingStats::new);
    let deviation = stats.deviation(current_val);
    stats.update(current_val);
    Some(Value::Number(deviation))
}

fn eval_binop(
    op: BinOp,
    left: &Expr,
    right: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    match op {
        BinOp::And => eval_logic_and(left, right, event, windows, baselines),
        BinOp::Or => eval_logic_or(left, right, event, windows, baselines),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            let lv = eval_expr_ext(left, event, windows, baselines)?;
            let rv = eval_expr_ext(right, event, windows, baselines)?;
            Some(Value::Bool(compare_values(op, &lv, &rv)))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let lv = eval_expr_ext(left, event, windows, baselines)?;
            let rv = eval_expr_ext(right, event, windows, baselines)?;
            let ln = value_to_f64(&lv)?;
            let rn = value_to_f64(&rv)?;
            eval_arithmetic(op, ln, rn)
        }
        _ => None,
    }
}

/// Three-valued (SQL NULL) logical AND.
///
/// Both sides are always evaluated so that partial information is preserved.
/// This is essential for close-step guards where one side references an
/// event field (missing at close time) and the other references
/// close_reason (missing during accumulation).
fn eval_logic_and(
    left: &Expr,
    right: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    let lv = eval_expr_ext(left, event, windows, baselines);
    let rv = eval_expr_ext(right, event, windows, baselines);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => Some(Value::Bool(false)),
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => Some(Value::Bool(true)),
        _ => None,
    }
}

/// Three-valued (SQL NULL) logical OR.
fn eval_logic_or(
    left: &Expr,
    right: &Expr,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    let lv = eval_expr_ext(left, event, windows, baselines);
    let rv = eval_expr_ext(right, event, windows, baselines);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(Value::Bool(true)),
        (Some(Value::Bool(false)), Some(Value::Bool(false))) => Some(Value::Bool(false)),
        _ => None,
    }
}

/// Arithmetic on two numeric values: +, -, *, /, %.
fn eval_arithmetic(op: BinOp, lv: f64, rv: f64) -> Option<Value> {
    let result = match op {
        BinOp::Add => lv + rv,
        BinOp::Sub => lv - rv,
        BinOp::Mul => lv * rv,
        BinOp::Div => {
            if rv == 0.0 {
                return None;
            }
            lv / rv
        }
        BinOp::Mod => {
            if rv == 0.0 {
                return None;
            }
            lv % rv
        }
        _ => return None,
    };
    Some(Value::Number(result))
}

/// Equality check for InList membership.
pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => (x - y).abs() < f64::EPSILON,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        _ => false,
    }
}

/// Evaluate basic function calls in guard context.
///
/// Supported functions:
/// - `contains(haystack, needle)` → Bool
/// - `lower(s)` → Str
/// - `upper(s)` → Str
/// - `len(s)` → Number
fn eval_func_call(
    name: &str,
    args: &[Expr],
    event: &Event,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) -> Option<Value> {
    match name {
        "contains" => {
            if args.len() != 2 {
                return None;
            }
            let haystack = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let needle = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            Some(Value::Bool(haystack.contains(&*needle)))
        }
        "lower" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.to_lowercase())),
                _ => None,
            }
        }
        "upper" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Str(s.to_uppercase())),
                _ => None,
            }
        }
        "len" => {
            if args.len() != 1 {
                return None;
            }
            match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => Some(Value::Number(s.len() as f64)),
                _ => None,
            }
        }
        "regex_match" => {
            if args.len() != 2 {
                return None;
            }
            let hay = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let pat = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Str(s) => s,
                _ => return None,
            };
            let re = regex::Regex::new(&pat).ok()?;
            Some(Value::Bool(re.is_match(&hay)))
        }
        "time_diff" => {
            if args.len() != 2 {
                return None;
            }
            let t1 = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let t2 = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            Some(Value::Number((t1 - t2).abs() / 1_000_000_000.0))
        }
        "time_bucket" => {
            if args.len() != 2 {
                return None;
            }
            let t = match eval_expr_ext(&args[0], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let interval = match eval_expr_ext(&args[1], event, windows, baselines)? {
                Value::Number(n) => n,
                _ => return None,
            };
            let interval_nanos = interval * 1_000_000_000.0;
            if interval_nanos == 0.0 {
                return None;
            }
            let bucketed = (t / interval_nanos).floor() * interval_nanos;
            Some(Value::Number(bucketed))
        }
        _ => None, // unsupported function
    }
}

fn compare_values(op: BinOp, lv: &Value, rv: &Value) -> bool {
    match (lv, rv) {
        (Value::Number(a), Value::Number(b)) => {
            let cmp = CmpOp::from_binop(op);
            compare(cmp, *a, *b)
        }
        (Value::Str(a), Value::Str(b)) => {
            let ord = a.cmp(b);
            match op {
                BinOp::Eq => ord.is_eq(),
                BinOp::Ne => !ord.is_eq(),
                BinOp::Lt => ord.is_lt(),
                BinOp::Gt => ord.is_gt(),
                BinOp::Le => ord.is_le(),
                BinOp::Ge => ord.is_ge(),
                _ => false,
            }
        }
        (Value::Bool(a), Value::Bool(b)) => match op {
            BinOp::Eq => a == b,
            BinOp::Ne => a != b,
            _ => false,
        },
        _ => false, // type mismatch
    }
}

/// Helper trait to convert BinOp comparison variants to CmpOp.
trait FromBinOp {
    fn from_binop(op: BinOp) -> Self;
}

impl FromBinOp for CmpOp {
    fn from_binop(op: BinOp) -> Self {
        match op {
            BinOp::Eq => CmpOp::Eq,
            BinOp::Ne => CmpOp::Ne,
            BinOp::Lt => CmpOp::Lt,
            BinOp::Gt => CmpOp::Gt,
            BinOp::Le => CmpOp::Le,
            BinOp::Ge => CmpOp::Ge,
            _ => CmpOp::Eq, // fallback (should not be reached for comparison ops)
        }
    }
}

// ---------------------------------------------------------------------------
// Threshold expression evaluation
// ---------------------------------------------------------------------------

/// Try to evaluate a threshold expression to f64.
/// Returns `Some(f64)` for Number, Neg, and constant arithmetic (BinOp on
/// numeric literals).  Returns `None` for expressions that cannot be
/// statically resolved to a number (field refs, function calls, etc.)
/// — callers must fall back to value-based comparison.
fn try_eval_expr_to_f64(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Number(n) => Some(*n),
        Expr::Neg(inner) => try_eval_expr_to_f64(inner).map(|v| -v),
        Expr::BinOp { op, left, right } => {
            let l = try_eval_expr_to_f64(left)?;
            let r = try_eval_expr_to_f64(right)?;
            match op {
                BinOp::Add => Some(l + r),
                BinOp::Sub => Some(l - r),
                BinOp::Mul => Some(l * r),
                BinOp::Div => {
                    if r == 0.0 {
                        None
                    } else {
                        Some(l / r)
                    }
                }
                BinOp::Mod => {
                    if r == 0.0 {
                        None
                    } else {
                        Some(l % r)
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Try to evaluate a threshold expression to a [`Value`].
/// Returns `Some` for literal constants (Number, String, Bool) and
/// constant arithmetic (Neg, BinOp on numeric literals).
/// Returns `None` for non-constant expressions (field refs, func calls, etc.).
fn try_eval_expr_to_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => try_eval_expr_to_f64(expr).map(Value::Number),
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Close-step accumulation (during advance)
// ---------------------------------------------------------------------------

/// Accumulate data for close steps during event processing.
///
/// For each close step branch whose `source == alias`:
/// - Evaluate guard against the event with **permissive** semantics: only an
///   explicit `false` blocks accumulation. `None` (e.g. `close_reason` not yet
///   available) is treated as "don't filter" so event-field guards filter
///   correctly while close_reason guards pass through.
/// - Apply transforms (Distinct dedup must happen during accumulation)
/// - Update measure accumulators (count++, sum+=, etc.)
fn accumulate_close_steps(
    alias: &str,
    event: &Event,
    close_steps: &[StepPlan],
    close_step_states: &mut [StepState],
    windows: Option<&dyn WindowLookup>,
    baselines: &mut HashMap<String, RollingStats>,
) {
    for (step_idx, step_plan) in close_steps.iter().enumerate() {
        let step_state = &mut close_step_states[step_idx];
        for (branch_idx, branch) in step_plan.branches.iter().enumerate() {
            if branch.source != alias {
                continue;
            }

            // Permissive guard: only explicit false blocks accumulation
            if let Some(guard) = &branch.guard
                && let Some(Value::Bool(false)) = eval_expr_ext(guard, event, windows, baselines)
            {
                continue;
            }

            let field_value = extract_branch_field(event, &branch.field);
            let bs = &mut step_state.branch_states[branch_idx];

            // Apply transforms (Distinct dedup during accumulation)
            if !apply_transforms(&branch.agg.transforms, &field_value, bs) {
                continue;
            }

            // Update measure accumulators
            update_measure(&branch.agg.measure, &field_value, bs);
        }
    }
}

// ---------------------------------------------------------------------------
// Close-step evaluation (at close time)
// ---------------------------------------------------------------------------

/// Evaluate close steps at close time.
///
/// Creates a synthetic event with `close_reason` for guard evaluation.
/// Reads already-accumulated measure state (no new accumulation).
/// Returns `(close_ok, close_step_data)`.
fn evaluate_close_steps(
    close_steps: &[StepPlan],
    close_step_states: &[StepState],
    reason: CloseReason,
) -> (bool, Vec<StepData>) {
    // Synthetic event for guard evaluation
    let synthetic_event = Event {
        fields: {
            let mut m = HashMap::new();
            m.insert(
                "close_reason".to_string(),
                Value::Str(reason.as_str().to_string()),
            );
            m
        },
    };

    let mut close_ok = true;
    let mut close_step_data = Vec::with_capacity(close_steps.len());

    for (step_idx, step_plan) in close_steps.iter().enumerate() {
        let step_state = &close_step_states[step_idx];
        match evaluate_close_step(step_plan, step_state, &synthetic_event) {
            Some((branch_idx, measure_value)) => {
                let label = step_plan.branches[branch_idx].label.clone();
                close_step_data.push(StepData {
                    satisfied_branch_index: branch_idx,
                    label,
                    measure_value,
                });
            }
            None => {
                close_ok = false;
                // Still record empty data for this step
                close_step_data.push(StepData {
                    satisfied_branch_index: 0,
                    label: None,
                    measure_value: 0.0,
                });
            }
        }
    }

    (close_ok, close_step_data)
}

/// Evaluate a single close step against accumulated state.
///
/// For each branch:
/// - Evaluate guard against synthetic event with **permissive** semantics:
///   only explicit `false` blocks. `None` (e.g. event field not in synthetic
///   event) is treated as "don't filter" — event-field guards were already
///   applied during accumulation.
/// - Check accumulated measure against threshold (NO new accumulation)
/// - First branch satisfied → step passes
fn evaluate_close_step(
    step_plan: &StepPlan,
    step_state: &StepState,
    synthetic_event: &Event,
) -> Option<(usize, f64)> {
    for (branch_idx, branch) in step_plan.branches.iter().enumerate() {
        // Permissive guard: only explicit false blocks
        if let Some(guard) = &branch.guard
            && let Some(Value::Bool(false)) = eval_expr(guard, synthetic_event)
        {
            continue;
        }

        // Check accumulated threshold (no new accumulation)
        let bs = &step_state.branch_states[branch_idx];
        if check_threshold(&branch.agg, bs) {
            let measure_val = compute_measure(&branch.agg.measure, bs);
            return Some((branch_idx, measure_val));
        }
    }
    None
}

/// Internal: evaluate close steps and build CloseOutput for a removed instance.
fn evaluate_close(
    rule_name: &str,
    plan: &MatchPlan,
    instance: Instance,
    reason: CloseReason,
    watermark_nanos: i64,
) -> CloseOutput {
    let (close_ok, close_step_data) =
        evaluate_close_steps(&plan.close_steps, &instance.close_step_states, reason);
    CloseOutput {
        rule_name: rule_name.to_string(),
        scope_key: instance.scope_key,
        close_reason: reason,
        event_ok: instance.event_ok,
        close_ok,
        event_step_data: instance.completed_steps,
        close_step_data,
        watermark_nanos,
    }
}
