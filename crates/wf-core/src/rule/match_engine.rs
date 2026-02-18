use std::collections::{HashMap, HashSet};

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure, Transform};
use wf_lang::plan::{MatchPlan, StepPlan};

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
    pub scope_key: Vec<String>,
    pub step_data: Vec<StepData>,
}

/// Per-step snapshot captured when a step is satisfied.
#[derive(Debug, Clone, PartialEq)]
pub struct StepData {
    pub satisfied_branch_index: usize,
    pub label: Option<String>,
    pub measure_value: f64,
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
    current_step: usize,
    step_states: Vec<StepState>,
    completed_steps: Vec<StepData>,
}

impl Instance {
    fn new(plan: &MatchPlan) -> Self {
        let step_states = plan
            .event_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        Self {
            current_step: 0,
            step_states,
            completed_steps: Vec::new(),
        }
    }

    fn reset(&mut self, plan: &MatchPlan) {
        self.current_step = 0;
        self.step_states = plan
            .event_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        self.completed_steps.clear();
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
}

impl CepStateMachine {
    /// Create a new state machine for the given rule + plan.
    pub fn new(rule_name: String, plan: MatchPlan) -> Self {
        Self {
            rule_name,
            plan,
            instances: HashMap::new(),
        }
    }

    /// Feed one event (arriving on `alias`) into the state machine.
    ///
    /// Returns [`StepResult::Matched`] when all steps are satisfied,
    /// [`StepResult::Advance`] when a step boundary is crossed,
    /// or [`StepResult::Accumulate`] otherwise.
    pub fn advance(&mut self, alias: &str, event: &Event) -> StepResult {
        // 1. Extract scope key from event
        let scope_key = match extract_key(event, &self.plan.keys) {
            Some(k) => k,
            None => return StepResult::Accumulate, // missing key field → skip
        };
        let instance_key = make_instance_key(&scope_key);

        // 2. Get or create instance
        let plan = &self.plan;
        let instance = self
            .instances
            .entry(instance_key)
            .or_insert_with(|| Instance::new(plan));

        // 3. Current step plan
        if instance.current_step >= plan.event_steps.len() {
            return StepResult::Accumulate;
        }
        let step_idx = instance.current_step;
        let step_plan = &plan.event_steps[step_idx];
        let step_state = &mut instance.step_states[step_idx];

        // 4. Evaluate step
        match evaluate_step(alias, event, step_plan, step_state) {
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
                    // All steps done → matched
                    let ctx = MatchedContext {
                        rule_name: self.rule_name.clone(),
                        scope_key,
                        step_data: instance.completed_steps.clone(),
                    };
                    instance.reset(plan);
                    StepResult::Matched(ctx)
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
}

// ---------------------------------------------------------------------------
// Key extraction
// ---------------------------------------------------------------------------

/// Extract the scope key values from an event using the plan's key fields.
///
/// `FieldRef::Simple("sip")` / `Qualified(_, "sip")` / `Bracketed(_, "sip")`
/// all resolve to `event.fields["sip"]`.
///
/// Returns `None` if any key field is missing from the event.
/// Returns `Some(vec![])` if the key list is empty (shared instance).
fn extract_key(event: &Event, keys: &[FieldRef]) -> Option<Vec<String>> {
    let mut result = Vec::with_capacity(keys.len());
    for key in keys {
        let field_name = field_ref_name(key);
        let val = event.fields.get(field_name)?;
        result.push(value_to_string(val));
    }
    Some(result)
}

fn field_ref_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        _ => "",
    }
}

fn make_instance_key(scope_key: &[String]) -> String {
    scope_key.join("\x1f") // unit separator
}

fn value_to_string(v: &Value) -> String {
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
) -> Option<(usize, f64)> {
    for (branch_idx, branch) in step_plan.branches.iter().enumerate() {
        // Source must match alias
        if branch.source != alias {
            continue;
        }

        // Guard check
        if let Some(guard) = &branch.guard {
            match eval_expr(guard, event) {
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

        // Compute current measure value and check threshold
        let measure_val = compute_measure(&branch.agg.measure, bs);
        let threshold = eval_expr_to_f64(&branch.agg.threshold);

        if compare(branch.agg.cmp, measure_val, threshold) {
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
            if let Some(v) = fval
                && v < bs.min
            {
                bs.min = v;
            }
        }
        Measure::Max => {
            if let Some(v) = fval
                && v > bs.max
            {
                bs.max = v;
            }
        }
        _ => {} // unknown measure — no-op
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

// ---------------------------------------------------------------------------
// Expression evaluator (L1)
// ---------------------------------------------------------------------------

/// Evaluate an expression against an event, returning a [`Value`].
///
/// Supports: literals, field refs, BinOp (And/Or/comparisons/arithmetic), Neg.
/// `FuncCall` and `InList` return `None` (not evaluated in guard context at L1).
fn eval_expr(expr: &Expr, event: &Event) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Field(fr) => {
            let name = field_ref_name(fr);
            event.fields.get(name).cloned()
        }
        Expr::Neg(inner) => {
            let v = eval_expr(inner, event)?;
            match v {
                Value::Number(n) => Some(Value::Number(-n)),
                _ => None,
            }
        }
        Expr::BinOp { op, left, right } => eval_binop(*op, left, right, event),
        Expr::FuncCall { .. } | Expr::InList { .. } => None,
        _ => None,
    }
}

fn eval_binop(op: BinOp, left: &Expr, right: &Expr, event: &Event) -> Option<Value> {
    match op {
        // Short-circuit logical ops
        BinOp::And => {
            let lv = eval_expr(left, event)?;
            match lv {
                Value::Bool(false) => Some(Value::Bool(false)),
                Value::Bool(true) => {
                    let rv = eval_expr(right, event)?;
                    match rv {
                        Value::Bool(b) => Some(Value::Bool(b)),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        BinOp::Or => {
            let lv = eval_expr(left, event)?;
            match lv {
                Value::Bool(true) => Some(Value::Bool(true)),
                Value::Bool(false) => {
                    let rv = eval_expr(right, event)?;
                    match rv {
                        Value::Bool(b) => Some(Value::Bool(b)),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        // Comparison ops — work on Number and Str
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            let lv = eval_expr(left, event)?;
            let rv = eval_expr(right, event)?;
            Some(Value::Bool(compare_values(op, &lv, &rv)))
        }
        // Arithmetic ops — numeric only
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let lv = eval_expr(left, event)?;
            let rv = eval_expr(right, event)?;
            let ln = value_to_f64(&lv)?;
            let rn = value_to_f64(&rv)?;
            let result = match op {
                BinOp::Add => ln + rn,
                BinOp::Sub => ln - rn,
                BinOp::Mul => ln * rn,
                BinOp::Div => {
                    if rn == 0.0 {
                        return None;
                    }
                    ln / rn
                }
                BinOp::Mod => {
                    if rn == 0.0 {
                        return None;
                    }
                    ln % rn
                }
                _ => unreachable!(),
            };
            Some(Value::Number(result))
        }
        _ => None, // unknown BinOp variant
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

/// Evaluate a threshold expression to f64.
/// Typically this is just `Expr::Number(f64)`.
fn eval_expr_to_f64(expr: &Expr) -> f64 {
    match expr {
        Expr::Number(n) => *n,
        Expr::Neg(inner) => -eval_expr_to_f64(inner),
        _ => 0.0,
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}
