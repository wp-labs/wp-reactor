use std::collections::HashMap;

use wf_lang::ast::{CmpOp, FieldSelector, Measure, Transform};
use wf_lang::plan::{AggPlan, StepPlan};

use super::eval::{eval_expr_ext, try_eval_expr_to_f64, try_eval_expr_to_value};
use super::key::value_to_string;
use super::state::{BranchState, StepState};
use super::types::{Event, RollingStats, Value, WindowLookup};

// ---------------------------------------------------------------------------
// Step evaluation
// ---------------------------------------------------------------------------

/// Evaluate all branches in a step. Returns the first branch that is
/// satisfied: `Some((branch_index, measure_value))`.
pub(super) fn evaluate_step(
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

pub(super) fn extract_branch_field(event: &Event, field: &Option<FieldSelector>) -> Option<Value> {
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
pub(super) fn apply_transforms(
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

pub(super) fn update_measure(
    measure: &Measure,
    field_value: &Option<Value>,
    bs: &mut BranchState,
) {
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

pub(super) fn compute_measure(measure: &Measure, bs: &BranchState) -> f64 {
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
pub(super) fn check_threshold(agg: &AggPlan, bs: &BranchState) -> bool {
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

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}
