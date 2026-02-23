use std::collections::HashMap;

use wf_lang::plan::{MatchPlan, StepPlan};

use super::eval::{eval_expr, eval_expr_ext};
use super::state::{Instance, StepState};
use super::step::{
    apply_transforms, check_threshold, compute_measure, extract_branch_field, update_measure,
};
use super::types::{CloseOutput, CloseReason, Event, RollingStats, StepData, Value, WindowLookup};

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
pub(super) fn accumulate_close_steps(
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
pub(super) fn evaluate_close(
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
