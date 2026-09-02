use wf_lang::ast::CloseMode;
use wf_lang::plan::{MatchPlan, StepPlan};

use super::eval::{eval_expr, eval_expr_ext};
use super::state::{Instance, StepState, snapshot_bind_data};
use super::step::{
    apply_transforms, check_threshold, collect_event_fields, compute_measure, extract_branch_field,
    push_capped, record_evidence_time, update_measure,
};
use super::types::{
    CloseOutput, CloseReason, EngineHashMap, Event, FieldSource, RollingStats, StepData, Value,
    WindowLookup,
};
use crate::match_engine::columnar::GuardMasks;

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
// Hot per-row path: flat args avoid a context struct on the accumulation
// loop (and its borrows); internal to the match engine only.
#[allow(clippy::too_many_arguments)]
pub(crate) fn accumulate_close_steps<E: FieldSource>(
    alias: &str,
    event: &E,
    event_time_nanos: i64,
    plan: &MatchPlan,
    close_step_states: &mut [StepState],
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
    row: usize,
    masks: Option<&GuardMasks>,
) {
    let close_steps = &plan.close_steps;
    let tracked_fields = plan.tracked_bind_fields.get(alias);
    for (step_idx, step_plan) in close_steps.iter().enumerate() {
        let step_state = &mut close_step_states[step_idx];
        for (branch_idx, branch) in step_plan.branches.iter().enumerate() {
            if branch.source != alias {
                continue;
            }

            // Permissive guard: only explicit false blocks accumulation. The
            // null-aware columnar mask mirrors this — null (missing field) is
            // permissive, and only an explicit `false` blocks.
            if let Some(guard) = &branch.guard {
                let blocks = match masks.and_then(|m| m.close_value(step_idx, branch_idx, row)) {
                    Some(Some(false)) => true,
                    Some(_) => false,
                    None => matches!(
                        eval_expr_ext(guard, event, windows, baselines),
                        Some(Value::Bool(false))
                    ),
                };
                if blocks {
                    continue;
                }
            }

            let field_value = extract_branch_field(event, &branch.field);
            let bs = &mut step_state.branch_states[branch_idx];

            // Apply transforms (Distinct dedup during accumulation)
            if !apply_transforms(&branch.agg.transforms, &field_value, bs) {
                continue;
            }

            record_evidence_time(bs, event_time_nanos);

            // The per-event field_values history feeds close-time `Field`
            // resolution (build_eval_context). `needs_field_history=false`
            // (compiler: close outputs read only the match keys / literals)
            // means those values are never consumed — skip the collection
            // entirely. q12-style count rules: saves the per-event
            // HashMap-insert + Vec push per tracked field on the advance hot
            // path.
            if plan.needs_field_history {
                collect_event_fields(
                    event,
                    bs,
                    tracked_fields,
                    &plan.tracked_plain_fields,
                    branch.field.as_ref(),
                );
            }

            // Update measure accumulators
            update_measure(&branch.agg.measure, &field_value, bs);

            // close 路径的 collected_values 同 gate（L3/非键读取时收集）。
            if plan.needs_field_history
                && let Some(val) = &field_value
            {
                push_capped(bs.collected_values_mut(), val.clone());
            }
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
            let mut m = EngineHashMap::default();
            m.insert("close_reason".into(), Value::Str(reason.as_str().into()));
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
                let collected_values = step_state.branch_states[branch_idx]
                    .collected_values
                    .as_deref()
                    .map(|q| q.iter().cloned().collect())
                    .unwrap_or_default();
                close_step_data.push(StepData {
                    satisfied_branch_index: branch_idx,
                    label,
                    measure_value,
                    event_first_time_nanos: step_state.branch_states[branch_idx]
                        .event_first_time_nanos,
                    event_last_time_nanos: step_state.branch_states[branch_idx]
                        .event_last_time_nanos,
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
            }
            None => {
                close_ok = false;
                // Still record empty data for this step
                close_step_data.push(StepData {
                    satisfied_branch_index: 0,
                    label: None,
                    measure_value: 0.0,
                    event_first_time_nanos: None,
                    event_last_time_nanos: None,
                    collected_values: Vec::new(),
                    field_values: EngineHashMap::default(),
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
    mut instance: Instance,
    scope_key: Vec<Value>,
    reason: CloseReason,
    watermark_nanos: i64,
    wall_nanos: Option<i64>,
) -> CloseOutput {
    let (close_ok, close_step_data) =
        evaluate_close_steps(&plan.close_steps, &instance.close_step_states, reason);
    // first_match（issue #82）：close 若为实例首次完整命中（从未 event-fire，本次
    // close qualified，即 `close_is_qualified` 语义），记录 close 处理墙钟；已
    // fire 过则保持首次值。未命中/未 qualified → None。须在 `completed_steps`
    // 移出前完成（`first_hit_wall` 需要 `&mut instance`）。
    let qualified_close = match plan.close_mode {
        CloseMode::And => instance.event_ok && close_ok,
        CloseMode::Or => close_ok && !close_step_data.is_empty(),
    };
    if qualified_close {
        instance.first_hit_wall(wall_nanos);
    }
    let first_match_time_nanos = instance.first_hit_wall_nanos;
    // 候选事件跨度（issue #82 方案 A，`@event_first_time`/`@event_last_time`）：
    // 必须在 `completed_steps` 移出前取（读 instance 字段）。
    let event_span = instance.event_span(instance.last_event_nanos);
    let last_event_nanos = instance.last_event_nanos;
    let event_step_data = instance.completed_steps;
    let evidence_range = match plan.close_mode {
        CloseMode::And => evidence_time_range(event_step_data.iter().chain(close_step_data.iter())),
        CloseMode::Or => evidence_time_range(close_step_data.iter()),
    };
    let (evidence_first, evidence_last) =
        evidence_range.unwrap_or((last_event_nanos, last_event_nanos));
    CloseOutput {
        rule_name: rule_name.to_string(),
        scope_key,
        machine_id: instance.machine_id,
        close_reason: reason,
        event_ok: instance.event_ok,
        close_ok,
        close_mode: plan.close_mode,
        event_emitted: instance.event_emitted,
        event_step_data,
        close_step_data,
        bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
        watermark_nanos,
        last_event_nanos,
        row_fields: None,
        row_field_names: None,
        event_first_time_nanos: event_span.0,
        event_last_time_nanos: event_span.1,
        evidence_first_time_nanos: evidence_first,
        evidence_last_time_nanos: evidence_last,
        window_start_time_nanos: instance.created_at,
        window_end_time_nanos: watermark_nanos,
        first_match_time_nanos,
    }
}

pub(super) fn evidence_time_range<'a>(
    steps: impl Iterator<Item = &'a StepData>,
) -> Option<(i64, i64)> {
    let mut first = None;
    let mut last = None;
    for step in steps {
        if let Some(value) = step.event_first_time_nanos {
            first = Some(first.map_or(value, |current: i64| current.min(value)));
        }
        if let Some(value) = step.event_last_time_nanos {
            last = Some(last.map_or(value, |current: i64| current.max(value)));
        }
    }
    Some((first?, last?))
}
