use std::time::Duration;

use wf_lang::ast::{FieldRef, JoinMode};
use wf_lang::plan::{JoinCondPlan, JoinPlan, StepPlan};

use crate::rule::match_engine::{
    Event, StepData, Value, WindowLookup, field_ref_name, values_equal,
};

/// Build a synthetic [`Event`] from match context for expression evaluation.
///
/// - Maps `keys[i]` field name → `scope_key[i]` value (original type preserved)
/// - Adds step labels as fields → `label` → `Value::Number(measure_value)`
/// - Labels that collide with key names are silently skipped (keys take priority)
/// - Adds `_step_{i}_values` fields with collected values for L3 functions
pub(super) fn build_eval_context(
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    step_plans: &[&StepPlan],
) -> Event {
    let mut fields = std::collections::HashMap::new();

    // Key fields — preserve original Value type
    for (fr, val) in keys.iter().zip(scope_key.iter()) {
        let name = field_ref_name(fr).to_string();
        fields.insert(name, val.clone());
    }

    // Step labels → measure values (skip if name collides with a key field)
    // Also store collected values for L3 functions
    for (step_idx, sd) in step_data.iter().enumerate() {
        if let Some(label) = &sd.label
            && !fields.contains_key(label.as_str())
        {
            fields.insert(label.clone(), Value::Number(sd.measure_value));
        }
        // Store collected values for L3 functions (collect_set/list, first/last, stddev/percentile)
        let values_field = format!("_step_{}_values", step_idx);
        let values_array = Value::Array(sd.collected_values.clone());
        fields.insert(values_field, values_array);
        if let Some(step_plan) = step_plans.get(step_idx)
            && let Some(branch) = step_plan.branches.get(sd.satisfied_branch_index)
        {
            let source_field = format!("_step_{}_source", step_idx);
            fields.insert(source_field, Value::Str(branch.source.clone()));
        }
    }

    Event { fields }
}

/// Execute join plans, enriching the eval context with joined fields.
///
/// For each join, dispatches on join mode:
/// - `Snapshot`: snapshots all rows and finds the first condition-matching row.
/// - `Asof`: gets timestamped rows, filters by time proximity, picks the latest match.
///
/// Matched fields are added to the context both as `window.field` (qualified)
/// and as plain `field` (if not already present).
pub(super) fn execute_joins(
    joins: &[JoinPlan],
    ctx: &mut Event,
    windows: &dyn WindowLookup,
    event_time_nanos: i64,
) {
    for join in joins {
        let matched_row = match &join.mode {
            JoinMode::Snapshot => {
                let Some(rows) = windows.snapshot(&join.right_window) else {
                    continue;
                };
                find_matching_row(&rows, &join.conds, ctx)
            }
            JoinMode::Asof { within } => {
                let Some(rows) = windows.snapshot_with_timestamps(&join.right_window) else {
                    continue;
                };
                find_asof_row(&rows, &join.conds, ctx, event_time_nanos, within.as_ref())
            }
            _ => {
                // Unknown join mode — skip gracefully
                continue;
            }
        };

        let Some(row) = matched_row else {
            continue;
        };

        for (field_name, value) in &row {
            let qualified = format!("{}.{}", join.right_window, field_name);
            ctx.fields.insert(qualified, value.clone());
            ctx.fields
                .entry(field_name.clone())
                .or_insert_with(|| value.clone());
        }
    }
}

/// Find the first row matching all join conditions.
fn find_matching_row(
    rows: &[std::collections::HashMap<String, Value>],
    conds: &[JoinCondPlan],
    ctx: &Event,
) -> Option<std::collections::HashMap<String, Value>> {
    rows.iter()
        .find(|row| row_matches_conds(row, conds, ctx))
        .cloned()
}

/// Find the latest row that matches all conditions AND has timestamp <= event_time.
/// If `within` is specified, also require timestamp >= event_time - within.
fn find_asof_row(
    rows: &[(i64, std::collections::HashMap<String, Value>)],
    conds: &[JoinCondPlan],
    ctx: &Event,
    event_time_nanos: i64,
    within: Option<&Duration>,
) -> Option<std::collections::HashMap<String, Value>> {
    let min_ts = within
        .map(|d| {
            let nanos = i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
            event_time_nanos.saturating_sub(nanos)
        })
        .unwrap_or(i64::MIN);

    rows.iter()
        .filter(|(ts, _)| *ts <= event_time_nanos && *ts >= min_ts)
        .filter(|(_, row)| row_matches_conds(row, conds, ctx))
        .max_by_key(|(ts, _)| *ts)
        .map(|(_, row)| row.clone())
}

/// Check whether a row satisfies all join conditions against the current context.
fn row_matches_conds(
    row: &std::collections::HashMap<String, Value>,
    conds: &[JoinCondPlan],
    ctx: &Event,
) -> bool {
    conds.iter().all(|cond| {
        let left_name = field_ref_name(&cond.left);
        let right_name = field_ref_name(&cond.right);
        match (ctx.fields.get(left_name), row.get(right_name)) {
            (Some(lv), Some(rv)) => values_equal(lv, rv),
            _ => false,
        }
    })
}
