use wf_lang::ast::FieldRef;
use wf_lang::plan::JoinPlan;

use crate::rule::match_engine::{
    Event, StepData, Value, WindowLookup, field_ref_name, values_equal,
};

/// Build a synthetic [`Event`] from match context for expression evaluation.
///
/// - Maps `keys[i]` field name → `scope_key[i]` value (original type preserved)
/// - Adds step labels as fields → `label` → `Value::Number(measure_value)`
/// - Labels that collide with key names are silently skipped (keys take priority)
pub(super) fn build_eval_context(
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
) -> Event {
    let mut fields = std::collections::HashMap::new();

    // Key fields — preserve original Value type
    for (fr, val) in keys.iter().zip(scope_key.iter()) {
        let name = field_ref_name(fr).to_string();
        fields.insert(name, val.clone());
    }

    // Step labels → measure values (skip if name collides with a key field)
    for sd in step_data {
        if let Some(label) = &sd.label
            && !fields.contains_key(label.as_str())
        {
            fields.insert(label.clone(), Value::Number(sd.measure_value));
        }
    }

    Event { fields }
}

/// Execute join plans, enriching the eval context with joined fields.
///
/// For each join, snapshots the right window and finds the first row
/// matching all join conditions. Matched fields are added to the context
/// both as `window.field` (qualified) and as plain `field` (if not already present).
///
/// Both `Snapshot` and `Asof` modes currently use the same row-matching logic.
/// Time-based asof refinement will be added in L3.
pub(super) fn execute_joins(joins: &[JoinPlan], ctx: &mut Event, windows: &dyn WindowLookup) {
    for join in joins {
        let Some(rows) = windows.snapshot(&join.right_window) else {
            continue;
        };

        let Some(row) = find_matching_row(&rows, &join.conds, ctx) else {
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
    conds: &[wf_lang::plan::JoinCondPlan],
    ctx: &Event,
) -> Option<std::collections::HashMap<String, Value>> {
    rows.iter()
        .find(|row| {
            conds.iter().all(|cond| {
                let left_name = field_ref_name(&cond.left);
                let right_name = field_ref_name(&cond.right);
                match (ctx.fields.get(left_name), row.get(right_name)) {
                    (Some(lv), Some(rv)) => values_equal(lv, rv),
                    _ => false,
                }
            })
        })
        .cloned()
}
