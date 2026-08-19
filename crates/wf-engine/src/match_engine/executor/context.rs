use std::time::Duration;

use wf_lang::ast::{FieldRef, JoinMode};
use wf_lang::plan::{JoinCondPlan, JoinPlan, StepPlan};

use crate::match_engine::match_engine::{
    BindData, EngineHashMap, Event, StepData, Value, WindowLookup, field_ref_name, values_equal,
};
use crate::match_engine::JoinRow;

/// Build a synthetic [`Event`] from match context for expression evaluation.
///
/// - Maps `keys[i]` field name → `scope_key[i]` value (original type preserved)
/// - Adds step labels as fields → `label` → `Value::Number(measure_value)`
/// - Labels that collide with key names are silently skipped (keys take priority)
/// - Adds `_step_{i}_values` fields with collected values for L3/aggregate functions
/// - Adds `_step_{i}_measure` and `_step_{i}_label` fields for close-path aggregates
pub(super) fn build_eval_context(
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    bind_data: &[BindData],
    step_plans: &[&StepPlan],
    trigger_event: Option<&Event>,
) -> Event {
    let mut fields = EngineHashMap::default();

    // Key fields — preserve original Value type
    for (fr, val) in keys.iter().zip(scope_key.iter()) {
        let name = field_ref_name(fr).to_string();
        fields.insert(name.into(), val.clone());
    }

    // Scalar fields from the triggering event (on-event fires): yields like
    // `b.auction` resolve from it directly. Rules that don't collect the
    // `field_values` history (needs_field_history=false) rely on this; the
    // step_data loop below skips a field already present (`contains_key`), so
    // the history never overrides the event's value. Close fires pass `None`
    // and keep reading from `field_values`.
    if let Some(ev) = trigger_event {
        for (name, value) in &ev.fields {
            if !fields.contains_key(name.as_str()) {
                fields.insert(name.clone(), value.clone());
            }
        }
    }

    // Step labels → measure values (skip if name collides with a key field)
    // Also store collected values for L3 functions
    for (step_idx, sd) in step_data.iter().enumerate() {
        if let Some(label) = &sd.label
            && !fields.contains_key(label.as_str())
        {
            fields.insert(label.clone().into(), Value::Number(sd.measure_value));
        }
        // Store collected values for L3 functions (collect_set/list, first/last, stddev/percentile)
        let values_field = format!("_step_{}_values", step_idx);
        let values_array = Value::Array(sd.collected_values.clone());
        fields.insert(values_field.into(), values_array);
        for (field_name, values) in &sd.field_values {
            let step_field = format!("_step_{}_field_{}", step_idx, field_name);
            fields.insert(step_field.into(), Value::Array(values.clone()));
            if let Some(last_val) = values.last()
                && !fields.contains_key(field_name.as_str())
            {
                fields.insert(field_name.clone().into(), last_val.clone());
            }
        }
        let measure_field = format!("_step_{}_measure", step_idx);
        fields.insert(measure_field.into(), Value::Number(sd.measure_value));
        if let Some(label) = &sd.label {
            let label_field = format!("_step_{}_label", step_idx);
            fields.insert(label_field.into(), Value::Str(label.clone().into()));
        }
        if let Some(step_plan) = step_plans.get(step_idx)
            && let Some(branch) = step_plan.branches.get(sd.satisfied_branch_index)
        {
            let source_field = format!("_step_{}_source", step_idx);
            fields.insert(
                source_field.into(),
                Value::Str(branch.source.clone().into()),
            );
        }
    }

    for bd in bind_data {
        fields.insert(
            format!("_bind_{}_count", bd.alias).into(),
            Value::Number(bd.count as f64),
        );
        for (field_name, values) in &bd.field_values {
            fields.insert(
                format!("_bind_{}_field_{}", bd.alias, field_name).into(),
                Value::Array(values.clone()),
            );
            if let Some(last_val) = values.last()
                && !fields.contains_key(field_name.as_str())
            {
                fields.insert(field_name.clone().into(), last_val.clone());
            }
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
/// Execute join plans. Returns `true` if the event should be kept,
/// `false` if it should be dropped (anti join matched).
pub(super) fn execute_joins(
    joins: &[JoinPlan],
    ctx: &mut Event,
    windows: &dyn WindowLookup,
    event_time_nanos: i64,
) -> bool {
    for join in joins {
        let matched_row = match &join.mode {
            JoinMode::Snapshot => {
                let Some((key_field, key_val)) = first_join_key(ctx, &join.conds) else {
                    continue;
                };
                let Some(rows) = windows.join_lookup(&join.right_window, &key_field, &key_val)
                else {
                    continue;
                };
                find_matching_row(&rows, &join.conds, ctx)
            }
            JoinMode::Asof { within } => {
                // asof still uses the timestamped scan path (the hash index is
                // not timestamp-aware yet).
                let Some(rows) = windows.snapshot_with_timestamps(&join.right_window) else {
                    continue;
                };
                find_asof_row(&rows, &join.conds, ctx, event_time_nanos, within.as_ref())
            }
            JoinMode::Anti => {
                let Some((key_field, key_val)) = first_join_key(ctx, &join.conds) else {
                    continue;
                };
                let Some(rows) = windows.join_lookup(&join.right_window, &key_field, &key_val)
                else {
                    // No anti-join window data yet — keep event
                    continue;
                };
                // Anti join: if a matching row is found, drop the event
                if find_matching_row(&rows, &join.conds, ctx).is_some() {
                    return false;
                }
                // No match — keep event, skip enrichment
                continue;
            }
            _ => {
                continue;
            }
        };

        let Some(row) = matched_row else {
            continue;
        };

        // Materialize the matched row's fields into the eval context — only the
        // matched row, on demand (JoinRow reads straight from the columns).
        for field_name in row.field_names() {
            let Some(value) = row.field_value(field_name) else {
                continue;
            };
            let qualified = format!("{}.{}", join.right_window, field_name);
            ctx.fields.insert(qualified.into(), value.clone());
            ctx.fields
                .entry(field_name.to_string().into())
                .or_insert_with(|| value.clone());
        }
    }
    true
}

/// Extract the first join condition's `(right key field, left value)`, so the
/// join can use a hash-index lookup for the primary key condition before
/// filtering by any remaining conditions.
fn first_join_key(ctx: &Event, conds: &[JoinCondPlan]) -> Option<(String, Value)> {
    let cond = conds.first()?;
    let left_name = field_ref_name(&cond.left);
    let val = ctx.fields.get(left_name)?.clone();
    Some((field_ref_name(&cond.right).to_string(), val))
}

/// Find the first row matching all join conditions.
fn find_matching_row(
    rows: &[JoinRow],
    conds: &[JoinCondPlan],
    ctx: &Event,
) -> Option<JoinRow> {
    rows.iter()
        .find(|row| row_matches_conds(row, conds, ctx))
        .cloned()
}

/// Find the latest row that matches all conditions AND has timestamp <= event_time.
/// If `within` is specified, also require timestamp >= event_time - within.
fn find_asof_row(
    rows: &[(i64, JoinRow)],
    conds: &[JoinCondPlan],
    ctx: &Event,
    event_time_nanos: i64,
    within: Option<&Duration>,
) -> Option<JoinRow> {
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
fn row_matches_conds(row: &JoinRow, conds: &[JoinCondPlan], ctx: &Event) -> bool {
    conds.iter().all(|cond| {
        let left_name = field_ref_name(&cond.left);
        let right_name = field_ref_name(&cond.right);
        match (ctx.fields.get(left_name), row.field_value(right_name)) {
            (Some(lv), Some(rv)) => values_equal(lv, &rv),
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_field_names_from_bind_data() {
        let mut field_values = EngineHashMap::default();
        field_values.insert("dip".to_string(), vec![Value::Str("7.180.78.236".into())]);
        field_values.insert("user".to_string(), vec![Value::Str("root".into())]);
        let bind_data = vec![BindData {
            alias: "e".into(),
            count: 15,
            field_values,
        }];
        let keys: Vec<FieldRef> = vec![FieldRef::Simple("sip".into())];
        let scope_key = vec![Value::Str("10.0.0.1".into())];
        let step_data: Vec<StepData> = vec![];
        let step_plans: Vec<&StepPlan> = vec![];

        let event =
            build_eval_context(&keys, &scope_key, &step_data, &bind_data, &step_plans, None);
        assert_eq!(
            event.fields.get("sip"),
            Some(&Value::Str("10.0.0.1".into()))
        );
        assert_eq!(
            event.fields.get("dip"),
            Some(&Value::Str("7.180.78.236".into()))
        );
        assert_eq!(event.fields.get("user"), Some(&Value::Str("root".into())));
        assert!(event.fields.contains_key("_bind_e_field_dip"));
    }

    #[test]
    fn columnar_join_row_matches_materialized_path() {
        // The columnar `JoinRow` (scan fallback) must be byte-identical to the
        // materialized `HashMap` rows the old path produced: same field values
        // (null → absent), same `find_matching_row` result, same enrichment set.
        use std::collections::HashMap;
        use std::sync::Arc;

        use arrow::array::{BooleanArray, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use crate::match_engine::{JoinRow, batch_to_events, columnar_join_rows};

        let schema = Arc::new(Schema::new(vec![
            Field::new("ip", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("10.0.0.1"),
                    None,
                    Some("10.0.0.2"),
                ])),
                Arc::new(Int64Array::from(vec![Some(80), Some(95), Some(100)])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), Some(true)])),
            ],
        )
        .unwrap();

        let col_rows = columnar_join_rows(vec![batch.clone()]);
        let map_rows: Vec<HashMap<String, Value>> = batch_to_events(&batch)
            .into_iter()
            .map(|ev| {
                ev.fields
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect()
            })
            .collect();
        assert_eq!(col_rows.len(), map_rows.len());

        // Per-row, per-field parity (null cell → None / absent).
        for (i, (c, m)) in col_rows.iter().zip(&map_rows).enumerate() {
            for name in ["ip", "score", "active"] {
                assert_eq!(c.field_value(name), m.get(name).cloned(), "row {i} field {name}");
            }
            for name in c.field_names() {
                assert_eq!(
                    c.field_value(name).is_some(),
                    m.contains_key(name),
                    "row {i} name {name} presence"
                );
            }
        }

        // `find_matching_row` agrees between the columnar view and the
        // materialized `Event` wrapper (the old HashMap path).
        let mut ctx_fields = EngineHashMap::default();
        ctx_fields.insert("ip".into(), Value::Str("10.0.0.2".into()));
        ctx_fields.insert("score".into(), Value::Number(100.0));
        let ctx = Event { fields: ctx_fields };
        let conds = vec![
            JoinCondPlan {
                left: FieldRef::Simple("ip".into()),
                right: FieldRef::Simple("ip".into()),
            },
            JoinCondPlan {
                left: FieldRef::Simple("score".into()),
                right: FieldRef::Simple("score".into()),
            },
        ];
        let event_rows: Vec<JoinRow> = map_rows
            .into_iter()
            .map(|row| {
                JoinRow::Event(Arc::new(Event {
                    fields: row
                        .into_iter()
                        .map(|(k, v)| (k.into(), v))
                        .collect(),
                }))
            })
            .collect();
        let matched_col = find_matching_row(&col_rows, &conds, &ctx).expect("columnar match");
        let matched_event = find_matching_row(&event_rows, &conds, &ctx).expect("event match");
        for name in ["ip", "score", "active"] {
            assert_eq!(
                matched_col.field_value(name),
                matched_event.field_value(name),
                "matched field {name}"
            );
        }
        // The columnar matched row is row 2 (ip=10.0.0.2, score=100).
        assert_eq!(
            matched_col.field_value("ip"),
            Some(Value::Str("10.0.0.2".into()))
        );
        assert_eq!(matched_col.field_value("score"), Some(Value::Number(100.0)));
    }
}
