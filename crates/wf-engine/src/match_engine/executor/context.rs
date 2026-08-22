use std::time::Duration;

use wf_lang::ast::{BoundVal, FieldRef, JoinMode};
use wf_lang::plan::{JoinCondPlan, JoinPlan, StepPlan};

use crate::match_engine::JoinRow;
use crate::match_engine::match_engine::{
    AsofLookup, BindData, EngineHashMap, Event, StepData, Value, WindowLookup, eval_expr,
    field_ref_name, values_equal,
};
use crate::time::normalize_epoch_timestamp_float_nanos;

/// Which context fields the close/match alert builders need materialized.
///
/// The score/entity/yield expressions of a rule reference a small, statically
/// known set of field names (match keys, step labels, collected field values).
/// Building every `_step_*` / `_bind_*` synthetic field on every close was a
/// measurable q12 hot spot (each close allocates a fresh `EngineHashMap` and
/// several `format!` keys + `Vec` clones that the output never reads). Rules
/// whose expressions contain function calls (L3 aggregations, window access)
/// fall back to the conservative all-fields build.
#[derive(Debug, Clone)]
pub(crate) enum CloseCtxFields {
    /// Build every synthetic field (expressions may reference `_step_*`,
    /// `_bind_*`, or any collected history).
    All,
    /// Build only the match keys plus these unqualified field names (from
    /// `field_ref_name` of every `Field` reference in score/entity/yield).
    Named(std::collections::HashSet<String>),
}

impl CloseCtxFields {
    fn is_all(&self) -> bool {
        matches!(self, CloseCtxFields::All)
    }

    fn wants(&self, name: &str) -> bool {
        match self {
            CloseCtxFields::All => true,
            CloseCtxFields::Named(set) => set.contains(name),
        }
    }
}

/// Build a synthetic [`Event`] from match context for expression evaluation.
///
/// - Maps `keys[i]` field name → `scope_key[i]` value (original type preserved)
/// - Adds step labels as fields → `label` → `Value::Number(measure_value)`
/// - Labels that collide with key names are silently skipped (keys take priority)
/// - Adds `_step_{i}_values` fields with collected values for L3/aggregate functions
/// - Adds `_step_{i}_measure` and `_step_{i}_label` fields for close-path aggregates
///
/// `needed` narrows the synthetic field set to what the rule's output
/// expressions can actually read; `CloseCtxFields::All` reproduces the
/// historical unconditional build.
pub(crate) fn build_eval_context(
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    bind_data: &[BindData],
    step_plans: &[&StepPlan],
    trigger_event: Option<&Event>,
    needed: &CloseCtxFields,
) -> Event {
    let mut fields = EngineHashMap::default();
    let all = needed.is_all();

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
            && (all || needed.wants(label.as_str()))
        {
            fields.insert(label.clone().into(), Value::Number(sd.measure_value));
        }
        if all {
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
        } else {
            // Narrow build: only the collected bare field names the output
            // expressions reference (`.last()` is the value a `Field` reads).
            for (field_name, values) in &sd.field_values {
                if needed.wants(field_name.as_str())
                    && !fields.contains_key(field_name.as_str())
                    && let Some(last_val) = values.last()
                {
                    fields.insert(field_name.clone().into(), last_val.clone());
                }
            }
        }
    }

    for bd in bind_data {
        let count_field = format!("_bind_{}_count", bd.alias);
        if all || needed.wants(&count_field) {
            fields.insert(count_field.into(), Value::Number(bd.count as f64));
        }
        for (field_name, values) in &bd.field_values {
            if all {
                fields.insert(
                    format!("_bind_{}_field_{}", bd.alias, field_name).into(),
                    Value::Array(values.clone()),
                );
            }
            if needed.wants(field_name.as_str())
                && !fields.contains_key(field_name.as_str())
                && let Some(last_val) = values.last()
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
pub(crate) fn execute_joins(
    joins: &[JoinPlan],
    ctx: &mut Event,
    windows: &dyn WindowLookup,
    event_time_nanos: i64,
) -> bool {
    for join in joins {
        // P3（deferred，`emit at`）由 rule_task deferred 分支处理——eager 路径跳过
        //（设计 §2.2：join 带 emit at → 整条规则转 deferred 输出路径）。
        if join.emit_at.is_some() {
            continue;
        }

        // P2：interval 时间谓词（within）——时间过滤匹配集后按 mode 选择
        //（设计 §5.1：asof_candidates → retain(ts ∈ [lo, hi]) → 存在/首/最新/anti）。
        if join.within.is_some() {
            if !execute_interval_join(join, ctx, windows, event_time_nanos) {
                return false;
            }
            continue;
        }

        let matched_row = match &join.mode {
            JoinMode::Inner => {
                // 缺省 inner（设计 D4）：命中则富化，miss 丢事件
                let Some((key_field, key_val)) = first_join_key(ctx, &join.conds) else {
                    return false;
                };
                let Some(rows) = windows.join_lookup(&join.right_window, &key_field, &key_val)
                else {
                    return false;
                };
                let Some(row) = find_matching_row(&rows, &join.conds, ctx) else {
                    return false;
                };
                Some(row)
            }
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
                // Asof = "latest row ≤ event_time (and ≥ event_time - within)".
                // Single-condition joins get an O(1) fast path through the
                // index's per-key `max_ts`; multi-condition joins (and any
                // window without an index / under a watermark) fall back to the
                // full timestamped candidate scan. `find_asof_row` still applies
                // every condition + the time-proximity filter, so both paths are
                // byte-identical. A `Miss` short-circuits the scan: when the
                // key's max timestamp is already older than the asof window,
                // the scan would return `None` too, so we skip it entirely.
                let Some((key_field, key_val)) = first_join_key(ctx, &join.conds) else {
                    continue;
                };
                if join.conds.len() == 1 {
                    match windows.asof_lookup_max(
                        &join.right_window,
                        &key_field,
                        &key_val,
                        event_time_nanos,
                        within.as_ref(),
                    ) {
                        AsofLookup::Hit(row) => Some(row),
                        AsofLookup::Miss => None,
                        AsofLookup::Fallback => {
                            let Some(rows) =
                                windows.asof_candidates(&join.right_window, &key_field, &key_val)
                            else {
                                continue;
                            };
                            find_asof_row(
                                &rows,
                                &join.conds,
                                ctx,
                                event_time_nanos,
                                within.as_ref(),
                            )
                        }
                    }
                } else {
                    let Some(rows) =
                        windows.asof_candidates(&join.right_window, &key_field, &key_val)
                    else {
                        continue;
                    };
                    find_asof_row(&rows, &join.conds, ctx, event_time_nanos, within.as_ref())
                }
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

        enrich_join_row(ctx, join, &row);
    }
    true
}

/// P2：eager interval join——`within [lo, hi]` 时间谓词过滤匹配集后按 mode 选择。
///
/// 返回 `false` 表示事件应被丢弃（inner miss / anti 命中）；`true` 保留。
/// 界为常量（相对左事件 ts 的时长）或行内表达式（左行绝对时间字段/函数）。
fn execute_interval_join(
    join: &JoinPlan,
    ctx: &mut Event,
    windows: &dyn WindowLookup,
    event_time_nanos: i64,
) -> bool {
    let wspec = join.within.as_ref().expect("caller checks within");

    // 区间界求值失败（左行缺字段等）→ 保守按 miss 处理
    let Some(lo_ns) = eval_interval_bound(&wspec.lo, ctx, event_time_nanos) else {
        return !matches!(join.mode, JoinMode::Inner);
    };
    let Some(hi_ns) = eval_interval_bound(&wspec.hi, ctx, event_time_nanos) else {
        return !matches!(join.mode, JoinMode::Inner);
    };

    let Some((key_field, key_val)) = first_join_key(ctx, &join.conds) else {
        return !matches!(join.mode, JoinMode::Inner);
    };
    let Some(rows) = windows.asof_candidates(&join.right_window, &key_field, &key_val) else {
        return !matches!(join.mode, JoinMode::Inner);
    };

    // 时间谓词 + 全部 join 条件（复刻 find_matching_row 的逐条件复核）
    let matched: Vec<&(i64, JoinRow)> = rows
        .iter()
        .filter(|(ts, row)| {
            in_interval(*ts, lo_ns, hi_ns, wspec.lo.open, wspec.hi.open)
                && row_matches_conds(row, &join.conds, ctx)
        })
        .collect();

    match &join.mode {
        JoinMode::Anti => {
            // 区间内有匹配 → 丢事件
            matched.is_empty()
        }
        JoinMode::Asof { .. } => {
            // 最新（ts 最大）
            let row = matched
                .into_iter()
                .max_by_key(|(ts, _)| *ts)
                .map(|(_, r)| r.clone());
            let Some(row) = row else {
                return true;
            };
            enrich_join_row(ctx, join, &row);
            true
        }
        JoinMode::Snapshot | JoinMode::Inner => {
            // 首匹配（ts 最小，确定性）；inner miss → 丢事件
            let row = matched
                .into_iter()
                .min_by_key(|(ts, _)| *ts)
                .map(|(_, r)| r.clone());
            let Some(row) = row else {
                return !matches!(join.mode, JoinMode::Inner);
            };
            enrich_join_row(ctx, join, &row);
            true
        }
        _ => true,
    }
}

/// 区间谓词：`lo ≤ ts ≤ hi`（`open` 记号取开区间）。
pub(crate) fn in_interval(ts: i64, lo_ns: i64, hi_ns: i64, lo_open: bool, hi_open: bool) -> bool {
    let lo_ok = if lo_open { ts > lo_ns } else { ts >= lo_ns };
    let hi_ok = if hi_open { ts < hi_ns } else { ts <= hi_ns };
    lo_ok && hi_ok
}

/// 区间界求值 → 纳秒：常量界相对左事件 ts（Dur，可负）；行内界为左行绝对时间表达式。
pub(crate) fn eval_interval_bound(
    bound: &wf_lang::ast::Bound,
    ctx: &Event,
    event_time_nanos: i64,
) -> Option<i64> {
    match &bound.val {
        BoundVal::Dur { dur, neg } => {
            let offset = i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX);
            let offset = if *neg { -offset } else { offset };
            Some(event_time_nanos.saturating_add(offset))
        }
        BoundVal::Expr(e) => {
            let value = eval_expr(e, ctx)?;
            match value {
                Value::Number(n) => normalize_epoch_timestamp_float_nanos(n),
                _ => None,
            }
        }
    }
}

/// 把匹配行的字段物化进 eval context（限定名 `window.field` + 裸名）。
pub(crate) fn enrich_join_row(ctx: &mut Event, join: &JoinPlan, row: &JoinRow) {
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

/// Extract the first join condition's `(right key field, left value)`, so the
/// join can use a hash-index lookup for the primary key condition before
/// filtering by any remaining conditions.
pub(crate) fn first_join_key(ctx: &Event, conds: &[JoinCondPlan]) -> Option<(String, Value)> {
    let cond = conds.first()?;
    let left_name = field_ref_name(&cond.left);
    let val = ctx.fields.get(left_name)?.clone();
    Some((field_ref_name(&cond.right).to_string(), val))
}

/// Find the first row matching all join conditions.
fn find_matching_row(rows: &[JoinRow], conds: &[JoinCondPlan], ctx: &Event) -> Option<JoinRow> {
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
pub(crate) fn row_matches_conds(row: &JoinRow, conds: &[JoinCondPlan], ctx: &Event) -> bool {
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
    fn execute_joins_asof_miss_keeps_event_without_enrichment() {
        use std::collections::HashSet;

        // A lookup that always reports `Miss` (the key's max timestamp is older
        // than the asof lower bound). The event must be kept (join is optional)
        // but not enriched with joined fields.
        struct MissLookup;
        impl WindowLookup for MissLookup {
            fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
                None
            }
            fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
                None
            }
            fn asof_lookup_max(
                &self,
                _w: &str,
                _key_field: &str,
                _key: &Value,
                _event_time_nanos: i64,
                _within: Option<&Duration>,
            ) -> AsofLookup {
                AsofLookup::Miss
            }
        }

        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("bidder".into(), Value::Number(1.0));

        let joins = vec![JoinPlan {
            right_window: "person_events".to_string(),
            mode: JoinMode::Asof {
                within: Some(Duration::from_secs(300)),
            },
            conds: vec![JoinCondPlan {
                left: FieldRef::Simple("bidder".to_string()),
                right: FieldRef::Simple("id".to_string()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];

        let ok = execute_joins(&joins, &mut ctx, &MissLookup, 500_000_000_000);
        assert!(ok, "an asof Miss must keep the event");
        assert!(
            !ctx.fields.contains_key("person_events.id"),
            "an asof Miss must not enrich the joined fields"
        );
    }

    #[test]
    fn execute_joins_asof_hit_enriches_event() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let joined = Arc::new(Event {
            fields: {
                let mut f = EngineHashMap::default();
                f.insert("id".into(), Value::Number(1.0));
                f.insert("name".into(), Value::Str("person".into()));
                f
            },
        });

        struct HitLookup(Arc<Event>);
        impl WindowLookup for HitLookup {
            fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
                None
            }
            fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
                None
            }
            fn asof_lookup_max(
                &self,
                _w: &str,
                _key_field: &str,
                _key: &Value,
                _event_time_nanos: i64,
                _within: Option<&Duration>,
            ) -> AsofLookup {
                AsofLookup::Hit(JoinRow::Event(Arc::clone(&self.0)))
            }
        }

        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("bidder".into(), Value::Number(1.0));

        let joins = vec![JoinPlan {
            right_window: "person_events".to_string(),
            mode: JoinMode::Asof {
                within: Some(Duration::from_secs(300)),
            },
            conds: vec![JoinCondPlan {
                left: FieldRef::Simple("bidder".to_string()),
                right: FieldRef::Simple("id".to_string()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];

        let ok = execute_joins(&joins, &mut ctx, &HitLookup(joined), 500_000_000_000);
        assert!(ok, "an asof Hit must keep the event");
        assert_eq!(
            ctx.fields.get("person_events.id"),
            Some(&Value::Number(1.0)),
            "qualified joined field must be enriched"
        );
        assert_eq!(
            ctx.fields.get("id"),
            Some(&Value::Number(1.0)),
            "plain joined field must be enriched"
        );
    }

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

        let event = build_eval_context(
            &keys,
            &scope_key,
            &step_data,
            &bind_data,
            &step_plans,
            None,
            &CloseCtxFields::All,
        );
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
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                ])),
            ],
        )
        .unwrap();

        let col_rows = columnar_join_rows(vec![batch.clone()], None);
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
                assert_eq!(
                    c.field_value(name),
                    m.get(name).cloned(),
                    "row {i} field {name}"
                );
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
                    fields: row.into_iter().map(|(k, v)| (k.into(), v)).collect(),
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

    // -------------------------------------------------------------------
    // P2：interval join（within 时间谓词，eager）
    // -------------------------------------------------------------------

    use std::sync::Arc;
    use wf_lang::ast::{Bound, BoundVal, Expr, WithinSpec};

    /// 带时间戳的候选 lookup（测试替身：`asof_candidates` 直接返回全部行）。
    struct TimedLookup(Vec<(i64, JoinRow)>);
    impl WindowLookup for TimedLookup {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
            Some(self.0.iter().map(|(_, r)| r.clone()).collect())
        }
        fn asof_candidates(
            &self,
            _w: &str,
            _key_field: &str,
            _key: &Value,
        ) -> Option<Vec<(i64, JoinRow)>> {
            Some(self.0.clone())
        }
    }

    /// 右窗行：`(ts, id, price)`。
    fn timed_row(ts: i64, id: f64, price: f64) -> (i64, JoinRow) {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), Value::Number(id));
        fields.insert("price".into(), Value::Number(price));
        (ts, JoinRow::Event(Arc::new(Event { fields })))
    }

    /// `on aid == right.id` 的单条件 join。
    fn interval_join(within: Option<WithinSpec>, mode: JoinMode) -> JoinPlan {
        JoinPlan {
            right_window: "bid_events".into(),
            mode,
            conds: vec![JoinCondPlan {
                left: FieldRef::Simple("aid".into()),
                right: FieldRef::Simple("id".into()),
            }],
            within,
            reduce: None,
            emit_at: None,
        }
    }

    /// `within [-10s, 0s]`（`within 10s` 糖的等价常量界）。
    fn within_lookback() -> WithinSpec {
        WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_secs(10),
                    neg: true,
                },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::ZERO,
                    neg: false,
                },
            },
        }
    }

    #[test]
    fn interval_inner_hit_enriches_and_miss_drops() {
        // t=500s 的事件，回看 10s：行 ts∈[490s, 500s]
        let rows = vec![
            timed_row(485_000_000_000, 1.0, 100.0),
            timed_row(495_000_000_000, 1.0, 200.0),
            timed_row(499_000_000_000, 1.0, 300.0),
        ];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));

        let joins = vec![interval_join(Some(within_lookback()), JoinMode::Inner)];
        let ok = execute_joins(&joins, &mut ctx, &lookup, 500_000_000_000);
        assert!(ok, "interval inner hit keeps event");
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(200.0)));

        // miss：ts=485s 落在 [490s, 500s] 之外 → 丢事件
        let lookup = TimedLookup(vec![timed_row(485_000_000_000, 1.0, 100.0)]);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within_lookback()), JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(!ok, "interval inner miss drops event");
    }

    #[test]
    fn interval_snapshot_picks_earliest() {
        let rows = vec![
            timed_row(495_000_000_000, 1.0, 200.0),
            timed_row(499_000_000_000, 1.0, 300.0),
            timed_row(497_000_000_000, 1.0, 250.0),
        ];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within_lookback()), JoinMode::Snapshot)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok);
        // 区间内最早 = ts 495s（price 200）
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(200.0)));
    }

    #[test]
    fn interval_asof_picks_latest() {
        let rows = vec![
            timed_row(495_000_000_000, 1.0, 200.0),
            timed_row(499_000_000_000, 1.0, 300.0),
        ];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(
                Some(within_lookback()),
                JoinMode::Asof { within: None },
            )],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok);
        // 区间内最新 = ts 499s（price 300）
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(300.0)));
    }

    #[test]
    fn interval_anti_drops_on_interval_match() {
        let rows = vec![timed_row(495_000_000_000, 1.0, 200.0)];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within_lookback()), JoinMode::Anti)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(
            !ok,
            "interval anti drops when a row matches in the interval"
        );

        // 区间外 → 保留
        let lookup = TimedLookup(vec![timed_row(485_000_000_000, 1.0, 200.0)]);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within_lookback()), JoinMode::Anti)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok, "interval anti keeps event when no row in interval");
    }

    #[test]
    fn interval_open_upper_bound_excludes_boundary() {
        // `[490s, <500s)`：恰在 500s 的行不匹配
        let within = WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_secs(10),
                    neg: true,
                },
            },
            hi: Bound {
                open: true,
                val: BoundVal::Dur {
                    dur: Duration::ZERO,
                    neg: false,
                },
            },
        };
        let lookup = TimedLookup(vec![timed_row(500_000_000_000, 1.0, 300.0)]);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within), JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(!ok, "upper-open bound excludes the boundary ts");
    }

    #[test]
    fn interval_field_bounds_use_left_row_absolute_time() {
        // 行内界（左行绝对时间字段）：`within [lo_f, hi_f]`，右行 ts ∈ [lo_f, hi_f]
        let within = WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("lo_f".into()))),
            },
            hi: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("hi_f".into()))),
            },
        };
        let rows = vec![
            timed_row(492_000_000_000_000_000, 1.0, 100.0),
            timed_row(494_000_000_000_000_000, 1.0, 200.0),
        ];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        ctx.fields
            .insert("lo_f".into(), Value::Number(490_000_000_000_000_000.0));
        ctx.fields
            .insert("hi_f".into(), Value::Number(493_000_000_000_000_000.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within), JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok, "row ts=492s inside [490s, 493s] matches");
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(100.0)));
    }

    #[test]
    fn deferred_emit_at_join_skipped_on_eager_path() {
        // `emit at`（P3 deferred）不在 eager 路径执行：事件保留、不富化。
        let mut join = interval_join(Some(within_lookback()), JoinMode::Inner);
        join.emit_at = Some(Expr::Field(FieldRef::Simple("expires".into())));
        let lookup = TimedLookup(vec![timed_row(495_000_000_000, 1.0, 200.0)]);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(&vec![join], &mut ctx, &lookup, 500_000_000_000);
        assert!(
            ok,
            "deferred join must not drop the event on the eager path"
        );
        assert!(
            !ctx.fields.contains_key("price"),
            "deferred join must not enrich on the eager path"
        );
    }

    /// 多条件 interval join：先按首条件键查，再逐条件复核（复刻 find_matching_row）。
    #[test]
    fn interval_multi_condition_rechecks_all_conds() {
        let rows = vec![
            // id 命中但 extra 不匹配（aid=1, extra=9）
            timed_row_extra(495_000_000_000, 1.0, 200.0, 9.0),
            // 全部条件命中
            timed_row_extra(496_000_000_000, 1.0, 250.0, 7.0),
        ];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        ctx.fields.insert("extra".into(), Value::Number(7.0));
        let join = JoinPlan {
            right_window: "bid_events".to_string(),
            mode: JoinMode::Inner,
            conds: vec![
                JoinCondPlan {
                    left: FieldRef::Simple("aid".into()),
                    right: FieldRef::Simple("id".into()),
                },
                JoinCondPlan {
                    left: FieldRef::Simple("extra".into()),
                    right: FieldRef::Simple("extra".into()),
                },
            ],
            within: Some(within_lookback()),
            reduce: None,
            emit_at: None,
        };
        let ok = execute_joins(&vec![join], &mut ctx, &lookup, 500_000_000_000);
        assert!(ok);
        // 仅 extra=7 的行通过全部条件
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(250.0)));
    }

    /// 闭区间：ts 恰在 lo / hi 边界上必须匹配。
    #[test]
    fn interval_closed_bounds_include_boundaries() {
        let rows = vec![
            timed_row(490_000_000_000, 1.0, 100.0), // == lo（t-10s）
            timed_row(500_000_000_000, 1.0, 200.0), // == hi（t）
        ];
        let lookup = TimedLookup(rows);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within_lookback()), JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok, "closed interval must include both boundary ts");
        // 最早 = ts 490s
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(100.0)));
    }

    /// snapshot interval miss：事件保留、不富化（与既有 snapshot 可选语义一致）。
    #[test]
    fn interval_snapshot_miss_keeps_event() {
        let lookup = TimedLookup(vec![timed_row(480_000_000_000, 1.0, 100.0)]);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(Some(within_lookback()), JoinMode::Snapshot)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok, "snapshot interval miss must keep the event");
        assert!(
            !ctx.fields.contains_key("price"),
            "snapshot interval miss must not enrich"
        );
    }

    /// 多 join 混合：interval inner 命中后继续处理下一个 plain snapshot join。
    #[test]
    fn interval_join_then_plain_join_both_enrich() {
        // 第二个 plain snapshot join 走 join_lookup（TimedLookup 只覆写 asof_candidates，
        // join_lookup 默认实现走 snapshot → 需要 snapshot 覆写）——这里直接构造组合 lookup
        struct TwoLookup;
        impl WindowLookup for TwoLookup {
            fn snapshot_field_values(
                &self,
                _w: &str,
                _f: &str,
            ) -> Option<std::collections::HashSet<String>> {
                None
            }
            fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
                let mut fields = EngineHashMap::default();
                fields.insert("rid".into(), Value::Number(1.0));
                fields.insert("region".into(), Value::Str("cn".into()));
                Some(vec![JoinRow::Event(Arc::new(Event { fields }))])
            }
            fn asof_candidates(
                &self,
                _w: &str,
                _key_field: &str,
                _key: &Value,
            ) -> Option<Vec<(i64, JoinRow)>> {
                Some(vec![timed_row(495_000_000_000, 1.0, 200.0)])
            }
        }

        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let joins = vec![
            interval_join(Some(within_lookback()), JoinMode::Inner),
            JoinPlan {
                right_window: "region_tbl".to_string(),
                mode: JoinMode::Snapshot,
                conds: vec![JoinCondPlan {
                    left: FieldRef::Simple("aid".into()),
                    right: FieldRef::Simple("rid".into()),
                }],
                within: None,
                reduce: None,
                emit_at: None,
            },
        ];
        let ok = execute_joins(&joins, &mut ctx, &TwoLookup, 500_000_000_000);
        assert!(ok);
        // interval join 富化 price
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(200.0)));
        // plain snapshot join 富化 region（裸名 or_insert——price 已存在，region 新增）
        assert_eq!(ctx.fields.get("region"), Some(&Value::Str("cn".into())));
    }

    /// 右窗行：`(ts, id, price, extra)`。
    fn timed_row_extra(ts: i64, id: f64, price: f64, extra: f64) -> (i64, JoinRow) {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), Value::Number(id));
        fields.insert("price".into(), Value::Number(price));
        fields.insert("extra".into(), Value::Number(extra));
        (ts, JoinRow::Event(Arc::new(Event { fields })))
    }

    #[test]
    fn inner_mode_without_interval_drops_on_miss() {
        // 缺省 inner（无 within）：命中富化、miss 丢（设计 D4）
        struct OneRowLookup(JoinRow);
        impl WindowLookup for OneRowLookup {
            fn snapshot_field_values(
                &self,
                _w: &str,
                _f: &str,
            ) -> Option<std::collections::HashSet<String>> {
                None
            }
            fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
                Some(vec![self.0.clone()])
            }
        }
        let row = timed_row(0, 1.0, 200.0).1;

        // miss
        let lookup = OneRowLookup(row.clone());
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(9.0));
        let ok = execute_joins(
            &vec![interval_join(None, JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(!ok, "plain inner miss drops event");

        // hit
        let lookup = OneRowLookup(row);
        let mut ctx = Event {
            fields: EngineHashMap::default(),
        };
        ctx.fields.insert("aid".into(), Value::Number(1.0));
        let ok = execute_joins(
            &vec![interval_join(None, JoinMode::Inner)],
            &mut ctx,
            &lookup,
            500_000_000_000,
        );
        assert!(ok, "plain inner hit keeps event");
        assert_eq!(ctx.fields.get("price"), Some(&Value::Number(200.0)));
    }
}
