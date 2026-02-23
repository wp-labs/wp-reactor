use std::sync::atomic::{AtomicU64, Ordering};

use orion_error::prelude::*;

use wf_lang::ast::FieldRef;
use wf_lang::plan::{JoinPlan, RulePlan};

use crate::alert::AlertRecord;
use crate::error::{CoreReason, CoreResult};
use crate::rule::match_engine::{
    CloseOutput, Event, MatchedContext, StepData, Value, WindowLookup, eval_expr, field_ref_name,
    value_to_string, values_equal,
};

// ---------------------------------------------------------------------------
// RuleExecutor
// ---------------------------------------------------------------------------

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`AlertRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
pub struct RuleExecutor {
    plan: RulePlan,
}

impl RuleExecutor {
    pub fn new(plan: RulePlan) -> Self {
        Self { plan }
    }

    pub fn plan(&self) -> &RulePlan {
        &self.plan
    }

    /// Produce an [`AlertRecord`] from an on-event match (L1 — no joins).
    pub fn execute_match(&self, matched: &MatchedContext) -> CoreResult<AlertRecord> {
        let ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
        );
        self.build_match_alert(matched, &ctx)
    }

    /// Produce an [`AlertRecord`] from an on-event match with join support.
    ///
    /// Executes joins before score/entity evaluation, enriching the eval
    /// context with joined fields from external windows.
    pub fn execute_match_with_joins(
        &self,
        matched: &MatchedContext,
        windows: &dyn WindowLookup,
    ) -> CoreResult<AlertRecord> {
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
        );
        execute_joins(&self.plan.joins, &mut ctx, windows);
        self.build_match_alert(matched, &ctx)
    }

    /// Internal: build the AlertRecord from an already-constructed eval context.
    fn build_match_alert(&self, matched: &MatchedContext, ctx: &Event) -> CoreResult<AlertRecord> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let fired_at = format_nanos_utc(matched.event_time_nanos);
        let alert_id = build_alert_id(&self.plan.name, &matched.scope_key, &fired_at);
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            None,
        );

        Ok(AlertRecord {
            alert_id,
            rule_name: self.plan.name.clone(),
            score,
            entity_type: self.plan.entity_plan.entity_type.clone(),
            entity_id,
            close_reason: None,
            fired_at,
            matched_rows: vec![],
            summary,
        })
    }

    /// Produce an [`AlertRecord`] from a close output (L1 — no joins).
    ///
    /// Returns `Ok(None)` when `!event_ok || !close_ok` — the instance
    /// did not fully satisfy the rule.
    pub fn execute_close(&self, close: &CloseOutput) -> CoreResult<Option<AlertRecord>> {
        if !close.event_ok || !close.close_ok {
            return Ok(None);
        }
        let all_step_data = combine_step_data(close);
        let ctx = build_eval_context(&self.plan.match_plan.keys, &close.scope_key, &all_step_data);
        self.build_close_alert(close, &all_step_data, &ctx)
    }

    /// Produce an [`AlertRecord`] from a close output with join support.
    pub fn execute_close_with_joins(
        &self,
        close: &CloseOutput,
        windows: &dyn WindowLookup,
    ) -> CoreResult<Option<AlertRecord>> {
        if !close.event_ok || !close.close_ok {
            return Ok(None);
        }
        let all_step_data = combine_step_data(close);
        let mut ctx =
            build_eval_context(&self.plan.match_plan.keys, &close.scope_key, &all_step_data);
        execute_joins(&self.plan.joins, &mut ctx, windows);
        self.build_close_alert(close, &all_step_data, &ctx)
    }

    /// Internal: build the AlertRecord from an already-constructed eval context.
    fn build_close_alert(
        &self,
        close: &CloseOutput,
        all_step_data: &[StepData],
        ctx: &Event,
    ) -> CoreResult<Option<AlertRecord>> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let close_reason_str = close.close_reason.as_str().to_string();
        let fired_at = format_nanos_utc(close.watermark_nanos);
        let alert_id = build_alert_id(&self.plan.name, &close.scope_key, &fired_at);
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &close.scope_key,
            all_step_data,
            Some(&close_reason_str),
        );

        Ok(Some(AlertRecord {
            alert_id,
            rule_name: self.plan.name.clone(),
            score,
            entity_type: self.plan.entity_plan.entity_type.clone(),
            entity_id,
            close_reason: Some(close_reason_str),
            fired_at,
            matched_rows: vec![],
            summary,
        }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a synthetic [`Event`] from match context for expression evaluation.
///
/// - Maps `keys[i]` field name → `scope_key[i]` value (original type preserved)
/// - Adds step labels as fields → `label` → `Value::Number(measure_value)`
/// - Labels that collide with key names are silently skipped (keys take priority)
fn build_eval_context(keys: &[FieldRef], scope_key: &[Value], step_data: &[StepData]) -> Event {
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

/// Combine event + close step data into a single vec.
fn combine_step_data(close: &CloseOutput) -> Vec<StepData> {
    close
        .event_step_data
        .iter()
        .chain(close.close_step_data.iter())
        .cloned()
        .collect()
}

/// Execute join plans, enriching the eval context with joined fields.
///
/// For each join, snapshots the right window and finds the first row
/// matching all join conditions. Matched fields are added to the context
/// both as `window.field` (qualified) and as plain `field` (if not already present).
///
/// Both `Snapshot` and `Asof` modes currently use the same row-matching logic.
/// Time-based asof refinement will be added in L3.
fn execute_joins(joins: &[JoinPlan], ctx: &mut Event, windows: &dyn WindowLookup) {
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

/// Evaluate the score expression and clamp to `[0, 100]`.
fn eval_score(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<f64> {
    let val = eval_expr(expr, ctx);
    let raw = match val {
        Some(Value::Number(n)) => n,
        Some(other) => {
            return StructError::from(CoreReason::RuleExec)
                .with_detail(format!(
                    "score expression evaluated to non-numeric value: {:?}",
                    other
                ))
                .err();
        }
        None => {
            return StructError::from(CoreReason::RuleExec)
                .with_detail("score expression evaluated to None")
                .err();
        }
    };
    Ok(clamp_score(raw))
}

fn clamp_score(v: f64) -> f64 {
    v.clamp(0.0, 100.0)
}

/// Evaluate the entity_id expression.
fn eval_entity_id(expr: &wf_lang::ast::Expr, ctx: &Event) -> CoreResult<String> {
    let val = eval_expr(expr, ctx);
    match val {
        Some(v) => Ok(value_to_string(&v)),
        None => StructError::from(CoreReason::RuleExec)
            .with_detail("entity_id expression evaluated to None")
            .err(),
    }
}

/// Format nanoseconds since epoch as ISO 8601 UTC string.
///
/// Reuses the Hinnant civil-from-days algorithm. For `nanos <= 0`
/// returns the epoch string.
pub(crate) fn format_nanos_utc(nanos: i64) -> String {
    if nanos <= 0 {
        return "1970-01-01T00:00:00.000Z".to_string();
    }
    let total_secs = (nanos / 1_000_000_000) as u64;
    let millis = ((nanos % 1_000_000_000) / 1_000_000) as u32;

    let secs_of_day = total_secs % 86400;
    let days_since_epoch = (total_secs / 86400) as i64;

    let (year, month, day) = civil_from_days(days_since_epoch);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hour, minute, second, millis
    )
}

/// Hinnant civil_from_days: convert days since 1970-01-01 to (y, m, d).
/// Reference: <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Process-wide monotonic counter for alert_id uniqueness.
static ALERT_SEQ: AtomicU64 = AtomicU64::new(0);

/// Percent-encode characters that would break alert_id structure.
///
/// Encodes `%`, `|`, `#`, and `\x1f` so the three-segment `|` split and
/// the `#seq` suffix can always be parsed unambiguously.
fn encode_alert_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '|' => out.push_str("%7C"),
            '#' => out.push_str("%23"),
            '\x1f' => out.push_str("%1F"),
            _ => out.push(ch),
        }
    }
    out
}

/// Build a composite alert id: `"rule|key1\x1fkey2|fired_at#seq"`.
///
/// - Each key value is percent-encoded (`|` → `%7C`, `#` → `%23`, `%` → `%25`)
///   so that the outer `|` split is always unambiguous.
/// - Keys joined with `\x1f` (unit separator) to avoid multi-key ambiguity.
/// - `seq` is a process-wide monotonic counter for same-millisecond uniqueness.
fn build_alert_id(rule_name: &str, scope_key: &[Value], fired_at: &str) -> String {
    let rule_enc = encode_alert_segment(rule_name);
    let keys_part = if scope_key.is_empty() {
        "global".to_string()
    } else {
        scope_key
            .iter()
            .map(|v| encode_alert_segment(&value_to_string(v)))
            .collect::<Vec<_>>()
            .join("\x1f")
    };
    let seq = ALERT_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{}|{}|{}#{}", rule_enc, keys_part, fired_at, seq)
}

/// Build a human-readable summary.
fn build_summary(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    close_reason: Option<&str>,
) -> String {
    let mut parts = Vec::new();

    parts.push(format!("rule={}", rule_name));

    if scope_key.is_empty() {
        parts.push("scope=global".to_string());
    } else {
        let key_strs: Vec<String> = keys
            .iter()
            .zip(scope_key.iter())
            .map(|(fr, val)| format!("{}={}", field_ref_name(fr), value_to_string(val)))
            .collect();
        parts.push(format!("scope=[{}]", key_strs.join(", ")));
    }

    for (i, sd) in step_data.iter().enumerate() {
        let label_part = match &sd.label {
            Some(l) => format!("{}={:.1}", l, sd.measure_value),
            None => format!("step{}={:.1}", i, sd.measure_value),
        };
        parts.push(label_part);
    }

    if let Some(reason) = close_reason {
        parts.push(format!("close_reason={}", reason));
    }

    parts.join("; ")
}
