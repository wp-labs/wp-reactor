use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};

use wf_lang::ast::FieldRef;
use wf_lang::plan::RulePlan;

use crate::alert::AlertRecord;
use crate::rule::match_engine::{
    CloseOutput, Event, MatchedContext, StepData, Value, eval_expr, field_ref_name, value_to_string,
};

// ---------------------------------------------------------------------------
// RuleExecutor
// ---------------------------------------------------------------------------

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`AlertRecord`]s from CEP match/close outputs.
///
/// No DataFusion — `JoinPlan` is empty for L1. DataFusion SQL execution
/// will be added in M25 (L2 join).
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

    /// Produce an [`AlertRecord`] from an on-event match.
    pub fn execute_match(&self, matched: &MatchedContext) -> Result<AlertRecord> {
        let ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
        );

        let score = eval_score(&self.plan.score_plan.expr, &ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, &ctx)?;
        let fired_at = format_fired_at(SystemTime::now());
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
            yield_target: self.plan.yield_plan.target.clone(),
        })
    }

    /// Produce an [`AlertRecord`] from a close output.
    ///
    /// Returns `Ok(None)` when `!event_ok || !close_ok` — the instance
    /// did not fully satisfy the rule.
    pub fn execute_close(&self, close: &CloseOutput) -> Result<Option<AlertRecord>> {
        if !close.event_ok || !close.close_ok {
            return Ok(None);
        }

        // Combine event + close step data for expression context
        let all_step_data: Vec<StepData> = close
            .event_step_data
            .iter()
            .chain(close.close_step_data.iter())
            .cloned()
            .collect();

        let ctx = build_eval_context(&self.plan.match_plan.keys, &close.scope_key, &all_step_data);

        let score = eval_score(&self.plan.score_plan.expr, &ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, &ctx)?;
        let close_reason_str = close.close_reason.as_str().to_string();
        let fired_at = format_fired_at(SystemTime::now());
        let alert_id = build_alert_id(&self.plan.name, &close.scope_key, &fired_at);
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &close.scope_key,
            &all_step_data,
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
            yield_target: self.plan.yield_plan.target.clone(),
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

/// Evaluate the score expression and clamp to `[0, 100]`.
fn eval_score(expr: &wf_lang::ast::Expr, ctx: &Event) -> Result<f64> {
    let val = eval_expr(expr, ctx);
    let raw = match val {
        Some(Value::Number(n)) => n,
        Some(other) => bail!(
            "score expression evaluated to non-numeric value: {:?}",
            other
        ),
        None => bail!("score expression evaluated to None"),
    };
    Ok(clamp_score(raw))
}

fn clamp_score(v: f64) -> f64 {
    v.clamp(0.0, 100.0)
}

/// Evaluate the entity_id expression.
fn eval_entity_id(expr: &wf_lang::ast::Expr, ctx: &Event) -> Result<String> {
    let val = eval_expr(expr, ctx);
    match val {
        Some(v) => Ok(value_to_string(&v)),
        None => bail!("entity_id expression evaluated to None"),
    }
}

/// Format `SystemTime` as ISO 8601 UTC string without `chrono`.
///
/// Uses the Hinnant civil-from-days algorithm to convert days since
/// UNIX epoch to (year, month, day).
pub(crate) fn format_fired_at(t: SystemTime) -> String {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = dur.as_secs();
    let millis = dur.subsec_millis();

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
