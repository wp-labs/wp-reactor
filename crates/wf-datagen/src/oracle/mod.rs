#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use wf_core::rule::{
    CepStateMachine, Event, RuleExecutor, StepResult, Value,
};
use wf_lang::plan::RulePlan;

use crate::datagen::stream_gen::GenEvent;

/// An oracle alert produced by the reference evaluator.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OracleAlert {
    pub rule_name: String,
    pub score: f64,
    pub entity_type: String,
    pub entity_id: String,
    pub close_reason: Option<String>,
    /// ISO 8601 — logical time (triggering event's timestamp).
    pub emit_time: String,
}

/// Result of oracle evaluation.
pub struct OracleResult {
    pub alerts: Vec<OracleAlert>,
}

/// Run the reference evaluator on generated events.
///
/// Creates a `CepStateMachine` + `RuleExecutor` per rule, feeds events in
/// timestamp order, and collects oracle alerts. Uses synthetic `Instant`s
/// derived from event timestamps so that window expiry works correctly in
/// single-threaded deterministic mode.
pub fn run_oracle(
    events: &[GenEvent],
    rule_plans: &[RulePlan],
    scenario_start: &DateTime<Utc>,
    scenario_duration: &Duration,
) -> anyhow::Result<OracleResult> {
    if rule_plans.is_empty() {
        return Ok(OracleResult { alerts: vec![] });
    }

    // Build per-rule engines
    let mut engines: Vec<RuleEngine> = rule_plans
        .iter()
        .map(|plan| {
            let alias_map = build_window_alias_map(plan);
            RuleEngine {
                sm: CepStateMachine::new(plan.name.clone(), plan.match_plan.clone()),
                executor: RuleExecutor::new(plan.clone()),
                alias_map,
            }
        })
        .collect();

    let base_instant = Instant::now();
    let mut alerts = Vec::new();

    // Process events in order (caller should have sorted by timestamp)
    for event in events {
        let offset = event
            .timestamp
            .signed_duration_since(*scenario_start)
            .to_std()
            .unwrap_or_default();
        let synthetic_now = base_instant + offset;

        let core_event = gen_event_to_core(event);

        for engine in &mut engines {
            // Scan for expired instances first
            let expired = engine.sm.scan_expired(synthetic_now);
            for close_out in expired {
                if let Ok(Some(alert_record)) = engine.executor.execute_close(&close_out) {
                    alerts.push(OracleAlert {
                        rule_name: alert_record.rule_name,
                        score: alert_record.score,
                        entity_type: alert_record.entity_type,
                        entity_id: alert_record.entity_id,
                        close_reason: alert_record.close_reason,
                        emit_time: event.timestamp.to_rfc3339(),
                    });
                }
            }

            // Find bind alias for this event's window
            let bind_alias = match engine.alias_map.get(&event.window_name) {
                Some(alias) => alias.clone(),
                None => continue, // this rule doesn't use this window
            };

            // Advance the state machine
            let result =
                engine
                    .sm
                    .advance_with_instant(&bind_alias, &core_event, synthetic_now);

            if let StepResult::Matched(ctx) = result {
                if let Ok(alert_record) = engine.executor.execute_match(&ctx) {
                    alerts.push(OracleAlert {
                        rule_name: alert_record.rule_name,
                        score: alert_record.score,
                        entity_type: alert_record.entity_type,
                        entity_id: alert_record.entity_id,
                        close_reason: None,
                        emit_time: event.timestamp.to_rfc3339(),
                    });
                }
            }
        }
    }

    // End-of-scenario sweep: flush remaining instances with 1h buffer
    let eos_offset = *scenario_duration + Duration::from_secs(3600);
    let eos_instant = base_instant + eos_offset;

    for engine in &mut engines {
        let expired = engine.sm.scan_expired(eos_instant);
        let eos_time = *scenario_start
            + chrono::Duration::from_std(*scenario_duration).unwrap_or_default();

        for close_out in expired {
            if let Ok(Some(alert_record)) = engine.executor.execute_close(&close_out) {
                alerts.push(OracleAlert {
                    rule_name: alert_record.rule_name,
                    score: alert_record.score,
                    entity_type: alert_record.entity_type,
                    entity_id: alert_record.entity_id,
                    close_reason: alert_record.close_reason,
                    emit_time: eos_time.to_rfc3339(),
                });
            }
        }
    }

    Ok(OracleResult { alerts })
}

// ---------------------------------------------------------------------------
// Internal types and helpers
// ---------------------------------------------------------------------------

struct RuleEngine {
    sm: CepStateMachine,
    executor: RuleExecutor,
    /// window_name → bind_alias
    alias_map: HashMap<String, String>,
}

/// Build a mapping from window name to bind alias for a rule.
fn build_window_alias_map(plan: &RulePlan) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for bind in &plan.binds {
        map.entry(bind.window.clone())
            .or_insert_with(|| bind.alias.clone());
    }
    map
}

/// Convert a GenEvent to a wf_core Event.
fn gen_event_to_core(event: &GenEvent) -> Event {
    let mut fields = HashMap::new();
    for (k, v) in &event.fields {
        if let Some(core_v) = json_to_core_value(v) {
            fields.insert(k.clone(), core_v);
        }
    }
    Event { fields }
}

fn json_to_core_value(v: &serde_json::Value) -> Option<Value> {
    match v {
        serde_json::Value::String(s) => Some(Value::Str(s.clone())),
        serde_json::Value::Number(n) => n.as_f64().map(Value::Number),
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        _ => None,
    }
}
