pub mod fault_gen;
pub mod field_gen;
pub mod inject_gen;
pub mod stream_gen;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rand::SeedableRng;
use rand::rngs::StdRng;
use wf_lang::WindowSchema;
use wf_lang::plan::RulePlan;

use crate::wfg_ast::WfgFile;
use inject_gen::generate_inject_events;
use stream_gen::{GenEvent, generate_stream_events};

/// Result of data generation.
pub struct GenResult {
    pub events: Vec<GenEvent>,
}

/// Generate events from a parsed and validated `.wfg` scenario.
///
/// When `rule_plans` is non-empty and the scenario contains inject blocks,
/// rule-aware inject events are generated (hit / near-miss / non-hit clusters)
/// and merged with background events. When `rule_plans` is empty or no inject
/// blocks exist, the behaviour is identical to the M31 baseline.
pub fn generate(
    wfg: &WfgFile,
    schemas: &[WindowSchema],
    rule_plans: &[RulePlan],
) -> anyhow::Result<GenResult> {
    let scenario = &wfg.scenario;

    // Parse start time
    let start: DateTime<Utc> = scenario.time_clause.start.parse().map_err(|e| {
        anyhow::anyhow!("invalid start time '{}': {}", scenario.time_clause.start, e)
    })?;

    let duration = scenario.time_clause.duration;
    let total = scenario.total;

    // Create deterministic RNG
    let mut rng = StdRng::seed_from_u64(scenario.seed);

    // --- Inject generation (if applicable) ---
    let mut inject_counts: HashMap<String, u64> = HashMap::new();
    let mut all_events = Vec::new();

    let has_inject = !scenario.injects.is_empty() && !rule_plans.is_empty();
    if has_inject {
        let inject_result =
            generate_inject_events(wfg, rule_plans, schemas, &start, &duration, &mut rng)?;
        inject_counts = inject_result.inject_counts;
        all_events.extend(inject_result.events);
    }

    // --- Background event generation ---
    let total_rate: f64 = scenario
        .streams
        .iter()
        .map(|s| s.rate.events_per_second())
        .sum();

    if total_rate == 0.0 {
        return Err(anyhow::anyhow!("total rate across all streams is 0"));
    }

    let mut remaining = total;

    for (i, stream) in scenario.streams.iter().enumerate() {
        let proportion = stream.rate.events_per_second() / total_rate;
        let stream_total = if i == scenario.streams.len() - 1 {
            remaining
        } else {
            let count = (total as f64 * proportion).round() as u64;
            let count = count.min(remaining);
            remaining -= count;
            count
        };

        // Subtract inject events from this stream's budget
        let inject_used = inject_counts.get(&stream.alias).copied().unwrap_or(0);
        let bg_count = stream_total.saturating_sub(inject_used);

        if bg_count == 0 {
            continue;
        }

        let schema = schemas
            .iter()
            .find(|s| s.name == stream.window)
            .ok_or_else(|| anyhow::anyhow!("schema not found for window '{}'", stream.window))?;

        let events = generate_stream_events(stream, schema, bg_count, &start, &duration, &mut rng);
        all_events.extend(events);
    }

    // Sort all events by timestamp
    all_events.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    Ok(GenResult { events: all_events })
}
