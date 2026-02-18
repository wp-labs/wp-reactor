pub mod field_gen;
pub mod stream_gen;
#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};
use rand::SeedableRng;
use rand::rngs::StdRng;
use wf_lang::WindowSchema;

use crate::wsc_ast::WscFile;
use stream_gen::{GenEvent, generate_stream_events};

/// Result of data generation.
pub struct GenResult {
    pub events: Vec<GenEvent>,
}

/// Generate events from a parsed and validated `.wsc` scenario.
///
/// The `schemas` parameter must contain all window schemas referenced by the scenario.
pub fn generate(wsc: &WscFile, schemas: &[WindowSchema]) -> anyhow::Result<GenResult> {
    let scenario = &wsc.scenario;

    // Parse start time
    let start: DateTime<Utc> = scenario.time_clause.start.parse().map_err(|e| {
        anyhow::anyhow!("invalid start time '{}': {}", scenario.time_clause.start, e)
    })?;

    let duration = scenario.time_clause.duration;
    let total = scenario.total;

    // Create deterministic RNG
    let mut rng = StdRng::seed_from_u64(scenario.seed);

    // Calculate rate proportions for event distribution
    let total_rate: f64 = scenario
        .streams
        .iter()
        .map(|s| s.rate.events_per_second())
        .sum();

    if total_rate == 0.0 {
        return Err(anyhow::anyhow!("total rate across all streams is 0"));
    }

    // Distribute events by rate proportion
    let mut all_events = Vec::new();
    let mut remaining = total;

    for (i, stream) in scenario.streams.iter().enumerate() {
        let proportion = stream.rate.events_per_second() / total_rate;
        let event_count = if i == scenario.streams.len() - 1 {
            // Last stream gets the remaining events to avoid rounding issues
            remaining
        } else {
            let count = (total as f64 * proportion).round() as u64;
            let count = count.min(remaining);
            remaining -= count;
            count
        };

        // Find the schema for this stream
        let schema = schemas
            .iter()
            .find(|s| s.name == stream.window)
            .ok_or_else(|| anyhow::anyhow!("schema not found for window '{}'", stream.window))?;

        let events =
            generate_stream_events(stream, schema, event_count, &start, &duration, &mut rng);
        all_events.extend(events);
    }

    // Sort all events by timestamp
    all_events.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    Ok(GenResult { events: all_events })
}
