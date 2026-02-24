use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::rngs::StdRng;
use wf_lang::WindowSchema;
use wf_lang::plan::RulePlan;

use super::extract::extract_inject_overrides;
use super::hit::generate_hit_clusters;
use super::near_miss::generate_near_miss_clusters;
use super::non_hit::generate_non_hit_events;
use super::structures::{AliasMap, RuleStructure};
use crate::datagen::stream_gen::GenEvent;
use crate::wfg_ast::{InjectLine, InjectMode, StreamBlock};

pub(super) fn compute_stream_totals(
    scenario: &crate::wfg_ast::ScenarioDecl,
) -> HashMap<String, u64> {
    let total = scenario.total;
    let total_rate: f64 = scenario
        .streams
        .iter()
        .map(|s| s.rate.events_per_second())
        .sum();

    if total_rate == 0.0 {
        return HashMap::new();
    }

    let mut result = HashMap::new();
    let mut remaining = total;

    for (i, stream) in scenario.streams.iter().enumerate() {
        let proportion = stream.rate.events_per_second() / total_rate;
        let count = if i == scenario.streams.len() - 1 {
            remaining
        } else {
            let c = (total as f64 * proportion).round() as u64;
            let c = c.min(remaining);
            remaining -= c;
            c
        };
        result.insert(stream.alias.clone(), count);
    }

    result
}

pub(super) fn build_alias_map(
    inject_streams: &[String],
    scenario_streams: &[StreamBlock],
    rule_plan: &RulePlan,
) -> anyhow::Result<AliasMap> {
    let mut bind_to_scenario = HashMap::new();

    // Per SC6/SC2a, each inject stream alias IS the rule bind alias.
    // The scenario stream declaration `stream fail: LoginWindow 100/s`
    // declares alias "fail" which matches `events fail : LoginWindow`
    // in the .wfl rule. Match directly by alias name.
    for scenario_alias in inject_streams {
        let stream_block = scenario_streams
            .iter()
            .find(|s| &s.alias == scenario_alias)
            .ok_or_else(|| {
                anyhow::anyhow!("inject stream '{}' not found in scenario", scenario_alias)
            })?;

        // Verify the alias exists in the rule's binds (SC6 should have
        // caught this at validation time, but belt-and-suspenders).
        if !rule_plan.binds.iter().any(|b| b.alias == *scenario_alias) {
            anyhow::bail!(
                "inject stream '{}' is not a bind alias in rule '{}'",
                scenario_alias,
                rule_plan.name
            );
        }

        bind_to_scenario.insert(
            scenario_alias.clone(),
            (scenario_alias.clone(), stream_block.window.clone()),
        );
    }

    Ok(AliasMap { bind_to_scenario })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn generate_for_line(
    inject_line: &InjectLine,
    rule_struct: &RuleStructure,
    stream_totals: &HashMap<String, u64>,
    schemas: &[WindowSchema],
    scenario_streams: &[StreamBlock],
    start: &DateTime<Utc>,
    duration: &Duration,
    rng: &mut StdRng,
    inject_counts: &mut HashMap<String, u64>,
) -> anyhow::Result<Vec<GenEvent>> {
    let overrides = extract_inject_overrides(inject_line);

    match inject_line.mode {
        InjectMode::Hit => generate_hit_clusters(
            inject_line.percent,
            rule_struct,
            stream_totals,
            schemas,
            scenario_streams,
            start,
            duration,
            rng,
            inject_counts,
            &overrides,
        ),
        InjectMode::NearMiss => generate_near_miss_clusters(
            inject_line.percent,
            rule_struct,
            stream_totals,
            schemas,
            scenario_streams,
            start,
            duration,
            rng,
            inject_counts,
            &overrides,
        ),
        InjectMode::NonHit => generate_non_hit_events(
            inject_line.percent,
            rule_struct,
            stream_totals,
            schemas,
            scenario_streams,
            start,
            duration,
            rng,
            inject_counts,
        ),
    }
}
