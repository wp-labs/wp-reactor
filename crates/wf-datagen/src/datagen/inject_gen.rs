use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use rand::rngs::StdRng;
use wf_lang::ast::{Expr, FieldRef, Measure};
use wf_lang::plan::{RulePlan, WindowSpec};
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::datagen::field_gen::generate_field_value;
use crate::datagen::stream_gen::GenEvent;
use crate::wfg_ast::{InjectLine, InjectMode, ParamValue, StreamBlock, WfgFile};

/// Result of inject event generation.
pub struct InjectGenResult {
    pub events: Vec<GenEvent>,
    /// Number of inject events per scenario stream alias.
    pub inject_counts: HashMap<String, u64>,
}

/// Extracted rule structure for inject generation.
#[allow(dead_code)]
struct RuleStructure {
    keys: Vec<String>,
    window_dur: Duration,
    steps: Vec<StepInfo>,
    entity_id_field: Option<String>,
}

#[derive(Clone)]
#[allow(dead_code)]
struct StepInfo {
    bind_alias: String,
    scenario_alias: String,
    window_name: String,
    #[allow(dead_code)]
    measure: Measure,
    threshold: u64,
}

/// Alias mapping between scenario streams and rule binds.
struct AliasMap {
    /// bind_alias → (scenario_alias, window_name)
    bind_to_scenario: HashMap<String, (String, String)>,
}

/// Generate inject events driven by rule plans.
///
/// For each inject block in the scenario, generates hit / near-miss / non-hit
/// event clusters according to the rule's structure and thresholds.
pub fn generate_inject_events(
    wfg: &WfgFile,
    rule_plans: &[RulePlan],
    schemas: &[WindowSchema],
    start: &DateTime<Utc>,
    duration: &Duration,
    rng: &mut StdRng,
) -> anyhow::Result<InjectGenResult> {
    let scenario = &wfg.scenario;
    let stream_totals = compute_stream_totals(scenario);

    let mut all_events = Vec::new();
    let mut inject_counts: HashMap<String, u64> = HashMap::new();

    for inject_block in &scenario.injects {
        let rule_plan = rule_plans
            .iter()
            .find(|p| p.name == inject_block.rule)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "inject references rule '{}' not found in compiled plans",
                    inject_block.rule
                )
            })?;

        let alias_map = build_alias_map(&inject_block.streams, &scenario.streams, rule_plan)?;
        let rule_struct = extract_rule_structure(rule_plan, &alias_map)?;

        for inject_line in &inject_block.lines {
            let events = generate_for_line(
                inject_line,
                &rule_struct,
                &stream_totals,
                schemas,
                &scenario.streams,
                start,
                duration,
                rng,
                &mut inject_counts,
            )?;
            all_events.extend(events);
        }
    }

    Ok(InjectGenResult {
        events: all_events,
        inject_counts,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn compute_stream_totals(scenario: &crate::wfg_ast::ScenarioDecl) -> HashMap<String, u64> {
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

fn build_alias_map(
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

fn extract_rule_structure(
    rule_plan: &RulePlan,
    alias_map: &AliasMap,
) -> anyhow::Result<RuleStructure> {
    let WindowSpec::Sliding(window_dur) = rule_plan.match_plan.window_spec;

    let keys: Vec<String> = rule_plan
        .match_plan
        .keys
        .iter()
        .map(|fr| field_ref_field_name(fr).to_string())
        .collect();

    let mut steps = Vec::new();
    for step_plan in &rule_plan.match_plan.event_steps {
        // P1: take first branch
        let branch = step_plan
            .branches
            .first()
            .ok_or_else(|| anyhow::anyhow!("step has no branches"))?;

        let bind_alias = &branch.source;

        // SC6: inject streams are a *subset* of rule aliases.
        // Skip steps whose bind alias is not covered by inject.
        let (scenario_alias, window_name) = match alias_map.bind_to_scenario.get(bind_alias) {
            Some(pair) => pair,
            None => continue,
        };

        let threshold = eval_const_threshold(&branch.agg.threshold)
            .ok_or_else(|| anyhow::anyhow!("cannot evaluate threshold as constant"))?
            as u64;

        steps.push(StepInfo {
            bind_alias: bind_alias.clone(),
            scenario_alias: scenario_alias.clone(),
            window_name: window_name.clone(),
            measure: branch.agg.measure,
            threshold,
        });
    }

    if steps.is_empty() {
        anyhow::bail!(
            "no inject streams map to any step in rule '{}'; \
             at least one inject alias must match a rule bind alias",
            rule_plan.name
        );
    }

    let entity_id_field = extract_entity_id_field(&rule_plan.entity_plan.entity_id_expr);

    Ok(RuleStructure {
        keys,
        window_dur,
        steps,
        entity_id_field,
    })
}

/// Extract a constant numeric value from an expression (L1 thresholds).
pub(crate) fn eval_const_threshold(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Number(n) => Some(*n),
        Expr::Neg(inner) => eval_const_threshold(inner).map(|v| -v),
        _ => None,
    }
}

/// Override parameters extracted from inject line params.
struct InjectOverrides {
    /// Override the threshold (events per entity) for hit/near_miss clusters.
    count_per_entity: Option<u64>,
    /// For near_miss multi-step: how many steps to complete (0-indexed last step).
    steps_completed: Option<usize>,
    /// Override the window duration for cluster time distribution.
    within: Option<Duration>,
}

fn extract_inject_overrides(inject_line: &InjectLine) -> InjectOverrides {
    let mut overrides = InjectOverrides {
        count_per_entity: None,
        steps_completed: None,
        within: None,
    };

    for param in &inject_line.params {
        match param.name.as_str() {
            "count_per_entity" => {
                if let ParamValue::Number(n) = &param.value {
                    overrides.count_per_entity = Some(*n as u64);
                }
            }
            "steps_completed" => {
                if let ParamValue::Number(n) = &param.value {
                    overrides.steps_completed = Some(*n as usize);
                }
            }
            "within" => {
                if let ParamValue::Duration(d) = &param.value {
                    overrides.within = Some(*d);
                }
            }
            _ => {}
        }
    }

    overrides
}

pub(crate) fn field_ref_field_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        _ => "",
    }
}

fn extract_entity_id_field(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Field(fr) => Some(field_ref_field_name(fr).to_string()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Per-line dispatch
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn generate_for_line(
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

// ---------------------------------------------------------------------------
// Hit cluster generation
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn generate_hit_clusters(
    percent: f64,
    rule_struct: &RuleStructure,
    stream_totals: &HashMap<String, u64>,
    schemas: &[WindowSchema],
    scenario_streams: &[StreamBlock],
    start: &DateTime<Utc>,
    duration: &Duration,
    rng: &mut StdRng,
    inject_counts: &mut HashMap<String, u64>,
    overrides: &InjectOverrides,
) -> anyhow::Result<Vec<GenEvent>> {
    // Apply count_per_entity override: use overridden threshold for cluster sizing
    let effective_steps: Vec<StepInfo> = if let Some(cpe) = overrides.count_per_entity {
        rule_struct
            .steps
            .iter()
            .map(|s| StepInfo {
                bind_alias: s.bind_alias.clone(),
                scenario_alias: s.scenario_alias.clone(),
                window_name: s.window_name.clone(),
                measure: s.measure,
                threshold: cpe, // override
            })
            .collect()
    } else {
        rule_struct.steps.clone()
    };

    let num_clusters = compute_cluster_count(percent, &effective_steps, stream_totals);
    if num_clusters == 0 {
        return Ok(Vec::new());
    }

    // Update inject counts
    for step in &effective_steps {
        *inject_counts
            .entry(step.scenario_alias.clone())
            .or_insert(0) += step.threshold * num_clusters;
    }

    let dur_secs = duration.as_secs_f64();
    let window_dur = overrides.within.unwrap_or(rule_struct.window_dur);
    let window_secs = window_dur.as_secs_f64();
    let max_start_offset = (dur_secs - window_secs).max(0.0);

    let mut events = Vec::new();

    for (entity_counter, _cluster_idx) in (0_u64..).zip(0..num_clusters) {
        let key_overrides = generate_key_values(
            &rule_struct.keys,
            entity_counter,
            "hit",
            schemas,
            &effective_steps,
        );

        let cluster_start_secs = if max_start_offset > 0.0 {
            rng.random_range(0.0..max_start_offset)
        } else {
            0.0
        };

        generate_cluster_events(
            &effective_steps,
            |_idx, step| step.threshold,
            &key_overrides,
            cluster_start_secs,
            window_secs,
            schemas,
            scenario_streams,
            start,
            rng,
            &mut events,
        )?;
    }

    Ok(events)
}

// ---------------------------------------------------------------------------
// Near-miss cluster generation
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn generate_near_miss_clusters(
    percent: f64,
    rule_struct: &RuleStructure,
    stream_totals: &HashMap<String, u64>,
    schemas: &[WindowSchema],
    scenario_streams: &[StreamBlock],
    start: &DateTime<Utc>,
    duration: &Duration,
    rng: &mut StdRng,
    inject_counts: &mut HashMap<String, u64>,
    overrides: &InjectOverrides,
) -> anyhow::Result<Vec<GenEvent>> {
    // For near-miss: N-1 events per cluster for the last step,
    // full threshold for preceding steps.
    // Multi-step near-miss: complete all steps except last.
    let steps = &rule_struct.steps;
    if steps.is_empty() {
        return Ok(Vec::new());
    }

    // Apply count_per_entity to override the near-miss step threshold
    let effective_threshold_nm = overrides
        .count_per_entity
        .unwrap_or(steps[steps.len() - 1].threshold);

    // Apply steps_completed to control which steps get full events.
    // steps_completed = index of the near-miss step (gets threshold-1).
    // Steps before it: full threshold. Steps after it: 0 events.
    let steps_completed = overrides.steps_completed.unwrap_or(steps.len() - 1);
    let nm_step_idx = steps_completed.min(steps.len() - 1);

    // Events per cluster for near-miss
    let near_miss_counts: Vec<u64> = steps
        .iter()
        .enumerate()
        .map(|(i, step)| {
            if i > nm_step_idx {
                // Beyond the near-miss step: no events
                0
            } else if i == nm_step_idx {
                // The near-miss step: threshold - 1
                effective_threshold_nm.saturating_sub(1)
            } else {
                // Fully completed preceding steps
                overrides.count_per_entity.unwrap_or(step.threshold)
            }
        })
        .collect();

    // Total events per cluster
    let events_per_cluster: u64 = near_miss_counts.iter().sum();
    if events_per_cluster == 0 {
        return Ok(Vec::new());
    }

    // Compute number of clusters from the near-miss step's budget
    let primary_step = &steps[nm_step_idx];
    let stream_total = *stream_totals
        .get(&primary_step.scenario_alias)
        .unwrap_or(&0);
    let budget = (stream_total as f64 * percent / 100.0).round() as u64;
    let nm_count = near_miss_counts[nm_step_idx];
    let num_clusters = if nm_count > 0 { budget / nm_count } else { 0 };

    if num_clusters == 0 {
        return Ok(Vec::new());
    }

    // Update inject counts
    for (i, step) in steps.iter().enumerate() {
        *inject_counts
            .entry(step.scenario_alias.clone())
            .or_insert(0) += near_miss_counts[i] * num_clusters;
    }

    let dur_secs = duration.as_secs_f64();
    let window_dur = overrides.within.unwrap_or(rule_struct.window_dur);
    let window_secs = window_dur.as_secs_f64();
    let max_start_offset = (dur_secs - window_secs).max(0.0);

    let mut events = Vec::new();

    for (entity_counter, _cluster_idx) in (0_u64..).zip(0..num_clusters) {
        let key_overrides = generate_key_values(
            &rule_struct.keys,
            entity_counter,
            "nm",
            schemas,
            &rule_struct.steps,
        );

        let cluster_start_secs = if max_start_offset > 0.0 {
            rng.random_range(0.0..max_start_offset)
        } else {
            0.0
        };

        let nm = near_miss_counts.clone();
        generate_cluster_events(
            steps,
            |idx, _step| nm[idx],
            &key_overrides,
            cluster_start_secs,
            window_secs,
            schemas,
            scenario_streams,
            start,
            rng,
            &mut events,
        )?;
    }

    Ok(events)
}

// ---------------------------------------------------------------------------
// Non-hit event generation
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn generate_non_hit_events(
    percent: f64,
    rule_struct: &RuleStructure,
    stream_totals: &HashMap<String, u64>,
    schemas: &[WindowSchema],
    scenario_streams: &[StreamBlock],
    start: &DateTime<Utc>,
    duration: &Duration,
    rng: &mut StdRng,
    inject_counts: &mut HashMap<String, u64>,
) -> anyhow::Result<Vec<GenEvent>> {
    let mut events = Vec::new();
    let dur_nanos = duration.as_nanos() as i64;

    // For non-hit, each event has a unique key → no clustering → no rule trigger.
    // Generate events on each participating stream.
    let mut entity_counter: u64 = 1_000_000; // offset to avoid collision with hit/nm

    for step in &rule_struct.steps {
        let stream_total = *stream_totals.get(&step.scenario_alias).unwrap_or(&0);
        let event_count = (stream_total as f64 * percent / 100.0).round() as u64;

        if event_count == 0 {
            continue;
        }

        *inject_counts
            .entry(step.scenario_alias.clone())
            .or_insert(0) += event_count;

        let schema = schemas
            .iter()
            .find(|s| s.name == step.window_name)
            .ok_or_else(|| anyhow::anyhow!("schema not found for '{}'", step.window_name))?;

        let stream_block = scenario_streams
            .iter()
            .find(|s| s.alias == step.scenario_alias)
            .unwrap();

        let overrides_map: HashMap<&str, &crate::wfg_ast::GenExpr> = stream_block
            .overrides
            .iter()
            .map(|o| (o.field_name.as_str(), &o.gen_expr))
            .collect();

        for i in 0..event_count {
            let key_overrides = generate_key_values(
                &rule_struct.keys,
                entity_counter,
                "bg",
                schemas,
                &rule_struct.steps,
            );
            entity_counter += 1;

            // Uniform timestamp distribution
            let offset_nanos = if event_count > 1 {
                dur_nanos * i as i64 / event_count as i64
            } else {
                dur_nanos / 2
            };
            let ts = *start + ChronoDuration::nanoseconds(offset_nanos);

            let fields = build_event_fields(schema, &overrides_map, &key_overrides, &ts, rng);

            events.push(GenEvent {
                stream_alias: step.scenario_alias.clone(),
                window_name: step.window_name.clone(),
                timestamp: ts,
                fields,
            });
        }
    }

    Ok(events)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Compute the number of clusters based on per-stream event budgets.
fn compute_cluster_count(
    percent: f64,
    steps: &[StepInfo],
    stream_totals: &HashMap<String, u64>,
) -> u64 {
    let mut min_clusters = u64::MAX;

    for step in steps {
        let stream_total = *stream_totals.get(&step.scenario_alias).unwrap_or(&0);
        let budget = (stream_total as f64 * percent / 100.0).round() as u64;
        if step.threshold > 0 {
            let clusters = budget / step.threshold;
            min_clusters = min_clusters.min(clusters);
        }
    }

    if min_clusters == u64::MAX {
        0
    } else {
        min_clusters
    }
}

/// Generate cluster events across all steps.
#[allow(clippy::too_many_arguments)]
fn generate_cluster_events(
    steps: &[StepInfo],
    count_fn: impl Fn(usize, &StepInfo) -> u64,
    key_overrides: &HashMap<String, serde_json::Value>,
    cluster_start_secs: f64,
    window_secs: f64,
    schemas: &[WindowSchema],
    scenario_streams: &[StreamBlock],
    start: &DateTime<Utc>,
    rng: &mut StdRng,
    out: &mut Vec<GenEvent>,
) -> anyhow::Result<()> {
    // Track cumulative time offset across steps for multi-step ordering
    let mut cumulative_offset = 0.0;
    let per_step_window = if steps.len() > 1 {
        window_secs / steps.len() as f64
    } else {
        window_secs
    };

    for (step_idx, step) in steps.iter().enumerate() {
        let event_count = count_fn(step_idx, step);
        if event_count == 0 {
            continue;
        }

        let schema = schemas
            .iter()
            .find(|s| s.name == step.window_name)
            .ok_or_else(|| anyhow::anyhow!("schema not found for '{}'", step.window_name))?;

        let stream_block = scenario_streams
            .iter()
            .find(|s| s.alias == step.scenario_alias)
            .unwrap();

        let overrides_map: HashMap<&str, &crate::wfg_ast::GenExpr> = stream_block
            .overrides
            .iter()
            .map(|o| (o.field_name.as_str(), &o.gen_expr))
            .collect();

        for i in 0..event_count {
            let event_offset_secs = cluster_start_secs
                + cumulative_offset
                + (per_step_window * i as f64 / event_count.max(1) as f64);
            let ts = *start + ChronoDuration::nanoseconds((event_offset_secs * 1e9) as i64);

            let fields = build_event_fields(schema, &overrides_map, key_overrides, &ts, rng);

            out.push(GenEvent {
                stream_alias: step.scenario_alias.clone(),
                window_name: step.window_name.clone(),
                timestamp: ts,
                fields,
            });
        }

        cumulative_offset += per_step_window;
    }

    Ok(())
}

/// Build event fields with key overrides applied.
fn build_event_fields(
    schema: &WindowSchema,
    overrides_map: &HashMap<&str, &crate::wfg_ast::GenExpr>,
    key_overrides: &HashMap<String, serde_json::Value>,
    ts: &DateTime<Utc>,
    rng: &mut StdRng,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();

    for field_def in &schema.fields {
        // Key field override
        if let Some(value) = key_overrides.get(&field_def.name) {
            fields.insert(field_def.name.clone(), value.clone());
            continue;
        }

        // Time field
        if matches!(&field_def.field_type, FieldType::Base(BaseType::Time)) {
            let override_expr = overrides_map.get(field_def.name.as_str()).copied();
            if override_expr.is_none()
                || matches!(override_expr, Some(crate::wfg_ast::GenExpr::GenFunc { name, .. }) if name == "timestamp")
            {
                fields.insert(
                    field_def.name.clone(),
                    serde_json::Value::String(ts.to_rfc3339()),
                );
                continue;
            }
        }

        // Normal field with possible stream override
        let override_expr = overrides_map.get(field_def.name.as_str()).copied();
        let value = generate_field_value(&field_def.field_type, override_expr, rng);
        fields.insert(field_def.name.clone(), value);
    }

    fields
}

/// Generate unique key values for a cluster entity.
///
/// Uses the entity counter and a prefix to produce deterministic unique values
/// based on the field type from the schema.
fn generate_key_values(
    key_names: &[String],
    entity_counter: u64,
    prefix: &str,
    schemas: &[WindowSchema],
    steps: &[StepInfo],
) -> HashMap<String, serde_json::Value> {
    let mut overrides = HashMap::new();

    // Find field types from the first step's schema
    let first_schema = steps
        .first()
        .and_then(|s| schemas.iter().find(|sch| sch.name == s.window_name));

    for (i, key_name) in key_names.iter().enumerate() {
        let field_type = first_schema.and_then(|sch| {
            sch.fields
                .iter()
                .find(|f| &f.name == key_name)
                .map(|f| &f.field_type)
        });

        let value = match field_type {
            Some(FieldType::Base(BaseType::Ip)) => {
                let id = entity_counter + i as u64;
                let a = ((id >> 16) & 0xFF) as u8;
                let b = ((id >> 8) & 0xFF) as u8;
                let c = (id & 0xFF) as u8;
                serde_json::Value::String(format!("10.{a}.{b}.{c}"))
            }
            Some(FieldType::Base(BaseType::Digit)) => {
                serde_json::json!(entity_counter as i64 + i as i64)
            }
            Some(FieldType::Base(BaseType::Float)) => {
                serde_json::json!(entity_counter as f64 + i as f64)
            }
            _ => {
                // Default: string
                serde_json::Value::String(format!(
                    "{prefix}_{key}_{id:06}",
                    key = key_name,
                    id = entity_counter
                ))
            }
        };

        overrides.insert(key_name.clone(), value);
    }

    overrides
}
