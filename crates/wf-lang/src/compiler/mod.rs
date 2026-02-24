use std::time::Duration;

use crate::ast::{EntityTypeVal, FieldRef, RuleDecl, WflFile};
use crate::checker::check_wfl;
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, ExceedAction, JoinCondPlan, JoinPlan, KeyMapPlan,
    LimitsPlan, MatchPlan, RateSpec, RulePlan, ScorePlan, StepPlan, WindowSpec, YieldField,
    YieldPlan,
};
use crate::schema::WindowSchema;

#[cfg(test)]
mod tests;

/// Compile a parsed WFL file into executable `RulePlan`s.
///
/// Runs semantic checks (`check_wfl`) first; returns an error if any check
/// fails.  This guarantees that a successful return implies the AST was both
/// syntactically and semantically valid — callers never need to remember to
/// call `check_wfl` separately.
///
/// Contracts, use declarations, and meta blocks are stripped — only rule
/// logic is compiled.
pub fn compile_wfl(file: &WflFile, schemas: &[WindowSchema]) -> anyhow::Result<Vec<RulePlan>> {
    let errors = check_wfl(file, schemas);
    if !errors.is_empty() {
        let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        anyhow::bail!("semantic errors:\n{}", msgs.join("\n"));
    }
    file.rules.iter().map(compile_rule).collect()
}

fn compile_rule(rule: &RuleDecl) -> anyhow::Result<RulePlan> {
    Ok(RulePlan {
        name: rule.name.clone(),
        binds: compile_binds(rule),
        match_plan: compile_match(rule),
        joins: compile_joins(&rule.joins),
        entity_plan: compile_entity(rule),
        yield_plan: compile_yield(rule),
        score_plan: compile_score(rule),
        conv_plan: None,
        limits_plan: compile_limits(&rule.limits),
    })
}

// ---------------------------------------------------------------------------
// Binds
// ---------------------------------------------------------------------------

fn compile_binds(rule: &RuleDecl) -> Vec<BindPlan> {
    rule.events
        .decls
        .iter()
        .map(|decl| BindPlan {
            alias: decl.alias.clone(),
            window: decl.window.clone(),
            filter: decl.filter.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Match
// ---------------------------------------------------------------------------

fn compile_match(rule: &RuleDecl) -> MatchPlan {
    let mc = &rule.match_clause;

    let (keys, key_map) = if let Some(ref km) = mc.key_mapping {
        // When key mapping is present, use logical key names as keys
        let logical_names: Vec<FieldRef> = km
            .iter()
            .map(|item| FieldRef::Simple(item.logical_name.clone()))
            .collect();
        // Deduplicate logical names (same logical name maps from multiple sources)
        let mut seen = std::collections::HashSet::new();
        let deduped: Vec<FieldRef> = logical_names
            .into_iter()
            .filter(|f| {
                if let FieldRef::Simple(name) = f {
                    seen.insert(name.clone())
                } else {
                    true
                }
            })
            .collect();
        let key_map_plans: Vec<KeyMapPlan> = km
            .iter()
            .filter_map(|item| {
                if let FieldRef::Qualified(alias, field) = &item.source_field {
                    Some(KeyMapPlan {
                        logical_name: item.logical_name.clone(),
                        source_alias: alias.clone(),
                        source_field: field.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        (deduped, Some(key_map_plans))
    } else {
        (mc.keys.clone(), None)
    };

    MatchPlan {
        keys,
        key_map,
        window_spec: WindowSpec::Sliding(mc.duration),
        event_steps: mc.on_event.iter().map(compile_step).collect(),
        close_steps: mc
            .on_close
            .as_ref()
            .map(|steps| steps.iter().map(compile_step).collect())
            .unwrap_or_default(),
    }
}

fn compile_step(step: &crate::ast::MatchStep) -> StepPlan {
    StepPlan {
        branches: step.branches.iter().map(compile_branch).collect(),
    }
}

fn compile_branch(branch: &crate::ast::StepBranch) -> BranchPlan {
    BranchPlan {
        label: branch.label.clone(),
        source: branch.source.clone(),
        field: branch.field.clone(),
        guard: branch.guard.clone(),
        agg: AggPlan {
            transforms: branch.pipe.transforms.clone(),
            measure: branch.pipe.measure,
            cmp: branch.pipe.cmp,
            threshold: branch.pipe.threshold.clone(),
        },
    }
}

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

fn compile_entity(rule: &RuleDecl) -> EntityPlan {
    let raw = match &rule.entity.entity_type {
        EntityTypeVal::Ident(s) | EntityTypeVal::StringLit(s) => s.clone(),
    };
    EntityPlan {
        entity_type: raw.to_ascii_lowercase(),
        entity_id_expr: rule.entity.id_expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

fn compile_score(rule: &RuleDecl) -> ScorePlan {
    ScorePlan {
        expr: rule.score.expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

fn compile_yield(rule: &RuleDecl) -> YieldPlan {
    YieldPlan {
        target: rule.yield_clause.target.clone(),
        version: rule.yield_clause.version,
        fields: rule
            .yield_clause
            .args
            .iter()
            .map(|arg| YieldField {
                name: arg.name.clone(),
                value: arg.value.clone(),
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

fn compile_joins(joins: &[crate::ast::JoinClause]) -> Vec<JoinPlan> {
    joins
        .iter()
        .map(|j| JoinPlan {
            right_window: j.target_window.clone(),
            mode: j.mode.clone(),
            conds: j
                .conditions
                .iter()
                .map(|c| JoinCondPlan {
                    left: c.left.clone(),
                    right: c.right.clone(),
                })
                .collect(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

fn compile_limits(limits: &Option<crate::ast::LimitsBlock>) -> Option<LimitsPlan> {
    let limits = limits.as_ref()?;

    let mut max_state_bytes = None;
    let mut max_cardinality = None;
    let mut max_emit_rate = None;
    let mut on_exceed = ExceedAction::Throttle; // default

    for item in &limits.items {
        match item.key.as_str() {
            "max_state" => {
                max_state_bytes = parse_byte_size(&item.value);
            }
            "max_cardinality" => {
                max_cardinality = item.value.parse::<usize>().ok();
            }
            "max_emit_rate" => {
                max_emit_rate = parse_rate_spec(&item.value);
            }
            "on_exceed" => {
                on_exceed = match item.value.as_str() {
                    "throttle" => ExceedAction::Throttle,
                    "drop_oldest" => ExceedAction::DropOldest,
                    "fail_rule" => ExceedAction::FailRule,
                    _ => ExceedAction::Throttle,
                };
            }
            _ => {}
        }
    }

    Some(LimitsPlan {
        max_state_bytes,
        max_cardinality,
        max_emit_rate,
        on_exceed,
    })
}

fn parse_byte_size(s: &str) -> Option<usize> {
    let s_upper = s.to_uppercase();
    if let Some(num_str) = s_upper.strip_suffix("GB") {
        num_str
            .trim()
            .parse::<usize>()
            .ok()
            .map(|n| n * 1024 * 1024 * 1024)
    } else if let Some(num_str) = s_upper.strip_suffix("MB") {
        num_str
            .trim()
            .parse::<usize>()
            .ok()
            .map(|n| n * 1024 * 1024)
    } else if let Some(num_str) = s_upper.strip_suffix("KB") {
        num_str.trim().parse::<usize>().ok().map(|n| n * 1024)
    } else {
        s.parse::<usize>().ok()
    }
}

fn parse_rate_spec(s: &str) -> Option<RateSpec> {
    let parts: Vec<&str> = s.splitn(2, '/').collect();
    if parts.len() != 2 {
        return None;
    }
    let count = parts[0].trim().parse::<u64>().ok()?;
    let per = match parts[1].trim() {
        "s" | "sec" => Duration::from_secs(1),
        "m" | "min" => Duration::from_secs(60),
        "h" | "hr" | "hour" => Duration::from_secs(3600),
        "d" | "day" => Duration::from_secs(86400),
        _ => return None,
    };
    Some(RateSpec { count, per })
}
