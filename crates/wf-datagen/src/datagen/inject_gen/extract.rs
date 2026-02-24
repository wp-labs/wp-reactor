use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::RulePlan;
use wf_lang::plan::WindowSpec;

use super::structures::{AliasMap, InjectOverrides, RuleStructure, StepInfo};
use crate::wfg_ast::{InjectLine, ParamValue};

pub(super) fn extract_rule_structure(
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

pub(crate) fn field_ref_field_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        _ => "",
    }
}

pub(super) fn extract_inject_overrides(inject_line: &InjectLine) -> InjectOverrides {
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

fn extract_entity_id_field(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Field(fr) => Some(field_ref_field_name(fr).to_string()),
        _ => None,
    }
}
