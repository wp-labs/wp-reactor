mod display;
mod format;
mod sections;
#[cfg(test)]
mod tests;

use crate::plan::RulePlan;
use crate::schema::WindowSchema;

pub use format::{format_cmp, format_expr, format_field_ref, format_measure};

use sections::{
    compute_lineage, explain_binds, explain_conv, explain_joins, explain_limits, explain_match,
    explain_yield,
};

/// Human-readable explanation of a compiled rule.
#[derive(Debug)]
pub struct RuleExplanation {
    pub name: String,
    pub pattern_origin: Option<(String, Vec<String>)>,
    pub bindings: Vec<BindingExpl>,
    pub match_expl: MatchExpl,
    pub score: String,
    pub joins: Vec<String>,
    pub entity_type: String,
    pub entity_id: String,
    pub yield_target: String,
    pub yield_fields: Vec<(String, String)>,
    pub conv: Option<Vec<String>>,
    pub limits: Option<String>,
    pub lineage: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct BindingExpl {
    pub alias: String,
    pub window: String,
    pub filter: Option<String>,
}

#[derive(Debug)]
pub struct MatchExpl {
    pub keys: String,
    pub window_spec: String,
    pub event_steps: Vec<String>,
    pub close_steps: Vec<String>,
}

/// Build explanations for a set of compiled rules.
pub fn explain_rules(plans: &[RulePlan], schemas: &[WindowSchema]) -> Vec<RuleExplanation> {
    plans.iter().map(|p| explain_rule(p, schemas)).collect()
}

fn explain_rule(plan: &RulePlan, schemas: &[WindowSchema]) -> RuleExplanation {
    let bindings = explain_binds(&plan.binds);
    let match_expl = explain_match(&plan.match_plan);
    let score = format_expr(&plan.score_plan.expr);
    let joins = explain_joins(&plan.joins);
    let entity_type = plan.entity_plan.entity_type.clone();
    let entity_id = format_expr(&plan.entity_plan.entity_id_expr);
    let yield_target = match plan.yield_plan.version {
        Some(v) => format!("{}@v{}", plan.yield_plan.target, v),
        None => plan.yield_plan.target.clone(),
    };
    let yield_fields = explain_yield(&plan.yield_plan);
    let conv = plan.conv_plan.as_ref().map(explain_conv);
    let limits = plan.limits_plan.as_ref().map(explain_limits);
    let lineage = compute_lineage(&plan.binds, &plan.yield_plan, schemas);
    let pattern_origin = plan.pattern_origin.as_ref().map(|po| {
        (po.pattern_name.clone(), po.args.clone())
    });

    RuleExplanation {
        name: plan.name.clone(),
        pattern_origin,
        bindings,
        match_expl,
        score,
        joins,
        entity_type,
        entity_id,
        yield_target,
        yield_fields,
        conv,
        limits,
        lineage,
    }
}
