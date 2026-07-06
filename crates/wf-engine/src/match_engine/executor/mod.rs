mod alert;
mod close_exec;
mod context;
mod each_exec;
mod eval;
mod match_exec;

use std::collections::HashMap;

use wf_lang::FieldType;
use wf_lang::ast::Expr;
use wf_lang::plan::RulePlan;

use self::eval::eval_bool_expr_with_lookup;
use crate::match_engine::match_engine::{Event, WindowLookup};

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`OutputRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct RuleExecutor {
    plan: RulePlan,
    yield_field_types: HashMap<String, FieldType>,
}

impl RuleExecutor {
    pub fn new(plan: RulePlan) -> Self {
        Self {
            plan,
            yield_field_types: HashMap::new(),
        }
    }

    pub fn new_with_yield_field_types(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
    ) -> Self {
        Self {
            plan,
            yield_field_types,
        }
    }

    pub fn plan(&self) -> &RulePlan {
        &self.plan
    }

    pub(crate) fn yield_field_type(&self, name: &str) -> Option<&FieldType> {
        self.yield_field_types.get(name)
    }

    pub(crate) fn build_machine_id(&self, machine_id: &str) -> String {
        if machine_id.is_empty() {
            self.plan.name.clone()
        } else {
            machine_id.to_string()
        }
    }

    pub(crate) fn build_scope_key(
        &self,
        keys: &[wf_lang::ast::FieldRef],
        scope_values: &[crate::match_engine::match_engine::Value],
    ) -> String {
        keys.iter()
            .zip(scope_values.iter())
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    crate::match_engine::match_engine::field_ref_name(k),
                    crate::match_engine::match_engine::value_to_string(v)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn event_matches_alias(
        &self,
        alias: &str,
        event: &Event,
        windows: Option<&dyn WindowLookup>,
    ) -> bool {
        let filter = self
            .plan
            .binds
            .iter()
            .find(|bind| bind.alias == alias)
            .and_then(|bind| bind.filter.as_ref());
        passes_bind_filter(filter, event, windows)
    }

    pub fn is_aux_bind_alias(&self, alias: &str) -> bool {
        !self
            .plan
            .match_plan
            .event_steps
            .iter()
            .chain(self.plan.match_plan.close_steps.iter())
            .flat_map(|step| step.branches.iter())
            .any(|branch| branch.source == alias)
    }
}

fn passes_bind_filter(
    filter: Option<&Expr>,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
) -> bool {
    match filter.and_then(|expr| eval_bool_expr_with_lookup(expr, event, windows)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}
