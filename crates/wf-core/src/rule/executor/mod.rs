mod alert;
mod close_exec;
mod context;
mod eval;
mod match_exec;

use wf_lang::plan::RulePlan;

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`OutputRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
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
}
