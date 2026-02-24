use crate::alert::OutputRecord;
use crate::error::CoreResult;
use crate::rule::match_engine::{Event, MatchedContext, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc};
use super::context::{build_eval_context, execute_joins};
use super::eval::{eval_entity_id, eval_score};

impl RuleExecutor {
    /// Produce an [`OutputRecord`] from an on-event match (L1 — no joins).
    pub fn execute_match(&self, matched: &MatchedContext) -> CoreResult<OutputRecord> {
        let ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
        );
        self.build_match_alert(matched, &ctx)
    }

    /// Produce an [`OutputRecord`] from an on-event match with join support.
    ///
    /// Executes joins before score/entity evaluation, enriching the eval
    /// context with joined fields from external windows.
    pub fn execute_match_with_joins(
        &self,
        matched: &MatchedContext,
        windows: &dyn WindowLookup,
    ) -> CoreResult<OutputRecord> {
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
        );
        execute_joins(
            &self.plan.joins,
            &mut ctx,
            windows,
            matched.event_time_nanos,
        );
        self.build_match_alert(matched, &ctx)
    }

    /// Internal: build the OutputRecord from an already-constructed eval context.
    fn build_match_alert(&self, matched: &MatchedContext, ctx: &Event) -> CoreResult<OutputRecord> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let fired_at = format_nanos_utc(matched.event_time_nanos);
        let wfx_id = build_wfx_id(
            &self.plan.name,
            &matched.scope_key,
            &fired_at,
            &matched.step_data,
            None,
        );
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            None,
        );

        Ok(OutputRecord {
            wfx_id,
            rule_name: self.plan.name.clone(),
            score,
            entity_type: self.plan.entity_plan.entity_type.clone(),
            entity_id,
            close_reason: None,
            fired_at,
            matched_rows: vec![],
            summary,
            yield_target: self.plan.yield_plan.target.clone(),
        })
    }
}
