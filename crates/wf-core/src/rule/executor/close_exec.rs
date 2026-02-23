use crate::alert::AlertRecord;
use crate::error::CoreResult;
use crate::rule::match_engine::{CloseOutput, Event, StepData, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_alert_id, build_summary, format_nanos_utc};
use super::context::{build_eval_context, execute_joins};
use super::eval::{eval_entity_id, eval_score};

impl RuleExecutor {
    /// Produce an [`AlertRecord`] from a close output (L1 — no joins).
    ///
    /// Returns `Ok(None)` when `!event_ok || !close_ok` — the instance
    /// did not fully satisfy the rule.
    pub fn execute_close(&self, close: &CloseOutput) -> CoreResult<Option<AlertRecord>> {
        if !close.event_ok || !close.close_ok {
            return Ok(None);
        }
        let all_step_data = combine_step_data(close);
        let ctx = build_eval_context(&self.plan.match_plan.keys, &close.scope_key, &all_step_data);
        self.build_close_alert(close, &all_step_data, &ctx)
    }

    /// Produce an [`AlertRecord`] from a close output with join support.
    pub fn execute_close_with_joins(
        &self,
        close: &CloseOutput,
        windows: &dyn WindowLookup,
    ) -> CoreResult<Option<AlertRecord>> {
        if !close.event_ok || !close.close_ok {
            return Ok(None);
        }
        let all_step_data = combine_step_data(close);
        let mut ctx =
            build_eval_context(&self.plan.match_plan.keys, &close.scope_key, &all_step_data);
        execute_joins(&self.plan.joins, &mut ctx, windows);
        self.build_close_alert(close, &all_step_data, &ctx)
    }

    /// Internal: build the AlertRecord from an already-constructed eval context.
    fn build_close_alert(
        &self,
        close: &CloseOutput,
        all_step_data: &[StepData],
        ctx: &Event,
    ) -> CoreResult<Option<AlertRecord>> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let close_reason_str = close.close_reason.as_str().to_string();
        let fired_at = format_nanos_utc(close.watermark_nanos);
        let alert_id = build_alert_id(&self.plan.name, &close.scope_key, &fired_at);
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &close.scope_key,
            all_step_data,
            Some(&close_reason_str),
        );

        Ok(Some(AlertRecord {
            alert_id,
            rule_name: self.plan.name.clone(),
            score,
            entity_type: self.plan.entity_plan.entity_type.clone(),
            entity_id,
            close_reason: Some(close_reason_str),
            fired_at,
            matched_rows: vec![],
            summary,
        }))
    }
}

/// Combine event + close step data into a single vec.
fn combine_step_data(close: &CloseOutput) -> Vec<StepData> {
    close
        .event_step_data
        .iter()
        .chain(close.close_step_data.iter())
        .cloned()
        .collect()
}
