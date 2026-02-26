use wf_lang::ast::CloseMode;

use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::CoreResult;
use crate::rule::match_engine::{CloseOutput, Event, StepData, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc};
use super::context::{build_eval_context, execute_joins};
use super::eval::{eval_entity_id, eval_score, eval_yield_expr};

/// Check whether a close output qualifies to produce an alert.
fn is_qualified(close: &CloseOutput) -> bool {
    match close.close_mode {
        CloseMode::And => close.event_ok && close.close_ok,
        CloseMode::Or => {
            // In OR mode, the close path only qualifies when close steps
            // exist. When there are no close steps (close_mode defaults to
            // Or when no close block is present), the close output should
            // not produce an alert — the event path already handles it.
            close.close_ok && !close.close_step_data.is_empty()
        }
    }
}

impl RuleExecutor {
    /// Produce an [`OutputRecord`] from a close output (L1 — no joins).
    ///
    /// Returns `Ok(None)` when the instance did not qualify for an alert.
    pub fn execute_close(&self, close: &CloseOutput) -> CoreResult<Option<OutputRecord>> {
        if !is_qualified(close) {
            return Ok(None);
        }
        let all_step_data = combine_step_data(close);
        let step_plans = combine_step_plans(self, close);
        let ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &close.scope_key,
            &all_step_data,
            &step_plans,
        );
        self.build_close_alert(close, &all_step_data, &ctx)
    }

    /// Produce an [`OutputRecord`] from a close output with join support.
    pub fn execute_close_with_joins(
        &self,
        close: &CloseOutput,
        windows: &dyn WindowLookup,
    ) -> CoreResult<Option<OutputRecord>> {
        if !is_qualified(close) {
            return Ok(None);
        }
        let all_step_data = combine_step_data(close);
        let step_plans = combine_step_plans(self, close);
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &close.scope_key,
            &all_step_data,
            &step_plans,
        );
        execute_joins(&self.plan.joins, &mut ctx, windows, close.last_event_nanos);
        self.build_close_alert(close, &all_step_data, &ctx)
    }

    /// Internal: build the OutputRecord from an already-constructed eval context.
    fn build_close_alert(
        &self,
        close: &CloseOutput,
        all_step_data: &[StepData],
        ctx: &Event,
    ) -> CoreResult<Option<OutputRecord>> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let origin = AlertOrigin::Close {
            reason: close.close_reason,
        };
        let fired_at = format_nanos_utc(close.watermark_nanos);
        let wfx_id = build_wfx_id(
            &self.plan.name,
            &close.scope_key,
            &fired_at,
            all_step_data,
            &origin,
        );
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &close.scope_key,
            all_step_data,
            &origin,
        );
        let yield_fields = self
            .plan
            .yield_plan
            .fields
            .iter()
            .filter_map(|field| {
                let value = eval_yield_expr(&field.value, ctx)?;
                Some((field.name.clone(), value))
            })
            .collect();

        Ok(Some(OutputRecord {
            wfx_id,
            rule_name: self.plan.name.clone(),
            score,
            entity_type: self.plan.entity_plan.entity_type.clone(),
            entity_id,
            origin,
            fired_at,
            matched_rows: vec![],
            summary,
            yield_target: self.plan.yield_plan.target.clone(),
            yield_fields,
            event_time_nanos: close.last_event_nanos,
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

fn combine_step_plans<'a>(
    executor: &'a RuleExecutor,
    close: &CloseOutput,
) -> Vec<&'a wf_lang::plan::StepPlan> {
    let event_count = close.event_step_data.len();
    let close_count = close.close_step_data.len();
    executor
        .plan
        .match_plan
        .event_steps
        .iter()
        .take(event_count)
        .chain(
            executor
                .plan
                .match_plan
                .close_steps
                .iter()
                .take(close_count),
        )
        .collect()
}
