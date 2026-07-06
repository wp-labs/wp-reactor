use wf_lang::ast::CloseMode;

use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::match_engine::{CloseOutput, Event, StepData, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc, format_now_utc};
use super::context::{build_eval_context, execute_joins};
use super::eval::{eval_entity_id, eval_score, eval_yield_expr_with_score};

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
            &close.bind_data,
            &step_plans,
        );
        let ctx = annotate_close_step_stages(ctx, close.event_step_data.len());
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
            &close.bind_data,
            &step_plans,
        );
        ctx = annotate_close_step_stages(ctx, close.event_step_data.len());
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
        let emit_time = format_now_utc();
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
            .map(|field| {
                let Some(value) = eval_yield_expr_with_score(&field.value, ctx, Some(score)) else {
                    return Err(
                        orion_error::StructError::from(CoreReason::RuleExec).with_detail(format!(
                            "close yield field {:?} expression evaluated to None",
                            field.name
                        )),
                    );
                };
                Ok((field.name.clone(), value))
            })
            .collect::<CoreResult<Vec<_>>>()?;
        let yield_field_types = self
            .plan
            .yield_plan
            .fields
            .iter()
            .filter_map(|field| {
                self.yield_field_type(&field.name)
                    .cloned()
                    .map(|field_type| (field.name.clone(), field_type))
            })
            .collect();

        let machine_id = self.build_machine_id(&close.machine_id);
        let scope_key = self.build_scope_key(&self.plan.match_plan.keys, &close.scope_key);

        Ok(Some(OutputRecord {
            wfx_id,
            rule_name: self.plan.name.clone(),
            score,
            entity_type: self.plan.entity_plan.entity_type.clone(),
            entity_id,
            origin,
            fired_at,
            emit_time,
            matched_rows: vec![],
            summary,
            yield_target: self.plan.yield_plan.target.clone(),
            yield_fields,
            yield_field_types,
            event_time_nanos: close.last_event_nanos,
            machine_id,
            scope_key,
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

fn annotate_close_step_stages(mut ctx: Event, event_step_count: usize) -> Event {
    for step_idx in 0..ctx
        .fields
        .keys()
        .filter_map(|key| {
            key.strip_prefix("_step_")?
                .split('_')
                .next()?
                .parse::<usize>()
                .ok()
        })
        .max()
        .map(|max_idx| max_idx + 1)
        .unwrap_or(0)
    {
        let stage = if step_idx < event_step_count {
            "event"
        } else {
            "close"
        };
        ctx.fields.insert(
            format!("_step_{}_stage", step_idx),
            crate::match_engine::match_engine::Value::Str(stage.to_string()),
        );
    }
    ctx
}
