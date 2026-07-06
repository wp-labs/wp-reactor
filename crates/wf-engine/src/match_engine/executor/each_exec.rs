use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::MACHINE_ID;
use crate::match_engine::match_engine::{CepStateMachine, Event, StepData, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_each_wfx_id, build_summary, format_nanos_utc, format_now_utc};
use super::context::execute_joins;
use super::eval::{eval_bool_expr, eval_entity_id, eval_score, eval_yield_expr_with_score};

impl RuleExecutor {
    /// Produce an [`OutputRecord`] from a single event in `on each` mode.
    ///
    /// Returns `Ok(None)` when the optional `where` filter rejects the event.
    pub fn execute_each(
        &self,
        event: &Event,
        event_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        let Some(each_plan) = &self.plan.each_plan else {
            return Err(orion_error::StructError::from(CoreReason::RuleExec)
                .with_detail("execute_each called for non-`on each` rule"));
        };
        if !passes_each_filter(each_plan.filter.as_ref(), event) {
            return Ok(None);
        }
        self.build_each_alert(event, event_time_nanos)
    }

    /// Produce an [`OutputRecord`] from a single event in `on each` mode with join support.
    pub fn execute_each_with_joins(
        &self,
        event: &Event,
        event_time_nanos: i64,
        windows: &dyn WindowLookup,
    ) -> CoreResult<Option<OutputRecord>> {
        let Some(each_plan) = &self.plan.each_plan else {
            return Err(orion_error::StructError::from(CoreReason::RuleExec)
                .with_detail("execute_each_with_joins called for non-`on each` rule"));
        };
        if !passes_each_filter(each_plan.filter.as_ref(), event) {
            return Ok(None);
        }
        let mut ctx = event.clone();
        if !execute_joins(&self.plan.joins, &mut ctx, windows, event_time_nanos) {
            return Ok(None);
        }
        self.build_each_alert(&ctx, event_time_nanos)
    }

    fn build_each_alert(
        &self,
        ctx: &Event,
        event_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let origin = AlertOrigin::Event;
        let fired_at = format_nanos_utc(event_time_nanos);
        let emit_time = format_now_utc();
        let empty_steps: Vec<StepData> = Vec::new();
        let wfx_id = build_each_wfx_id(&self.plan.name, event_time_nanos, ctx, &origin);
        let summary = build_summary(&self.plan.name, &[], &[], &empty_steps, &origin);
        let yield_fields = self
            .plan
            .yield_plan
            .fields
            .iter()
            .map(|field| {
                let Some(value) = eval_yield_expr_with_score(&field.value, ctx, Some(score)) else {
                    return Err(
                        orion_error::StructError::from(CoreReason::RuleExec).with_detail(format!(
                            "on each yield field {:?} expression evaluated to None",
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

        let machine_id = CepStateMachine::extract_event_str(ctx, MACHINE_ID);

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
            event_time_nanos,
            machine_id,
            scope_key: self.plan.name.clone(),
        }))
    }
}

fn passes_each_filter(filter: Option<&wf_lang::ast::Expr>, event: &Event) -> bool {
    match filter.and_then(|expr| eval_bool_expr(expr, event)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}
