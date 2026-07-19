use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::match_engine::{Event, MatchedContext, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc, now_nanos};
use super::context::{build_eval_context, execute_joins};
use super::eval::{
    YieldMeta, eval_entity_id, eval_score, eval_yield_expr_with_meta, with_yield_eval_scope,
};

impl RuleExecutor {
    /// Produce an [`OutputRecord`] from an on-event match (L1 — no joins).
    pub fn execute_match(&self, matched: &MatchedContext) -> CoreResult<OutputRecord> {
        let step_plans: Vec<_> = self.plan.match_plan.event_steps.iter().collect();
        let ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
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
    ) -> CoreResult<Option<OutputRecord>> {
        let step_plans: Vec<_> = self.plan.match_plan.event_steps.iter().collect();
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
        );
        if !execute_joins(
            &self.plan.joins,
            &mut ctx,
            windows,
            matched.event_time_nanos,
        ) {
            return Ok(None);
        }
        self.build_match_alert(matched, &ctx).map(Some)
    }

    /// Internal: build the OutputRecord from an already-constructed eval context.
    fn build_match_alert(&self, matched: &MatchedContext, ctx: &Event) -> CoreResult<OutputRecord> {
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let origin = AlertOrigin::Event;
        let fired_at = format_nanos_utc(matched.event_time_nanos);
        let emit_time_nanos = now_nanos();
        let emit_time = format_nanos_utc(emit_time_nanos);
        let wfx_id = build_wfx_id(
            &self.plan.name,
            &matched.scope_key,
            &fired_at,
            &matched.step_data,
            &origin,
        );
        let summary = build_summary(
            &self.plan.name,
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &origin,
        );
        let yield_fields = with_yield_eval_scope(|| {
            let yield_meta = YieldMeta {
                score: Some(score),
                wfx_id: Some(&wfx_id),
                rule_name: Some(&self.plan.name),
                entity_type: Some(&self.plan.entity_plan.entity_type),
                entity_id: Some(&entity_id),
                origin: Some(origin.as_str()),
                close_reason: Some(""),
                fired_at: Some(&fired_at),
                emit_time: Some(&emit_time),
                summary: Some(&summary),
                event_first_time_nanos: Some(matched.event_first_time_nanos),
                event_last_time_nanos: Some(matched.event_last_time_nanos),
                window_start_time_nanos: Some(matched.window_start_time_nanos),
                window_end_time_nanos: Some(matched.window_end_time_nanos),
                emit_time_nanos: Some(emit_time_nanos),
            };
            self.plan
                .yield_plan
                .fields
                .iter()
                .map(|field| {
                    let Some(value) = eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                    else {
                        return Err(orion_error::StructError::from(CoreReason::RuleExec)
                            .with_detail(format!(
                                "match yield field {:?} expression evaluated to None",
                                field.name
                            )));
                    };
                    Ok((field.name.clone(), value))
                })
                .collect::<CoreResult<Vec<_>>>()
        })?;
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

        let machine_id = self.build_machine_id(&matched.machine_id);
        let scope_key = self.build_scope_key(&self.plan.match_plan.keys, &matched.scope_key);

        Ok(OutputRecord {
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
            event_time_nanos: matched.event_time_nanos,
            machine_id,
            scope_key,
        })
    }
}
