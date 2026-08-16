use std::sync::Arc;

use smol_str::SmolStr;

use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::MACHINE_ID;
use crate::match_engine::match_engine::{CepStateMachine, Event, WindowLookup};

use super::RuleExecutor;
use super::alert::{build_each_wfx_id, format_nanos_utc, now_nanos};
use super::context::execute_joins;
use super::eval::{
    YieldMeta, eval_bool_expr, eval_entity_id, eval_score, eval_yield_expr_with_meta,
    with_yield_eval_scope,
};

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
        self.build_each_alert(event, event_time_nanos, &[], now_nanos())
    }

    /// Produce an [`OutputRecord`] from a single event in `on each` mode with
    /// join support.
    ///
    /// `field_order` is the event schema's field names in sorted order,
    /// precomputed once per batch by the caller (events within one batch share
    /// the window schema). Pass `&[]` to compute the order per event instead.
    ///
    /// `emit_time_nanos` is the record's emit timestamp. The runtime passes a
    /// batch-level cached wall clock so `emit_time` formats once per batch
    /// (see [`RuleExecutor::cached_emit_time`]).
    pub fn execute_each_with_joins(
        &self,
        event: &Event,
        event_time_nanos: i64,
        windows: &dyn WindowLookup,
        field_order: &[&SmolStr],
        emit_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        let Some(each_plan) = &self.plan.each_plan else {
            return Err(orion_error::StructError::from(CoreReason::RuleExec)
                .with_detail("execute_each_with_joins called for non-`on each` rule"));
        };
        if !passes_each_filter(each_plan.filter.as_ref(), event) {
            return Ok(None);
        }
        // Rules without joins never mutate the event — skip the per-event
        // `fields` HashMap clone entirely (profile: the clone + its drop were
        // ~3% of on-CPU samples on pass-through rules).
        if self.plan.joins.is_empty() {
            return self.build_each_alert(event, event_time_nanos, field_order, emit_time_nanos);
        }
        let mut ctx = event.clone();
        if !execute_joins(&self.plan.joins, &mut ctx, windows, event_time_nanos) {
            return Ok(None);
        }
        self.build_each_alert(&ctx, event_time_nanos, field_order, emit_time_nanos)
    }

    fn build_each_alert(
        &self,
        ctx: &Event,
        event_time_nanos: i64,
        field_order: &[&SmolStr],
        emit_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        let statics = self.output_static();
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let origin = AlertOrigin::Event;
        let fired_at = format_nanos_utc(event_time_nanos);
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let wfx_id =
            build_each_wfx_id(&self.plan.name, event_time_nanos, ctx, &origin, field_order);
        // Summary is a plan constant on this path (empty scope + empty steps)
        // — precomputed in `OutputStatic`, no per-event formatting.
        let summary = Arc::clone(statics.each_summary.as_ref().expect(
            "on-each rule missing precomputed summary",
        ));
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
                emit_time: Some(&*emit_time),
                summary: Some(&summary),
                event_first_time_nanos: Some(event_time_nanos),
                event_last_time_nanos: Some(event_time_nanos),
                window_start_time_nanos: Some(event_time_nanos),
                window_end_time_nanos: Some(event_time_nanos),
                emit_time_nanos: Some(emit_time_nanos),
                time_format: Some(self.output_config().time_format.as_str()),
            };
            // Plan fields and precomputed specs are index-aligned; iterate
            // both at once — no per-field name clone or type-map lookup.
            self.plan
                .yield_plan
                .fields
                .iter()
                .zip(statics.yield_specs.iter())
                .map(|(field, (name, field_type))| {
                    let Some(value) = eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                    else {
                        return Err(orion_error::StructError::from(CoreReason::RuleExec)
                            .with_detail(format!(
                                "on each yield field {:?} expression evaluated to None",
                                field.name
                            )));
                    };
                    let Some(value) =
                        RuleExecutor::coerce_yield_field_value_with(name, field_type.as_ref(), value)?
                    else {
                        // Optional input field was missing → omit it from the
                        // output record (wp-labs/warp-fusion#62).
                        return Ok(None);
                    };
                    Ok(Some((Arc::clone(name), value)))
                })
                .filter_map(Result::transpose)
                .collect::<CoreResult<Vec<_>>>()
        })?;

        let machine_id = CepStateMachine::extract_event_str(ctx, MACHINE_ID);

        Ok(Some(OutputRecord {
            wfx_id,
            rule_name: Arc::clone(&statics.rule_name),
            score,
            entity_type: Arc::clone(&statics.entity_type),
            entity_id,
            origin,
            fired_at,
            emit_time,
            matched_rows: vec![],
            summary,
            yield_target: Arc::clone(&statics.yield_target),
            yield_fields,
            yield_field_types: Arc::clone(&statics.yield_field_types),
            event_time_nanos,
            machine_id,
            scope_key: Arc::clone(&statics.rule_name),
        }))
    }
}

fn passes_each_filter(filter: Option<&wf_lang::ast::Expr>, event: &Event) -> bool {
    match filter.and_then(|expr| eval_bool_expr(expr, event)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}
