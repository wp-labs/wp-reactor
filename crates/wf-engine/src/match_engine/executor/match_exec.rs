use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::Expr;

use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::CoreResult;
use crate::match_engine::match_engine::{
    Event, MatchedContext, Value, WindowLookup, eval_field_value, value_to_string,
};

use super::RuleExecutor;
use super::YieldKind;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc, now_nanos};
use super::context::{build_eval_context, execute_joins};
use super::eval::{
    YieldMeta, eval_entity_id, eval_score, eval_yield_expr_with_meta, with_yield_eval_scope,
};

impl RuleExecutor {
    /// Produce an [`OutputRecord`] from an on-event match (L1 — no joins).
    pub fn execute_match(&self, matched: &MatchedContext) -> CoreResult<OutputRecord> {
        self.execute_match_at(matched, now_nanos())
    }

    /// [`execute_match`] with an explicit emit timestamp (the runtime's
    /// batch-level cached wall clock), so every record in a batch shares one
    /// `emit_time` without a per-record `SystemTime::now()` syscall.
    pub fn execute_match_at(
        &self,
        matched: &MatchedContext,
        emit_time_nanos: i64,
    ) -> CoreResult<OutputRecord> {
        let step_plans: Vec<_> = self.plan.match_plan.event_steps.iter().collect();
        let ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_deref(),
            &self.close_ctx_fields,
        );
        self.build_match_alert(matched, &ctx, emit_time_nanos)
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
        self.execute_match_with_joins_at(matched, windows, now_nanos())
    }

    /// [`execute_match_with_joins`] with an explicit emit timestamp.
    pub fn execute_match_with_joins_at(
        &self,
        matched: &MatchedContext,
        windows: &dyn WindowLookup,
        emit_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        let step_plans: Vec<_> = self.plan.match_plan.event_steps.iter().collect();
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_deref(),
            &self.close_ctx_fields,
        );
        if !execute_joins(
            &self.live_joins,
            &mut ctx,
            windows,
            matched.event_time_nanos,
        ) {
            return Ok(None);
        }
        // Post-join `where`: strict — false/None (e.g. join miss leaves the
        // joined field absent) suppresses the output (INNER JOIN semantics).
        if !self.where_ok(&ctx) {
            return Ok(None);
        }
        self.build_match_alert(matched, &ctx, emit_time_nanos)
            .map(Some)
    }

    /// Internal: build the OutputRecord from an already-constructed eval context.
    pub(crate) fn build_match_alert(
        &self,
        matched: &MatchedContext,
        ctx: &Event,
        emit_time_nanos: i64,
    ) -> CoreResult<OutputRecord> {
        let score = match self.output_static().score_const {
            Some(s) => s,
            None => eval_score(&self.plan.score_plan.expr, ctx)?,
        };
        // Field-typed entity (e.g. `digit(b.auction)`) takes the direct flat
        // lookup — skipping the interpreter's per-record eval-time scope. A
        // missing field degrades to an empty string, byte-identical to the
        // interpreter wrapper (`eval_yield_expr_with_meta` substitutes `""`).
        let entity_id = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => eval_field_value(&ctx.fields, fr)
                .map(|v| value_to_string(&v))
                .unwrap_or_default(),
            _ => eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?,
        };
        let origin = AlertOrigin::Event;
        let fired_at = format_nanos_utc(matched.event_time_nanos);
        let emit_time = self.cached_emit_time(emit_time_nanos);
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
                time_format: Some(self.output_config().time_format.as_str()),
            };
            // Plan fields, precomputed specs, and precomputed yield kinds are
            // all index-aligned (see `OutputStatic`) — no per-field name clone,
            // type-map lookup, or expression re-classification on the hot path.
            self.plan
                .yield_plan
                .fields
                .iter()
                .zip(self.output_static().yield_specs.iter())
                .zip(self.output_static().yield_kinds.iter())
                .map(|((field, (name, field_type)), kind)| {
                    let value = match kind {
                        YieldKind::Lit(v) => v.clone(),
                        YieldKind::Field => {
                            let Expr::Field(fr) = &field.value else {
                                unreachable!("YieldKind::Field implies an Expr::Field value")
                            };
                            // Missing field falls back to an empty string,
                            // exactly like the interpreter wrapper.
                            eval_field_value(&ctx.fields, fr)
                                .unwrap_or_else(|| Value::Str(SmolStr::default()))
                        }
                        YieldKind::General => {
                            eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                                .expect("eval_yield_expr_with_meta never returns None")
                        }
                    };
                    let Some(value) = RuleExecutor::coerce_yield_field_value_with(
                        name,
                        field_type.as_ref(),
                        value,
                    )?
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

        let machine_id = self.build_machine_id(&matched.machine_id);
        let scope_key = self.build_scope_key(&self.plan.match_plan.keys, &matched.scope_key);
        let statics = self.output_static();

        Ok(OutputRecord {
            wfx_id,
            rule_name: Arc::clone(&statics.rule_name),
            score,
            entity_type: Arc::clone(&statics.entity_type),
            entity_id,
            origin,
            fired_at,
            emit_time,
            matched_rows: vec![],
            summary: Arc::from(summary),
            yield_target: Arc::clone(&statics.yield_target),
            yield_fields,
            yield_field_types: Arc::clone(&statics.yield_field_types),
            event_time_nanos: matched.event_time_nanos,
            machine_id,
            scope_key: Arc::from(scope_key),
        })
    }
}
