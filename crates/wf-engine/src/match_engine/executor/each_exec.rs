use std::borrow::Cow;
use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::{Expr, FieldRef};

use crate::alert::{AlertColumnBuilder, EachRowCells};
use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::MACHINE_ID;
use crate::match_engine::event_bridge::ColumnarEvent;
use crate::match_engine::match_engine::{
    CepStateMachine, Event, Value, WindowLookup, eval_field_value, field_ref_name, value_to_string,
};

use super::RuleExecutor;
use super::alert::{
    build_each_wfx_id, build_each_wfx_id_columnar_reusing, build_each_wfx_id_reusing,
    format_nanos_utc, now_nanos,
};
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

    /// On-each direct-write emit (plan C2): evaluates the event and appends
    /// the row straight into `builder`'s columns, skipping the per-record
    /// `OutputRecord` materialization entirely (record struct + yield-field
    /// `Vec` + per-record `String`→`Arc` copies of the constant system
    /// fields were the dominant remaining build/drop cost after C1).
    ///
    /// Semantics are identical to [`Self::execute_each_with_joins`] followed
    /// by `AlertColumnBuilder::append_record` — locked by unit test. Returns
    /// `Ok(false)` when the optional `where` filter rejects the event (or a
    /// join rejects it), in which case nothing was appended.
    ///
    /// Only for rules whose yield target is a sink (not an intermediate
    /// pipe) — the pipe path stages full row records column-wise
    /// (`PipeBatchStager` in wf-runtime), so callers keep the record path
    /// there.
    pub fn execute_each_direct(
        &self,
        event: &Event,
        event_time_nanos: i64,
        windows: &dyn WindowLookup,
        field_order: &[&SmolStr],
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
    ) -> CoreResult<bool> {
        let Some(each_plan) = &self.plan.each_plan else {
            return Err(orion_error::StructError::from(CoreReason::RuleExec)
                .with_detail("execute_each_direct called for non-`on each` rule"));
        };
        if !passes_each_filter(each_plan.filter.as_ref(), event) {
            return Ok(false);
        }
        // Rules without joins never mutate the event — skip the per-event
        // `fields` HashMap clone (same optimization as the record path).
        if self.plan.joins.is_empty() {
            self.build_each_direct(
                event,
                event_time_nanos,
                field_order,
                emit_time_nanos,
                builder,
            )?;
            return Ok(true);
        }
        let mut ctx = event.clone();
        if !execute_joins(&self.plan.joins, &mut ctx, windows, event_time_nanos) {
            return Ok(false);
        }
        self.build_each_direct(
            &ctx,
            event_time_nanos,
            field_order,
            emit_time_nanos,
            builder,
        )?;
        Ok(true)
    }

    /// Batch form of [`Self::execute_each_direct`] (build_each_direct
    /// vectorization): appends rows for a whole event batch, hoisting the
    /// plan-constant work out of the per-row loop.
    ///
    /// What is hoisted (vs. calling `execute_each_direct` per event):
    /// - constant expressions evaluate once per call: a literal score
    ///   (`Number`) is clamped once, a literal entity id / literal yield
    ///   values are built once and cloned per row;
    /// - `Expr::Field` yields resolve through `eval_field_value` directly,
    ///   skipping the recursive expression interpreter and its per-node
    ///   eval-time scope traffic;
    /// - the wfx_id rendering scratch `String` and the hex buffer are reused
    ///   across rows (byte stream identical — the scratch is cleared per
    ///   field, exactly as within one call);
    /// - the builder's columns are reserved up front.
    ///
    /// Semantics per row are identical to `execute_each_direct` — filter and
    /// join rejections skip the row, an evaluation/conversion failure skips
    /// the row (counted in `failed`, logged) without touching any column,
    /// and optional-field omission leaves sparse cells. The per-row eval-time
    /// scope is still entered per row, so `now()`-style functions observe the
    /// same per-event time they would on the per-event path. Locked by unit
    /// test against the per-event path.
    ///
    /// `appended_out` (cleared) receives the indices into `rows` that were
    /// appended, so callers can run per-row telemetry without holding the
    /// builder lock.
    pub fn execute_each_direct_batch(
        &self,
        rows: &[(&Event, i64)],
        windows: &dyn WindowLookup,
        field_order: &[&SmolStr],
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
        appended_out: &mut Vec<usize>,
    ) -> EachDirectBatchStats {
        appended_out.clear();
        let mut stats = EachDirectBatchStats::default();
        let Some(each_plan) = &self.plan.each_plan else {
            log::warn!(
                "execute_each_direct_batch called for non-`on each` rule {}; skipping {} rows",
                self.plan.name,
                rows.len()
            );
            stats.failed = rows.len();
            return stats;
        };
        let filter = each_plan.filter.as_ref();
        let statics = self.output_static();
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let summary = Arc::clone(
            statics
                .each_summary
                .as_ref()
                .expect("on-each rule missing precomputed summary"),
        );
        let origin = AlertOrigin::Event;

        // -- Plan-constant specialization (evaluated once per batch) -------
        let score_const = match &self.plan.score_plan.expr {
            // eval_score on a Number literal is clamp(n), independent of ctx.
            Expr::Number(n) => Some(n.clamp(0.0, 100.0)),
            _ => None,
        };
        let entity_const = match &self.plan.entity_plan.entity_id_expr {
            // eval_entity_id on a String literal is the string itself.
            Expr::StringLit(s) => Some(s.to_string()),
            _ => None,
        };
        // Literal yield values are built once; Field refs take the direct
        // lookup; everything else goes through the full interpreter with the
        // per-row meta.
        let yield_kinds: Vec<YieldKind> = self
            .plan
            .yield_plan
            .fields
            .iter()
            .map(|field| match &field.value {
                Expr::Number(n) => YieldKind::Lit(Value::Number(*n)),
                Expr::StringLit(s) => YieldKind::Lit(Value::Str(s.clone().into())),
                Expr::Bool(b) => YieldKind::Lit(Value::Bool(*b)),
                Expr::Field(_) => YieldKind::Field,
                _ => YieldKind::General,
            })
            .collect();

        builder.reserve_rows(rows.len());
        let mut wfx_scratch = String::new();

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            if !passes_each_filter(filter, event) {
                stats.rejected += 1;
                continue;
            }
            // Rules without joins never mutate the event — borrow instead of
            // cloning (same optimization as the per-event path).
            let ctx: Cow<'_, Event> = if self.plan.joins.is_empty() {
                Cow::Borrowed::<Event>(*event)
            } else {
                let mut ctx = Cow::<Event>::Owned((**event).clone());
                if !execute_joins(&self.plan.joins, ctx.to_mut(), windows, *event_time_nanos) {
                    stats.rejected += 1;
                    continue;
                }
                ctx
            };
            let ctx = &*ctx;

            // -- Per-row system values --------------------------------------
            let score = match score_const {
                Some(s) => s,
                None => match eval_score(&self.plan.score_plan.expr, ctx) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("alert export error: {e}");
                        stats.failed += 1;
                        continue;
                    }
                },
            };
            let entity_id = match entity_const.as_deref() {
                Some(s) => s.to_string(),
                None => match eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("alert export error: {e}");
                        stats.failed += 1;
                        continue;
                    }
                },
            };
            let fired_at = format_nanos_utc(*event_time_nanos);
            let wfx_id = build_each_wfx_id_reusing(
                &self.plan.name,
                *event_time_nanos,
                ctx,
                &origin,
                field_order,
                &mut wfx_scratch,
            );
            let yield_meta = self.each_yield_meta(
                &wfx_id,
                &fired_at,
                &emit_time,
                &summary,
                score,
                &entity_id,
                &origin,
                *event_time_nanos,
                emit_time_nanos,
            );

            // -- Yield staging (fallible work before any column push) ------
            builder.begin_row();
            let staged: CoreResult<()> = with_yield_eval_scope(|| {
                for ((field, (name, field_type)), kind) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                {
                    let value = match kind {
                        YieldKind::Lit(v) => v.clone(),
                        YieldKind::Field => {
                            let Expr::Field(fr) = &field.value else {
                                unreachable!("YieldKind::Field implies an Expr::Field value")
                            };
                            // Missing field falls back to an empty string,
                            // exactly like the interpreter path's wrapper.
                            eval_field_value(&ctx.fields, fr)
                                .unwrap_or_else(|| Value::Str(SmolStr::default()))
                        }
                        // Same fallback as the per-event path: a general
                        // expression never yields None here (the wrapper
                        // substitutes an empty string).
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
                        // Optional input field was missing → omit it from
                        // the output row (wp-labs/warp-fusion#62).
                        continue;
                    };
                    builder.stage_yield_cell(name, field_type.as_ref(), &value)?;
                }
                Ok(())
            });
            if let Err(e) = staged {
                log::warn!("alert export error: {e}");
                stats.failed += 1;
                continue;
            }
            builder.commit_each_row(EachRowCells {
                wfx_id,
                score,
                entity_id,
                fired_at,
                rule_name: &statics.rule_name,
                entity_type: &statics.entity_type,
                origin: &statics.each_origin,
                close_reason: &statics.each_close_reason,
                emit_time: &emit_time,
                summary: &summary,
            });
            stats.appended += 1;
            appended_out.push(idx);
        }
        stats
    }

    /// Whether the on-each plan can run the columnar fast path: no joins, no
    /// each filter, constant score, entity = field/const, yield values =
    /// literal/field (flat refs only), and every bind filter absent or
    /// columnar (a non-columnar bind filter falls back to the per-event
    /// interpreted `event_matches_alias`, which the columnar branch does not
    /// replicate). Anything else falls back to the Event-based path, keeping
    /// both paths byte-identical by construction.
    pub fn each_plan_columnar_safe(&self) -> bool {
        let Some(each_plan) = &self.plan.each_plan else {
            return false;
        };
        if !self.plan.joins.is_empty() || each_plan.filter.is_some() {
            return false;
        }
        if !self.plan.binds.iter().all(|b| {
            b.filter
                .as_ref()
                .map_or(true, |f| wf_lang::columnar::expr_is_columnar(f))
        }) {
            return false;
        }
        if !matches!(self.plan.score_plan.expr, Expr::Number(_)) {
            return false;
        }
        let flat = |fr: &FieldRef| {
            matches!(
                fr,
                FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
            )
        };
        match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(_) => {}
            Expr::Field(fr) if flat(fr) => {}
            _ => return false,
        }
        self.plan
            .yield_plan
            .fields
            .iter()
            .all(|field| match &field.value {
                Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => true,
                Expr::Field(fr) => flat(fr),
                _ => false,
            })
    }

    /// Columnar form of [`Self::execute_each_direct_batch`]: reads field
    /// values straight from the Arrow columns via [`ColumnarEvent`], skipping
    /// per-row `Event` materialization entirely (design doc §3.5「on each
    /// 完全不物化」).
    ///
    /// Caller must gate on [`Self::each_plan_columnar_safe`]; the per-row
    /// output (wfx_id / entity_id / fired_at / yield cells) is byte-identical
    /// to the Event-based path — locked by the deferred-vs-columnar 对拍 test.
    pub fn execute_each_direct_batch_columnar(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        sorted_fields: &[(String, usize)],
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
        appended_out: &mut Vec<usize>,
    ) -> EachDirectBatchStats {
        appended_out.clear();
        let mut stats = EachDirectBatchStats::default();
        let Some(each_plan) = &self.plan.each_plan else {
            log::warn!(
                "execute_each_direct_batch_columnar called for non-`on each` rule {}; skipping {} rows",
                self.plan.name,
                rows.len()
            );
            stats.failed = rows.len();
            return stats;
        };
        debug_assert!(self.each_plan_columnar_safe());
        let _ = each_plan; // filter is None by the safety gate
        let statics = self.output_static();
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let summary = Arc::clone(
            statics
                .each_summary
                .as_ref()
                .expect("on-each rule missing precomputed summary"),
        );
        let origin = AlertOrigin::Event;

        // Plan-constant specialization — the safety gate guarantees these
        // shapes (score const; entity StringLit or flat Field; yields Lit/Field).
        let score_const = match &self.plan.score_plan.expr {
            Expr::Number(n) => n.clamp(0.0, 100.0),
            _ => unreachable!("columnar gate requires a constant score"),
        };
        let entity_const: Option<String> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(s) => Some(s.clone()),
            _ => None,
        };
        let entity_field: Option<&FieldRef> = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => Some(fr),
            _ => None,
        };
        let yield_kinds: Vec<YieldKind> = self
            .plan
            .yield_plan
            .fields
            .iter()
            .map(|field| match &field.value {
                Expr::Number(n) => YieldKind::Lit(Value::Number(*n)),
                Expr::StringLit(s) => YieldKind::Lit(Value::Str(s.clone().into())),
                Expr::Bool(b) => YieldKind::Lit(Value::Bool(*b)),
                Expr::Field(_) => YieldKind::Field,
                _ => unreachable!("columnar gate excludes general yield exprs"),
            })
            .collect();
        let yield_field_refs: Vec<Option<&FieldRef>> = self
            .plan
            .yield_plan
            .fields
            .iter()
            .map(|field| match &field.value {
                Expr::Field(fr) => Some(fr),
                _ => None,
            })
            .collect();

        builder.reserve_rows(rows.len());
        let mut wfx_scratch = String::new();

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            // -- Per-row system values (identical to the Event-based path) ---
            let score = score_const;
            let entity_id = match &entity_const {
                Some(s) => s.clone(),
                None => match entity_field
                    .and_then(|fr| event.field_value(field_ref_name(fr)))
                    .as_ref()
                    .map(value_to_string)
                {
                    Some(s) => s,
                    None => {
                        // eval_entity_id on None is an error → failed row.
                        log::warn!("alert export error: entity_id expression evaluated to None");
                        stats.failed += 1;
                        continue;
                    }
                },
            };
            let fired_at = format_nanos_utc(*event_time_nanos);
            let wfx_id = build_each_wfx_id_columnar_reusing(
                &self.plan.name,
                *event_time_nanos,
                event,
                sorted_fields,
                &origin,
                &mut wfx_scratch,
            );
            // (yield_meta is only consumed by General yield exprs — excluded
            // by the columnar gate, so it is not built here.)

            // -- Yield staging (fallible work before any column push) ------
            builder.begin_row();
            let staged: CoreResult<()> = with_yield_eval_scope(|| {
                for (((_field, (name, field_type)), kind), field_ref) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                    .zip(yield_field_refs.iter())
                {
                    let value = match kind {
                        YieldKind::Lit(v) => v.clone(),
                        YieldKind::Field => {
                            // Missing field falls back to an empty string,
                            // exactly like the interpreter path's wrapper.
                            event
                                .field_value(field_ref_name(
                                    field_ref.expect("Field kind implies a field ref"),
                                ))
                                .unwrap_or_else(|| Value::Str(SmolStr::default()))
                        }
                        YieldKind::General => {
                            unreachable!("columnar gate excludes general yield exprs")
                        }
                    };
                    let Some(value) = RuleExecutor::coerce_yield_field_value_with(
                        name,
                        field_type.as_ref(),
                        value,
                    )?
                    else {
                        // Optional input field was missing → omit it from
                        // the output row (wp-labs/warp-fusion#62).
                        continue;
                    };
                    builder.stage_yield_cell(name, field_type.as_ref(), &value)?;
                }
                Ok(())
            });
            if let Err(e) = staged {
                log::warn!("alert export error: {e}");
                stats.failed += 1;
                continue;
            }
            builder.commit_each_row(EachRowCells {
                wfx_id,
                score,
                entity_id,
                fired_at,
                rule_name: &statics.rule_name,
                entity_type: &statics.entity_type,
                origin: &statics.each_origin,
                close_reason: &statics.each_close_reason,
                emit_time: &emit_time,
                summary: &summary,
            });
            stats.appended += 1;
            appended_out.push(idx);
        }
        stats
    }

    fn build_each_direct(
        &self,
        ctx: &Event,
        event_time_nanos: i64,
        field_order: &[&SmolStr],
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
    ) -> CoreResult<()> {
        let statics = self.output_static();
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let origin = AlertOrigin::Event;
        let fired_at = format_nanos_utc(event_time_nanos);
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let wfx_id =
            build_each_wfx_id(&self.plan.name, event_time_nanos, ctx, &origin, field_order);
        let summary = Arc::clone(
            statics
                .each_summary
                .as_ref()
                .expect("on-each rule missing precomputed summary"),
        );
        let yield_meta = self.each_yield_meta(
            &wfx_id,
            &fired_at,
            &emit_time,
            &summary,
            score,
            &entity_id,
            &origin,
            event_time_nanos,
            emit_time_nanos,
        );
        // All fallible work (eval + coerce + typed conversion + name
        // validation) happens while staging; commit is pure column pushes.
        builder.begin_row();
        with_yield_eval_scope(|| {
            for (field, (name, field_type)) in self
                .plan
                .yield_plan
                .fields
                .iter()
                .zip(statics.yield_specs.iter())
            {
                let Some(value) = eval_yield_expr_with_meta(&field.value, ctx, yield_meta) else {
                    return Err(
                        orion_error::StructError::from(CoreReason::RuleExec).with_detail(format!(
                            "on each yield field {:?} expression evaluated to None",
                            field.name
                        )),
                    );
                };
                let Some(value) =
                    RuleExecutor::coerce_yield_field_value_with(name, field_type.as_ref(), value)?
                else {
                    // Optional input field was missing → omit it from the
                    // output row (wp-labs/warp-fusion#62).
                    continue;
                };
                builder.stage_yield_cell(name, field_type.as_ref(), &value)?;
            }
            Ok(())
        })?;
        builder.commit_each_row(EachRowCells {
            wfx_id,
            score,
            entity_id,
            fired_at,
            rule_name: &statics.rule_name,
            entity_type: &statics.entity_type,
            origin: &statics.each_origin,
            close_reason: &statics.each_close_reason,
            emit_time: &emit_time,
            summary: &summary,
        });
        Ok(())
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
        let summary = Arc::clone(
            statics
                .each_summary
                .as_ref()
                .expect("on-each rule missing precomputed summary"),
        );
        let yield_meta = self.each_yield_meta(
            &wfx_id,
            &fired_at,
            &emit_time,
            &summary,
            score,
            &entity_id,
            &origin,
            event_time_nanos,
            emit_time_nanos,
        );
        let yield_fields = with_yield_eval_scope(|| {
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

    /// The `YieldMeta` for an `on each` output — shared by the record path
    /// ([`Self::build_each_alert`]) and the direct-write path
    /// ([`Self::execute_each_direct`]) so both evaluate yield expressions
    /// against identical meta values.
    #[allow(clippy::too_many_arguments)]
    fn each_yield_meta<'a>(
        &'a self,
        wfx_id: &'a str,
        fired_at: &'a str,
        emit_time: &'a Arc<str>,
        summary: &'a Arc<str>,
        score: f64,
        entity_id: &'a str,
        origin: &'a AlertOrigin,
        event_time_nanos: i64,
        emit_time_nanos: i64,
    ) -> YieldMeta<'a> {
        YieldMeta {
            score: Some(score),
            wfx_id: Some(wfx_id),
            rule_name: Some(&self.plan.name),
            entity_type: Some(&self.plan.entity_plan.entity_type),
            entity_id: Some(entity_id),
            origin: Some(origin.as_str()),
            close_reason: Some(""),
            fired_at: Some(fired_at),
            emit_time: Some(&**emit_time),
            summary: Some(&**summary),
            event_first_time_nanos: Some(event_time_nanos),
            event_last_time_nanos: Some(event_time_nanos),
            window_start_time_nanos: Some(event_time_nanos),
            window_end_time_nanos: Some(event_time_nanos),
            emit_time_nanos: Some(emit_time_nanos),
            time_format: Some(self.output_config().time_format.as_str()),
        }
    }

    /// Machine id of an event, as carried by `OutputRecord::machine_id` on
    /// the on-each path. Exposed for the runtime's sampled per-alert
    /// telemetry on the direct-write path (which no longer materializes the
    /// record); extracting only on the 1-in-N sample avoids the per-event
    /// `String` clone.
    pub fn machine_id_of(event: &Event) -> String {
        CepStateMachine::extract_event_str(event, MACHINE_ID)
    }
}

fn passes_each_filter(filter: Option<&wf_lang::ast::Expr>, event: &Event) -> bool {
    match filter.and_then(|expr| eval_bool_expr(expr, event)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}

/// Outcome of [`RuleExecutor::execute_each_direct_batch`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EachDirectBatchStats {
    /// Rows appended to the builder.
    pub appended: usize,
    /// Rows skipped by the `where` filter or a join rejection.
    pub rejected: usize,
    /// Rows skipped by an evaluation/conversion error (logged; no partial
    /// row was committed).
    pub failed: usize,
}

/// Per-yield-field evaluation strategy for the batched on-each direct path.
enum YieldKind {
    /// Literal expression — value built once per batch, cloned per row.
    Lit(Value),
    /// `Expr::Field` — direct field lookup, skipping the interpreter.
    Field,
    /// Anything else — full interpreter evaluation with the per-row meta.
    General,
}
