use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::{CloseMode, Expr, FieldRef};

use crate::alert::{AlertColumnBuilder, AlertOrigin, OutputRecord};
use crate::error::CoreResult;
use crate::match_engine::match_engine::{
    CloseOutput, Event, StepData, Value, WindowLookup, eval_field_value, field_ref_name,
    value_to_string,
};

use super::EachDirectBatchStats;
use super::RuleExecutor;
use super::YieldKind;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc, now_nanos};
use super::context::{build_eval_context, execute_joins};
use super::eval::{
    YieldMeta, eval_entity_id, eval_score, eval_yield_expr_with_meta, with_yield_eval_scope,
};

// ---------------------------------------------------------------------------
// Close-path stage profiler (E2): split execute_close_with_joins into its
// phases so the q12-style close fan-out cost can be read from stderr. Enabled
// with `E2_TIMER=1`; buckets cover combine / ctx-build / alert-build.
// ---------------------------------------------------------------------------
#[derive(Default)]
struct E2Profiler {
    on: bool,
    buckets: [u64; 3],
    calls: u64,
}

static E2_STATE: std::sync::OnceLock<std::sync::Mutex<E2Profiler>> = std::sync::OnceLock::new();

impl E2Profiler {
    fn maybe() -> Self {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let on = *ENABLED.get_or_init(|| {
            std::env::var("E2_TIMER").is_ok() && std::env::var("E2_TIMER").as_deref() != Ok("0")
        });
        E2Profiler {
            on,
            buckets: [0; 3],
            calls: 0,
        }
    }
    #[inline(always)]
    fn add(&mut self, bucket: usize, start: std::time::Instant) {
        if self.on {
            self.buckets[bucket] += start.elapsed().as_nanos() as u64;
        }
    }
    /// Fold this call's buckets into the shared accumulator; report + reset
    /// every 65536 calls so stderr stays bounded while the run covers millions
    /// of closes.
    fn fold(&mut self) {
        if !self.on {
            return;
        }
        let mut shared = E2_STATE
            .get_or_init(|| std::sync::Mutex::new(E2Profiler::default()))
            .lock()
            .unwrap();
        for (i, b) in self.buckets.iter().enumerate() {
            shared.buckets[i] += b;
        }
        shared.calls += 1;
        if shared.calls >= 65536 {
            let snap = std::mem::take(&mut *shared);
            snap.report();
        }
    }
    fn report(&self) {
        if self.calls == 0 {
            return;
        }
        let total: u64 = self.buckets.iter().sum();
        let n = self.calls as f64;
        eprintln!(
            "[E2-profiler] calls={} total={:.1}ns/call",
            self.calls,
            total as f64 / n
        );
        let names = ["\u{7c} combine ", "\u{7c} ctx     ", "\u{7c} build   "];
        for (name, ns) in names.iter().zip(self.buckets.iter()) {
            eprintln!(
                "  {} {:>7.1} ns/call ({:>4.1}%)\n",
                name,
                *ns as f64 / n,
                if total > 0 {
                    *ns as f64 / total as f64 * 100.0
                } else {
                    0.0
                }
            );
        }
    }
}

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
            None,
            &self.close_ctx_fields,
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
        let mut prof = E2Profiler::maybe();
        let _t_combine = prof.on.then(std::time::Instant::now);
        let all_step_data = combine_step_data(close);
        let step_plans = combine_step_plans(self, close);
        if let Some(t) = _t_combine {
            prof.add(0, t);
        }
        let _t_ctx = prof.on.then(std::time::Instant::now);
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &close.scope_key,
            &all_step_data,
            &close.bind_data,
            &step_plans,
            None,
            &self.close_ctx_fields,
        );
        ctx = annotate_close_step_stages(ctx, close.event_step_data.len());
        // join 返回值：缺省 inner/interval inner miss 与 anti 命中 → 抑制 close 输出
        //（与 match/on-each 路径一致，设计 D4「miss → 丢」）。
        if !execute_joins(&self.live_joins, &mut ctx, windows, close.last_event_nanos) {
            return Ok(None);
        }
        // Post-join `where`: strict — false/None suppresses the output.
        if !self.where_ok(&ctx) {
            return Ok(None);
        }
        if let Some(t) = _t_ctx {
            prof.add(1, t);
        }
        let _t_build = prof.on.then(std::time::Instant::now);
        let result = self.build_close_alert(close, &all_step_data, &ctx);
        if let Some(t) = _t_build {
            prof.add(2, t);
        }
        prof.fold();
        result
    }

    /// Internal: build the OutputRecord from an already-constructed eval context.
    fn build_close_alert(
        &self,
        close: &CloseOutput,
        all_step_data: &[StepData],
        ctx: &Event,
    ) -> CoreResult<Option<OutputRecord>> {
        let score = match self.output_static().score_const {
            Some(s) => s,
            None => eval_score(&self.plan.score_plan.expr, ctx)?,
        };
        // Field-typed entity takes the direct flat lookup (see the match path);
        // a missing field degrades to an empty string, byte-identical to the
        // interpreter wrapper.
        let entity_id = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => eval_field_value(&ctx.fields, fr)
                .map(|v| value_to_string(&v))
                .unwrap_or_default(),
            _ => eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?,
        };
        let origin = AlertOrigin::Close {
            reason: close.close_reason,
        };
        let fired_at = format_nanos_utc(close.watermark_nanos);
        let emit_time_nanos = now_nanos();
        let emit_time = Arc::from(format_nanos_utc(emit_time_nanos));
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
        let yield_fields = with_yield_eval_scope(|| -> CoreResult<Vec<(Arc<str>, Value)>> {
            let yield_meta = YieldMeta {
                score: Some(score),
                wfx_id: Some(&wfx_id),
                rule_name: Some(&self.plan.name),
                entity_type: Some(&self.plan.entity_plan.entity_type),
                entity_id: Some(&entity_id),
                origin: Some(origin.as_str()),
                close_reason: Some(close.close_reason.as_str()),
                fired_at: Some(&fired_at),
                emit_time: Some(&emit_time),
                summary: Some(&summary),
                event_first_time_nanos: Some(close.event_first_time_nanos),
                event_last_time_nanos: Some(close.event_last_time_nanos),
                window_start_time_nanos: Some(close.window_start_time_nanos),
                window_end_time_nanos: Some(close.window_end_time_nanos),
                emit_time_nanos: Some(emit_time_nanos),
                time_format: Some(self.output_config().time_format.as_str()),
            };
            // 预分配：yield 字段数静态已知——`Vec::from_iter` 对 filter_map
            // 迭代器无法预知长度，每次渐进扩容（match/close 输出热路径，
            // nexmark_hotpath 采样热点 spec_from_iter）。结果顺序与语义不变。
            let mut out = Vec::with_capacity(self.plan.yield_plan.fields.len());
            for ((field, (name, field_type)), kind) in self
                .plan
                .yield_plan
                .fields
                .iter()
                .zip(self.output_static().yield_specs.iter())
                .zip(self.output_static().yield_kinds.iter())
            {
                let value = match kind {
                    YieldKind::Lit(v) => v.clone(),
                    YieldKind::Field => {
                        let Expr::Field(fr) = &field.value else {
                            unreachable!("YieldKind::Field implies an Expr::Field value")
                        };
                        eval_field_value(&ctx.fields, fr)
                            .unwrap_or_else(|| Value::Str(SmolStr::default()))
                    }
                    YieldKind::General => eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                        .expect("eval_yield_expr_with_meta never returns None"),
                };
                let Some(value) =
                    RuleExecutor::coerce_yield_field_value_with(name, field_type.as_ref(), value)?
                else {
                    // Optional input field was missing → omit it from the
                    // output record (wp-labs/warp-fusion#62).
                    continue;
                };
                out.push((Arc::clone(name), value));
            }
            Ok(out)
        })?;

        let machine_id = self.build_machine_id(&close.machine_id);
        let scope_key = self.build_scope_key(&self.plan.match_plan.keys, &close.scope_key);
        let statics = self.output_static();

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
            summary: Arc::from(summary),
            yield_target: Arc::clone(&statics.yield_target),
            yield_fields,
            yield_field_types: Arc::clone(&statics.yield_field_types),
            event_time_nanos: close.last_event_nanos,
            machine_id,
            scope_key,
        }))
    }

    /// Columnar-safety gate for the batched close emit path. Mirrors the
    /// on-each gate (`each_plan_columnar_safe`): constant score, entity
    /// StringLit / plain Field, yields Lit / plain Field. Joins are unsupported
    /// on this path yet — rules with joins fall back to the per-record
    /// join-enriched path (q4/q6 style). Field references to the synthetic
    /// `_step_*` / `_bind_*` ctx fields are rejected: the columnar resolver
    /// only reads keys / step labels / `field_values` / `bind_data`.
    pub fn close_plan_columnar_safe(&self) -> bool {
        if !matches!(self.plan.score_plan.expr, Expr::Number(_)) {
            return false;
        }
        match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(_) => {}
            Expr::Field(fr)
                if !matches!(fr, FieldRef::Path { .. }) && !field_ref_name(fr).starts_with('_') => {
            }
            _ => return false,
        }
        if !self.plan.yield_plan.fields.iter().all(|f| match &f.value {
            Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => true,
            Expr::Field(fr) => {
                !matches!(fr, FieldRef::Path { .. }) && !field_ref_name(fr).starts_with('_')
            }
            _ => false,
        }) {
            return false;
        }
        if !self.live_joins.is_empty() {
            return false;
        }
        true
    }

    /// Batched columnar close emit (L4): appends a whole batch of
    /// [`CloseOutput`]s straight into the columnar builder — no per-close
    /// `OutputRecord` / synthetic `Event` ctx (the q12 hot spot: ctx build
    /// + alert build measured ~95% of `execute_close_with_joins`).
    ///
    /// Output is byte-identical to `execute_close_with_joins` + `OutputRecord`
    /// for gate-passing shapes (locked by the
    /// `columnar_close_matches_per_record_close` test), with one documented
    /// difference: `emit_time` is **batch-level** (shared by the whole
    /// segment), matching the on-each columnar path; verify compares EMIT
    /// counts, not payload bytes, and `emit_time` never feeds semantics.
    ///
    /// Field resolution replicates `build_eval_context`'s precedence: match
    /// keys → step labels / `field_values.last()` (event steps then close
    /// steps) → `bind_data.field_values.last()`.
    pub fn execute_close_direct_batch_columnar(
        &self,
        closes: &[CloseOutput],
        builder: &mut AlertColumnBuilder,
        emit_time_nanos: i64,
    ) -> EachDirectBatchStats {
        let mut stats = EachDirectBatchStats::default();
        debug_assert!(self.close_plan_columnar_safe());
        let statics = self.output_static();
        let keys = &self.plan.match_plan.keys;
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let score_const = match &self.plan.score_plan.expr {
            Expr::Number(n) => *n,
            _ => unreachable!("columnar close gate requires a constant score"),
        };
        let entity_const: Option<&str> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(s) => Some(s.as_str()),
            _ => None,
        };
        let entity_field_name: Option<&str> = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => Some(field_ref_name(fr)),
            _ => None,
        };
        let yield_specs = &statics.yield_specs;

        // Batch-constant literal yields: coerced + exported once here and
        // registered as constant columns (per-row staging skipped, gap-filled
        // by the commit). Field yields register as ordinary columns.
        for (field, (name, field_type)) in
            self.plan.yield_plan.fields.iter().zip(yield_specs.iter())
        {
            let literal: Option<Value> = match &field.value {
                Expr::Number(n) => Some(Value::Number(*n)),
                Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
                Expr::Bool(b) => Some(Value::Bool(*b)),
                _ => None,
            };
            let const_value = literal.map(|v| {
                RuleExecutor::coerce_yield_field_value_with(name, field_type.as_ref(), v).and_then(
                    |v| {
                        let v = v.expect("literal yield values are never omitted");
                        crate::alert::export_yield_value(&v, field_type.as_ref())
                    },
                )
            });
            match const_value {
                Some(Ok((meta, model_value))) => {
                    if let Err(e) = builder.register_yield_column(name, Some((meta, model_value))) {
                        log::warn!("alert export error: {e}");
                        stats.failed = closes.len();
                        return stats;
                    }
                }
                Some(Err(e)) => {
                    log::warn!("alert export error: {e}");
                    stats.failed = closes.len();
                    return stats;
                }
                None => {
                    // Field yield: ordinary column, staged per row.
                    if let Err(e) = builder.register_yield_column(name, None) {
                        log::warn!("alert export error: {e}");
                        stats.failed = closes.len();
                        return stats;
                    }
                }
            }
        }

        builder.reserve_rows(closes.len());

        let mut wfx_ids: Vec<String> = Vec::with_capacity(closes.len());
        let mut scores: Vec<f64> = Vec::with_capacity(closes.len());
        let mut entity_ids: Vec<String> = Vec::with_capacity(closes.len());
        let mut fired_ats: Vec<String> = Vec::with_capacity(closes.len());
        let mut origins: Vec<Arc<str>> = Vec::with_capacity(closes.len());
        let mut close_reasons: Vec<Arc<str>> = Vec::with_capacity(closes.len());
        let mut summaries: Vec<Arc<str>> = Vec::with_capacity(closes.len());
        let mut staged_rows: Vec<
            Vec<(
                usize,
                wp_model_core::model::DataType,
                wp_model_core::model::Value,
            )>,
        > = Vec::with_capacity(closes.len());

        'close: for close in closes {
            if !is_qualified(close) {
                stats.rejected += 1;
                continue;
            }
            let origin = AlertOrigin::Close {
                reason: close.close_reason,
            };
            let fired_at = format_nanos_utc(close.watermark_nanos);
            // entity
            let entity_id: String = if let Some(s) = entity_const {
                s.to_string()
            } else {
                // eval_entity_id → eval_yield_expr falls back to an empty
                // string when the field is absent (never errors) — mirror that
                // instead of failing the close.
                resolve_close_field(close, keys, entity_field_name.unwrap_or(""))
                    .map(|v| value_to_string(&v))
                    .unwrap_or_default()
            };
            // wfx_id / summary need the combined step data (same byte stream
            // as build_wfx_id/build_summary on the per-record path).
            let all_step_data = combine_step_data(close);
            let wfx_id = build_wfx_id(
                &self.plan.name,
                &close.scope_key,
                &fired_at,
                &all_step_data,
                &origin,
            );
            let summary = build_summary(
                &self.plan.name,
                keys,
                &close.scope_key,
                &all_step_data,
                &origin,
            );

            // Field yields: resolve each from keys / field_values / bind.
            for (field, (name, field_type)) in
                self.plan.yield_plan.fields.iter().zip(yield_specs.iter())
            {
                if !matches!(field.value, Expr::Field(_)) {
                    continue; // literal — constant column, gap-filled
                }
                let value = resolve_close_field(close, keys, field_ref_name_of(&field.value))
                    .unwrap_or_else(|| Value::Str(String::new().into()));
                match RuleExecutor::coerce_yield_field_value_with(name, field_type.as_ref(), value)
                {
                    Ok(Some(v)) => {
                        if let Err(e) = builder.stage_yield_cell(name, field_type.as_ref(), &v) {
                            log::warn!("alert export error: {e}");
                            stats.failed += 1;
                            builder.take_staged();
                            continue 'close;
                        }
                    }
                    Ok(None) => { /* optional missing field omitted */ }
                    Err(e) => {
                        log::warn!("alert export error: {e}");
                        stats.failed += 1;
                        builder.take_staged();
                        continue 'close;
                    }
                }
            }
            staged_rows.push(builder.take_staged());

            wfx_ids.push(wfx_id);
            scores.push(score_const);
            entity_ids.push(entity_id);
            fired_ats.push(fired_at);
            origins.push(Arc::from(origin.as_str()));
            close_reasons.push(Arc::from(origin.close_reason().map_or("", |r| r.as_str())));
            summaries.push(Arc::from(summary));
            stats.appended += 1;
        }

        if !wfx_ids.is_empty() {
            builder.commit_close_rows_batch(
                &wfx_ids,
                &scores,
                &entity_ids,
                &fired_ats,
                &statics.rule_name,
                &statics.entity_type,
                &origins,
                &close_reasons,
                &emit_time,
                &summaries,
                &staged_rows,
            );
        }
        stats
    }
}

/// Resolve a bare field name against a close output with `build_eval_context`
/// precedence: match keys (scope_key) → step labels / `field_values.last()`
/// (event steps then close steps) → `bind_data.field_values.last()`. Returns
/// `None` when absent everywhere — the per-record path then reads `None` from
/// the synthetic ctx (entity errors, yield falls back to empty string).
fn resolve_close_field(close: &CloseOutput, keys: &[FieldRef], name: &str) -> Option<Value> {
    // Keys first (build_eval_context inserts keys before anything else; a key
    // with no scope value is absent from the ctx, so fall through to the
    // field_values/bind lookups below).
    for (i, k) in keys.iter().enumerate() {
        if field_ref_name(k) == name
            && let Some(v) = close.scope_key.get(i)
        {
            return Some(v.clone());
        }
    }
    for sd in close
        .event_step_data
        .iter()
        .chain(close.close_step_data.iter())
    {
        if let Some(label) = &sd.label
            && label.as_str() == name
        {
            return Some(Value::Number(sd.measure_value));
        }
        if let Some(v) = sd.field_values.get(name).and_then(|vs| vs.last()) {
            return Some(v.clone());
        }
    }
    for bd in &close.bind_data {
        if let Some(v) = bd.field_values.get(name).and_then(|vs| vs.last()) {
            return Some(v.clone());
        }
    }
    None
}

fn field_ref_name_of(expr: &Expr) -> &str {
    match expr {
        Expr::Field(fr) => field_ref_name(fr),
        _ => "",
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
            format!("_step_{}_stage", step_idx).into(),
            crate::match_engine::match_engine::Value::Str(stage.into()),
        );
    }
    ctx
}
