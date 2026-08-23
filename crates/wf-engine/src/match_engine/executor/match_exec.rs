use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::{Expr, FieldRef};

use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::CoreResult;
use crate::match_engine::match_engine::{
    Event, MatchedContext, Value, WindowLookup, eval_field_value, field_ref_name, value_to_string,
};

use super::RuleExecutor;
use super::YieldKind;
use super::alert::{build_summary, build_wfx_id, format_nanos_utc, now_nanos};
use super::context::{build_eval_context, execute_joins};
use super::eval::{
    YieldMeta, eval_entity_id, eval_score, eval_yield_expr_with_meta, with_yield_eval_scope,
};

/// ctx 来源抽象：完整事件 ctx（HashMap 构建）或 ctx-free（字段直读
/// scope_key + trigger_event，2026-08-23 F8.5——q6 等输出只读键 + 触发
/// 事件字段的规则免每事件 HashMap 构建）。gate 由编译期
/// `compute_match_ctx_free` 保证 Free 模式只用 Lit/Field 表达式。
enum ResolveCtx<'a> {
    Full(&'a Event),
    Free {
        keys: &'a [FieldRef],
        scope_key: &'a [Value],
        trigger_event: Option<&'a Event>,
    },
}

impl ResolveCtx<'_> {
    fn resolve_field(&self, fr: &FieldRef) -> Option<Value> {
        match self {
            ResolveCtx::Full(ctx) => eval_field_value(&ctx.fields, fr),
            ResolveCtx::Free {
                keys,
                scope_key,
                trigger_event,
            } => {
                let name = field_ref_name(fr);
                // 键名优先（同 ctx 注入语义：keys 覆盖 trigger_event 字段）。
                keys.iter()
                    .zip(scope_key.iter())
                    .find(|(k, _)| field_ref_name(k) == name)
                    .map(|(_, v)| v.clone())
                    .or_else(|| trigger_event.and_then(|ev| ev.fields.get(name).cloned()))
            }
        }
    }
}

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
        // ctx-free 快路径（F8.5）：score 常量 + entity/yield 全 Field/Lit +
        // 无 where + live_joins 空时免 build_eval_context（q6 等每事件 emit）。
        if self.output_static().match_ctx_free {
            return self.build_match_alert_free(matched, emit_time_nanos);
        }
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
        // ctx-free 快路径（F8.5）：gate 保证 live_joins 空 + 无 where——
        // execute_joins/where_ok 空转可整体跳过，字段直读。
        if self.output_static().match_ctx_free {
            return self
                .build_match_alert_free(matched, emit_time_nanos)
                .map(Some);
        }
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
        self.build_match_alert_inner(matched, &ResolveCtx::Full(ctx), emit_time_nanos)
    }

    /// ctx-free 变体（F8.5）：字段直读 scope_key + trigger_event，免
    /// `build_eval_context` 的 HashMap 构建。gate 由
    /// `compute_match_ctx_free` 保证：score 常量、entity/yield 全 Field/Lit、
    /// 无 where、live_joins 空、字段不依赖 step label/tracked 集合。
    pub(crate) fn build_match_alert_free(
        &self,
        matched: &MatchedContext,
        emit_time_nanos: i64,
    ) -> CoreResult<OutputRecord> {
        debug_assert!(
            self.output_static().match_ctx_free,
            "ctx-free 快路径必须有编译期 gate"
        );
        let resolve = ResolveCtx::Free {
            keys: &self.plan.match_plan.keys,
            scope_key: &matched.scope_key,
            trigger_event: matched.trigger_event.as_deref(),
        };
        self.build_match_alert_inner(matched, &resolve, emit_time_nanos)
    }

    fn build_match_alert_inner(
        &self,
        matched: &MatchedContext,
        resolve: &ResolveCtx<'_>,
        emit_time_nanos: i64,
    ) -> CoreResult<OutputRecord> {
        let free = matches!(resolve, ResolveCtx::Free { .. });
        let score = match self.output_static().score_const {
            Some(s) => s,
            None => {
                // ctx-free gate 要求 score 常量；非常量分支仅 Full 模式可达。
                assert!(!free, "ctx-free 路径不允许非常量 score");
                let ResolveCtx::Full(ctx) = resolve else {
                    unreachable!()
                };
                eval_score(&self.plan.score_plan.expr, ctx)?
            }
        };
        // Field-typed entity (e.g. `digit(b.auction)`) takes the direct flat
        // lookup — skipping the interpreter's per-record eval-time scope. A
        // missing field degrades to an empty string, byte-identical to the
        // interpreter wrapper (`eval_yield_expr_with_meta` substitutes `""`).
        let entity_id = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => resolve
                .resolve_field(fr)
                .map(|v| value_to_string(&v))
                .unwrap_or_default(),
            _ => {
                // ctx-free gate 要求 entity 为 Field；复杂表达式仅 Full 模式。
                assert!(!free, "ctx-free 路径不允许非 Field entity");
                let ResolveCtx::Full(ctx) = resolve else {
                    unreachable!()
                };
                eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?
            }
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
        let yield_fields = with_yield_eval_scope(|| -> CoreResult<Vec<(Arc<str>, Value)>> {
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
            // 预分配：yield 字段数静态已知——`Vec::from_iter` 对 filter_map
            // 迭代器无法预知长度，每事件渐进扩容（nexmark_hotpath 采样热点
            // spec_from_iter；q6 26M 每事件 emit）。结果顺序与语义不变。
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
                        // Missing field falls back to an empty string,
                        // exactly like the interpreter wrapper.
                        resolve
                            .resolve_field(fr)
                            .unwrap_or_else(|| Value::Str(SmolStr::default()))
                    }
                    YieldKind::General => {
                        // ctx-free gate 排除 General 表达式（需要完整 ctx）。
                        assert!(!free, "ctx-free 路径不允许 General yield 表达式");
                        let ResolveCtx::Full(ctx) = resolve else {
                            unreachable!()
                        };
                        eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                            .expect("eval_yield_expr_with_meta never returns None")
                    }
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
            scope_key,
        })
    }
}
