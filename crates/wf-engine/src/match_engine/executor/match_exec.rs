use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::{Expr, FieldRef};

use crate::alert::{AlertColumnBuilder, AlertOrigin, EachRowCells, OutputRecord};
use crate::error::CoreResult;
use crate::match_engine::columnar::cscalar_to_value;
use crate::match_engine::event_bridge::TriggerEvent;
use crate::match_engine::match_engine::{
    Event, FieldSource, MatchedContext, Value, WindowLookup, eval_field_value, field_ref_name,
    value_to_string,
};

use super::close_exec::CloseBatchVecs;
use super::each_exec::EachDirectBatchStats;

use super::RuleExecutor;
use super::YieldKind;
use super::alert::{EntityIdCache, build_summary, build_wfx_id, format_nanos_utc, now_nanos};
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
        trigger_event: Option<&'a TriggerEvent>,
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
                    .or_else(|| trigger_event.and_then(|ev| ev.field_value(name)))
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
        let mut ctx = build_eval_context(
            &self.plan.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_ref(),
            &self.close_ctx_fields,
            None,
        );
        // let 派生字段（2026-08-31，issue #79）：对齐 on-each 语义——求值后注入
        // ctx 字段图，后续 yield/score/entity 按裸名引用；在 join 之前求值
        // （let 表达式不引用 join 富化字段，与 execute_each_with_joins 一致）。
        if !self.plan.lets.is_empty() {
            self.apply_lets(&mut ctx);
        }
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
            matched.trigger_event.as_ref(),
            &self.close_ctx_fields,
            None,
        );
        // let 派生字段（2026-08-31，issue #79）：在 join 之前求值注入（与
        // execute_each_with_joins 同位置语义——let 不引用 join 富化字段）。
        if !self.plan.lets.is_empty() {
            self.apply_lets(&mut ctx);
        }
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
            trigger_event: matched.trigger_event.as_ref(),
        };
        self.build_match_alert_inner(matched, &resolve, emit_time_nanos)
    }

    /// 列式批输出门控（q6 形态）：score 常量、entity/yield 全 Lit/Field、
    /// 无 where；输出字段来源限定为 scope_key（键）∪ trigger_event（左窗驱动
    /// 字段）——join 已在上游 advance 完成（键预解析），输出不引用非键右窗字段
    /// 则跳过 join 富化仍字节一致（右窗限定且属于 keys 的字段从 scope_key 读）。
    /// 裸名在有活 join 时歧义（可能来自右窗 enrich 注入）→ 排除。
    pub fn match_plan_columnar_safe(&self) -> bool {
        let plan = &self.plan;
        // let 派生字段（2026-08-31，issue #79）：列式视图（resolve_match_field）
        // 无 let 视图，解释路径 apply_lets 的注入语义靠列式内联等价——但
        // match 列式直写 gate 从简：有 let 的 match 规则整体回落行式（正确性
        // 优先；带 let 的规则通常不是每事件 hot path）。列式内联留给后续优化。
        if !plan.lets.is_empty() {
            return false;
        }
        if plan.r#where.is_some() || !matches!(plan.score_plan.expr, Expr::Number(_)) {
            return false;
        }
        let out_shape_ok = |e: &Expr| {
            matches!(
                e,
                Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) | Expr::Field(_)
            )
        };
        if !out_shape_ok(&plan.entity_plan.entity_id_expr)
            || !plan.yield_plan.fields.iter().all(|f| {
                out_shape_ok(&f.value)
                    || (wf_lang::columnar::columnar_output_expr(&f.value)
                        && super::yield_general_columnar_safe(&f.value))
            })
        {
            return false;
        }
        // 有活 join 时 General 禁止（层 2 收口）：列式物化视图（resolve_match_field）
        // 只有 keys/trigger/steps/bind，无 join 富化字段——General 引用右窗字段
        // 会静默读空 → 与解释路径（Full ctx 含 enrich）分叉。
        if !self.live_joins.is_empty()
            && plan
                .yield_plan
                .fields
                .iter()
                .any(|f| !out_shape_ok(&f.value))
        {
            return false;
        }
        let left_aliases: std::collections::HashSet<&str> =
            plan.binds.iter().map(|b| b.alias.as_str()).collect();
        let key_windows: std::collections::HashSet<&str> = plan
            .match_plan
            .keys
            .iter()
            .filter_map(|k| match k {
                FieldRef::Qualified(win, _) => Some(win.as_str()),
                _ => None,
            })
            .collect();
        let mut out_fields: Vec<&FieldRef> = Vec::new();
        if let Expr::Field(fr) = &plan.entity_plan.entity_id_expr {
            out_fields.push(fr);
        }
        out_fields.extend(
            plan.yield_plan
                .fields
                .iter()
                .filter_map(|f| match &f.value {
                    Expr::Field(fr) => Some(fr),
                    _ => None,
                }),
        );
        out_fields.into_iter().all(|fr| match fr {
            FieldRef::Qualified(win, _) => {
                left_aliases.contains(win.as_str()) || key_windows.contains(win.as_str())
            }
            FieldRef::Simple(_) => self.live_joins.is_empty(),
            _ => false,
        })
    }

    /// 列式批级 General yield 槽位（层 2 收口）：`resolve_match_field` 的字段
    /// 解析与 `build_eval_context`（Named 窄化）注入优先级逐位对齐（keys →
    /// trigger_event → step labels/field_values → bind），物化列与解释 ctx
    /// 无分叉。match 传空 lets——有 let 的 match 规则已被
    /// `match_plan_columnar_safe` 排除（回落行式解释），见其注释（2026-08-31）。
    pub(crate) fn match_batch_prepare(&self, matched: &[&MatchedContext]) -> CloseBatchVecs {
        let n = matched.len();
        let slots = self.plan.yield_plan.fields.len();
        let ref_fields = self.yield_ref_fields(false);
        if n == 0 || ref_fields.is_empty() {
            return CloseBatchVecs {
                general_cvecs: (0..slots).map(|_| None).collect(),
            };
        }
        let keys = &self.plan.match_plan.keys;
        CloseBatchVecs {
            general_cvecs: self.compile_general_slots(
                &ref_fields,
                n,
                |row, name| resolve_match_field(matched[row], keys, name),
                &[],
            ),
        }
    }

    /// 批量 match 命中输出直写列式 builder（跳过 `OutputRecord` 中间物化）。
    /// 字段来源 = scope_key（键优先）∪ trigger_event；join 不在此执行（门控保证
    /// 输出不依赖非键右窗字段）。逐 ctx 构造 wfx_id/summary（依赖 step_data，
    /// 每上下文固有），yield 与 entity 走 `ResolveCtx::Free` 直读，省
    /// `append_record` 的 OutputRecord 包装 + 二次转换。
    ///
    /// Caller must gate on [`Self::match_plan_columnar_safe`]。
    pub fn execute_match_direct_batch_columnar(
        &self,
        matched: &[&MatchedContext],
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
        appended_out: &mut Vec<usize>,
    ) -> EachDirectBatchStats {
        let mut stats = EachDirectBatchStats::default();
        debug_assert!(self.match_plan_columnar_safe());
        let statics = self.output_static();
        let keys = &self.plan.match_plan.keys;
        let origin = AlertOrigin::Event;
        let emit_time = self.cached_emit_time(emit_time_nanos);
        // 层 2 收口：列式批级 General yield cell（match_batch_prepare——
        // resolve_match_field 与解释 ctx 注入优先级逐位对齐）。
        let prepared = self.match_batch_prepare(matched);
        let score_const = match &self.plan.score_plan.expr {
            Expr::Number(n) => n.clamp(0.0, 100.0),
            _ => unreachable!("columnar match gate requires a constant score"),
        };
        let entity_const: Option<String> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(s) => Some(s.clone()),
            _ => None,
        };
        let entity_field: Option<&FieldRef> = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => Some(fr),
            _ => None,
        };
        // 批级常量 yield 注册（Lit）；Field 行级 stage（同 each 列式路径）。
        for ((_field, (name, field_type)), kind) in self
            .plan
            .yield_plan
            .fields
            .iter()
            .zip(statics.yield_specs.iter())
            .zip(statics.yield_kinds.iter())
        {
            let const_value = match kind {
                YieldKind::Lit(v) => {
                    let converted = RuleExecutor::coerce_yield_field_value_with(
                        name,
                        field_type.as_ref(),
                        v.clone(),
                    )
                    .and_then(|v| {
                        let v = v.expect("literal yield values are never omitted");
                        crate::alert::export_yield_value(&v, field_type.as_ref())
                    });
                    match converted {
                        Ok((meta, model_value)) => Some((meta, model_value)),
                        Err(e) => {
                            log::warn!("alert export error: {e}");
                            stats.failed = matched.len();
                            return stats;
                        }
                    }
                }
                _ => None,
            };
            if let Err(e) = builder.register_yield_column(name, const_value) {
                log::warn!("alert export error: {e}");
                stats.failed = matched.len();
                return stats;
            }
        }
        builder.reserve_rows(matched.len());
        // entity 连续缓存（通用 EntityIdCache, P6）: 同 scope_key 相邻 matched
        // 复用 entity_id（q6 同 key 多事件触发）, 免每行 resolve + value_to_string。
        let mut entity_cache = EntityIdCache::new();
        for (idx, m) in matched.iter().enumerate() {
            let resolve = ResolveCtx::Free {
                keys,
                scope_key: &m.scope_key,
                trigger_event: m.trigger_event.as_ref(),
            };
            let entity_id = match &entity_const {
                Some(s) => s.clone(),
                None => entity_cache.get_or(&m.scope_key, || {
                    entity_field
                        .and_then(|fr| resolve.resolve_field(fr))
                        .map(|v| value_to_string(&v))
                        .unwrap_or_default()
                }),
            };
            let fired_at = format_nanos_utc(m.event_time_nanos);
            let wfx_id = build_wfx_id(
                &self.plan.name,
                &m.scope_key,
                &fired_at,
                &m.step_data,
                &origin,
            );
            let summary = Arc::from(build_summary(
                &self.plan.name,
                keys,
                &m.scope_key,
                &m.step_data,
                &origin,
            ));
            // yield staging（Lit 由批级 const 列补齐；Field 逐行 resolve+stage；
            // General 列式 cell（槽位 None → 逐行 Full ctx 回退））。
            let mut row_failed = false;
            let mut ctx: Option<Event> = None;
            for (field_idx, ((field, (name, field_type)), kind)) in self
                .plan
                .yield_plan
                .fields
                .iter()
                .zip(statics.yield_specs.iter())
                .zip(statics.yield_kinds.iter())
                .enumerate()
            {
                match kind {
                    YieldKind::Lit(_) => {}
                    YieldKind::Field => {
                        let Expr::Field(fr) = &field.value else {
                            unreachable!("YieldKind::Field implies an Expr::Field value")
                        };
                        let value = resolve
                            .resolve_field(fr)
                            .unwrap_or_else(|| Value::Str(SmolStr::default()));
                        match RuleExecutor::coerce_yield_field_value_with(
                            name,
                            field_type.as_ref(),
                            value,
                        ) {
                            Ok(Some(v)) => {
                                if let Err(e) =
                                    builder.stage_yield_cell(name, field_type.as_ref(), &v)
                                {
                                    log::warn!("alert export error: {e}");
                                    row_failed = true;
                                }
                            }
                            Ok(None) => { /* optional input field missing → omit */ }
                            Err(e) => {
                                log::warn!("alert export error: {e}");
                                row_failed = true;
                            }
                        }
                    }
                    YieldKind::General => {
                        // 层 2 收口（2026-08-25）：列式批级 cell——槽位命中直接
                        // 取（null 行 → 空串，同解释 None→""）；槽位 None（编译
                        // 失败/物化类型不一致）→ 逐行回退：Full ctx（build_eval_
                        // context 含 trigger_event，与解释路径一致）。
                        let value = match prepared
                            .general_cvecs
                            .get(field_idx)
                            .and_then(|c| c.as_ref())
                        {
                            Some(cvec) => match cvec.scalar_at(idx) {
                                Some(s) => cscalar_to_value(&s),
                                None => Value::Str(SmolStr::default()),
                            },
                            None => {
                                // 与解释路径（execute_match_with_joins_at）
                                // 一致：step_plans 参与 ctx 构建（All 分支的
                                // `_step_{i}_source`；General 门控不引用合成
                                // 字段，主要保证注入语义逐位对齐）。
                                let step_plans: Vec<&wf_lang::plan::StepPlan> =
                                    self.plan.match_plan.event_steps.iter().collect();
                                let ctx = ctx.get_or_insert_with(|| {
                                    build_eval_context(
                                        keys,
                                        &m.scope_key,
                                        &m.step_data,
                                        &m.bind_data,
                                        &step_plans,
                                        m.trigger_event.as_ref(),
                                        &self.close_ctx_fields,
                                        None,
                                    )
                                });
                                // 复用循环外已算的 wfx_id / summary（字节一致）。
                                let yield_meta = YieldMeta {
                                    score: Some(score_const),
                                    wfx_id: Some(&wfx_id),
                                    rule_name: Some(&self.plan.name),
                                    entity_type: Some(&self.plan.entity_plan.entity_type),
                                    entity_id: Some(&entity_id),
                                    origin: Some(origin.as_str()),
                                    close_reason: Some(""),
                                    fired_at: Some(&fired_at),
                                    emit_time: Some(&emit_time),
                                    summary: Some(&summary),
                                    event_first_time_nanos: Some(m.event_first_time_nanos),
                                    event_last_time_nanos: Some(m.event_last_time_nanos),
                                    window_start_time_nanos: Some(m.window_start_time_nanos),
                                    window_end_time_nanos: Some(m.window_end_time_nanos),
                                    emit_time_nanos: Some(emit_time_nanos),
                                    time_format: Some(self.output_config().time_format.as_str()),
                                };
                                with_yield_eval_scope(|| {
                                    eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                                })
                                .expect("eval_yield_expr_with_meta never returns None")
                            }
                        };
                        match RuleExecutor::coerce_yield_field_value_with(
                            name,
                            field_type.as_ref(),
                            value,
                        ) {
                            Ok(Some(v)) => {
                                if let Err(e) =
                                    builder.stage_yield_cell(name, field_type.as_ref(), &v)
                                {
                                    log::warn!("alert export error: {e}");
                                    row_failed = true;
                                }
                            }
                            Ok(None) => { /* optional input field missing → omit */ }
                            Err(e) => {
                                log::warn!("alert export error: {e}");
                                row_failed = true;
                            }
                        }
                    }
                }
                if row_failed {
                    break;
                }
            }
            if row_failed {
                // 丢弃已 stage 的残余 yield cell（部分字段成功、后续失败）。
                builder.take_staged();
                stats.failed += 1;
                continue;
            }
            builder.commit_each_row(EachRowCells {
                wfx_id: SmolStr::from(wfx_id),
                score: score_const,
                entity_id: SmolStr::from(entity_id),
                fired_at,
                rule_name: &statics.rule_name,
                entity_type: &statics.entity_type,
                origin: &statics.each_origin,
                close_reason: &statics.each_close_reason,
                emit_time: &emit_time,
                summary: &summary,
            });
            appended_out.push(idx);
            stats.appended += 1;
        }
        stats
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
                let &ResolveCtx::Full(ctx) = resolve else {
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
                let &ResolveCtx::Full(ctx) = resolve else {
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
                        let &ResolveCtx::Full(ctx) = resolve else {
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

/// match 字段解析（层 2 收口）：keys（scope_key）→ trigger_event → step
/// labels/field_values → bind——与 `build_eval_context`（Named 窄化）注入
/// 优先级逐位一致，物化列与解释 ctx 无分叉。
fn resolve_match_field(m: &MatchedContext, keys: &[FieldRef], name: &str) -> Option<Value> {
    for (i, k) in keys.iter().enumerate() {
        if field_ref_name(k) == name
            && let Some(v) = m.scope_key.get(i)
        {
            return Some(v.clone());
        }
    }
    if let Some(ev) = m.trigger_event.as_ref()
        && let Some(v) = ev.field_value(name)
    {
        return Some(v);
    }
    for sd in &m.step_data {
        if let Some(label) = &sd.label
            && label.as_str() == name
        {
            return Some(Value::Number(sd.measure_value));
        }
        if let Some(v) = sd.field_values.get(name).and_then(|vs| vs.last()) {
            return Some(v.clone());
        }
    }
    for bd in &m.bind_data {
        if let Some(v) = bd.field_values.get(name).and_then(|vs| vs.last()) {
            return Some(v.clone());
        }
    }
    None
}
