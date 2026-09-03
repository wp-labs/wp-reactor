//! on-each 行式直发执行器 + 列式安全门（2026-09-04 自 each_exec.rs 拆出）：
//! `execute_each*` 逐事件/逐批直发路径、`apply_lets` 事件 let 注入，以及
//! 列式路径的总门控（`each_plan_columnar_safe` / `each_pipe_columnar_safe`）。
//! 列式批执行器见 `col_exec.rs`，输出组装/join 见 `col_join.rs`。

use std::borrow::Cow;
use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::{Expr, FieldRef, PathSegment};

use crate::alert::{AlertColumnBuilder, AlertOrigin, EachRowCells, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::cep::{Event, Value, WindowLookup, eval_field_value};
use crate::match_engine::columnar::cscalar_to_value;

use super::super::RuleExecutor;
use super::super::YieldKind;
use super::super::alert::{build_each_wfx_id_reusing, format_nanos_utc, now_nanos};
use super::super::close_exec::CloseBatchVecs;
use super::super::context::execute_joins;
use super::super::eval::{
    YieldMeta, eval_entity_id, eval_expr_with_l3, eval_score, eval_yield_expr_with_meta,
    with_yield_eval_scope,
};

use super::*;

impl RuleExecutor {
    /// Evaluate the plan's per-event `let` bindings against `ctx` and inject
    /// the results into the event's field map, so later expressions resolve
    /// them by bare name. Bindings evaluate in order — a later `let` may
    /// reference an earlier one; a binding that fails to evaluate (null)
    /// leaves no injected field (later references then read as absent/null).
    pub(crate) fn apply_lets(&self, ctx: &mut Event) {
        for l in &self.plan.lets {
            if let Some(v) = eval_expr_with_l3(&l.expr, ctx, YieldMeta::default()) {
                ctx.fields.insert(l.name.clone().into(), v);
            }
        }
    }
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
        if self.plan.lets.is_empty() {
            self.build_each_alert(event, event_time_nanos, &[], now_nanos())
        } else {
            let mut ctx = event.clone();
            self.apply_lets(&mut ctx);
            self.build_each_alert(&ctx, event_time_nanos, &[], now_nanos())
        }
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
        // Rules without joins or `let` bindings never mutate the event — skip
        // the per-event `fields` HashMap clone entirely (profile: the clone +
        // its drop were ~3% of on-CPU samples on pass-through rules).
        if self.plan.joins.is_empty() && self.plan.lets.is_empty() {
            return self.build_each_alert(event, event_time_nanos, field_order, emit_time_nanos);
        }
        let mut ctx = event.clone();
        self.apply_lets(&mut ctx);
        if !execute_joins(&self.live_joins, &mut ctx, windows, event_time_nanos) {
            return Ok(None);
        }
        // Post-join `where`: strict — false/None suppresses the output.
        if !self.where_ok(&ctx) {
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
        // Rules without joins or `let` bindings never mutate the event — skip
        // the per-event `fields` HashMap clone (same optimization as the
        // record path).
        if self.plan.joins.is_empty() && self.plan.lets.is_empty() {
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
        self.apply_lets(&mut ctx);
        if !execute_joins(&self.live_joins, &mut ctx, windows, event_time_nanos) {
            return Ok(false);
        }
        // Post-join `where`: strict — false/None suppresses the output.
        if !self.where_ok(&ctx) {
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

        // 层 2 收口（2026-08-25）：行式批路径的 General yield 也走列式批级
        // cell——Event 数组物化（resolve = 事件字段裸名直查，let 内联在编译层；
        // 无活 join 才启用：join 富化字段不在物化视图，引用会静默读空 → 分叉，
        // 有 join 保持逐行解释）。槽位 None → 循环内逐行回退。
        let prepared = if self.live_joins.is_empty() {
            self.event_batch_prepare(rows)
        } else {
            CloseBatchVecs {
                general_cvecs: (0..yield_kinds.len()).map(|_| None).collect(),
            }
        };

        builder.reserve_rows(rows.len());
        let mut wfx_scratch = String::new();

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            if !passes_each_filter(filter, event) {
                stats.rejected += 1;
                continue;
            }
            // Rules without joins or `let` bindings never mutate the event —
            // borrow instead of cloning (same optimization as the per-event
            // path). 2026-09-02 gap-3 修复：**无 where** 也是借用前提——死 join
            // 形状（plan.joins 非空、live_joins 空）下 where 只读驱动列，借用
            // 短路会静默跳过 `where_ok`（与逐事件 execute_each_direct / oracle
            // 的 plan.joins 判定分叉，实测 batched 生产路径 where 失效）。
            let ctx: Cow<'_, Event> = if self.live_joins.is_empty()
                && self.plan.lets.is_empty()
                && self.plan.r#where.is_none()
            {
                Cow::Borrowed::<Event>(*event)
            } else {
                let mut ctx = Cow::<Event>::Owned((**event).clone());
                self.apply_lets(ctx.to_mut());
                if !execute_joins(&self.live_joins, ctx.to_mut(), windows, *event_time_nanos) {
                    stats.rejected += 1;
                    continue;
                }
                // Post-join `where`: strict — false/None suppresses the row.
                if !self.where_ok(ctx.to_mut()) {
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
                for (field_idx, ((field, (name, field_type)), kind)) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                    .enumerate()
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
                        // 层 2 收口：列式批级 cell（槽位命中直接取——null 行 →
                        // 空串，同解释 None→""）；槽位 None → 逐行解释回退。
                        YieldKind::General => {
                            match prepared
                                .general_cvecs
                                .get(field_idx)
                                .and_then(|c| c.as_ref())
                            {
                                Some(cvec) => match cvec.scalar_at(idx) {
                                    Some(s) => cscalar_to_value(&s),
                                    None => Value::Str(SmolStr::default()),
                                },
                                None => eval_yield_expr_with_meta(&field.value, ctx, yield_meta)
                                    .expect("eval_yield_expr_with_meta never returns None"),
                            }
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
                wfx_id: SmolStr::from(wfx_id),
                score,
                entity_id: SmolStr::from(entity_id),
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

    /// Whether the on-each plan can run the columnar fast path: 形状门控合
    /// 集（2026-09-02 gap 3-7 收口后）——where 无或可列式、each filter 无活
    /// join 任意（gap-4 逐行解释回退）、score 常量 / 常量×flat / 无活 join 可列
    /// 式（gap-6 score_cvec + 逐行回退）、entity 字面量 / flat / 无活 join 可列
    /// 式（gap-7 entity_cvec + 逐行回退）、yield ∈ {字面量, flat, list-index
    /// （gap-5）, 列式输出函数/表达式}、bind filter 无或列式（非列式逐行
    /// `event_matches_alias` 回退）、let RHS 可列式且非 yield 表达式不引用 let。
    /// 活 join 须满足列式 join 形状（each_join_plan 非 None）。Anything else
    /// falls back to the Event-based path, keeping both paths byte-identical by
    /// construction（不满足的残项见 §11.2 表 + execution_path 矩阵）。
    pub fn each_plan_columnar_safe(&self) -> bool {
        let Some(each_plan) = &self.plan.each_plan else {
            return false;
        };
        // lets（2026-08-25 层 2，q22 形态）：允许 let 绑定——前提：无活 join
        // （列式 join 富化路径未接 let）；每个 let RHS 可列式编译
        // （expr_is_columnar / columnar_output_expr，split/mvindex/concat 等——
        // yield 的 let 引用经编译期内联展开）；非 yield 表达式
        // （score/entity/filter/where/bind filter）不得引用 let 变量（列式
        // mask/score 无 let 视图，引用会静默读空 → 失真）。yield 的 let 引用
        // 只允许出现在 General 表达式（内联 + 编译失败逐行回退，回退已注入
        // let）；Field yield 引用 let 变量 → 拒绝（列式字段读无 let 视图）。
        let let_names: std::collections::HashSet<&str> =
            self.plan.lets.iter().map(|l| l.name.as_str()).collect();
        if !self.plan.lets.is_empty()
            && (!self.live_joins.is_empty()
                || !self.plan.lets.iter().all(|l| {
                    wf_lang::columnar::expr_is_columnar(&l.expr)
                        || wf_lang::columnar::columnar_output_expr(&l.expr)
                })
                || each_plan
                    .filter
                    .as_ref()
                    .is_some_and(|f| expr_refs_let(f, &let_names))
                || self
                    .plan
                    .r#where
                    .as_ref()
                    .is_some_and(|w| expr_refs_let(w, &let_names))
                || expr_refs_let(&self.plan.score_plan.expr, &let_names)
                || expr_refs_let(&self.plan.entity_plan.entity_id_expr, &let_names)
                || !self.plan.binds.iter().all(|b| {
                    b.filter
                        .as_ref()
                        .is_none_or(|f| !expr_refs_let(f, &let_names))
                }))
        {
            return false;
        }
        // 无活 join：形状检查走无 join 列式路径。后置 `where`（gap-3 列式化
        // 2026-09-02）：无 join 时 where 只读驱动列 → 可列式（expr_is_columnar）
        // 时放行（批级守卫掩码，行式 where_ok 严格语义对拍锁定）；非列式
        // where → 回退行式。单活 join：必须满足列式 join 形状（each_join_plan
        // 非 None）——where/输出字段的限定来源由 `parse_each_join_columnar`
        // 一并校验。多 join / 活 join 不满足形状 → 回退行式。
        let join_ok = if self.live_joins.is_empty() {
            self.plan
                .r#where
                .as_ref()
                .is_none_or(wf_lang::columnar::expr_is_columnar)
        } else {
            self.each_join_plan.is_some()
        };
        // each filter：无活 join 时任意形状放行（gap-4 2026-09-02）——可列式
        // 走批级掩码（Q14 `0.908*price in (1M, 50M)`），非列式走 execute 行
        // 循环逐行 `passes_each_filter` 解释回退（与行式路径字节一致，仅不
        // 物化整批）；有活 join 时列式 join 富化路径未接入 filter 求值 →
        // 保守回退行式。
        let filter_ok = match &each_plan.filter {
            None => true,
            Some(_) if self.live_joins.is_empty() => true,
            Some(_) => false,
        };
        if !join_ok || !filter_ok {
            return false;
        }
        // binds 无需形状检查（gap-4 2026-09-02）：filter 可列式（掩码）或
        // 非列式（process_batch 的 columnar_each 块逐行 `event_matches_alias`
        // 解释兜底）；引用 let 的 filter 已在上面统一拒绝（列式视图无 let 覆盖）。
        // 无 join 时的字段形状（Simple/Qualified/Bracketed flat）；有 join 时
        // 输出字段来源已被 `parse_each_join_columnar` 校验（左窗/右窗限定）。
        let flat = |fr: &FieldRef| {
            matches!(
                fr,
                FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
            )
        };
        let out_shape_ok = |fr: &FieldRef| -> bool {
            if self.live_joins.is_empty() {
                flat(fr)
            } else {
                // 有 join：限定引用且限定符 ∈ {左窗 alias, 右窗名}（Simple 是
                // 歧义裸名——可能来自 enrich 裸名注入，列式无法分辨，保守回退）。
                let Some(join_plan) = &self.each_join_plan else {
                    return false;
                };
                match fr {
                    FieldRef::Qualified(win, _) => {
                        win == &join_plan.left_alias || win == &join_plan.right_window
                    }
                    _ => false,
                }
            }
        };
        // Score 形状：常量（原有）或「常量×flat 字段」（q1 `0.908*b.price`）。
        // 无活 join 时**任意可列式表达式**放行（gap-6 2026-09-02：字段×字段
        // 等编译批级 cvec，编译失败逐行 eval_score 回退）；有活 join 时仍只允
        // 许常量——join 列式富化路径的 score 未接右窗字段读取，BinOp 一律回退
        // 行式，避免 columnar_join 的 unreachable。
        let score_ok = match score_shape(&self.plan.score_plan.expr) {
            Some(ScoreShape::Const(_)) => true,
            Some(ScoreShape::MulConst { field, .. }) => {
                if self.live_joins.is_empty() {
                    // 常量×flat（快通道）或常量×list-index（`0.5*c.tags[0]`，
                    // gap-6 2026-09-02 review 发现：归一般 cvec——ListIndex × 常量）
                    out_shape_ok(field) || wf_lang::columnar::field_ref_is_list_index(field)
                } else {
                    false
                }
            }
            None => {
                self.live_joins.is_empty()
                    && wf_lang::columnar::expr_is_columnar(&self.plan.score_plan.expr)
            }
        };
        if !score_ok {
            return false;
        }
        match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(_) => {}
            Expr::Field(fr) if out_shape_ok(fr) => {}
            // gap-7（2026-09-02）：无活 join 的**可列式** entity 表达式（如
            // list-index 字段、flat 组件构成的 if/cmp 表达式）→ 编译 cvec，编译
            // 失败逐行 eval_entity_id 回退；非列式（对象/数组嵌套 Path 等）仍行式。
            expr if self.live_joins.is_empty() && wf_lang::columnar::expr_is_columnar(expr) => {}
            _ => return false,
        }
        self.plan
            .yield_plan
            .fields
            .iter()
            .all(|field| match &field.value {
                Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => true,
                Expr::Field(fr) => {
                    // 无活 join：flat 或 list-index（`c.tags[0]`，gap-5
                    // 2026-09-02，编译 ListIndex cvec）；有活 join 保持限定。
                    let shape_ok = if self.live_joins.is_empty() {
                        out_shape_ok(fr) || wf_lang::columnar::field_ref_is_list_index(fr)
                    } else {
                        out_shape_ok(fr)
                    };
                    // list-index root 引用 let（`x[0]`）→ 拒绝：列式无 let 视图，
                    // 编译 root 列缺失 → 全 null 静默失真（与行式 let 值分叉）。
                    let root_is_let = wf_lang::columnar::field_ref_is_list_index(fr)
                        && matches!(
                            fr,
                            FieldRef::Path { segments, .. }
                                if matches!(
                                    segments.first(),
                                    Some(PathSegment::Field(root))
                                        if let_names.contains(root.as_str())
                                )
                        );
                    shape_ok
                        && !root_is_let
                        && !matches!(fr, FieldRef::Simple(name) if let_names.contains(name.as_str()))
                }
                // 列式输出函数（fmt/strftime/count_char，参数为字面量/flat 字段）
                // 走无 join 路径的批量 cell 求值；有活 join 时拒绝（列式 join
                // 富化路径未接入批量 cell，回退行式避免 unreachable panic）。
                // **例外（2026-08-25 q13b 列式化）**：`fmt("{}", 限定字段)`
                // 单参数恒等——列式 join 富化路径读字段后按 fmt 语义渲染
                // （Str 透传 / 非 Str value_to_string），双侧门控校验字段限定。
                other if !self.live_joins.is_empty() => {
                    fmt_identity_field(other).is_some_and(&out_shape_ok)
                }
                other => wf_lang::columnar::columnar_output_expr(other),
            })
    }

    /// Whether the on-each plan can run the **intermediate-pipe** columnar
    /// fast path (q13a 等 each→pipe 生产路径，2026-08-25 q13a 列式化)。
    ///
    /// 形状（比 sink 的 [`Self::each_plan_columnar_safe`] 更严，因为 pipe
    /// 列式路径是新建的、按保守形状实现）：无 joins / lets / where / each
    /// filter（无每行拒绝）、bind filter 列式或无、score 常量、entity =
    /// 字面量/flat 字段、yield 值 ∈ {字面量, flat 字段, `expr_is_columnar`
    /// （BinOp 如 q13a `auction % 10000` 编译为批级 cvec）}。Anything else
    /// falls back to the Event-based record path
    /// (`execute_each_with_joins` + `PipeBatchStager::push_record`).
    pub fn each_pipe_columnar_safe(&self) -> bool {
        let Some(each_plan) = &self.plan.each_plan else {
            return false;
        };
        if !self.plan.lets.is_empty() || !self.live_joins.is_empty() || each_plan.filter.is_some() {
            return false;
        }
        // gap-3（2026-09-02）：无 join 时允许**可列式**的 post-join where
        // （批级守卫掩码，逐行 where_ok 语义一致）；非列式 where → 回退行式。
        if self
            .plan
            .r#where
            .as_ref()
            .is_some_and(|w| !wf_lang::columnar::expr_is_columnar(w))
        {
            return false;
        }
        if !self.plan.binds.iter().all(|b| {
            b.filter
                .as_ref()
                .is_none_or(wf_lang::columnar::expr_is_columnar)
        }) {
            return false;
        }
        // score 常量（meta 回退列 `_wfu_meta_score` 用；q13a `score(10.0)`）。
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
                other => wf_lang::columnar::expr_is_columnar(other),
            })
    }
}
