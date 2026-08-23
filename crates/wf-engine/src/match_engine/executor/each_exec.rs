use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use arrow::array::{Array, ArrayAccessor, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};

use crate::alert::{AlertColumnBuilder, EachRowCells};
use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::MACHINE_ID;
use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow};
use crate::match_engine::match_engine::{
    CepStateMachine, Event, JoinKey, Value, WindowLookup, eval_field_value, field_ref_name,
    value_to_string, values_equal,
};

use super::RuleExecutor;
use super::YieldKind;
use super::alert::{
    EachWfxPrefix, build_each_wfx_id, build_each_wfx_id_reusing, format_nanos_utc, now_nanos,
    write_int64_value,
};
use super::context::execute_joins;
use super::eval::{
    YieldMeta, eval_bool_expr, eval_entity_id, eval_expr_with_l3, eval_score,
    eval_yield_expr_with_meta, with_yield_eval_scope,
};
use wf_lang::plan::{JoinPlan, RulePlan};

// L3 batched write (now unconditional): collect a segment's column values and
// bulk-`extend` each builder column once at the end via
// `commit_each_rows_batch`, instead of per-row `commit_each_row`. Cell staging
// still runs through the builder (same validation+export); only the final
// column push is batched. Byte-identical to the per-row commit (see the
// `commit_each_rows_batch_*` equivalence tests) — Q1 on-each is fill-bound and
// this is ~4× cheaper on CPU and ~half the RSS.

/// Columnar join-enrichment plan for `on each` + one live Snapshot join
/// (2026-08-23, 列式 join 富化 — q20 等 each+join 查询 2.5M/s → 列式量级).
///
/// v1 形状（q20 等）：单 Snapshot join、单条件、左右均 flat 限定引用；
/// `where` 为「右窗限定字段 <cmp> 字面量」的合取；yield/entity 为字面量 /
/// 左窗（驱动）限定字段 / 右窗限定字段。行式路径（`execute_each_direct`）
/// 每事件 `Event::clone()` + `enrich_join_row` 全字段注入 + `find_matching_row`
/// 复核；列式版批级去重 join_lookup + 列式读右窗字段，输出字节一致。
#[derive(Debug, Clone)]
pub(crate) struct EachJoinPlan {
    /// 右窗名（enrich 限定前缀，如 `auction_events`）。
    pub(super) right_window: String,
    /// 右窗 join key 字段（索引键，如 `auction_events.id`）。
    pub(super) right_key_field: String,
    /// 左字段名（驱动列，如 `b.auction`）。
    pub(super) left_field: String,
    /// 驱动 bind alias（如 `b`），区分左窗/右窗限定引用。
    pub(super) left_alias: String,
    /// `where` 谓词（右窗字段 <cmp> 字面量，合取）。空 = 无 where。
    pub(super) where_preds: Vec<WherePred>,
}

/// 一个 `where` 谓词：右窗字段 `<op> 字面量`。
#[derive(Debug, Clone)]
pub(super) struct WherePred {
    pub(super) field: String,
    pub(super) op: wf_lang::ast::BinOp,
    pub(super) const_val: Value,
}

/// 解析 each 规则的列式 join 支持性。`Some` = 可走列式 join 路径；
/// `None` = 形状不支持（回退行式 `execute_each_direct`）。
///
/// 基于 `live_joins`（死 join 消除后）解析——死 join 不参与执行，规则有 1 死
/// 1 活 join 时活 join 若满足形状仍可列式化（2026-08-23 review：旧版基于
/// `plan.joins`，死 join 存在时误拒活 join）。
pub(crate) fn parse_each_join_columnar(
    plan: &RulePlan,
    live_joins: &[JoinPlan],
) -> Option<EachJoinPlan> {
    let join = live_joins.first()?;
    if live_joins.len() != 1 {
        return None;
    }
    if !matches!(join.mode, JoinMode::Snapshot) {
        return None;
    }
    if join.within.is_some() || join.reduce.is_some() || join.emit_at.is_some() {
        return None;
    }
    if join.conds.len() != 1 {
        return None;
    }
    let cond = &join.conds[0];
    let left_field = field_ref_name(&cond.left).to_string();
    let right_key_field = field_ref_name(&cond.right).to_string();
    if left_field.is_empty() || right_key_field.is_empty() {
        return None;
    }
    // 左右 key 必须 flat（Simple/Qualified/Bracketed）——Path（嵌套 object）
    // 在列式路径下无法按列名解析。
    let flat = |fr: &FieldRef| {
        matches!(
            fr,
            FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
        )
    };
    if !flat(&cond.left) || !flat(&cond.right) {
        return None;
    }
    let left_alias = plan.each_plan.as_ref()?.alias.clone();
    let right_window = join.right_window.clone();
    // join 条件左字段的限定符必须是驱动别名或裸字段（checker 保证左字段来自
    // 驱动事件；此处防御——Qualified 其他窗名时列式无法从驱动列解析）。
    if let FieldRef::Qualified(win, _) = &cond.left {
        if win.as_str() != left_alias {
            return None;
        }
    }

    // where：右窗限定字段 <cmp> 字面量 的合取（&&）。其他形状（左窗字段、
    // 函数、Simple 引用、`in` 列表）→ 不支持 → 回退行式。
    let mut where_preds = Vec::new();
    if let Some(w) = &plan.r#where {
        if !parse_where_preds(w, &right_window, &mut where_preds) {
            return None;
        }
    }

    // 输出字段来源：每个引用必须是 字面量 / 左窗限定 / 右窗限定。
    // Simple/Bracketed/Path/一般表达式 → 不支持（无法确定来源，保守回退）。
    let out_ok = |fr: &FieldRef| -> bool {
        match fr {
            FieldRef::Qualified(win, _) => win == &left_alias || win == &right_window,
            _ => false,
        }
    };
    for field in &plan.yield_plan.fields {
        match &field.value {
            Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => {}
            Expr::Field(fr) => {
                if !out_ok(fr) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    match &plan.entity_plan.entity_id_expr {
        Expr::StringLit(_) => {}
        Expr::Field(fr) => {
            if !out_ok(fr) {
                return None;
            }
        }
        _ => return None,
    }
    Some(EachJoinPlan {
        right_window,
        right_key_field,
        left_field,
        left_alias,
        where_preds,
    })
}

/// 递归解析 `where` 为右窗字段比较的合取。
fn parse_where_preds(expr: &Expr, right_window: &str, out: &mut Vec<WherePred>) -> bool {
    match expr {
        Expr::BinOp {
            op: BinOp::And,
            left,
            right,
        } => {
            parse_where_preds(left, right_window, out)
                && parse_where_preds(right, right_window, out)
        }
        Expr::BinOp { op, left, right }
            if matches!(
                op,
                BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge
            ) =>
        {
            let Expr::Field(FieldRef::Qualified(win, f)) = left.as_ref() else {
                return false;
            };
            if win != right_window {
                return false;
            }
            let const_val = match right.as_ref() {
                Expr::Number(n) => Value::Number(*n),
                Expr::StringLit(s) => Value::Str(s.clone().into()),
                Expr::Bool(b) => Value::Bool(*b),
                _ => return false,
            };
            out.push(WherePred {
                field: f.clone(),
                op: *op,
                const_val,
            });
            true
        }
        _ => false,
    }
}

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

        builder.reserve_rows(rows.len());
        let mut wfx_scratch = String::new();

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            if !passes_each_filter(filter, event) {
                stats.rejected += 1;
                continue;
            }
            // Rules without joins or `let` bindings never mutate the event —
            // borrow instead of cloning (same optimization as the per-event
            // path).
            let ctx: Cow<'_, Event> = if self.live_joins.is_empty() && self.plan.lets.is_empty() {
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
        if !self.plan.lets.is_empty() {
            return false;
        }
        // 无活 join：形状检查走无 join 列式路径（后置 where 列式不执行——bind
        // filter 已下推为事件过滤，plan.r#where 非空 → 回退行式）。单活 join：
        // 必须满足列式 join 形状（each_join_plan 非 None）——where/输出字段的
        // 限定来源由 `parse_each_join_columnar` 一并校验。多 join / 活 join
        // 不满足形状 → 回退行式。
        let join_ok = if self.live_joins.is_empty() {
            self.plan.r#where.is_none()
        } else {
            self.each_join_plan.is_some()
        };
        if !join_ok || each_plan.filter.is_some() {
            return false;
        }
        if !self.plan.binds.iter().all(|b| {
            b.filter
                .as_ref()
                .is_none_or(wf_lang::columnar::expr_is_columnar)
        }) {
            return false;
        }
        if !matches!(self.plan.score_plan.expr, Expr::Number(_)) {
            return false;
        }
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
        match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(_) => {}
            Expr::Field(fr) if out_shape_ok(fr) => {}
            _ => return false,
        }
        self.plan
            .yield_plan
            .fields
            .iter()
            .all(|field| match &field.value {
                Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => true,
                Expr::Field(fr) => out_shape_ok(fr),
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
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
        appended_out: &mut Vec<usize>,
    ) -> EachDirectBatchStats {
        appended_out.clear();
        let mut stats = EachDirectBatchStats::default();
        let mut prof = E1Profiler::maybe();
        let _ = &mut prof;
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

        // Batch-constant wfx_id FNV prefix: `rule_name \x00` hashed once per
        // batch (rule names run tens of bytes and were previously re-hashed
        // per row); the per-row suffix is only time LE + separators + origin.
        let wfx_prefix = EachWfxPrefix::new(&self.plan.name);

        // Batch-level constant-yield caching: literal fields (alert_type /
        // detail / request_count in Q1) are coerced + exported once here and
        // registered as batch-constant columns — the per-row loop skips
        // their staging entirely and `fill_row_gaps` fills the constant.
        // Field yields register as ordinary columns (layout-cache entry).
        for (((_field, (name, field_type)), kind), _field_ref) in self
            .plan
            .yield_plan
            .fields
            .iter()
            .zip(statics.yield_specs.iter())
            .zip(yield_kinds.iter())
            .zip(yield_field_refs.iter())
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
                            stats.failed = rows.len();
                            return stats;
                        }
                    }
                }
                YieldKind::Field | YieldKind::General => None,
            };
            if let Err(e) = builder.register_yield_column(name, const_value) {
                log::warn!("alert export error: {e}");
                stats.failed = rows.len();
                return stats;
            }
        }

        // Reserve AFTER registration: `register_yield_column` above may have
        // (re)created yield columns — the first call after a flush finds them
        // empty (`finish()` drops capacities) — and those columns must receive
        // this segment's capacity here. Reserving before registration left
        // them growing 0→N amortized, every ALERT_BATCH_SIZE segment.
        builder.reserve_rows(rows.len());

        // Batch-level column-index resolution: hoist the per-row `index_of`
        // schema lookups (Q1 entity and the variable yield id both read the
        // `auction` column — previously 2 `index_of` + column re-reads per row).
        // Column indices are stable for the batch lifetime (the schema is
        // Arc-shared and immutable), so resolve once here and read via
        // `ColumnarEvent::value_at` in the loop.
        let batch0 = rows.first().map(|(ev, _)| ev.batch());
        let resolve = |name: Option<&str>| -> Option<usize> {
            name.and_then(|n| batch0.and_then(|b| b.schema().index_of(n).ok()))
        };
        let entity_idx: Option<usize> = if entity_const.is_some() {
            None
        } else {
            resolve(entity_field.map(field_ref_name))
        };
        let yield_field_idxs: Vec<Option<usize>> = yield_field_refs
            .iter()
            .map(|fr| resolve(fr.map(field_ref_name)))
            .collect();
        // Batch-level typed entity column (P2): ONE downcast per batch — all
        // rows share `batch0` (the caller builds every ColumnarEvent from one
        // batch; the index resolution above already relies on this), so the
        // row loop reads the column with zero `&dyn Array` dispatch. Int64 /
        // Timestamp(ns) share the i64 rendering (`write_int64_value`,
        // byte-identical to the old value_at + value_to_string lane); plain
        // (non-structured) Utf8 reads `&str` directly — that is the qradar
        // entity shape (sip/source_ip/user). Structured Utf8 columns stay
        // Generic: `extract_field_value` must JSON-parse them, which the fast
        // lanes must not skip.
        let entity_col: EntityCol<'_> = match (entity_idx, batch0) {
            (Some(idx), Some(b)) => {
                let schema = b.schema();
                let field = schema.field(idx);
                let col = b.column(idx);
                match field.data_type() {
                    DataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map_or(EntityCol::Generic, |a| EntityCol::I64(I64Col::Int64(a))),
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => col
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .map_or(EntityCol::Generic, |a| EntityCol::I64(I64Col::TsNanos(a))),
                    DataType::Utf8 if !crate::match_engine::is_wfl_structured_field(field) => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map_or(EntityCol::Generic, EntityCol::Utf8),
                    _ => EntityCol::Generic,
                }
            }
            _ => EntityCol::Generic,
        };

        // L3 batched write: collect each segment row's column values and commit
        // them once at the end (see function-level doc). Cell staging still runs
        // through the builder (same validation+export); only the final column
        // push is batched.
        let mut wfx_ids: Vec<String> = Vec::new();
        let mut scores: Vec<f64> = Vec::new();
        let mut entity_ids: Vec<String> = Vec::new();
        let mut fired_ats: Vec<String> = Vec::new();
        // `Vec<(usize, DataType, ModelValue)>` — one row of staged yield cells
        // per segment row, drained via `builder.take_staged()`. Inferred here.
        let mut staged_rows: Vec<Vec<_>> = Vec::new();

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            // -- Per-row system values (identical to the Event-based path) ---
            let t_entity = if prof.enabled() {
                Some(Instant::now())
            } else {
                None
            };
            let score = score_const;
            // For a field-entity (Q1: `entity(digit, b.auction)`), hold the read
            // `Value` so a yield field referencing the same column (id=b.auction)
            // reuses it instead of re-reading the column per row. `entity_f64`
            // is the raw number on typed numeric lanes, letting that same yield
            // stage directly without constructing a `Value` (last materialization).
            let (entity_id, entity_val, entity_f64): (String, Option<Value>, Option<f64>) =
                match &entity_const {
                    Some(s) => (s.clone(), None, None),
                    None => match &entity_col {
                        EntityCol::I64(i64col) => match i64col.read(event.row()) {
                            Some(v) => {
                                let mut es = String::with_capacity(20);
                                write_int64_value(&mut es, v);
                                (es, Some(Value::Number(v as f64)), Some(v as f64))
                            }
                            None => {
                                let (eid, eval) = empty_entity_pair();
                                (eid, eval, None)
                            }
                        },
                        EntityCol::Utf8(arr) => {
                            let row = event.row();
                            if arr.is_null(row) {
                                let (eid, eval) = empty_entity_pair();
                                (eid, eval, None)
                            } else {
                                let s = arr.value(row);
                                (String::from(s), Some(Value::Str(s.into())), None)
                            }
                        }
                        EntityCol::Generic => {
                            match entity_idx.and_then(|idx| event.value_at(idx)) {
                                Some(v) => (value_to_string(&v), Some(v), None),
                                None => {
                                    let (eid, eval) = empty_entity_pair();
                                    (eid, eval, None)
                                }
                            }
                        }
                    },
                };
            if let Some(t) = t_entity {
                prof.add(e1_bucket_entity(), t);
            }
            let t_fired = if prof.enabled() {
                Some(Instant::now())
            } else {
                None
            };
            let fired_at = format_nanos_utc(*event_time_nanos);
            if let Some(t) = t_fired {
                prof.add(e1_bucket_fired(), t);
            }
            let t_wfx = if prof.enabled() {
                Some(Instant::now())
            } else {
                None
            };
            let wfx_id = wfx_prefix.wfx_id(*event_time_nanos, &origin);
            if let Some(t) = t_wfx {
                prof.add(e1_bucket_wfx(), t);
            }
            // (yield_meta is only consumed by General yield exprs — excluded
            // by the columnar gate, so it is not built here.)

            // -- Yield staging (fallible work before any column push) ------
            // Literal fields were registered batch-level above and are filled
            // by `fill_row_gaps` — only field (per-row value) yields stage.
            let t_stage = if prof.enabled() {
                Some(Instant::now())
            } else {
                None
            };
            // No `with_yield_eval_scope` here: the columnar gate excludes
            // General yield exprs, so nothing in this loop reads the
            // eval-time scope (`now()`) — the per-row TLS enter/leave was
            // pure overhead on this path.
            builder.begin_row();
            let staged: CoreResult<()> = (|| {
                for ((((_field, (name, field_type)), kind), _field_ref), field_idx_opt) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                    .zip(yield_field_refs.iter())
                    .zip(yield_field_idxs.iter().copied())
                {
                    let value = match kind {
                        YieldKind::Lit(_) => {
                            // Batch-constant: pre-registered, no per-row work.
                            continue;
                        }
                        YieldKind::Field => {
                            // last-materialization fast path: when this field
                            // is the same column as a typed-numeric entity (Q1
                            // id=b.auction) and the target type is numeric
                            // (digit/float/chars/untyped), stage the raw f64
                            // directly — no per-row `Value` construction, no
                            // `coerce` round-trip. `export_yield_f64` replicates
                            // the coerce+export byte-for-byte for these targets;
                            // other targets fall back below.
                            if let (Some(idx), Some(e_idx)) = (field_idx_opt, entity_idx)
                                && idx == e_idx
                                && let Some(n) = entity_f64
                                && is_numeric_yield_type(field_type.as_ref())
                            {
                                builder.stage_yield_cell_f64(name, field_type.as_ref(), n)?;
                                continue;
                            }
                            // Read by pre-resolved column index, skipping the
                            // per-row `index_of`; when the field is the same
                            // column as the field-entity (Q1: id=b.auction ==
                            // entity auction), reuse the value already read for
                            // entity_id instead of re-reading the column.
                            // A `None` index (column absent from the batch
                            // schema) falls back to empty string, exactly like
                            // `field_value(name).unwrap_or_else(default)` originally.
                            match (field_idx_opt, entity_idx) {
                                (Some(idx), Some(e_idx)) if idx == e_idx => entity_val
                                    .clone()
                                    .unwrap_or_else(|| Value::Str(SmolStr::default())),
                                (Some(idx), _) => event
                                    .value_at(idx)
                                    .unwrap_or_else(|| Value::Str(SmolStr::default())),
                                (None, _) => Value::Str(SmolStr::default()),
                            }
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
            })();
            if let Err(e) = staged {
                log::warn!("alert export error: {e}");
                stats.failed += 1;
                continue;
            }
            if let Some(t) = t_stage {
                prof.add(e1_bucket_stage(), t);
            }
            let t_commit = if prof.enabled() {
                Some(Instant::now())
            } else {
                None
            };
            // Batch-write: collect this row's columns; the per-row staged
            // cells are drained from the builder (same validation/export as
            // per-row). Commit all rows once after the loop.
            wfx_ids.push(wfx_id);
            scores.push(score);
            entity_ids.push(entity_id);
            fired_ats.push(fired_at);
            staged_rows.push(builder.take_staged());
            if let Some(t) = t_commit {
                prof.add(e1_bucket_commit(), t);
            }
            stats.appended += 1;
            appended_out.push(idx);
        }
        // L3 batched commit: one bulk column append for the whole segment.
        if !wfx_ids.is_empty() {
            builder.commit_each_rows_batch(
                &wfx_ids,
                &scores,
                &entity_ids,
                &fired_ats,
                &statics.rule_name,
                &statics.entity_type,
                &statics.each_origin,
                &statics.each_close_reason,
                &emit_time,
                &summary,
                &staged_rows,
            );
        }
        prof.report(rows.len());
        stats
    }

    /// Columnar join-enrichment form of [`Self::execute_each_direct_batch`]
    /// (2026-08-23, 列式 join 富化): like [`Self::execute_each_direct_batch_columnar`]
    /// but for `on each` + one live Snapshot join (q20 等).
    ///
    /// Row-level semantics are byte-identical to `execute_each_direct`:
    /// - join lookup via the shared index (`JoinKey::from_value` truncation +
    ///   `find_matching_row` first-hit; float left keys additionally re-check
    ///   `values_equal` per row against the bucket — the f64→Int truncation
    ///   would otherwise false-match);
    /// - Snapshot miss keeps the event but with no enrichment → a `where` on a
    ///   right-window field suppresses it, and right-window yield/entity reads
    ///   yield an empty value (identical to the eager ctx without the field);
    /// - per-event `Event::clone()` + `enrich_join_row` full-field injection
    ///   are eliminated — right-window fields are read on demand from the
    ///   columnar [`JoinRow`].
    ///
    /// Caller must gate on [`Self::each_plan_columnar_safe`] AND
    /// `self.each_join_plan.is_some()`.
    pub fn execute_each_direct_batch_columnar_join(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        windows: &dyn WindowLookup,
        emit_time_nanos: i64,
        builder: &mut AlertColumnBuilder,
        appended_out: &mut Vec<usize>,
    ) -> EachDirectBatchStats {
        appended_out.clear();
        let mut stats = EachDirectBatchStats::default();
        let join_plan = self
            .each_join_plan
            .as_ref()
            .expect("columnar join gate requires each_join_plan");
        let Some(each_plan) = &self.plan.each_plan else {
            stats.failed = rows.len();
            return stats;
        };
        let _ = each_plan;
        let statics = self.output_static();
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let summary = Arc::clone(
            statics
                .each_summary
                .as_ref()
                .expect("on-each rule missing precomputed summary"),
        );
        let origin = AlertOrigin::Event;
        let score_const = match &self.plan.score_plan.expr {
            Expr::Number(n) => n.clamp(0.0, 100.0),
            _ => unreachable!("columnar gate requires a constant score"),
        };

        // -- 输出字段来源解析（yield / entity）---------------------------
        // Left = 驱动列（按列名），Right = 命中 JoinRow（按右窗字段名）。
        // 字段名短、批级解析一次——owned String 避免闭包生命周期纠缠。
        enum FieldSrc {
            Left(String),
            Right(String),
        }
        let field_src = |fr: &FieldRef| -> Option<FieldSrc> {
            match fr {
                FieldRef::Qualified(win, f) if win == &join_plan.left_alias => {
                    Some(FieldSrc::Left(f.to_string()))
                }
                FieldRef::Qualified(win, f) if win == &join_plan.right_window => {
                    Some(FieldSrc::Right(f.to_string()))
                }
                _ => None,
            }
        };
        let yield_srcs: Vec<Option<FieldSrc>> = self
            .plan
            .yield_plan
            .fields
            .iter()
            .map(|field| match &field.value {
                Expr::Field(fr) => field_src(fr),
                _ => None,
            })
            .collect();
        let entity_src: Option<FieldSrc> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(_) => None, // handled by entity_const
            Expr::Field(fr) => field_src(fr),
            _ => None,
        };
        let entity_const: Option<String> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(s) => Some(s.clone()),
            _ => None,
        };
        // yield 字段种类（Lit/Field），同无 join 列式路径。
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
                _ => unreachable!("columnar join gate excludes general yield exprs"),
            })
            .collect();

        // -- 批级 join 查找 ----------------------------------------------
        // 左 key 列 index（batch0 共享 schema）。左列缺列 → 每行 key=None →
        // Snapshot miss（保留无富化），下面 row_match 全 None 路径一致。
        let batch0 = rows.first().map(|(ev, _)| ev.batch());
        let left_idx = batch0.and_then(|b| b.schema().index_of(&join_plan.left_field).ok());
        let left_is_float = match (batch0, left_idx) {
            (Some(b), Some(idx)) => matches!(
                b.schema().field(idx).data_type(),
                DataType::Float32 | DataType::Float64
            ),
            _ => false,
        };

        let mut per_row_vals: Vec<Option<Value>> = Vec::with_capacity(rows.len());
        let mut key_rows: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        for (i, (ev, _)) in rows.iter().enumerate() {
            let val = left_idx.and_then(|idx| ev.value_at(idx));
            match val.as_ref().and_then(|v| JoinKey::from_value(v)) {
                Some(k) => {
                    key_rows.entry(k).or_default().push(i);
                    per_row_vals.push(val);
                }
                None => per_row_vals.push(None),
            }
        }

        // 批级预查（快照）：每唯一 key 一次索引 lookup，hot key 享受去重。
        // 索引只增不减：批快照「命中」的行在行式逐事件时点必然也命中 → 与行式
        // 一致；「批快照 miss」的行在行循环时点**实时复查**（与行式逐事件同时
        // 机）——否则批处理期间并行 ingest 补 append 的实体（q20 lead 引用未来
        // auction）会被列式快照漏掉，EMIT 系统性偏少（rate=1m 实测 -8 万行）。
        let mut row_match: Vec<Option<JoinRow>> = vec![None; rows.len()];
        for idxs in key_rows.values() {
            let first_val = per_row_vals[*idxs.first().unwrap()]
                .as_ref()
                .expect("key_rows rows always have a value");
            let bucket = windows.join_lookup(
                &join_plan.right_window,
                &join_plan.right_key_field,
                first_val,
            );
            let first = if left_is_float {
                None
            } else {
                bucket.as_ref().and_then(|rs| rs.first().cloned())
            };
            for &i in idxs {
                let lv = per_row_vals[i]
                    .as_ref()
                    .expect("key_rows rows always have a value");
                row_match[i] = if left_is_float {
                    bucket.as_ref().and_then(|rs| {
                        rs.iter()
                            .find(|r| {
                                r.field_value(&join_plan.right_key_field)
                                    .is_some_and(|rv| values_equal(lv, &rv))
                            })
                            .cloned()
                    })
                } else {
                    first.clone()
                };
            }
        }

        // -- 输出构建（复用无 join 列式模式：L3 批量提交）----------------
        let wfx_prefix = EachWfxPrefix::new(&self.plan.name);
        let mut wfx_ids: Vec<String> = Vec::new();
        let mut scores: Vec<f64> = Vec::new();
        let mut entity_ids: Vec<String> = Vec::new();
        let mut fired_ats: Vec<String> = Vec::new();
        let mut staged_rows: Vec<Vec<_>> = Vec::new();

        // 批级解析 Left（驱动列）字段的列 index —— 循环内按列名 index_of 是
        // 每行开销；schema 批内共享（batch0）。
        let resolve_left =
            |name: &str| -> Option<usize> { batch0.and_then(|b| b.schema().index_of(name).ok()) };
        let yield_left_idxs: Vec<Option<usize>> = yield_srcs
            .iter()
            .map(|src| match src {
                Some(FieldSrc::Left(f)) => resolve_left(f),
                _ => None,
            })
            .collect();
        let entity_left_idx: Option<usize> = match &entity_src {
            Some(FieldSrc::Left(f)) => resolve_left(f),
            _ => None,
        };

        // 批级常量 yield 字段注册（同无 join 列式路径）：字面量字段
        // （alert_type/detail/request_count 等）coerce+export 一次并注册为
        // 批级常量列，行循环跳过其 staging，`fill_row_gaps` 填充。
        for ((_field, (name, field_type)), kind) in self
            .plan
            .yield_plan
            .fields
            .iter()
            .zip(statics.yield_specs.iter())
            .zip(yield_kinds.iter())
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
                            stats.failed = rows.len();
                            return stats;
                        }
                    }
                }
                YieldKind::Field | YieldKind::General => None,
            };
            if let Err(e) = builder.register_yield_column(name, const_value) {
                log::warn!("alert export error: {e}");
                stats.failed = rows.len();
                return stats;
            }
        }
        builder.reserve_rows(rows.len());

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            // 批快照 miss 的行：行循环时点实时复查（与行式逐事件同时机——并行
            // ingest 在批处理期间补 append 的实体此时可见）。命中行沿用批快照
            // （索引只增，快照命中 ⇔ 逐事件命中）。
            let matched: Option<JoinRow> = if row_match[idx].is_some() {
                row_match[idx].clone()
            } else if let Some(v) = &per_row_vals[idx] {
                let bucket =
                    windows.join_lookup(&join_plan.right_window, &join_plan.right_key_field, v);
                if left_is_float {
                    bucket.as_ref().and_then(|rs| {
                        rs.iter()
                            .find(|r| {
                                r.field_value(&join_plan.right_key_field)
                                    .is_some_and(|rv| values_equal(v, &rv))
                            })
                            .cloned()
                    })
                } else {
                    bucket.as_ref().and_then(|rs| rs.first().cloned())
                }
            } else {
                None
            };
            let matched = matched.as_ref();
            // Post-join `where`（严格）：右窗字段比较；miss → 字段缺失 → false
            // → 抑制（对齐行式 where_ok：false/None 抑制）。
            let where_ok = join_plan.where_preds.iter().all(|p| {
                matched
                    .and_then(|r| r.field_value(&p.field))
                    .map(|v| join_cmp(p.op, &v, &p.const_val))
                    .unwrap_or(false)
            });
            if !where_ok {
                stats.rejected += 1;
                continue;
            }
            // entity（来源：常量 / 左窗列 / 右窗 JoinRow；缺失 → 空串，同行式）。
            let entity_id: String = match &entity_const {
                Some(s) => s.clone(),
                None => match &entity_src {
                    Some(FieldSrc::Left(_)) => entity_left_idx
                        .and_then(|eidx| event.value_at(eidx))
                        .map(|v| value_to_string(&v))
                        .unwrap_or_default(),
                    Some(FieldSrc::Right(f)) => matched
                        .and_then(|r| r.field_value(f))
                        .map(|v| value_to_string(&v))
                        .unwrap_or_default(),
                    None => String::new(),
                },
            };
            let fired_at = format_nanos_utc(*event_time_nanos);
            let wfx_id = wfx_prefix.wfx_id(*event_time_nanos, &origin);

            // -- Yield staging -------------------------------------------
            builder.begin_row();
            let staged: CoreResult<()> = (|| {
                for (yield_i, (((_field, (name, field_type)), kind), src)) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                    .zip(yield_srcs.iter())
                    .enumerate()
                {
                    let value = match kind {
                        YieldKind::Lit(_) => continue, // 批级常量，fill_row_gaps 填充
                        YieldKind::Field => match src {
                            Some(FieldSrc::Left(_)) => yield_left_idxs
                                .get(yield_i)
                                .copied()
                                .flatten()
                                .and_then(|fidx| event.value_at(fidx))
                                .unwrap_or_else(|| Value::Str(SmolStr::default())),
                            Some(FieldSrc::Right(f)) => matched
                                .and_then(|r| r.field_value(f))
                                .unwrap_or_else(|| Value::Str(SmolStr::default())),
                            None => Value::Str(SmolStr::default()),
                        },
                        YieldKind::General => {
                            unreachable!("columnar join gate excludes general yield exprs")
                        }
                    };
                    let Some(value) = RuleExecutor::coerce_yield_field_value_with(
                        name,
                        field_type.as_ref(),
                        value,
                    )?
                    else {
                        continue;
                    };
                    builder.stage_yield_cell(name, field_type.as_ref(), &value)?;
                }
                Ok(())
            })();
            if let Err(e) = staged {
                log::warn!("alert export error: {e}");
                stats.failed += 1;
                continue;
            }
            wfx_ids.push(wfx_id);
            scores.push(score_const);
            entity_ids.push(entity_id);
            fired_ats.push(fired_at);
            staged_rows.push(builder.take_staged());
            stats.appended += 1;
            appended_out.push(idx);
        }
        if !wfx_ids.is_empty() {
            builder.commit_each_rows_batch(
                &wfx_ids,
                &scores,
                &entity_ids,
                &fired_ats,
                &statics.rule_name,
                &statics.entity_type,
                &statics.each_origin,
                &statics.each_close_reason,
                &emit_time,
                &summary,
                &staged_rows,
            );
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
        self.build_each_alert_with(
            ctx,
            event_time_nanos,
            AlertOrigin::Event,
            field_order,
            emit_time_nanos,
        )
    }

    /// [`Self::build_each_alert`] 的可参数化版本：允许自定义 [`AlertOrigin`] 与
    /// `fired_at` 事件时间（P3 deferred join 到期输出用 `origin=Deferred`、
    /// `fired_at=到期 watermark`）。
    pub(crate) fn build_each_alert_with(
        &self,
        ctx: &Event,
        fired_at_nanos: i64,
        origin: AlertOrigin,
        field_order: &[&SmolStr],
        emit_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        let statics = self.output_static();
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let fired_at = format_nanos_utc(fired_at_nanos);
        let emit_time = self.cached_emit_time(emit_time_nanos);
        let wfx_id = build_each_wfx_id(&self.plan.name, fired_at_nanos, ctx, &origin, field_order);
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
            fired_at_nanos,
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

        let machine_id = Arc::from(CepStateMachine::extract_event_str(ctx, MACHINE_ID));

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
            event_time_nanos: fired_at_nanos,
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

/// Env-gated per-row segment profiler for the columnar on-each execute path
/// (Q1 bisection). Defaults to off with one `OnceLock`-cached `Instant`-free
/// check; `E1_TIMER=1` breaks the per-row budget into entity / fired_at /
/// wfx_id / begin+stage / commit buckets and prints ns/row after the batch.
/// Intended for `each_bench` and end-to-end profiling, never shipped hot-path.
struct E1Profiler {
    on: bool,
    buckets: [u64; 5],
}

#[inline(always)]
fn e1_bucket_entity() -> usize {
    0
}
#[inline(always)]
fn e1_bucket_fired() -> usize {
    1
}
#[inline(always)]
fn e1_bucket_wfx() -> usize {
    2
}
#[inline(always)]
fn e1_bucket_stage() -> usize {
    3
}
#[inline(always)]
fn e1_bucket_commit() -> usize {
    4
}

impl E1Profiler {
    fn maybe() -> Self {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        let on = *ENABLED.get_or_init(|| {
            std::env::var("E1_TIMER").is_ok() && std::env::var("E1_TIMER").as_deref() != Ok("0")
        });
        E1Profiler {
            on,
            buckets: [0; 5],
        }
    }
    #[inline(always)]
    fn enabled(&self) -> bool {
        self.on
    }
    #[inline(always)]
    fn add(&mut self, bucket: usize, start: Instant) {
        if self.on {
            self.buckets[bucket] += start.elapsed().as_nanos() as u64;
        }
    }
    fn report(&self, rows: usize) {
        if !self.on || rows == 0 {
            return;
        }
        let total: u64 = self.buckets.iter().sum();
        let n = rows as f64;
        eprintln!(
            "[E1-profiler] rows={rows} total={:.1}ns/row",
            total as f64 / n
        );
        let names = [
            "\u{7c} entity  ",
            "\u{7c} fired_at",
            "\u{7c} wfx_id  ",
            "\u{7c} stage   ",
            "\u{7c} commit  ",
        ];
        for (name, ns) in names.iter().zip(self.buckets.iter()) {
            eprintln!(
                "  {} {:>7.1} ns/row  ({:>4.1}% of segment total)\n",
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

/// The null / missing-column entity fallback on the columnar on-each path:
/// the Event reference path routes a missing entity field through the yield
/// empty-string fallback, so the row still appends with `entity_id = ""` and
/// a shared-column yield reads the empty string too.
#[inline(always)]
fn empty_entity_pair() -> (String, Option<Value>) {
    (String::new(), Some(Value::Str(SmolStr::default())))
}

/// Whether `export_yield_f64` handles the target type natively (no `Value`
/// fallback), so the entity==yield numeric fast lane can stage the raw number
/// directly and stay byte-identical to the `Value::Number` coerce+export path.
#[inline(always)]
fn is_numeric_yield_type(field_type: Option<&wf_lang::FieldType>) -> bool {
    matches!(
        field_type,
        None | Some(wf_lang::FieldType::Base(wf_lang::BaseType::Digit))
            | Some(wf_lang::FieldType::Base(wf_lang::BaseType::Float))
            | Some(wf_lang::FieldType::Base(wf_lang::BaseType::Chars))
    )
}

/// Batch-resolved typed entity column (P2): ONE downcast per batch, direct
/// typed reads per row — replaces the per-row `value_at` +
/// `write_flat_column_scratch` double dynamic dispatch on the entity path.
enum EntityCol<'a> {
    /// Int64 / Timestamp(ns) — physically i64 arrays; one typed read feeds
    /// both the `write_int64_value` rendering and the `Value` held for
    /// shared-column yield reuse.
    I64(I64Col<'a>),
    /// Plain (non-structured) Utf8 — `&str` read pushed directly (the qradar
    /// entity shape: sip / source_ip / user). Structured Utf8 columns must
    /// stay [`EntityCol::Generic`] — their values JSON-parse in
    /// `extract_field_value`.
    Utf8(&'a StringArray),
    /// Everything else keeps the existing `value_at` + `value_to_string` lane.
    Generic,
}

/// The two physically-i64 column flavors an [`EntityCol::I64`] can hold.
enum I64Col<'a> {
    Int64(&'a Int64Array),
    TsNanos(&'a TimestampNanosecondArray),
}

impl I64Col<'_> {
    /// Typed read with the same null gate as `ColumnarEvent::value_at`
    /// (`None` on a null slot → the shared entity-failure branch).
    #[inline(always)]
    fn read(&self, row: usize) -> Option<i64> {
        match self {
            I64Col::Int64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            I64Col::TsNanos(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
        }
    }
}

/// 复刻 `eval::compare_values` 的标量比较语义（列式 where 谓词求值用；与行式
/// where_ok 的 `eval_bool_expr` 输出逐位一致）：
/// - Eq/Ne → `values_equal`（Number 容差、Str/Bool 相等）；
/// - 有序比较 → 同类型 Number/Str/Bool 直接比；跨类型 → false。
fn join_cmp(op: BinOp, lv: &Value, rv: &Value) -> bool {
    match op {
        BinOp::Eq => values_equal(lv, rv),
        BinOp::Ne => !values_equal(lv, rv),
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => match (lv, rv) {
            (Value::Number(a), Value::Number(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            (Value::Str(a), Value::Str(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            (Value::Bool(a), Value::Bool(b)) => match op {
                BinOp::Lt => a < b,
                BinOp::Gt => a > b,
                BinOp::Le => a <= b,
                BinOp::Ge => a >= b,
                _ => false,
            },
            _ => false,
        },
        _ => false,
    }
}
