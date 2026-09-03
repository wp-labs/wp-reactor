//! 直发（on-each）执行路径：逐事件/逐批推进 step 分支，命中即组装输出行
//! （alert 列构建复用），覆盖 deferred / pipe 等变体；与 stats_exec（窗口
//! 统计）正交。引擎内执行器组织见 `executor/mod.rs`。

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use arrow::array::{Array, ArrayAccessor, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode, PathSegment};

use crate::alert::{AlertColumnBuilder, EachRowCells};
use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::MACHINE_ID;
use crate::match_engine::cep::{
    CepStateMachine, Event, FieldSource, JoinKey, Value, WindowLookup, eval_field_value,
    field_ref_name, value_to_string, values_equal,
};
use crate::match_engine::columnar::{
    CVec, ColumnarBatch, compile_guard, compile_yield_cvec, cscalar_to_value,
};
use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow};

use super::RuleExecutor;
use super::YieldKind;
use super::alert::{
    EachWfxPrefix, build_each_wfx_id, build_each_wfx_id_reusing, format_nanos_utc, now_nanos,
    write_int64_value,
};
use super::close_exec::CloseBatchVecs;
use super::context::execute_joins;
use super::eval::{
    YieldMeta, eval_bool_expr, eval_entity_id, eval_expr_with_l3, eval_score,
    eval_yield_expr_with_meta, with_yield_eval_scope,
};
use wf_lang::plan::{JoinPlan, RulePlan};

/// Score 表达式形状（列式门控）：常量，或「常量 × 字段」（q1 的
/// `score(0.908 * b.price)`）。常量×字段可在列式路径按行从 Arrow 列读 f64
/// 乘常量——与解释求值 `ln * rn` 字节一致（IEEE f64 乘法交换，clamp 相同）。
/// 其他形状（含 Add/Div、字段×字段）仍回退行式，保持两路径字节一致。
enum ScoreShape<'a> {
    Const(f64),
    MulConst { const_v: f64, field: &'a FieldRef },
}

fn score_shape(expr: &Expr) -> Option<ScoreShape<'_>> {
    match expr {
        Expr::Number(n) => Some(ScoreShape::Const(*n)),
        Expr::BinOp {
            op: BinOp::Mul,
            left,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Number(c), Expr::Field(fr)) | (Expr::Field(fr), Expr::Number(c)) => {
                Some(ScoreShape::MulConst {
                    const_v: *c,
                    field: fr,
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// 列式执行时的 score 求值计划（`ScoreShape` 的拥有版本）。
#[derive(Clone)]
enum ScorePlan {
    Const(f64),
    MulConst { const_v: f64, field: FieldRef },
}

impl ScorePlan {
    fn parse(expr: &Expr) -> Option<ScorePlan> {
        match score_shape(expr)? {
            ScoreShape::Const(n) => Some(ScorePlan::Const(n)),
            ScoreShape::MulConst { const_v, field } => Some(ScorePlan::MulConst {
                const_v,
                field: field.clone(),
            }),
        }
    }

    fn field(&self) -> Option<&FieldRef> {
        match self {
            ScorePlan::Const(_) => None,
            ScorePlan::MulConst { field, .. } => Some(field),
        }
    }

    /// 按行求值：常量直接返回；常量×字段从 `score_idx` 列读 f64 乘常量。
    /// 返回 None = 字段缺失/非数值（与解释路径 `eval_score` 的 Err 对应）。
    fn eval(&self, event: &ColumnarEvent<'_>, score_idx: Option<usize>) -> Option<f64> {
        match self {
            ScorePlan::Const(n) => Some(n.clamp(0.0, 100.0)),
            ScorePlan::MulConst { const_v, .. } => {
                let idx = score_idx?;
                let v = event.value_at(idx)?;
                match v {
                    Value::Number(n) => Some((n * const_v).clamp(0.0, 100.0)),
                    _ => None,
                }
            }
        }
    }
}

/// flat FieldRef（Simple/Qualified/Bracketed）——score/entity 快通道字段定义
/// （无活 join 时 out_shape_ok 的字段形状；本执行器只服务无活 join 的
/// each-direct 列式路径）。
fn is_flat_field(field: &FieldRef) -> bool {
    matches!(
        field,
        FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
    )
}

/// score 是否为「一般列式表达式」（非 常量 / 常量×**flat** 快通道形状）——gate
/// 的 score_ok 与列式执行器的 score_cvec 槽位共用同一分类（gap-6 2026-09-02）。
/// 常量×list-index 字段（`0.5 * c.tags[0]`）**不是**快通道形状：快车道
/// `value_at` 只读 flat 列，索引元素需 offset 读 → 归一般（编译
/// ListIndex × 常量 cvec）。
fn score_is_general(expr: &Expr) -> bool {
    match score_shape(expr) {
        Some(ScoreShape::Const(_)) => false,
        Some(ScoreShape::MulConst { field, .. }) => !is_flat_field(field),
        None => true,
    }
}

/// entity 是否为「一般列式表达式」（非 StringLit / flat Field 快通道形状）——
/// gate 放行与执行器 entity_cvec 槽位共用同一分类（gap-7 2026-09-02）。
fn entity_is_general(expr: &Expr) -> bool {
    !matches!(expr, Expr::StringLit(_)) && !matches!(expr, Expr::Field(fr) if is_flat_field(fr))
}

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
    if let FieldRef::Qualified(win, _) = &cond.left
        && win.as_str() != left_alias
    {
        return None;
    }

    // where：右窗限定字段 <cmp> 字面量 的合取（&&）。其他形状（左窗字段、
    // 函数、Simple 引用、`in` 列表）→ 不支持 → 回退行式。
    let mut where_preds = Vec::new();
    if let Some(w) = &plan.r#where
        && !parse_where_preds(w, &right_window, &mut where_preds)
    {
        return None;
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
            // 2026-08-25 q13b 列式化：`fmt("{}", 左/右窗 flat 字段)` = 字段值的
            // 字符串渲染（fmt 单参数恒等，模板恰为 "{}"）。列式 join 路径读
            // 字段后按 fmt 语义渲染（Str 透传 / 非 Str `value_to_string`），
            // 免 row path 的 Event clone + fmt 解释（q13b 1.3µs → 列式 462ns，
            // 分配量大降——q13a 分片放开后 RSS 28.9GB 的分配大头）。
            Expr::FuncCall {
                qualifier: None,
                name,
                args,
            } if name == "fmt"
                && args.len() == 2
                && matches!(&args[0], Expr::StringLit(t) if t == "{}")
                && matches!(&args[1], Expr::Field(fr) if out_ok(fr)) => {}
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

/// `fmt("{}", fr)` 单参数恒等（q13b 列式化）：模板恰为 `"{}"` 且参数是
/// 单字段引用。语义 = `value_to_string(字段值)`；Str 透传、非 Str 渲染——
/// 与解释器 fmt 的 `apply_fmt_template` 逐字节一致（对拍锁定）。
/// `None` = 不是该形状 → 行式回退。
pub(crate) fn fmt_identity_field(expr: &Expr) -> Option<&FieldRef> {
    match expr {
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } if name == "fmt"
            && args.len() == 2
            && matches!(&args[0], Expr::StringLit(t) if t == "{}") =>
        {
            match &args[1] {
                Expr::Field(fr) => Some(fr),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Batch-level precomputed on-each columnar state: the general-yield output
/// cvecs (`fmt`/`strftime`/`count_char`, batch-evaluated once) and the
/// each-filter mask. Opaque to callers — evaluated once per frame via
/// [`RuleExecutor::each_batch_prepare`] and reused across the runtime's
/// `ALERT_BATCH_SIZE` segments of one batch.
#[derive(Default)]
pub struct EachBatchVecs {
    general_cvecs: Vec<Option<CVec>>,
    filter_cvec: Option<CVec>,
    /// post-join `where` 掩码（无 join 时仅驱动列；gap-3 列式化 2026-09-02）：
    /// `None` = 无 where / 编译失败（结构化参数等）→ 逐行 `where_ok` 回退。
    where_cvec: Option<CVec>,
    /// 一般 score 表达式（非 常量/常量×flat，P4 gap-6 2026-09-02）批级 cvec：
    /// 逐行 cell → Number → clamp；`None` = 非一般形状 / 编译失败（读结构化
    /// 列等）→ 逐行 `eval_score` 回退（与行式字节一致）。
    score_cvec: Option<CVec>,
    /// 一般 entity 表达式（非 StringLit / flat Field，P4 gap-7 2026-09-02）
    /// 批级 cvec：逐行 cell → Value → `value_to_string`；`None` = 非一般形状 /
    /// 编译失败 → 逐行 `eval_entity_id` 回退。
    entity_cvec: Option<CVec>,
    /// Prepared batch row count + address — `debug_assert!` that the executor's
    /// segment rows read the same batch (misuse would index the wrong cvecs).
    num_rows: usize,
    batch_ptr: usize,
}

impl RuleExecutor {
    /// 列式批级 General yield 槽位（行式批路径，层 2 收口）：Event 数组物化
    /// （resolve = 事件字段裸名直查——`field_ref_name` 与 each 列式视图一致；
    /// let 内联在编译层，`yield_ref_fields` 已展开 let RHS 引用的 schema 字段）。
    /// 调用方须保证无活 join（join 富化字段不在物化视图）。
    pub(crate) fn event_batch_prepare(&self, rows: &[(&Event, i64)]) -> CloseBatchVecs {
        let n = rows.len();
        let slots = self.plan.yield_plan.fields.len();
        let ref_fields = self.yield_ref_fields(true);
        if n == 0 || ref_fields.is_empty() {
            return CloseBatchVecs {
                general_cvecs: (0..slots).map(|_| None).collect(),
            };
        }
        CloseBatchVecs {
            general_cvecs: self.compile_general_slots(
                &ref_fields,
                n,
                |row, name| rows[row].0.fields.get(name).cloned(),
                &self.plan.lets,
            ),
        }
    }

    /// Compile + batch-evaluate the on-each columnar output state for one
    /// `batch` (frame): general-yield cvecs (`fmt`/`strftime`/`count_char`,
    /// one slot per yield field, `None` = compile failed → per-row row
    /// fallback) and the each-filter mask (`None` = no filter or compile
    /// failed → per-row `passes_each_filter`).
    ///
    /// Caller must gate on [`Self::each_plan_columnar_safe`]; evaluation
    /// happens once per frame, so the per-segment executor work stays
    /// O(segment) instead of O(frame × segments).
    pub fn each_batch_prepare(&self, batch: &RecordBatch) -> EachBatchVecs {
        let view = ColumnarBatch::from_all_fields(batch);
        let n = view.num_rows();
        let general_cvecs: Vec<Option<CVec>> = self
            .plan
            .yield_plan
            .fields
            .iter()
            // 统一编译入口（compile_yield_cvec）：输出函数（fmt/strftime/
            // count_char）与**任意可列式表达式**（expr_is_columnar：BinOp 如
            // q13a `auction % 10000`、守卫函数）统一编译为批级 cvec——q13a 的
            // mod_key BinOp 因此走列式 each 路径（2026-08-25 q13a 列式化）。
            // Lit/Field 走各自快通道（不编译）。编译失败（结构化列参数等）→
            // 槽位 None → 行式回退。close 列式路径共用同一入口。
            .map(|field| compile_yield_cvec(field, &view, n, &self.plan.lets))
            .collect();
        // each filter：结构化字段（OBJECT/ARRAY 元数据列）比较在列式读原始
        // JSON 文本、解释器解析成 Object/Array，字节可分叉（与输出函数同源）
        // → 不编译（槽位 None → 逐行 `passes_each_filter` 解释回退）。
        let filter_cvec = self
            .plan
            .each_plan
            .as_ref()
            .and_then(|ep| ep.filter.as_ref())
            .filter(|f| !crate::match_engine::columnar::arg_reads_structured(&view, f))
            .and_then(|f| compile_guard(f, &view))
            .map(|plan| plan.eval_vec(&view, n));
        // post-join `where`（P4 gap-3，2026-09-02）：无 join 时仅驱动列，与
        // bind/each filter 同机制编译为批级守卫掩码（行式 `where_ok` 严格语义
        // ——false/缺失抑制输出）。结构化字段同样不编译（逐行 where_ok 回退，
        // 见 execute 行循环）。
        let where_cvec = self
            .plan
            .r#where
            .as_ref()
            .filter(|w| !crate::match_engine::columnar::arg_reads_structured(&view, w))
            .and_then(|w| compile_guard(w, &view))
            .map(|plan| plan.eval_vec(&view, n));
        // 一般 score / entity（P4 gap-6/7，2026-09-02）：非快通道形状
        // （常量 / 常量×flat、StringLit / flat Field）的可列式表达式编译为批级
        // cvec——快通道形状不编译（score_cvec/entity_cvec = None，行循环走原
        // 有 lane）。读结构化列的表达式不编译（列式读原始 JSON 文本 vs 解释器
        // 解析成 Object/Array 可分叉）→ 逐行 eval_score / eval_entity_id 回退。
        let score_cvec = if score_is_general(&self.plan.score_plan.expr) {
            let expr = &self.plan.score_plan.expr;
            (!crate::match_engine::columnar::arg_reads_structured(&view, expr))
                .then(|| compile_guard(expr, &view))
                .flatten()
                .map(|plan| plan.eval_vec(&view, n))
        } else {
            None
        };
        let entity_expr = &self.plan.entity_plan.entity_id_expr;
        let entity_cvec = if entity_is_general(entity_expr) {
            (!crate::match_engine::columnar::arg_reads_structured(&view, entity_expr))
                .then(|| compile_guard(entity_expr, &view))
                .flatten()
                .map(|plan| plan.eval_vec(&view, n))
        } else {
            None
        };
        EachBatchVecs {
            general_cvecs,
            filter_cvec,
            where_cvec,
            score_cvec,
            entity_cvec,
            num_rows: n,
            batch_ptr: batch as *const RecordBatch as usize,
        }
    }
}

impl RuleExecutor {
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
        let prepared = match rows.first() {
            Some((ev, _)) => self.each_batch_prepare(ev.batch()),
            None => EachBatchVecs::default(),
        };
        self.execute_each_direct_batch_columnar_with(
            rows,
            emit_time_nanos,
            &prepared,
            builder,
            appended_out,
        )
    }

    /// [`Self::execute_each_direct_batch_columnar`] with the batch-level
    /// columnar state **pre-evaluated once per batch** ([`Self::each_batch_prepare`])
    /// and reused across the runtime's `ALERT_BATCH_SIZE` segments —
    /// re-evaluating the general-yield cvecs + each-filter mask per segment
    /// over the full frame was O(frame × segments) (Q14 列式 4600 vs 466 ns/evt
    /// 的墙：65k 帧 × 16 段全帧重算)。
    ///
    /// `prepared` must be built from the same batch the `rows` read
    /// ([`Self::each_batch_prepare`] on `rows.first().batch()`); `debug_assert!`
    /// in release builds only checks row-count bounds, so the invariant is on
    /// the caller.
    ///
    /// Caller must gate on [`Self::each_plan_columnar_safe`]; the per-row
    /// output (wfx_id / entity_id / fired_at / yield cells) is byte-identical
    /// to the Event-based path — locked by the deferred-vs-columnar 对拍 test.
    pub fn execute_each_direct_batch_columnar_with(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        emit_time_nanos: i64,
        prepared: &EachBatchVecs,
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
        // shapes: score 常量 / 常量×flat（快通道）或可列式表达式（gap-6，批级
        // score_cvec，编译失败逐行 eval_score 回退）；entity StringLit / flat
        // Field（快通道）或可列式表达式（gap-7，entity_cvec 同款回退）。
        let score_plan = ScorePlan::parse(&self.plan.score_plan.expr);
        let entity_const: Option<String> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(s) => Some(s.clone()),
            _ => None,
        };
        let entity_field: Option<&FieldRef> = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(
                fr @ (FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)),
            ) => Some(fr),
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
                // list-index 字段（`c.tags[0]`，gap-5 2026-09-02）：Field 快
                // 通道只读 flat 列——索引元素走 General cvec（ListIndex）。
                Expr::Field(fr) if wf_lang::columnar::field_ref_is_list_index(fr) => {
                    YieldKind::General
                }
                Expr::Field(_) => YieldKind::Field,
                // 列式输出函数（fmt/strftime/count_char）→ General：批量 cell
                // 求值（general_cvecs），编译失败（结构化列参数）行式回退。
                // gate（each_plan_columnar_safe）保证 General 只含输出函数。
                _ => YieldKind::General,
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

        // 列式输出函数（fmt/strftime/count_char）与 each filter 掩码：批级编译
        // + `eval_vec` 整帧求值**一次**（`each_batch_prepare`），行循环只取
        // cell（向量化 cell 求值）；编译失败（结构化列参数等）→ 该 yield 行式
        // 回退（prepared 槽位 None）。
        // 仅当某 General yield 的槽位是 None（prepare 编译失败 → 每行解释回退）
        // 才构造每行 meta——全编译（Q14：fmt/strftime/count_char 槽位全 Some）
        // 时 meta 只被回退分支读取，构造是纯开销（Arc bump + Vec 分配）。
        let need_yield_meta = yield_kinds
            .iter()
            .zip(prepared.general_cvecs.iter())
            .any(|(kind, cvec)| matches!(kind, YieldKind::General) && cvec.is_none());

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
        debug_assert!(
            rows.is_empty()
                || prepared.batch_ptr == 0
                || batch0.is_some_and(|b| (b as *const RecordBatch as usize) == prepared.batch_ptr),
            "each_batch_prepare 必须来自 rows 的同一批"
        );
        debug_assert!(
            prepared.num_rows == 0 || rows.iter().all(|(ev, _)| ev.row() < prepared.num_rows),
            "rows 行号越界 prepared 批"
        );
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
        // Score 列索引（常量×字段快通道）：批级解析一次，行循环 value_at 读取。
        // 一般 score（gap-6）无单列——用批级 score_cvec。
        let score_idx: Option<usize> = match score_plan.as_ref().and_then(|p| p.field()) {
            Some(fr) => resolve(Some(field_ref_name(fr))),
            None => None,
        };
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
        let mut wfx_ids: Vec<SmolStr> = Vec::new();
        let mut scores: Vec<f64> = Vec::new();
        let mut entity_ids: Vec<SmolStr> = Vec::new();
        let mut fired_ats: Vec<String> = Vec::new();
        // `Vec<(usize, DataType, ModelValue)>` — one row of staged yield cells
        // per segment row, drained via `builder.take_staged()`. Inferred here.
        let mut staged_rows: Vec<Vec<_>> = Vec::new();

        for (idx, (event, event_time_nanos)) in rows.iter().enumerate() {
            // -- each filter（与行式 `passes_each_filter` 语义一致）--------
            // 列式掩码：null/非布尔 cell → 拒绝（行式 filter 求值 None → false）；
            // 掩码缺失（无 filter / 编译失败兜底行式）→ 解释逐行。
            let filter_pass = match (&each_plan.filter, &prepared.filter_cvec) {
                (None, _) => true,
                (Some(_), Some(cvec)) => cvec.bool_at(event.row()).unwrap_or(false),
                (Some(_), None) => passes_each_filter(each_plan.filter.as_ref(), &event.to_event()),
            };
            if !filter_pass {
                stats.rejected += 1;
                continue;
            }
            // -- post-join `where`（P4 gap-3，无 join 时仅驱动列）---------
            // 与行式 `where_ok` 严格语义一致：false/缺失（None）抑制输出。
            // 列式掩码：null/非布尔 cell → 拒绝；掩码缺失（无 where / 编译
            // 失败兜底）→ 解释逐行 where_ok（与 filter 同一回退模式）。
            let where_pass = match (&self.plan.r#where, &prepared.where_cvec) {
                (None, _) => true,
                (Some(_), Some(cvec)) => cvec.bool_at(event.row()).unwrap_or(false),
                (Some(_), None) => self.where_ok(&event.to_event()),
            };
            if !where_pass {
                stats.rejected += 1;
                continue;
            }
            // -- Per-row system values (identical to the Event-based path) ---
            let t_entity = if prof.enabled() {
                Some(Instant::now())
            } else {
                None
            };
            // -- score（与行式 `eval_score` 严格一致：非数值/缺失 → 整行跳过）--
            // 一般列式表达式（gap-6 2026-09-02，含 常量×list-index 字段）：批级
            // score_cvec cell → Number → clamp；槽位 None（编译失败 / 读结构化
            // 列）→ 逐行 eval_score 回退（Event 视图，行式语义）。快通道（常量 /
            // 常量×flat）：`ScorePlan::eval` value_at 直读。分类统一以
            // `score_is_general` 为 key（与 gate/prepare 同源）。
            let score = if score_is_general(&self.plan.score_plan.expr) {
                match &prepared.score_cvec {
                    Some(cvec) => match cvec.scalar_at(event.row()) {
                        Some(s) => match cscalar_to_value(&s) {
                            Value::Number(n) => Some(n.clamp(0.0, 100.0)),
                            _ => None,
                        },
                        None => None,
                    },
                    None => eval_score(&self.plan.score_plan.expr, &event.to_event()).ok(),
                }
            } else {
                score_plan
                    .as_ref()
                    .expect("非一般 score → ScorePlan 解析必然成功")
                    .eval(event, score_idx)
            };
            let Some(score) = score else {
                stats.failed += 1;
                continue;
            };
            // For a field-entity (Q1: `entity(digit, b.auction)`), hold the read
            // `Value` so a yield field referencing the same column (id=b.auction)
            // reuses it instead of re-reading the column per row. `entity_f64`
            // is the raw number on typed numeric lanes, letting that same yield
            // stage directly without constructing a `Value` (last materialization).
            let (entity_id, entity_val, entity_f64): (String, Option<Value>, Option<f64>) =
                if let Some(s) = &entity_const {
                    (s.clone(), None, None)
                } else if entity_field.is_some() {
                    match &entity_col {
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
                    }
                } else {
                    // gap-7：可列式 entity 表达式——批级 entity_cvec cell →
                    // Value → `value_to_string`（同 entity 快通道的 Generic 渲染）；
                    // 槽位 None（编译失败 / 读结构化列）→ 逐行 eval_entity_id。
                    match &prepared.entity_cvec {
                        Some(cvec) => match cvec.scalar_at(event.row()) {
                            Some(s) => {
                                let v = cscalar_to_value(&s);
                                (value_to_string(&v), None, None)
                            }
                            None => {
                                let (eid, eval) = empty_entity_pair();
                                (eid, eval, None)
                            }
                        },
                        None => match eval_entity_id(
                            &self.plan.entity_plan.entity_id_expr,
                            &event.to_event(),
                        ) {
                            Ok(eid) => (eid, None, None),
                            Err(e) => {
                                log::warn!("alert export error: {e}");
                                stats.failed += 1;
                                continue;
                            }
                        },
                    }
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
            // 仅当存在 General yield 且 prepare 编译失败（需逐行解释回退）时
            // 构造 meta——全编译（Q14）与纯 Lit/Field 输出（q1）都不构造，
            // 避免每行开销（原注释：被 gate 排除时 TLS 进出是纯开销）。
            let yield_meta = need_yield_meta.then(|| {
                self.each_yield_meta(
                    &wfx_id,
                    &fired_at,
                    &emit_time,
                    &summary,
                    score,
                    &entity_id,
                    &origin,
                    *event_time_nanos,
                    emit_time_nanos,
                )
            });

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
                for (
                    field_idx,
                    ((((field, (name, field_type)), kind), _field_ref), field_idx_opt),
                ) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                    .zip(yield_field_refs.iter())
                    .zip(yield_field_idxs.iter().copied())
                    .enumerate()
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
                            // 列式输出函数批量 cell：从预计算列取 cell；None
                            // （缺字段/null）→ 空串，与 eval_yield_expr_with_meta
                            // 的 None→空串一致。编译失败（结构化列参数等）→
                            // 行式回退（构造 Event ctx）。
                            // 槽位按 **yield 字段位置** 索引（general_cvecs 与
                            // yield_plan.fields 对齐，每字段一个槽位；非输出函数
                            // 字段是 None）——不能用只数 General 的游标（前面有
                            // Field/Lit 字段时会错位取到错误槽位，真实 q14 的
                            // id/alert_type 前置字段曾触发）。
                            match prepared
                                .general_cvecs
                                .get(field_idx)
                                .and_then(|oc| oc.as_ref())
                            {
                                Some(cvec) => match cvec.scalar_at(event.row()) {
                                    Some(s) => cscalar_to_value(&s),
                                    None => Value::Str(SmolStr::default()),
                                },
                                None => {
                                    // 逐行回退（编译失败）：有 let 绑定须先
                                    // 注入——`to_event()` 是原始行，无 let 视图
                                    // （q22 形态：let parts = split(...)，yield
                                    // 引用 parts）。apply_lets 幂等，多字段回退
                                    // 重复注入无害。
                                    let mut ev = event.to_event();
                                    if !self.plan.lets.is_empty() {
                                        self.apply_lets(&mut ev);
                                    }
                                    eval_yield_expr_with_meta(
                                        &field.value,
                                        &ev,
                                        yield_meta.expect("need_yield_meta → meta 已构造"),
                                    )
                                    .expect("eval_yield_expr_with_meta never returns None")
                                }
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
            entity_ids.push(SmolStr::from(entity_id));
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

    /// Columnar on-each emit for **intermediate pipe targets** (q13a 等
    /// each→pipe 生产路径，2026-08-25）：与 [`Self::execute_each_direct_batch_columnar_with`]
    /// 同源——逐行从 [`ColumnarEvent`] 直读字段（零 `Event` 物化、零
    /// `OutputRecord`/wfx_id/fired_at 脚手架），yield 表达式经批级 cvec
    /// （`%` BinOp 等，见 [`Self::each_batch_prepare`]）求值，结果经
    /// `coerce_yield_field_value_with` 同矩阵收口后交 runtime 装入 pipe 的
    /// 类型列。
    ///
    /// 行语义与 `execute_each_with_joins` → `PipeBatchStager::push_record`
    /// 字节一致（对拍测试钉死）。Caller must gate on
    /// [`Self::each_pipe_columnar_safe`]（无 filter/join/let；可列式 where
    /// gap-3 2026-09-02 → 逐行拒绝；其余全行 append）；`prepared` 必须来自
    /// rows 同一批。
    pub fn execute_each_pipe_batch_columnar(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        prepared: &EachBatchVecs,
        sink: &mut dyn PipeRowSink,
    ) -> EachDirectBatchStats {
        let mut stats = EachDirectBatchStats::default();
        let Some(_each_plan) = &self.plan.each_plan else {
            log::warn!(
                "execute_each_pipe_batch_columnar called for non-`on each` rule {}; skipping {} rows",
                self.plan.name,
                rows.len()
            );
            stats.failed = rows.len();
            return stats;
        };
        debug_assert!(self.each_pipe_columnar_safe());
        let statics = self.output_static();

        // score 常量（门控保证 Number 字面量）——批级求值一次，非每行。
        let score = match &self.plan.score_plan.expr {
            Expr::Number(n) => n.clamp(0.0, 100.0),
            _ => unreachable!("pipe columnar gate requires const score"),
        };
        let entity_const: Option<&str> = match &self.plan.entity_plan.entity_id_expr {
            Expr::StringLit(s) => Some(s.as_str()),
            _ => None,
        };
        let entity_field: Option<&FieldRef> = match &self.plan.entity_plan.entity_id_expr {
            Expr::Field(fr) => Some(fr),
            _ => None,
        };
        // yield 分类与列索引（与 sink 列式路径同款；Lit 批级常量、Field 按
        // 预解析列索引直读、General 从批级 cvec 取 cell）。
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
        let batch0 = rows.first().map(|(ev, _)| ev.batch());
        debug_assert!(
            rows.is_empty()
                || prepared.batch_ptr == 0
                || batch0.is_some_and(|b| (b as *const RecordBatch as usize) == prepared.batch_ptr),
            "each_batch_prepare 必须来自 rows 的同一批"
        );
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

        // 流式装载的可复用 scratch（**每批各一次分配**，而非每行）：
        // 原实现每行新建 `Vec<Option<Value>>` + `String`（实测 404 B/行）。
        let mut values: Vec<Option<Value>> = Vec::with_capacity(self.plan.yield_plan.fields.len());
        let mut entity_scratch = String::with_capacity(24);
        for (event, event_time_nanos) in rows {
            // post-join `where`（P4 gap-3，2026-09-02，无 join 时仅驱动列）：
            // 与行式 `where_ok` 严格语义一致（false/缺失抑制输出）；掩码缺失
            // （无 where / 编译失败）→ 解释逐行 where_ok。
            let where_pass = match (&self.plan.r#where, &prepared.where_cvec) {
                (None, _) => true,
                (Some(_), Some(cvec)) => cvec.bool_at(event.row()).unwrap_or(false),
                (Some(_), None) => self.where_ok(&event.to_event()),
            };
            if !where_pass {
                stats.rejected += 1;
                continue;
            }
            entity_scratch.clear();
            match &entity_const {
                Some(s) => entity_scratch.push_str(s),
                None => match &entity_col {
                    EntityCol::I64(i64col) => {
                        if let Some(v) = i64col.read(event.row()) {
                            write_int64_value(&mut entity_scratch, v);
                        }
                    }
                    EntityCol::Utf8(arr) => {
                        let row = event.row();
                        if !arr.is_null(row) {
                            entity_scratch.push_str(arr.value(row));
                        }
                    }
                    EntityCol::Generic => {
                        if let Some(v) = entity_idx.and_then(|idx| event.value_at(idx)) {
                            entity_scratch.push_str(&value_to_string(&v));
                        }
                    }
                },
            }
            let entity_id = &entity_scratch;
            // 门控无 General-编译失败？防御：构造一次 meta 供行式回退
            // （与 sink 路径的 need_yield_meta 同款，仅编译失败时真用到）。
            let yield_meta = yield_kinds
                .iter()
                .zip(prepared.general_cvecs.iter())
                .any(|(kind, cvec)| matches!(kind, YieldKind::General) && cvec.is_none())
                .then(|| self.each_yield_meta_light(entity_id, score, *event_time_nanos));
            values.clear();
            let mut row_ok = true;
            for (field_idx, ((((field, (name, field_type)), kind), _field_ref), field_idx_opt)) in
                self.plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(statics.yield_specs.iter())
                    .zip(yield_kinds.iter())
                    .zip(yield_field_refs.iter())
                    .zip(yield_field_idxs.iter().copied())
                    .enumerate()
            {
                let value = match kind {
                    YieldKind::Lit(v) => v.clone(),
                    YieldKind::Field => match field_idx_opt {
                        Some(idx) => event
                            .value_at(idx)
                            .unwrap_or_else(|| Value::Str(SmolStr::default())),
                        None => Value::Str(SmolStr::default()),
                    },
                    YieldKind::General => match prepared
                        .general_cvecs
                        .get(field_idx)
                        .and_then(|oc| oc.as_ref())
                    {
                        Some(cvec) => match cvec.scalar_at(event.row()) {
                            Some(s) => cscalar_to_value(&s),
                            None => Value::Str(SmolStr::default()),
                        },
                        None => eval_yield_expr_with_meta(
                            &field.value,
                            &event.to_event(),
                            yield_meta.expect("need_yield_meta → meta 已构造"),
                        )
                        .expect("eval_yield_expr_with_meta never returns None"),
                    },
                };
                match RuleExecutor::coerce_yield_field_value_with(name, field_type.as_ref(), value)
                {
                    Ok(Some(v)) => values.push(Some(v)),
                    Ok(None) => values.push(None), // 可选字段缺失 → 省略 cell
                    Err(e) => {
                        log::warn!("alert export error: {e}");
                        row_ok = false;
                        break;
                    }
                }
            }
            if !row_ok {
                stats.failed += 1;
                continue;
            }
            match sink.push_pipe_row(
                score,
                &statics.entity_type,
                entity_id,
                &values,
                *event_time_nanos,
            ) {
                Ok(()) => stats.appended += 1,
                Err(e) => {
                    // sink 装载失败（coercion/JSON 渲染）——与求值失败同口径：
                    // 记 failed 并继续下一行，不中断批次（同 sink 路径惯例）。
                    log::warn!("pipe row stage error: {e}");
                    stats.failed += 1;
                }
            }
        }
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
                // fmt("{}", fr) 恒等：按内部字段解析来源（值读回后渲染）。
                Expr::FuncCall {
                    qualifier: None,
                    name,
                    args,
                } if name == "fmt"
                    && args.len() == 2
                    && matches!(&args[0], Expr::StringLit(t) if t == "{}") =>
                {
                    match &args[1] {
                        Expr::Field(fr) => field_src(fr),
                        _ => None,
                    }
                }
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
        // yield 字段种类（Lit/Field），同无 join 列式路径。fmt("{}", 字段)
        // 恒等归入 Field（读值后按 fmt 语义渲染——`yield_fmt_render` 标记）。
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
                Expr::FuncCall {
                    qualifier: None,
                    name,
                    args,
                } if name == "fmt"
                    && args.len() == 2
                    && matches!(&args[0], Expr::StringLit(t) if t == "{}")
                    && matches!(&args[1], Expr::Field(_)) =>
                {
                    YieldKind::Field
                }
                _ => unreachable!("columnar join gate excludes general yield exprs"),
            })
            .collect();
        // fmt 单参数恒等标记：读值后非 Str → `value_to_string` 渲染（与解释器
        // fmt 的 `apply_fmt_template` 渲染一致）；Str 透传（零成本，q13b
        // side_input.value 是 Str）。
        let yield_fmt_render: Vec<bool> = self
            .plan
            .yield_plan
            .fields
            .iter()
            .map(|field| {
                matches!(
                    &field.value,
                    Expr::FuncCall {
                        qualifier: None,
                        name,
                        args,
                    } if name == "fmt"
                        && args.len() == 2
                        && matches!(&args[0], Expr::StringLit(t) if t == "{}")
                )
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
            match val.as_ref().and_then(JoinKey::from_value) {
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
        let mut row_match: Vec<Option<Arc<JoinRow>>> = vec![None; rows.len()];
        for idxs in key_rows.values() {
            let first_val = per_row_vals[*idxs.first().unwrap()]
                .as_ref()
                .expect("key_rows rows always have a value");
            let bucket = windows.join_lookup(
                &join_plan.right_window,
                &join_plan.right_key_field,
                first_val,
            );
            if left_is_float {
                for &i in idxs {
                    let lv = per_row_vals[i]
                        .as_ref()
                        .expect("key_rows rows always have a value");
                    row_match[i] = bucket.as_ref().and_then(|rs| {
                        rs.iter()
                            .find(|r| {
                                r.field_value(&join_plan.right_key_field)
                                    .is_some_and(|rv| values_equal(lv, &rv))
                            })
                            .cloned()
                            .map(Arc::new)
                    });
                }
            } else {
                // 非浮点左键：桶内所有行共享同一个首行 JoinRow——每个桶只搬移
                // 一次（`into_iter().next()` 零 Arc bump），每行仅 1 次 Arc clone
                // （此前每行 `first.clone()` 是 4 个 Arc bump，共享批 Arc 跨线程
                // 争用 → 采样 40% 线程时间在 drop_glue）。
                let first_arc: Option<Arc<JoinRow>> =
                    bucket.and_then(|rs| rs.into_iter().next()).map(Arc::new);
                for &i in idxs {
                    row_match[i] = match &first_arc {
                        Some(a) => Some(Arc::clone(a)),
                        None => None,
                    };
                }
            }
        }

        // -- 输出构建（复用无 join 列式模式；2026-08-26 改为**逐行 commit**）----
        // 此前：5 个中转 Vec（wfx_ids/scores/entity_ids/fired_ats/staged_rows）
        // 累积整批后 `commit_each_rows_batch`——该批式提交会**二次拷贝**
        // （`extend_from_slice` clone 每行 3 个 String + staged cell clone）。
        // 行式路径（`execute_each_direct`）一直用 `commit_each_row`（owned String
        // move、零拷贝）；等价性由
        // `commit_each_rows_batch_matches_repeated_commit_each_row` 守护。
        // 这是 q13b 输出链内存的 per-row 分配 churn 的一部分（2026-08-26 定位）。
        let wfx_prefix = EachWfxPrefix::new(&self.plan.name);

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
        // entity 列直读（2026-08-26 移植无 join 列式路径的 `EntityCol`）：
        // q13b 的 `entity(digit, m.bidder)` 是 Left Int64——原实现每行走
        // `event.value_at` → `Value::Number(f64)` → `value_to_string`（SmolStr
        // + 浮点 format，27.6M 行的 per-row 分配 churn 之一）。直读 Int64 列用
        // `write_int64_value` 直写 String（整数格式化，无 Value/SmolStr 中转）。
        let entity_col: EntityCol<'_> = match (entity_left_idx, batch0) {
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
            // 命中行直接借用 row_match 的 Arc 内容（零克隆——此前每行
            // `row_match[idx].clone()` 是 4 个 Arc bump + 行尾 drop）；miss 行
            // 实时复查结果暂存 miss_hold，仅 miss 行承担 lookup 成本。
            let miss_hold: Option<Arc<JoinRow>>;
            let matched: Option<&JoinRow> = match row_match[idx].as_ref() {
                Some(r) => Some(r.as_ref()),
                None => {
                    miss_hold = if let Some(v) = &per_row_vals[idx] {
                        let bucket = windows.join_lookup(
                            &join_plan.right_window,
                            &join_plan.right_key_field,
                            v,
                        );
                        if left_is_float {
                            bucket.as_ref().and_then(|rs| {
                                rs.iter()
                                    .find(|r| {
                                        r.field_value(&join_plan.right_key_field)
                                            .is_some_and(|rv| values_equal(v, &rv))
                                    })
                                    .cloned()
                                    .map(Arc::new)
                            })
                        } else {
                            bucket.and_then(|rs| rs.into_iter().next()).map(Arc::new)
                        }
                    } else {
                        None
                    };
                    miss_hold.as_ref().map(|a| a.as_ref())
                }
            };
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
            // entity（来源：常量 / 左窗列直读 / 左窗列通用 / 右窗 JoinRow；
            // 缺失 → 空串，同行式）。2026-08-26：Left 来源优先列直读
            // （EntityCol::I64/Utf8——零 Value/SmolStr 中转），仅通用类型回退
            // `value_at` + `value_to_string`。三元组对齐无 join 列式路径：
            // `entity_val`（同列 yield 复用）+ `entity_f64`（数字快车道 stage）。
            let (entity_id, entity_val, entity_f64): (
                smol_str::SmolStr,
                Option<Value>,
                Option<f64>,
            ) = match &entity_const {
                Some(s) => (smol_str::SmolStr::from(s.as_str()), None, None),
                None => match &entity_src {
                    Some(FieldSrc::Left(_)) => {
                        let row = event.row();
                        match &entity_col {
                            // 2026-08-26：SmolStrBuilder 直写——bidder 等数字
                            // ≤20 字符落在内联上限（22B），零堆分配（此前 String
                            // 每行一次堆分配，q13b 27.6M 行的 churn 之一）。
                            EntityCol::I64(i64col) => match i64col.read(row) {
                                Some(v) => {
                                    let mut b = smol_str::SmolStrBuilder::new();
                                    write_int64_value(&mut b, v);
                                    (b.into(), Some(Value::Number(v as f64)), Some(v as f64))
                                }
                                None => (smol_str::SmolStr::new(""), None, None),
                            },
                            EntityCol::Utf8(arr) => {
                                if arr.is_null(row) {
                                    (smol_str::SmolStr::new(""), None, None)
                                } else {
                                    (
                                        smol_str::SmolStr::from(arr.value(row)),
                                        Some(Value::Str(arr.value(row).into())),
                                        None,
                                    )
                                }
                            }
                            EntityCol::Generic => match entity_left_idx
                                .and_then(|eidx| event.value_at(eidx))
                            {
                                Some(v) => {
                                    (smol_str::SmolStr::from(value_to_string(&v)), Some(v), None)
                                }
                                None => (smol_str::SmolStr::new(""), None, None),
                            },
                        }
                    }
                    Some(FieldSrc::Right(f)) => (
                        smol_str::SmolStr::from(
                            matched
                                .and_then(|r| r.field_value(f))
                                .map(|v| value_to_string(&v))
                                .unwrap_or_default(),
                        ),
                        None,
                        None,
                    ),
                    None => (smol_str::SmolStr::new(""), None, None),
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
                        YieldKind::Field => {
                            // f64 快车道（对齐无 join 列式路径）：yield 字段与
                            // entity 同一左列（q13b：id=m.bidder == entity bidder）
                            // 且目标数字类型 → stage 原始 f64 直接写，跳过每行
                            // `value_at` + Value 构造 + coerce 中转。
                            if let (Some(FieldSrc::Left(yf)), Some(FieldSrc::Left(ef))) =
                                (&src, &entity_src)
                                && yf == ef
                                && let Some(n) = entity_f64
                                && is_numeric_yield_type(field_type.as_ref())
                            {
                                builder.stage_yield_cell_f64(name, field_type.as_ref(), n)?;
                                continue;
                            }
                            let mut value = match src {
                                Some(FieldSrc::Left(f)) => {
                                    // 同列复用 entity 已读值（不重读列）；否则按
                                    // 预解析列 index 直读（无每行 index_of）。
                                    if matches!(&entity_src, Some(FieldSrc::Left(ef)) if ef == f)
                                        && let Some(ev) = entity_val.clone()
                                    {
                                        ev
                                    } else {
                                        yield_left_idxs
                                            .get(yield_i)
                                            .copied()
                                            .flatten()
                                            .and_then(|fidx| event.value_at(fidx))
                                            .unwrap_or_else(|| Value::Str(SmolStr::default()))
                                    }
                                }
                                Some(FieldSrc::Right(f)) => matched
                                    .and_then(|r| r.field_value(f))
                                    .unwrap_or_else(|| Value::Str(SmolStr::default())),
                                None => Value::Str(SmolStr::default()),
                            };
                            // fmt("{}", x) 恒等：非 Str 值按 fmt 语义渲染为字符串
                            //（`apply_fmt_template` 的 value_to_string；Str 透传）。
                            if yield_fmt_render[yield_i] && !matches!(value, Value::Str(_)) {
                                value = Value::Str(value_to_string(&value).into());
                            }
                            value
                        }
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
            // 直连逐行 commit（owned 值 move 进列，零二次拷贝：wfx_id/entity_id
            // 是 SmolStr、fired_at 是 String）。
            builder.commit_each_row(EachRowCells {
                wfx_id,
                score: score_const,
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

    /// 中间窗轻量 build 就绪（2026-08-26 q4a deferred 轻量化）：yield 表达式
    /// **不引用任何 `__wfu_*` meta**——light `YieldMeta`（[`Self::each_yield_meta_light`]，
    /// q13a pipe 路径同款）里 `wfx_id`/`origin`/`fired_at`/`emit_time`/`summary`
    /// 是空槽，yield 若引用会拿到空值（静默变值）；不引用则空槽不可观测。
    /// `SystemVar` 全部由 light meta 提供真值（score/event times/emit_time_nanos），
    /// 无需排除。命中 → [`Self::build_each_alert_pipe`] 跳过告警字段构建。
    pub fn pipe_light_build_ready(&self) -> bool {
        self.plan
            .yield_plan
            .fields
            .iter()
            .all(|f| !expr_references_wfu_meta(&f.value))
    }

    /// 中间窗轻量 build（2026-08-26 q4a deferred）：跳过 sink 才需要的告警字段
    /// 构建——`wfx_id` 哈希+hex（`build_each_wfx_id`）、`fired_at` ISO8601 格式化
    /// （`format_nanos_utc`）、`machine_id` 提取——中间窗消费者（stats/列式 join）
    /// 按列读，不需要这些。yield 表达式由 [`Self::pipe_light_build_ready`] 保证
    /// 不引用空槽 meta。产出与全量 build 的 yield_fields/meta/event_time 逐位一致
    /// （对拍测试锁）。
    pub fn build_each_alert_pipe(
        &self,
        ctx: &Event,
        event_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        debug_assert!(self.pipe_light_build_ready());
        let statics = self.output_static();
        let score = eval_score(&self.plan.score_plan.expr, ctx)?;
        let entity_id = eval_entity_id(&self.plan.entity_plan.entity_id_expr, ctx)?;
        let yield_meta = self.each_yield_meta_light(&entity_id, score, event_time_nanos);
        let yield_fields = with_yield_eval_scope(|| {
            // 与 build_each_alert_with 完全相同的求值/coerce 矩阵（对拍契约）。
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
        Ok(Some(OutputRecord {
            wfx_id: String::new(),
            rule_name: Arc::clone(&statics.rule_name),
            score,
            entity_type: Arc::clone(&statics.entity_type),
            entity_id,
            origin: AlertOrigin::Deferred,
            fired_at: String::new(),
            emit_time: Arc::from(""),
            matched_rows: vec![],
            summary: Arc::from(""),
            yield_target: Arc::clone(&statics.yield_target),
            yield_fields,
            yield_field_types: Arc::clone(&statics.yield_field_types),
            event_time_nanos,
            machine_id: Arc::from(""),
            scope_key: Arc::from(""),
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
            evidence_first_time_nanos: Some(event_time_nanos),
            evidence_last_time_nanos: Some(event_time_nanos),
            window_start_time_nanos: Some(event_time_nanos),
            window_end_time_nanos: Some(event_time_nanos),
            emit_time_nanos: Some(emit_time_nanos),
            first_match_time_nanos: Some(emit_time_nanos),
            time_format: Some(self.output_config().time_format.as_str()),
        }
    }

    /// Light `YieldMeta` for the pipe-columnar path's rare interpreter
    /// fallback (a gate-passing yield whose cvec compile failed). The pipe
    /// gate ([`Self::each_pipe_columnar_safe`]) restricts yields to
    /// `expr_is_columnar`, which by construction can never read the meta keys
    /// left empty here (`wfx_id` / `origin` / `fired_at` / `emit_time` /
    /// `summary`) — SystemVar / WfuMeta expressions are excluded from
    /// `expr_is_columnar` — so the empty slots are unobservable by the
    /// fallback's evaluation.
    fn each_yield_meta_light<'a>(
        &'a self,
        entity_id: &'a str,
        score: f64,
        event_time_nanos: i64,
    ) -> YieldMeta<'a> {
        YieldMeta {
            score: Some(score),
            wfx_id: None,
            rule_name: Some(&self.plan.name),
            entity_type: Some(&self.plan.entity_plan.entity_type),
            entity_id: Some(entity_id),
            origin: None,
            close_reason: None,
            fired_at: None,
            emit_time: None,
            summary: None,
            event_first_time_nanos: Some(event_time_nanos),
            event_last_time_nanos: Some(event_time_nanos),
            evidence_first_time_nanos: Some(event_time_nanos),
            evidence_last_time_nanos: Some(event_time_nanos),
            window_start_time_nanos: Some(event_time_nanos),
            window_end_time_nanos: Some(event_time_nanos),
            emit_time_nanos: Some(event_time_nanos),
            first_match_time_nanos: Some(event_time_nanos),
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

/// yield 表达式是否引用任何 `__wfu_*` meta（2026-08-26 q4a 中间窗轻量化）：
/// 引用 → 回退全量 build（light YieldMeta 的空槽不可观测性不成立）。
/// `SystemVar` 由 light meta 提供真值，不视为引用。
fn expr_references_wfu_meta(expr: &wf_lang::ast::Expr) -> bool {
    use wf_lang::ast::Expr;
    match expr {
        Expr::WfuMeta(_) => true,
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::Field(_)
        | Expr::PresetParam(_) => false,
        Expr::Neg(e) | Expr::Not(e) => expr_references_wfu_meta(e),
        Expr::BinOp { left, right, .. } => {
            expr_references_wfu_meta(left) || expr_references_wfu_meta(right)
        }
        Expr::FuncCall { args, .. } => args.iter().any(expr_references_wfu_meta),
        Expr::Object(items) => items.iter().any(|i| expr_references_wfu_meta(&i.value)),
        Expr::Array(items) => items.iter().any(expr_references_wfu_meta),
        Expr::InList { expr, list, .. } => {
            expr_references_wfu_meta(expr) || list.iter().any(expr_references_wfu_meta)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
            ..
        } => {
            expr_references_wfu_meta(cond)
                || expr_references_wfu_meta(then_expr)
                || expr_references_wfu_meta(else_expr)
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            expr_references_wfu_meta(expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(expr_references_wfu_meta)
                        || expr_references_wfu_meta(&arm.value)
                })
                || default
                    .as_ref()
                    .is_some_and(|d| expr_references_wfu_meta(d))
        }
        // 保守兜底：未知变体（non_exhaustive）→ 回退全量 build。
        _ => true,
    }
}

/// One on-each **pipe** row (2026-08-25 q13a 列式化): score / entity meta
/// (for `_wfu_meta_*` fallback columns when the pipe schema carries them)
/// plus the coerced yield values in yield-plan order. A `None` value means
/// the optional input field was missing → cell omitted, same as the record
/// path's `#62` skip.
#[derive(Debug, Default)]
pub struct PipeEachRow {
    pub score: f64,
    pub entity_type: std::sync::Arc<str>,
    pub entity_id: String,
    pub values: Vec<Option<Value>>,
}

/// 中间管道行接收器（2026-08-25 pipe 写入分配足迹）。
///
/// executor **逐行回调**本 trait，实现方（wf-runtime 的 `PipeBatchStager`）直接
/// 装列——避开先物化全批 `Vec<PipeEachRow>`（每行一个 `values` Vec + 一个
/// `entity_id` String，实测 404 B/行）。`entity_id` / `values` 都是 executor 的
/// **可复用 scratch 借用**，实现方不得跨行持有。
///
/// 与 sink 路径（`execute_each_direct_batch(..., &mut AlertColumnBuilder, ...)`）
/// 同构：错误由 executor 记 `failed` 并继续下一行，不中断批次。
pub trait PipeRowSink {
    /// 装载一行。`values` 与 yield 字段顺序一一对应（`None` = 可选字段缺失）。
    /// `Err` = 本行装载失败（executor 记 failed）。
    fn push_pipe_row(
        &mut self,
        score: f64,
        entity_type: &str,
        entity_id: &str,
        values: &[Option<Value>],
        event_time_nanos: i64,
    ) -> Result<(), String>;
}

/// 对照/兼容实现：按行物化成 `PipeEachRow`（每行两次堆分配，与流式改造前
/// 等价）。仅用于 bench 对照与对拍测试，生产路径用 stager 直接实现。
impl PipeRowSink for Vec<PipeEachRow> {
    fn push_pipe_row(
        &mut self,
        score: f64,
        entity_type: &str,
        entity_id: &str,
        values: &[Option<Value>],
        _event_time_nanos: i64,
    ) -> Result<(), String> {
        self.push(PipeEachRow {
            score,
            entity_type: std::sync::Arc::from(entity_type),
            entity_id: entity_id.to_string(),
            values: values.to_vec(),
        });
        Ok(())
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

/// 表达式是否引用（裸名）let 变量——列式 mask/score 无 let 视图，非 yield
/// 表达式引用 let 变量会静默读空（失真）；只有 yield 的 let 引用经编译期
/// 内联展开（安全）。只匹配 `FieldRef::Simple`（let 以裸名注入 ctx，限定
/// 引用走窗口字段）。
fn expr_refs_let(expr: &Expr, let_names: &std::collections::HashSet<&str>) -> bool {
    match expr {
        Expr::Field(fr) => {
            matches!(fr, FieldRef::Simple(name) if let_names.contains(name.as_str()))
        }
        Expr::BinOp { left, right, .. } => {
            expr_refs_let(left, let_names) || expr_refs_let(right, let_names)
        }
        Expr::Neg(inner) | Expr::Not(inner) => expr_refs_let(inner, let_names),
        Expr::Array(items) => items.iter().any(|i| expr_refs_let(i, let_names)),
        Expr::InList {
            expr: inner, list, ..
        } => expr_refs_let(inner, let_names) || list.iter().any(|i| expr_refs_let(i, let_names)),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            expr_refs_let(cond, let_names)
                || expr_refs_let(then_expr, let_names)
                || expr_refs_let(else_expr, let_names)
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            expr_refs_let(expr, let_names)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(|p| expr_refs_let(p, let_names))
                        || expr_refs_let(&arm.value, let_names)
                })
                || default
                    .as_ref()
                    .is_some_and(|d| expr_refs_let(d, let_names))
        }
        Expr::Object(items) => items.iter().any(|it| expr_refs_let(&it.value, let_names)),
        Expr::FuncCall { args, .. } => args.iter().any(|a| expr_refs_let(a, let_names)),
        _ => false,
    }
}
