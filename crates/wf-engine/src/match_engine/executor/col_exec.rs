//! on-each 列式批执行器（2026-09-04 自 each_exec.rs 拆出）：
//! 批级列式状态（`EachBatchVecs` / `each_batch_prepare` / `event_batch_prepare`）与
//! 列式直发批执行（`execute_each_direct_batch_columnar[_with]`）、pipe 段
//! （`execute_each_pipe_batch_columnar`）。列式 join 富化见 `col_join.rs`。

use std::sync::Arc;

use arrow::array::{Array, ArrayAccessor, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{Expr, FieldRef};

use crate::alert::{AlertColumnBuilder, AlertOrigin};
use crate::error::CoreResult;
use crate::match_engine::cep::{Event, FieldSource, Value, field_ref_name, value_to_string};
use crate::match_engine::columnar::{
    CVec, ColumnarBatch, compile_guard, compile_yield_cvec, cscalar_to_value,
};
use crate::match_engine::event_bridge::ColumnarEvent;

use super::super::RuleExecutor;
use super::super::YieldKind;
use super::super::alert::{EachWfxPrefix, format_nanos_utc, write_int64_value};
use super::super::close_exec::CloseBatchVecs;
use super::super::eval::{eval_entity_id, eval_score, eval_yield_expr_with_meta};

use super::*;

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
}
