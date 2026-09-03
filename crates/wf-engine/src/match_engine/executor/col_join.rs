//! on-each 列式 join 富化 + 输出组装（2026-09-04 自 each_exec.rs 拆出）：
//! 单活 Snapshot join 的列式富化执行（`execute_each_direct_batch_columnar_join`）、
//! 输出行组装（`build_each_direct` / `build_each_alert[_with]` / pipe light 组）、
//! pipe 行与统计类型（`PipeEachRow`/`PipeRowSink`/`EachDirectBatchStats`）及
//! 相关标量原语（`join_cmp` / `expr_references_wfu_meta`）。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayAccessor, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef};

use crate::alert::{AlertColumnBuilder, AlertOrigin, EachRowCells, OutputRecord};
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::MACHINE_ID;
use crate::match_engine::cep::{
    CepStateMachine, Event, JoinKey, Value, WindowLookup, value_to_string, values_equal,
};
use crate::match_engine::event_bridge::{ColumnarEvent, JoinRow};

use super::super::RuleExecutor;
use super::super::YieldKind;
use super::super::alert::{EachWfxPrefix, build_each_wfx_id, format_nanos_utc, write_int64_value};
use super::super::eval::{
    YieldMeta, eval_entity_id, eval_score, eval_yield_expr_with_meta, with_yield_eval_scope,
};

use super::*;

impl RuleExecutor {
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

    pub(super) fn build_each_direct(
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

    pub(super) fn build_each_alert(
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
    pub(super) fn each_yield_meta<'a>(
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
    pub(super) fn each_yield_meta_light<'a>(
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
