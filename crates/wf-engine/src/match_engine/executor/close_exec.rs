use std::sync::Arc;

use smol_str::SmolStr;
use wf_lang::ast::{CloseMode, Expr, FieldRef};

use crate::alert::{AlertColumnBuilder, AlertOrigin, OutputRecord};
use crate::error::CoreResult;
use crate::match_engine::columnar::{CVec, cscalar_to_value};
use crate::match_engine::executor::StatsCloseBucket;
use crate::match_engine::match_engine::{
    CloseOutput, CloseReason, Event, StepData, Value, WindowLookup, eval_field_value,
    field_ref_name, value_to_string,
};

use super::EachDirectBatchStats;
use super::RuleExecutor;
use super::YieldKind;
use super::alert::{
    EntityIdCache, OriginArcs, WfxPrefixCache, build_summary, build_summary_from_labels,
    build_summary_split, build_wfx_id, format_nanos_utc, now_nanos,
};
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
            close_row_fields(close),
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
            close_row_fields(close),
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
    /// StringLit / plain Field, yields Lit / plain Field **or a General
    /// expression that only references plain fields**（fmt/strftime/count_char
    /// 等 q15-q19 detail——列式 close 对 General 走轻量 ctx 求值）。Joins are
    /// unsupported on this path yet — rules with joins fall back to the
    /// per-record join-enriched path (q4/q6 style). Field references to the
    /// synthetic `_step_*` / `_bind_*` ctx fields are rejected: the columnar
    /// resolver only reads keys / step labels / `field_values` / `bind_data`.
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
            general => Self::yield_general_columnar_safe(general),
        }) {
            return false;
        }
        if !self.live_joins.is_empty() {
            return false;
        }
        true
    }

    /// General yield（fmt/strftime/count_char 等）在列式 close 路径可安全求值:
    /// 表达式引用的全部字段都是普通字段（非 `_step_*`/`_bind_*` 合成字段、非
    /// Path、非空名）——Named 窄化的 `build_eval_context` 才会注入这些字段。
    /// 合成字段只在 all 分支注入, 求值会读到空 → 输出失真, 门控拒绝。
    fn yield_general_columnar_safe(expr: &Expr) -> bool {
        match expr {
            Expr::Field(fr) => {
                let n = field_ref_name(fr);
                !n.is_empty() && !n.starts_with('_') && !matches!(fr, FieldRef::Path { .. })
            }
            Expr::BinOp { left, right, .. } => {
                Self::yield_general_columnar_safe(left) && Self::yield_general_columnar_safe(right)
            }
            Expr::Neg(inner) | Expr::Not(inner) => Self::yield_general_columnar_safe(inner),
            Expr::Array(items) => items.iter().all(Self::yield_general_columnar_safe),
            Expr::InList {
                expr: inner, list, ..
            } => {
                Self::yield_general_columnar_safe(inner)
                    && list.iter().all(Self::yield_general_columnar_safe)
            }
            Expr::IfThenElse {
                cond,
                then_expr,
                else_expr,
            } => {
                Self::yield_general_columnar_safe(cond)
                    && Self::yield_general_columnar_safe(then_expr)
                    && Self::yield_general_columnar_safe(else_expr)
            }
            Expr::Object(items) => items
                .iter()
                .all(|it| Self::yield_general_columnar_safe(&it.value)),
            Expr::FuncCall { args, .. } => args.iter().all(Self::yield_general_columnar_safe),
            // Number/StringLit/Bool/SystemVar/WfuMeta/PresetParam: 读字面量/
            // YieldMeta/参数体, 无 ctx 字段访问。
            _ => true,
        }
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
        // 记录已注册 const 的字段（层 2 Part B：主循环跳过这些字段的逐行
        // stage——commit gap-fill 常量，字节一致）。
        let mut const_yields: std::collections::HashSet<&str> = std::collections::HashSet::new();
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
                    const_yields.insert(field.name.as_str());
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

        let mut wfx_ids: Vec<SmolStr> = Vec::with_capacity(closes.len());
        let mut scores: Vec<f64> = Vec::with_capacity(closes.len());
        let mut entity_ids: Vec<SmolStr> = Vec::with_capacity(closes.len());
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
        // 窗口级 fired_at 缓存: 同一窗口所有 close 的 watermark_nanos 相同——
        // format_nanos_utc（civil_from_days + 24B 分配）每窗算一次（q19 单窗
        // 百万级 close, 省百万次）。
        let mut fired_at_cache: Option<(i64, String)> = None;
        // wfx_id 前缀缓存（P6）: q19 同桶 top-10 条共享
        // rule/scope_key/fired_at/labels，FNV 前缀 state 缓存续算。
        let mut wfx_cache: Option<WfxPrefixCache> = None;
        // entity 连续缓存（通用 EntityIdCache）: 同 scope_key 相邻 close 复用
        // entity_id（q19 同桶 top-10 条共享 auction, 免每 close 一次
        // resolve + value_to_string）。
        let mut entity_cache = EntityIdCache::new();

        // 层 1（2026-08-25）：列式批级 General yield cell（fmt/strftime/
        // count_char——close_batch_prepare 物化引用字段为 Arrow 列并编译求值，
        // 与 each 列式路径同一编译入口）。槽位 None（无 General / 编译失败 /
        // 类型不一致）→ 循环内逐行解释回退。
        let prepared = self.close_batch_prepare(closes);

        // origin/reason Arc 预建（P7, 2026-08-26）: 静态字符串免每 close 两次
        // Arc::from 堆分配（二分定位 ~22ns/entry），循环内 Arc::clone。
        let origin_arcs = OriginArcs::new();

        'close: for (row_idx, close) in closes.iter().enumerate() {
            if !is_qualified(close) {
                stats.rejected += 1;
                continue;
            }
            let origin = AlertOrigin::Close {
                reason: close.close_reason,
            };
            let fired_at = match &fired_at_cache {
                Some((w, s)) if *w == close.watermark_nanos => s.clone(),
                _ => {
                    let s = format_nanos_utc(close.watermark_nanos);
                    fired_at_cache = Some((close.watermark_nanos, s.clone()));
                    s
                }
            };
            // entity（连续缓存：q19 同桶 top-10 条共享 scope_key，复用字符串
            // 免每 close 一次 resolve + value_to_string）
            let entity_id: String = if let Some(s) = entity_const {
                s.to_string()
            } else {
                let key = close.scope_key.as_slice();
                entity_cache.get_or(key, || {
                    // eval_entity_id → eval_yield_expr falls back to an empty
                    // string when the field is absent (never errors) — mirror that
                    // instead of failing the close.
                    resolve_close_field(close, keys, entity_field_name.unwrap_or(""))
                        .map(|v| value_to_string(&v))
                        .unwrap_or_default()
                })
            };
            // wfx_id：前缀缓存（P6）——q19 每桶 top-10 条共享
            // rule/scope_key/fired_at/labels，FNV 前缀 state 缓存续算，
            // 每 close 只 hash 变化的 measure + origin。
            let wfx_id = match &wfx_cache {
                Some(c)
                    if c.prefix_matches(
                        &close.scope_key,
                        &fired_at,
                        &close.event_step_data,
                        &close.close_step_data,
                    ) =>
                {
                    c.finish(&close.event_step_data, &close.close_step_data, &origin)
                }
                _ => {
                    let cache = WfxPrefixCache::build(
                        &self.plan.name,
                        &close.scope_key,
                        &fired_at,
                        &close.event_step_data,
                        &close.close_step_data,
                    );
                    let id = cache.finish(&close.event_step_data, &close.close_step_data, &origin);
                    wfx_cache = Some(cache);
                    id
                }
            };
            let summary = build_summary_split(
                &self.plan.name,
                keys,
                &close.scope_key,
                &close.event_step_data,
                &close.close_step_data,
                &origin,
            );

            // Field yields: resolve each from keys / field_values / bind.
            // General yields（fmt/strftime/count_char——门控保证只引用普通字段）:
            // 轻量 ctx 求值——build_eval_context 的 Named 窄化只注入输出引用
            // 字段, 跳过 per-record 路径的 combine_step_plans / annotate /
            // joins / where / OutputRecord（q15-q19 detail 的 fmt 批量列式化）。
            let mut ctx: Option<Event> = None;
            for (field_idx, (field, (name, field_type))) in self
                .plan
                .yield_plan
                .fields
                .iter()
                .zip(yield_specs.iter())
                .enumerate()
            {
                let value = match &field.value {
                    // const 列（Lit yield）已在 execute 顶部注册 + 校验——跳过
                    // 逐行 stage（commit 对缺 staged cell 的行 gap-fill 常量，
                    // 字节一致；2026-08-25 层 2 Part B：省 Lit 字段的
                    // coerce/export/staged push，q12/q15-q19 通用）。
                    // 防御：非 const 注册的 Lit（理论不可达）仍走取值 + stage。
                    Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_)
                        if const_yields.contains(field.name.as_str()) =>
                    {
                        continue;
                    }
                    Expr::Number(n) => Value::Number(*n),
                    Expr::StringLit(s) => Value::Str(s.clone().into()),
                    Expr::Bool(b) => Value::Bool(*b),
                    Expr::Field(_) => {
                        resolve_close_field(close, keys, field_ref_name_of(&field.value))
                            .unwrap_or_else(|| Value::Str(String::new().into()))
                    }
                    general => {
                        // 列式批级 cell：命中直接取（null 行 → 空串，同解释路径
                        // None→""）；槽位 None → 逐行回退（轻量 ctx 求值）。
                        match prepared
                            .general_cvecs
                            .get(field_idx)
                            .and_then(|c| c.as_ref())
                        {
                            Some(cvec) => match cvec.scalar_at(row_idx) {
                                Some(s) => cscalar_to_value(&s),
                                None => Value::Str(SmolStr::default()),
                            },
                            None => {
                                let ctx = ctx.get_or_insert_with(|| {
                                    let all_step_data = combine_step_data(close);
                                    build_eval_context(
                                        keys,
                                        &close.scope_key,
                                        &all_step_data,
                                        &close.bind_data,
                                        &[],
                                        None,
                                        &self.close_ctx_fields,
                                        close_row_fields(close),
                                    )
                                });
                                let yield_meta = YieldMeta {
                                    score: Some(score_const),
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
                                with_yield_eval_scope(|| {
                                    eval_yield_expr_with_meta(general, ctx, yield_meta)
                                })
                                .expect("eval_yield_expr_with_meta never returns None")
                            }
                        }
                    }
                };
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

            wfx_ids.push(SmolStr::from(wfx_id));
            scores.push(score_const);
            entity_ids.push(SmolStr::from(entity_id));
            fired_ats.push(fired_at);
            origins.push(Arc::clone(origin_arcs.origin(close.close_reason)));
            close_reasons.push(Arc::clone(origin_arcs.close_reason(close.close_reason)));
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
    // stats last/top 行字段引用（2026-08-26 q18 close 内存）: Named 下 field_values
    // 不注入行字段, 由 CloseOutput.row_fields 携带——按需 value_at（零拷贝）,
    // 与 field_values 注入语义一致（值相同）。
    if let (Some(rf), Some(names)) = (&close.row_fields, &close.row_field_names)
        && let Some(pos) = names.iter().position(|n| n == name)
    {
        return rf.value_at(pos);
    }
    for bd in &close.bind_data {
        if let Some(v) = bd.field_values.get(name).and_then(|vs| vs.last()) {
            return Some(v.clone());
        }
    }
    None
}

/// 2026-08-26 q18 close 内存: CloseOutput 行字段引用（Named 下 field_values 不
/// 注入行字段, 装载/ctx 按需读）。返回 (RowFields Arc, 列名 Arc) 或 None。
fn close_row_fields(
    close: &CloseOutput,
) -> Option<(
    &std::sync::Arc<crate::match_engine::executor::RowFields>,
    &std::sync::Arc<Vec<String>>,
)> {
    match (&close.row_fields, &close.row_field_names) {
        (Some(rf), Some(names)) => Some((rf, names)),
        _ => None,
    }
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

/// 列式 close 的 General yield 批级求值状态（层 1，2026-08-25）：
/// [`RuleExecutor::close_batch_prepare`] 把一批 `CloseOutput` 引用字段物化为
/// Arrow 列 → `ColumnarBatch` 视图 → 编译 General yield（fmt/strftime/
/// count_char 等）→ `eval_vec` 批量 cell。槽位按 **yield 字段位置** 索引
/// （与 `yield_plan.fields` 对齐；Lit/Field 为 `None`）；`None` = 无 General /
/// 编译失败 / 字段类型不一致 → 逐行解释回退（与 each 路径同款契约）。
#[derive(Default)]
pub(crate) struct CloseBatchVecs {
    pub(crate) general_cvecs: Vec<Option<CVec>>,
}

impl RuleExecutor {
    /// Compile + batch-evaluate the columnar close General-yield state for one
    /// `closes` batch（窗口 close 一次调用，语义 = 解释路径的
    /// `build_eval_context`（Named 窄化/All）+ `eval_yield_expr_with_meta`）。
    ///
    /// 只物化 General 表达式实际引用的普通字段 + 键名（ctx 恒注入键）；缺失
    /// 字段 → 不建列 → `ColumnarBatch` 解析为 Null ColKind → null cell →
    /// 空串，与解释路径 None→"" 一致。Number→Float64 / Str→Utf8 / Bool→
    /// Boolean 列（`cscalar_to_value` 还原为原 `Value`，渲染字节一致）。
    pub(crate) fn close_batch_prepare(&self, closes: &[CloseOutput]) -> CloseBatchVecs {
        let keys: &[FieldRef] = &self.plan.match_plan.keys;
        self.close_batch_prepare_with(closes.len(), |row, name| {
            resolve_close_field(&closes[row], keys, name)
        })
    }

    /// 物化源参数化的批级准备（2026-08-26 q18 stats 直写）: stats close 直装载
    /// 用（输入 StatsCloseBucket 而非 CloseOutput, 免 CloseOutput 构建）。语义
    /// 与 [`Self::close_batch_prepare`] 一致（键名恒注入 + General 引用字段）。
    pub(crate) fn close_batch_prepare_with<F>(&self, n: usize, resolve: F) -> CloseBatchVecs
    where
        F: Fn(usize, &str) -> Option<Value>,
    {
        let slots = self.plan.yield_plan.fields.len();
        if n == 0 {
            return CloseBatchVecs {
                general_cvecs: (0..slots).map(|_| None).collect(),
            };
        }
        // 1. 引用字段集 = 键名（ctx 无条件注入）∪ General yield 引用的普通字段
        //    （close 编译不内联 let → 非内联收集，保持一致）
        let ref_fields = self.yield_ref_fields(false);
        if ref_fields.is_empty() {
            return CloseBatchVecs {
                general_cvecs: (0..slots).map(|_| None).collect(),
            };
        }
        // 2. 统一物化器 + 槽位编译（层 2 收口，`RuleExecutor::compile_general_slots`）；
        //    物化失败（类型不一致/结构化值）→ 整批回退逐行（保守）。close 传空
        //    lets：解释 close 路径（build_eval_context）无 let 视图，内联会分叉。
        CloseBatchVecs {
            general_cvecs: self.compile_general_slots(&ref_fields, n, resolve, &[]),
        }
    }

    /// stats close 列式直写（2026-08-26 q18 close 内存 v3）: 输入为
    /// [`StatsCloseBucket`] 批次（stats_task 流式分批产出）——**不构建
    /// CloseOutput**（省 per-record 的 rule_name String / scope_key Vec /
    /// StepData 深结构 ≈ 500B × 千万条分配, allocator 保留致 RSS 虚高）。
    /// 每桶轻量 StepData 一次（label 克隆, field_values 空 HashMap 零分配）,
    /// k 记录只更新 measure_value; Field/General yield 从 scope_key /
    /// row_fields 按需读。输出与 `execute_close_direct_batch_columnar` 字节
    /// 一致（对拍契约）。门控: 调用方须 `close_plan_columnar_safe()`（列式
    /// 前置, 无逐行 fallback——门控保证 general 编译成功）。
    pub fn execute_stats_close_batch_columnar(
        &self,
        buckets: &[StatsCloseBucket],
        labels: &[String],
        row_names: Option<&std::sync::Arc<Vec<String>>>,
        builder: &mut AlertColumnBuilder,
        emit_time_nanos: i64,
        window_end_nanos: i64,
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

        // 常量 yield 注册（与 execute_close_direct_batch_columnar 相同）
        let mut const_yields: std::collections::HashSet<&str> = std::collections::HashSet::new();
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
                        stats.failed += stats_bucket_rows(buckets);
                        return stats;
                    }
                    const_yields.insert(field.name.as_str());
                }
                Some(Err(e)) => {
                    log::warn!("alert export error: {e}");
                    stats.failed += stats_bucket_rows(buckets);
                    return stats;
                }
                None => {
                    if let Err(e) = builder.register_yield_column(name, None) {
                        log::warn!("alert export error: {e}");
                        stats.failed += stats_bucket_rows(buckets);
                        return stats;
                    }
                }
            }
        }

        // 行数 = Σ桶 n_records（last=1, top=N）; 全局行号 → (桶, 记录) 映射
        let mut row_index: Vec<(usize, usize)> = Vec::new();
        let mut total = 0usize;
        for (bi, b) in buckets.iter().enumerate() {
            let n = b.measures.iter().map(Vec::len).max().unwrap_or(1);
            for k in 0..n {
                row_index.push((bi, k));
            }
            total += n;
        }
        builder.reserve_rows(total);

        // prepare: General yield 物化源 = (桶, 记录) → 字段值
        let prepared = self.close_batch_prepare_with(total, |row, name| {
            let (bi, k) = row_index[row];
            resolve_stats_bucket_field(&buckets[bi], k, keys, labels, row_names, name)
        });

        let mut wfx_ids: Vec<SmolStr> = Vec::with_capacity(total);
        let mut scores: Vec<f64> = Vec::with_capacity(total);
        let mut entity_ids: Vec<SmolStr> = Vec::with_capacity(total);
        let mut fired_ats: Vec<String> = Vec::with_capacity(total);
        let mut origins: Vec<Arc<str>> = Vec::with_capacity(total);
        let mut close_reasons: Vec<Arc<str>> = Vec::with_capacity(total);
        let mut summaries: Vec<Arc<str>> = Vec::with_capacity(total);
        let mut staged_rows: Vec<
            Vec<(
                usize,
                wp_model_core::model::DataType,
                wp_model_core::model::Value,
            )>,
        > = Vec::with_capacity(total);
        // wfx_id 前缀缓存（P6 补齐, 2026-08-27）: 同桶 top-N 条目共享
        // rule/scope/fired_at/labels 前缀——v4 去 StepData 时误丢（旧 CloseOutput
        // 路径有缓存）, 逐条目全量重 hash 的回归缺口。labels 迭代器变体零 StepData。
        let mut wfx_cache: Option<WfxPrefixCache> = None;
        let mut entity_cache = EntityIdCache::new();
        let origin_arcs = OriginArcs::new();
        // fired_at 每窗一次（window_end_nanos 常量——wfx 前缀/entity 连续缓存依赖;
        // 单次 flush 内恒同, 无需缓存）。
        let fired_at = format_nanos_utc(window_end_nanos);
        let mut global_row = 0usize;

        for bucket in buckets.iter() {
            let n_records = bucket.measures.iter().map(Vec::len).max().unwrap_or(1);
            let scope_values = stats_scope_key_to_values(&bucket.key);
            for k in 0..n_records {
                // (label, measure_value) 惰性迭代器——零 StepData 构造（2026-08-26
                // v4: 删每桶 4 个 StepData ≈ 4G 分配）。
                let step_iter = labels.iter().enumerate().map(|(i, label)| {
                    let mv = bucket
                        .measures
                        .get(i)
                        .and_then(|m| m.get(usize::min(k, m.len().saturating_sub(1))))
                        .map_or(0.0, |e| e.measure_value);
                    (Some(label.as_str()), mv)
                });
                // measures 单独迭代（finish_from_labels 与缓存的 labels 交错）。
                let measures = labels.iter().enumerate().map(|(i, _)| {
                    bucket
                        .measures
                        .get(i)
                        .and_then(|m| m.get(usize::min(k, m.len().saturating_sub(1))))
                        .map_or(0.0, |e| e.measure_value)
                });
                let origin = AlertOrigin::Close {
                    reason: CloseReason::Timeout,
                };
                let entity_id: String = if let Some(s) = entity_const {
                    s.to_string()
                } else {
                    let key = scope_values.as_slice();
                    entity_cache.get_or(key, || {
                        resolve_stats_bucket_field(
                            bucket,
                            k,
                            keys,
                            labels,
                            row_names,
                            entity_field_name.unwrap_or(""),
                        )
                        .map(|v| value_to_string(&v))
                        .unwrap_or_default()
                    })
                };
                // wfx_id: 前缀命中则只 hash 变化的 measure + origin（与
                // `wfx_prefix_cache_matches_split` 同款判定; 换桶/窗自动重建）。
                let wfx_id = match &wfx_cache {
                    Some(c) if c.prefix_matches_labels(
                        &scope_values,
                        &fired_at,
                        labels.iter().map(|l| Some(l.as_str())),
                    ) => c.finish_from_labels(measures, &origin),
                    _ => {
                        let c = WfxPrefixCache::build_from_labels(
                            &self.plan.name,
                            &scope_values,
                            &fired_at,
                            labels.iter().map(|l| Some(l.as_str())),
                        );
                        let id = c.finish_from_labels(measures, &origin);
                        wfx_cache = Some(c);
                        id
                    }
                };
                let summary = build_summary_from_labels(
                    &self.plan.name,
                    keys,
                    &scope_values,
                    step_iter,
                    &origin,
                );
                for (field_idx, (field, (name, field_type))) in self
                    .plan
                    .yield_plan
                    .fields
                    .iter()
                    .zip(yield_specs.iter())
                    .enumerate()
                {
                    let value = match &field.value {
                        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_)
                            if const_yields.contains(field.name.as_str()) =>
                        {
                            continue;
                        }
                        Expr::Number(n) => Value::Number(*n),
                        Expr::StringLit(s) => Value::Str(s.clone().into()),
                        Expr::Bool(b) => Value::Bool(*b),
                        Expr::Field(_) => resolve_stats_bucket_field(
                            bucket,
                            k,
                            keys,
                            labels,
                            row_names,
                            field_ref_name_of(&field.value),
                        )
                        .unwrap_or_else(|| Value::Str(String::new().into())),
                        general => match prepared
                            .general_cvecs
                            .get(field_idx)
                            .and_then(|c| c.as_ref())
                        {
                            Some(cvec) => match cvec.scalar_at(global_row) {
                                Some(s) => cscalar_to_value(&s),
                                None => Value::Str(SmolStr::default()),
                            },
                            None => {
                                // 门控保证 compiled 成功; 防御性空串
                                let _ = general;
                                Value::Str(SmolStr::default())
                            }
                        },
                    };
                    match RuleExecutor::coerce_yield_field_value_with(
                        name,
                        field_type.as_ref(),
                        value,
                    ) {
                        Ok(Some(v)) => {
                            if let Err(e) = builder.stage_yield_cell(name, field_type.as_ref(), &v)
                            {
                                log::warn!("alert export error: {e}");
                                stats.failed += 1;
                                builder.take_staged();
                                continue;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            log::warn!("alert export error: {e}");
                            stats.failed += 1;
                            builder.take_staged();
                            continue;
                        }
                    }
                }
                staged_rows.push(builder.take_staged());

                wfx_ids.push(SmolStr::from(wfx_id));
                scores.push(score_const);
                entity_ids.push(SmolStr::from(entity_id));
                fired_ats.push(fired_at.clone());
                origins.push(Arc::clone(origin_arcs.origin(CloseReason::Timeout)));
                close_reasons.push(Arc::clone(origin_arcs.close_reason(CloseReason::Timeout)));
                summaries.push(Arc::from(summary));
                stats.appended += 1;
                global_row += 1;
            }
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

/// Σ桶行数（stats 直写失败计数用）。
fn stats_bucket_rows(buckets: &[StatsCloseBucket]) -> usize {
    buckets
        .iter()
        .map(|b| b.measures.iter().map(Vec::len).max().unwrap_or(1))
        .sum()
}

/// 桶键拆解为字段值列表（Pair 先序展开, 顺序与 keys 一致; stats 直写局部版）。
fn stats_scope_key_to_values(key: &crate::match_engine::match_engine::ScopeKey) -> Vec<Value> {
    match key {
        crate::match_engine::match_engine::ScopeKey::Empty => vec![],
        crate::match_engine::match_engine::ScopeKey::Int(i) => vec![Value::Number(*i as f64)],
        crate::match_engine::match_engine::ScopeKey::Float(b) => {
            vec![Value::Number(f64::from_bits(*b))]
        }
        crate::match_engine::match_engine::ScopeKey::Str(s) => vec![Value::Str(s.clone())],
        crate::match_engine::match_engine::ScopeKey::Pair(a, b) => {
            let mut v = stats_scope_key_to_values(a);
            v.extend(stats_scope_key_to_values(b));
            v
        }
    }
}

/// stats 桶字段解析（列式直写用; 语义 = `resolve_close_field` 对 stats 桶数据）:
/// 键字段（scope_key）→ 度量 label → measure_value → 行字段（row_fields value_at）。
fn resolve_stats_bucket_field(
    bucket: &StatsCloseBucket,
    record: usize,
    keys: &[FieldRef],
    labels: &[String],
    row_names: Option<&std::sync::Arc<Vec<String>>>,
    name: &str,
) -> Option<Value> {
    // 1. 键字段（scope_key 先序展开）
    let scope_values = stats_scope_key_to_values(&bucket.key);
    for (i, k) in keys.iter().enumerate() {
        if field_ref_name(k) == name {
            return scope_values.get(i).cloned();
        }
    }
    // 2. 度量 label → measure_value（首个匹配; 与 resolve_close_field 的
    //    label → Number(measure_value) 同语义）
    for (i, label) in labels.iter().enumerate() {
        if label == name {
            let mv = bucket
                .measures
                .get(i)
                .and_then(|m| m.get(usize::min(record, m.len().saturating_sub(1))))
                .map_or(0.0, |e| e.measure_value);
            return Some(Value::Number(mv));
        }
    }
    // 3. 行字段（row_fields → value_at, 与 CloseOutput.row_fields 同口径）
    if let (Some(names), Some(rf)) = (
        row_names,
        bucket.measures.iter().find_map(|m| {
            m.get(usize::min(record, m.len().saturating_sub(1)))
                .and_then(|e| e.row_fields.as_ref())
        }),
    ) && let Some(pos) = names.iter().position(|n| n == name)
    {
        return rf.value_at(pos);
    }
    None
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
