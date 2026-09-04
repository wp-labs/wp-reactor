//! `RuleExecutor` 方法实现整块下沉（struct 留 `executor/mod.rs`，fanout/
//! rule_task 先例）+ 其独占模块级原语：guard 缓存 schema 指纹、General-yield
//! 列式门控/字段收集（collect_general_plain_fields）、yield 值强制转换链
//! （coerce_yield_value 家族）与 bind filter 行式求值（passes_bind_filter）。
//! 这些原语只被本模块 impl 消费；`yield_general_columnar_safe` 另经
//! executor/mod.rs `pub(crate) use` 转发供 `match_exec` 引用。
//! 可见性：私有 fn 保持私有；方法级 `pub`/`pub(crate)` 面随 impl 原样保留。

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use arrow::array::BooleanArray;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceRawErr, ToStructError};

use wf_config::OutputConfig;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{JoinPlan, RulePlan};
use wf_lang::{BaseType, FieldType};

use super::alert::build_summary;
use super::context::CloseCtxFields;
use super::each_exec::parse_each_join_columnar;
use super::eval::{eval_bool_expr, eval_bool_expr_with_lookup};
use super::plan_analysis::{
    compute_live_joins, compute_match_ctx_free, plan_close_ctx_fields, plan_reduce_label_reads,
};
use super::{OutputStatic, RuleExecutor, RuleExecutorOptions, YieldKind};

use crate::alert::AlertOrigin;
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::cep::{Event, FieldSource, Value, WindowLookup, field_ref_name};
use crate::match_engine::columnar::{
    CVec, ColumnarBatch, GuardMasks, compile_guard, compile_yield_cvec, eval_compiled_guard,
};
use crate::time::normalize_epoch_timestamp_float_nanos;

/// Schema fingerprint for the compiled-guard cache: field name + data type +
/// metadata (the metadata marks structured JSON-array columns, which change
/// the compiled [`ColKind`]). Two batches of the same window share a schema, so
/// the fingerprint is stable across them; any drift recompiles.
fn guard_schema_fingerprint(batch: &RecordBatch) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for f in batch.schema().fields() {
        f.name().hash(&mut h);
        format!("{:?}", f.data_type()).hash(&mut h);
        for (k, v) in f.metadata() {
            k.hash(&mut h);
            v.hash(&mut h);
        }
    }
    h.finish()
}

impl RuleExecutor {
    pub fn new(plan: RulePlan) -> Self {
        Self::new_with_options(plan, RuleExecutorOptions::default())
    }

    pub fn new_with_yield_field_types(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
    ) -> Self {
        Self::new_with_options(
            plan,
            RuleExecutorOptions {
                yield_field_types,
                output: OutputConfig::default(),
            },
        )
    }

    pub fn new_with_yield_field_types_and_output(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
        output: OutputConfig,
    ) -> Self {
        Self::new_with_options(
            plan,
            RuleExecutorOptions {
                yield_field_types,
                output,
            },
        )
    }

    pub fn new_with_options(plan: RulePlan, options: RuleExecutorOptions) -> Self {
        // 临时诊断（WFU_RULE_CENSUS=1）：每规则一条 census——trigger_event_needed
        // 决定 fire 是否物化 trigger Event（to_event + 结构化列 JSON 解析）。
        // 与 metrics 的每规则 matches_total/emitted_total 外联可得每规则 fire 量。
        if std::env::var_os("WFU_RULE_CENSUS").is_some() {
            let kind = if plan.each_plan.is_some() {
                "each"
            } else if plan.stats_plan.is_some() {
                "stats"
            } else {
                "match"
            };
            eprintln!(
                "WFU_CENSUS rule={} kind={} trigger_event_needed={} nkeys={} nbind_filters={} njoins={} nlets={} nsteps={}",
                plan.name,
                kind,
                plan.match_plan.trigger_event_needed,
                plan.match_plan.keys.len(),
                plan.binds.iter().filter(|b| b.filter.is_some()).count(),
                plan.joins.len(),
                plan.lets.len(),
                plan.match_plan.event_steps.len(),
            );
        }
        let live_joins = compute_live_joins(&plan);
        let each_join_plan = parse_each_join_columnar(&plan, &live_joins);
        let bind_filters = plan
            .binds
            .iter()
            .map(|b| (b.alias.clone(), b.filter.clone()))
            .collect();
        // Precompute plan-level output constants (see `OutputStatic`). The
        // yield field types map comes from runtime schema knowledge, which is
        // exactly what `new_with_options` receives.
        let yield_specs: Vec<(Arc<str>, Option<FieldType>)> = plan
            .yield_plan
            .fields
            .iter()
            .map(|field| {
                (
                    Arc::from(field.name.as_str()),
                    options.yield_field_types.get(&field.name).cloned(),
                )
            })
            .collect();
        let typed_fields: Vec<(Arc<str>, FieldType)> = yield_specs
            .iter()
            .filter_map(|(name, field_type)| {
                field_type
                    .clone()
                    .map(|field_type| (Arc::clone(name), field_type))
            })
            .collect();
        let each_summary = plan.each_plan.as_ref().map(|_| {
            Arc::from(build_summary(
                &plan.name,
                &[],
                &[],
                &[],
                &AlertOrigin::Event,
            ))
        });
        // Precompute per-field yield specialization (literal value / direct
        // field lookup / full interpreter) and the constant score, so the
        // per-record hot path never re-classifies expressions.
        let yield_kinds: Vec<YieldKind> = plan
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
        let score_const = match &plan.score_plan.expr {
            Expr::Number(n) => Some(n.clamp(0.0, 100.0)),
            _ => None,
        };
        let close_ctx_fields = plan_close_ctx_fields(&plan);
        let reduce_label_reads = plan_reduce_label_reads(&plan);
        let match_ctx_free = compute_match_ctx_free(&plan, &live_joins, &yield_kinds);
        // M1：规则级 fire 投影（Named 窄化集）——`to_event` 只物化 ctx 读的字段；
        // All（无法静态窄化）→ None（回退窗口 materialize_fields）。非 match 规则
        // （each/stats 不消费 RowEvent 投影）无影响。
        let fire_trigger_projection: Option<std::sync::Arc<std::collections::HashSet<String>>> =
            match &close_ctx_fields {
                CloseCtxFields::Named(set) if !set.is_empty() => {
                    Some(std::sync::Arc::new(set.clone()))
                }
                _ => None,
            };
        Self {
            output_static: OutputStatic {
                rule_name: Arc::from(plan.name.as_str()),
                entity_type: Arc::from(plan.entity_plan.entity_type.as_str()),
                yield_target: Arc::from(plan.yield_plan.target.as_str()),
                yield_specs: Arc::from(yield_specs),
                yield_field_types: Arc::from(typed_fields),
                yield_kinds: Arc::from(yield_kinds),
                score_const,
                each_summary,
                each_origin: Arc::from(AlertOrigin::Event.as_str()),
                each_close_reason: Arc::from(""),
                match_ctx_free,
            },
            plan,
            each_join_plan,
            live_joins,
            yield_field_types: options.yield_field_types,
            output: options.output,
            bind_filters,
            emit_time_cache: Mutex::new((0, Arc::from(""))),
            close_ctx_fields,
            fire_trigger_projection,
            reduce_label_reads,
            compiled_guards: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// 规则级 fire 投影（M1）：match 行 `to_event` 只物化 ctx 读取的 Named 字段。
    /// `None` = 无法窄化（All）/非 match → 调用方回退窗口 materialize_fields。
    pub fn fire_trigger_projection(
        &self,
    ) -> Option<std::sync::Arc<std::collections::HashSet<String>>> {
        self.fire_trigger_projection.clone()
    }

    /// Formatted emit time for `nanos`, cached: consecutive calls with the
    /// same nanos (the batch-shared wall clock) return the same `Arc<str>`
    /// with no re-formatting.
    pub(crate) fn cached_emit_time(&self, nanos: i64) -> Arc<str> {
        let mut cache = self.emit_time_cache.lock().unwrap();
        if cache.0 != nanos || cache.1.is_empty() {
            *cache = (nanos, Arc::from(super::alert::format_nanos_utc(nanos)));
        }
        Arc::clone(&cache.1)
    }

    pub fn plan(&self) -> &RulePlan {
        &self.plan
    }

    /// Live (output-referenced) joins after dead-join elimination. The runtime
    /// uses this to route between the join-free and join columnar each paths.
    pub fn live_joins(&self) -> &[JoinPlan] {
        &self.live_joins
    }

    /// Whether this rule can run the columnar join-enrichment each path
    /// (q20 等：each + 单 Snapshot join + 受限 where/输出形状)。
    pub fn each_join_columnar_ready(&self) -> bool {
        self.each_join_plan.is_some()
    }

    /// Post-join `where` filter check: evaluated after joins enrich the event
    /// context, before alert construction. Strict semantics — `false` or a
    /// missing field (`None`) suppresses the output, aligning INNER JOIN
    /// miss-drop (q3 state filter / q20 category filter).
    pub(crate) fn where_ok(&self, ctx: &Event) -> bool {
        match &self.plan.r#where {
            None => true,
            Some(expr) => matches!(eval_bool_expr(expr, ctx), Some(true)),
        }
    }

    /// Precomputed plan-level output constants (see [`OutputStatic`]).
    pub(crate) fn output_static(&self) -> &OutputStatic {
        &self.output_static
    }

    /// Plan-constant yield target as the precomputed `Arc` — used by the
    /// runtime's direct-write on-each emit (plan C2) to locate the columnar
    /// builder without re-deriving or re-allocating the target string.
    pub fn static_yield_target(&self) -> &Arc<str> {
        &self.output_static().yield_target
    }

    /// close ctx 是否 All（保守全字段构建——L3 聚合/窗口访问表达式用）。
    /// stats close 据此决定行字段注入方式（Named 窄化下多 last 度量共享同一
    /// [`RowFields`] Arc, 只需首个度量注入; All 下每度量独立注入, 保
    /// `_step_i_field_*` 完整性）。
    pub fn close_ctx_is_all(&self) -> bool {
        matches!(self.close_ctx_fields, CloseCtxFields::All)
    }

    pub(crate) fn output_config(&self) -> &OutputConfig {
        &self.output
    }

    /// Coerce a yield field value against a precomputed type (from
    /// `output_static().yield_specs`) — avoids the per-field `HashMap`
    /// lookup on the hot path.
    pub(crate) fn coerce_yield_field_value_with(
        name: &str,
        field_type: Option<&FieldType>,
        value: Value,
    ) -> CoreResult<Option<Value>> {
        let Some(field_type) = field_type else {
            return Ok(Some(value));
        };
        coerce_yield_value(name, field_type, value)
    }

    pub(crate) fn build_machine_id(&self, machine_id: &str) -> Arc<str> {
        if machine_id.is_empty() {
            // 热路径（q6 等无自定义 machine_id）：直接 Arc 复用 rule 名——
            // OutputRecord.machine_id 由 String 改 Arc<str> 后免每事件 String
            // clone + 堆分配（sample: String::clone 9+7+5）。
            Arc::clone(&self.output_static().rule_name)
        } else {
            Arc::from(machine_id)
        }
    }

    pub(crate) fn build_scope_key(
        &self,
        keys: &[wf_lang::ast::FieldRef],
        scope_values: &[crate::match_engine::cep::Value],
    ) -> Arc<str> {
        use std::fmt::Write as _;
        // 单 String 一次写入（旧实现每 key 一个 format! String + Vec + join
        // → 每事件 2 次分配 + Vec 分配；q6 每事件 emit 路径的分配热点之一）。
        let mut out = String::with_capacity(scope_values.len() * 16);
        for (i, (k, v)) in keys.iter().zip(scope_values.iter()).enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(
                out,
                "{}={}",
                crate::match_engine::cep::field_ref_name(k),
                crate::match_engine::cep::value_to_string(v)
            );
        }
        Arc::from(out)
    }

    pub fn event_matches_alias(
        &self,
        alias: &str,
        event: &dyn FieldSource,
        windows: Option<&dyn WindowLookup>,
    ) -> bool {
        passes_bind_filter(self.bind_filter(alias), event, windows)
    }

    /// The bind filter for `alias`, if any.
    ///
    /// Few binds: a linear scan is cheaper than hashing the alias. Many binds:
    /// the precomputed map keeps this O(1) instead of O(binds) per event.
    /// Measured crossover: the map wins from ~24 binds (24: 5.1M vs 5.8M q/s;
    /// 16: linear still 1.3x faster).
    fn bind_filter(&self, alias: &str) -> Option<&Expr> {
        if self.plan.binds.len() <= 24 {
            self.plan
                .binds
                .iter()
                .find(|b| b.alias == alias)
                .and_then(|b| b.filter.as_ref())
        } else {
            self.bind_filters.get(alias).and_then(|f| f.as_ref())
        }
    }

    /// 该 alias 是否声明了 bind filter。列式掩码缺失（`None`）时区分「无
    /// filter → 全放行」与「非列式 filter → 需逐行解释」（gap-4 2026-09-02：
    /// columnar_each 块对非列式 bind filter 逐行 `event_matches_alias`，不
    /// 再静默全放行丢过滤子集）。
    pub fn bind_filter_present(&self, alias: &str) -> bool {
        self.bind_filter(alias).is_some()
    }

    /// Columnar evaluation of `alias`'s bind filter over a whole batch.
    ///
    /// Returns `None` when there is no filter (nothing to reject) or the filter
    /// is not columnar (caller falls back to per-event [`Self::event_matches_alias`]).
    /// `Some(mask)` has one boolean per row; `false` = bind filter rejected that
    /// row, matching `event_matches_alias`'s `false`.
    pub fn bind_filter_columnar_mask(
        &self,
        alias: &str,
        batch: &RecordBatch,
    ) -> Option<BooleanArray> {
        let filter = self.bind_filter(alias)?;
        if !wf_lang::columnar::expr_is_columnar(filter) {
            return None;
        }
        let view = ColumnarBatch::from_all_fields(batch);
        Some(self.guard_mask(&format!("bind:{alias}"), filter, &view, batch))
    }

    /// Compiled-guard cache lookup-or-compile: the compiled [`ColumnExpr`] tree
    /// is batch-independent (leaf [`ColRef`]s are projection slot + type tag),
    /// so identical `(site, schema)` calls reuse the tree instead of rebuilding
    /// it — and re-parsing / recompiling its literal constants — per batch.
    /// The schema fingerprint in the key means a schema-drifted batch recompiles
    /// rather than evaluating a stale tree.
    fn guard_mask(
        &self,
        site: &str,
        filter: &Expr,
        view: &ColumnarBatch<'_>,
        batch: &RecordBatch,
    ) -> BooleanArray {
        let key = (site.to_string(), guard_schema_fingerprint(batch));
        let mut cache = self
            .compiled_guards
            .lock()
            .expect("compiled-guard cache poisoned");
        if let Some(plan) = cache.get(&key) {
            return eval_compiled_guard(plan, view);
        }
        let Some(plan) = compile_guard(filter, view) else {
            // Uncompilable (a shape outside the gate, or an invalid literal
            // constant) → all rows miss, matching `eval_guard_columnar`'s
            // fallback; not cached (the failure is per-call cheap).
            return BooleanArray::from(vec![false; view.num_rows()]);
        };
        let plan_ref = cache.entry(key).or_insert(plan);
        eval_compiled_guard(plan_ref, view)
    }

    /// Whether every bind of `window` can be evaluated columnarly — every
    /// filter is absent (nothing to reject) or [`expr_is_columnar`]. This is
    /// the rule-task's local check for safe deferred (columnar) materialization
    /// when a raw batch is present: a non-columnar filter would otherwise be
    /// skipped entirely (the deferred path's missing-mask fallback accepts all
    /// rows), silently corrupting the filtered subset.
    pub fn bind_filters_columnar_safe(&self, window: &str) -> bool {
        self.plan
            .binds
            .iter()
            .filter(|b| b.window == window)
            .all(|b| {
                b.filter
                    .as_ref()
                    .is_none_or(wf_lang::columnar::expr_is_columnar)
            })
    }

    /// Columnar evaluation of the `on each` filter over a whole batch.
    ///
    /// Same `None` / `Some(mask)` contract as [`Self::bind_filter_columnar_mask`],
    /// but for `plan.each_plan.filter`.
    pub fn each_filter_columnar_mask(&self, batch: &RecordBatch) -> Option<BooleanArray> {
        let filter = self.plan.each_plan.as_ref()?.filter.as_ref()?;
        if !wf_lang::columnar::expr_is_columnar(filter) {
            return None;
        }
        let view = ColumnarBatch::from_all_fields(batch);
        Some(self.guard_mask("each", filter, &view, batch))
    }

    /// Columnar branch-guard masks for the three per-event guard sites:
    ///
    /// - event steps, keyed `(event_step_idx, branch_idx)`;
    /// - close-step accumulation guards, keyed `(close_step_idx, branch_idx)`;
    /// - seq negation guards, keyed `(neg_idx, 0)`.
    ///
    /// Non-columnar / absent guards are simply not inserted, so the state
    /// machine falls back to interpreted evaluation for those branches.
    pub fn branch_guard_masks(&self, batch: &RecordBatch) -> GuardMasks {
        // 无列式 guard（qradar c/s 等无 guard 规则）：跳过 `ColumnarBatch` 视图
        // 构建——`from_all_fields` 是 O(fields²) 的 schema 线性解析，每批每规则
        // 一次，对无 guard 规则纯浪费（2026-08-31 惰性化；guardless 规则直接
        // 空掩码，状态机回退解释求值，语义不变）。
        if !self.has_columnar_guard() {
            return GuardMasks::default();
        }
        let view = ColumnarBatch::from_all_fields(batch);
        let mut masks = GuardMasks::default();
        for (step_idx, step) in self.plan.match_plan.event_steps.iter().enumerate() {
            for (branch_idx, branch) in step.branches.iter().enumerate() {
                if let Some(guard) = &branch.guard
                    && wf_lang::columnar::expr_is_columnar(guard)
                {
                    let site = format!("event:{step_idx}:{branch_idx}");
                    masks.insert_event(
                        step_idx,
                        branch_idx,
                        self.guard_mask(&site, guard, &view, batch),
                    );
                }
            }
        }
        for (step_idx, step) in self.plan.match_plan.close_steps.iter().enumerate() {
            for (branch_idx, branch) in step.branches.iter().enumerate() {
                if let Some(guard) = &branch.guard
                    && wf_lang::columnar::expr_is_columnar(guard)
                {
                    let site = format!("close:{step_idx}:{branch_idx}");
                    masks.insert_close(
                        step_idx,
                        branch_idx,
                        self.guard_mask(&site, guard, &view, batch),
                    );
                }
            }
        }
        if let Some(seq) = &self.plan.match_plan.seq {
            // Negation steps only, in the same order `SeqRuntime::build` emits
            // them (so `neg_idx` lines up with `meta.negs`).
            let mut neg_idx = 0usize;
            for step in &seq.steps {
                if step.neg {
                    if let Some(guard) = &step.branch.guard
                        && wf_lang::columnar::expr_is_columnar(guard)
                    {
                        let site = format!("neg:{neg_idx}");
                        masks.insert_neg(neg_idx, 0, self.guard_mask(&site, guard, &view, batch));
                    }
                    neg_idx += 1;
                }
            }
        }
        masks
    }

    /// 规则是否含任意可列式求值的 guard（event/close/seq-neg 三处站点）。
    fn has_columnar_guard(&self) -> bool {
        let plan = &self.plan.match_plan;
        plan.event_steps
            .iter()
            .chain(plan.close_steps.iter())
            .flat_map(|s| s.branches.iter())
            .any(|b| {
                b.guard
                    .as_ref()
                    .is_some_and(wf_lang::columnar::expr_is_columnar)
            })
            || plan.seq.as_ref().is_some_and(|seq| {
                seq.steps.iter().any(|s| {
                    s.neg
                        && s.branch
                            .guard
                            .as_ref()
                            .is_some_and(wf_lang::columnar::expr_is_columnar)
                })
            })
    }

    pub fn is_aux_bind_alias(&self, alias: &str) -> bool {
        !self
            .plan
            .match_plan
            .event_steps
            .iter()
            .chain(self.plan.match_plan.close_steps.iter())
            .flat_map(|step| step.branches.iter())
            .any(|branch| branch.source == alias)
    }

    /// 引用字段集 = 键名（ctx 恒注入）∪ General yield **内联 let 后**引用的普通
    /// 字段（层 2 收口，close/match/行式批共用；去重保序）。内联展开保证物化
    /// 视图包含 let RHS 引用的 schema 字段（q22：`let parts = split(url)` → 需
    /// 物化 url，而非 let 名 parts）。
    ///
    /// `inline` 必须与 `compile_general_slots` 的 `lets` 参数一致：each/行式批
    /// 传 true（编译也内联）；close/match 传 false（编译传空 lets——解释路径
    /// 无 let 视图，内联会产生值 vs 解释空串的分叉；此时 let RHS 字段不收集，
    /// 物化只含 yield 直接引用的字段，编译对 let 名读 Null → 空串，一致）。
    pub(crate) fn yield_ref_fields(&self, inline: bool) -> Vec<String> {
        let mut ref_fields: Vec<String> = Vec::new();
        for k in &self.plan.match_plan.keys {
            push_uniq(&mut ref_fields, field_ref_name(k));
        }
        for field in &self.plan.yield_plan.fields {
            if !matches!(
                field.value,
                Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) | Expr::Field(_)
            ) {
                if inline && !self.plan.lets.is_empty() {
                    let inlined = crate::match_engine::columnar::inline_lets(
                        &field.value,
                        &self.plan.lets,
                        &mut Vec::new(),
                    );
                    collect_general_plain_fields(&inlined, &mut ref_fields);
                } else {
                    collect_general_plain_fields(&field.value, &mut ref_fields);
                }
            }
        }
        ref_fields
    }

    /// 统一列式 General yield 槽位编译（层 2 收口）：引用字段物化（统一
    /// `materialize_fields`）→ `ColumnarBatch` 视图 → 逐 yield 字段
    /// `compile_yield_cvec`。槽位按 yield 字段位置索引（Lit/Field → None）；
    /// 物化失败（类型不一致/结构化）/ 编译失败 → None → 调用方逐行回退。
    pub(crate) fn compile_general_slots<F>(
        &self,
        ref_fields: &[String],
        n: usize,
        resolve: F,
        lets: &[wf_lang::plan::LetPlan],
    ) -> Vec<Option<CVec>>
    where
        F: FnMut(usize, &str) -> Option<Value>,
    {
        let slots = self.plan.yield_plan.fields.len();
        if n == 0 || ref_fields.is_empty() {
            return (0..slots).map(|_| None).collect();
        }
        let Some((schema_fields, arrays)) =
            crate::match_engine::columnar::materialize_fields(ref_fields, n, resolve)
        else {
            return (0..slots).map(|_| None).collect();
        };
        match RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), arrays) {
            Ok(batch) => {
                let view = ColumnarBatch::from_all_fields(&batch);
                self.plan
                    .yield_plan
                    .fields
                    .iter()
                    .map(|field| compile_yield_cvec(field, &view, n, lets))
                    .collect()
            }
            Err(_) => (0..slots).map(|_| None).collect(),
        }
    }
}

/// General yield 表达式引用的普通字段名（非空、非 `_` 合成、非 Path）——与
/// `yield_general_columnar_safe` 的门控形状一致（门控已保证只引用这些字段，
/// 这里静态收集供物化用；去重保序）。close/match/行式批共用（层 2 收口）。
pub(crate) fn collect_general_plain_fields(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Field(fr) => push_uniq(out, field_ref_name(fr)),
        Expr::BinOp { left, right, .. } => {
            collect_general_plain_fields(left, out);
            collect_general_plain_fields(right, out);
        }
        Expr::Neg(inner) | Expr::Not(inner) => collect_general_plain_fields(inner, out),
        Expr::Array(items) => {
            for item in items {
                collect_general_plain_fields(item, out);
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            collect_general_plain_fields(inner, out);
            for item in list {
                collect_general_plain_fields(item, out);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_general_plain_fields(cond, out);
            collect_general_plain_fields(then_expr, out);
            collect_general_plain_fields(else_expr, out);
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            collect_general_plain_fields(expr, out);
            for arm in arms {
                for pattern in &arm.patterns {
                    collect_general_plain_fields(pattern, out);
                }
                collect_general_plain_fields(&arm.value, out);
            }
            if let Some(d) = default {
                collect_general_plain_fields(d, out);
            }
        }
        Expr::Object(items) => {
            for it in items {
                collect_general_plain_fields(&it.value, out);
            }
        }
        Expr::FuncCall { args, .. } => {
            for a in args {
                collect_general_plain_fields(a, out);
            }
        }
        _ => {}
    }
}

pub(crate) fn push_uniq(v: &mut Vec<String>, name: &str) {
    if !name.is_empty() && !v.iter().any(|f| f == name) {
        v.push(name.to_string());
    }
}

/// General yield（fmt/strftime/count_char 等）在列式 close/match 路径可安全求值:
/// 表达式引用的全部字段都是普通字段（非 `_step_*`/`_bind_*` 合成字段、非 Path、
/// 非空名）——Named 窄化的 `build_eval_context` 才会注入这些字段。合成字段只
/// 在 all 分支注入, 求值会读到空 → 输出失真, 门控拒绝。close/match 门控共用
/// （层 2 收口）。
pub(crate) fn yield_general_columnar_safe(expr: &Expr) -> bool {
    match expr {
        Expr::Field(fr) => {
            let n = field_ref_name(fr);
            !n.is_empty() && !n.starts_with('_') && !matches!(fr, FieldRef::Path { .. })
        }
        Expr::BinOp { left, right, .. } => {
            yield_general_columnar_safe(left) && yield_general_columnar_safe(right)
        }
        Expr::Neg(inner) | Expr::Not(inner) => yield_general_columnar_safe(inner),
        Expr::Array(items) => items.iter().all(yield_general_columnar_safe),
        Expr::InList {
            expr: inner, list, ..
        } => yield_general_columnar_safe(inner) && list.iter().all(yield_general_columnar_safe),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            yield_general_columnar_safe(cond)
                && yield_general_columnar_safe(then_expr)
                && yield_general_columnar_safe(else_expr)
        }
        Expr::Object(items) => items
            .iter()
            .all(|it| yield_general_columnar_safe(&it.value)),
        Expr::FuncCall { args, .. } => args.iter().all(yield_general_columnar_safe),
        // Number/StringLit/Bool/SystemVar/WfuMeta/PresetParam: 读字面量/
        // YieldMeta/参数体, 无 ctx 字段访问。
        _ => true,
    }
}

fn coerce_yield_value(
    name: &str,
    field_type: &FieldType,
    value: Value,
) -> CoreResult<Option<Value>> {
    // A yield expression referencing a missing input field evaluates to the
    // empty-string fallback (see `eval_yield_expr_with_meta`). For targets that
    // can never be a valid empty string, treat it as an absent/optional field:
    // omit it from the output instead of failing the whole record
    // (wp-labs/warp-fusion#62). Explicit NaN/Infinity values still fail below.
    if matches!(&value, Value::Str(s) if s.is_empty())
        && !matches!(field_type, FieldType::Base(BaseType::Chars))
    {
        return Ok(None);
    }
    match field_type {
        FieldType::Base(base_type) => coerce_yield_base_value(name, base_type, value).map(Some),
        FieldType::Array(_) | FieldType::ArrayAny => match value {
            Value::Array(_) => Ok(Some(value)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an array value"))
                .err(),
        },
        FieldType::Object => match value {
            Value::Object(_) => Ok(Some(value)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an object value"))
                .err(),
        },
    }
}

fn coerce_yield_base_value(name: &str, base_type: &BaseType, value: Value) -> CoreResult<Value> {
    match base_type {
        BaseType::Chars => match value {
            // Already a string: pass through without re-rendering — saves one
            // `String` allocation per cell on the hot path and is
            // byte-identical to `render_yield_value_as_string`.
            Value::Str(_) => Ok(value),
            _ => render_yield_value_as_string(value).map(|s| Value::Str(s.into())),
        },
        BaseType::Digit => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 => Ok(Value::Number(n)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} expects an integer-compatible number"
                ))
                .err(),
        },
        BaseType::Float => match value {
            Value::Number(n) if n.is_finite() => Ok(Value::Number(n)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects a finite number"))
                .err(),
        },
        BaseType::Bool => match value {
            Value::Bool(_) => Ok(value),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects a boolean value"))
                .err(),
        },
        BaseType::Time => coerce_yield_time_value(name, value),
        BaseType::Ip => match value {
            Value::Str(text) => {
                IpAddr::from_str(&text).source_raw_err(
                    CoreReason::DataFormat,
                    format!("yield field {name:?} has invalid ip literal {text:?}"),
                )?;
                Ok(Value::Str(text))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an ip string"))
                .err(),
        },
        BaseType::Hex => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 && n >= 0.0 => {
                Ok(Value::Number(n))
            }
            Value::Str(text) => {
                let normalized = text
                    .strip_prefix("0x")
                    .or_else(|| text.strip_prefix("0X"))
                    .unwrap_or(&text);
                u128::from_str_radix(normalized, 16).source_raw_err(
                    CoreReason::DataFormat,
                    format!("yield field {name:?} has invalid hex literal {text:?}"),
                )?;
                Ok(Value::Str(text))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} expects a hex string or non-negative integer"
                ))
                .err(),
        },
    }
}

fn coerce_yield_time_value(name: &str, value: Value) -> CoreResult<Value> {
    match value {
        Value::Number(n) => {
            normalize_epoch_timestamp_float_nanos(n).ok_or_else(|| {
                orion_error::StructError::from(CoreReason::DataFormat).with_detail(format!(
                    "yield field {name:?} expects a valid epoch timestamp"
                ))
            })?;
            Ok(Value::Number(n))
        }
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail(format!(
                "yield field {name:?} expects an explicit time expression or epoch timestamp"
            ))
            .err(),
    }
}

fn render_yield_value_as_string(value: Value) -> CoreResult<String> {
    match value {
        Value::Str(s) => Ok(s.to_string()),
        Value::Number(n) if n.is_finite() => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(&yield_value_to_json(&value)?)
            .source_raw_err(CoreReason::DataFormat, "serialize structured yield value"),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("yield string conversion requires finite numeric values")
            .err(),
    }
}

fn yield_value_to_json(value: &Value) -> CoreResult<serde_json::Value> {
    match value {
        Value::Number(n) if n.is_finite() => Ok(serde_json::Value::from(*n)),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
        Value::Str(s) => Ok(serde_json::Value::from(s.as_str())),
        Value::Bool(b) => Ok(serde_json::Value::from(*b)),
        Value::Array(items) => Ok(serde_json::Value::Array(
            items
                .iter()
                .map(yield_value_to_json)
                .collect::<CoreResult<Vec<_>>>()?,
        )),
        Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), yield_value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

fn passes_bind_filter(
    filter: Option<&Expr>,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
) -> bool {
    match filter.and_then(|expr| eval_bool_expr_with_lookup(expr, event, windows)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}
