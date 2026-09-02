//! P3：前瞻 deferred join 执行（join-family-design §5.2/§5.3）。
//!
//! 规则声明 `join ... within [lo, hi] on ... emit at <expr>` 时，整条规则走
//! deferred 输出路径：驱动事件到达时**不即时输出**，而是挂起为一个
//! [`DeferredPending`] 实例（expiry = `emit at` 求值）；当事件时间 watermark
//! ≥ expiry 时到期评估：`asof_candidates` → 区间过滤 → reduce/exists → 输出。
//!
//! 挂起/到期求值都放在 engine 层（`deferred_pending_for` / `execute_deferred_join`），
//! rule_task 只负责队列管理与到期调度。

use std::cmp::Ordering;

use wf_lang::ast::{FieldRef, ReduceMeasure, TieSpec};
use wf_lang::plan::JoinCondPlan;

use crate::alert::{AlertOrigin, OutputRecord};
use crate::error::CoreResult;
use crate::match_engine::match_engine::{
    EngineHashMap, Event, Value, WindowLookup, eval_expr, field_ref_name,
};
use crate::match_engine::{FieldSource, JoinRow};
use crate::time::normalize_epoch_timestamp_float_nanos;

use super::RuleExecutor;
use super::context::{enrich_join_row_bare, eval_interval_bound, in_interval, row_matches_conds};

/// deferred 驱动（左）行的读取门面。
///
/// P4 gap-1（q4/q8/q9）：驱动事件从 [`Event`]（每行 HashMap 物化）降为列式视图
/// [`JoinRow::Columnar`]（Arc batch + 行号，同批共享），挂起队列的内存与注入期
/// 分配随之下降。`Event` 变体保留为回退（无原始 batch / 有 let 绑定时的解释路径
/// ——见 [`RuleExecutor::deferred_pending_for`]）。
#[derive(Clone)]
pub enum DeferredLeft {
    /// 列式视图（无 let 规则的默认形态）。
    Columnar(JoinRow),
    /// 预物化 Event（有 let 绑定 / 无原始 batch 的回退形态）。
    Event(Event),
}

impl FieldSource for DeferredLeft {
    fn field_value(&self, name: &str) -> Option<Value> {
        match self {
            DeferredLeft::Columnar(row) => {
                // 投影遮蔽：与 `batch_to_events_filtered` 的裁剪语义字节一致——
                // 投影外的字段读 None（而非返回原始列值），对齐 eager 路径的
                // 裁剪 Event（字段不在 map 里 → 引用读空）。
                if let JoinRow::Columnar { projection, .. } = row
                    && projection.as_ref().is_some_and(|p| !p.contains(name))
                {
                    return None;
                }
                row.field_value(name)
            }
            DeferredLeft::Event(ev) => ev.fields.get(name).cloned(),
        }
    }

    fn field_names(&self) -> Vec<&str> {
        match self {
            DeferredLeft::Columnar(row) => row.field_names(),
            DeferredLeft::Event(ev) => ev.fields.keys().map(|k| k.as_str()).collect(),
        }
    }

    fn to_event(&self) -> Event {
        match self {
            DeferredLeft::Columnar(row) => {
                let mut fields = EngineHashMap::default();
                for name in row.field_names() {
                    if let Some(value) = row.field_value(name) {
                        fields.insert(name.into(), value);
                    }
                }
                Event { fields }
            }
            DeferredLeft::Event(ev) => ev.clone(),
        }
    }
}

impl std::fmt::Debug for DeferredLeft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeferredLeft::Columnar(row) => write!(
                f,
                "DeferredLeft::Columnar(fields={})",
                row.field_names().len()
            ),
            DeferredLeft::Event(_) => write!(f, "DeferredLeft::Event(..)"),
        }
    }
}

/// 一个挂起的 deferred join 实例：驱动（左）行 + 预计算的区间与触发点。
///
/// 界与触发点基于左行字段（`a.dateTime`/`a.expires`/`bucket_end(...)`）与左事件时间，
/// 在挂起时一次性求值；到期时只需查右窗 + 区间过滤 + reduce。
#[derive(Debug, Clone)]
pub struct DeferredPending {
    /// 右窗 join 键字段（`conds.first().right` 的叶子名）。
    pub key_field: String,
    /// 左行 join 键值。
    pub key: Value,
    /// within 下/上界（绝对纳秒）。
    pub lo_ns: i64,
    pub hi_ns: i64,
    /// 开闭记号（`<` 前缀 = 开区间）。
    pub lo_open: bool,
    pub hi_open: bool,
    /// `emit at` 触发点 = 到期 watermark（也是输出 `fired_at`）。
    pub expiry_nanos: i64,
    /// 左行字段（entity/score/yield/join 条件复核/界求值来源）。无 let 规则为
    /// 列式视图（`JoinRow::Columnar`），有 let 为预物化 Event（见
    /// [`DeferredLeft`]）。
    pub left: DeferredLeft,
}

impl RuleExecutor {
    /// 驱动事件到达时的挂起求值：提取 join 键 + 区间界 + `emit at` 触发点。
    ///
    /// 返回 `None` 表示该事件无法挂起（无 emit_at / 无 within / 键或界求值失败）。
    /// `left` 为列式视图时且规则有 let 绑定 → 自动物化 + 注入绑定（回退语义）；
    /// 无 let 时保持列式（零物化）。
    pub fn deferred_pending_for(
        &self,
        join_idx: usize,
        left: &DeferredLeft,
        event_time_nanos: i64,
    ) -> Option<DeferredPending> {
        let join = self.plan.joins.get(join_idx)?;
        let emit_at = join.emit_at.as_ref()?;
        let wspec = join.within.as_ref()?;
        // `let` 绑定注入：列式视图 + 有 let → 物化 Event 后注入（字节一致由
        // `to_event`（batch_to_events 语义）保证）；无 let 保持列式零物化。
        // `Event` 变体 = 旧路径 clone + apply_lets。
        let left = match left {
            DeferredLeft::Columnar(_) if self.plan.lets.is_empty() => left.clone(),
            other => {
                let mut ctx = other.to_event();
                self.apply_lets(&mut ctx);
                DeferredLeft::Event(ctx)
            }
        };
        let (key_field, key) = first_join_key_local(&left, &join.conds)?;
        let lo_ns = eval_interval_bound(&wspec.lo, &left, event_time_nanos)?;
        let hi_ns = eval_interval_bound(&wspec.hi, &left, event_time_nanos)?;
        let expiry_nanos = eval_expr(emit_at, &left).and_then(|v| match v {
            Value::Number(n) => normalize_epoch_timestamp_float_nanos(n),
            _ => None,
        })?;
        Some(DeferredPending {
            key_field,
            key,
            lo_ns,
            hi_ns,
            lo_open: wspec.lo.open,
            hi_open: wspec.hi.open,
            expiry_nanos,
            left,
        })
    }

    /// 到期评估：右窗候选 → 区间过滤 + 条件复核 → reduce/exists → 输出。
    ///
    /// 空集（Q9 无 bid 的 auction）→ `Ok(None)` 不输出；post-join `where` 拒绝同理。
    /// `fired_at` = 到期 watermark（`pending.expiry_nanos`），origin = `deferred`。
    pub fn execute_deferred_join(
        &self,
        join_idx: usize,
        pending: &DeferredPending,
        windows: &dyn WindowLookup,
        emit_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        match self.evaluate_deferred_join(join_idx, pending, windows)? {
            Some(out_ctx) => {
                self.build_deferred_output(&out_ctx, pending.expiry_nanos, emit_time_nanos)
            }
            None => Ok(None),
        }
    }

    /// 评估 deferred join（2026-08-26 q4a 中间窗轻量化拆分）：查询 + 区间过滤 +
    /// 条件复核 + 归约/存在 + 富化，返回**输出 ctx**（`Event`）。`None` = 无匹配。
    /// 输出物化（build）由调用方按目标选择：sink → [`Self::build_deferred_output`]
    /// （全量告警字段），中间窗 → 轻量 build（`build_each_alert_pipe`，跳过
    /// wfx_id/fired_at/summary——中间窗消费者按列读不需要）。
    pub fn evaluate_deferred_join(
        &self,
        join_idx: usize,
        pending: &DeferredPending,
        windows: &dyn WindowLookup,
    ) -> CoreResult<Option<Event>> {
        let Some(join) = self.plan.joins.get(join_idx) else {
            return Ok(None);
        };
        let Some(rows) =
            windows.asof_candidates(&join.right_window, &pending.key_field, &pending.key)
        else {
            return Ok(None);
        };
        // 条件复核冗余跳过（2026-08-26 q4a/q9）：`asof_candidates` 已按
        // `pending.key_field` + `pending.key` 过滤候选（索引/扫描同口径，
        // 候选行 key 字段值 == key）；`pending.key` 来自 `first_join_key_local`
        // （ctx[cond.left 字段名]），`pending.key_field` 即 `cond.right` 字段名。
        // 单条件且右字段 == key_field → 条件复核恒真，跳过 `row_matches_conds`
        // （每候选一次 Event 字段查找+比较）。多条件（key 只来自第一个 cond，
        // 其余条件必须复核）/右字段非 key 字段 → 保留复核。
        let cond_recheck_redundant =
            join.conds.len() == 1 && pending.key_field == field_ref_name(&join.conds[0].right);
        // 区间过滤 + 全部 join 条件复核（复刻 find_matching_row 语义）
        let matched: Vec<(i64, crate::match_engine::JoinRow)> = rows
            .into_iter()
            .filter(|(ts, row)| {
                in_interval(
                    *ts,
                    pending.lo_ns,
                    pending.hi_ns,
                    pending.lo_open,
                    pending.hi_open,
                ) && (cond_recheck_redundant || row_matches_conds(row, &join.conds, &pending.left))
            })
            .collect();
        if matched.is_empty() {
            return Ok(None);
        }

        // 输出 ctx = 左行 + 富化/注入（列式视图在此物化——仅到期评估的实例
        // 才物化，挂起队列全程保持列式）
        let mut out_ctx = pending.left.to_event();
        match &join.reduce {
            Some(rc) => {
                let Some(row) = select_reduce_row(matched, &rc.measure) else {
                    return Ok(None);
                };
                // 胜出行的字段以裸名富化（`winner.bidder` 编译成 Path{alias:"winner",
                // segments:["bidder"]}，eval_field_value 丢弃 alias、把 segments[0]
                // 当根字段名读 `fields["bidder"]`）。
                // 2026-08-26：bare 注入（省 qualified 死数据）——deferred 表达式
                // 对 Qualified 引用也读裸名，qualified 键不可达。
                enrich_join_row_bare(&mut out_ctx, &row);
                // `as label`：归约整行以裸键 object value 注入（review R2）。
                // 仅当规则真能读到该 object（裸 `winner` / `field_ref_name` 命中
                // 标签名，见 plan_reduce_label_reads）时才物化——`label.field`
                // 形态读的是上面 enrich 的裸名，object 纯冗余（2026-08-24
                // deferred_bench：eval-maxrow 1353 → 1113 ns/op）。
                if let Some(label) = &rc.label
                    && self.reduce_label_reads.needs(label)
                {
                    inject_reduce_label(&mut out_ctx, label, &row);
                }
            }
            None => {
                // 纯存在（Q8）：区间内最早行富化
                let row = matched
                    .iter()
                    .min_by_key(|(ts, _)| *ts)
                    .map(|(_, r)| r.clone());
                let Some(row) = row else {
                    return Ok(None);
                };
                enrich_join_row_bare(&mut out_ctx, &row);
            }
        }

        // Post-join `where`：strict——false/None 抑制输出
        if !self.where_ok(&out_ctx) {
            return Ok(None);
        }

        Ok(Some(out_ctx))
    }

    /// 全量 build（sink 目标）：评估 ctx → 完整 OutputRecord（告警字段全构建）。
    pub fn build_deferred_output(
        &self,
        out_ctx: &Event,
        expiry_nanos: i64,
        emit_time_nanos: i64,
    ) -> CoreResult<Option<OutputRecord>> {
        self.build_each_alert_with(
            out_ctx,
            expiry_nanos,
            AlertOrigin::Deferred,
            &[],
            emit_time_nanos,
        )
    }
}

/// 按 key 过滤前的 join 键提取（deferred 挂起时读取驱动行视图）。
fn first_join_key_local(left: &dyn FieldSource, conds: &[JoinCondPlan]) -> Option<(String, Value)> {
    let cond = conds.first()?;
    let left_name = field_ref_name(&cond.left);
    let val = left.field_value(left_name)?;
    Some((field_ref_name(&cond.right).to_string(), val))
}

/// 从匹配集选行（设计 §4.2）：
/// - `maxrow(field)`：field 值最大的行（`tie` 破平，仍并列按右窗 ts 大者，确定性）；
/// - `minrow(field)`：field 值最小的行；
/// - `last(field)`：右窗时间（ts）最新的行（v1 语义；field 参数保留）；
/// - `top(N, field)`：按 field 降序取前 N，返回首行。
pub(crate) fn select_reduce_row(
    rows: Vec<(i64, crate::match_engine::JoinRow)>,
    measure: &ReduceMeasure,
) -> Option<crate::match_engine::JoinRow> {
    match measure {
        ReduceMeasure::Maxrow { field, tie } => rows
            .into_iter()
            .max_by(|a, b| reduce_row_cmp(a, b, field, tie.as_ref()))
            .map(|(_, r)| r),
        ReduceMeasure::Minrow { field, tie } => rows
            .into_iter()
            .min_by(|a, b| reduce_row_cmp_min(a, b, field, tie.as_ref()))
            .map(|(_, r)| r),
        ReduceMeasure::Last { .. } => rows.into_iter().max_by_key(|(ts, _)| *ts).map(|(_, r)| r),
        ReduceMeasure::Top { n, field } => {
            let mut sorted: Vec<(i64, crate::match_engine::JoinRow)> = rows;
            // 降序（max 优先）排序后取前 N
            sorted.sort_by(|a, b| reduce_row_cmp(b, a, field, None));
            sorted.truncate((*n).max(1) as usize);
            sorted.into_iter().next().map(|(_, r)| r)
        }
    }
}

/// `max_by`/`min_by` 的自然序比较器：主键升序（`max_by` 取大者 / `min_by` 取小者）；
/// 同主键按 tie 破平（asc 小者胜、desc 大者胜）；仍并列按右窗 ts（新者胜）。
/// 全部比较稳定（相等时保持候选顺序，确定性破平，设计 §9 风险 2）。
fn reduce_row_cmp(
    a: &(i64, crate::match_engine::JoinRow),
    b: &(i64, crate::match_engine::JoinRow),
    field: &FieldRef,
    tie: Option<&TieSpec>,
) -> Ordering {
    let name = field_ref_name(field);
    let ord = cmp_row_num(&a.1, &b.1, name); // 升序：主键大者 max
    if ord != Ordering::Equal {
        return ord;
    }
    if let Some(t) = tie {
        let tname = field_ref_name(&t.field);
        let tord = cmp_row_num(&a.1, &b.1, tname);
        // asc：小者胜 → 反转（小者排后，max 取到）；desc：大者胜 → 升序
        return if t.desc { tord } else { tord.reverse() };
    }
    a.0.cmp(&b.0) // ts 升序：新者 max（确定性破平）
}

/// `minrow` 方向：主键小者胜（`min_by` 直接取小者）；同主键 tie 与主键同向
///（`tie asc` = 小者胜，等价 SQL `ORDER BY price ASC, dateTime ASC`）；ts 旧者胜。
fn reduce_row_cmp_min(
    a: &(i64, crate::match_engine::JoinRow),
    b: &(i64, crate::match_engine::JoinRow),
    field: &FieldRef,
    tie: Option<&TieSpec>,
) -> Ordering {
    let name = field_ref_name(field);
    let ord = cmp_row_num(&a.1, &b.1, name); // 升序：主键小者 min
    if ord != Ordering::Equal {
        return ord;
    }
    if let Some(t) = tie {
        let tname = field_ref_name(&t.field);
        let tord = cmp_row_num(&a.1, &b.1, tname);
        // asc：小者胜 → 升序（小者排前，min 取到）；desc：大者胜 → 反转
        return if t.desc { tord.reverse() } else { tord };
    }
    a.0.cmp(&b.0) // ts 升序：旧者 min（确定性破平）
}

/// 两行的 field 数值比较（升序）。缺失/非数值视为最小。
fn cmp_row_num(
    a: &crate::match_engine::JoinRow,
    b: &crate::match_engine::JoinRow,
    field: &str,
) -> Ordering {
    match (row_num(a, field), row_num(b, field)) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn row_num(row: &crate::match_engine::JoinRow, field: &str) -> Option<f64> {
    match row.field_value(field)? {
        Value::Number(n) => Some(n),
        _ => None,
    }
}

/// `as label`：归约整行以裸键 object value 注入 eval context。
fn inject_reduce_label(ctx: &mut Event, label: &str, row: &crate::match_engine::JoinRow) {
    let mut map = EngineHashMap::default();
    for name in row.field_names() {
        if let Some(v) = row.field_value(name) {
            map.insert(name.into(), v);
        }
    }
    ctx.fields.insert(label.into(), Value::Object(map));
}
