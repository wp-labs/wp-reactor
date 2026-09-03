//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet};

mod masks;
pub use masks::StatsMaskCache;
mod accum;
pub use accum::{DistinctKey, DistinctSet, StatsAccum};
pub(crate) use accum::{NumericAccum, NumericKind, NumericSoA, NumericSoALayout, TopEntry};
mod state;
pub use state::{StatsBucketAccs, StatsWindowState};
// StatsBucket 仅 stats_soa_bench 经 executor:: 转发消费（lib 无生产引用）——
// re-export 链保留，避免在 lib cfg 下误报 unused。
#[allow(unused_imports)]
pub use state::StatsBucket;
use state::{SPILL_DRAIN_CHUNK, SpillCreateSpec, vec_to_bucket_accs};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use crate::match_engine::cep::{Event, ScopeKey, field_ref_name};
use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::spill::SpillStore;
use crate::match_engine::{EngineHashMap, Value};
use crate::window::scope_key_columnar;
use crate::window::scope_key_from_column;
use wf_cep::rows::{RowFieldLayout, RowFields};

/// 执行器: 消费行/批次, 按 StatsPlan 归并, 窗口 close 时产出度量值。
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsExecutor {
    pub plan: StatsPlan,
    /// 窗口状态（桶表; 空键规则仅 Empty 桶）。
    pub window: StatsWindowState,
    /// 当前窗口的过期上界（水印推进, 触发 close）。
    pub watermark_nanos: i64,
    /// 去重后的 where 表达式（相同条件共享一次求值——q15 9 个度量 where → 3
    /// 个唯一条件; 行式实现的关键优化: 每行 1 次 Event 构建 + n_unique 次 eval）。
    unique_wheres: Vec<Expr>,
    /// 每度量对应 `unique_wheres` 的索引; `None` = 无条件度量（恒通过）。
    measure_where: Vec<Option<usize>>,
    /// P5 紧凑化: 子集字段名的**确定排序**（executor 构造时从传入子集排序
    /// 派生; None = 全列, 列序在提取时确定）。行字段列数组按此列序存储。
    row_field_names: Option<std::sync::Arc<Vec<String>>>,
    /// 每度量字段在行字段列数组中的位置（预计算, 热路径免字符串查找;
    /// last/top 且字段在子集内 → Some, 其余 None）。
    measure_field_idx: Vec<Option<usize>>,
    /// 行字段类型分派（2026-08-26 q18/q19 紧凑化）：列式路径首次 `process_batch`
    /// 从 batch schema 构建；行式路径（无静态类型）退化全 Other（不紧凑但正确）。
    row_field_layout: Option<std::sync::Arc<RowFieldLayout>>,
    /// 待创建 redb spill store（M4）：路径 + 落盘上限 + 规则级共享计数。延迟到
    /// 首次 `process_*`（行字段 layout 解析后）创建——store 的 layout 必须与
    /// executor 一致。窗口 reset 后保留（下一窗口沿用同路径, create 语义重建文件）。
    spill_redb: Option<(
        std::path::PathBuf,
        Option<usize>,
        Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    )>,
    /// 规则级共享内存占用计数（`max_memory` = 规则总驻留上限, 分片数是引擎
    /// 内部细节; None = 未配置共享 → 本片独立预算）。reset_window 用它恢复
    /// 新窗口的共享计数（与 `spill_redb` 同模式, 跨窗口保留）。
    mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// 纯数值计划 SoA 布局（与 `window.soa_layout` 同源; executor 级副本——
    /// 热路径在 `window` 被 `&mut` 借用时仍可读, 免借用冲突）。None = 非 SoA。
    soa_layout: Option<NumericSoALayout>,
}

impl StatsExecutor {
    pub fn new(plan: StatsPlan) -> Self {
        Self::with_row_fields(plan, None)
    }

    /// 行字段 layout（2026-08-26）：列式已建 → 复用；否则按字段名全 Other
    /// （行式无静态 schema 类型——不紧凑但语义一致）。
    fn row_fields_layout_for_row(
        &self,
        names: Option<&[String]>,
    ) -> std::sync::Arc<RowFieldLayout> {
        if let Some(l) = &self.row_field_layout {
            return std::sync::Arc::clone(l);
        }
        let names: Vec<String> = match names {
            Some(ns) => ns.to_vec(),
            None => vec![],
        };
        std::sync::Arc::new(RowFieldLayout::all_other(&names))
    }

    /// 列式路径确保 layout（首次从 batch schema 构建并缓存）。
    fn ensure_row_field_layout(&mut self, batch: &RecordBatch) -> std::sync::Arc<RowFieldLayout> {
        if let Some(l) = &self.row_field_layout {
            return std::sync::Arc::clone(l);
        }
        let names: Vec<String> = match &self.row_field_names {
            Some(ns) => ns.as_ref().clone(),
            None => {
                let mut ns: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                ns.sort();
                ns
            }
        };
        let layout = std::sync::Arc::new(RowFieldLayout::from_schema(&names, &batch.schema()));
        self.row_field_layout = Some(std::sync::Arc::clone(&layout));
        layout
    }

    /// 指定 last/top 行字段提取子集（None = 全部 schema 列）。
    ///
    /// **生产路径（spawn）恒传子集**——`None` 仅测试/缺省: 无子集时列数组列序
    /// 在提取时确定（行式行键排序/列式 schema 排序）, `measure_field_idx` 无法
    /// 构造期预计算, last/top 的**度量值**在标量/close 路径退化为 0.0（行字段
    /// 仍保留, 注入需任务层另行约定列序）。Q18/Q19 类规则必须传子集。
    pub fn with_row_fields(
        plan: StatsPlan,
        row_fields: Option<std::sync::Arc<HashSet<String>>>,
    ) -> Self {
        // 同表达式对同行的求值结果一致, 可安全共享（设计 §6.2 段1c 的行式对应）。
        let mut unique_wheres: Vec<Expr> = Vec::new();
        let mut measure_where: Vec<Option<usize>> = Vec::with_capacity(plan.measures.len());
        for m in &plan.measures {
            match &m.where_expr {
                None => measure_where.push(None),
                Some(e) => match unique_wheres.iter().position(|u| u == e) {
                    Some(i) => measure_where.push(Some(i)),
                    None => {
                        measure_where.push(Some(unique_wheres.len()));
                        unique_wheres.push(e.clone());
                    }
                },
            }
        }
        // P5: 子集字段名确定排序（列数组的列序）+ 每度量字段位置（last/top 热
        // 路径零查找）。`None` 子集 = 全列（列序在提取时确定, 见文档注释）。
        let row_field_names = row_fields.as_ref().map(|s| {
            let mut names: Vec<String> = s.iter().cloned().collect();
            names.sort(); // 确定性列序（对拍契约: 同 plan 恒同序）
            std::sync::Arc::new(names)
        });
        let measure_field_idx = plan
            .measures
            .iter()
            .map(|m| match (&m.field, &row_field_names) {
                (Some(fr), Some(names)) => names.iter().position(|n| n == field_name(fr)),
                _ => None,
            })
            .collect();
        // 空键规则预建 Empty 桶由 `StatsWindowState::new` 处理（快路径; 带 key
        // 惰性建桶）。
        let window = StatsWindowState::new(EngineHashMap::default(), &plan);
        let soa_layout = window.soa_layout.clone();
        Self {
            plan,
            window,
            watermark_nanos: 0,
            unique_wheres,
            measure_where,
            row_field_names,
            measure_field_idx,
            row_field_layout: None,
            spill_redb: None,
            mem_used_shared: None,
            soa_layout,
        }
    }

    /// 处理一批行（row-based; 列式段为 P1.5）。
    ///
    /// `extract(row, name) -> Option<Value>`: 由调用方提供行字段读取。
    ///
    /// where 过滤**内建求值**: 每行构建 1 次 ctx Event, 对去重后的唯一 where
    /// 表达式求值一次（结果共享给所有同条件度量）, 不再依赖调用方注入。
    /// 三值语义与 CEP `where_ok` 一致（eval 非 `Bool(true)` 即过滤）。
    ///
    /// 桶键（P2）: 按 `plan.keys` 分桶归并; 键缺失/null → 行跳过（对齐 CEP
    /// key 缺失不匹配语义）。空键规则恒单桶。
    pub fn process_rows<F>(&mut self, rows: &[HashMap<String, Value>], extract: F)
    where
        F: Fn(&HashMap<String, Value>, &str) -> Option<Value>,
    {
        // 延迟创建 redb spill store（layout 已确定——行式 all_other 或列式缓存）。
        self.ensure_spill_store();
        // where 结果缓存: 行间复用 buffer（无逐行分配）; 无 where 规则时保持空。
        let mut where_ok: Vec<bool> = Vec::with_capacity(self.unique_wheres.len());
        // SoA 布局（纯数值计划; 行循环内 `self.window` 被 &mut 借用时仍可读）。
        let soa_layout = self.soa_layout.as_ref();
        let has_row_measures = self
            .plan
            .measures
            .iter()
            .any(|m| matches!(m.agg, StatsAggPlan::Last | StatsAggPlan::Top));
        for row in rows {
            where_ok.clear();
            if !self.unique_wheres.is_empty() {
                let ctx = Event {
                    fields: row
                        .iter()
                        .map(|(k, v)| (k.as_str().into(), v.clone()))
                        .collect(),
                };
                for expr in &self.unique_wheres {
                    where_ok.push(matches!(
                        super::eval::eval_bool_expr(expr, &ctx),
                        Some(true)
                    ));
                }
            }
            // 桶键: 缺失/null → 行跳过
            let Some(bucket_key) = eval_row_key(&self.plan.keys, row) else {
                continue;
            };
            // 行字段列名（P5）: 子集 → 直接用; None → 本行键排序（仅测试/缺省,
            // 生产恒有子集）。last/top 度量才需计算。
            let row_names: Option<Box<[String]>> = if has_row_measures {
                match &self.row_field_names {
                    Some(ns) => Some(ns.as_slice().into()),
                    None => {
                        let mut keys: Vec<String> = row.keys().cloned().collect();
                        keys.sort();
                        Some(keys.into_boxed_slice())
                    }
                }
            } else {
                None
            };
            // 行字段列数组懒提取（每行一次, 多 last/top 度量共享同一 Arc——与
            // accumulate_keyed_row 的 row_cache 对齐）。
            let mut row_cache: Option<std::sync::Arc<RowFields>> = None;
            // 2026-08-26 q18/q19：行式路径 layout（列式已建 → 复用；否则全 Other）。
            let row_layout = self.row_fields_layout_for_row(row_names.as_deref());
            // 新桶超限（内存 guard）→ 该行跳过（与列式路径一致）。
            let Some(bucket) = self.window.bucket_mut(&bucket_key, &self.plan) else {
                continue;
            };
            // SoA 桶（纯数值计划）: 数组直写（无枚举/Box——行式路径同构）。
            if let StatsBucketAccs::Numeric(soa) = bucket {
                let layout = soa_layout.unwrap();
                for (idx, measure) in self.plan.measures.iter().enumerate() {
                    if let Some(wi) = self.measure_where[idx]
                        && !where_ok[wi]
                    {
                        continue;
                    }
                    soa.counts[idx] += 1;
                    if let Some(field) = &measure.field
                        && let Some(val) = extract(row, field_name(field))
                        && let Some(n) = value_to_i128(&val)
                    {
                        match measure.agg {
                            StatsAggPlan::Sum | StatsAggPlan::Avg => {
                                soa.sums[layout.sum_slot[idx].unwrap() as usize] += n;
                            }
                            StatsAggPlan::Min => {
                                let s = layout.min_slot[idx].unwrap() as usize;
                                let cur = &mut soa.mins[s];
                                *cur = Some(match *cur {
                                    Some(m) if m <= n => m,
                                    _ => n,
                                });
                            }
                            StatsAggPlan::Max => {
                                let s = layout.max_slot[idx].unwrap() as usize;
                                let cur = &mut soa.maxs[s];
                                *cur = Some(match *cur {
                                    Some(m) if m >= n => m,
                                    _ => n,
                                });
                            }
                            StatsAggPlan::Count => {}
                            _ => unreachable!("SoA 桶仅数值度量"),
                        }
                    }
                }
            } else if let StatsBucketAccs::Classic(accs) = bucket {
                for (idx, measure) in self.plan.measures.iter().enumerate() {
                    if let Some(wi) = self.measure_where[idx]
                        && !where_ok[wi]
                    {
                        continue;
                    }
                    let acc = &mut accs[idx];
                    // count 仅 Numeric 度量维护（avg 的 count/sum 同步——D6）;
                    // distinct/last/top 变体无 count（输出不读, 原字段为死状态）。
                    match measure.agg {
                        StatsAggPlan::Count
                        | StatsAggPlan::Sum
                        | StatsAggPlan::Avg
                        | StatsAggPlan::Min
                        | StatsAggPlan::Max => {
                            let nacc = acc.numeric_mut();
                            nacc.count += 1;
                            if let Some(field) = &measure.field
                                && let Some(val) = extract(row, field_name(field))
                            {
                                match measure.agg {
                                    StatsAggPlan::Count => {}
                                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                                        if let Some(n) = value_to_i128(&val) {
                                            nacc.sum += n;
                                        }
                                    }
                                    StatsAggPlan::Min => {
                                        if let Some(n) = value_to_i128(&val) {
                                            nacc.min = Some(match nacc.min {
                                                Some(m) if m <= n => m,
                                                _ => n,
                                            });
                                        }
                                    }
                                    StatsAggPlan::Max => {
                                        if let Some(n) = value_to_i128(&val) {
                                            nacc.max = Some(match nacc.max {
                                                Some(m) if m >= n => m,
                                                _ => n,
                                            });
                                        }
                                    }
                                    _ => unreachable!("Numeric 分派内仅数值度量"),
                                }
                            }
                        }
                        StatsAggPlan::DistinctCount => {
                            if let Some(field) = &measure.field
                                && let Some(val) = extract(row, field_name(field))
                            {
                                let key = value_to_distinct_key(&val);
                                acc.distinct_mut().insert(key);
                            }
                        }
                        StatsAggPlan::Last | StatsAggPlan::Top => {
                            if let Some(field) = &measure.field {
                                if let Some(val) = extract(row, field_name(field)) {
                                    // 快速淘汰预检（在构建行字段前）: top 已满且 key 进不了
                                    // 前 N → 跳过, 免每行 row_fields 提取 + Arc 分配。
                                    // 与列式路径同一口径（value_to_f64 同义）。
                                    if measure.agg == StatsAggPlan::Top {
                                        let n = measure.arg.unwrap_or(10) as usize;
                                        if n == 0 {
                                            continue; // top(0): 不保留任何条目
                                        }
                                        if let Some(key) = value_to_f64(&val)
                                            && let entries = acc.top()
                                            && entries.len() == n
                                            && key <= entries[n - 1].key
                                        {
                                            continue;
                                        }
                                    }
                                    // 行式路径: 按 row_names 列序提取（与列式
                                    // row_fields_from_batch 对齐; 同桶多 last 度量 Arc
                                    // 共享 1 份内存）。
                                    let row = row_cache.get_or_insert_with(|| {
                                        row_fields_from_row(row, row_names.as_deref(), &row_layout)
                                    });
                                    let fidx = measure_field_position(
                                        &self.plan,
                                        &self.measure_field_idx,
                                        idx,
                                        row_names.as_deref(),
                                    );
                                    apply_last_top(acc, measure, row, fidx);
                                } else if measure.agg == StatsAggPlan::Last {
                                    // 字段缺失: last 仍保留整行（yield 读其它字段）
                                    let row = row_cache.get_or_insert_with(|| {
                                        row_fields_from_row(row, row_names.as_deref(), &row_layout)
                                    });
                                    *acc.last_mut() = Some(std::sync::Arc::clone(row));
                                }
                            }
                        }
                    }
                }
            }
            self.window.event_count += 1;
        }
        // 2026-08-26 q16：批末刷新估算（distinct 集合计入真实 len）。
        self.window.refresh_estimated_bytes(&self.plan);
    }

    /// last/top 行字段列名（列数组列序; `None` = 无子集且未定——任务层仅在
    /// 生产子集路径使用）。
    pub fn row_field_names(&self) -> Option<&std::sync::Arc<Vec<String>>> {
        self.row_field_names.as_ref()
    }

    /// 计算最终度量值（空键兼容: 取单桶; 带 key 用 by_bucket 版本）。
    pub fn final_measure_values(&self) -> Vec<f64> {
        self.final_measure_values_by_bucket()
            .into_iter()
            .next()
            .map(|(_, values)| values)
            .unwrap_or_else(|| vec![0.0; self.plan.measures.len()])
    }

    /// 按桶的最终度量值（桶序 = ScopeKey 升序, 确定性输出对拍契约; avg 由
    /// sum/count 求得, D6）。标量访问器——last 取字段数值, top 为多值不适用
    /// （用 [`Self::close_window_by_bucket_rows`]）。
    pub fn final_measure_values_by_bucket(&self) -> Vec<(ScopeKey, Vec<f64>)> {
        let mut buckets: Vec<(ScopeKey, Vec<f64>)> = self
            .window
            .buckets
            .values()
            .flat_map(|chain| chain.iter())
            .map(|b| {
                (
                    b.scope_key.clone(),
                    bucket_measure_values(
                        &self.plan,
                        &b.accs,
                        self.window.soa_layout.as_ref(),
                        &self.measure_field_idx,
                    ),
                )
            })
            .collect();
        buckets.sort_by(|a, b| a.0.cmp(&b.0));
        buckets
    }

    /// 按桶 close 输出（rich 版, last/top 用; 标量计划同样适用）: 每桶每度量一个
    /// 值列表——标量恒 1 条目, `top` 产生 N 条目（rank 序, key DESC）。
    /// 条目携带行字段（last/top 的整行）, 供 yield 经 field_values 读 `b.*`。
    /// 桶序 = ScopeKey 升序; 同时清空窗口状态。
    pub fn close_window_by_bucket_rows(&mut self) -> Vec<StatsCloseBucket> {
        let buckets = self.window.take_buckets(&self.plan);
        let out = self.close_buckets_to_rows(buckets);
        self.reset_window();
        out
    }

    /// 一批桶 → StatsCloseBucket（流式 close 用，2026-08-26 q18 100M）: 从
    /// [`Self::take_buckets_up_to`] 分批取桶后转换, 避免一次性全量
    /// `StatsCloseBucket`（2935 万桶 ~5.9G）与状态数据同时驻留的峰值。
    /// 批内桶序 = 传入序（调用方 `take_buckets_up_to` 已按 ScopeKey 升序）。
    pub fn close_buckets_to_rows(
        &self,
        buckets: Vec<(ScopeKey, StatsBucketAccs)>,
    ) -> Vec<StatsCloseBucket> {
        buckets
            .into_iter()
            .map(|(key, accs)| StatsCloseBucket {
                key,
                measures: bucket_close_entries(
                    &self.plan,
                    &accs,
                    self.window.soa_layout.as_ref(),
                    &self.measure_field_idx,
                ),
            })
            .collect()
    }

    /// 流式 close 收尾: 清窗（保留限额配置 + 拒收计数跨窗口）。
    /// [`Self::take_buckets_up_to`] 全部取完（返回空）后调用。
    pub fn finish_close_window(&mut self) {
        self.reset_window();
    }

    /// 一批桶 → 度量值（流式 close 标量路径, 2026-08-26 同 rich 流式）; 批内
    /// 桶序 = 传入序（调用方 `take_buckets_up_to` 已按 ScopeKey 升序）。
    pub fn close_bucket_values(
        &self,
        buckets: Vec<(ScopeKey, StatsBucketAccs)>,
    ) -> Vec<(ScopeKey, Vec<f64>)> {
        buckets
            .into_iter()
            .map(|(key, accs)| {
                (
                    key,
                    bucket_measure_values(
                        &self.plan,
                        &accs,
                        self.window.soa_layout.as_ref(),
                        &self.measure_field_idx,
                    ),
                )
            })
            .collect()
    }

    /// 分批取内存桶（流式 close 的一部分）: 从桶表取最多 n 个链并移除（链内桶
    /// 拍平）, 批内 ScopeKey 升序; 全部取完（返回空）后调用方须
    /// [`Self::finish_close_window`]。不 reset（还有剩余桶, 下一批继续）。
    ///
    /// **M5-3**：不再并入 spill（流式 close 用 [`Self::take_next_close_batch`]
    /// 从内存 + spill 两源归并取桶——避免 close 全量 drain 的内存峰值）。
    /// 2026-08-26 review: 用 `retain` 原地移除已取链——v1 用 `mem::take` 全表 +
    /// 剩余重插新 HashMap（每批 O(剩余) 哈希 + 分配, 100M 30 批 ≈ 4.4 亿次重插
    /// close +~9s）; retain 每批 O(n) 轻量回调（无哈希重建, close ~3s）。
    pub fn take_buckets_up_to(&mut self, n: usize) -> Vec<(ScopeKey, StatsBucketAccs)> {
        let mut out = Vec::new();
        if n == 0 {
            return out;
        }
        self.window.buckets.retain(|_hash, chain| {
            if out.len() >= n {
                return true; // 已取够: 保留剩余
            }
            out.extend(
                std::mem::take(chain)
                    .into_iter()
                    .map(|b| (b.scope_key, b.accs)),
            );
            false // 本链已取空: 删除
        });
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// **流式 close 取桶（M5-3）**：从内存桶（预算内, 小）与 spill（游标续读,
    /// 分批）两源各取一批, 归并排序后返回（批内 ScopeKey 升序——对拍契约）。
    /// 两源都空 → 返回空（close 循环终止）。close 峰值 = 批大小, 不再全量
    /// drain 到内存（q18 30M 曾 43GB → swap 风暴挂死）。
    ///
    /// 批大小 clamp 到 [`SPILL_DRAIN_CHUNK`]（默认 5 万, `WF_SPILL_DRAIN_CHUNK`
    /// 可调）——与输出 `emit_chunk`（默认 100 万）解耦: 输出批大没关系,
    /// 但**从 redb 读回的批必须小**（反序列化驻留是 close 内存峰值的直接来源）。
    pub fn take_next_close_batch(&mut self, n: usize) -> Vec<(ScopeKey, StatsBucketAccs)> {
        let n = n.min(SPILL_DRAIN_CHUNK);
        let mut out = Vec::with_capacity(n);
        if n == 0 {
            return out;
        }
        let mem = self.take_buckets_up_to(n);
        // spill 批补足配额（两源之和 ≤ n）; 批内排序后与内存批归并。
        let spill_n = n.saturating_sub(mem.len()).max(1);
        let spill = self.window.spill_drain_up_to(spill_n, &self.plan);
        // spill 序列化契约是 Vec<StatsAccum> → 转回桶累加器载体（统一两源类型）。
        let mut spill: Vec<(ScopeKey, StatsBucketAccs)> = spill
            .into_iter()
            .map(|(k, accs)| (k, vec_to_bucket_accs(accs, self.window.soa_layout.as_ref())))
            .collect();
        spill.sort_by(|a, b| a.0.cmp(&b.0));
        // 归并两个有序序列（peek 比较 + next 取走, 无 clone）。
        let mut mem_iter = mem.into_iter().peekable();
        let mut spill_iter = spill.into_iter().peekable();
        loop {
            match (mem_iter.peek(), spill_iter.peek()) {
                (Some(x), Some(y)) => {
                    if x.0 <= y.0 {
                        out.push(mem_iter.next().expect("peek 即存在"));
                    } else {
                        out.push(spill_iter.next().expect("peek 即存在"));
                    }
                }
                (Some(_), None) => out.push(mem_iter.next().expect("peek 即存在")),
                (None, Some(_)) => out.push(spill_iter.next().expect("peek 即存在")),
                (None, None) => break,
            }
        }
        out
    }

    /// 窗口 close: 冻结当前值（空键单桶）, 清空状态。
    pub fn close_window(&mut self) -> Vec<f64> {
        let values = self.final_measure_values();
        self.reset_window();
        values
    }

    /// 窗口 close（按桶）: 每桶一行 `(key, values)`, 清空状态。
    pub fn close_window_by_bucket(&mut self) -> Vec<(ScopeKey, Vec<f64>)> {
        let out = self.final_measure_values_by_bucket();
        self.reset_window();
        out
    }

    fn reset_window(&mut self) {
        // 状态内存估算 vs 实际（诊断，2026-08-27 q18 RSS 校准）：bucket_allowance
        // 估算（estimated_bytes）与 actual_bytes 对比——低估则实际超预算才驱逐。
        log::info!(
            "stats 状态内存(规则 {}): 估算 {:>9.1}MB / 实际 {:>9.1}MB / 桶 {}（估算每键 {:.0}B）",
            self.window.rule_name,
            self.window.estimated_bytes as f64 / 1e6,
            self.window.actual_bytes() as f64 / 1e6,
            self.window.buckets.len(),
            if self.window.buckets.is_empty() {
                0.0
            } else {
                self.window.estimated_bytes as f64 / self.window.buckets.len() as f64
            },
        );
        // cleanup 旧 spill（窗口文件删除）——新窗口由 spawn 层重新注入 store。
        if let Some(spill) = &mut self.window.spill {
            spill.cleanup();
        }
        // 释放本窗占用的共享内存计数（流式 close 分批取桶但账本未递减;
        // take_buckets 路径已扣, 此处为 0 幂等）。预算随窗口释放可复用。
        self.window.mem_sub(self.window.estimated_bytes);
        let limit = self.window.limit_bytes;
        let rule_name = self.window.rule_name.clone();
        let over_limit = self.window.over_limit_new_buckets;
        let spill_evictions = self.window.spill_evictions;
        let spill_readbacks = self.window.spill_readbacks;
        let spill_scan_ns = self.window.spill_scan_ns;
        let spill_clone_ns = self.window.spill_clone_ns;
        let spill_write_ns = self.window.spill_write_ns;
        let spill_evict_calls = self.window.spill_evict_calls;
        // 空键 Empty 桶由 new 预建（keys 空时）; SoA 布局按 plan 重算。
        self.window = StatsWindowState::new(EngineHashMap::default(), &self.plan);
        // 保留限额配置 + 规则级共享内存计数（executor 字段持有）+ 拒收/抖动
        // 计数跨窗口（guard 持续生效; 计数供指标/告警）。spill 不跨窗口（store
        // 已 cleanup 丢弃, 文件删除; 共享落盘计数由 ensure_spill_store 重新注入）。
        self.window.set_memory_limit_shared(
            &rule_name,
            limit.map(|b| b as usize),
            self.mem_used_shared.clone(),
        );
        self.window.over_limit_new_buckets = over_limit;
        self.window.spill_evictions = spill_evictions;
        self.window.spill_readbacks = spill_readbacks;
        self.window.spill_scan_ns = spill_scan_ns;
        self.window.spill_clone_ns = spill_clone_ns;
        self.window.spill_write_ns = spill_write_ns;
        self.window.spill_evict_calls = spill_evict_calls;
    }

    /// 注入状态内存上限（字节; None = 不设防）——超限拒收新键桶, 已有桶继续。
    /// 未注入共享计数 → 本片独立预算（测试/单片, 与旧行为一致）。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.set_memory_limit_shared(rule_name, bytes, None);
    }

    /// 注入状态内存上限（规则级共享版）：`max_memory` = 规则总驻留上限——同
    /// 规则全部分片共用一个内存占用计数（spawn 层规则级创建, 分片 clone 注入）。
    pub fn set_memory_limit_shared(
        &mut self,
        rule_name: &str,
        bytes: Option<usize>,
        mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.mem_used_shared = mem_used_shared;
        self.window
            .set_memory_limit_shared(rule_name, bytes, self.mem_used_shared.clone());
    }

    /// 注入状态外溢存储（M3; 窗口开始时调用; None = 关闭 spill）。
    /// `max_spill_bytes` = 落盘上限（None = 不限; 三层预算阶梯第二层）。
    /// `spill_used` = 规则级共享落盘计数（同规则全部分片共用一个——
    /// `max_disk` 是规则总上限, 分片数是引擎内部细节; None = 未配置）。
    pub fn set_spill(
        &mut self,
        store: Option<Box<dyn SpillStore + Send + Sync>>,
        max_spill_bytes: Option<usize>,
        spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.spill_redb = None;
        self.window.set_spill(store, max_spill_bytes, spill_used);
    }

    /// 便捷：redb spill（M4, `limits { disk_provider = "redb" }`, 旧键 `spill` 为
    /// 兼容别名）——记录待创建配置,
    /// **延迟到首次 `process_*`**（行字段 layout 解析后）创建 store：
    /// store 的 layout 必须与 executor 一致（列式 from_schema / 行式 all_other）。
    /// `max_spill_bytes` = 落盘上限（None = 不限）。
    /// `spill_used` = 规则级共享落盘计数（见 [`Self::set_spill`]）。
    pub fn set_spill_redb(
        &mut self,
        path: impl AsRef<std::path::Path>,
        max_spill_bytes: Option<usize>,
        spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.spill_redb = Some((path.as_ref().to_path_buf(), max_spill_bytes, spill_used));
    }

    /// 惰性注册 spill store 创建规格（P0 修复 2026-08-27）：不直接创建 redb 库——
    /// 把创建推迟到**首次驱逐**（`account_new_bucket` 超限落盘时）。配置了
    /// `spill` 但全程无驱逐的窗口不建库/不起写 worker（q19 100M 实测 170 次
    /// create/cleanup churn → RSS +6GB）。预置落盘上限/共享计数（与创建后
    /// `set_spill` 同参数）；spec 被 take 后直接 `RedbSpillStore::create`。
    fn ensure_spill_store(&mut self) {
        let Some((path, max_spill_bytes, spill_used)) = self.spill_redb.clone() else {
            return;
        };
        if self.window.spill.is_some() || self.window.spill_create.is_some() {
            return;
        }
        let layout = match &self.row_field_layout {
            Some(l) => std::sync::Arc::clone(l),
            // 行式路径（无列式 schema）：按行字段子集 all_other（与
            // `row_fields_layout_for_row` 同构; 生产恒有子集, 见其文档）。
            None => {
                let names: Vec<String> = self
                    .row_field_names
                    .clone()
                    .map(|ns| ns.as_ref().clone())
                    .unwrap_or_default();
                std::sync::Arc::new(RowFieldLayout::all_other(&names))
            }
        };
        self.window.spill_limit_bytes = max_spill_bytes.map(|b| b as u64);
        self.window.spill_used = match spill_used {
            Some(u) => Some(u),
            // 未注入规则级共享计数（测试/单片直连 set_spill_redb）→ 自建本片
            // 独立计数, 与 `set_spill` 的退化语义一致。
            None => Some(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0))),
        };
        self.window.spill_create = Some(SpillCreateSpec { path, layout });
    }

    /// 提取本片已关闭窗口的**原始累加状态**（输入分区分片归并用）并重置窗口。
    /// 返回 `(桶原始状态, 本片事件数)`——协调片把它合并进自己的窗口后再 close。
    /// 仅空键/可交换度量（count/sum/min/max/distinct）分片使用（last/top 被
    /// spawn 门控排除——行序敏感不可归并）。
    pub fn take_partial(&mut self) -> (Vec<(ScopeKey, StatsBucketAccs)>, u64) {
        let buckets = self.window.take_buckets(&self.plan);
        let count = self.window.event_count;
        self.reset_window();
        (buckets, count)
    }

    /// 归并另一片的原始累加状态（输入分区分片）: 计数相加、sum 相加、min/max
    /// 取极值、distinct 集 union（`extend` 用自身 hasher 重插, 跨片 hasher 可
    /// 不同）、事件数相加。last/top 不在此路径（spawn 门控排除）。
    ///
    /// **串行（2026-08-24 实测）**: `thread::scope` 并行 union 在协调片 async
    /// close 里阻塞 tokio worker, 与其余片 ingest 争核 → EPS 5.96~7.86M 波动
    /// （比串行 7.86M 更差）。q15 EOS 归并 ~883ms 是固定尾部成本; 若要并行须
    /// 走 `spawn_blocking`/异步任务（数据移出 `&mut self`, 未做）。
    pub fn merge_partial(&mut self, buckets: Vec<(ScopeKey, StatsBucketAccs)>, event_count: u64) {
        for (key, accs) in buckets {
            // 超限（guard）→ 该片该键跳过（协调片侧同样受桶预算约束）。
            let Some(target) = self.window.bucket_mut(&key, &self.plan) else {
                continue;
            };
            merge_bucket_accs(target, accs);
        }
        self.window.event_count += event_count;
        // 2026-08-26 q16：分片归并后刷新估算（distinct union 可能大幅增长）。
        self.window.refresh_estimated_bytes(&self.plan);
    }

    /// 列式批处理（P1.5, 设计 §6.2）: 消费 fanout 投递的 raw [`RecordBatch`]。
    ///
    /// - 段 1: where 列式 mask（去重后的唯一条件, 每批一次 [`eval_guard_columnar`]）
    /// - 段 1d: count/sum/min/max **整列归并**（无逐行循环; avg 输出时 sum/count 求得）
    /// - 段 2: distinct 行式段——按 mask 的 true 行读**原生列值**插入（每行 1 次
    ///   哈希不可回避; 批内预去重为后续优化）
    ///
    /// 返回 `false` 表示本计划/批不满足列式前置（where 不可列式化, 或 distinct
    /// 字段列类型不在支持集）——调用方**必须回退** [`Self::process_rows`] 保证语义。
    pub fn process_batch(&mut self, batch: &RecordBatch) -> bool {
        self.process_batch_rows(batch, None)
    }

    /// 列式批处理（P1.5, 设计 §6.2）+ 行子集（P2 分片: `rows` = 本片拥有的
    /// 行索引; `None` = 全批）。归并只对行域内的行生效; where mask 仍整批计算。
    pub fn process_batch_rows(&mut self, batch: &RecordBatch, rows: Option<&[u32]>) -> bool {
        self.process_batch_rows_impl(batch, rows, None)
    }

    /// 分片缓存版（2026-08-27 q17）: where mask 经 [`StatsMaskCache`] 分片共享——
    /// 同一批被 S 片各自整批 eval 的重复（S×）消除为首片 1×（其余片 Arc 命中）。
    /// 语义与 [`Self::process_batch_rows`] 完全一致（mask 为批的纯函数）。
    pub fn process_batch_rows_cached(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
        cache: &StatsMaskCache,
    ) -> bool {
        self.process_batch_rows_impl(batch, rows, Some(cache))
    }

    fn process_batch_rows_impl(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
        mask_cache: Option<&StatsMaskCache>,
    ) -> bool {
        // 前置（**必须在任何累加副作用之前**——返回 false 时调用方回退
        // process_rows, 部分应用会把已累加的计数再算一遍）:
        // 1. 全部 where 表达式可列式化（eval_guard_columnar 对不可列式表达式
        //    返回全 false, 不可静默使用）
        // 2. distinct 字段列类型在支持集（段 2 失败同样造成部分应用）
        // 3. 桶键可列式化（全部为简单字段; 含 bucket/tier 等函数键 → 回退行式）
        for e in &self.unique_wheres {
            if !wf_lang::columnar::expr_is_columnar(e) {
                return false;
            }
        }
        if !distinct_fields_columnar_safe(batch, &self.plan) {
            return false;
        }
        let key_cols: Option<Vec<usize>> = self
            .plan
            .keys
            .iter()
            .map(|k| match k {
                Expr::Field(fr) => batch.schema().index_of(field_ref_name(fr)).ok(),
                _ => None,
            })
            .collect();
        let Some(key_cols) = key_cols else {
            return false;
        };
        // 延迟创建 redb spill store：先解析行字段 layout（列式 from_schema），
        // 再建 store（layout 一致是读回正确性的前提）。幂等。
        self.ensure_row_field_layout(batch);
        self.ensure_spill_store();
        let n = batch.num_rows();
        // 行域（P2 分片）: `rows` = 本片拥有的行索引子集（绝对行号, 升序）。
        // 归并段（段 1d/段 2）改为**行域驱动**（count_domain/sum_domain/
        // insert_distinct_domain 只遍历本片行）——不再构建全批 domain mask。
        let view = ColumnarBatch::from_all_fields(batch);
        // 段 1: where 列式 mask（去重后唯一条件, 每批一次; 分片缓存版共享首片结果）。
        let masks: Vec<BooleanArray> = match mask_cache {
            Some(cache) => (*cache.get_or_compute(batch, || {
                let view = ColumnarBatch::from_all_fields(batch);
                self.unique_wheres
                    .iter()
                    .map(|e| eval_guard_columnar(e, &view))
                    .collect()
            }))
            .clone(), // Vec<BooleanArray> clone = 列数组 Arc 浅拷贝
            None => self
                .unique_wheres
                .iter()
                .map(|e| eval_guard_columnar(e, &view))
                .collect(),
        };
        // 行字段列名（P5）: 子集 → 直接用; None → 排序的 schema 字段名（与行式
        // None 同序, 任务层注入同序）。仅 last/top 计划需要。
        let has_row_measures = self
            .plan
            .measures
            .iter()
            .any(|m| matches!(m.agg, StatsAggPlan::Last | StatsAggPlan::Top));
        let row_names: Option<Box<[String]>> = if has_row_measures {
            match &self.row_field_names {
                Some(ns) => Some(ns.as_slice().into()),
                None => {
                    let mut ns: Vec<String> = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                    ns.sort();
                    Some(ns.into_boxed_slice())
                }
            }
        } else {
            None
        };
        // 行字段列索引（P5+ 优化: 每批预解析一次, 免逐行 schema.index_of）
        let row_field_cols: Option<Box<[Option<usize>]>> = row_names.as_deref().map(|ns| {
            ns.iter()
                .map(|n| batch.schema().index_of(n).ok())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
        // 度量字段列索引（2026-08-27 q17 修复）: 每批预解析一次, 免逐行逐度量
        // `schema().index_of` 线性扫描——q17 4 个数值度量 min/max/avg/sum 全命中
        // 旧缺陷（column_i128 每行 index_of + downcast）。count 无字段 → None。
        let measure_field_cols: Vec<Option<usize>> = self
            .plan
            .measures
            .iter()
            .map(|m| {
                m.field
                    .as_ref()
                    .and_then(|f| batch.schema().index_of(field_name(f)).ok())
            })
            .collect();
        // 带 key（P2）: 逐行按桶归并（mask 列式 + 桶键列式, 无解释器 eval）。
        // 空键保持整列归并快路径（P1.5）。
        if !self.plan.keys.is_empty() {
            // 键列类型每批预解析一次（免逐行 downcast_ref 分派）
            let key_columns = resolve_key_columns(batch, &key_cols);
            return self.process_batch_keyed(
                batch,
                &masks,
                &key_cols,
                &key_columns,
                n,
                rows,
                row_names.as_deref(),
                row_field_cols.as_deref(),
                &measure_field_cols,
            );
        }
        // 段 1d: 纯归并度量整列累加（**行域驱动**——2026-08-24 分片裁剪: 只遍历
        // 本片行, 消除每片对全批的 O(n) 冗余扫描; 全批路径 `rows=None` 行为不变）。
        // 行式语义: 满足 where 的行对**每个**度量都 `count += 1`（在字段读取前）
        // ——avg 的 count 必须与 sum 同步累加, 否则 avg = sum/count 输出 0（D6）。
        let soa_layout = self.soa_layout.as_ref();
        // 空键规则恒单桶（预建, 不参与限额——guard 只针对键空间膨胀）。
        let bucket = self
            .window
            .bucket_mut(&ScopeKey::Empty, &self.plan)
            .expect("Empty 桶恒存在");
        if let StatsBucketAccs::Numeric(soa) = bucket {
            let layout = soa_layout.unwrap();
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                let wi = self.measure_where[idx];
                let rows_in = count_domain(rows, n, &masks, wi);
                match measure.agg {
                    StatsAggPlan::Count => {
                        soa.counts[idx] += rows_in;
                    }
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        soa.counts[idx] += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            let s = layout.sum_slot[idx].unwrap() as usize;
                            soa.sums[s] += sum_domain(&col, rows, n, &masks, wi);
                        }
                    }
                    StatsAggPlan::Min => {
                        soa.counts[idx] += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            let s = layout.min_slot[idx].unwrap() as usize;
                            minmax_domain_one(&col, rows, n, &masks, wi, true, &mut soa.mins[s]);
                        }
                    }
                    StatsAggPlan::Max => {
                        soa.counts[idx] += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            let s = layout.max_slot[idx].unwrap() as usize;
                            minmax_domain_one(&col, rows, n, &masks, wi, false, &mut soa.maxs[s]);
                        }
                    }
                    _ => unreachable!("SoA 桶仅数值度量"),
                }
            }
        } else if let StatsBucketAccs::Classic(accs) = bucket {
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                let wi = self.measure_where[idx];
                let acc = &mut accs[idx];
                let rows_in = count_domain(rows, n, &masks, wi);
                match measure.agg {
                    StatsAggPlan::Count => {
                        acc.numeric_mut().count += rows_in;
                    }
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        let nacc = acc.numeric_mut();
                        nacc.count += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            nacc.sum += sum_domain(&col, rows, n, &masks, wi);
                        }
                    }
                    StatsAggPlan::Min | StatsAggPlan::Max => {
                        let nacc = acc.numeric_mut();
                        nacc.count += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            minmax_domain(&col, rows, n, &masks, wi, &mut nacc.min, &mut nacc.max);
                        }
                    }
                    StatsAggPlan::DistinctCount => {
                        // 输出只用 distinct_set（无 count 字段——原 count 维护为死状态）
                    }
                    StatsAggPlan::Last | StatsAggPlan::Top => {
                        // P1 不实现（Q18/Q19 扩展）
                    }
                }
            }
        }
        // 段 2: distinct/last/top 行式段（原生列值按行域 + where 过滤; last/top 提取
        // 行字段供 yield 注入）
        // 2026-08-26 q18/q19：行字段 layout 在桶借用前 ensure（ensure 需 &mut self）。
        let row_layout = self.ensure_row_field_layout(batch);
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            if !matches!(
                measure.agg,
                StatsAggPlan::DistinctCount | StatsAggPlan::Last | StatsAggPlan::Top
            ) {
                continue;
            }
            let wi = self.measure_where[idx];
            // distinct/last/top 计划恒 Classic（soa_layout 仅纯数值计划）——
            // 解包取数组（形态不符 = 内部错误）。
            let accs = match self
                .window
                .bucket_mut(&ScopeKey::Empty, &self.plan)
                .expect("Empty 桶恒存在")
            {
                StatsBucketAccs::Classic(accs) => accs,
                StatsBucketAccs::Numeric(_) => {
                    unreachable!("distinct/last/top 计划不走 SoA 桶")
                }
            };
            let acc = &mut accs[idx];
            if matches!(measure.agg, StatsAggPlan::DistinctCount) {
                let Some(field) = &measure.field else {
                    continue;
                };
                let set = acc.distinct_mut();
                if !insert_distinct_domain(batch, field_name(field), rows, n, &masks, wi, set) {
                    return false;
                }
            } else {
                // last/top: 逐行按行域 + where 更新（子集行字段提取; 空键 last 规则少用）
                let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
                for r in domain_rows(rows, n).filter(|&r| passes(r)) {
                    let row =
                        row_fields_from_batch(batch, r, row_field_cols.as_deref(), &row_layout);
                    let fidx = measure_field_position(
                        &self.plan,
                        &self.measure_field_idx,
                        idx,
                        row_names.as_deref(),
                    );
                    apply_last_top(acc, measure, &row, fidx);
                }
            }
        }
        self.window.event_count += rows.map_or(n as u64, |rs| rs.len() as u64);
        // 2026-08-26 q16：批末刷新估算（distinct 集合计入真实 len）。
        self.window.refresh_estimated_bytes(&self.plan);
        true
    }

    /// 带 key 的批处理: 逐行按桶归并（P2）。桶键列式（`scope_key_columnar`）,
    /// where 列式 mask, 归并行式（每桶独立累积）。键 null → 行跳过（对齐 CEP
    /// key 缺失语义; 与 fanout 的 missing-key → shard 0 分片口径一致——分片只
    /// 影响投递归属, 桶归并仍按完整键）。
    ///
    /// `rows`（P2 分片）= 本片行子集: **只归并行域内的行**——否则每片处理全批,
    /// 每个键被 N 片各算一遍, close 重复输出 N 倍（Q16 实测 EMIT 10 倍）。
    #[allow(clippy::too_many_arguments)] // 列式批处理签名: 键列/掩码/行域/行字段提取列 4 组参数
    fn process_batch_keyed(
        &mut self,
        batch: &RecordBatch,
        masks: &[BooleanArray],
        key_cols: &[usize],
        key_columns: &[KeyColumn<'_>],
        n: usize,
        rows: Option<&[u32]>,
        row_names: Option<&[String]>,
        row_field_cols: Option<&[Option<usize>]>,
        measure_field_cols: &[Option<usize>],
    ) -> bool {
        // 局部寄存器计数（F1 修复的 event_count 口径 + 零热路径开销）: 归并成功
        // 才计, 批末一次性写回——避免每行一次 `self.window.event_count += 1` 的
        // 内存往返（q19 列式实测 +2.3%）。
        let mut counted: u64 = 0;
        match rows {
            Some(rs) => {
                for &r in rs {
                    if (r as usize) >= n {
                        continue; // 防御: 越界行号（与 materialize_rows 一致跳过）
                    }
                    if self.accumulate_keyed_row(
                        batch,
                        masks,
                        key_cols,
                        key_columns,
                        r as usize,
                        row_names,
                        row_field_cols,
                        measure_field_cols,
                    ) {
                        counted += 1;
                    }
                }
            }
            None => {
                for row in 0..n {
                    if self.accumulate_keyed_row(
                        batch,
                        masks,
                        key_cols,
                        key_columns,
                        row,
                        row_names,
                        row_field_cols,
                        measure_field_cols,
                    ) {
                        counted += 1;
                    }
                }
            }
        }
        self.window.event_count += counted;
        true
    }

    /// 单行桶归并（P2 复合键逐行路径的公共主体, 供全批/行域两分支复用）。
    /// 返回 `true` = 归并成功（行计入 event_count）; `false` = 键 null/超限拒收
    /// （行跳过, 不计入——F1 行式/列式对拍口径）。
    ///
    /// 字段读取走**原生列值**（`column_i128_at`/`column_distinct_key_at`, 列索引
    /// 批级预解析——与空键列式段同精度（D7/D8: ≥2^53 的 Int64 不得经
    /// `Value::Number(f64)` 舍入; Timestamp 列 distinct 也走原生 i64, 不得静默跳过）。
    ///
    /// **复合键优化（P5+）**: 键数 ≤ 4 走扁平键路径——栈上叶数组（`key_column_comp`
    /// 预解析列, 无 Box 分配/无逐行 downcast）→ `comps_hash` → `keyed_bucket_mut`
    /// （链扫描, `ScopeKey` 仅每桶首见构建一次）; 键数 > 4 回退完整键
    /// `scope_key_columnar`（罕见）。
    #[allow(clippy::too_many_arguments)] // 与 process_batch_keyed 同签名族
    fn accumulate_keyed_row(
        &mut self,
        batch: &RecordBatch,
        masks: &[BooleanArray],
        key_cols: &[usize],
        key_columns: &[KeyColumn<'_>],
        row: usize,
        row_names: Option<&[String]>,
        row_field_cols: Option<&[Option<usize>]>,
        measure_field_cols: &[Option<usize>],
    ) -> bool {
        const MAX_STACK_KEYS: usize = 4;
        // 2026-08-26 q18/q19：行字段 layout（首次从 schema 构建并缓存）；
        // 在桶借用前取（ensure 需 &mut self）。soa_layout 同源取——窗口被 &mut
        // 借用时仍可读（字段级拆分借用）。
        let row_layout = self.ensure_row_field_layout(batch);
        let soa_layout = self.soa_layout.as_ref();
        if key_columns.len() <= MAX_STACK_KEYS {
            let mut comps: [ScopeKey; MAX_STACK_KEYS] = std::array::from_fn(|_| ScopeKey::Empty);
            for (i, kc) in key_columns.iter().enumerate() {
                let Some(c) = key_column_comp(kc, batch, row) else {
                    return false; // 键 null → 跳过
                };
                comps[i] = c;
            }
            let comps = &comps[..key_columns.len()];
            let hash = comps_hash(comps);
            // 新桶超限（内存 guard）→ 该行跳过。
            let Some(bucket) = self.window.keyed_bucket_mut(hash, comps, &self.plan) else {
                return false;
            };
            accumulate_bucket_row(
                bucket,
                soa_layout,
                &self.plan,
                &self.measure_where,
                &self.measure_field_idx,
                row_names,
                row_field_cols,
                batch,
                masks,
                row,
                &row_layout,
                measure_field_cols,
            );
            return true;
        }
        let Some(key) = scope_key_columnar(batch, key_cols, row) else {
            return false; // 键 null → 跳过
        };
        // 新桶超限（内存 guard）→ 该行跳过。
        let Some(bucket) = self.window.bucket_mut(&key, &self.plan) else {
            return false;
        };
        accumulate_bucket_row(
            bucket,
            soa_layout,
            &self.plan,
            &self.measure_where,
            &self.measure_field_idx,
            row_names,
            row_field_cols,
            batch,
            masks,
            row,
            &row_layout,
            measure_field_cols,
        );
        true
    }
}

/// 单行桶累加入口（按桶形态分派一次）: SoA → [`accumulate_soa`]; Classic →
/// [`accumulate_column_row`]（枚举/Box 循环）。分派每行一次（非每度量）——
/// SoA 计划的热路径进入后无枚举分派无 Box 解引用。
#[allow(clippy::too_many_arguments)] // 与 accumulate_column_row 同签名族 + soa_layout
fn accumulate_bucket_row(
    bucket: &mut StatsBucketAccs,
    soa_layout: Option<&NumericSoALayout>,
    plan: &StatsPlan,
    measure_where: &[Option<usize>],
    measure_field_idx: &[Option<usize>],
    row_names: Option<&[String]>,
    row_field_cols: Option<&[Option<usize>]>,
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
    row_layout: &std::sync::Arc<RowFieldLayout>,
    measure_field_cols: &[Option<usize>],
) {
    match bucket {
        StatsBucketAccs::Numeric(soa) => accumulate_soa(
            soa,
            soa_layout.unwrap(),
            measure_where,
            measure_field_cols,
            batch,
            masks,
            row,
        ),
        StatsBucketAccs::Classic(accs) => accumulate_column_row(
            accs,
            plan,
            measure_where,
            measure_field_idx,
            row_names,
            row_field_cols,
            batch,
            masks,
            row,
            row_layout,
            measure_field_cols,
        ),
    }
}

/// 单行 SoA 桶累加（纯数值计划; 2026-08-27 q17）: 段 1 counts 全度量（含 where
/// 过滤）; 段 2 按字段分组数值更新——同字段度量共享 1 次 [`column_i128_at`]
/// （旧路径 sum/avg/min/max 各自读取同一列, 每行 4 次重复）。免: 每度量枚举
/// 分派 + `Box` 解引用 + 同列重复列读取。pub(crate) 供 SoA 对照 bench。
pub(crate) fn accumulate_soa(
    soa: &mut NumericSoA,
    layout: &NumericSoALayout,
    measure_where: &[Option<usize>],
    measure_field_cols: &[Option<usize>],
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
) {
    for (idx, wi) in measure_where.iter().enumerate() {
        if let Some(wi) = wi
            && !masks[*wi].value(row)
        {
            continue;
        }
        soa.counts[idx] += 1;
    }
    for g in layout.groups.iter() {
        // 组内度量同字段 → 列索引相同; 取第一个度量的批级列索引读一次。
        let Some(ci) = measure_field_cols[g.entries[0].0] else {
            continue;
        };
        let Some(v) = column_i128_at(batch, ci, row) else {
            continue;
        };
        for &(idx, kind) in g.entries.iter() {
            match kind {
                NumericKind::Sum => {
                    soa.sums[layout.sum_slot[idx].unwrap() as usize] += v;
                }
                NumericKind::Min => {
                    let s = layout.min_slot[idx].unwrap() as usize;
                    let cur = &mut soa.mins[s];
                    *cur = Some(match *cur {
                        Some(m) if m <= v => m,
                        _ => v,
                    });
                }
                NumericKind::Max => {
                    let s = layout.max_slot[idx].unwrap() as usize;
                    let cur = &mut soa.maxs[s];
                    *cur = Some(match *cur {
                        Some(m) if m >= v => m,
                        _ => v,
                    });
                }
            }
        }
    }
}

/// 单行桶累加主体（列式路径; 自由函数——调用点持有 `&mut self.window` 桶借用,
/// 方法会整 self 借用冲突）。**Classic 路径**（含 distinct/last/top 的计划）;
/// 纯数值计划走 [`accumulate_soa`]。
///
/// last/top 行字段每行懒提取一次, 多度量共享同一 Arc（Q18 4 个 last 度量内存
/// 1 份; 提取列序 = row_names, 免整行 8 字段）。pub(crate) 供 SoA 对照 bench。
#[allow(clippy::too_many_arguments)] // 单行桶累加: 桶/计划/掩码索引/行字段/列/行号 6 组参数
pub(crate) fn accumulate_column_row(
    bucket: &mut [StatsAccum],
    plan: &StatsPlan,
    measure_where: &[Option<usize>],
    measure_field_idx: &[Option<usize>],
    row_names: Option<&[String]>,
    row_field_cols: Option<&[Option<usize>]>,
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
    row_layout: &std::sync::Arc<RowFieldLayout>,
    measure_field_cols: &[Option<usize>],
) {
    let mut row_cache: Option<std::sync::Arc<RowFields>> = None;
    for (idx, measure) in plan.measures.iter().enumerate() {
        if let Some(wi) = measure_where[idx]
            && !masks[wi].value(row)
        {
            continue;
        }
        let acc = &mut bucket[idx];
        // count 仅 Numeric 度量维护（distinct/last/top 变体无 count——原 count
        // 字段为死状态, 输出不读）。
        match measure.agg {
            StatsAggPlan::Count
            | StatsAggPlan::Sum
            | StatsAggPlan::Avg
            | StatsAggPlan::Min
            | StatsAggPlan::Max => {
                let nacc = acc.numeric_mut();
                nacc.count += 1;
                // 度量字段列索引（批级预解析, 免每行 schema.index_of——q17 修复）;
                // count 无字段 → None → 只计数。
                let Some(ci) = measure_field_cols[idx] else {
                    continue;
                };
                match measure.agg {
                    StatsAggPlan::Count => {}
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        if let Some(nn) = column_i128_at(batch, ci, row) {
                            nacc.sum += nn;
                        }
                    }
                    StatsAggPlan::Min => {
                        if let Some(nn) = column_i128_at(batch, ci, row) {
                            nacc.min = Some(match nacc.min {
                                Some(m) if m <= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    StatsAggPlan::Max => {
                        if let Some(nn) = column_i128_at(batch, ci, row) {
                            nacc.max = Some(match nacc.max {
                                Some(m) if m >= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    _ => unreachable!("Numeric 分派内仅数值度量"),
                }
            }
            StatsAggPlan::DistinctCount => {
                if let Some(ci) = measure_field_cols[idx]
                    && let Some(k) = column_distinct_key_at(batch, ci, row)
                {
                    acc.distinct_mut().insert(k);
                }
            }
            StatsAggPlan::Last | StatsAggPlan::Top => {
                // 快速淘汰预检（**在构建行字段前**）: top 已满且 key 进不了前 N
                // → 直接跳过, 免每行 row_fields 的 Arc 分配 + 字段提取。q19 每
                // auction 的 bid 绝大多数低于当前 top-10 门槛（bench ~99.8% 行
                // 被此预检挡下）。列索引经构造期预计算的 `measure_field_idx` 取
                // （零 index_of / 零 names 回退——无子集时 idx 恒 None → 不预检,
                // 仅测试/缺省路径, 性能不敏感）。列值口径与行字段提取后
                // `value_to_f64` 一致（Int64→as f64 / Float64 原值）。
                if measure.agg == StatsAggPlan::Top {
                    let n = measure.arg.unwrap_or(10) as usize;
                    if n == 0 {
                        continue; // top(0): 不保留任何条目, 无需行字段
                    }
                    if let Some(ci) = measure_field_idx[idx].and_then(|i| {
                        row_field_cols
                            .and_then(|cols| cols.get(i).copied())
                            .flatten()
                    }) && let Some(key) = column_f64_at(batch, ci, row)
                        && let entries = acc.top()
                        && entries.len() == n
                        && key <= entries[n - 1].key
                    {
                        continue;
                    }
                }
                let row = row_cache.get_or_insert_with(|| {
                    row_fields_from_batch(batch, row, row_field_cols, row_layout)
                });
                let fidx = measure_field_position(plan, measure_field_idx, idx, row_names);
                apply_last_top(acc, measure, row, fidx);
            }
        }
    }
}

fn field_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(n) => n,
        FieldRef::Qualified(_, n) | FieldRef::Bracketed(_, n) => n,
        FieldRef::Path { segments, .. } => match segments.first() {
            Some(wf_lang::ast::PathSegment::Field(root)) => root,
            _ => "",
        },
        _ => "",
    }
}

/// 度量字段在行字段列数组中的位置: 子集模式走构造期预计算; 无子集按
/// `names` 名查（last/top 且字段在列内 → Some, 其余 None）。
/// 自由函数而非方法: 调用点同时持有 `&mut self.window` 的桶借用, 方法会整
/// self 借用冲突; 自由函数只借 `plan` + `measure_field_idx` 两字段。
fn measure_field_position(
    plan: &StatsPlan,
    measure_field_idx: &[Option<usize>],
    idx: usize,
    names: Option<&[String]>,
) -> Option<usize> {
    match measure_field_idx[idx] {
        Some(i) => Some(i),
        None => match (&plan.measures[idx].field, names) {
            (Some(f), Some(ns)) => ns.iter().position(|n| n == field_name(f)),
            _ => None,
        },
    }
}

// ---------------------------------------------------------------------------
// 归并（输入分区分片: 可交换度量）
// ---------------------------------------------------------------------------

/// 归并两个累加器（count 相加 / sum 相加 / min·max 取极值 / distinct 集 union）。
/// 仅可交换度量路径使用（last/top 被 spawn 门控排除——行序敏感不可归并）。
/// 变体不匹配 = plan/构造不一致的内部错误（panic 尽早暴露）。
fn merge_accum(t: &mut StatsAccum, o: &StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            t.min = match (t.min, o.min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            t.max = match (t.max, o.max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        (StatsAccum::Distinct(t), StatsAccum::Distinct(o)) => t.extend_other(o),
        (StatsAccum::Last(_), StatsAccum::Last(_)) | (StatsAccum::Top(_), StatsAccum::Top(_)) => {
            // 行序敏感度量不走分片归并（spawn 门控）; 防御性静默（对齐旧行为）。
        }
        _ => unreachable!("StatsAccum 归并变体不匹配（plan 与构造不一致的内部错误）"),
    }
}

/// 归并两个桶累加器载体（分派按桶形态; 同 plan 两片恒同形态——不一致为内部
/// 错误）。SoA: 平行数组逐元素合并; Classic: 走 [`merge_accum`]。
fn merge_bucket_accs(t: &mut StatsBucketAccs, o: StatsBucketAccs) {
    match (t, o) {
        (StatsBucketAccs::Numeric(t), StatsBucketAccs::Numeric(o)) => {
            for (i, c) in o.counts.iter().enumerate() {
                t.counts[i] += c;
            }
            for (i, s) in o.sums.iter().enumerate() {
                t.sums[i] += s;
            }
            for (i, m) in o.mins.iter().enumerate() {
                if let Some(v) = *m {
                    t.mins[i] = Some(match t.mins[i] {
                        Some(x) if x <= v => x,
                        _ => v,
                    });
                }
            }
            for (i, m) in o.maxs.iter().enumerate() {
                if let Some(v) = *m {
                    t.maxs[i] = Some(match t.maxs[i] {
                        Some(x) if x >= v => x,
                        _ => v,
                    });
                }
            }
        }
        (StatsBucketAccs::Classic(t), StatsBucketAccs::Classic(o)) => {
            for (t, o) in t.iter_mut().zip(o.iter()) {
                merge_accum(t, o);
            }
        }
        _ => unreachable!("StatsBucketAccs 归并形态不匹配（同 plan 两片恒同形态）"),
    }
}

// ---------------------------------------------------------------------------
// 键列预解析（P5+ 优化: 每批一次类型分派, 免逐行 `downcast_ref`）
// ---------------------------------------------------------------------------
//
// `fanout::scope_key_from_column` 每行每键重复做 `col.data_type()` match +
// `downcast_ref` 动态分派（Q18 双键 × 27.6M 行）。此处每批解析一次键列类型为
// `KeyColumn`（借用 batch 列数组）, 逐行直接 `is_null` + `value`——规范化与
// `scope_key_from_column` 完全一致（Int64/Timestamp → Int, Float64 → 规范化位,
// Utf8 → Str, Boolean → Str "true"/"false"）; 不支持类型回退 `Other`。

enum KeyColumn<'a> {
    Int64(&'a Int64Array),
    Timestamp(&'a arrow::array::TimestampNanosecondArray),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    /// 其它类型 → 逐行回退 `scope_key_from_column`（罕见）。
    Other(usize),
}

fn resolve_key_columns<'a>(batch: &'a RecordBatch, key_cols: &[usize]) -> Vec<KeyColumn<'a>> {
    use arrow::datatypes::{DataType, TimeUnit};
    key_cols
        .iter()
        .map(|&ci| {
            let col = batch.column(ci);
            match col.data_type() {
                DataType::Int64 => col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(KeyColumn::Int64)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .map(KeyColumn::Timestamp)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(KeyColumn::Float64)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(KeyColumn::Utf8)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Boolean => col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(KeyColumn::Boolean)
                    .unwrap_or(KeyColumn::Other(ci)),
                _ => KeyColumn::Other(ci),
            }
        })
        .collect()
}

/// 从预解析键列读单行叶键（null → None; 规范化与 `scope_key_from_column` 同）。
fn key_column_comp<'a>(
    col: &KeyColumn<'a>,
    batch: &'a RecordBatch,
    row: usize,
) -> Option<ScopeKey> {
    match col {
        KeyColumn::Int64(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Int(a.value(row)))
            }
        }
        KeyColumn::Timestamp(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Int(a.value(row)))
            }
        }
        KeyColumn::Float64(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(scope_key_from_f64(a.value(row)))
            }
        }
        KeyColumn::Utf8(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Str(a.value(row).into()))
            }
        }
        KeyColumn::Boolean(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Str(
                    if a.value(row) { "true" } else { "false" }.into(),
                ))
            }
        }
        KeyColumn::Other(ci) => scope_key_from_column(batch, *ci, row),
    }
}

/// f64 → 键叶（与 `ScopeKey::from_value(Number)` 同规范化: 整数 <2^53 → Int,
/// 否则 Float(规范化位)）。
fn scope_key_from_f64(n: f64) -> ScopeKey {
    if n.fract() == 0.0 && n.abs() < TWO_POW_53 {
        ScopeKey::Int(n as i64)
    } else {
        ScopeKey::Float(canonical_f64_bits(n))
    }
}

/// 规范化 f64 位（0.0 → +0.0, NaN → canonical NaN; 与 key.rs 同口径）。
fn canonical_f64_bits(n: f64) -> u64 {
    if n == 0.0 {
        0.0f64.to_bits()
    } else if n.is_nan() {
        f64::NAN.to_bits()
    } else {
        n.to_bits()
    }
}

/// <2^53 的整数可被 f64 精确表示（与 `ScopeKey::from_value` 一致）。
const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

// ---------------------------------------------------------------------------
// 复合键扁平哈希（P5+ 优化）
// ---------------------------------------------------------------------------
//
// 桶表键 = 扁平键的 FNV 式混合 u64。`comps_hash`（列式叶数组）与
// `scope_key_hash`（行式完整树）字节级同构——同一逻辑键两种路径产出同值
// （`stats_composite_key_hash_flat_matches_tree` 锁定）; 碰撞链内以完整键比较
// 消歧。混合风格与 `key.rs::scope_key_shard_index` 一致（确定性, 无随机种子）。

const KEY_HASH_BASE: u64 = 0xcbf2_9ce4_8422_2325;

fn mix_byte(hash: &mut u64, b: u8) {
    *hash ^= u64::from(b);
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        mix_byte(hash, b);
    }
}

/// 完整 `ScopeKey` 树哈希（行式路径）——与 `key.rs::scope_key_shard_index`
/// 同字节序列（tag + 嵌套 payload, Pair 以 0x1f 分隔）。pub(crate) 供
/// 哈希同构契约测试。
pub(crate) fn scope_key_hash(key: &ScopeKey) -> u64 {
    let mut h = KEY_HASH_BASE;
    let tag = match key {
        ScopeKey::Empty => 0u8,
        ScopeKey::Int(_) => 1,
        ScopeKey::Float(_) => 2,
        ScopeKey::Str(_) => 3,
        ScopeKey::Pair(_, _) => 4,
    };
    mix_byte(&mut h, tag);
    nested_key_bytes(&mut h, key);
    h
}

fn nested_key_bytes(h: &mut u64, key: &ScopeKey) {
    match key {
        ScopeKey::Empty => {}
        ScopeKey::Int(v) => hash_bytes(h, &v.to_ne_bytes()),
        ScopeKey::Float(bits) => hash_bytes(h, &bits.to_ne_bytes()),
        ScopeKey::Str(s) => hash_bytes(h, s.as_bytes()),
        ScopeKey::Pair(a, b) => {
            nested_key_bytes(h, a);
            mix_byte(h, 0x1f);
            nested_key_bytes(h, b);
        }
    }
}

/// 叶数组（列式扁平键）哈希——**字节级镜像** `scope_key_hash` 的树字节序列:
/// 仅最外层 Pair tag（内层叶只 payload, 与嵌套树一致——内层类型歧义由碰撞链
/// 完整比较消歧）; N-1 个 0x1f 分隔。同逻辑键两路径产出同值（契约测试锁定）。
/// pub(crate) 供哈希同构契约测试。
pub(crate) fn comps_hash(comps: &[ScopeKey]) -> u64 {
    let mut h = KEY_HASH_BASE;
    let n = comps.len();
    match n {
        0 => {}
        1 => mix_leaf(&mut h, &comps[0], true), // 顶层单叶: tag + payload
        _ => {
            mix_byte(&mut h, 4); // 顶层 Pair tag（仅最外层 mix tag）
            for (i, c) in comps.iter().enumerate() {
                mix_leaf(&mut h, c, false); // 内层叶只 payload
                if i + 1 < n {
                    mix_byte(&mut h, 0x1f);
                }
            }
        }
    }
    h
}

/// 叶字节混入: `with_tag` = 顶层叶（mix 类型 tag）; 内层叶仅 payload。
fn mix_leaf(h: &mut u64, c: &ScopeKey, with_tag: bool) {
    match c {
        ScopeKey::Int(v) => {
            if with_tag {
                mix_byte(h, 1);
            }
            hash_bytes(h, &v.to_ne_bytes());
        }
        ScopeKey::Float(bits) => {
            if with_tag {
                mix_byte(h, 2);
            }
            hash_bytes(h, &bits.to_ne_bytes());
        }
        ScopeKey::Str(s) => {
            if with_tag {
                mix_byte(h, 3);
            }
            hash_bytes(h, s.as_bytes());
        }
        _ => unreachable!("comps 只含叶变体"),
    }
}

/// 左深 Pair 树与叶数组比较（列式命中校验）: `comps[start..end]` 是否被
/// `scope` 完全匹配。右叶恒为单键, 与 `comps[end-1]` 直接相等比较。
fn comps_match(scope: &ScopeKey, comps: &[ScopeKey], start: usize, end: usize) -> bool {
    match scope {
        ScopeKey::Empty => start == end,
        ScopeKey::Int(_) | ScopeKey::Float(_) | ScopeKey::Str(_) => {
            start + 1 == end && comps.get(start) == Some(scope)
        }
        ScopeKey::Pair(l, r) => {
            if start >= end {
                return false;
            }
            comps.get(end - 1) == Some(r.as_ref()) && comps_match(l, comps, start, end - 1)
        }
    }
}

/// 叶数组 → 完整 `ScopeKey`（左深 Pair 链; 每桶一次, 建桶时）。
/// pub(crate) 供哈希同构契约测试。
pub(crate) fn scope_key_from_comps(comps: &[ScopeKey]) -> ScopeKey {
    let mut acc: Option<ScopeKey> = None;
    for c in comps {
        acc = Some(match acc {
            None => c.clone(),
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(c.clone())),
        });
    }
    acc.unwrap_or(ScopeKey::Empty)
}

fn value_to_i128(v: &Value) -> Option<i128> {
    match v {
        Value::Number(n) => Some(*n as i128),
        _ => None,
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

/// last/top 行更新（Q18/Q19, 非归并状态）:
/// - `last`: 最近合格行的行字段列数组替换（流有序 = 事件时间最新, 权威 Q18
///   ORDER BY dateTime DESC）; 同桶多 last 度量共享同一 Arc（内存 1 份）。
/// - `top`: 按 key DESC 插入有界 top-N（同 key 先到者优先——流有序下的确定性
///   tie-break, 权威 Q19 未指定平局顺序）。
///
/// `field_idx` = 度量字段在行字段列数组中的位置（P5 预计算, 免字符串查找）;
/// `None` = 字段不在子集/无字段 → top 无键跳过, last 仍保留行。
fn apply_last_top(
    acc: &mut StatsAccum,
    measure: &StatsMeasurePlan,
    row: &std::sync::Arc<RowFields>,
    field_idx: Option<usize>,
) {
    match measure.agg {
        StatsAggPlan::Last => {
            *acc.last_mut() = Some(std::sync::Arc::clone(row));
        }
        StatsAggPlan::Top => {
            let Some(key) = field_idx.and_then(|i| row.f64_at(i)) else {
                return; // 非数值键 → 跳过（与 sum 跳过非数值一致）
            };
            let n = measure.arg.unwrap_or(10) as usize;
            if n == 0 {
                return; // top(0, ...): 不保留任何条目
            }
            let entries = acc.top_mut();
            // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目
            // 必插在既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义
            // 不变, 免去每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
            if entries.len() == n && key <= entries[n - 1].key {
                return;
            }
            // Arc 深拷贝为独立 Box（top 条目各自的行, 不共享）
            insert_top(entries, key, row.as_ref().clone(), n);
        }
        _ => {}
    }
}

/// top-N 插入: key DESC 有序保留前 N; 同 key 新条目插在已有同 key 条目之后
/// （先到者在前）。n=0 时清空（top(0, ...) 边界）。
fn insert_top(entries: &mut Vec<TopEntry>, key: f64, row: RowFields, n: usize) {
    if n == 0 {
        return;
    }
    // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目必插在
    // 既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义不变, 免去
    // 每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
    if entries.len() == n && key <= entries[n - 1].key {
        return;
    }
    let pos = entries
        .iter()
        .position(|e| key > e.key)
        .unwrap_or(entries.len());
    entries.insert(pos, TopEntry { key, row });
    if entries.len() > n {
        entries.truncate(n);
    }
}

fn value_to_distinct_key(v: &Value) -> DistinctKey {
    match v {
        Value::Number(n) => DistinctKey::from_f64(*n),
        Value::Str(s) => DistinctKey::from_str(s),
        Value::Bool(b) => DistinctKey::Int(if *b { 1 } else { 0 }),
        _ => DistinctKey::Str(format!("{:?}", v).into()),
    }
}

// ---------------------------------------------------------------------------
// 桶键求值（P2 复合键: Field / bucket() / tier()）
// ---------------------------------------------------------------------------

/// 由 `plan.measures` + 桶累加器计算度量值（avg 输出时 sum/count, D6）。
/// last 取字段数值（非数值 → 0; 无子集时 idx 未知 → 0, 标量访问器需子集）;
/// top 为多值不适用, 返回 0（rich close 用）。
/// SoA 桶 → 每度量 f64 值（语义与 [`measure_values`] 的 Numeric 分支一致:
/// count/sum 直取、avg = sum/count（count==0 → 0.0）、min/max unwrap_or(0)）。
/// pub(crate) 供 SoA 对照 bench。
pub(crate) fn measure_values_soa(
    plan: &StatsPlan,
    soa: &NumericSoA,
    layout: &NumericSoALayout,
) -> Vec<f64> {
    plan.measures
        .iter()
        .enumerate()
        .map(|(idx, m)| match m.agg {
            StatsAggPlan::Count => soa.counts[idx] as f64,
            StatsAggPlan::Sum => soa.sums[layout.sum_slot[idx].unwrap() as usize] as f64,
            StatsAggPlan::Avg => {
                let s = layout.sum_slot[idx].unwrap() as usize;
                let n = soa.counts[idx];
                if n == 0 {
                    0.0
                } else {
                    soa.sums[s] as f64 / n as f64
                }
            }
            StatsAggPlan::Min => {
                soa.mins[layout.min_slot[idx].unwrap() as usize].unwrap_or(0) as f64
            }
            StatsAggPlan::Max => {
                soa.maxs[layout.max_slot[idx].unwrap() as usize].unwrap_or(0) as f64
            }
            _ => unreachable!("SoA 桶仅数值度量（count/sum/avg/min/max）"),
        })
        .collect()
}

/// 桶累加器 → 每度量 f64 值（按桶形态分派）。
fn bucket_measure_values(
    plan: &StatsPlan,
    accs: &StatsBucketAccs,
    soa_layout: Option<&NumericSoALayout>,
    measure_field_idx: &[Option<usize>],
) -> Vec<f64> {
    match accs {
        StatsBucketAccs::Numeric(soa) => measure_values_soa(plan, soa, soa_layout.unwrap()),
        StatsBucketAccs::Classic(accs) => measure_values(plan, accs, measure_field_idx),
    }
}

/// 桶累加器 → close 条目列表（按桶形态分派）。SoA 全标量（无 row_fields——
/// SoA 计划不含 last/top）; Classic 走 [`bucket_measure_entries`]。
fn bucket_close_entries(
    plan: &StatsPlan,
    accs: &StatsBucketAccs,
    soa_layout: Option<&NumericSoALayout>,
    measure_field_idx: &[Option<usize>],
) -> Vec<Vec<StatsCloseEntry>> {
    match accs {
        StatsBucketAccs::Numeric(soa) => measure_values_soa(plan, soa, soa_layout.unwrap())
            .into_iter()
            .map(|v| {
                vec![StatsCloseEntry {
                    measure_value: v,
                    row_fields: None,
                }]
            })
            .collect(),
        StatsBucketAccs::Classic(accs) => plan
            .measures
            .iter()
            .zip(accs.iter())
            .zip(measure_field_idx.iter())
            .map(|((m, acc), fidx)| bucket_measure_entries(m, acc, *fidx))
            .collect(),
    }
}

fn measure_values(
    plan: &StatsPlan,
    accs: &[StatsAccum],
    measure_field_idx: &[Option<usize>],
) -> Vec<f64> {
    plan.measures
        .iter()
        .zip(accs.iter())
        .zip(measure_field_idx.iter())
        .map(|((m, acc), fidx)| match m.agg {
            StatsAggPlan::Count => acc.numeric().count as f64,
            StatsAggPlan::Sum => acc.numeric().sum as f64,
            StatsAggPlan::Avg => {
                let n = acc.numeric();
                if n.count == 0 {
                    0.0
                } else {
                    n.sum as f64 / n.count as f64
                }
            }
            StatsAggPlan::Min => acc.numeric().min.unwrap_or(0) as f64,
            StatsAggPlan::Max => acc.numeric().max.unwrap_or(0) as f64,
            StatsAggPlan::DistinctCount => match acc {
                StatsAccum::Distinct(d) => d.len() as f64,
                _ => 0.0,
            },
            StatsAggPlan::Last => match (acc.last(), fidx) {
                (Some(row), Some(i)) => row.f64_at(*i).unwrap_or(0.0),
                _ => 0.0,
            },
            StatsAggPlan::Top => 0.0,
        })
        .collect()
}

/// 每桶输出条目: 度量值 + 可选行字段紧凑存储（last/top 注入 yield 用; 标量 =
/// None）。行字段为 Arc（与状态共享, close 零拷贝; 构造 alert 时才逐值构造）。
/// 列序 = `StatsExecutor::row_field_names()`（None 子集 = schema 列序）。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsCloseEntry {
    pub measure_value: f64,
    pub row_fields: Option<std::sync::Arc<RowFields>>,
}

/// 每桶 close 输出: 每度量一个值列表（标量 = 1; top = N, 按 rank 序）。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsCloseBucket {
    pub key: ScopeKey,
    pub measures: Vec<Vec<StatsCloseEntry>>,
}

/// 单个度量的 close 条目列表（标量 = 1; last = 1 带行字段列数组; top = N 带行
/// 字段列数组）。行字段为 Arc（与状态共享, close 零拷贝; 构造 alert 时才逐值
/// 克隆）。`field_idx` = 该度量字段在列数组中的位置（P5 预计算）。
fn bucket_measure_entries(
    m: &StatsMeasurePlan,
    acc: &StatsAccum,
    field_idx: Option<usize>,
) -> Vec<StatsCloseEntry> {
    let scalar = |value: f64| StatsCloseEntry {
        measure_value: value,
        row_fields: None,
    };
    match m.agg {
        StatsAggPlan::Count => vec![scalar(acc.numeric().count as f64)],
        StatsAggPlan::Sum => vec![scalar(acc.numeric().sum as f64)],
        StatsAggPlan::Avg => {
            let n = acc.numeric();
            vec![scalar(if n.count == 0 {
                0.0
            } else {
                n.sum as f64 / n.count as f64
            })]
        }
        StatsAggPlan::Min => vec![scalar(acc.numeric().min.unwrap_or(0) as f64)],
        StatsAggPlan::Max => vec![scalar(acc.numeric().max.unwrap_or(0) as f64)],
        StatsAggPlan::DistinctCount => {
            let len = match acc {
                StatsAccum::Distinct(d) => d.len(),
                _ => 0,
            };
            vec![scalar(len as f64)]
        }
        StatsAggPlan::Last => {
            let row = acc.last();
            let value = match (row, field_idx) {
                (Some(row), Some(i)) => row.f64_at(i).unwrap_or(0.0),
                _ => 0.0,
            };
            vec![StatsCloseEntry {
                measure_value: value,
                row_fields: row.clone(),
            }]
        }
        StatsAggPlan::Top => {
            let entries = acc.top();
            if entries.is_empty() {
                // 空条目（top(0, ...) 或全部非数值键）: 不产出——n_records 由其它
                // 度量驱动; 全是 top 时整桶不产出（与 CEP 无实例无输出一致）。
                return vec![];
            }
            entries
                .iter()
                .map(|e| StatsCloseEntry {
                    measure_value: e.key,
                    row_fields: Some(std::sync::Arc::from(e.row.clone())),
                })
                .collect()
        }
    }
}

/// 行式桶键（复合键: 逐 key 求值 → Pair 组合）。任一 key 缺失/不可求值 → None。
fn eval_row_key(keys: &[Expr], row: &HashMap<String, Value>) -> Option<ScopeKey> {
    let mut acc: Option<ScopeKey> = None;
    for e in keys {
        let v = eval_row_bucket_key(e, row)?;
        acc = Some(match acc {
            None => v,
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(v)),
        });
    }
    Some(acc.unwrap_or(ScopeKey::Empty))
}

/// 单个桶键表达式求值（P2 桶键函数子集）:
/// - `Field` → 读字段（`ScopeKey::from_value` 与列式 `scope_key_columnar` 同构）
/// - `bucket(field, 'day'|'hour'|...)` → 时间下界（整数 nanos 桶）
/// - `tier(field, b1, b2, ...)` → 区间桶索引（边界升序, `v < b_i` 归属 i）
/// - 其它/不可求值 → None（行跳过）
fn eval_row_bucket_key(expr: &Expr, row: &HashMap<String, Value>) -> Option<ScopeKey> {
    match expr {
        Expr::Field(fr) => row.get(field_name(fr)).map(ScopeKey::from_value),
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } => match name.as_str() {
            "bucket" => {
                let field = args.first()?;
                let unit = bucket_unit_nanos(args.get(1)?)?;
                let v = field_value_of(field, row)?;
                let nanos = v as i64;
                Some(ScopeKey::Int((nanos / unit) * unit))
            }
            "tier" => {
                let field = args.first()?;
                let v = field_value_of(field, row)?;
                let bounds: Vec<f64> = args[1..]
                    .iter()
                    .map(|b| match b {
                        Expr::Number(n) => Some(*n),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
                Some(ScopeKey::Int(tier_index(v, &bounds)))
            }
            _ => None,
        },
        _ => None,
    }
}

fn field_value_of(expr: &Expr, row: &HashMap<String, Value>) -> Option<f64> {
    match expr {
        Expr::Field(fr) => match row.get(field_name(fr)) {
            Some(Value::Number(n)) => Some(*n),
            _ => None,
        },
        Expr::Number(n) => Some(*n),
        _ => None,
    }
}

fn bucket_unit_nanos(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::StringLit(s) => match s.as_str() {
            "day" => Some(86_400_000_000_000),
            "hour" => Some(3_600_000_000_000),
            "minute" => Some(60_000_000_000),
            "second" => Some(1_000_000_000),
            _ => None,
        },
        _ => None,
    }
}

/// 边界升序; `v < bounds[0]` → 0, `< bounds[1]` → 1, ..., 否则 `bounds.len()`。
fn tier_index(v: f64, bounds: &[f64]) -> i64 {
    bounds.iter().position(|b| v < *b).unwrap_or(bounds.len()) as i64
}

/// 行式 last/top 行字段提取（与列式 [`row_fields_from_batch`] 对齐, P5 紧凑化）:
/// 按 `names` 列序返回 `Box<[Option<Value>]>`（缺失 = `None`）。`None` = 全列,
/// 行键**排序**（确定性; 仅测试/缺省——生产经 spawn 恒有子集）。
fn row_fields_from_row(
    row: &HashMap<String, Value>,
    names: Option<&[String]>,
    layout: &std::sync::Arc<RowFieldLayout>,
) -> std::sync::Arc<RowFields> {
    let mut fields = RowFields::empty(std::sync::Arc::clone(layout));
    match names {
        Some(ns) => {
            for (i, n) in ns.iter().enumerate() {
                fields.set(i, row.get(n).cloned());
            }
        }
        None => {
            let mut keys: Vec<&String> = row.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                fields.set(i, row.get(*k).cloned());
            }
        }
    }
    std::sync::Arc::new(fields)
}

/// 从 batch 行提取字段列数组（last/top 列式路径用, P5 紧凑化）: 按 `cols`
/// （每字段列索引, 每批预解析一次——免逐行 `schema.index_of`）提取, null/缺失 =
/// `None`。`cols = None` 时全部 schema 列按字段名**排序**（与行式 None 同序——
/// 行键 == schema 字段时两路径列序一致; 测试/缺省路径）。
fn row_fields_from_batch(
    batch: &RecordBatch,
    row: usize,
    cols: Option<&[Option<usize>]>,
    layout: &std::sync::Arc<RowFieldLayout>,
) -> std::sync::Arc<RowFields> {
    let schema = batch.schema();
    let mut fields = RowFields::empty(std::sync::Arc::clone(layout));
    match cols {
        Some(cols) => {
            for (i, ci) in cols.iter().enumerate() {
                let v = match ci {
                    Some(ci) => {
                        let col = batch.column(*ci);
                        if col.is_null(row) {
                            None
                        } else {
                            extract_field_value(schema.field(*ci), col.as_ref(), row)
                        }
                    }
                    None => None, // 字段缺失 → None
                };
                fields.set(i, v);
            }
        }
        None => {
            let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            names.sort();
            for (i, name) in names.iter().enumerate() {
                let col_idx = schema.index_of(name).expect("schema 字段必存在");
                let col = batch.column(col_idx);
                if col.is_null(row) {
                    fields.set(i, None);
                } else {
                    fields.set(
                        i,
                        extract_field_value(schema.field(col_idx), col.as_ref(), row),
                    );
                }
            }
        }
    }
    std::sync::Arc::new(fields)
}

/// 从 batch 列读单行原生数值（Int64 原生 i64 → i128, 不走 f64——D8: ≥2^53 的
/// Int64 经 `Value::Number(f64)` 会丢精度; Float64 按 `sum_masked` 同口径截断）。
/// null / 非数值列 → None（与行式 `value_to_i128` 的 None 一致）。
fn column_i128_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<i128> {
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as i128);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row) as i128);
    }
    None
}

/// 从 batch 列读单行原生数值（top 快速淘汰预检用; 列索引预解析, 零 index_of）。
/// Int64 → as f64 / Float64 → 原值——与行字段提取后 `value_to_f64(Value::Number)`
/// 同口径（event_bridge 契约: Int64 → Number(i as f64), Float64 → Number(f)）。
/// 非数值类型 → None（调用方回退原路径, 语义不变）。
fn column_f64_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<f64> {
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    None
}

/// 索引版 distinct 键读取（列索引批级预解析, 免每行 schema.index_of——q17 同款修复）。
/// 与列式段 `insert_distinct_column` 同类型分派, 原生值构造（D7: 禁止
/// `Value::Number(f64)` 化 ≥2^53 的 Int64）。null / 类型不在支持集 → None。
fn column_distinct_key_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<DistinctKey> {
    use arrow::array::TimestampNanosecondArray;
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(DistinctKey::from_i64(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(DistinctKey::from_f64(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Some(DistinctKey::from_str(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return Some(DistinctKey::from_f64(if a.value(row) { 1.0 } else { 0.0 }));
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(DistinctKey::from_i64(a.value(row)));
    }
    None
}

// ---------------------------------------------------------------------------
// 列式段辅助（P1.5）
// ---------------------------------------------------------------------------

/// 行域迭代（P2 分片裁剪）: `rows` = 本片行索引（升序, 绝对行号）; `None` =
/// 全批 `0..n`。行域驱动的归并段（count/sum/minmax/distinct）只遍历本片行,
/// 消除每片对全批的 O(n) 冗余扫描（q15 输入分片 10× 冗余归因, 2026-08-24）。
fn domain_rows(rows: Option<&[u32]>, n: usize) -> Box<dyn Iterator<Item = usize> + '_> {
    match rows {
        Some(rs) => Box::new(rs.iter().map(|&r| r as usize)),
        None => Box::new(0..n),
    }
}

/// 行域内满足 where 过滤的行数（`wi` = unique_wheres 索引; `None` = 恒通过）。
/// 等价 `count_true(combine(domain, where))`——逐行查 where mask 位（null slot
/// 读 false, 与 `BooleanArray::value` 一致）。
fn count_domain(rows: Option<&[u32]>, n: usize, masks: &[BooleanArray], wi: Option<usize>) -> u64 {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    match rows {
        Some(rs) => rs.iter().filter(|&&r| passes(r as usize)).count() as u64,
        None => (0..n).filter(|&r| passes(r)).count() as u64,
    }
}

/// 行域驱动求和（null 跳过; 数值按行式 `value_to_i128` 截断, D8）。
fn sum_domain(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
) -> i128 {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    match col {
        NumCol::Int64(c) => domain_rows(rows, n)
            .filter(|&r| passes(r) && !c.is_null(r))
            .map(|r| c.value(r) as i128)
            .sum(),
        NumCol::Float64(c) => domain_rows(rows, n)
            .filter(|&r| passes(r) && !c.is_null(r))
            .map(|r| c.value(r) as i128)
            .sum(),
    }
}

/// 行域驱动 min/max（null 跳过）。
fn minmax_domain(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    min: &mut Option<i128>,
    max: &mut Option<i128>,
) {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    let fold = |v: i128, min: &mut Option<i128>, max: &mut Option<i128>| {
        *min = Some(match *min {
            Some(m) if m <= v => m,
            _ => v,
        });
        *max = Some(match *max {
            Some(m) if m >= v => m,
            _ => v,
        });
    };
    match col {
        NumCol::Int64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, min, max);
                }
            }
        }
        NumCol::Float64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, min, max);
                }
            }
        }
    }
}

/// 行域驱动的单极值更新（SoA 用: Min 度量只写 min 槽, Max 度量只写 max 槽——
/// SoA 无死状态; 语义 = [`minmax_domain`] 的对应半段）。
fn minmax_domain_one(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    is_min: bool,
    out: &mut Option<i128>,
) {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    let fold = |v: i128, out: &mut Option<i128>| {
        *out = Some(match *out {
            Some(x) if (is_min && x <= v) || (!is_min && x >= v) => x,
            _ => v,
        });
    };
    match col {
        NumCol::Int64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, out);
                }
            }
        }
        NumCol::Float64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, out);
                }
            }
        }
    }
}

/// 行域驱动的 distinct 插入（原生列值按行域 + where 过滤）——等价
/// `insert_distinct_column` 的 mask 全批扫描, 但只遍历本片行。
fn insert_distinct_domain(
    batch: &RecordBatch,
    name: &str,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    set: &mut DistinctSet,
) -> bool {
    let Some(idx) = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)
    else {
        return true; // 字段缺失 → 全 null（与行式 extract None 一致）
    };
    let col = batch.column(idx).as_ref();
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    if let Some(c) = col.as_any().downcast_ref::<Int64Array>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_i64(c.value(r)));
            }
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<Float64Array>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_f64(c.value(r)));
            }
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<StringArray>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_str(c.value(r)));
            }
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<BooleanArray>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_f64(if c.value(r) { 1.0 } else { 0.0 }));
            }
        }
        return true;
    }
    if let Some(c) = col
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
    {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_i64(c.value(r)));
            }
        }
        return true;
    }
    false
}

/// distinct 度量字段列类型支持检查（Int64/Float64/Utf8/Bool/TimestampNs）。
/// 字段缺失视为安全（与行式 extract None 一致, 不插入）。
///
/// **必须在 `process_batch` 任何累加副作用之前调用**: 段 2 中途失败返回 false
/// 时调用方会回退 `process_rows`, 此时段 1 已累加的 count/sum 会被重复计算
/// （部分应用 bug）——类型支持与否必须一次性前置判定。
fn distinct_fields_columnar_safe(batch: &RecordBatch, plan: &StatsPlan) -> bool {
    for m in &plan.measures {
        if !matches!(m.agg, StatsAggPlan::DistinctCount) {
            continue;
        }
        let Some(field) = &m.field else {
            continue;
        };
        let Some(idx) = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == field_name(field))
        else {
            continue; // 字段缺失 → 全 null
        };
        let dt = batch.column(idx).data_type();
        let supported = matches!(
            dt,
            DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Boolean
                | DataType::Timestamp(TimeUnit::Nanosecond, _)
        );
        if !supported {
            return false;
        }
    }
    true
}

/// 数值列引用（distinct 支持集之外的列式归并）。
enum NumCol<'a> {
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
}

fn numeric_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<NumCol<'a>> {
    let idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)?;
    let col = batch.column(idx).as_ref();
    if let Some(c) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(NumCol::Int64(c));
    }
    if let Some(c) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(NumCol::Float64(c));
    }
    None
}
