//! StatsExecutor 执行器主实现（消费行/批次 → StatsPlan 归并 → close 度量产出），
//! 含 accumulate_* 驱动函数。窗口/桶状态见 state.rs，累加类型见 accum.rs。

use std::collections::{HashMap, HashSet};

use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;
use wf_lang::ast::Expr;
use wf_lang::plan::{StatsAggPlan, StatsPlan};

use crate::match_engine::cep::{Event, ScopeKey, field_ref_name};
use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::spill::SpillStore;
use crate::match_engine::{EngineHashMap, Value};
use crate::window::scope_key_columnar;
use wf_cep::rows::{RowFieldLayout, RowFields};

use super::*;

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
                        crate::match_engine::executor::eval::eval_bool_expr(expr, &ctx),
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
            // SoA/Classic 桶的分支累加收敛到 row_acc 自由函数（单行/桶累加簇;
            // 调用点持有 `&mut window` 桶借用, 免整 self 借用冲突）。
            match bucket {
                StatsBucketAccs::Numeric(soa) => accumulate_row_map_soa(
                    soa,
                    soa_layout.expect("SoA 桶恒有布局"),
                    &self.plan,
                    &self.measure_where,
                    &where_ok,
                    row,
                    &extract,
                ),
                StatsBucketAccs::Classic(accs) => accumulate_row_map_classic(
                    accs,
                    &self.plan,
                    &self.measure_where,
                    &self.measure_field_idx,
                    &where_ok,
                    row,
                    &extract,
                    row_names.as_deref(),
                    &row_layout,
                    &mut row_cache,
                ),
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

    /// 列式前置检查（**必须在任何累加副作用之前**——返回 `None` 时调用方回退
    /// [`Self::process_rows`], 部分应用会把已累加的计数再算一遍）:
    /// 1. 全部 where 表达式可列式化（`eval_guard_columnar` 对不可列式表达式返回
    ///    全 false, 不可静默使用）;
    /// 2. distinct 字段列类型在支持集（段 2 失败同样造成部分应用）;
    /// 3. 桶键可列式化（全部为简单字段; 含 bucket/tier 等函数键 → 回退行式）。
    ///
    /// 通过时返回桶键列索引（`plan.keys` → schema 列号; 键字段缺失同样回退）。
    fn columnar_batch_preflight(&self, batch: &RecordBatch) -> Option<Vec<usize>> {
        for e in &self.unique_wheres {
            if !wf_lang::columnar::expr_is_columnar(e) {
                return None;
            }
        }
        if !distinct_fields_columnar_safe(batch, &self.plan) {
            return None;
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
        key_cols
    }

    fn process_batch_rows_impl(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
        mask_cache: Option<&StatsMaskCache>,
    ) -> bool {
        // 前置（见 [`Self::columnar_batch_preflight`]）: where 列式化 + distinct
        // 字段类型 + 桶键列——任一不满足 → 回退 process_rows（无累加副作用）。
        let Some(key_cols) = self.columnar_batch_preflight(batch) else {
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
        // 空键规则恒单桶（预建, 不参与限额——guard 只针对键空间膨胀）。
        let soa_layout = self.soa_layout.as_ref();
        let bucket = self
            .window
            .bucket_mut(&ScopeKey::Empty, &self.plan)
            .expect("Empty 桶恒存在");
        match bucket {
            StatsBucketAccs::Numeric(soa) => accumulate_empty_bucket_numeric(
                soa,
                soa_layout.expect("SoA 桶恒有布局"),
                &self.plan,
                &self.measure_where,
                batch,
                &masks,
                rows,
                n,
            ),
            StatsBucketAccs::Classic(accs) => accumulate_empty_bucket_classic(
                accs,
                &self.plan,
                &self.measure_where,
                batch,
                &masks,
                rows,
                n,
            ),
        }
        // 段 2: distinct/last/top 行式段（原生列值按行域 + where 过滤; last/top
        // 提取行字段供 yield 注入）——行字段 layout 在桶借用前 ensure（&mut self）。
        let row_layout = self.ensure_row_field_layout(batch);
        if !accumulate_empty_bucket_row_measures(
            &mut self.window,
            &self.plan,
            &self.measure_where,
            &self.measure_field_idx,
            batch,
            &masks,
            rows,
            n,
            row_names.as_deref(),
            row_field_cols.as_deref(),
            &row_layout,
        ) {
            return false;
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
