//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::match_engine::{Event, ScopeKey, field_ref_name};
use crate::match_engine::{EngineHashMap, EngineHashSet, Value};
use crate::window::scope_key_columnar;
use crate::window::scope_key_from_column;

// ---------------------------------------------------------------------------
// 状态结构（v6 §6.1 — 无匹配进度, 纯累加）
// ---------------------------------------------------------------------------

/// 单桶累加器。avg 不作状态——输出时 sum/count 求得（D6）。
#[derive(Debug, Clone, Default)]
pub struct StatsAccum {
    pub count: u64,
    pub sum_i128: i128,
    pub min: Option<i128>,
    pub max: Option<i128>,
    pub distinct_set: Option<EngineHashSet<DistinctKey>>,
    /// `last(field)` 用（Q18）: 最近合格行的**行字段列数组**（P5 紧凑化——按
    /// `row_field_names` 列序存储, 缺失/null = `None`; 旧 `EngineHashMap` 每桶
    /// 6 个 SmolStr key + hash 节点 ≈ 400B+/桶, 5.29M 桶直接顶到 ~19GB）。
    /// 每次合格行替换（流有序 = 事件时间最新）。**Arc 跨同桶多个 last 度量共享**;
    /// `Arc<[T]>` 单块分配（Arc 头 + 数组同块, 免 Arc→Box→数组两层间接）。
    pub last_row: Option<std::sync::Arc<[Option<Value>]>>,
    /// `top(N, field)` 用（Q19）: 按 key DESC 有序的 top-N 条目（含行字段列数组）。
    pub top_entries: Option<Vec<TopEntry>>,
}

/// top-N 条目: 排序键 + 行字段列数组（yield 经 field_values 注入读 `b.*`）。
#[derive(Debug, Clone)]
pub struct TopEntry {
    /// 排序键（数值; 与行式 `value_to_f64` 同口径）。
    pub key: f64,
    /// 条目行字段列数组（null 跳过, 与行式 Event 一致; 列序 = `row_field_names`）。
    pub row: Box<[Option<Value>]>,
}

/// Distinct key: 从列式原生值构造（i64/timestamp 域内哈希, D7）——
/// 禁止 f64 化（ValueKey::from_value 的 >2^53 分歧）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DistinctKey {
    Int(i64),
    /// 非整数数值（小数）—— 保持原 f64 位（canonical）。
    Float(u64),
    Str(Box<str>),
}

impl DistinctKey {
    pub fn from_i64(v: i64) -> Self {
        DistinctKey::Int(v)
    }
    pub fn from_f64(v: f64) -> Self {
        if v.fract() == 0.0 && v.abs() < 9_007_199_254_740_992.0 {
            DistinctKey::Int(v as i64)
        } else {
            DistinctKey::Float(if v == 0.0 {
                0.0f64.to_bits()
            } else if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            })
        }
    }
    #[allow(clippy::should_implement_trait)] // 与 from_i64/from_f64 平行的构造器命名，非 FromStr 实现
    pub fn from_str(s: &str) -> Self {
        DistinctKey::Str(s.into())
    }
}

// ---------------------------------------------------------------------------
// StatsExecutor — 执行状态
// ---------------------------------------------------------------------------

/// 窗口状态: 桶 → 度量累加器数组（索引对齐 `StatsPlan.measures`）。
/// 空键规则恒单桶（`ScopeKey::Empty` 键, P1 快路径不变）; 带 key（P2）每
/// `(key 组合)` 一桶。
///
/// **复合键优化（P5+）**: 桶表键为**扁平哈希 u64** + 碰撞链 `Vec<StatsBucket>`——
/// 列式路径每事件只做「栈上叶数组 + 哈希 + 链扫描」, 无每事件 `ScopeKey::Pair`
/// Box 分配; 完整 `ScopeKey` 仅**每桶首见**时构建一次（Q18: 27.6M → 5.29M 次盒装）。
/// 哈希为字节级同构 FNV 混合（`scope_key_hash` == `comps_hash`, 行式/列式两路径
/// 同桶）; 碰撞由链内 `ScopeKey` 完整比较消歧（概率极低, 正确性不受影响）。
#[derive(Default)]
pub struct StatsWindowState {
    pub buckets: EngineHashMap<u64, Vec<StatsBucket>>,
    pub window_start_nanos: i64,
    pub last_event_nanos: i64,
    pub event_count: u64,
    /// 状态内存上限（字节; None = 不设防）。由规则 `limits.max_memory` 注入
    /// （spawn 层）。**超限后拒绝新建桶**（已有桶继续累积, 内存有界）——语义上
    /// 是「新键丢失」的优雅降级, 有日志 + 计数可观测, 不是静默膨胀到 OOM。
    limit_bytes: Option<u64>,
    /// 估算的在用状态内存（桶级预算模型: 新桶固定 allowance, 含 top/last 条目
    /// 预算——保守上界, 偏安全方向）。窗口 close 时清零。
    estimated_bytes: u64,
    /// 累计超限拒收的新桶数（跨窗口累计, 供指标/告警）。
    over_limit_new_buckets: u64,
    /// 当前窗口是否已告警（每窗口一次, 防刷屏）。
    limit_warned: bool,
    /// 告警用的规则名（set_memory_limit 注入）。
    rule_name: String,
}

impl StatsWindowState {
    /// 新建窗口状态（无内存限制, 由 spawn 层按规则 limits 注入）。
    fn new(buckets: EngineHashMap<u64, Vec<StatsBucket>>) -> Self {
        StatsWindowState {
            buckets,
            window_start_nanos: 0,
            last_event_nanos: 0,
            event_count: 0,
            limit_bytes: None,
            estimated_bytes: 0,
            over_limit_new_buckets: 0,
            limit_warned: false,
            rule_name: String::new(),
        }
    }

    /// 注入状态内存上限（字节; None = 不设防）。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.rule_name = rule_name.to_string();
        self.limit_bytes = bytes.map(|b| b as u64);
    }

    /// 当前估算的在用状态内存（桶级预算）。
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// 累计因超限被拒收的新桶数。
    pub fn over_limit_new_buckets(&self) -> u64 {
        self.over_limit_new_buckets
    }

    /// 新桶预算（保守上界）: 固定基数 + 每度量结构 + top/last 条目预算
    /// （含行字段列数组）。
    ///
    /// **已知限制（文档注记）**: `distinct_set` 不在预算内——带 key + distinct 规则
    /// 下每桶 distinct 集按值增长（无界）, guard 只限桶数不限每桶 distinct 集。
    /// 这是有意取舍: 逐条目记账需侵入累加热路径且估算复杂; 现网规则集（q 系列）
    /// 带 key 规则均无 distinct（q15 的 distinct 是空键单桶, 内存本身固定）。
    fn bucket_allowance(plan: &StatsPlan, n_measures: usize) -> u64 {
        let mut bytes = 512u64 + n_measures as u64 * 128;
        for m in &plan.measures {
            match m.agg {
                StatsAggPlan::Top => {
                    bytes += m.arg.unwrap_or(10) * 160;
                }
                StatsAggPlan::Last => bytes += 160,
                _ => {}
            }
        }
        bytes
    }

    /// 新建桶前的限额检查: 超限 → 计数 + 每窗口告警一次 + 拒绝（false）。
    ///
    /// **计数口径（按行/尝试, 非按新键）**: 被拒的键不建桶 → 后续同键行仍走
    /// 查找未命中 → 每次尝试都计数。这是有意取舍——「每新键一次」需记录被拒键
    /// 集合（无界, 违背 guard 的内存有界承诺）; 按行计数不引入新状态, 只对
    /// 已在桶内的键不计数（命中）。告警/metrics 的 `over_limit_new_buckets`
    /// 实际含义是「被拒行数」。
    fn account_new_bucket(&mut self, plan: &StatsPlan, n_measures: usize) -> bool {
        let allowance = Self::bucket_allowance(plan, n_measures);
        if let Some(limit) = self.limit_bytes
            && self.estimated_bytes + allowance > limit
        {
            self.over_limit_new_buckets += 1;
            if !self.limit_warned {
                self.limit_warned = true;
                log::warn!(
                    "stats 状态内存超限（规则 {}, 估算 {}B / 上限 {}B）——拒绝新建键桶, 已有桶继续累积; 累计拒收 {} 行（新桶尝试）",
                    self.rule_name,
                    self.estimated_bytes,
                    limit,
                    self.over_limit_new_buckets
                );
            }
            return false;
        }
        self.estimated_bytes += allowance;
        true
    }
}

/// 单桶: 完整 [`ScopeKey`]（close 排序/输出; 每桶一次构建）+ 累加器数组。
#[derive(Debug, Clone)]
pub struct StatsBucket {
    pub scope_key: ScopeKey,
    pub accs: Vec<StatsAccum>,
}

impl StatsWindowState {
    /// 预建空键单桶（`ScopeKey::Empty`）——哈希路径 `bucket_mut(&Empty)` 命中。
    fn seed_empty_bucket(buckets: &mut EngineHashMap<u64, Vec<StatsBucket>>, n_measures: usize) {
        buckets.insert(
            scope_key_hash(&ScopeKey::Empty),
            vec![StatsBucket {
                scope_key: ScopeKey::Empty,
                accs: vec![StatsAccum::default(); n_measures],
            }],
        );
    }

    /// 取/建一个桶（完整键路径: 行式回退 / 空键规则用）。哈希与列式
    /// `keyed_bucket_mut` 同值, 链内按 ScopeKey 完整比较消歧。
    /// 新桶先过限额检查（超限 → None, 调用方跳过该行——内存有界）。
    fn bucket_mut(
        &mut self,
        key: &ScopeKey,
        plan: &StatsPlan,
        n_measures: usize,
    ) -> Option<&mut Vec<StatsAccum>> {
        let hash = scope_key_hash(key);
        // 先只读查找（entry 可变借用会与限额记账的 &mut self 冲突）。
        let pos = self
            .buckets
            .get(&hash)
            .and_then(|chain| chain.iter().position(|b| &b.scope_key == key));
        if let Some(i) = pos {
            return Some(&mut self.buckets.get_mut(&hash).expect("命中即存在")[i].accs);
        }
        if !self.account_new_bucket(plan, n_measures) {
            return None;
        }
        let chain = self.buckets.entry(hash).or_default();
        chain.push(StatsBucket {
            scope_key: key.clone(),
            accs: vec![StatsAccum::default(); n_measures],
        });
        Some(&mut chain.last_mut().expect("just pushed").accs)
    }

    /// 取/建一个桶（列式扁平键路径）: `hash` = 叶数组哈希, `comps` = 栈上叶
    /// 数组（列序）。链内按 `comps` 与完整键比较消歧; 未命中时构建完整键
    /// （每桶一次）。新桶先过限额检查（超限 → None）。
    fn keyed_bucket_mut(
        &mut self,
        hash: u64,
        comps: &[ScopeKey],
        plan: &StatsPlan,
        n_measures: usize,
    ) -> Option<&mut Vec<StatsAccum>> {
        let pos = self.buckets.get(&hash).and_then(|chain| {
            chain
                .iter()
                .position(|b| comps_match(&b.scope_key, comps, 0, comps.len()))
        });
        if let Some(i) = pos {
            return Some(&mut self.buckets.get_mut(&hash).expect("命中即存在")[i].accs);
        }
        if !self.account_new_bucket(plan, n_measures) {
            return None;
        }
        let chain = self.buckets.entry(hash).or_default();
        let scope_key = scope_key_from_comps(comps);
        chain.push(StatsBucket {
            scope_key,
            accs: vec![StatsAccum::default(); n_measures],
        });
        Some(&mut chain.last_mut().expect("just pushed").accs)
    }

    /// 按完整键取桶（测试/调试用; 生产走哈希路径）。
    pub fn find_bucket(&self, key: &ScopeKey) -> Option<&Vec<StatsAccum>> {
        self.buckets
            .get(&scope_key_hash(key))
            .and_then(|chain| chain.iter().find(|b| &b.scope_key == key))
            .map(|b| &b.accs)
    }

    /// 清空并拍平全部桶（close 用）: `(ScopeKey, accs)` 按 ScopeKey 升序。
    /// 同时清零内存账本（新窗口重新累积; 拒收计数保留——指标用）。
    fn take_buckets(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        let mut out: Vec<(ScopeKey, Vec<StatsAccum>)> = std::mem::take(&mut self.buckets)
            .into_values()
            .flat_map(|chain| chain.into_iter().map(|b| (b.scope_key, b.accs)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        self.estimated_bytes = 0;
        self.limit_warned = false;
        out
    }
}

/// 执行器: 消费行/批次, 按 StatsPlan 归并, 窗口 close 时产出度量值。
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
}

impl StatsExecutor {
    pub fn new(plan: StatsPlan) -> Self {
        Self::with_row_fields(plan, None)
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
        let n = plan.measures.len();
        // 空键规则预建 Empty 桶（快路径; 带 key 惰性建桶）。
        let mut buckets = EngineHashMap::default();
        if plan.keys.is_empty() {
            StatsWindowState::seed_empty_bucket(&mut buckets, n);
        }
        Self {
            plan,
            window: StatsWindowState::new(buckets),
            watermark_nanos: 0,
            unique_wheres,
            measure_where,
            row_field_names,
            measure_field_idx,
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
        // where 结果缓存: 行间复用 buffer（无逐行分配）; 无 where 规则时保持空。
        let mut where_ok: Vec<bool> = Vec::with_capacity(self.unique_wheres.len());
        let n_measures = self.plan.measures.len();
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
            let mut row_cache: Option<std::sync::Arc<[Option<Value>]>> = None;
            // 新桶超限（内存 guard）→ 该行跳过（与列式路径一致）。
            let Some(bucket) = self.window.bucket_mut(&bucket_key, &self.plan, n_measures) else {
                continue;
            };
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                if let Some(wi) = self.measure_where[idx]
                    && !where_ok[wi]
                {
                    continue;
                }
                let acc = &mut bucket[idx];
                acc.count += 1;
                if let Some(field) = &measure.field {
                    if let Some(val) = extract(row, field_name(field)) {
                        match measure.agg {
                            StatsAggPlan::Count => {}
                            StatsAggPlan::Sum | StatsAggPlan::Avg => {
                                if let Some(n) = value_to_i128(&val) {
                                    acc.sum_i128 += n;
                                }
                            }
                            StatsAggPlan::Min => {
                                if let Some(n) = value_to_i128(&val) {
                                    acc.min = Some(match acc.min {
                                        Some(m) if m <= n => m,
                                        _ => n,
                                    });
                                }
                            }
                            StatsAggPlan::Max => {
                                if let Some(n) = value_to_i128(&val) {
                                    acc.max = Some(match acc.max {
                                        Some(m) if m >= n => m,
                                        _ => n,
                                    });
                                }
                            }
                            StatsAggPlan::DistinctCount => {
                                let key = value_to_distinct_key(&val);
                                acc.distinct_set
                                    .get_or_insert_with(EngineHashSet::default)
                                    .insert(key);
                            }
                            StatsAggPlan::Last | StatsAggPlan::Top => {
                                // 行式路径: 按 row_names 列序提取（与列式
                                // row_fields_from_batch 对齐; 同桶多 last 度量 Arc
                                // 共享 1 份内存）。
                                let row = row_cache.get_or_insert_with(|| {
                                    row_fields_from_row(row, row_names.as_deref())
                                });
                                let fidx = measure_field_position(
                                    &self.plan,
                                    &self.measure_field_idx,
                                    idx,
                                    row_names.as_deref(),
                                );
                                apply_last_top(acc, measure, row, fidx);
                            }
                        }
                    } else if matches!(measure.agg, StatsAggPlan::Last | StatsAggPlan::Top) {
                        // 字段缺失: last 仍保留整行（yield 读其它字段）, top 无键跳过
                        if measure.agg == StatsAggPlan::Last {
                            let row = row_cache.get_or_insert_with(|| {
                                row_fields_from_row(row, row_names.as_deref())
                            });
                            acc.last_row = Some(std::sync::Arc::clone(row));
                        }
                    }
                }
            }
            self.window.event_count += 1;
        }
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
                    measure_values(&self.plan, &b.accs, &self.measure_field_idx),
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
        let buckets = self.window.take_buckets();
        let out: Vec<StatsCloseBucket> = buckets
            .into_iter()
            .map(|(key, accs)| StatsCloseBucket {
                key,
                measures: self
                    .plan
                    .measures
                    .iter()
                    .zip(accs.iter())
                    .zip(self.measure_field_idx.iter())
                    .map(|((m, acc), fidx)| bucket_measure_entries(m, acc, *fidx))
                    .collect(),
            })
            .collect();
        self.reset_window();
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
        let n = self.plan.measures.len();
        let mut buckets = EngineHashMap::default();
        if self.plan.keys.is_empty() {
            StatsWindowState::seed_empty_bucket(&mut buckets, n);
        }
        let limit = self.window.limit_bytes;
        let rule_name = self.window.rule_name.clone();
        let over_limit = self.window.over_limit_new_buckets;
        self.window = StatsWindowState::new(buckets);
        // 保留限额配置 + 拒收计数跨窗口（guard 持续生效; 计数供指标/告警）。
        self.window.set_memory_limit(&rule_name, limit.map(|b| b as usize));
        self.window.over_limit_new_buckets = over_limit;
    }

    /// 注入状态内存上限（字节; None = 不设防）——超限拒收新键桶, 已有桶继续。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.window.set_memory_limit(rule_name, bytes);
    }

    /// 提取本片已关闭窗口的**原始累加状态**（输入分区分片归并用）并重置窗口。
    /// 返回 `(桶原始状态, 本片事件数)`——协调片把它合并进自己的窗口后再 close。
    /// 仅空键/可交换度量（count/sum/min/max/distinct）分片使用（last/top 被
    /// spawn 门控排除——行序敏感不可归并）。
    pub fn take_partial(&mut self) -> (Vec<(ScopeKey, Vec<StatsAccum>)>, u64) {
        let buckets = self.window.take_buckets();
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
    pub fn merge_partial(&mut self, buckets: Vec<(ScopeKey, Vec<StatsAccum>)>, event_count: u64) {
        let n = self.plan.measures.len();
        for (key, accs) in buckets {
            // 超限（guard）→ 该片该键跳过（协调片侧同样受桶预算约束）。
            let Some(target) = self.window.bucket_mut(&key, &self.plan, n) else {
                continue;
            };
            for (t, o) in target.iter_mut().zip(accs.iter()) {
                merge_accum(t, o);
            }
        }
        self.window.event_count += event_count;
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
        let n = batch.num_rows();
        // 行域（P2 分片）: `rows` = 本片拥有的行索引子集（绝对行号, 升序）。
        // 归并段（段 1d/段 2）改为**行域驱动**（count_domain/sum_domain/
        // insert_distinct_domain 只遍历本片行）——不再构建全批 domain mask。
        let view = ColumnarBatch::from_all_fields(batch);
        // 段 1: where 列式 mask（去重后唯一条件, 每批一次）
        let masks: Vec<BooleanArray> = self
            .unique_wheres
            .iter()
            .map(|e| eval_guard_columnar(e, &view))
            .collect();
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
            );
        }
        // 段 1d: 纯归并度量整列累加（**行域驱动**——2026-08-24 分片裁剪: 只遍历
        // 本片行, 消除每片对全批的 O(n) 冗余扫描; 全批路径 `rows=None` 行为不变）。
        // 行式语义: 满足 where 的行对**每个**度量都 `count += 1`（在字段读取前）
        // ——avg 的 count 必须与 sum 同步累加, 否则 avg = sum/count 输出 0（D6）。
        let n_measures = self.plan.measures.len();
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            let wi = self.measure_where[idx];
            // 空键规则恒单桶（预建, 不参与限额——guard 只针对键空间膨胀）。
            let acc =
                &mut self.window.bucket_mut(&ScopeKey::Empty, &self.plan, n_measures).expect("Empty 桶恒存在")[idx];
            let rows_in = count_domain(rows, n, &masks, wi);
            match measure.agg {
                StatsAggPlan::Count => {
                    acc.count += rows_in;
                }
                StatsAggPlan::Sum | StatsAggPlan::Avg => {
                    acc.count += rows_in;
                    if let Some(field) = &measure.field
                        && let Some(col) = numeric_col(batch, field_name(field))
                    {
                        acc.sum_i128 += sum_domain(&col, rows, n, &masks, wi);
                    }
                }
                StatsAggPlan::Min | StatsAggPlan::Max => {
                    acc.count += rows_in;
                    if let Some(field) = &measure.field
                        && let Some(col) = numeric_col(batch, field_name(field))
                    {
                        minmax_domain(&col, rows, n, &masks, wi, &mut acc.min, &mut acc.max);
                    }
                }
                StatsAggPlan::DistinctCount => {
                    // count 与行式一致维护（输出只用 distinct_set; 状态保持等价）
                    acc.count += rows_in;
                }
                StatsAggPlan::Last | StatsAggPlan::Top => {
                    // P1 不实现（Q18/Q19 扩展）
                }
            }
        }
        // 段 2: distinct/last/top 行式段（原生列值按行域 + where 过滤; last/top 提取
        // 行字段供 yield 注入）
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            if !matches!(
                measure.agg,
                StatsAggPlan::DistinctCount | StatsAggPlan::Last | StatsAggPlan::Top
            ) {
                continue;
            }
            let wi = self.measure_where[idx];
            let acc =
                &mut self.window.bucket_mut(&ScopeKey::Empty, &self.plan, n_measures).expect("Empty 桶恒存在")[idx];
            if matches!(measure.agg, StatsAggPlan::DistinctCount) {
                let Some(field) = &measure.field else {
                    continue;
                };
                let set = acc.distinct_set.get_or_insert_with(EngineHashSet::default);
                if !insert_distinct_domain(batch, field_name(field), rows, n, &masks, wi, set) {
                    return false;
                }
            } else {
                // last/top: 逐行按行域 + where 更新（子集行字段提取; 空键 last 规则少用）
                let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
                for r in domain_rows(rows, n).filter(|&r| passes(r)) {
                    let row = row_fields_from_batch(batch, r, row_field_cols.as_deref());
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
    ) -> bool {
        let n_measures = self.plan.measures.len();
        match rows {
            Some(rs) => {
                for &r in rs {
                    if (r as usize) >= n {
                        continue; // 防御: 越界行号（与 materialize_rows 一致跳过）
                    }
                    self.accumulate_keyed_row(
                        batch,
                        masks,
                        key_cols,
                        key_columns,
                        r as usize,
                        n_measures,
                        row_names,
                        row_field_cols,
                    );
                }
            }
            None => {
                for row in 0..n {
                    self.accumulate_keyed_row(
                        batch,
                        masks,
                        key_cols,
                        key_columns,
                        row,
                        n_measures,
                        row_names,
                        row_field_cols,
                    );
                }
            }
        }
        true
    }

    /// 单行桶归并（P2 复合键逐行路径的公共主体, 供全批/行域两分支复用）。
    ///
    /// 字段读取走**原生列值**（`column_i128`/`column_distinct_key`）——与空键
    /// 列式段同精度（D7/D8: ≥2^53 的 Int64 不得经 `Value::Number(f64)` 舍入;
    /// Timestamp 列 distinct 也走原生 i64, 不得静默跳过）。
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
        n_measures: usize,
        row_names: Option<&[String]>,
        row_field_cols: Option<&[Option<usize>]>,
    ) {
        const MAX_STACK_KEYS: usize = 4;
        if key_columns.len() <= MAX_STACK_KEYS {
            let mut comps: [ScopeKey; MAX_STACK_KEYS] = std::array::from_fn(|_| ScopeKey::Empty);
            for (i, kc) in key_columns.iter().enumerate() {
                let Some(c) = key_column_comp(kc, batch, row) else {
                    return; // 键 null → 跳过
                };
                comps[i] = c;
            }
            let comps = &comps[..key_columns.len()];
            let hash = comps_hash(comps);
            // 新桶超限（内存 guard）→ 该行跳过。
            let Some(bucket) = self.window.keyed_bucket_mut(hash, comps, &self.plan, n_measures)
            else {
                return;
            };
            accumulate_column_row(
                bucket,
                &self.plan,
                &self.measure_where,
                &self.measure_field_idx,
                row_names,
                row_field_cols,
                batch,
                masks,
                row,
            );
            self.window.event_count += 1; // 归并成功才计数（对齐行式路径）
            return;
        }
        let Some(key) = scope_key_columnar(batch, key_cols, row) else {
            return; // 键 null → 跳过
        };
        // 新桶超限（内存 guard）→ 该行跳过。
        let Some(bucket) = self.window.bucket_mut(&key, &self.plan, n_measures) else {
            return;
        };
        accumulate_column_row(
            bucket,
            &self.plan,
            &self.measure_where,
            &self.measure_field_idx,
            row_names,
            row_field_cols,
            batch,
            masks,
            row,
        );
        self.window.event_count += 1; // 归并成功才计数（对齐行式路径）
    }
}

/// 单行桶累加主体（列式路径; 自由函数——调用点持有 `&mut self.window` 桶借用,
/// 方法会整 self 借用冲突）。
///
/// last/top 行字段每行懒提取一次, 多度量共享同一 Arc（Q18 4 个 last 度量内存
/// 1 份; 提取列序 = row_names, 免整行 8 字段）。
#[allow(clippy::too_many_arguments)] // 单行桶累加: 桶/计划/掩码索引/行字段/列/行号 6 组参数
fn accumulate_column_row(
    bucket: &mut [StatsAccum],
    plan: &StatsPlan,
    measure_where: &[Option<usize>],
    measure_field_idx: &[Option<usize>],
    row_names: Option<&[String]>,
    row_field_cols: Option<&[Option<usize>]>,
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
) {
    let mut row_cache: Option<std::sync::Arc<[Option<Value>]>> = None;
    for (idx, measure) in plan.measures.iter().enumerate() {
        if let Some(wi) = measure_where[idx]
            && !masks[wi].value(row)
        {
            continue;
        }
        let acc = &mut bucket[idx];
        acc.count += 1;
        let Some(field) = &measure.field else {
            continue;
        };
        match measure.agg {
            StatsAggPlan::Count => {}
            StatsAggPlan::Sum | StatsAggPlan::Avg => {
                if let Some(nn) = column_i128(batch, field_name(field), row) {
                    acc.sum_i128 += nn;
                }
            }
            StatsAggPlan::Min => {
                if let Some(nn) = column_i128(batch, field_name(field), row) {
                    acc.min = Some(match acc.min {
                        Some(m) if m <= nn => m,
                        _ => nn,
                    });
                }
            }
            StatsAggPlan::Max => {
                if let Some(nn) = column_i128(batch, field_name(field), row) {
                    acc.max = Some(match acc.max {
                        Some(m) if m >= nn => m,
                        _ => nn,
                    });
                }
            }
            StatsAggPlan::DistinctCount => {
                if let Some(k) = column_distinct_key(batch, field_name(field), row) {
                    acc.distinct_set
                        .get_or_insert_with(EngineHashSet::default)
                        .insert(k);
                }
            }
            StatsAggPlan::Last | StatsAggPlan::Top => {
                let row = row_cache
                    .get_or_insert_with(|| row_fields_from_batch(batch, row, row_field_cols));
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
fn merge_accum(t: &mut StatsAccum, o: &StatsAccum) {
    t.count += o.count;
    t.sum_i128 += o.sum_i128;
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
    match (&mut t.distinct_set, &o.distinct_set) {
        (Some(ts), Some(os)) => ts.extend(os.iter().cloned()),
        (None, Some(os)) => t.distinct_set = Some(os.clone()),
        _ => {}
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
    row: &std::sync::Arc<[Option<Value>]>,
    field_idx: Option<usize>,
) {
    match measure.agg {
        StatsAggPlan::Last => {
            acc.last_row = Some(std::sync::Arc::clone(row));
        }
        StatsAggPlan::Top => {
            let Some(key) = field_idx
                .and_then(|i| row.get(i))
                .and_then(|v| v.as_ref())
                .and_then(value_to_f64)
            else {
                return; // 非数值键 → 跳过（与 sum 跳过非数值一致）
            };
            let n = measure.arg.unwrap_or(10) as usize;
            if n == 0 {
                return; // top(0, ...): 不保留任何条目
            }
            let entries = acc.top_entries.get_or_insert_with(Vec::new);
            // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目
            // 必插在既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义
            // 不变, 免去每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
            if entries.len() == n && key <= entries[n - 1].key {
                return;
            }
            // Arc<[T]> 深拷贝为独立 Box（top 条目各自的行, 不共享）
            insert_top(entries, key, row.as_ref().to_vec().into_boxed_slice(), n);
        }
        _ => {}
    }
}

/// top-N 插入: key DESC 有序保留前 N; 同 key 新条目插在已有同 key 条目之后
/// （先到者在前）。n=0 时清空（top(0, ...) 边界）。
fn insert_top(entries: &mut Vec<TopEntry>, key: f64, row: Box<[Option<Value>]>, n: usize) {
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
            StatsAggPlan::Count => acc.count as f64,
            StatsAggPlan::Sum => acc.sum_i128 as f64,
            StatsAggPlan::Avg => {
                if acc.count == 0 {
                    0.0
                } else {
                    acc.sum_i128 as f64 / acc.count as f64
                }
            }
            StatsAggPlan::Min => acc.min.unwrap_or(0) as f64,
            StatsAggPlan::Max => acc.max.unwrap_or(0) as f64,
            StatsAggPlan::DistinctCount => acc.distinct_set.as_ref().map_or(0, HashSet::len) as f64,
            StatsAggPlan::Last => match (&acc.last_row, fidx) {
                (Some(row), Some(i)) => row
                    .get(*i)
                    .and_then(|v| v.as_ref())
                    .and_then(value_to_f64)
                    .unwrap_or(0.0),
                _ => 0.0,
            },
            StatsAggPlan::Top => 0.0,
        })
        .collect()
}

/// 每桶输出条目: 度量值 + 可选行字段列数组（last/top 注入 yield 用; 标量 =
/// None）。行字段为 Arc（与状态共享, close 零拷贝; 构造 alert 时才逐值克隆）。
/// 列序 = `StatsExecutor::row_field_names()`（None 子集 = schema 列序）。
#[derive(Debug, Clone)]
pub struct StatsCloseEntry {
    pub measure_value: f64,
    pub row_fields: Option<std::sync::Arc<[Option<Value>]>>,
}

/// 每桶 close 输出: 每度量一个值列表（标量 = 1; top = N, 按 rank 序）。
#[derive(Debug, Clone)]
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
        StatsAggPlan::Count => vec![scalar(acc.count as f64)],
        StatsAggPlan::Sum => vec![scalar(acc.sum_i128 as f64)],
        StatsAggPlan::Avg => vec![scalar(if acc.count == 0 {
            0.0
        } else {
            acc.sum_i128 as f64 / acc.count as f64
        })],
        StatsAggPlan::Min => vec![scalar(acc.min.unwrap_or(0) as f64)],
        StatsAggPlan::Max => vec![scalar(acc.max.unwrap_or(0) as f64)],
        StatsAggPlan::DistinctCount => {
            vec![scalar(
                acc.distinct_set.as_ref().map_or(0, HashSet::len) as f64
            )]
        }
        StatsAggPlan::Last => {
            let value = match (&acc.last_row, field_idx) {
                (Some(row), Some(i)) => row
                    .get(i)
                    .and_then(|v| v.as_ref())
                    .and_then(value_to_f64)
                    .unwrap_or(0.0),
                _ => 0.0,
            };
            vec![StatsCloseEntry {
                measure_value: value,
                row_fields: acc.last_row.clone(),
            }]
        }
        StatsAggPlan::Top => match &acc.top_entries {
            Some(entries) if !entries.is_empty() => entries
                .iter()
                .map(|e| StatsCloseEntry {
                    measure_value: e.key,
                    row_fields: Some(std::sync::Arc::from(e.row.clone())),
                })
                .collect(),
            // 空条目（top(0, ...) 或全部非数值键）: 不产出——n_records 由其它
            // 度量驱动; 全是 top 时整桶不产出（与 CEP 无实例无输出一致）。
            _ => vec![],
        },
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
) -> std::sync::Arc<[Option<Value>]> {
    let vals: Vec<Option<Value>> = match names {
        Some(ns) => ns.iter().map(|n| row.get(n).cloned()).collect(),
        None => {
            let mut keys: Vec<&String> = row.keys().collect();
            keys.sort();
            keys.into_iter().map(|k| row.get(k).cloned()).collect()
        }
    };
    std::sync::Arc::from(vals) // 单块分配: Arc 头 + 数组同块
}

/// 从 batch 行提取字段列数组（last/top 列式路径用, P5 紧凑化）: 按 `cols`
/// （每字段列索引, 每批预解析一次——免逐行 `schema.index_of`）提取, null/缺失 =
/// `None`。`cols = None` 时全部 schema 列按字段名**排序**（与行式 None 同序——
/// 行键 == schema 字段时两路径列序一致; 测试/缺省路径）。
fn row_fields_from_batch(
    batch: &RecordBatch,
    row: usize,
    cols: Option<&[Option<usize>]>,
) -> std::sync::Arc<[Option<Value>]> {
    let schema = batch.schema();
    let mut fields: Vec<Option<Value>> = Vec::with_capacity(cols.map_or(0, |c| c.len()));
    match cols {
        Some(cols) => {
            for ci in cols {
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
                fields.push(v);
            }
        }
        None => {
            let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            names.sort();
            for name in names {
                let col_idx = schema.index_of(name).expect("schema 字段必存在");
                let col = batch.column(col_idx);
                if col.is_null(row) {
                    fields.push(None);
                } else {
                    fields.push(extract_field_value(
                        schema.field(col_idx),
                        col.as_ref(),
                        row,
                    ));
                }
            }
        }
    }
    std::sync::Arc::from(fields) // 单块分配: Arc 头 + 数组同块
}

/// 从 batch 列读单行原生数值（Int64 原生 i64 → i128, 不走 f64——D8: ≥2^53 的
/// Int64 经 `Value::Number(f64)` 会丢精度; Float64 按 `sum_masked` 同口径截断）。
/// null / 非数值列 → None（与行式 `value_to_i128` 的 None 一致）。
fn column_i128(batch: &RecordBatch, name: &str, row: usize) -> Option<i128> {
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
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

/// 从 batch 列读单行原生 distinct 键（与列式段 `insert_distinct_column` 同类型
/// 分派, 原生值构造——D7: 禁止 `Value::Number(f64)` 化 ≥2^53 的 Int64）。
/// null / 类型不在支持集 → None（与行式 extract None 一致）。
fn column_distinct_key(batch: &RecordBatch, name: &str, row: usize) -> Option<DistinctKey> {
    use arrow::array::TimestampNanosecondArray;
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
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

/// 行域驱动的 distinct 插入（原生列值按行域 + where 过滤）——等价
/// `insert_distinct_column` 的 mask 全批扫描, 但只遍历本片行。
fn insert_distinct_domain(
    batch: &RecordBatch,
    name: &str,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    set: &mut EngineHashSet<DistinctKey>,
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
