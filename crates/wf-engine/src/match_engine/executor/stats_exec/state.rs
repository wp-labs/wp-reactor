//! 窗口状态（v6 §6.4）：桶表 + spill 驱逐/读回。

use std::collections::{HashSet, VecDeque};

use wf_cep::rows::{RowFieldLayout, RowFields};
use wf_lang::plan::{StatsAggPlan, StatsPlan};

use crate::match_engine::EngineHashMap;
use crate::match_engine::cep::ScopeKey;
use crate::match_engine::spill::SpillStore;

use super::{
    NumericAccum, NumericSoA, NumericSoALayout, StatsAccum, comps_match, merge_accum,
    scope_key_from_comps, scope_key_hash,
};

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
///
/// 惰性 spill store 创建规格（P0 修复 2026-08-27）：路径 + 行字段 layout。
/// 纯数据（`PathBuf` + `Arc<RowFieldLayout>`，天然 Send + Sync）——首次驱逐时
/// 才 `RedbSpillStore::create`（零驱逐窗口不建库/不起写 worker，零开销）。
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub(crate) struct SpillCreateSpec {
    pub(crate) path: std::path::PathBuf,
    pub(crate) layout: std::sync::Arc<RowFieldLayout>,
}

#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsWindowState {
    pub buckets: EngineHashMap<u64, Vec<StatsBucket>>,
    pub window_start_nanos: i64,
    pub last_event_nanos: i64,
    pub event_count: u64,
    /// 状态内存上限（字节; None = 不设防）。由规则 `limits.max_memory` 注入
    /// （spawn 层）。**超限后拒绝新建桶**（已有桶继续累积, 内存有界）——语义上
    /// 是「新键丢失」的优雅降级, 有日志 + 计数可观测, 不是静默膨胀到 OOM。
    /// **规则级全局语义**（2026-08-27）：同规则全部分片共享一个 `mem_used`
    /// 计数器——`max_memory` 是用户配置的规则总驻留上限（分片数是引擎内部
    /// 细节, 用户不可见）。
    pub(crate) limit_bytes: Option<u64>,
    /// 共享已用状态内存计数器（跨分片; None = 未配置共享 → 用本地
    /// `estimated_bytes`, 测试/单片退化）。检查/驱逐/记账全部走它。
    mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// 估算的在用状态内存（桶级预算模型: 新桶固定 allowance, 含 top/last 条目
    /// 预算——保守上界, 偏安全方向）。窗口 close 时清零。**本片账本**——
    /// 共享模式下与共享计数同步增减, 诊断/close 扣减用。
    pub(crate) estimated_bytes: u64,
    /// 累计超限拒收的新桶数（跨窗口累计, 供指标/告警）。
    pub(crate) over_limit_new_buckets: u64,
    /// 当前窗口是否已告警（每窗口一次, 防刷屏）。
    limit_warned: bool,
    /// 告警用的规则名（set_memory_limit 注入）。
    pub(crate) rule_name: String,
    /// 状态外溢存储（M3，`docs/design/stats-state-spill-redb.md`）。None = 未配置
    /// spill（Noop 语义，热路径零开销）。
    pub(crate) spill: Option<Box<dyn SpillStore + Send + Sync>>,
    /// 已 spill 键的存在性索引（hot path 未命中时 O(1) 查，不碰持久层）。
    pub(crate) spill_index: HashSet<u64>,
    /// 落盘字节上限（None = 不限）。三层预算阶梯第二层（内存→磁盘→拒收兜底）。
    /// **规则级全局语义**（2026-08-27）：同规则全部分片共享一个 `spill_used`
    /// 计数器——`max_disk` 是用户配置的规则总落盘上限（分片数是引擎
    /// 内部细节，用户不可见）。
    pub(crate) spill_limit_bytes: Option<u64>,
    /// 共享已落盘字节计数器（跨分片；None = 未配置 spill）。
    pub(crate) spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// spill 写失败/满后的拒收回退标记（避免反复尝试写）。
    spill_failed: bool,
    /// 落盘满/写失败的告警标记（每窗口一次，防刷屏）。
    spill_warned: bool,
    /// 惰性 spill store 创建规格（P0 修复 2026-08-27）：配置了 `spill` 但未驱逐
    /// 的窗口不建 redb 库/写 worker——q19 100M 实测 17 窗口 × 10 片 = 170 次
    /// create/cleanup churn → RSS +6GB。首次驱逐（`account_new_bucket` 超限
    /// 落盘）时才 take 并创建；零驱逐窗口恒 None → 零开销。
    pub(crate) spill_create: Option<SpillCreateSpec>,
    /// 时钟队列（近似 LRU）：桶**创建序**的 hash 环。驱逐扫描队首：
    /// 二次机会（touch > 0）→ 递减回队尾；否则驱逐。每在内存键至多一个条目。
    /// pub(crate) 供 spill 测试断言快速路径不维护（无 spill 时恒空）。
    pub(crate) clock: VecDeque<u64>,
    /// 已读回（take）的键 hash（M5-2）：take 只读不删——redb 中旧条目在 close
    /// 时按此集合过滤（内存副本更新，避免重复输出）。与 spill_index 互补：
    /// 读回 → 出 spill_index 入 readback；再驱逐 → 入 spill_index 出 readback。
    pub(crate) readback: HashSet<u64>,
    /// 累计驱逐键数（跨窗口，指标/抖动观测用）。
    pub(crate) spill_evictions: u64,
    /// 累计读回次数（跨窗口，指标/抖动观测用）。
    pub(crate) spill_readbacks: u64,
    /// 驱逐分段耗时（ns，跨窗口累计；性能定位用——扫描/clone/redb 写三段的占比）。
    pub(crate) spill_scan_ns: u64,
    pub(crate) spill_clone_ns: u64,
    pub(crate) spill_write_ns: u64,
    /// 驱逐调用次数（分段耗时的分母）。
    pub(crate) spill_evict_calls: u64,
    /// 纯数值计划 SoA 布局（None = 含 distinct/last/top, 走 Classic 累加器）。
    /// 窗口重建（reset）后不变——按 plan 重算。
    pub(crate) soa_layout: Option<NumericSoALayout>,
}

impl StatsWindowState {
    /// 新建窗口状态（无内存限制, 由 spawn 层按规则 limits 注入）。空键规则
    /// 在此预建 Empty 单桶（快路径）。
    pub(crate) fn new(buckets: EngineHashMap<u64, Vec<StatsBucket>>, plan: &StatsPlan) -> Self {
        // 全数值计划（count/sum/avg/min/max）→ SoA 桶; 含 distinct/last/top → Classic。
        let soa_layout = plan
            .measures
            .iter()
            .all(|m| {
                matches!(
                    m.agg,
                    StatsAggPlan::Count
                        | StatsAggPlan::Sum
                        | StatsAggPlan::Avg
                        | StatsAggPlan::Min
                        | StatsAggPlan::Max
                )
            })
            .then(|| NumericSoALayout::build(plan));
        let mut buckets = buckets;
        if buckets.is_empty() && plan.keys.is_empty() {
            StatsWindowState::seed_empty_bucket(&mut buckets, plan, soa_layout.as_ref());
        }
        StatsWindowState {
            buckets,
            window_start_nanos: 0,
            last_event_nanos: 0,
            event_count: 0,
            limit_bytes: None,
            mem_used_shared: None,
            estimated_bytes: 0,
            over_limit_new_buckets: 0,
            limit_warned: false,
            rule_name: String::new(),
            spill: None,
            spill_index: HashSet::new(),
            spill_limit_bytes: None,
            spill_used: None,
            spill_failed: false,
            spill_warned: false,
            spill_create: None,
            clock: VecDeque::new(),
            readback: HashSet::new(),
            spill_evictions: 0,
            spill_readbacks: 0,
            spill_scan_ns: 0,
            spill_clone_ns: 0,
            spill_write_ns: 0,
            spill_evict_calls: 0,
            soa_layout,
        }
    }

    /// 注入状态内存上限（字节; None = 不设防）。未注入共享计数（测试/直接
    /// 调用）→ 本片独立预算（共享语义退化为单片, 与旧行为一致）。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.set_memory_limit_shared(rule_name, bytes, None);
    }

    /// 注入状态内存上限（规则级共享版）：`mem_used_shared` = 同规则全部分片
    /// 共用一个内存占用计数——`max_memory` 是规则总驻留上限, 分片数是引擎
    /// 内部细节（spawn 层规则级创建, 分片 clone 注入）。
    pub fn set_memory_limit_shared(
        &mut self,
        rule_name: &str,
        bytes: Option<usize>,
        mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.rule_name = rule_name.to_string();
        self.limit_bytes = bytes.map(|b| b as u64);
        self.mem_used_shared = mem_used_shared;
    }

    /// 已用状态内存（检查口径）：共享计数读值（规则级）或本片估算（未共享）。
    fn mem_used_bytes(&self) -> u64 {
        self.mem_used_shared
            .as_ref()
            .map(|u| u.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(self.estimated_bytes)
    }

    /// 已用状态内存入账（共享计数同步; 未共享时只走本地 `estimated_bytes`）。
    fn mem_add(&self, n: u64) {
        if let Some(u) = &self.mem_used_shared {
            u.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// 已用状态内存出账（共享计数同步; 未共享时只走本地）。
    pub(crate) fn mem_sub(&self, n: u64) {
        if let Some(u) = &self.mem_used_shared {
            u.fetch_sub(n, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// 当前估算的在用状态内存（桶级预算）。
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// distinct 集合每项估算字节（2026-08-26 q16）：`HashSet<i64>` 8B/项 +
    /// foldhash 控制字/负载因子（87.5% 满）≈ 12B；others（enum 16B + 同开销）
    /// ≈ 24B。统一取 24B（保守上界，guard 宁可早拒）。
    const DISTINCT_ENTRY_BYTES: u64 = 24;

    /// 批末刷新状态内存估算（2026-08-26 q16）：
    /// `estimated_bytes = 桶数×allowance + Σ桶 distinct len×DISTINCT_ENTRY_BYTES`
    /// ——distinct 集合此前完全不计（q16 带 key + 8 distinct_count 的 19G 实际
    /// vs 8GB 估算形同虚设）。O(桶数) 每批（q16 ~10k 桶 × 2857 批可接受）。
    /// guard 检查（新建桶）用刷新后的值，反映真实。
    pub(crate) fn refresh_estimated_bytes(&mut self, plan: &StatsPlan) {
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        let mut distinct_bytes = 0u64;
        for buckets in self.buckets.values() {
            for bucket in buckets {
                if let StatsBucketAccs::Classic(accs) = &bucket.accs {
                    for acc in accs {
                        if let StatsAccum::Distinct(set) = acc {
                            distinct_bytes += set.len() as u64 * Self::DISTINCT_ENTRY_BYTES;
                        }
                    }
                }
            }
        }
        let new = self.buckets.len() as u64 * allowance + distinct_bytes;
        // 共享计数同步差值（本片账本刷新 → 共享计数跟随）。
        let old = self.estimated_bytes;
        if new >= old {
            self.mem_add(new - old);
        } else {
            self.mem_sub(old - new);
        }
        self.estimated_bytes = new;
    }

    /// 累计因超限被拒收的新桶数。
    pub fn over_limit_new_buckets(&self) -> u64 {
        self.over_limit_new_buckets
    }

    /// 新桶预算（保守上界）: 固定基数 + Σ每度量变体 + 行字段共享份额 +
    /// top/last 条目预算。`soa` = 纯数值计划（SoA 桶）——平行数组紧凑口径。
    ///
    /// **2026-08-26 q18 校准**（对齐度量专用累加器）: 旧口径 512 + n×128 +
    /// last 160B/度量 → 1664B/键，高估真实 1.55× → 16GB 预算拒收阈值 961 万
    /// 键 < 30M 数据键数 2300 万 → **静默丢键**（over_limit_new_buckets）。
    /// 现按变体实际求和 + 行字段**每桶共享 1 份**（last/top 度量同桶同一
    /// [`RowFields`] Arc——`row_cache` 每行 1 份）。
    ///
    /// **2026-08-27 SoA 校准**: SoA 桶无 Box/枚举——固定基数 + counts n×8 +
    /// sums n_sum×16 + mins n_min×16 + maxs n_max×16（q17 8 度量 → 384B, 旧
    /// Classic 口径 896B 高估 2.3×——预算小/键多时误拒）。1.6× 余量同 Classic。
    ///
    /// **已知限制**: `distinct_set` 值域增长不在固定基数内（q16 教训）——由
    /// [`Self::refresh_estimated_bytes`] 批末按真实 len 计入（保守上界）。
    /// pub(crate) 供 stats_spill_test 按计划算桶预算（SoA/Classic 口径不同）。
    pub(crate) fn bucket_allowance(plan: &StatsPlan, soa: bool) -> u64 {
        // 桶固定: ScopeKey 栈+堆(~72B) + StatsBucket 头 + accs Vec + HashMap
        // 槽(~64B) ≈ 160B → 取 256（1.6× 保守余量）。
        let mut bytes = 256u64;
        if soa {
            // SoA 桶: 平行数组紧凑布局（无 Box 无枚举）——按类型子集求和。
            let n = plan.measures.len() as u64;
            let (mut n_sum, mut n_min, mut n_max) = (0u64, 0u64, 0u64);
            for m in &plan.measures {
                match m.agg {
                    StatsAggPlan::Sum | StatsAggPlan::Avg => n_sum += 1,
                    StatsAggPlan::Min => n_min += 1,
                    StatsAggPlan::Max => n_max += 1,
                    StatsAggPlan::Count => {}
                    _ => unreachable!("SoA 仅数值度量"),
                }
            }
            return bytes + n * 8 + n_sum * 16 + n_min * 16 + n_max * 16;
        }
        let mut has_row_fields = false;
        for m in &plan.measures {
            match m.agg {
                StatsAggPlan::Top => {
                    bytes += 24; // Vec<TopEntry> 头
                    bytes += m.arg.unwrap_or(10) * 160; // 条目（key + 行字段）
                    has_row_fields = true;
                }
                StatsAggPlan::Last => {
                    bytes += 16; // Option<Arc<RowFields>>
                    has_row_fields = true;
                }
                StatsAggPlan::DistinctCount => bytes += 96, // DistinctSet（Box 外）
                _ => bytes += 80,                           // NumericAccum（Box 外）
            }
        }
        if has_row_fields {
            bytes += 112; // 共享 1 份 RowFields 堆（q18 6 字段 ≈ 104B + 余量）
        }
        // 校准系数 2.2（2026-08-27 q18 实测）：估算 432B/键 vs 实际 960B/键
        // ——低估来源 = StatsAccum enum 实际 24B（估 16）+ RowFields 实测 168B
        // （估 112）+ hashbrown 桶数组/容量 + mimalloc 16B 对齐×每分配 + Vec
        // 容量。估算低估 → 驱逐水位失真（实际 2.2× 预算才驱逐）。
        // 常量（非 env）：热路径每新键调用, 且避免测试 env 竞态。
        (bytes as f64 * 2.2).round() as u64
    }
    /// 新建桶前的限额检查: 超限 → 先尝试 spill 腾空间（M3 三层预算阶梯第二层）
    /// → 仍超限 → 计数 + 每窗口告警一次 + 拒绝（false）。
    ///
    /// **计数口径（按行/尝试, 非按新键）**: 被拒的键不建桶 → 后续同键行仍走
    /// 查找未命中 → 每次尝试都计数。这是有意取舍——「每新键一次」需记录被拒键
    /// 集合（无界, 违背 guard 的内存有界承诺）; 按行计数不引入新状态, 只对
    /// 已在桶内的键不计数（命中）。告警/metrics 的 `over_limit_new_buckets`
    /// 实际含义是「被拒行数」。
    fn account_new_bucket(&mut self, plan: &StatsPlan) -> bool {
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        if let Some(limit) = self.limit_bytes
            && self.mem_used_bytes() + allowance > limit
        {
            // 惰性创建（P0 修复）：首次驱逐前才建 store——零驱逐窗口零开销
            // （不建 redb 库/不起写 worker, q19 100M 曾 RSS +6GB）。spec 由
            // executor 每窗口 process 时注册（layout 解析后）。
            if self.spill.is_none()
                && !self.spill_failed
                && let Some(spec) = self.spill_create.take()
            {
                let store =
                    crate::match_engine::spill::RedbSpillStore::create(&spec.path, spec.layout)
                        .unwrap_or_else(|e| {
                            panic!("spill redb 创建失败(致命) {}: {e}", spec.path.display())
                        });
                self.spill = Some(Box::new(store));
            }
            // spill 启用且未失败: 先驱逐最老键腾空间（批量, 目标降到上限 90%）。
            if self.spill.is_some() && !self.spill_failed {
                self.evict_to_spill(plan);
                if self.mem_used_bytes() + allowance <= limit {
                    self.estimated_bytes += allowance;
                    self.mem_add(allowance);
                    return true;
                }
                // 落盘满/写失败 → 落到拒收兜底（下面）
            }
            self.over_limit_new_buckets += 1;
            if !self.limit_warned {
                self.limit_warned = true;
                log::warn!(
                    "stats 状态内存超限（规则 {}, 估算 {}B / 上限 {}B, spill {}{}）——拒绝新建键桶, 已有桶继续累积; 累计拒收 {} 行（新桶尝试）",
                    self.rule_name,
                    self.mem_used_bytes(),
                    limit,
                    if self.spill.is_some() {
                        "已满/失败"
                    } else {
                        "未启用"
                    },
                    self.spill_limit_bytes
                        .map(|b| format!(" 落盘 {}/{}B", self.spill_used_bytes(), b))
                        .unwrap_or_default(),
                    self.over_limit_new_buckets
                );
            }
            return false;
        }
        self.estimated_bytes += allowance;
        self.mem_add(allowance);
        true
    }

    /// 已落盘字节（共享计数器读值；诊断/告警）。
    fn spill_used_bytes(&self) -> u64 {
        self.spill_used
            .as_ref()
            .map(|u| u.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// 注入状态外溢存储（窗口开始时由 spawn 层调用）。
    /// `store = None` 关闭 spill（Noop 语义）。`max_spill_bytes = None` 不限落盘。
    /// `spill_used` = 规则级共享落盘计数器（同规则全部分片共用一个——
    /// `max_disk` 是规则总上限, 分片数是引擎内部细节）。
    pub fn set_spill(
        &mut self,
        store: Option<Box<dyn SpillStore + Send + Sync>>,
        max_spill_bytes: Option<usize>,
        spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.spill = store;
        self.spill_limit_bytes = max_spill_bytes.map(|b| b as u64);
        // 未注入规则级共享计数（测试/直接调用）但启用了 store → 自建本片独立
        // 计数：预算检查/记账口径不变（共享语义退化为单片, 与旧行为一致）。
        if self.spill.is_some() && spill_used.is_none() {
            self.spill_used = Some(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)));
        } else {
            self.spill_used = spill_used;
        }
        self.spill_index.clear();
        self.spill_failed = false;
        self.spill_warned = false;
        self.clock.clear();
        self.readback.clear();
    }

    /// 累计驱逐键数（跨窗口；抖动观测——驱逐多 + 读回多 = 抖动）。
    pub fn spill_evictions(&self) -> u64 {
        self.spill_evictions
    }

    /// 当前桶数（诊断）。
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// 累计读回次数（跨窗口；抖动观测）。
    pub fn spill_readbacks(&self) -> u64 {
        self.spill_readbacks
    }

    /// 驱逐分段耗时（跨窗口累计；性能定位）：(扫描 ns, clone ns, 写 ns, 调用次数)。
    pub fn spill_profile_ns(&self) -> (u64, u64, u64, u64) {
        (
            self.spill_scan_ns,
            self.spill_clone_ns,
            self.spill_write_ns,
            self.spill_evict_calls,
        )
    }

    /// 实际内存字节（诊断/估算校准）：遍历桶表计算真实驻留（含树/盒堆、
    /// 字符串、HashMap 条目），与 `estimated_bytes`（bucket_allowance 估算）
    /// 对比，校准驱逐水位（估算低估 → 实际超预算才驱逐）。
    pub fn actual_bytes(&self) -> u64 {
        fn scope_key_bytes(key: &ScopeKey) -> u64 {
            match key {
                ScopeKey::Empty => 16,
                ScopeKey::Int(_) | ScopeKey::Float(_) => 24,
                ScopeKey::Str(s) => 40 + s.len() as u64,
                ScopeKey::Pair(a, b) => 24 + scope_key_bytes(a) + scope_key_bytes(b),
            }
        }
        fn row_fields_bytes(rf: &RowFields) -> u64 {
            // 固定：struct 头(Arc 8 + 3 Box 8) + 4 Box 头(16×4) ≈ 112
            112 + rf.numeric().len() as u64 * 8
                + rf.strings().len() as u64 * 24 // SmolStr 内联
                + rf.others().len() as u64 * 56
                + rf.null_mask().len() as u64 * 8
        }
        fn soa_bytes(soa: &NumericSoA) -> u64 {
            48 + soa.counts.len() as u64 * 8
                + soa.sums.len() as u64 * 16
                + soa.mins.len() as u64 * 16
                + soa.maxs.len() as u64 * 16
        }
        let mut total = 0u64;
        for chain in self.buckets.values() {
            for b in chain {
                // StatsBucket: scope_key + touch + accs 载体 + 对齐
                total += scope_key_bytes(&b.scope_key) + 64;
                match &b.accs {
                    StatsBucketAccs::Numeric(soa) => total += soa_bytes(soa),
                    StatsBucketAccs::Classic(accs) => {
                        for acc in accs {
                            total += match acc {
                                StatsAccum::Numeric(_) => 8 + 48,
                                StatsAccum::Last(Some(rf)) => 16 + row_fields_bytes(rf),
                                StatsAccum::Last(None) => 16,
                                StatsAccum::Distinct(_) => 8 + 96,
                                StatsAccum::Top(t) => 24 + t.len() as u64 * 176,
                            };
                        }
                    }
                }
            }
            total += 24; // hashbrown 条目（hash + 值 + ctrl）
        }
        total
    }

    /// 驱逐最老未更新键到 spill（clock 二次机会近似 LRU），直到内存预算降到
    /// `min(上限-单桶, 上限 90%)`（滞后带：既要放得下新桶，又避免每新键都驱逐）。
    ///
    /// 落盘上限已满/写失败 → 置 `spill_failed`, 由 `account_new_bucket` 落到
    /// 拒收兜底（不丢内存键）。
    fn evict_to_spill(&mut self, plan: &StatsPlan) {
        let Some(limit) = self.limit_bytes else {
            return;
        };
        // 落盘上限已到（规则级共享计数）→ 停止驱逐（拒收兜底）。
        if let Some(sl) = self.spill_limit_bytes
            && self.spill_used_bytes() >= sl
        {
            self.warn_spill_full();
            return;
        }
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        // 驱逐目标: 上限-单桶 与 上限 90% 取小（前者保证新桶放得下）。
        let target = limit
            .saturating_sub(allowance)
            .min(limit.saturating_mul(9) / 10);
        let mut batch: Vec<(u64, ScopeKey, Vec<StatsAccum>)> = Vec::new();
        let mut batch_hashes: Vec<u64> = Vec::new();
        // **逐链预订驱逐**（2026-08-27 修复过度驱逐）：每选一个链就原子扣减
        // 共享内存计数（`mem_sub`）——共享计数成为**单一事实源**, 循环条件用
        // 实时值。修复前 `pending` 是每片局部：10 片并发超限时每片各驱逐水位差
        // （25GB 配置下每片驱逐 2.5GB × 10 = 25GB, 需求仅 3.2GB——过度驱逐
        // 10×, 驱逐耗时 scan+clone 2.6s/片同步阻塞热路径, EPS 反降）。
        // 逐链原子扣减后: 多片并发时共享计数停在 target, 总驱逐 = 超限部分。
        // 写盘失败/满时按 `reserved` 归还（驱逐未生效, 内存键未删）。
        let mut reserved = 0u64;
        // 防活锁: 最多扫 (TOUCH_MAX+2)× 时钟长度（全活跃时停止——拒收兜底正确）。
        let max_scan = self.clock.len().saturating_mul(TOUCH_MAX as usize + 2);
        let mut scanned = 0usize;
        let mut scan_ns = 0u64;
        let mut clone_ns = 0u64;
        while self.mem_used_bytes() > target && scanned < max_scan {
            let s0 = std::time::Instant::now();
            let Some(h) = self.clock.pop_front() else {
                break;
            };
            scanned += 1;
            let Some(chain) = self.buckets.get_mut(&h) else {
                continue; // 链已不在（close 分批取走）
            };
            // 二次机会计数（M5-2）: 本轮所有桶 touch 递减 1（命中已刷新回 MAX），
            // 全 0 才驱逐——活跃键多轮保护，死键 MAX 轮内自然衰减。
            if chain.iter().any(|b| b.touch > 0) {
                for b in chain.iter_mut() {
                    b.touch = b.touch.saturating_sub(1);
                }
                self.clock.push_back(h);
                scan_ns += s0.elapsed().as_nanos() as u64;
                continue;
            }
            // 驱逐整链（先 clone 进 batch；落盘成功后才从桶表移除——写失败
            // 不丢内存键）。accs 载体 → spill 序列化向量（Classic clone / SoA 还原）。
            let c0 = std::time::Instant::now();
            let chain_len = chain.len();
            for b in chain.iter() {
                batch.push((
                    h,
                    b.scope_key.clone(),
                    accs_to_spill_vec(&b.accs, self.soa_layout.as_ref(), plan),
                ));
            }
            batch_hashes.push(h);
            scan_ns += s0.elapsed().as_nanos() as u64;
            clone_ns += c0.elapsed().as_nanos() as u64;
            // 预订驱逐（chain 借用已结束; 本片账本 + 共享计数同步扣减）。
            let chain_bytes = allowance * chain_len as u64;
            self.estimated_bytes = self.estimated_bytes.saturating_sub(chain_bytes);
            self.mem_sub(chain_bytes);
            reserved += chain_bytes;
        }
        if batch.is_empty() {
            if reserved > 0 {
                self.estimated_bytes = self.estimated_bytes.saturating_add(reserved);
                self.mem_add(reserved);
            }
            return;
        }
        // 落盘预算检查（写入前）：超出则归还预订 + 拒收兜底（规则级共享计数）。
        let add_bytes = batch_hashes.len() as u64 * allowance;
        if let Some(sl) = self.spill_limit_bytes
            && self.spill_used_bytes() + add_bytes > sl
        {
            self.estimated_bytes = self.estimated_bytes.saturating_add(reserved);
            self.mem_add(reserved);
            self.warn_spill_full();
            return;
        }
        // 单事务批量写。
        let w0 = std::time::Instant::now();
        let result = self
            .spill
            .as_mut()
            .expect("调用方已确认 spill 启用")
            .put_batch(batch);
        self.spill_write_ns += w0.elapsed().as_nanos() as u64;
        self.spill_scan_ns += scan_ns;
        self.spill_clone_ns += clone_ns;
        self.spill_evict_calls += 1;
        if let Err(e) = result {
            // 写失败（磁盘满/IO）→ 归还预订（驱逐未生效, 内存键未删）+
            // 回退拒收（§5 三层阶梯兜底），不丢内存键。
            self.estimated_bytes = self.estimated_bytes.saturating_add(reserved);
            self.mem_add(reserved);
            self.spill_failed = true;
            log::error!("spill 写失败(规则 {}): {e}——回退拒收新键", self.rule_name);
            return;
        }
        // 落盘成功 → 从桶表移除 + 落盘记账（内存占用已在预订时从共享计数
        // 扣减——此处只加落盘字节, 不再重复扣内存）。内存/spill 不相交不变量
        // 成立。
        for h in &batch_hashes {
            if let Some(chain) = self.buckets.remove(h) {
                let n = chain.len() as u64;
                if let Some(u) = &self.spill_used {
                    u.fetch_add(allowance * n, std::sync::atomic::Ordering::SeqCst);
                }
            }
            self.spill_index.insert(*h);
            self.readback.remove(h); // 键已回 redb（覆盖旧条目）——close 不再过滤它
        }
        self.spill_evictions += batch_hashes.len() as u64;
    }

    /// 落盘满/失败告警（每窗口一次）。
    fn warn_spill_full(&mut self) {
        if self.spill_warned {
            return;
        }
        self.spill_warned = true;
        log::warn!(
            "spill 落盘上限/写失败（规则 {}, 已落盘 {}B / 上限 {:?}）——停止驱逐, 回退拒收新键",
            self.rule_name,
            self.spill_used_bytes(),
            self.spill_limit_bytes
        );
    }

    /// close 前把 spill 全部并入内存桶。**take 只读化（M5-2）后**：redb 中
    /// 仍有已读回键的旧条目——drain 后按 `readback` 集合过滤（内存副本更新，
    /// 每个键恰好一次）。并入后走原有 `take_buckets` / `take_buckets_up_to` 路径。
    fn merge_spill_into_buckets(&mut self, plan: &StatsPlan) {
        let Some(spill) = &mut self.spill else { return };
        if spill.is_empty() {
            return;
        }
        let drained = spill.drain();
        self.spill_index.clear();
        self.clock.clear();
        // 规则级共享计数（2026-08-27）：本窗落盘的键随 close 并入内存 → 从共享
        // 计数扣减（每键 allowance 与驱逐记账同口径）。readback 键在读回时已扣过
        // （redb 残留旧条目, 此处跳过）——只扣实际并入的键。
        let mut merged_n = 0u64;
        for (key, accs) in drained {
            let hash = scope_key_hash(&key);
            if self.readback.contains(&hash) {
                continue; // 已读回且在内存（副本更新）——跳过 redb 旧条目
            }
            merged_n += 1;
            let chain = self
                .buckets
                .entry(hash)
                .or_insert_with(|| Vec::with_capacity(1));
            // 不变量: 非 readback 的 spill 键与内存不相交——但防御性合并。
            match chain.iter_mut().find(|b| b.scope_key == key) {
                Some(existing) => {
                    // 双方统一转 spill 向量合并（Classic 直接; SoA 转出再转回）。
                    let mut e = accs_to_spill_vec(&existing.accs, self.soa_layout.as_ref(), plan);
                    for (t, o) in e.iter_mut().zip(accs.iter()) {
                        merge_accum(t, o);
                    }
                    existing.accs = vec_to_bucket_accs(e, self.soa_layout.as_ref());
                }
                None => chain.push(StatsBucket {
                    scope_key: key,
                    accs: vec_to_bucket_accs(accs, self.soa_layout.as_ref()),
                    touch: TOUCH_MAX,
                }),
            }
        }
        if let Some(u) = &self.spill_used {
            u.fetch_sub(
                merged_n * Self::bucket_allowance(plan, self.soa_layout.is_some()),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        self.readback.clear();
    }
    /// 分批读回 spill 键（流式 close, M5-3）：store 游标续读 + readback 过滤
    /// （take 只读后 redb 残留旧条目, 内存副本更新——跳过）。批内顺序无要求
    /// （调用方排序）。
    pub(crate) fn spill_drain_up_to(
        &mut self,
        n: usize,
        plan: &StatsPlan,
    ) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        let Some(spill) = &mut self.spill else {
            return Vec::new();
        };
        let drained = spill.drain_up_to(n);
        let mut out = Vec::with_capacity(drained.len());
        for (key, accs) in drained {
            let hash = scope_key_hash(&key);
            if self.readback.contains(&hash) {
                continue; // 已读回且在内存（副本更新）——跳过 redb 旧条目
            }
            out.push((key, accs));
        }
        // 共享计数扣减：只扣实际读回的键（readback 键已在读回时扣过）。
        if let Some(u) = &self.spill_used {
            u.fetch_sub(
                out.len() as u64 * Self::bucket_allowance(plan, self.soa_layout.is_some()),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        out
    }
}

/// 累加器载体 → spill 序列化向量（Classic 直接 clone; Numeric SoA 按 plan
/// 逐度量还原 [`NumericAccum`]——spill 序列化契约是 `Vec<StatsAccum>`）。
/// 自由函数（非方法）: 驱逐/归并时 `buckets` 链被借用, 不可再借 `&self`。
fn accs_to_spill_vec(
    accs: &StatsBucketAccs,
    layout: Option<&NumericSoALayout>,
    plan: &StatsPlan,
) -> Vec<StatsAccum> {
    match accs {
        StatsBucketAccs::Classic(v) => v.clone(),
        StatsBucketAccs::Numeric(soa) => {
            let layout = layout.expect("Numeric 载体即有布局");
            (0..plan.measures.len())
                .map(|i| {
                    let count = soa.counts[i];
                    let sum = layout.sum_slot[i]
                        .map(|s| soa.sums[s as usize])
                        .unwrap_or(0);
                    let min = layout.min_slot[i].and_then(|s| soa.mins[s as usize]);
                    let max = layout.max_slot[i].and_then(|s| soa.maxs[s as usize]);
                    StatsAccum::Numeric(Box::new(NumericAccum {
                        count,
                        sum,
                        min,
                        max,
                    }))
                })
                .collect()
        }
    }
}

/// spill 读回向量 → 累加器载体（按 soa_layout 分派：SoA 计划还原
/// [`NumericSoA`], Classic 计划原样放回）。自由函数（同 [`accs_to_spill_vec`]）。
pub(crate) fn vec_to_bucket_accs(
    accs: Vec<StatsAccum>,
    soa_layout: Option<&NumericSoALayout>,
) -> StatsBucketAccs {
    let Some(layout) = soa_layout else {
        return StatsBucketAccs::Classic(accs);
    };
    let mut soa = layout.zeros();
    for (i, acc) in accs.iter().enumerate() {
        let StatsAccum::Numeric(n) = acc else {
            panic!("SoA 计划 spill 读回应为 Numeric 累加器(致命)");
        };
        soa.counts[i] = n.count;
        if let Some(s) = layout.sum_slot[i] {
            soa.sums[s as usize] = n.sum;
        }
        if let Some(s) = layout.min_slot[i] {
            soa.mins[s as usize] = n.min;
        }
        if let Some(s) = layout.max_slot[i] {
            soa.maxs[s as usize] = n.max;
        }
    }
    StatsBucketAccs::Numeric(soa)
}

/// 时钟二次机会计数上限（M5-2）：命中置此值，驱逐扫描递减到 0 才驱逐。
const TOUCH_MAX: u8 = 3;

/// 流式 close drain 批大小上限（M5-3）：默认 5 万键/批（≈35MB 反序列化驻留）。
/// `WF_SPILL_DRAIN_CHUNK` 可调。与输出 chunk 解耦——输出批大无妨, 读回批
/// 必须小（q18 30M close 峰值 43GB → 22GB 的直接原因）。
pub(crate) const SPILL_DRAIN_CHUNK: usize = 50_000;

/// 限额记账（2026-08-27 拆为自由函数）: `entry` 匹配内 `buckets` 已被占用时,
/// 限额字段（estimated_bytes/over_limit/limit_warned/rule_name）与 `buckets`
/// 借用不相交, 可同时访问——语义与旧 `account_new_bucket` 方法完全一致。
/// **碰撞路径 + 无 spill 快速路径专用**：链被借用时无法调 `&mut self`。
/// **规则级共享计数（2026-08-27 合并修复）**：超限判断与入账走共享计数
/// （`mem_used_shared` 传入, 与 `account_new_bucket`/`mem_add` 同口径）——
/// 否则无 spill 快速路径下 B 片本片 0 键不超限, 破坏「A 占满 B 拒收」规则语义。
fn account_bucket_allowed(
    limit_bytes: Option<u64>,
    mem_used_shared: Option<&std::sync::atomic::AtomicU64>,
    estimated_bytes: &mut u64,
    over_limit_new_buckets: &mut u64,
    limit_warned: &mut bool,
    rule_name: &str,
    allowance: u64,
) -> bool {
    // 超限判断口径 = `mem_used_bytes()`: 共享计数（规则级）或本片估算。
    let used = mem_used_shared
        .map(|u| u.load(std::sync::atomic::Ordering::SeqCst))
        .unwrap_or(*estimated_bytes);
    if let Some(limit) = limit_bytes
        && used + allowance > limit
    {
        *over_limit_new_buckets += 1;
        if !*limit_warned {
            *limit_warned = true;
            log::warn!(
                "stats 状态内存超限（规则 {}, 估算 {}B / 上限 {}B）——拒绝新建键桶, 已有桶继续累积; 累计拒收 {} 行（新桶尝试）",
                rule_name,
                used,
                limit,
                over_limit_new_buckets
            );
        }
        return false;
    }
    *estimated_bytes += allowance;
    if let Some(u) = mem_used_shared {
        u.fetch_add(allowance, std::sync::atomic::Ordering::SeqCst);
    }
    true
}

/// 桶累加器载体: 纯数值计划 → [`Numeric`](StatsBucketAccs::Numeric)（SoA, q17
/// 形态）; 含 distinct/last/top → [`Classic`](StatsBucketAccs::Classic)（原有
/// [`StatsAccum`] 数组）。分派在累积/读取/合并入口各一次（每行, 非每度量）。
/// spill 序列化以 [`StatsWindowState::accs_to_spill_vec`] 统一转 `Vec<StatsAccum>`。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.StatsEngine")]
pub enum StatsBucketAccs {
    Numeric(NumericSoA),
    Classic(Vec<StatsAccum>),
}

/// 单桶: 完整 [`ScopeKey`]（close 排序/输出; 每桶一次构建）+ 累加器载体 +
/// 时钟二次机会计数。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsBucket {
    pub scope_key: ScopeKey,
    pub accs: StatsBucketAccs,
    /// 时钟（clock）算法的二次机会计数（M5-2：bool → u8，0..=TOUCH_MAX）。
    /// 命中置 TOUCH_MAX，驱逐扫描递减，到 0 才驱逐——活跃键最多保 3 轮
    /// （q18 每键回访 3.4 次，1 位二次机会过早踢掉活跃键致驱逐-回访抖动）。
    /// spill 未启用时恒 0，零开销。pub(crate) 供 tests/ bench 构造。
    pub(crate) touch: u8,
}

/// 新桶累加器载体（按计划形态分派）: 全数值 → SoA; 含 distinct/last/top →
/// Classic（原有 [`StatsAccum`] 数组）。
fn new_bucket_accs(plan: &StatsPlan, soa_layout: Option<&NumericSoALayout>) -> StatsBucketAccs {
    match soa_layout {
        Some(layout) => StatsBucketAccs::Numeric(layout.zeros()),
        None => StatsBucketAccs::Classic(StatsAccum::accs_for_plan(plan)),
    }
}

impl StatsWindowState {
    /// 预建空键单桶（`ScopeKey::Empty`）——哈希路径 `bucket_mut(&Empty)` 命中。
    fn seed_empty_bucket(
        buckets: &mut EngineHashMap<u64, Vec<StatsBucket>>,
        plan: &StatsPlan,
        soa_layout: Option<&NumericSoALayout>,
    ) {
        buckets.insert(
            scope_key_hash(&ScopeKey::Empty),
            vec![StatsBucket {
                scope_key: ScopeKey::Empty,
                accs: new_bucket_accs(plan, soa_layout),
                touch: 0,
            }],
        );
    }

    /// 取/建一个桶（完整键路径: 行式回退 / 空键规则用）。哈希与列式
    /// `keyed_bucket_mut` 同值, 链内按 ScopeKey 完整比较消歧。
    /// 新桶先过限额检查（超限 → spill 腾空间 → 仍超限 → None, 调用方跳过
    /// 该行——内存有界）。
    ///
    /// **entry 借用取舍（2026-08-27 合并）**: 远程单查（`entry` 占用分支）在
    /// spill 场景不适用——`Entry` 借用 `buckets` 期间无法调 `&mut self` 限额检查
    /// （`account_new_bucket` 含 spill 驱逐）。**无 spill 快速路径**（绝大多数
    /// 规则, 如 q17 纯 SoA）走 [`Self::bucket_mut_no_spill`] 单查零退化; 仅
    /// spill 配置的规则命中退化为 get + get_mut 双查（q18 回访 3.4 次/键, 2 次
    /// 哈希 vs 1 次的开销相对驱逐/落盘成本可忽略）。
    pub(crate) fn bucket_mut(
        &mut self,
        key: &ScopeKey,
        plan: &StatsPlan,
    ) -> Option<&mut StatsBucketAccs> {
        let hash = scope_key_hash(key);
        // 真无 spill（未配置且未注册惰性 spec）才走单查快速路径——惰性创建前
        // `spill` 为 None 但 `spill_create` 已注册（配置了 spill）, 桶须维护
        // touch/clock（首次驱逐时 store 才建）。
        if self.spill.is_none() && self.spill_create.is_none() {
            return self.bucket_mut_no_spill(key, hash, plan);
        }
        // 未命中前置检查: 键可能已 spill → 读回（take 只读，M5-2）。
        if self.spill_index.contains(&hash) {
            let taken = self
                .spill
                .as_mut()
                .expect("spill_index 非空即有 store")
                .take(hash)
                .unwrap_or_else(|| {
                    panic!("spill 索引与存储不一致(致命): hash {hash:#x} 索引在但 take 空")
                });
            if taken.0 != *key {
                // hash 碰撞且非同一键：致命（绝不静默丢键）。
                panic!("spill hash 碰撞(致命): {hash:#x} 键不匹配");
            }
            return Some(self.readback_bucket_mut(hash, taken, plan));
        }
        // 命中（不可变查 + 可变改）: 刷新二次机会计数。
        if let Some(i) = self
            .buckets
            .get(&hash)
            .and_then(|chain| chain.iter().position(|b| &b.scope_key == key))
        {
            let chain = self.buckets.get_mut(&hash).expect("命中即存在");
            chain[i].touch = TOUCH_MAX;
            return Some(&mut chain[i].accs);
        }
        // 未命中（新键）: 限额检查 + spill 驱逐（&mut self 自由）→ 建桶。
        if !self.account_new_bucket(plan) {
            return None;
        }
        // 链 Vec 容量精确 1（2026-08-26 q18 状态 2.3× 归因）：空 Vec push
        // 1 个后 capacity=4 → 每链 4 桶容量纯浪费。`with_capacity(1)` 精确 1 桶。
        let chain = self
            .buckets
            .entry(hash)
            .or_insert_with(|| Vec::with_capacity(1));
        chain.push(StatsBucket {
            scope_key: key.clone(),
            accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
            touch: 0,
        });
        self.clock.push_back(hash); // 创建序入队（队尾 = 最新）
        Some(&mut chain.last_mut().expect("just pushed").accs)
    }

    /// 无 spill 快速路径（远程原版 entry 单查, 命中 1 次哈希——q17 类命中
    /// 主流零退化）。无 spill ⇒ `spill_index` 恒空（驱逐才插入）⇒ 不查读回;
    /// touch/clock 无用（恒 0/空）⇒ 不维护。碰撞用自由函数限额（无 spill 时
    /// 驱逐本就不存在, 与远程语义一致）。
    fn bucket_mut_no_spill(
        &mut self,
        key: &ScopeKey,
        hash: u64,
        plan: &StatsPlan,
    ) -> Option<&mut StatsBucketAccs> {
        use std::collections::hash_map::Entry;
        match self.buckets.entry(hash) {
            Entry::Occupied(o) => {
                let chain = o.into_mut();
                if let Some(i) = chain.iter().position(|b| &b.scope_key == key) {
                    return Some(&mut chain[i].accs);
                }
                // 碰撞（同 hash 不同键, 极罕见）: 记账 + push。
                let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
                if !account_bucket_allowed(
                    self.limit_bytes,
                    self.mem_used_shared.as_deref(),
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                chain.push(StatsBucket {
                    scope_key: key.clone(),
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                    touch: 0,
                });
                let last = chain.len() - 1;
                Some(&mut chain[last].accs)
            }
            Entry::Vacant(v) => {
                if !account_bucket_allowed(
                    self.limit_bytes,
                    self.mem_used_shared.as_deref(),
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    Self::bucket_allowance(plan, self.soa_layout.is_some()),
                ) {
                    return None;
                }
                let chain = v.insert(vec![StatsBucket {
                    scope_key: key.clone(),
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                    touch: 0,
                }]);
                Some(&mut chain[0].accs)
            }
        }
    }

    /// 读回已 spill 的键并放回内存（take 只读，M5-2）：
    /// 出 `spill_index` 入 `readback`（close 按此过滤 redb 旧条目）；入账
    /// allowance；超限先驱逐最老键（此刻未借用 chain）；建桶 touch=TOUCH_MAX。
    fn readback_bucket_mut(
        &mut self,
        hash: u64,
        taken: (ScopeKey, Vec<StatsAccum>),
        plan: &StatsPlan,
    ) -> &mut StatsBucketAccs {
        self.spill_index.remove(&hash);
        self.readback.insert(hash);
        self.spill_readbacks += 1;
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        if let Some(u) = &self.spill_used {
            u.fetch_sub(allowance, std::sync::atomic::Ordering::SeqCst);
        }
        self.estimated_bytes += allowance;
        self.mem_add(allowance);
        if let Some(limit) = self.limit_bytes
            && self.mem_used_bytes() > limit
        {
            self.evict_to_spill(plan);
        }
        let chain = self
            .buckets
            .entry(hash)
            .or_insert_with(|| Vec::with_capacity(1));
        chain.push(StatsBucket {
            scope_key: taken.0,
            accs: vec_to_bucket_accs(taken.1, self.soa_layout.as_ref()),
            touch: TOUCH_MAX,
        });
        self.clock.push_back(hash);
        &mut chain.last_mut().expect("just pushed").accs
    }

    /// 取/建一个桶（列式扁平键路径）: `hash` = 叶数组哈希, `comps` = 栈上叶
    /// 数组（列序）。链内按 `comps` 与完整键比较消歧; 未命中时构建完整键
    /// （每桶一次）。新桶先过限额检查（超限 → spill 腾空间 → 仍超限 → None）。
    ///
    /// **单次 entry 查找（2026-08-27 q17）**: 命中主流（在航 auction 窗口内
    /// 重复引用 ~100%）。无 spill 快速路径（[`Self::keyed_bucket_mut_no_spill`]）
    /// 单查零退化; spill 配置的规则命中双查（同 [`Self::bucket_mut`] 取舍）。
    /// pub(crate) 供 rules 段分解 bench（q17_rules_breakdown）。
    pub(crate) fn keyed_bucket_mut(
        &mut self,
        hash: u64,
        comps: &[ScopeKey],
        plan: &StatsPlan,
    ) -> Option<&mut StatsBucketAccs> {
        if self.spill.is_none() && self.spill_create.is_none() {
            return self.keyed_bucket_mut_no_spill(hash, comps, plan);
        }
        // 命中检查（不可变查 + 可变改, 见 bucket_mut 的 entry 借用取舍注释）。
        if let Some(i) = self.buckets.get(&hash).and_then(|chain| {
            chain
                .iter()
                .position(|b| comps_match(&b.scope_key, comps, 0, comps.len()))
        }) {
            let chain = self.buckets.get_mut(&hash).expect("命中即存在");
            chain[i].touch = TOUCH_MAX; // 刷新时钟二次机会计数
            return Some(&mut chain[i].accs);
        }
        // 未命中前置检查: 键可能已 spill → 读回（take 只读，M5-2）。
        if self.spill_index.contains(&hash) {
            let taken = self
                .spill
                .as_mut()
                .expect("spill_index 非空即有 store")
                .take(hash)
                .unwrap_or_else(|| {
                    panic!("spill 索引与存储不一致(致命): hash {hash:#x} 索引在但 take 空")
                });
            if !comps_match(&taken.0, comps, 0, comps.len()) {
                // hash 碰撞且非同一键：致命（绝不静默丢键）。
                panic!("spill hash 碰撞(致命): {hash:#x} 键不匹配");
            }
            return Some(self.readback_bucket_mut(hash, taken, plan));
        }
        if !self.account_new_bucket(plan) {
            return None;
        }
        let scope_key = scope_key_from_comps(comps);
        let chain = self
            .buckets
            .entry(hash)
            .or_insert_with(|| Vec::with_capacity(1));
        chain.push(StatsBucket {
            scope_key,
            accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
            touch: 0,
        });
        self.clock.push_back(hash); // 创建序入队（队尾 = 最新）
        Some(&mut chain.last_mut().expect("just pushed").accs)
    }

    /// 无 spill 快速路径（远程原版 entry 单查）——同 [`Self::bucket_mut_no_spill`]
    /// 取舍（不查读回/不维护 touch/clock, 碰撞用自由函数限额）。
    pub(crate) fn keyed_bucket_mut_no_spill(
        &mut self,
        hash: u64,
        comps: &[ScopeKey],
        plan: &StatsPlan,
    ) -> Option<&mut StatsBucketAccs> {
        use std::collections::hash_map::Entry;
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        match self.buckets.entry(hash) {
            Entry::Occupied(o) => {
                let chain = o.into_mut();
                if let Some(i) = chain
                    .iter()
                    .position(|b| comps_match(&b.scope_key, comps, 0, comps.len()))
                {
                    return Some(&mut chain[i].accs);
                }
                // 碰撞（同 hash 不同键 = 新桶, 极罕见）: 记账 + push。
                if !account_bucket_allowed(
                    self.limit_bytes,
                    self.mem_used_shared.as_deref(),
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                let scope_key = scope_key_from_comps(comps);
                chain.push(StatsBucket {
                    scope_key,
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                    touch: 0,
                });
                let last = chain.len() - 1;
                Some(&mut chain[last].accs)
            }
            Entry::Vacant(v) => {
                if !account_bucket_allowed(
                    self.limit_bytes,
                    self.mem_used_shared.as_deref(),
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                let scope_key = scope_key_from_comps(comps);
                let chain = v.insert(vec![StatsBucket {
                    scope_key,
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                    touch: 0,
                }]);
                Some(&mut chain[0].accs)
            }
        }
    }

    /// 按完整键取桶（测试/调试用; 生产走哈希路径）。
    pub fn find_bucket(&self, key: &ScopeKey) -> Option<&StatsBucketAccs> {
        self.buckets
            .get(&scope_key_hash(key))
            .and_then(|chain| chain.iter().find(|b| &b.scope_key == key))
            .map(|b| &b.accs)
    }

    /// 清空并拍平全部桶（close 用）: `(ScopeKey, accs)` 按 ScopeKey 升序。
    /// 同时清零内存账本（新窗口重新累积; 拒收计数保留——指标用）。
    /// spill 启用时先并入 spill 键（每个键恰好一次, 见 [`Self::merge_spill_into_buckets`]）。
    pub(crate) fn take_buckets(&mut self, plan: &StatsPlan) -> Vec<(ScopeKey, StatsBucketAccs)> {
        self.merge_spill_into_buckets(plan);
        let mut out: Vec<(ScopeKey, StatsBucketAccs)> = std::mem::take(&mut self.buckets)
            .into_values()
            .flat_map(|chain| chain.into_iter().map(|b| (b.scope_key, b.accs)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        // 本片账本清零 → 共享计数同步扣减（本片净占用 == estimated_bytes——
        // 每次增减都已同步; 扣完预算随窗口释放可复用）。
        self.mem_sub(self.estimated_bytes);
        self.estimated_bytes = 0;
        self.limit_warned = false;
        out
    }
}
