//! 近端 B：跨规则共享的在线基线历史表（B-b，2026-09-07；相位同窗版 2026-09-08）。
//!
//! 纯内存、零 IO（在 wf-cep 依赖墙内）：producer（wf-engine/wf-runtime 的
//! stats 收盘）与 judge（cep eval 的 `baseline_dev()`）经本模块的全局句柄
//! 访问同一份状态——镜像 `external::set_external_handler` 的注册模式。
//!
//! 数据契约（baseline-online-design.md §11.3）：每 `(entity, metric[, phase])`
//! 保留**最近 K 个已收盘窗口**的可加三元组 `(n, sum, sum_sq)` + 窗口边界；
//! `append` 以 `win_start` 幂等 upsert（重放替换）；`deviation` 由近期窗口
//! 合并推导 μ/σ（等权窗口：μ = Σsum/Σn，σ² = max(0, Σsum_sq/Σn − μ²)）——
//! 半衰期加权为同一合并函数的参数化演进，见 §5.3。
//!
//! **相位同窗（2026-09-08）**：安装时可带 `Phase{period, bucket}`（如
//! period=7d/bucket=5m）。开启后 `append` 按 `win_start` 折叠相位桶、每桶独立
//! 保留最近 K 窗；`baseline_dev` 按**事件时间**折叠到同相位桶，只与"历史同期"
//! 比较（早高峰只跟早高峰比）。相位随收盘自然推进，无需外部刷新。
//! 关闭相位（默认）= 原行为（该键最近 K 窗，不过滤）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

/// 默认每键保留窗口数。
pub const DEFAULT_K: usize = 8;

/// 相位折叠参数：`phase(ts) = (ts mod period) div bucket`（epoch 折叠，无时区）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase {
    pub period_nanos: u64,
    pub bucket_nanos: u64,
}

impl Phase {
    fn bucket_of(&self, ts: i64) -> u32 {
        if ts < 0 {
            return 0;
        }
        let t = ts as u64;
        ((t % self.period_nanos) / self.bucket_nanos) as u32
    }
}

/// 一个已收盘窗口的可加基线记录。
#[derive(Debug, Clone, Copy)]
pub struct BaselineWindow {
    pub win_start_nanos: i64,
    pub win_end_nanos: i64,
    pub n: f64,
    pub sum: f64,
    pub sum_sq: f64,
}

/// 桶级窗口容器（一个相位桶的窗口列表）。
type BucketMap = HashMap<u32, Entry>;
/// metric → 桶表。
type MetricMap = HashMap<String, BucketMap>;
/// entity → metric 表。内层为 (entity, metric[, phase]) 三层嵌套——judge 每事件
/// 按 (entity, metric) 查只触碰目标键（2026-09-08 bench 驱动：旧全表扫描在
/// 共享大实体空间下每事件 O(store 键数)，改嵌套后 O(该键桶数)，查询零分配）。
type EntityMap = HashMap<String, MetricMap>;

#[derive(Debug)]
struct Entry {
    /// 按 win_start_nanos 升序；长度 ≤ K。
    windows: VecDeque<BaselineWindow>,
}

impl Default for Entry {
    fn default() -> Self {
        Entry {
            windows: VecDeque::new(),
        }
    }
}

/// 规则级共享基线历史表（内部 Mutex，可经 `&'static` 跨规则/线程访问）。
/// `decay=true` 时合并为**半衰期加权**（§5.3）：以合并集最新窗 `win_start` 为参照，
/// 旧窗权重 `0.5^(age/半衰期)`，半衰期参照序列重现间距——相位关闭 = 窗序列相邻，
/// 4×窗跨度（`HALF_LIFE_WINDOWS=4`）；相位开启 = 同相位窗按 `period` 重现，
/// 4×period（保留 ~4 期同相位画像、更早渐隐）。`decay=false` 为等权（对拍 oracle 用）。
/// `phase=Some` 时按键的相位桶分桶（见模块 doc）。
///
/// 内部为 `entity → metric → 相位桶 → Entry` 三层嵌套（见 [`EntityMap`]），
/// 同一把 Mutex 保护。
pub struct BaselineStore {
    k: usize,
    decay: bool,
    phase: Option<Phase>,
    inner: Mutex<EntityMap>,
}

/// 半衰期（相对序列重现间距的倍数）：权重降半的龄期。
/// 相位关闭 = 窗序列相邻（窗跨度）；相位开启 = 同相位窗按 `period` 重现（见 [`Phase`]）。
pub const HALF_LIFE_WINDOWS: f64 = 4.0;

/// 相位未开启时的占位桶。
const NO_PHASE: u32 = 0;

impl BaselineStore {
    /// 每键最多保留最近 `k` 个窗口；等权合并（确定对拍用）。
    pub fn new(k: usize) -> Self {
        BaselineStore {
            k: k.max(1),
            decay: false,
            phase: None,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 半衰期加权版（生产默认，见 §5.3）。
    pub fn decaying(k: usize) -> Self {
        BaselineStore {
            k: k.max(1),
            decay: true,
            phase: None,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 相位同窗版（`phase` 为周期折叠参数；非法参数退化为无相位）。
    pub fn phased(k: usize, decay: bool, phase: Phase) -> Self {
        let phase = if phase.bucket_nanos > 0 && phase.period_nanos >= phase.bucket_nanos {
            Some(phase)
        } else {
            None
        };
        BaselineStore {
            k: k.max(1),
            decay,
            phase,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 当前每键窗口容量（诊断/单测）。
    pub fn k(&self) -> usize {
        self.k
    }

    /// 是否半衰期加权合并（诊断/单测）。
    pub fn decayed(&self) -> bool {
        self.decay
    }

    /// 相位参数（诊断/单测）；`None` = 未开启。
    pub fn phase(&self) -> Option<Phase> {
        self.phase
    }

    /// 当前复合键数（含空窗键，demo/诊断用）= 桶级 Entry 总数。
    pub fn key_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .values()
            .map(|metrics| metrics.values().map(|b| b.len()).sum::<usize>())
            .sum()
    }

    /// 某 (entity, metric) 当前保留的窗口总数（跨相位求和；相位关闭时即原语义，
    /// 用于 warmup 判定/诊断）。
    pub fn window_count(&self, entity: &str, metric: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .get(entity)
            .and_then(|metrics| metrics.get(metric))
            .map_or(0, |buckets| buckets.values().map(|e| e.windows.len()).sum())
    }

    /// 某 (entity, metric, 相位桶) 的窗口数（诊断/单测）。
    pub fn bucket_window_count(&self, entity: &str, metric: &str, bucket: u32) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .get(entity)
            .and_then(|metrics| metrics.get(metric))
            .and_then(|buckets| buckets.get(&bucket))
            .map_or(0, |e| e.windows.len())
    }

    fn bucket_for(&self, ts: i64) -> u32 {
        self.phase.map_or(NO_PHASE, |p| p.bucket_of(ts))
    }

    /// 收盘追加（幂等：同 (键, win_start) 重放 → 替换，不重复计数）。
    /// 相位开启时按 `win_start` 折桶分存。
    /// 非有限值（NaN/Inf）整窗忽略——污染会让该键基线全 NaN，判定静默失效。
    pub fn append(&self, entity: &str, metric: &str, window: BaselineWindow) {
        if !window.n.is_finite() || !window.sum.is_finite() || !window.sum_sq.is_finite() {
            return;
        }
        let bucket = self.bucket_for(window.win_start_nanos);
        let mut inner = self.inner.lock().unwrap();
        let entry = inner
            .entry(entity.to_string())
            .or_default()
            .entry(metric.to_string())
            .or_default()
            .entry(bucket)
            .or_default();

        if let Some(pos) = entry
            .windows
            .iter()
            .position(|w| w.win_start_nanos == window.win_start_nanos)
        {
            // 同窗重放（batch 重跑/分片合并重复触发）→ 幂等替换。
            entry.windows[pos] = window;
            return;
        }
        // 按 win_start 保序插入（producer 单调收盘时等价 push_back；乱序也正确）。
        let pos = entry
            .windows
            .partition_point(|w| w.win_start_nanos < window.win_start_nanos);
        entry.windows.insert(pos, window);
        // 裁剪：每相位桶只留最近 K 窗（丢最旧）。
        while entry.windows.len() > self.k {
            entry.windows.pop_front();
        }
    }

    /// 启动 warm：批量载入已收盘记录（读 t_baseline 导出文件等）。
    pub fn warm<I>(&self, records: I)
    where
        I: IntoIterator<Item = (String, String, BaselineWindow)>,
    {
        for (entity, metric, w) in records {
            self.append(&entity, &metric, w);
        }
    }

    /// 收集某 (entity, metric) 的窗口（相位关闭 = 全部；相位开启 = 指定桶；None = 全部
    /// 桶的并集），按 win_start 升序。嵌套索引：只触碰目标 (entity, metric) 的桶，
    /// 不扫全 store（bench 2026-09-08：旧全表扫描在共享大实体空间下每事件 O(store
    /// 键数)，见 baseline_bench.rs）。
    fn windows_for(&self, entity: &str, metric: &str, at: Option<i64>) -> Vec<BaselineWindow> {
        let inner = self.inner.lock().unwrap();
        let Some(metrics) = inner.get(entity) else {
            return Vec::new();
        };
        let Some(buckets) = metrics.get(metric) else {
            return Vec::new();
        };
        let target_phase = match (self.phase, at) {
            (Some(_), Some(ts)) => Some(self.bucket_for(ts)),
            _ => None, // 相位关闭，或开启但未给时间 → 全桶并集（时间未知的旧调用）
        };
        let mut out = Vec::new();
        for (&b, entry) in buckets {
            if let Some(p) = target_phase {
                if b != p {
                    continue;
                }
            }
            out.extend(entry.windows.iter().copied());
        }
        out.sort_by_key(|w| w.win_start_nanos);
        out
    }

    /// 由窗口列表合并出 (n, μ, σ)。`decay` 时以合并集最新窗为参照半衰期加权：
    /// 相位关闭按各窗跨度（半衰期 = 4×span，原语义）；相位开启按周期（半衰期 =
    /// 4×period，同相位窗每期重现一次——见 [`HALF_LIFE_WINDOWS`]）。
    fn merge(&self, windows: &[BaselineWindow]) -> Option<(f64, f64, f64)> {
        if windows.is_empty() {
            return None;
        }
        let t_ref = windows.last().map_or(0i64, |w| w.win_start_nanos);
        let phase_half_life = self
            .phase
            .map(|p| HALF_LIFE_WINDOWS * p.period_nanos as f64);
        let (mut n, mut sum, mut sq) = (0.0f64, 0.0f64, 0.0f64);
        for w in windows {
            let weight = if self.decay {
                let half_life = match phase_half_life {
                    Some(hl) => hl,
                    None => {
                        let span = (w.win_end_nanos - w.win_start_nanos).max(1) as f64;
                        HALF_LIFE_WINDOWS * span
                    }
                };
                let age = (t_ref - w.win_start_nanos).max(0) as f64;
                0.5f64.powf(age / half_life)
            } else {
                1.0
            };
            n += w.n * weight;
            sum += w.sum * weight;
            sq += w.sum_sq * weight;
        }
        if n <= 0.0 {
            return None;
        }
        let mu = sum / n;
        let sigma = (sq / n - mu * mu).max(0.0).sqrt();
        Some((n, mu, sigma))
    }

    /// 由近期窗口合并出 (n, μ, σ)；键不存在或无窗口 → None。
    /// `at` = 事件时间（纳秒）：相位开启时只取同相位桶的最近 K 窗；`None` = 全部
    /// 桶并集（相位关闭时的原语义；诊断/旧调用路径）。
    pub fn summary_at(
        &self,
        entity: &str,
        metric: &str,
        at: Option<i64>,
    ) -> Option<(f64, f64, f64)> {
        let windows = self.windows_for(entity, metric, at);
        self.merge(&windows)
    }

    /// 相位关闭时的便捷形态（等价 `summary_at(.., None)`）。
    pub fn summary(&self, entity: &str, metric: &str) -> Option<(f64, f64, f64)> {
        self.summary_at(entity, metric, None)
    }

    /// z-score：`value` 相对该键（`at` 折叠相位）基线（μ/σ）的偏离。
    /// 无基线 → None；非有限值 → None；σ≈0 → 0.0（无离散时不判离群，防除零）。
    pub fn deviation_at(
        &self,
        entity: &str,
        metric: &str,
        value: f64,
        at: Option<i64>,
    ) -> Option<f64> {
        if !value.is_finite() {
            return None;
        }
        let (_, mu, sigma) = self.summary_at(entity, metric, at)?;
        if sigma <= 1e-9 {
            return Some(0.0);
        }
        Some((value - mu) / sigma)
    }

    /// 相位关闭时的便捷形态（等价 `deviation_at(.., None)`）。
    pub fn deviation(&self, entity: &str, metric: &str, value: f64) -> Option<f64> {
        self.deviation_at(entity, metric, value, None)
    }
}

// ---------------------------------------------------------------------------
// 全局句柄（镜像 external：cep eval 与 wf-runtime/wf-engine 共享同一实例）
// ---------------------------------------------------------------------------

static STORE: OnceLock<BaselineStore> = OnceLock::new();

/// 安装规则级共享 store（等权/半衰期，无相位；runtime 启动早期调用，指定每键
/// 窗口容量 K 与是否半衰期加权合并）。若已被使用（测试先初始化）返回 false，
/// 保持既有实例。
pub fn install(k: usize, decay: bool) -> bool {
    let store = if decay {
        BaselineStore::decaying(k)
    } else {
        BaselineStore::new(k)
    };
    STORE.set(store).is_ok()
}

/// 安装带相位的共享 store（见 [`Phase`]；`install` 的无相位特例）。
pub fn install_phased(k: usize, decay: bool, phase: Phase) -> bool {
    STORE.set(BaselineStore::phased(k, decay, phase)).is_ok()
}

/// 获取全局 store（未安装 → 默认等权 K）。
pub fn store() -> &'static BaselineStore {
    STORE.get_or_init(|| BaselineStore::new(DEFAULT_K))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn win(start: i64, n: f64, sum: f64, sum_sq: f64) -> BaselineWindow {
        BaselineWindow {
            win_start_nanos: start,
            win_end_nanos: start + 60_000_000_000,
            n,
            sum,
            sum_sq,
        }
    }

    // ---- 既有行为回归（无相位，原语义不变） --------------------------------

    #[test]
    fn append_trims_to_k_keeping_newest_in_order() {
        let s = BaselineStore::new(3);
        for i in 0..5 {
            s.append("e", "m", win(i * 60, 1.0, 10.0, 100.0));
        }
        assert_eq!(s.window_count("e", "m"), 3, "超 K 裁剪");
        let (n, mu, _) = s.summary("e", "m").expect("有基线");
        assert_eq!(n, 3.0);
        assert_eq!(mu, 10.0);
        let inner = s.inner.lock().unwrap();
        let starts: Vec<i64> = inner
            .get("e")
            .and_then(|m| m.get("m"))
            .map(|b| {
                b.values()
                    .flat_map(|en| en.windows.iter().map(|w| w.win_start_nanos))
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(starts, vec![120, 180, 240]);
    }

    #[test]
    fn append_same_win_start_is_idempotent() {
        let s = BaselineStore::new(4);
        s.append("e", "m", win(0, 2.0, 20.0, 210.0));
        s.append("e", "m", win(0, 3.0, 30.0, 300.0)); // 重放同窗 → 替换
        assert_eq!(s.window_count("e", "m"), 1);
        let (n, mu, _) = s.summary("e", "m").unwrap();
        assert_eq!((n, mu), (3.0, 10.0));
    }

    #[test]
    fn deviation_matches_hand_computation() {
        // [3,4,5]: n=3 sum=12 sum_sq=50 → μ=4, σ=√(50/3−16)=√(2/3)≈0.8165
        let s = BaselineStore::new(4);
        s.append("e", "m", win(0, 3.0, 12.0, 50.0));
        let dev = s.deviation("e", "m", 10.0).expect("有基线");
        let expect = (10.0 - 4.0) / (2.0f64 / 3.0f64).sqrt();
        assert!((dev - expect).abs() < 1e-9, "z 失配 {dev} vs {expect}");
        assert!(s.deviation("nope", "m", 1.0).is_none());
    }

    #[test]
    fn zero_sigma_returns_zero_not_div_zero() {
        let s = BaselineStore::new(4);
        s.append("e", "m", win(0, 2.0, 10.0, 50.0)); // [5,5]
        assert_eq!(s.deviation("e", "m", 5.0), Some(0.0));
        s.append("e", "m", win(60, 2.0, 10.0, 50.0));
        assert_eq!(s.deviation("e", "m", 5.0), Some(0.0));
    }

    #[test]
    fn keys_are_isolated() {
        let s = BaselineStore::new(4);
        s.append("svc_a", "qps", win(0, 2.0, 20.0, 210.0));
        assert!(s.deviation("svc_a", "latency", 5.0).is_none());
        assert!(s.deviation("svc_b", "qps", 5.0).is_none());
    }

    #[test]
    fn decay_weights_recent_windows_over_old() {
        let s = BaselineStore::decaying(8);
        s.append(
            "e",
            "m",
            win(0, 100.0, 100_000.0, 100_000_000.0), // μ=1000
        );
        s.append(
            "e",
            "m",
            win(960_000_000_000, 100.0, 200_000.0, 400_000_000.0), // μ=2000（新窗）
        );
        let (_, mu, _) = s.summary("e", "m").expect("有基线");
        let w = 0.5f64.powf(960.0 / (4.0 * 60.0));
        let expect = (w * 100_000.0 + 200_000.0) / (w * 100.0 + 100.0);
        assert!(
            (mu - expect).abs() < 1e-6,
            "半衰期加权 μ 失配 {mu} vs {expect}"
        );
        assert!(mu > 1500.0, "decay 应偏向近期窗（μ={mu}）");
        let eq = BaselineStore::new(8);
        eq.append("e", "m", win(0, 100.0, 100_000.0, 100_000_000.0));
        eq.append(
            "e",
            "m",
            win(960_000_000_000, 100.0, 200_000.0, 400_000_000.0),
        );
        assert!((eq.summary("e", "m").unwrap().1 - 1500.0).abs() < 1e-9);
    }

    #[test]
    fn append_out_of_order_keeps_windows_sorted() {
        let s = BaselineStore::new(5);
        s.append("e", "m", win(120, 1.0, 10.0, 100.0));
        s.append("e", "m", win(0, 1.0, 10.0, 100.0));
        s.append("e", "m", win(60, 1.0, 10.0, 100.0));
        let inner = s.inner.lock().unwrap();
        let starts: Vec<i64> = inner
            .get("e")
            .and_then(|m| m.get("m"))
            .map(|b| {
                b.values()
                    .flat_map(|en| en.windows.iter().map(|w| w.win_start_nanos))
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(starts, vec![0, 60, 120], "乱序 append 仍按 win_start 升序");
    }

    #[test]
    fn append_ignores_non_finite_windows() {
        let s = BaselineStore::new(4);
        s.append(
            "e",
            "m",
            BaselineWindow {
                win_start_nanos: 0,
                win_end_nanos: 60_000_000_000,
                n: f64::NAN,
                sum: 1.0,
                sum_sq: 1.0,
            },
        );
        s.append(
            "e",
            "m",
            BaselineWindow {
                win_start_nanos: 0,
                win_end_nanos: 60_000_000_000,
                n: 1.0,
                sum: f64::INFINITY,
                sum_sq: 1.0,
            },
        );
        assert_eq!(s.window_count("e", "m"), 0, "非有限窗应被忽略");
        assert!(s.deviation("e", "m", 1.0).is_none());
    }

    #[test]
    fn deviation_rejects_non_finite_value() {
        let s = BaselineStore::new(4);
        s.append("e", "m", win(0, 2.0, 20.0, 210.0));
        assert!(s.deviation("e", "m", f64::NAN).is_none());
        assert!(s.deviation("e", "m", f64::INFINITY).is_none());
    }

    #[test]
    fn concurrent_append_and_read_are_consistent() {
        let s = std::sync::Arc::new(BaselineStore::new(64));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let s = std::sync::Arc::clone(&s);
                std::thread::spawn(move || {
                    for i in 0..200 {
                        let entity = format!("e{t}");
                        let start = if i % 2 == 0 { 200 - i as i64 } else { i as i64 };
                        s.append(&entity, "m", win(start, 2.0, 20.0, 210.0));
                        let _ = s.summary(&entity, "m");
                        let _ = s.deviation(&entity, "m", 10.0);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread");
        }
        for t in 0..8 {
            assert!(s.window_count(&format!("e{t}"), "m") <= 64);
        }
        assert!(s.key_count() >= 1);
    }

    // ---- 相位同窗（2026-09-08） --------------------------------------------

    /// period=60s、bucket=15s：相位桶在每 60s 周期内循环。
    fn phase_store(k: usize, decay: bool) -> BaselineStore {
        BaselineStore::phased(
            k,
            decay,
            Phase {
                period_nanos: 60_000_000_000,
                bucket_nanos: 15_000_000_000,
            },
        )
    }

    fn bucket_of(ts: i64) -> u32 {
        ((ts as u64) % 60_000_000_000 / 15_000_000_000) as u32
    }

    #[test]
    fn phase_appends_are_partitioned_per_bucket() {
        let s = phase_store(8, false);
        // 同一 (entity,metric) 四个不同桶各一窗
        s.append("e", "m", win(0, 1.0, 10.0, 100.0)); // 桶0
        s.append("e", "m", win(15_000_000_000, 2.0, 20.0, 200.0)); // 桶1
        s.append("e", "m", win(30_000_000_000, 3.0, 30.0, 300.0)); // 桶2
        s.append("e", "m", win(45_000_000_000, 4.0, 40.0, 400.0)); // 桶3
        assert_eq!(s.window_count("e", "m"), 4, "跨桶求和为 4 窗");
        assert_eq!(s.key_count(), 4, "四桶各自成键");

        // 每桶各自 K=1 时只留各自最新
        let s1 = phase_store(1, false);
        for i in 0..3 {
            // 桶0 出现 3 次（0s / 60s / 120s…即 0、60、120 秒起点）
            s1.append("e", "m", win(i as i64 * 60_000_000_000, 1.0, 10.0, 100.0));
        }
        assert_eq!(s1.window_count("e", "m"), 1, "同桶裁剪到 K=1");
    }

    #[test]
    fn deviation_at_uses_same_phase_only() {
        let s = phase_store(8, false);
        // 桶1 有两次历史：μ=10（恒定，σ=0）
        s.append("e", "m", win(15_000_000_000, 1.0, 10.0, 100.0));
        s.append("e", "m", win(75_000_000_000, 1.0, 10.0, 100.0)); // 60s 后同桶1
        // 桶0 有一次异常高历史（μ=1000）
        s.append("e", "m", win(0, 1.0, 1000.0, 1_000_000.0));

        // 桶1 时刻：σ≈0 → deviation 0（不判离群），而非被桶0 的高基线污染
        assert_eq!(
            s.deviation_at("e", "m", 1000.0, Some(15_000_000_000)),
            Some(0.0),
            "同相位（桶1）基线与异相位（桶0）隔离"
        );
        // 桶0 时刻：μ=1000 σ≈0 → value=1100 → 0
        assert_eq!(s.deviation_at("e", "m", 1100.0, Some(0)), Some(0.0));
    }

    #[test]
    fn phase_decay_uses_period_referenced_half_life() {
        // 相位开启：半衰期 = 4×period(=60s) = 240s。旧同桶窗(15s)对最新同桶窗(75s)
        // 龄期 60s → 权重 0.5^(60/240)≈0.8409 → μ≈154.3。
        // （旧实现按 4×窗宽=60s 半衰期 → 权重 0.5 → μ≈166.7；本测试精确锚定新语义。）
        let s = phase_store(8, true);
        s.append("e", "m", win(15_000_000_000, 1.0, 100.0, 10_000.0)); // 旧同桶 μ=100
        s.append("e", "m", win(75_000_000_000, 1.0, 200.0, 40_000.0)); // 新同桶 μ=200
        s.append("e", "m", win(0, 1.0, 50_000.0, 2_500_000_000.0)); // 异桶 μ=50000（不进）
        let (_, mu, _) = s.summary_at("e", "m", Some(15_000_000_000)).unwrap();
        let expect = (0.8409 * 100.0 + 200.0) / 1.8409;
        assert!(
            (mu - expect).abs() < 0.5,
            "同桶 decay μ 应≈{expect}（period 参照半衰期），实际 {mu}"
        );
    }

    #[test]
    fn phase_install_validation_falls_back_when_invalid() {
        let bad = BaselineStore::phased(
            4,
            false,
            Phase {
                period_nanos: 0,
                bucket_nanos: 10,
            },
        );
        assert!(bad.phase().is_none(), "非法参数应退化为无相位");
    }

    // ---- 相位同窗补测（2026-09-08，六轮 review 后补充） ---------------------

    #[test]
    fn phase_summary_at_none_unions_all_buckets() {
        // at=None（事件缺 event_time 的保守回退）= 全桶并集；与 at=Some 的同相位
        // 过滤必须给出不同（可区分）的基线——并集 σ 含跨相位离散，绝不可与单桶混淆。
        let s = phase_store(8, false);
        s.append("e", "m", win(0, 1.0, 10.0, 100.0)); // 桶0 μ=10
        s.append("e", "m", win(15_000_000_000, 1.0, 20.0, 400.0)); // 桶1 μ=20
        s.append("e", "m", win(30_000_000_000, 1.0, 30.0, 900.0)); // 桶2 μ=30

        // 同相位：只含桶1 → n=1 μ=20 σ=0
        let (n1, mu1, sig1) = s.summary_at("e", "m", Some(15_000_000_000)).unwrap();
        assert_eq!((n1, mu1, sig1), (1.0, 20.0, 0.0), "同相位基线应只含桶1");

        // 全桶并集：n=3 sum=60 sq=900 → μ=20，σ=√(1400/3−400)=√(200/3)≈8.165
        let (n3, mu3, sig3) = s.summary_at("e", "m", None).unwrap();
        assert_eq!(n3, 3.0);
        assert_eq!(mu3, 20.0);
        let expect_sigma = (200.0f64 / 3.0).sqrt();
        assert!(
            (sig3 - expect_sigma).abs() < 1e-9,
            "并集 σ 失配 {sig3} vs {expect_sigma}"
        );
        let d = s.deviation_at("e", "m", 40.0, None).unwrap();
        assert!((d - 20.0 / expect_sigma).abs() < 1e-9, "并集 z 失配 {d}");
    }

    #[test]
    fn phase_deviation_at_empty_bucket_returns_none() {
        // 目标相位桶无历史而其它桶有 → None（严格隔离：异相位不得泄漏成基线，
        // 否则会拿"另一时段"的画像给本时段误判）。
        let s = phase_store(8, false);
        s.append("e", "m", win(0, 1.0, 1000.0, 1_000_000.0)); // 桶0 高基线
        s.append("e", "m", win(15_000_000_000, 1.0, 10.0, 100.0)); // 桶1
        assert!(
            s.summary_at("e", "m", Some(45_000_000_000)).is_none(),
            "桶3 无历史 → summary None"
        );
        assert!(
            s.deviation_at("e", "m", 5000.0, Some(45_000_000_000))
                .is_none(),
            "桶3 无历史 → deviation None（不误报）"
        );
        // 对照：有历史的桶正常判定
        assert!(
            s.deviation_at("e", "m", 10.0, Some(15_000_000_000))
                .is_some()
        );
    }

    #[test]
    fn phase_replay_replaces_within_bucket_across_periods() {
        // 同 (键含桶, win_start) 重放 → 替换不重复计数；跨周期同桶各自保留。
        let s = phase_store(8, false);
        s.append("e", "m", win(0, 1.0, 10.0, 100.0)); // 周期0 桶0
        s.append("e", "m", win(60_000_000_000, 1.0, 10.0, 100.0)); // 周期1 桶0
        s.append("e", "m", win(0, 2.0, 20.0, 200.0)); // 重放周期0 同窗 → 替换
        assert_eq!(s.window_count("e", "m"), 2, "重放不得重复计数");
        let (n, mu, _) = s.summary_at("e", "m", Some(0)).unwrap();
        assert_eq!((n, mu), (3.0, 10.0), "桶0 = 替换窗(n=2) + 周期1窗(n=1)");
    }

    #[test]
    fn phase_trim_is_per_bucket_not_global() {
        // K=2：某桶 3 窗只留其最新 2，其它桶不受牵连（裁剪作用域=桶）。
        let s = phase_store(2, false);
        s.append("e", "m", win(0, 1.0, 10.0, 100.0)); // 桶0
        s.append("e", "m", win(60_000_000_000, 1.0, 20.0, 400.0)); // 桶0 周期1
        s.append("e", "m", win(120_000_000_000, 1.0, 30.0, 900.0)); // 桶0 周期2 → 裁掉 ts=0
        s.append("e", "m", win(15_000_000_000, 1.0, 50.0, 2500.0)); // 桶1（不受牵连）
        assert_eq!(s.window_count("e", "m"), 3, "桶0 裁到 2 + 桶1 保留 1");
        let (n, mu, _) = s.summary_at("e", "m", Some(0)).unwrap();
        assert_eq!((n, mu), (2.0, 25.0), "桶0 仅留 ts=60s/120s（μ 20/30 → 25）");
        let (n1, mu1, _) = s.summary_at("e", "m", Some(15_000_000_000)).unwrap();
        assert_eq!((n1, mu1), (1.0, 50.0), "桶1 不受桶0 裁剪影响");
    }

    #[test]
    fn phase_bucket_folding_wraps_at_period() {
        let p = Phase {
            period_nanos: 60_000_000_000,
            bucket_nanos: 15_000_000_000,
        };
        assert_eq!(p.bucket_of(0), 0);
        assert_eq!(p.bucket_of(15_000_000_000), 1);
        assert_eq!(p.bucket_of(59_000_000_000), 3);
        assert_eq!(p.bucket_of(60_000_000_000), 0, "周期边界折回桶0");
        assert_eq!(p.bucket_of(60_000_000_000 + 15_000_000_000), 1);
        assert_eq!(p.bucket_of(2 * 60_000_000_000 + 30_000_000_000), 2);
        assert_eq!(p.bucket_of(-5), 0, "负时间（防御）→ 桶0");
        // i64 大值（真实 epoch 纳秒量级）与镜像一致、不溢出
        let ts = 1_767_225_600_000_000_000i64 + 7 * 86_400_000_000_000i64;
        assert_eq!(
            p.bucket_of(ts),
            ((ts as u64) % 60_000_000_000 / 15_000_000_000) as u32
        );
    }

    #[test]
    fn phase_bucket_ns_fold_equals_epoch_second_fold() {
        // A/B 桶等价对拍（§11.6 挂账闭环）：B=近端 B Phase::bucket_of（epoch 纳秒
        // mod period div bucket）与 A=事件打标/供给标签（epoch 秒 % period div
        // bucket，phase_cfg/phase_now 同口径）在 period/bucket 为整秒时**严格等价**
        // ——含亚秒时刻：bucket 边界在整秒，floor 秒折桶与纳秒折桶一致
        // （floor(floor(t/1e9)/b) == floor(t/(b*1e9))）。
        let sec_fold = |ts_ns: i64, period_s: u64, bucket_s: u64| {
            ((ts_ns as u64 / 1_000_000_000) % period_s) / bucket_s
        };
        let base = 1_767_225_600_000_000_000i64; // 2026-01-01T00:00:00Z（任意基座）
        let ns = |s: i64, rem: i64| base + s * 1_000_000_000 + rem;

        // 全量秒级扫描（跨 ≥2 周期 + 桶边界）：每整秒带三种亚秒余量（0/1ns/末ns）
        for (period_s, bucket_s) in [(240u64, 15u64), (60, 15)] {
            let p = Phase {
                period_nanos: period_s * 1_000_000_000,
                bucket_nanos: bucket_s * 1_000_000_000,
            };
            let cycles = 2usize;
            let period_s_i = period_s as i64;
            for s in 0..(period_s_i * cycles as i64) {
                for rem in [0i64, 1, 999_999_999] {
                    let ts = ns(s, rem);
                    assert_eq!(
                        p.bucket_of(ts),
                        sec_fold(ts, period_s, bucket_s) as u32,
                        "period={period_s} bucket={bucket_s} ts=base+{s}s+{rem}ns"
                    );
                }
            }
            // 周期回绕与跨周期复现点
            for s in [period_s_i, 2 * period_s_i, 2 * period_s_i + bucket_s as i64] {
                assert_eq!(
                    p.bucket_of(ns(s, 0)),
                    sec_fold(ns(s, 0), period_s, bucket_s) as u32
                );
            }
        }

        // 生产量级（24h/1h）：选点含桶边界/周期末/回绕
        let (period_s, bucket_s) = (86_400u64, 3_600u64);
        let p = Phase {
            period_nanos: period_s * 1_000_000_000,
            bucket_nanos: bucket_s * 1_000_000_000,
        };
        for s in [0i64, 3599, 3600, 86_399, 86_400, 86_400 + 1800] {
            for rem in [0i64, 500_000_000] {
                let ts = ns(s, rem);
                assert_eq!(
                    p.bucket_of(ts),
                    sec_fold(ts, period_s, bucket_s) as u32,
                    "24h 档 ts=base+{s}s+{rem}ns"
                );
            }
        }
    }

    #[test]
    fn phase_period_equals_bucket_is_single_slot_and_stable() {
        // period==bucket：合法（单格周期），折叠退化为全时域同一桶——行为稳定、
        // 不产生分裂键（相位最细粒度=周期本身，等价关闭的可用形态）。
        let s = BaselineStore::phased(
            8,
            false,
            Phase {
                period_nanos: 60_000_000_000,
                bucket_nanos: 60_000_000_000,
            },
        );
        assert!(s.phase().is_some(), "period==bucket 应保留相位（单桶退化）");
        s.append("e", "m", win(0, 1.0, 10.0, 100.0));
        s.append("e", "m", win(120_000_000_000, 1.0, 20.0, 400.0));
        let (n, mu, _) = s.summary_at("e", "m", Some(7_000_000_000)).unwrap();
        assert_eq!((n, mu), (2.0, 15.0), "任意时刻看到同一份基线");
        assert_eq!(s.key_count(), 1, "全时域同桶，无键分裂");
    }

    #[test]
    fn concurrent_phase_append_and_read_are_consistent() {
        // 相位 store 多线程：多实体 × 多桶跨周期 append（含重放）+ summary_at/
        // deviation_at 并发读——无 panic/死锁；每桶窗口数 ≤ K（裁剪按桶隔离）。
        let s = std::sync::Arc::new(phase_store(8, true));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let s = std::sync::Arc::clone(&s);
                std::thread::spawn(move || {
                    for i in 0..200 {
                        let entity = format!("p{t}");
                        let slot = (i % 4) as i64; // 相位格 0..3
                        let cycle = i / 4; // 周期序号
                        let ts = (cycle * 60 + slot * 15) * 1_000_000_000i64;
                        s.append(&entity, "m", win(ts, 2.0, 20.0, 210.0));
                        if i % 3 == 0 {
                            s.append(&entity, "m", win(ts, 2.0, 20.0, 210.0)); // 重放幂等
                        }
                        // 同格中部时刻折同桶读
                        let at = (cycle * 60 + slot * 15 + 7) * 1_000_000_000i64;
                        let _ = s.summary_at(&entity, "m", Some(at));
                        let _ = s.deviation_at(&entity, "m", 10.0, Some(at));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread");
        }
        for t in 0..8 {
            let entity = format!("p{t}");
            let total = s.window_count(&entity, "m");
            assert!(total <= 8 * 4, "跨桶求和 ≤ K×桶数，实际 {total}");
            assert!(total > 0, "每实体应有数据");
            for b in 0..4u32 {
                let cnt = s
                    .inner
                    .lock()
                    .unwrap()
                    .get(&entity)
                    .and_then(|m| m.get("m"))
                    .and_then(|bm| bm.get(&b))
                    .map_or(0, |en| en.windows.len());
                assert!(cnt <= 8, "桶 {b} 窗口数超 K：{cnt}");
            }
        }
    }
}
