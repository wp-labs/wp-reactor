//! 近端 B：跨规则共享的在线基线历史表（B-b，2026-09-07）。
//!
//! 纯内存、零 IO（在 wf-cep 依赖墙内）：producer（wf-engine/wf-runtime 的
//! stats 收盘）与 judge（cep eval 的 `baseline_dev()`）经本模块的全局句柄
//! 访问同一份状态——镜像 `external::set_external_handler` 的注册模式。
//!
//! 数据契约（baseline-online-design.md §11.3）：每 `(entity, metric)` 保留
//! **最近 K 个已收盘窗口**的可加三元组 `(n, sum, sum_sq)` + 窗口边界；
//! `append` 以 `win_start` 幂等 upsert（重放替换）；`deviation` 由近期窗口
//! 合并推导 μ/σ（等权窗口：μ = Σsum/Σn，σ² = max(0, Σsum_sq/Σn − μ²)）——
//! 半衰期加权为同一合并函数的参数化演进，见 §5.3。

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

/// 默认每键保留窗口数。
pub const DEFAULT_K: usize = 8;

/// 一个已收盘窗口的可加基线记录。
#[derive(Debug, Clone, Copy)]
pub struct BaselineWindow {
    pub win_start_nanos: i64,
    pub win_end_nanos: i64,
    pub n: f64,
    pub sum: f64,
    pub sum_sq: f64,
}

/// (entity, metric) 复合键。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key(String, String);

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
/// `decay=true` 时合并为**半衰期加权**（§5.3）：以最新窗 `win_start` 为参照，
/// 旧窗权重 `0.5^(age/半衰期)`，半衰期 = 4×该窗跨度（`HALF_LIFE_WINDOWS=4`）——
/// 追随近期常态但不丢长程背景。`decay=false` 为等权（对拍 oracle 用）。
pub struct BaselineStore {
    k: usize,
    decay: bool,
    inner: Mutex<HashMap<Key, Entry>>,
}

/// 半衰期（相对窗口跨度倍数）：权重降半的龄期。
pub const HALF_LIFE_WINDOWS: f64 = 4.0;

impl BaselineStore {
    /// 每键最多保留最近 `k` 个窗口；等权合并（确定对拍用）。
    pub fn new(k: usize) -> Self {
        BaselineStore {
            k: k.max(1),
            decay: false,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 半衰期加权版（生产默认，见 §5.3）。
    pub fn decaying(k: usize) -> Self {
        BaselineStore {
            k: k.max(1),
            decay: true,
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

    /// 当前实体键数（含空键集合的键，demo/诊断用）。
    pub fn key_count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// 某键当前保留的窗口数（用于 warmup 判定/诊断）。
    pub fn window_count(&self, entity: &str, metric: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .get(&Key(entity.to_string(), metric.to_string()))
            .map_or(0, |e| e.windows.len())
    }

    /// 收盘追加（幂等：同 `win_start` 重放 → 替换，不重复计数）。
    /// 非有限值（NaN/Inf）整窗忽略——污染会让该键基线全 NaN，判定静默失效。
    pub fn append(&self, entity: &str, metric: &str, window: BaselineWindow) {
        if !window.n.is_finite() || !window.sum.is_finite() || !window.sum_sq.is_finite() {
            return;
        }
        let key = Key(entity.to_string(), metric.to_string());
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.entry(key).or_default();

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
        // 裁剪：只留最近 K 窗（丢最旧）。
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

    /// 由近期窗口合并出 (n, μ, σ)；键不存在或无窗口 → None。
    /// `decay=true` 时按 §5.3 半衰期加权矩（旧窗权重指数衰减，参照最新窗）。
    pub fn summary(&self, entity: &str, metric: &str) -> Option<(f64, f64, f64)> {
        let inner = self.inner.lock().unwrap();
        let entry = inner.get(&Key(entity.to_string(), metric.to_string()))?;
        let t_ref = entry.windows.back().map_or(0i64, |w| w.win_start_nanos);
        let (mut n, mut sum, mut sq) = (0.0f64, 0.0f64, 0.0f64);
        for w in &entry.windows {
            let weight = if self.decay {
                let span = (w.win_end_nanos - w.win_start_nanos).max(1) as f64;
                let half_life = HALF_LIFE_WINDOWS * span;
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

    /// z-score：`value` 相对该键近期窗口基线（μ/σ）的偏离。
    /// 无基线 → None；非有限值 → None；σ≈0 → 0.0（无离散时不判离群，防除零）。
    pub fn deviation(&self, entity: &str, metric: &str, value: f64) -> Option<f64> {
        if !value.is_finite() {
            return None;
        }
        let (_, mu, sigma) = self.summary(entity, metric)?;
        if sigma <= 1e-9 {
            return Some(0.0);
        }
        Some((value - mu) / sigma)
    }
}

// ---------------------------------------------------------------------------
// 全局句柄（镜像 external：cep eval 与 wf-runtime/wf-engine 共享同一实例）
// ---------------------------------------------------------------------------

static STORE: OnceLock<BaselineStore> = OnceLock::new();

/// 安装规则级共享 store（runtime 启动早期调用，指定每键窗口容量 K 与是否
/// 半衰期加权合并）。若已被使用（测试先初始化）返回 false，保持既有实例。
pub fn install(k: usize, decay: bool) -> bool {
    let store = if decay {
        BaselineStore::decaying(k)
    } else {
        BaselineStore::new(k)
    };
    STORE.set(store).is_ok()
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

    fn deviation_oracle(s: &BaselineStore, e: &str, m: &str, v: f64) -> Option<f64> {
        s.deviation(e, m, v)
    }

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
        // 留下的应是最近 3 窗（win_start 120/180/240）
        let inner = s.inner.lock().unwrap();
        let starts: Vec<i64> = inner[&Key("e".into(), "m".into())]
            .windows
            .iter()
            .map(|w| w.win_start_nanos)
            .collect();
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
        let dev = deviation_oracle(&s, "e", "m", 10.0).expect("有基线");
        let expect = (10.0 - 4.0) / (2.0f64 / 3.0f64).sqrt();
        assert!((dev - expect).abs() < 1e-9, "z 失配 {dev} vs {expect}");
        // 未知键 → None
        assert!(s.deviation("nope", "m", 1.0).is_none());
    }

    #[test]
    fn zero_sigma_returns_zero_not_div_zero() {
        // 恒定值窗：σ=0 → 返回 0（不除零、不判离群）
        let s = BaselineStore::new(4);
        s.append("e", "m", win(0, 2.0, 10.0, 50.0)); // [5,5]
        assert_eq!(s.deviation("e", "m", 5.0), Some(0.0));
        // 跨窗合并等权：w1 [5,5] + w2 [5,5] → μ=5 σ=0
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
        // 旧窗（1000 均值）与新窗（2000 均值）：decay 下 μ 偏向新窗（>1500）。
        // 旧窗 age=960s，半衰期 = 4×60s=240s → 权重 0.5^4 = 0.0625。
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
        // 等权对照：μ=1500
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
        let starts: Vec<i64> = inner[&Key("e".into(), "m".into())]
            .windows
            .iter()
            .map(|w| w.win_start_nanos)
            .collect();
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
                        // 同键多窗 + 乱序（前一半倒序追加）
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
        // 每键窗口数 ≤ K，且至少一个键有数据（无 panic/死锁/数据竞争即可）。
        for t in 0..8 {
            assert!(s.window_count(&format!("e{t}"), "m") <= 64);
        }
        assert!(s.key_count() >= 1);
    }
}
