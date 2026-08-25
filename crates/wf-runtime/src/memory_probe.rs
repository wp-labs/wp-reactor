//! 分配器级内存度量（**仅测试构建**）：把「内存峰值是否随数据总量增长」从
//! bench + 外部 footprint 采样，变成确定性的 `cargo test` 断言。
//!
//! ## 为什么需要
//! 窗口层已有精确会计（`window.memory_bytes` / `content_bytes`），但 q13 的
//! 内存问题恰恰**不在窗口里**：30M 窗口 3.87GB / footprint 峰值 14GB，100M
//! 窗口 5.7GB / 峰值 26GB——非窗口部分 10GB→20GB 才是随总量翻倍的那块
//! （见 `docs/issues/q13-memory-peak-scales-with-volume.md`）。窗口会计看不见
//! 它，进程 RSS 又受 mimalloc 段区与机器负载干扰（单点采样已误判过一次）。
//! 本模块补上中间层：**同进程、原子计数、无 OS 干扰**的分配水位。
//!
//! ## 口径
//! - `current`：活跃分配字节（alloc 加、dealloc 减）——稳态占用。
//! - `peak`：`current` 的历史最高水位——对应 footprint 曲线的峰值，是
//!   「内存与总量无关」要断言的量。
//! - 统计的是**请求字节**（`Layout::size()`），不含分配器自身元数据/碎片，
//!   所以数值系统性低于 RSS；用于**同一测试内的规模对比**（N vs 3N），
//!   不与 RSS 绝对值对齐。
//!
//! ## 并发注意
//! 计数器是**进程全局**的：并行测试会互相污染 `peak`。所以规模对比测试必须
//! 通过 [`MemoryProbe::exclusive`] 串行化（内部全局锁），并在测量段起点重置
//! peak 基线。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// 计数分配器：包装 `System`，用两个原子维护 current/peak。
///
/// 热路径开销 = 每次 alloc 一个 `fetch_add` + 一次 CAS 循环更新 peak、
/// 每次 dealloc 一个 `fetch_sub`（Relaxed）。仅测试构建启用，生产二进制
/// （wfusion CLI 的 mimalloc）不受影响。
pub struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            bump(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            // 先减旧再加新：realloc 成功后旧块已释放。
            CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
            bump(new_size);
        }
        new_ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            bump(layout.size());
        }
        ptr
    }
}

fn bump(size: usize) {
    let now = CURRENT.fetch_add(size, Ordering::Relaxed) + size;
    // peak = max(peak, now)：CAS 循环（竞争下重试，Relaxed 足够——peak 只用于
    // 测量报告，不参与同步）。
    let mut peak = PEAK.load(Ordering::Relaxed);
    while now > peak {
        match PEAK.compare_exchange_weak(peak, now, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

/// 测量段的独占锁：并行测试共享全局计数器，`peak` 会互相污染。
static EXCLUSIVE: Mutex<()> = Mutex::new(());

/// 一次内存测量：`exclusive()` 取独占锁并重置 peak 基线，`current()`/`peak()`
/// 读当前水位（相对基线的绝对值——基线处的 current 由 `baseline()` 给出）。
pub struct MemoryProbe {
    _guard: MutexGuard<'static, ()>,
    baseline: usize,
}

impl MemoryProbe {
    /// 取独占测量权（串行化并行测试），把 peak 重置到当前 current。
    ///
    /// 返回的 guard 存活期间其他 `exclusive()` 调用阻塞——测量段内的分配水位
    /// 只反映本测试。注意：其他测试线程若在本段内运行仍会计入（Rust 测试
    /// 框架不提供真正的独占执行），所以规模对比测试建议配
    /// `--test-threads=1`，或接受同量级噪声（对比的是**差值**，共同噪声抵消）。
    pub fn exclusive() -> Self {
        let guard = EXCLUSIVE.lock().unwrap_or_else(|e| e.into_inner());
        let baseline = CURRENT.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        Self {
            _guard: guard,
            baseline,
        }
    }

    /// 测量段起点的活跃字节。
    pub fn baseline(&self) -> usize {
        self.baseline
    }

    /// 当前活跃分配字节（绝对值）。
    pub fn current(&self) -> usize {
        CURRENT.load(Ordering::Relaxed)
    }

    /// 测量段内的历史最高活跃字节（绝对值）。
    pub fn peak(&self) -> usize {
        PEAK.load(Ordering::Relaxed)
    }

    /// 相对基线的峰值增量——**规模对比断言用这个**。
    pub fn peak_growth(&self) -> usize {
        self.peak().saturating_sub(self.baseline)
    }

    /// 相对基线的当前增量（测量段结束时应接近 0：全部释放）。
    pub fn current_growth(&self) -> usize {
        self.current().saturating_sub(self.baseline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 计数器基本正确性：分配 → current/peak 上涨；释放 → current 回落、
    /// peak 保持（历史水位）。
    #[test]
    fn counts_allocations_and_tracks_peak() {
        let probe = MemoryProbe::exclusive();
        assert_eq!(probe.peak_growth(), 0, "基线处峰值增量为 0");

        let big: Vec<u8> = vec![7u8; 4 << 20]; // 4MiB
        let after_alloc = probe.current_growth();
        assert!(
            after_alloc >= 4 << 20,
            "4MiB 分配应被计入（实际 {after_alloc}）"
        );
        let peak_with = probe.peak_growth();
        assert!(peak_with >= 4 << 20);

        drop(big);
        assert!(
            probe.current_growth() < 4 << 20,
            "释放后 current 应回落（实际 {}）",
            probe.current_growth()
        );
        assert!(
            probe.peak_growth() >= peak_with,
            "peak 是历史水位，释放后不回落"
        );
    }

    /// `peak_growth` 对**同量级重复**不累加：连续分配-释放同样大小，峰值
    /// 只反映单次并存量——这是「内存与总量无关」断言的基础语义。
    #[test]
    fn peak_reflects_concurrent_not_cumulative() {
        let probe = MemoryProbe::exclusive();
        for _ in 0..8 {
            let chunk: Vec<u8> = vec![1u8; 1 << 20]; // 1MiB
            std::hint::black_box(&chunk);
        }
        let peak = probe.peak_growth();
        assert!(
            peak < 4 << 20,
            "8 次串行 1MiB 分配-释放，峰值应远小于累计 8MiB（实际 {peak}）"
        );
    }
}
