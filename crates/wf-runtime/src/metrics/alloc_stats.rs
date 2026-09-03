//! 分配器级内存分账（进程范围）——把「内存峰值到底是谁持有的」变成 bench
//! 可直接读的数字，而不是靠外部采样 + 推断。
//!
//! ## 为什么需要
//! q13 的内存问题暴露了度量盲区（见
//! `docs/issues/q13-memory-peak-scales-with-volume.md`）：引擎会计的
//! `window_bytes` 只有 3.9GB（30M）/ 5.7GB（100M），而进程峰值是 14GB / 26GB
//! ——中间那 ~8GB / ~20GB 既看不见持有者，也分不清性质。三个候选性质需要
//! **不同的修复方向**，混在一个 RSS 数字里无法区分：
//!
//! | 分账关系 | 性质 | 方向 |
//! |---|---|---|
//! | `peak_commit ≈ window_bytes`，`peak_rss` 远大 | OS/段区伪影 | 分配器归还策略 |
//! | `peak_commit ≫ window_bytes` | 引擎真持有 | 找持有者（规则状态/缓冲/临时量） |
//!
//! ## 为什么放在这里（而不是测试探针）
//! 现象只在**真实规模 + 持续满载**显形（100M ~30s）。测试里凑不出这个规模，
//! 且测试用 System 分配器、生产用 mimalloc，碎片行为不可比
//! （`memory_probe` 仍保留：它数**请求字节**，用于小规模路径消融与回归保护）。
//!
//! ## 解耦设计
//! wp-reactor 不引 mimalloc 依赖：这里只定义 [`AllocStats`] 与全局
//! provider 钩子，由二进制入口（warp-fusion 的 wfusion CLI，那里持有
//! `#[global_allocator]`）在启动时用 `mi_process_info` 装入实现。未装入时
//! 快照里不出现这些指标（`None`），行为与现状一致。

use std::sync::OnceLock;

/// 分配器/进程级内存读数。字段与 mimalloc `mi_process_info` 对齐；
/// 非 mimalloc 实现可只填能拿到的项（其余 0）。
///
/// macOS 上 `current_rss`/`peak_rss` 由 mimalloc 精确报告；`commit` 是
/// mimalloc 保留的可读写内存（macOS 为估算）——**它才是"分配器实际持有"的
/// 口径**，与 `window_bytes` 对比即可分账。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub struct AllocStats {
    /// 当前工作集（已触碰页）字节。
    pub current_rss: u64,
    /// 进程历史峰值工作集字节。
    pub peak_rss: u64,
    /// 当前已提交（分配器保留的可读写）字节。
    pub current_commit: u64,
    /// 历史峰值已提交字节。
    pub peak_commit: u64,
    /// 硬缺页次数（累计）。
    pub page_faults: u64,
}

/// 读取分配器统计的函数指针。由二进制入口装入（见 [`install_provider`]）。
pub type AllocStatsProvider = fn() -> AllocStats;

static PROVIDER: OnceLock<AllocStatsProvider> = OnceLock::new();

/// 装入分配器统计 provider（只生效一次；重复调用返回 `false`）。
///
/// 由持有 `#[global_allocator]` 的二进制在启动早期调用，例如 wfusion CLI 用
/// `mi_process_info` 实现。库自身不假设分配器种类。
pub fn install_provider(provider: AllocStatsProvider) -> bool {
    PROVIDER.set(provider).is_ok()
}

/// 当前分配器读数；未装入 provider 时返回 `None`（快照略过这些指标）。
pub fn read() -> Option<AllocStats> {
    PROVIDER.get().map(|p| p())
}

/// 是否已装入 provider。
pub fn is_installed() -> bool {
    PROVIDER.get().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 未装入时 `read()` 为 `None`——快照不产出 allocator 指标（与现状一致）。
    /// 注：`PROVIDER` 是进程全局 `OnceLock`，本测试与
    /// `install_provider_is_idempotent` 共享它，故只断言"装入后可读"这一侧，
    /// 避免测试间顺序耦合。
    #[test]
    fn read_returns_none_or_installed_value() {
        match read() {
            None => assert!(!is_installed()),
            Some(_) => assert!(is_installed()),
        }
    }

    /// 装入幂等：首次成功、重复失败；装入后 `read()` 拿到该实现的值。
    #[test]
    fn install_provider_is_idempotent() {
        fn probe() -> AllocStats {
            AllocStats {
                current_rss: 1,
                peak_rss: 2,
                current_commit: 3,
                peak_commit: 4,
                page_faults: 5,
            }
        }
        // 可能已被另一测试装入：两种情形都要自洽。
        let first = install_provider(probe);
        assert!(is_installed(), "装入后必须可见");
        if first {
            let s = read().expect("装入后可读");
            assert_eq!(s.peak_commit, 4);
            assert!(!install_provider(probe), "重复装入必须失败（只生效一次）");
        }
    }
}
