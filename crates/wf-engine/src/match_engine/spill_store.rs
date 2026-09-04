//! 外溢存储抽象与内存实现（2026-09-04 自 spill.rs 拆出；`#[path]` sibling 子模块）：
//! [`SpillError`] 错误类型 + [`SpillStore`] trait + 默认空实现 [`NoopSpillStore`] +
//! 内存目录 [`MemSpillStore`]（trait 与 impl 同文件，整组自洽）。redb 持久化在
//! `super::redb_store`，字节编解码在 `super::serde`。

use crate::match_engine::ScopeKey;
use crate::match_engine::executor::StatsAccum;

/// spill 存储错误。
#[derive(Debug, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.SpillStore")]
pub enum SpillError {
    /// 反序列化损坏（长度越界 / 未知 tag / 截断）——致命，调用方须 panic。
    Corrupt(String),
    /// 状态含 spill 不支持的形态（如 last 行的结构化 Array/Object 值）——
    /// 致命（显式拒绝，绝不静默改写）。
    Unsupported(String),
    /// 文件 IO 错误（如打开前清空旧文件失败）——致命（绝不打开脏库）。
    Io(std::io::Error),
    /// redb 存储错误（IO/损坏/类型不符）——写失败可回退拒收（§5 三层阶梯），
    /// 读失败致命。
    Redb(redb::Error),
    /// 异步写通道已关闭（store 已 cleanup/窗口结束）——调用方回退拒收。
    Closed,
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Corrupt(msg) => write!(f, "spill 数据损坏: {msg}"),
            SpillError::Unsupported(msg) => write!(f, "spill 不支持: {msg}"),
            SpillError::Io(e) => write!(f, "spill 文件 IO 错误: {e}"),
            SpillError::Redb(e) => write!(f, "redb 错误: {e}"),
            SpillError::Closed => write!(f, "spill 存储已关闭"),
        }
    }
}

impl std::error::Error for SpillError {}

/// 状态外溢存储抽象（见模块文档）。
///
/// **不变量（M3）**：内存桶与 spill 存储**不相交**——驱逐（buckets → put_batch）
/// 与读回（take → buckets）互逆；close 只需 drain + 并入内存，无需 flush。
///
/// **take 只读化（2026-08-26 M5-2）**：`take` **不删除**条目——读回是高频
/// 路径（q18 每键回访 3.4 次），写事务成本不可接受；redb 中保留的旧条目由
/// 调用方在 close 时按「已读回集合」过滤（内存副本更新）。
pub trait SpillStore {
    /// 键是否已 spill（hot path 存在性检查，O(1) 内存操作）。
    fn contains(&self, hash: u64) -> bool;

    /// 批量 spill 多个键（**单次持久层事务**——驱逐是批量事件，逐键事务会
    /// 产生 26M 次独立 txn/fsync）。键已从 buckets 移除后调用。
    fn put_batch(
        &mut self,
        entries: Vec<(u64, ScopeKey, Vec<StatsAccum>)>,
    ) -> Result<(), SpillError>;

    /// 读回一个键（**只读**，低频语义但 q18 高频出现——每键回访 3.4 次）。
    /// 不删除条目（redb 中旧条目由调用方 close 时按已读回集合过滤）。
    fn take(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)>;

    /// 分批读回 spill 键（**流式 close, M5-3**）：每批最多 `n` 个，内部游标
    /// 保持位置，全部读完后返回空。批间顺序无要求（调用方排序）。
    /// 实现须在每批间保持迭代状态（redb 游标 / mem 删除推进）。
    fn drain_up_to(&mut self, n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)>;

    /// close：读回全部 spill 键（非流式路径兼容；流式用 [`Self::drain_up_to`]
    /// 循环——避免全量物化）。默认实现 = drain_up_to 循环。
    fn drain(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        let mut out = Vec::new();
        loop {
            let batch = self.drain_up_to(usize::MAX);
            if batch.is_empty() {
                break;
            }
            out.extend(batch);
        }
        out
    }

    /// 窗口结束清理外部资源（redb 删除文件；Noop/Mem 空操作）。
    /// 调用后本 store 不再可用（新窗口重新 create）。
    fn cleanup(&mut self);

    /// 当前已 spill 键数（诊断/指标）。
    fn len(&self) -> usize;

    /// 是否无 spill 键（默认实现：`len() == 0`）。
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 默认空实现：未配置 spill 时零开销。
#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SpillStore")]
pub struct NoopSpillStore;

impl SpillStore for NoopSpillStore {
    fn contains(&self, _hash: u64) -> bool {
        false
    }
    fn put_batch(
        &mut self,
        _entries: Vec<(u64, ScopeKey, Vec<StatsAccum>)>,
    ) -> Result<(), SpillError> {
        Ok(())
    }
    fn take(&mut self, _hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)> {
        None
    }
    fn drain_up_to(&mut self, _n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        Vec::new()
    }
    fn cleanup(&mut self) {}
    fn len(&self) -> usize {
        0
    }
}

/// 内存 spill 目录（M2 redb 之前的最小可用版）：HashMap<hash, (ScopeKey, accs)>。
/// 用于对拍/测试（与 redb 行为等价，纯内存）。
#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SpillStore")]
pub struct MemSpillStore {
    map: std::collections::HashMap<u64, (ScopeKey, Vec<StatsAccum>)>,
}

impl MemSpillStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SpillStore for MemSpillStore {
    fn contains(&self, hash: u64) -> bool {
        self.map.contains_key(&hash)
    }
    fn put_batch(
        &mut self,
        entries: Vec<(u64, ScopeKey, Vec<StatsAccum>)>,
    ) -> Result<(), SpillError> {
        for (hash, key, accs) in entries {
            self.map.insert(hash, (key, accs));
        }
        Ok(())
    }
    fn take(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)> {
        self.map.get(&hash).map(|(k, a)| (k.clone(), a.clone()))
    }
    fn drain_up_to(&mut self, n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        if n == 0 || self.map.is_empty() {
            return Vec::new();
        }
        // 取前 n 个 hash（迭代序任意）→ remove（推进 = 删除, 分批幂等）。
        let hashes: Vec<u64> = self.map.keys().take(n).copied().collect();
        hashes
            .into_iter()
            .filter_map(|h| self.map.remove(&h))
            .collect()
    }
    fn drain(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        std::mem::take(&mut self.map)
            .into_values()
            .collect::<Vec<_>>()
    }
    fn cleanup(&mut self) {}
    fn len(&self) -> usize {
        self.map.len()
    }
}
