//! stats 状态 spill 存储（2026-08-26 设计，见 `docs/design/stats-state-spill-redb.md`）。
//!
//! ## 分层
//! - [`SpillStore`]：外溢存储抽象（trait）。hot path 只调 [`SpillStore::contains`]
//!   （O(1) 内存操作，不碰磁盘）；put_batch/take 是低频（LRU 驱逐 / spill 键回访）。
//! - [`NoopSpillStore`]：默认（未配置 spill）——`contains` 恒 false，put_batch/
//!   take/drain 空操作，hot path 一个分支预测，零开销。
//! - [`RedbSpillStore`]：redb 持久化（M2 实现，单事务批量写/读回移除/文件清理）。
//!
//! ## 序列化（手写字节编码，非 serde）
//! ScopeKey / StatsAccum / RowFields 的字节编码与防损坏红线见 `serde` 子模块
//! （`spill_serde.rs`），存储实现细节见 `redb` / `store` 子模块。
//!
//! ## 文件组织（2026-09-04 拆件，#[path] sibling 子模块）
//! - `store`（`spill_store.rs`）：[`SpillError`] + [`SpillStore`] trait +
//!   [`NoopSpillStore`] / [`MemSpillStore`]（trait 与 impl 同文件）；
//! - `redb_store`（`spill_redb.rs`）：[`RedbSpillStore`] + 写侧后端 [`RedbBatchWriter`] /
//!   [`SpillItem`]；
//! - `serde`（`spill_serde.rs`）：字节编解码（[`serialize_accs`] / [`deserialize_accs`]
//!   等）；
//! - `tests`（`spill_tests.rs`，cfg(test)）。
//!
//! 公开面逐路径经下方 re-export 保持——`spill::X` 路径与可见级与拆前一致。

#[path = "spill_redb.rs"]
mod redb_store;
#[path = "spill_serde.rs"]
mod serde;
#[path = "spill_store.rs"]
mod store;

pub use redb_store::{RedbBatchWriter, RedbSpillStore, SpillItem};
pub use serde::{
    deserialize_accs, deserialize_scope_key, deserialize_spill_value, serialize_accs,
    serialize_scope_key, serialize_spill_value, spill_hash,
};
pub use store::{MemSpillStore, NoopSpillStore, SpillError, SpillStore};

#[cfg(test)]
#[path = "spill_tests.rs"]
mod tests;
