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
//! - [`ScopeKey`] 编码与 `scope_key_hash` 的字节序同构（tag + payload），
//!   round-trip 对拍保证与 `comps_match` / `scope_key_from_comps` 一致。
//! - [`StatsAccum`] 按变体 tag 分派；[`RowFields`] 按 layout 槽序写数组
//!   （**layout 不序列化**——读回时按当前 executor 的 layout 解释，同一
//!   executor 生命周期内不变，成立）。
//!
//! ## 正确性红线
//! 反序列化遇损坏数据 → 返回 `Err(SpillError::Corrupt)`（调用方 panic，绝不
//! 静默丢键）。长度字段带上限校验（防恶意/损坏长度导致 OOM）。

use crate::match_engine::executor::{
    DistinctKey, DistinctSet, NumericAccum, RowFieldLayout, RowFields, StatsAccum, TopEntry,
};
use crate::match_engine::ScopeKey;
use redb::{ReadableDatabase, ReadableTableMetadata};

/// spill 存储错误。
#[derive(Debug)]
pub enum SpillError {
    /// 反序列化损坏（长度越界 / 未知 tag / 截断）——致命，调用方须 panic。
    Corrupt(String),
    /// 状态含 spill 不支持的形态（如 last 行的结构化 Array/Object 值）——
    /// 致命（显式拒绝，绝不静默改写）。
    Unsupported(String),
    /// redb 存储错误（IO/损坏/类型不符）——写失败可回退拒收（§5 三层阶梯），
    /// 读失败致命。
    Redb(redb::Error),
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Corrupt(msg) => write!(f, "spill 数据损坏: {msg}"),
            SpillError::Unsupported(msg) => write!(f, "spill 不支持: {msg}"),
            SpillError::Redb(e) => write!(f, "redb 错误: {e}"),
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
#[derive(Default)]
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
#[derive(Default)]
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

// ---------------------------------------------------------------------------
// redb 持久化实现（M2）
// ---------------------------------------------------------------------------

/// redb 表：`u64 hash → 序列化 spill 值`（`serialize_spill_value` 字节）。
/// 单键/单 hash（M1 trait 即单键语义）；两不同 ScopeKey 撞同一 u64 hash 的
/// 概率 ~2.2e-11（29M 键生日界），put 覆盖旧值——文档化限制（§10），
/// 不引入链式值（§7 的链为碰撞安全所设，实际概率可忽略）。
const REDB_TABLE: redb::TableDefinition<u64, &[u8]> =
    redb::TableDefinition::new("state");

/// redb 持久化实现。
///
/// - 文件按任务实例/窗口隔离（`spill_{rule}_{window_start}.rb`，M3/M4 接线）；
///   本结构只负责单库读写，`cleanup` 删文件（窗口结束/重置时调用）。
/// - `take` 只读（M5-2）：redb 旧条目由调用方 close 时按已读回集合过滤。
/// - `drain_up_to` 流式（M5-3）：游标续读，close 峰值 = 批大小而非全量。
/// - 读失败 = 致命：redb 错误在无 Result 通道的 trait 方法里直接 panic
///   （绝不静默丢键）；`put_batch` 保留 Result 供 M3 三层预算回退拒收。
pub struct RedbSpillStore {
    db: Option<redb::Database>,
    /// 库文件路径（`cleanup` 删除用；含 redb 可能的 `.rbr` 侧车文件）。
    path: std::path::PathBuf,
    /// 读回时解释 RowFields 的 layout（executor 生命周期内不变）。
    layout: std::sync::Arc<RowFieldLayout>,
    /// put_batch 调用计数（每 8 批一次 Immediate 周期 flush, 限脏页）。
    put_batches: u64,
    /// drain_up_to 游标：下一批从 `Excluded(cursor)` 续读（None = 从头）。
    drain_cursor: Option<u64>,
}

impl RedbSpillStore {
    /// 打开/新建库（`Database::create` 语义：文件不存在则初始化，存在则打开）。
    /// 初始化时确保 `state` 表存在（读事务 `open_table` 要求表已存在）。
    ///
    /// **页缓存设界**（2026-08-26 M5 实测）：redb 默认缓存 1GiB/库——q18 10 片
    /// 潜在 10GB 无谓 RSS。取 `WF_SPILL_CACHE_MB`（默认 64MB）。
    ///
    /// **无持久化语义**（设计 §8）：spill 是内存换磁盘的临时缓冲，崩溃即重
    /// ingest——正确性不依赖落盘；`put_batch` 用 `Immediate` 仅为了把脏页
    /// 刷出页缓存（批量低频, fsync 成本可忽略）。
    pub fn create(
        path: impl AsRef<std::path::Path>,
        layout: std::sync::Arc<RowFieldLayout>,
    ) -> Result<Self, SpillError> {
        let cache_mb: usize = std::env::var("WF_SPILL_CACHE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64);
        let db = redb::Database::builder()
            .set_cache_size(cache_mb.saturating_mul(1024 * 1024))
            .create(path.as_ref())
            .map_err(|e| SpillError::Redb(e.into()))?;
        let write_txn = db.begin_write().map_err(|e| SpillError::Redb(e.into()))?;
        {
            let _ = write_txn
                .open_table(REDB_TABLE)
                .map_err(|e| SpillError::Redb(e.into()))?;
        }
        write_txn.commit().map_err(|e| SpillError::Redb(e.into()))?;
        Ok(Self {
            db: Some(db),
            path: path.as_ref().to_path_buf(),
            layout,
            put_batches: 0,
            drain_cursor: None,
        })
    }

    /// 写事务（durability 由调用方指定）：
    /// - `put_batch` 默认 `None`（无 fsync——close 期 drain 读内存页更快）,
    ///   但每 8 批做一次 `Immediate` 周期 flush（redb 的 Immediate 提交会连带
    ///   持久化之前所有 None 提交——脏页有界, 不会爬到 29GB RSS）。
    fn write_txn(&self, durability: redb::Durability) -> Result<redb::WriteTransaction, SpillError> {
        let db = self.db.as_ref().expect("已 cleanup");
        let mut txn = db.begin_write().map_err(|e| SpillError::Redb(e.into()))?;
        txn.set_durability(durability)
            .map_err(|e| SpillError::Redb(e.into()))?;
        Ok(txn)
    }

    fn redb_expect(msg: &str) -> ! {
        panic!("spill redb 失败(致命): {msg}")
    }
}

impl SpillStore for RedbSpillStore {
    fn contains(&self, hash: u64) -> bool {
        let txn = match self.db.as_ref().expect("已 cleanup").begin_read() {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("begin_read: {e}")),
        };
        let table = match txn.open_table(REDB_TABLE) {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("open_table: {e}")),
        };
        match table.get(hash) {
            Ok(v) => v.is_some(),
            Err(e) => Self::redb_expect(&format!("get: {e}")),
        }
    }

    fn put_batch(
        &mut self,
        entries: Vec<(u64, ScopeKey, Vec<StatsAccum>)>,
    ) -> Result<(), SpillError> {
        if entries.is_empty() {
            return Ok(());
        }
        // 单事务批量写（驱逐批量事件——逐键事务/fsync 会压死 26M 键场景）。
        // 默认 None（无 fsync, close drain 读内存页更快）; 每 8 批一次 Immediate
        // 周期 flush（redb 连带持久化之前所有 None 提交——脏页有界）。
        self.put_batches += 1;
        let durability = if self.put_batches.is_multiple_of(8) {
            redb::Durability::Immediate
        } else {
            redb::Durability::None
        };
        let write_txn = self.write_txn(durability)?;
        {
            let mut table = write_txn
                .open_table(REDB_TABLE)
                .map_err(|e| SpillError::Redb(e.into()))?;
            for (hash, key, accs) in entries {
                let bytes = serialize_spill_value(&key, &accs)?;
                table
                    .insert(hash, bytes.as_slice())
                    .map_err(|e| SpillError::Redb(e.into()))?;
            }
        }
        write_txn.commit().map_err(|e| SpillError::Redb(e.into()))?;
        Ok(())
    }

    /// 只读读回（M5-2）：`begin_read + get`——无写事务/WAL/锁争用（读事务并发）。
    /// **不删除条目**：redb 中旧条目由调用方 close 时按「已读回集合」过滤。
    fn take(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)> {
        let db = self.db.as_ref().expect("已 cleanup");
        let txn = match db.begin_read() {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("begin_read: {e}")),
        };
        let table = match txn.open_table(REDB_TABLE) {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("open_table: {e}")),
        };
        let guard = match table.get(hash) {
            Ok(v) => v?,
            Err(e) => Self::redb_expect(&format!("get: {e}")),
        };
        let bytes = guard.value().to_vec();
        match deserialize_spill_value(&bytes, &self.layout) {
            Ok(v) => Some(v),
            Err(e) => panic!("spill 读回损坏(致命): {e}"),
        }
    }

    /// 流式分批读回（M5-3）：从 `Excluded(drain_cursor)` 起取最多 n 条，
    /// 推进游标。全部读完后返回空（游标到尾部）。close 峰值 = 批大小。
    /// 不删除条目（close 后 cleanup 删整个文件）。
    fn drain_up_to(&mut self, n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        if n == 0 {
            return Vec::new();
        }
        let db = self.db.as_ref().expect("已 cleanup");
        let txn = match db.begin_read() {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("begin_read: {e}")),
        };
        let table = match txn.open_table(REDB_TABLE) {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("open_table: {e}")),
        };
        let start = match self.drain_cursor {
            Some(last) => std::ops::Bound::Excluded(last),
            None => std::ops::Bound::Unbounded,
        };
        let range = match table.range((start, std::ops::Bound::Unbounded)) {
            Ok(r) => r,
            Err(e) => Self::redb_expect(&format!("range: {e}")),
        };
        let mut out = Vec::new();
        for entry in range.take(n) {
            let (k, v) = match entry {
                Ok(e) => e,
                Err(e) => Self::redb_expect(&format!("range 条目: {e}")),
            };
            let hash = k.value();
            let bytes = v.value().to_vec();
            let (key, accs) = deserialize_spill_value(&bytes, &self.layout)
                .unwrap_or_else(|e| panic!("spill 读回损坏(致命): {e}"));
            self.drain_cursor = Some(hash);
            out.push((key, accs));
        }
        out
    }

    fn cleanup(&mut self) {
        // 关闭数据库（释放文件句柄）后删除库文件与可能的侧车 WAL。
        self.db.take();
        let path = &self.path;
        for p in [path.clone(), path.with_extension("rbr")] {
            if p.exists()
                && let Err(e) = std::fs::remove_file(&p)
            {
                log::warn!("spill 清理残留失败 {}: {e}", p.display());
            }
        }
    }

    fn len(&self) -> usize {
        let txn = match self.db.as_ref().expect("已 cleanup").begin_read() {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("begin_read: {e}")),
        };
        let table = match txn.open_table(REDB_TABLE) {
            Ok(t) => t,
            Err(e) => Self::redb_expect(&format!("open_table: {e}")),
        };
        match table.len() {
            Ok(n) => n as usize,
            Err(e) => Self::redb_expect(&format!("len: {e}")),
        }
    }
}

// ---------------------------------------------------------------------------
// 字节写入器/读取器（小端，长度前缀带上限）
// ---------------------------------------------------------------------------

/// 单键/单桶的序列化长度上限（防护：损坏长度导致 OOM）。ScopeKey 树 8 层、
/// accs 16 度量、行字段 64 字段的合理上界 ~1MB。
const MAX_SERIALIZED_BYTES: usize = 1 << 20;

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn i128(&mut self, v: i128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn u8(&mut self) -> Result<u8, SpillError> {
        let v = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| SpillError::Corrupt("u8 越界".into()))?;
        self.pos += 1;
        Ok(v)
    }
    fn u64(&mut self) -> Result<u64, SpillError> {
        let end = self
            .pos
            .checked_add(8)
            .ok_or_else(|| SpillError::Corrupt("u64 长度溢出".into()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| SpillError::Corrupt("u64 越界".into()))?;
        self.pos = end;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, SpillError> {
        Ok(self.u64()? as i64)
    }
    fn i128(&mut self) -> Result<i128, SpillError> {
        let end = self
            .pos
            .checked_add(16)
            .ok_or_else(|| SpillError::Corrupt("i128 长度溢出".into()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| SpillError::Corrupt("i128 越界".into()))?;
        self.pos = end;
        Ok(i128::from_le_bytes(bytes.try_into().unwrap()))
    }
    fn f64(&mut self) -> Result<f64, SpillError> {
        Ok(f64::from_bits(self.u64()?))
    }
    fn bytes(&mut self) -> Result<&'a [u8], SpillError> {
        let len = self.u64()? as usize;
        if len > MAX_SERIALIZED_BYTES {
            return Err(SpillError::Corrupt(format!("bytes 长度 {len} 超上限")));
        }
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| SpillError::Corrupt("bytes 长度溢出".into()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| SpillError::Corrupt("bytes 越界".into()))?;
        self.pos = end;
        Ok(bytes)
    }
}

// ---------------------------------------------------------------------------
// ScopeKey 序列化
// ---------------------------------------------------------------------------

/// tag 常量（与 `scope_key_hash` 的 tag 同构：Empty=0 Int=1 Float=2 Str=3 Pair=4）。
const TAG_EMPTY: u8 = 0;
const TAG_INT: u8 = 1;
const TAG_FLOAT: u8 = 2;
const TAG_STR: u8 = 3;
const TAG_PAIR: u8 = 4;

/// 递归编码（字节序与 `scope_key_hash` 同构——`comps_hash` 镜像）。
/// 嵌套深度限 [`MAX_SCOPE_KEY_DEPTH`]（防损坏数据超深 Pair 递归栈溢出）。
fn write_scope_key(w: &mut Writer, key: &ScopeKey) {
    match key {
        ScopeKey::Empty => w.u8(TAG_EMPTY),
        ScopeKey::Int(v) => {
            w.u8(TAG_INT);
            w.i64(*v);
        }
        ScopeKey::Float(bits) => {
            w.u8(TAG_FLOAT);
            w.u64(*bits);
        }
        ScopeKey::Str(s) => {
            w.u8(TAG_STR);
            w.bytes(s.as_bytes());
        }
        ScopeKey::Pair(a, b) => {
            w.u8(TAG_PAIR);
            write_scope_key(w, a);
            write_scope_key(w, b);
        }
    }
}

/// ScopeKey 树嵌套深度上限（正常键组合 ~8 层；深度超限 = 损坏）。
const MAX_SCOPE_KEY_DEPTH: usize = 32;

fn read_scope_key(r: &mut Reader<'_>) -> Result<ScopeKey, SpillError> {
    read_scope_key_depth(r, 0)
}

fn read_scope_key_depth(r: &mut Reader<'_>, depth: usize) -> Result<ScopeKey, SpillError> {
    if depth > MAX_SCOPE_KEY_DEPTH {
        return Err(SpillError::Corrupt("ScopeKey 嵌套过深".into()));
    }
    match r.u8()? {
        TAG_EMPTY => Ok(ScopeKey::Empty),
        TAG_INT => Ok(ScopeKey::Int(r.i64()?)),
        TAG_FLOAT => Ok(ScopeKey::Float(r.u64()?)),
        TAG_STR => {
            let s = r.bytes()?;
            let s = std::str::from_utf8(s)
                .map_err(|_| SpillError::Corrupt("ScopeKey Str 非 UTF-8".into()))?;
            Ok(ScopeKey::Str(s.into()))
        }
        TAG_PAIR => {
            let a = read_scope_key_depth(r, depth + 1)?;
            let b = read_scope_key_depth(r, depth + 1)?;
            Ok(ScopeKey::Pair(Box::new(a), Box::new(b)))
        }
        other => Err(SpillError::Corrupt(format!("ScopeKey 未知 tag {other}"))),
    }
}

// ---------------------------------------------------------------------------
// StatsAccum 序列化
// ---------------------------------------------------------------------------

/// StatsAccum 变体 tag。
const TAG_NUMERIC: u8 = 0;
const TAG_DISTINCT: u8 = 1;
const TAG_LAST: u8 = 2;
const TAG_TOP: u8 = 3;

/// RowFields 序列化：按 layout 槽序写数组（layout 不序列化——读回时外部传入）。
/// 写：numeric（f64×n）→ strings（bytes×n，SmolStr）→ others（tag+payload）→ null_mask。
fn write_row_fields(w: &mut Writer, rf: &RowFields) -> Result<(), SpillError> {
    let layout = rf.layout();
    w.u64(layout.n_fields() as u64);
    // 直接读内部数组（layout 槽序，与 value_at 口径一致；访问器 pub(crate) 同 crate）。
    for v in rf.numeric() {
        w.f64(*v);
    }
    for s in rf.strings() {
        w.bytes(s.as_bytes());
    }
    for v in rf.others() {
        match v {
            None => w.u8(0),
            Some(v) => {
                w.u8(1);
                write_value(w, v)?;
            }
        }
    }
    for m in rf.null_mask() {
        w.u64(*m);
    }
    Ok(())
}

/// Value 序列化（RowFields.others 的 `Option<Value>` 用）。
/// Array/Object 结构化值拒绝 spill（否则读回空值 = 静默丢数据）。
fn write_value(w: &mut Writer, v: &crate::match_engine::Value) -> Result<(), SpillError> {
    match v {
        crate::match_engine::Value::Number(n) => {
            w.u8(0);
            w.f64(*n);
            Ok(())
        }
        crate::match_engine::Value::Str(s) => {
            w.u8(1);
            w.bytes(s.as_bytes());
            Ok(())
        }
        crate::match_engine::Value::Bool(b) => {
            w.u8(2);
            w.u8(*b as u8);
            Ok(())
        }
        crate::match_engine::Value::Array(_) => Err(SpillError::Unsupported(
            "RowFields others 含 Array 值".into(),
        )),
        crate::match_engine::Value::Object(_) => Err(SpillError::Unsupported(
            "RowFields others 含 Object 值".into(),
        )),
    }
}

fn read_value(r: &mut Reader<'_>) -> Result<crate::match_engine::Value, SpillError> {
    Ok(match r.u8()? {
        0 => crate::match_engine::Value::Number(r.f64()?),
        1 => {
            let s = r.bytes()?;
            crate::match_engine::Value::Str(
                std::str::from_utf8(s)
                    .map_err(|_| SpillError::Corrupt("Value Str 非 UTF-8".into()))?
                    .into(),
            )
        }
        2 => crate::match_engine::Value::Bool(r.u8()? != 0),
        3 => crate::match_engine::Value::Array(Vec::new()),
        4 => crate::match_engine::Value::Object(Default::default()),
        other => return Err(SpillError::Corrupt(format!("Value 未知 tag {other}"))),
    })
}

/// DistinctKey 序列化。
fn write_distinct_key(w: &mut Writer, k: &DistinctKey) {
    match k {
        DistinctKey::Int(v) => {
            w.u8(0);
            w.i64(*v);
        }
        DistinctKey::Float(bits) => {
            w.u8(1);
            w.u64(*bits);
        }
        DistinctKey::Str(s) => {
            w.u8(2);
            w.bytes(s.as_bytes());
        }
    }
}

fn read_distinct_key(r: &mut Reader<'_>) -> Result<DistinctKey, SpillError> {
    Ok(match r.u8()? {
        0 => DistinctKey::Int(r.i64()?),
        1 => DistinctKey::Float(r.u64()?),
        2 => DistinctKey::Str(
            std::str::from_utf8(r.bytes()?)
                .map_err(|_| SpillError::Corrupt("DistinctKey Str 非 UTF-8".into()))?
                .into(),
        ),
        other => return Err(SpillError::Corrupt(format!("DistinctKey 未知 tag {other}"))),
    })
}

/// 序列化 accs 数组 + 每桶行字段 layout（写时随 accs 写 layout 描述）。
/// 返回 (编码字节, 写时 layout 的字段名序) —— 读回时若 layout 不一致需重建。
pub fn serialize_accs(accs: &[StatsAccum]) -> Result<Vec<u8>, SpillError> {
    let mut w = Writer::new();
    w.u64(accs.len() as u64);
    for acc in accs {
        match acc {
            StatsAccum::Numeric(n) => {
                w.u8(TAG_NUMERIC);
                w.u64(n.count);
                // sum/min/max 为 i128（可超 i64）——全宽写，读回无截断。
                w.i128(n.sum);
                match n.min {
                    Some(m) => {
                        w.u8(1);
                        w.i128(m);
                    }
                    None => w.u8(0),
                }
                match n.max {
                    Some(m) => {
                        w.u8(1);
                        w.i128(m);
                    }
                    None => w.u8(0),
                }
            }
            StatsAccum::Distinct(d) => {
                w.u8(TAG_DISTINCT);
                // ints 集合
                let ints: Vec<i64> = d.ints().iter().copied().collect();
                w.u64(ints.len() as u64);
                for v in ints {
                    w.i64(v);
                }
                // others 集合
                let others: Vec<&DistinctKey> = d.others().iter().collect();
                w.u64(others.len() as u64);
                for k in others {
                    write_distinct_key(&mut w, k);
                }
            }
            StatsAccum::Last(rf) => {
                w.u8(TAG_LAST);
                match rf {
                    Some(rf) => {
                        w.u8(1);
                        write_row_fields(&mut w, rf)?;
                    }
                    None => w.u8(0),
                }
            }
            StatsAccum::Top(entries) => {
                w.u8(TAG_TOP);
                w.u64(entries.len() as u64);
                for e in entries {
                    w.f64(e.key);
                    write_row_fields(&mut w, &e.row)?;
                }
            }
        }
    }
    if w.buf.len() > MAX_SERIALIZED_BYTES {
        return Err(SpillError::Corrupt(format!(
            "accs 序列化超上限 {}B",
            w.buf.len()
        )));
    }
    Ok(w.finish())
}

/// 反序列化 accs 数组。`layout` = 当前 executor 的 RowFieldLayout（读回
/// RowFields 按此解释；若与写时 layout 不一致 → Corrupt）。
pub fn deserialize_accs(
    bytes: &[u8],
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<Vec<StatsAccum>, SpillError> {
    let mut r = Reader::new(bytes);
    let n = r.u64()? as usize;
    if n > 1024 {
        return Err(SpillError::Corrupt(format!("accs 数量 {n} 超上限")));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let acc = match r.u8()? {
            TAG_NUMERIC => {
                let count = r.u64()?;
                let sum = r.i128()?;
                let min = if r.u8()? == 1 {
                    Some(r.i128()?)
                } else {
                    None
                };
                let max = if r.u8()? == 1 {
                    Some(r.i128()?)
                } else {
                    None
                };
                StatsAccum::Numeric(Box::new(NumericAccum {
                    count,
                    sum,
                    min,
                    max,
                }))
            }
            TAG_DISTINCT => {
                let n_ints = r.u64()? as usize;
                if n_ints > MAX_SERIALIZED_BYTES / 8 {
                    return Err(SpillError::Corrupt("distinct ints 超上限".into()));
                }
                let mut ints = crate::match_engine::EngineHashSet::default();
                for _ in 0..n_ints {
                    ints.insert(r.i64()?);
                }
                let n_others = r.u64()? as usize;
                if n_others > MAX_SERIALIZED_BYTES / 8 {
                    return Err(SpillError::Corrupt("distinct others 超上限".into()));
                }
                let mut others = crate::match_engine::EngineHashSet::default();
                for _ in 0..n_others {
                    others.insert(read_distinct_key(&mut r)?);
                }
                StatsAccum::Distinct(Box::new(DistinctSet::from_parts(ints, others)))
            }
            TAG_LAST => {
                let rf = if r.u8()? == 1 {
                    Some(read_row_fields_with_layout(&mut r, layout)?)
                } else {
                    None
                };
                StatsAccum::Last(rf.map(std::sync::Arc::new))
            }
            TAG_TOP => {
                let n = r.u64()? as usize;
                if n > MAX_SERIALIZED_BYTES / 64 {
                    return Err(SpillError::Corrupt("top 条目超上限".into()));
                }
                let mut entries = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = r.f64()?;
                    let row = read_row_fields_with_layout(&mut r, layout)?;
                    entries.push(TopEntry { key, row });
                }
                StatsAccum::Top(entries)
            }
            other => return Err(SpillError::Corrupt(format!("StatsAccum 未知 tag {other}"))),
        };
        out.push(acc);
    }
    Ok(out)
}

/// RowFields 反序列化（带 layout 版）。
fn read_row_fields_with_layout(
    r: &mut Reader<'_>,
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<RowFields, SpillError> {
    let n_fields = r.u64()? as usize;
    if n_fields != layout.n_fields() {
        return Err(SpillError::Corrupt(format!(
            "RowFields 字段数 {n_fields} != layout {}",
            layout.n_fields()
        )));
    }
    let mut numeric = Vec::with_capacity(layout.n_numeric());
    for _ in 0..layout.n_numeric() {
        numeric.push(r.f64()?);
    }
    let mut strings = Vec::with_capacity(layout.n_strings());
    for _ in 0..layout.n_strings() {
        let s = r.bytes()?;
        strings.push(
            std::str::from_utf8(s)
                .map_err(|_| SpillError::Corrupt("RowFields Str 非 UTF-8".into()))?
                .into(),
        );
    }
    let mut others = Vec::with_capacity(layout.n_others());
    for _ in 0..layout.n_others() {
        others.push(if r.u8()? == 1 {
            Some(read_value(r)?)
        } else {
            None
        });
    }
    let n_words = n_fields.div_ceil(64);
    let mut null_mask = Vec::with_capacity(n_words);
    for _ in 0..n_words {
        null_mask.push(r.u64()?);
    }
    Ok(RowFields::from_parts(
        std::sync::Arc::clone(layout),
        numeric.into_boxed_slice(),
        strings.into_boxed_slice(),
        others.into_boxed_slice(),
        null_mask.into_boxed_slice(),
    ))
}

/// ScopeKey 序列化。
pub fn serialize_scope_key(key: &ScopeKey) -> Vec<u8> {
    let mut w = Writer::new();
    write_scope_key(&mut w, key);
    w.finish()
}

/// ScopeKey 反序列化。
pub fn deserialize_scope_key(bytes: &[u8]) -> Result<ScopeKey, SpillError> {
    let mut r = Reader::new(bytes);
    let key = read_scope_key(&mut r)?;
    // 尾部不应有残留（严格性：长度不符 = 损坏）。
    if r.pos != bytes.len() {
        return Err(SpillError::Corrupt("ScopeKey 序列化尾部残留".into()));
    }
    Ok(key)
}

/// 便捷：完整 spill 值（key + accs）编码（redb value = 此字节）。
pub fn serialize_spill_value(key: &ScopeKey, accs: &[StatsAccum]) -> Result<Vec<u8>, SpillError> {
    let mut w = Writer::new();
    write_scope_key(&mut w, key);
    let accs_bytes = serialize_accs(accs)?;
    w.bytes(&accs_bytes);
    Ok(w.finish())
}

/// 便捷：完整 spill 值解码。
pub fn deserialize_spill_value(
    bytes: &[u8],
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<(ScopeKey, Vec<StatsAccum>), SpillError> {
    let mut r = Reader::new(bytes);
    let key = read_scope_key(&mut r)?;
    let accs_bytes = r.bytes()?;
    let accs = deserialize_accs(accs_bytes, layout)?;
    // 尾部不应有残留（严格性：长度不符 = 损坏）。
    if r.pos != bytes.len() {
        return Err(SpillError::Corrupt("spill value 序列化尾部残留".into()));
    }
    Ok((key, accs))
}

/// 便捷：单桶 hash（spill key 用 `scope_key_hash` 同值）。
pub fn spill_hash(key: &ScopeKey) -> u64 {
    crate::match_engine::executor::scope_key_hash(key)
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_engine::executor::RowFieldLayout;

    fn sample_layout() -> std::sync::Arc<RowFieldLayout> {
        // numeric: price/dateTime；str: channel/url；other: 1 个。
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new(
                "dateTime",
                arrow::datatypes::DataType::Int64,
                false,
            ),
            arrow::datatypes::Field::new("channel", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("url", arrow::datatypes::DataType::Utf8, false),
        ]);
        std::sync::Arc::new(RowFieldLayout::from_schema(
            &["price", "dateTime", "channel", "url"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            &schema,
        ))
    }

    #[test]
    fn scope_key_roundtrip_all_variants() {
        let keys = [
            ScopeKey::Empty,
            ScopeKey::Int(42),
            ScopeKey::Int(-7),
            ScopeKey::Float(1234.5f64.to_bits()),
            ScopeKey::Str("hello".into()),
            ScopeKey::Pair(
                Box::new(ScopeKey::Int(1)),
                Box::new(ScopeKey::Str("a".into())),
            ),
            ScopeKey::Pair(
                Box::new(ScopeKey::Pair(
                    Box::new(ScopeKey::Int(1)),
                    Box::new(ScopeKey::Int(2)),
                )),
                Box::new(ScopeKey::Str("深".into())),
            ),
        ];
        for k in &keys {
            let bytes = serialize_scope_key(k);
            let back = deserialize_scope_key(&bytes).expect("roundtrip");
            assert_eq!(&back, k, "ScopeKey roundtrip 不一致: {k:?}");
        }
    }

    #[test]
    fn scope_key_corrupt_rejected() {
        // 未知 tag
        assert!(matches!(
            deserialize_scope_key(&[99]),
            Err(SpillError::Corrupt(_))
        ));
        // 截断（Int 缺 payload）
        assert!(matches!(
            deserialize_scope_key(&[TAG_INT]),
            Err(SpillError::Corrupt(_))
        ));
        // 尾部残留
        let bytes = serialize_scope_key(&ScopeKey::Int(1));
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(matches!(
            deserialize_scope_key(&bad),
            Err(SpillError::Corrupt(_))
        ));
    }

    #[test]
    fn scope_key_deep_pair_nesting_rejected() {
        // 深度超限（构造 64 层 Pair）→ Corrupt（非栈溢出）
        let mut bytes = vec![TAG_PAIR; 64];
        bytes.push(TAG_EMPTY);
        bytes.push(TAG_EMPTY);
        assert!(matches!(
            deserialize_scope_key(&bytes),
            Err(SpillError::Corrupt(msg)) if msg.contains("嵌套过深")
        ));
    }

    #[test]
    fn numeric_accum_i128_wide_roundtrip() {
        // sum/min/max 超 i64 范围（1<<70 ≈ 1.18e21 > i64::MAX ≈ 9.2e18）——
        // 全宽往返，无截断。
        let layout = sample_layout();
        let accs = vec![StatsAccum::Numeric(Box::new(NumericAccum {
            count: 3,
            sum: (1i128 << 70) + 12345,
            min: Some(-(1i128 << 65) - 7),
            max: Some((1i128 << 66) + 999),
        }))];
        let bytes = serialize_accs(&accs).expect("serialize");
        let back = deserialize_accs(&bytes, &layout).expect("deserialize");
        let n = back[0].numeric();
        assert_eq!(n.count, 3);
        assert_eq!(n.sum, (1i128 << 70) + 12345);
        assert_eq!(n.min, Some(-(1i128 << 65) - 7));
        assert_eq!(n.max, Some((1i128 << 66) + 999));
    }

    #[test]
    fn structured_value_in_last_rejected_not_silently_dropped() {
        // Boolean 字段在 from_schema 中路由到 others 槽——Array 值若出现
        // 必须显式拒绝（Unsupported），不能静默改写成空值。
        let bool_layout = std::sync::Arc::new(RowFieldLayout::from_schema(
            &["flag".to_string()],
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "flag",
                arrow::datatypes::DataType::Boolean,
                false,
            )]),
        ));
        let mut rf = RowFields::empty(std::sync::Arc::clone(&bool_layout));
        rf.set(0, Some(crate::match_engine::Value::Array(vec![])));
        let accs = vec![StatsAccum::Last(Some(std::sync::Arc::new(rf)))];
        assert!(matches!(
            serialize_accs(&accs),
            Err(SpillError::Unsupported(_))
        ));

        // Bool（合法的 others 值）往返不受影响
        let mut rf2 = RowFields::empty(std::sync::Arc::clone(&bool_layout));
        rf2.set(0, Some(crate::match_engine::Value::Bool(true)));
        let accs2 = vec![StatsAccum::Last(Some(std::sync::Arc::new(rf2)))];
        let bytes = serialize_accs(&accs2).expect("serialize");
        let back = deserialize_accs(&bytes, &bool_layout).expect("deserialize");
        let rf_back = back[0].last().as_ref().expect("last");
        assert_eq!(
            rf_back.value_at(0),
            Some(crate::match_engine::Value::Bool(true))
        );
    }

    #[test]
    fn stats_accum_roundtrip_all_variants() {
        let layout = sample_layout();
        // Numeric
        let numeric = StatsAccum::Numeric(Box::new(NumericAccum {
            count: 5,
            sum: 100,
            min: Some(10),
            max: Some(30),
        }));
        // Distinct
        let mut d = DistinctSet::default();
        d.insert(DistinctKey::Int(1));
        d.insert(DistinctKey::Int(2));
        d.insert(DistinctKey::Float(1.5f64.to_bits()));
        d.insert(DistinctKey::Str("x".into()));
        let distinct = StatsAccum::Distinct(Box::new(d));
        // Last
        let mut rf = RowFields::empty(std::sync::Arc::clone(&layout));
        rf.set(0, Some(crate::match_engine::Value::Number(9800.0)));
        rf.set(2, Some(crate::match_engine::Value::Str("Google".into())));
        let last = StatsAccum::Last(Some(std::sync::Arc::new(rf)));
        // Top
        let mut e1 = RowFields::empty(std::sync::Arc::clone(&layout));
        e1.set(1, Some(crate::match_engine::Value::Number(1.0)));
        let top = StatsAccum::Top(vec![TopEntry {
            key: 100.0,
            row: e1,
        }]);

        let accs = vec![numeric, distinct, last, top];
        let bytes = serialize_accs(&accs).expect("serialize");
        let back = deserialize_accs(&bytes, &layout).expect("deserialize");
        assert_eq!(back.len(), accs.len());
        // Numeric 逐字段
        assert_eq!(back[0].numeric().count, 5);
        assert_eq!(back[0].numeric().sum, 100);
        assert_eq!(back[0].numeric().min, Some(10));
        assert_eq!(back[0].numeric().max, Some(30));
        // Distinct 集合
        let StatsAccum::Distinct(d) = &back[1] else {
            panic!("期望 Distinct 变体");
        };
        assert_eq!(d.len(), 4);
        // Last 行字段
        let last_back = back[2].last().as_ref().expect("last");
        assert_eq!(last_back.value_at(0), Some(crate::match_engine::Value::Number(9800.0)));
        assert_eq!(last_back.value_at(2), Some(crate::match_engine::Value::Str("Google".into())));
        // Top
        assert_eq!(back[3].top().len(), 1);
        assert_eq!(back[3].top()[0].key, 100.0);
    }

    #[test]
    fn spill_value_roundtrip_with_layout_mismatch_rejected() {
        let layout = sample_layout();
        let key = ScopeKey::Pair(
            Box::new(ScopeKey::Int(123)),
            Box::new(ScopeKey::Int(456)),
        );
        let accs = vec![StatsAccum::Last(None)];
        let bytes = serialize_spill_value(&key, &accs).expect("serialize");
        let (k, a) = deserialize_spill_value(&bytes, &layout).expect("deserialize");
        assert_eq!(k, key);
        assert_eq!(a.len(), 1);
        assert!(matches!(a[0], StatsAccum::Last(None)));

        // layout 字段数不一致 → Corrupt
        let other_layout = std::sync::Arc::new(RowFieldLayout::from_schema(
            &["only_one".to_string()],
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "only_one",
                arrow::datatypes::DataType::Int64,
                false,
            )]),
        ));
        let mut rf = RowFields::empty(std::sync::Arc::clone(&layout));
        rf.set(0, Some(crate::match_engine::Value::Number(1.0)));
        let accs2 = vec![StatsAccum::Last(Some(std::sync::Arc::new(rf)))];
        let bytes2 = serialize_accs(&accs2).expect("serialize");
        assert!(matches!(
            deserialize_accs(&bytes2, &other_layout),
            Err(SpillError::Corrupt(_))
        ));

        // 尾部残留 → Corrupt
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            deserialize_spill_value(&trailing, &layout),
            Err(SpillError::Corrupt(_))
        ));
    }

    #[test]
    fn noop_spill_is_empty() {
        let mut s = NoopSpillStore;
        assert!(!s.contains(1));
        assert!(s.take(1).is_none());
        assert!(s.drain().is_empty());
        assert_eq!(s.len(), 0);
        assert!(s.put_batch(vec![(1, ScopeKey::Int(1), vec![])]).is_ok());
        assert!(!s.contains(1));
        s.cleanup();
    }

    #[test]
    fn mem_spill_roundtrip() {
        let mut s = MemSpillStore::new();
        let key = ScopeKey::Pair(
            Box::new(ScopeKey::Int(1)),
            Box::new(ScopeKey::Int(2)),
        );
        let accs = vec![StatsAccum::Last(None)];
        s.put_batch(vec![(spill_hash(&key), key.clone(), accs)])
            .expect("put");
        assert!(s.contains(spill_hash(&key)));
        assert_eq!(s.len(), 1);
        // take 只读回（M5-2：不删除——close 由调用方按已读回集合过滤）
        let (k, a) = s.take(spill_hash(&key)).expect("take");
        assert_eq!(k, key);
        assert_eq!(a.len(), 1);
        assert_eq!(s.len(), 1, "take 不删除条目");
        assert!(s.contains(spill_hash(&key)));
        // 覆盖更新后 drain 全部 + 清空
        s.put_batch(vec![(spill_hash(&key), key, vec![StatsAccum::Last(None)])])
            .expect("put");
        let drained = s.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(s.len(), 0);
        s.cleanup();
    }

    /// 测试用唯一路径（temp 目录 + 名称 + pid + 纳秒，防并行测试撞文件）。
    fn spill_test_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_test_{}_{}_{}.rb",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    #[test]
    fn redb_spill_roundtrip_and_drain() {
        let layout = sample_layout();
        let path = spill_test_path("redb_roundtrip");
        let mut s = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout))
            .expect("create");

        let k1 = ScopeKey::Int(1001);
        let accs1 = vec![
            StatsAccum::Numeric(Box::new(NumericAccum {
                count: 2,
                sum: 30,
                min: Some(10),
                max: Some(20),
            })),
            StatsAccum::Last(None),
        ];
        let h1 = spill_hash(&k1);

        let k2 = ScopeKey::Pair(
            Box::new(ScopeKey::Int(1)),
            Box::new(ScopeKey::Str("auction".into())),
        );
        let mut rf = RowFields::empty(std::sync::Arc::clone(&layout));
        rf.set(0, Some(crate::match_engine::Value::Number(9800.0)));
        let accs2 = vec![StatsAccum::Top(vec![TopEntry { key: 1.5, row: rf }])];
        let h2 = spill_hash(&k2);

        s.put_batch(vec![(h1, k1.clone(), accs1), (h2, k2.clone(), accs2)])
            .expect("put_batch");

        assert!(s.contains(h1));
        assert!(s.contains(h2));
        assert!(!s.contains(u64::MAX));
        assert_eq!(s.len(), 2);

        // take 只读回（不删除——redb 旧条目 close 时按已读回集合过滤）
        let (gk, ga) = s.take(h1).expect("take1");
        assert_eq!(gk, k1);
        assert_eq!(ga[0].numeric().count, 2);
        assert_eq!(s.len(), 2, "take 不删除条目");
        assert!(s.contains(h1), "take 后条目仍在");

        // 覆盖更新（读回键再驱逐会 put 覆盖旧条目）后 drain 全部
        s.put_batch(vec![(h1, k1, vec![StatsAccum::Last(None)])])
            .expect("put again");
        let mut drained = s.drain();
        drained.sort_by_key(|(k, _)| format!("{k:?}"));
        assert_eq!(drained.len(), 2);
        // drain 不重写树（M5-2: 紧跟 cleanup 删文件）——条目仍在, cleanup 后消失
        assert_eq!(s.len(), 2, "drain 后条目仍保留（cleanup 删文件）");
        s.cleanup();
        assert!(!path.exists());
    }

    #[test]
    fn redb_persists_across_reopen() {
        let layout = sample_layout();
        let path = spill_test_path("redb_reopen");
        let k = ScopeKey::Str("persist".into());
        let h = spill_hash(&k);
        {
            let mut s = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout))
                .expect("create");
            s.put_batch(vec![(h, k.clone(), vec![StatsAccum::Last(None)])])
                .expect("put");
            // 不 cleanup（模拟崩溃残留/跨窗口复用前重开）
        }
        // 重开（create 对已存在文件 = open）：数据仍在
        let mut s2 = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout))
            .expect("reopen");
        assert!(s2.contains(h));
        let (k2, a2) = s2.take(h).expect("take");
        assert_eq!(k2, k);
        assert!(matches!(a2[0], StatsAccum::Last(None)));
        assert!(s2.contains(h), "take 只读——条目保留");
        s2.cleanup();
        assert!(!path.exists());
    }

    #[test]
    fn redb_drain_up_to_streams_all() {
        // 流式分批读回（M5-3）：分 3 批读完 5 键, 无重复无遗漏, 尾批后返回空。
        let layout = sample_layout();
        let path = spill_test_path("redb_drain_stream");
        let mut s = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout))
            .expect("create");
        let mut entries = Vec::new();
        for i in 0..5i64 {
            let k = ScopeKey::Int(1000 + i);
            entries.push((spill_hash(&k), k, vec![StatsAccum::Last(None)]));
        }
        s.put_batch(entries).expect("put_batch");

        let b1 = s.drain_up_to(2);
        let b2 = s.drain_up_to(2);
        let b3 = s.drain_up_to(2);
        let b4 = s.drain_up_to(2);
        assert_eq!(b1.len(), 2);
        assert_eq!(b2.len(), 2);
        assert_eq!(b3.len(), 1, "尾批 1 键");
        assert!(b4.is_empty(), "读完后返回空");
        let mut keys: Vec<i64> = b1
            .into_iter()
            .chain(b2)
            .chain(b3)
            .map(|(k, _)| match k {
                ScopeKey::Int(v) => v,
                _ => panic!("期望 Int"),
            })
            .collect();
        keys.sort();
        assert_eq!(keys, vec![1000, 1001, 1002, 1003, 1004], "全部键恰好一次");
        s.cleanup();
    }
}
