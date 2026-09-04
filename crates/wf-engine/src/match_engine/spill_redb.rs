//! redb 持久化实现（2026-09-04 自 spill.rs 拆出；`#[path]` sibling 子模块；M2/M5/M6，
//! 见 `docs/design/stats-state-spill-redb.md` 与 `docs/design/async-persist.md`）：
//! [`RedbSpillStore`] 读侧 + 写侧异步后端 [`RedbBatchWriter`] / [`SpillItem`]。

use super::*;
use crate::match_engine::ScopeKey;
use crate::match_engine::async_persist::{AsyncPersister, BatchWriter};
use crate::match_engine::executor::{RowFieldLayout, StatsAccum};
use redb::{ReadableDatabase, ReadableTableMetadata};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

// ---------------------------------------------------------------------------
// redb 持久化实现（M2）
// ---------------------------------------------------------------------------

/// redb 表：`u64 hash → 序列化 spill 值`（`serialize_spill_value` 字节）。
/// 单键/单 hash（M1 trait 即单键语义）；两不同 ScopeKey 撞同一 u64 hash 的
/// 概率 ~2.2e-11（29M 键生日界），put 覆盖旧值——文档化限制（§10），
/// 不引入链式值（§7 的链为碰撞安全所设，实际概率可忽略）。
const REDB_TABLE: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("state");

/// redb 持久化实现。
///
/// - 文件按任务实例/分片隔离（`spill_{rule}_{pid}{_shard}.rb`，M3/M4 接线）；
///   同一实例的连续窗口复用同一路径（窗口串行, close 即 cleanup 删文件,
///   create 打开前删旧建新）——本结构只负责单库读写。
/// - `take` 只读（M5-2）：redb 旧条目由调用方 close 时按已读回集合过滤。
/// - `drain_up_to` 流式（M5-3）：游标续读，close 峰值 = 批大小而非全量。
/// - 写侧异步（M6，`docs/design/async-persist.md`）：驱逐写事务由后台 worker
///   消化（热路径只入队）；读侧（contains/take/drain_up_to）前先 flush 保证
///   "已提交 = 已落盘"。
/// - 读失败 = 致命：redb 错误在无 Result 通道的 trait 方法里直接 panic
///   （绝不静默丢键）；`put_batch` 保留 Result 供 M3 三层预算回退拒收。
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SpillStore")]
pub struct RedbSpillStore {
    /// 读侧数据库句柄（写侧 RedbBatchWriter 持 Arc clone 共用同一库）。
    db: Option<std::sync::Arc<redb::Database>>,
    /// 库文件路径（`cleanup` 删除用；含 redb 可能的 `.rbr` 侧车文件）。
    path: std::path::PathBuf,
    /// 读回时解释 RowFields 的 layout（executor 生命周期内不变）。
    layout: std::sync::Arc<RowFieldLayout>,
    /// drain_up_to 游标：下一批从 `Excluded(cursor)` 续读（None = 从头）。
    drain_cursor: Option<u64>,
    /// 异步写队列（M6）：本分片独立写 worker（每分片 = 每文件 = 单写者，驱逐
    /// 与写流水线自洽——实测最优；共享队列/全局单写者均负优化）。
    writer: Option<AsyncPersister<SpillItem, RedbBatchWriter>>,
    /// 写失败标记（writer error_cb 置位 → put_batch 拒收）。
    write_failed: std::sync::Arc<AtomicBool>,
}

impl RedbSpillStore {
    /// 打开/新建库（`Database::create` 语义：文件不存在则初始化，存在则打开）。
    /// 初始化时确保 `state` 表存在（读事务 `open_table` 要求表已存在）。
    /// 内部创建本分片的独立异步写队列（M6：每分片 = 每文件 = 单写者）。
    ///
    /// **页缓存设界**（2026-08-26 M5 实测）：redb 默认缓存 1GiB/库——q18 10 片
    /// 潜在 10GB 无谓 RSS。取 `WF_SPILL_CACHE_MB`（默认 64MB）。
    ///
    /// **无持久化语义**（设计 §8）：spill 是内存换磁盘的临时缓冲，崩溃即重
    /// ingest——正确性不依赖落盘。
    pub fn create(
        path: impl AsRef<std::path::Path>,
        layout: std::sync::Arc<RowFieldLayout>,
    ) -> Result<Self, SpillError> {
        let path = path.as_ref();
        // **打开前清空旧文件**（2026-08-27 review）：spill 无持久化语义（设计 §8）
        // ——库文件只服务当前窗口, close 后 cleanup 删除。若此处已存在文件, 只可
        // 能是 ① 上一窗口 cleanup rm 失败残留 ② 崩溃/pid 复用残留。直接打开会把
        // 旧窗口的键混进新窗口（close drain 遍历全表 → 旧窗键污染新窗输出）。
        // 删除失败 → 致命（绝不打开脏库; 调用方视为创建失败 panic）。
        for p in [path.to_path_buf(), path.with_extension("rbr")] {
            if p.exists() {
                std::fs::remove_file(&p).map_err(SpillError::Io)?;
            }
        }
        let cache_mb: usize = std::env::var("WF_SPILL_CACHE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64);
        let db = redb::Database::builder()
            .set_cache_size(cache_mb.saturating_mul(1024 * 1024))
            .create(path)
            .map_err(|e| SpillError::Redb(e.into()))?;
        let db = std::sync::Arc::new(db);
        let write_txn = db.begin_write().map_err(|e| SpillError::Redb(e.into()))?;
        {
            let _ = write_txn
                .open_table(REDB_TABLE)
                .map_err(|e| SpillError::Redb(e.into()))?;
        }
        write_txn.commit().map_err(|e| SpillError::Redb(e.into()))?;
        // 异步写队列（M6）：驱逐写移出热路径。每分片独立（单 worker）。
        let queue_bytes: usize = std::env::var("WF_SPILL_QUEUE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512 * 1024 * 1024);
        let batch_bytes: usize = std::env::var("WF_SPILL_BATCH_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64 * 1024 * 1024);
        let fsync_every: u64 = std::env::var("WF_SPILL_FSYNC_EVERY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        let write_failed = std::sync::Arc::new(AtomicBool::new(false));
        let error_cb: std::sync::Arc<dyn Fn(&str) + Send + Sync> = {
            let flag = std::sync::Arc::clone(&write_failed);
            std::sync::Arc::new(move |e: &str| {
                flag.store(true, AtomicOrdering::SeqCst);
                log::error!("spill 异步写失败(已驱逐键丢失, 后续驱逐拒收): {e}");
            })
        };
        let writer = AsyncPersister::new(
            vec![RedbBatchWriter::new(db.clone(), fsync_every)],
            queue_bytes,
            32,
            batch_bytes,
            Some(error_cb),
        );
        Ok(Self {
            db: Some(db),
            path: path.to_path_buf(),
            layout,
            drain_cursor: None,
            writer: Some(writer),
            write_failed,
        })
    }

    /// 写侧异步队列：等所有已提交驱逐批落盘（读回前调用——"已提交 = 已可见"）。
    fn flush_writer(&self) {
        if let Some(w) = &self.writer
            && let Err(e) = w.flush()
        {
            Self::redb_expect(&format!("spill flush 失败: {e}"));
        }
    }

    fn redb_expect(msg: &str) -> ! {
        panic!("spill redb 失败(致命): {msg}")
    }
}

impl Drop for RedbSpillStore {
    fn drop(&mut self) {
        // 停本分片写队列（join worker，排空剩余批）——Drop 时不留后台线程。
        if let Some(w) = self.writer.take() {
            w.shutdown();
        }
    }
}

/// 单个 spill 条目（异步写队列的数据单元）——本分片独立 worker，无需目标标识。
pub type SpillItem = (u64, ScopeKey, Vec<StatsAccum>);

/// 写侧后端：redb 单事务批量 insert（M6——由 AsyncPersister 后台 worker 调用）。
/// 每分片一个实例（每分片 = 每文件 = 单写者）。
///
/// durability 策略与 M5-1 一致：默认 `None`（无 fsync，close 期 drain 读内存页
/// 更快），每 `fsync_every` 批一次 `Immediate` 周期 flush（redb 连带持久化之前
/// 所有 None 提交——脏页有界）。
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SpillStore")]
pub struct RedbBatchWriter {
    db: std::sync::Arc<redb::Database>,
    put_batches: u64,
    /// 每 N 批一次 `Immediate`（fsync）周期 flush（M5-1：限脏页有界）。
    /// `WF_SPILL_FSYNC_EVERY` 可调（A/B 实验已排除 fsync 频率为主瓶颈）。
    fsync_every: u64,
}

impl RedbBatchWriter {
    pub fn new(db: std::sync::Arc<redb::Database>, fsync_every: u64) -> Self {
        Self {
            db,
            put_batches: 0,
            fsync_every,
        }
    }
}

impl BatchWriter<SpillItem> for RedbBatchWriter {
    fn write_batch(&mut self, items: Vec<SpillItem>) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        // 批内按 hash 排序（2026-08-27 bench 实测 1.4-2.4x）：B+树随机插入
        // 页分裂多/缓存命中差；排序后近似顺序写，逼近连续 key 理论上限
        // （见 tests/spill_write_bench.rs）。排序成本 ~50ms/24.8 万键，远小于
        // 写耗时。
        let mut items = items;
        items.sort_by_key(|(h, _, _)| *h);
        let t1 = std::time::Instant::now();
        self.put_batches += 1;
        let durability = if self.put_batches.is_multiple_of(self.fsync_every) {
            redb::Durability::Immediate
        } else {
            redb::Durability::None
        };
        let mut txn = self
            .db
            .begin_write()
            .map_err(|e| format!("begin_write: {e}"))?;
        txn.set_durability(durability)
            .map_err(|e| format!("set_durability: {e}"))?;
        let mut serialize_ns = 0u64;
        let mut insert_ns = 0u64;
        {
            let mut table = txn
                .open_table(REDB_TABLE)
                .map_err(|e| format!("open_table: {e}"))?;
            for (hash, key, accs) in items {
                let s0 = std::time::Instant::now();
                let bytes = serialize_spill_value(&key, &accs).map_err(|e| e.to_string())?;
                serialize_ns += s0.elapsed().as_nanos() as u64;
                let i0 = std::time::Instant::now();
                table
                    .insert(hash, bytes.as_slice())
                    .map_err(|e| format!("insert: {e}"))?;
                insert_ns += i0.elapsed().as_nanos() as u64;
            }
        }
        let c0 = std::time::Instant::now();
        txn.commit().map_err(|e| format!("commit: {e}"))?;
        let commit_ns = c0.elapsed().as_nanos() as u64;
        if std::env::var("WF_SPILL_PROFILE").is_ok() {
            log::info!(
                "spill worker 批: 总 {:>8.1}ms | sort {:>6.1}ms | 序列化 {:>6.1}ms | insert {:>6.1}ms | commit {:>6.1}ms",
                t0.elapsed().as_secs_f64() * 1e3,
                t1.duration_since(t0).as_secs_f64() * 1e3,
                serialize_ns as f64 / 1e6,
                insert_ns as f64 / 1e6,
                commit_ns as f64 / 1e6,
            );
        }
        Ok(())
    }
}

impl SpillStore for RedbSpillStore {
    fn contains(&self, hash: u64) -> bool {
        // 异步写：读前 flush，保证已提交驱逐可见（测试/诊断用；热路径查内存索引）。
        self.flush_writer();
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
        // 写失败标记 → 拒收（防继续丢键）。
        if self.write_failed.load(AtomicOrdering::SeqCst) {
            return Err(SpillError::Closed);
        }
        let Some(writer) = self.writer.as_ref() else {
            return Err(SpillError::Closed);
        };
        // 入队（M6 异步写）：热路径不阻塞 redb 事务；队列满/超预算时阻塞
        // （背压 = 内存有界的代价）。est 按每项保守 1KB 估算；单 worker 队列
        // route 恒 0。
        let est = entries.len().saturating_mul(1024);
        writer
            .submit_batch(0, entries, est)
            .map_err(|_| SpillError::Closed)
    }

    /// 只读读回（M5-2）：`begin_read + get`——无写事务/WAL/锁争用（读事务并发）。
    /// **不删除条目**：redb 中旧条目由调用方 close 时按「已读回集合」过滤。
    /// 读前 flush 异步队列（M6）——保证刚驱逐未落盘的键可见。
    fn take(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)> {
        self.flush_writer();
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
    /// 不删除条目（close 后 cleanup 删整个文件）。读前 flush 异步队列（M6）。
    fn drain_up_to(&mut self, n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        if n == 0 {
            return Vec::new();
        }
        self.flush_writer();
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
        // 停本分片写队列（排空剩余批）→ 关闭数据库（释放文件句柄）→ 删除库
        // 文件与可能的侧车 WAL。
        if let Some(w) = self.writer.take() {
            w.shutdown();
        }
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
        // 读前 flush 异步队列（M6）——与 contains/take/drain_up_to 一致：
        // 刚驱逐未落盘的键必须可见（否则 close 前 `is_empty` 早退会丢 spill 键）。
        self.flush_writer();
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
