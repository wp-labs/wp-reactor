mod cursor;
mod eviction;
mod types;
mod watermark;

#[cfg(test)]
mod tests;

pub use types::{AppendOutcome, WindowParams};

use std::collections::{BTreeMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// join_index 用 parking_lot::RwLock（读优先）——join 目标是读多写少热点
// （q4a 33M 到期读 vs 900 batch 写），std RwLock 写者优先策略让读者在
// 写者排队时被阻塞（100M q4 卡 28× 的锁竞争根因，2026-08-25）。
// 其余窗口锁保持 std（log 写多、progress/parked_pin 低频）。
use parking_lot::RwLock as PLRwLock;

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use smol_str::SmolStr;
use wf_config::WindowConfig;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::{
    AsofLookup, Event, JoinKey, JoinRow, Value, batch_raw_ts_nanos, build_field_index,
};
use crate::window::WindowProgress;

use types::TimedBatch;

/// Hash index for join lookups: maps a scalar key value to columnar row
/// locators (`IndexedRow`). Maintained incrementally on append/evict/expire,
/// with **no per-row `Event` materialization** — the index holds `(batch, row)`
/// and reads fields on demand, so join-target windows stay columnar. Only
/// present on windows configured as join targets (`set_join_key`).
///
/// **Sharded**: `shards.len()` (power of two) independent read-preferring
/// `RwLock`s, one hash map each. q4 100M 断崖根因（2026-08-25）：单锁索引在
/// 写者（bid append 的 `index_batch`，每 batch ~36.5k 行哈希插入，单批持锁
/// ~5ms）持锁期间阻塞**全部**读者（deferred 到期查找）——30s 采样 21969/21969
/// 全在 `lock_shared_slow`，EPS 7.6M→0.27M（28×）。分片后：
///   - 查找只锁 key 所在一片（`shard_of & mask`）；
///   - 写者先按片分组、再逐片短暂持写锁（每片临界区 36.5k/64 ≈ 570 行）。
///
/// deferred_bench `index_contention` 实测：写者活跃时读者吞吐 0.15M→6.9M
/// ops/s（43–46×，达无写者天花板的 ~86%）。
pub(super) struct JoinIndex {
    key_field: SmolStr,
    /// The window's `materialize_fields` projection: enrich reads only these
    /// columns from the joined rows. `None` = all columns.
    projection: Option<Arc<HashSet<String>>>,
    /// Columnar row locators per key, split across `shards.len()` maps. Each
    /// [`KeyedRows`] keeps a running `max_ts` so the asof fast path can answer
    /// the common "latest row ≤ event_time" case in O(1) without scanning every
    /// candidate.
    shards: Vec<PLRwLock<crate::match_engine::EngineHashMap<JoinKey, KeyedRows>>>,
    /// `shards.len() - 1`（2 的幂，选片用 `hash & mask`）。
    mask: usize,
    /// 每 batch 索引过的去重 key 集（`seq → keys`），供**增量驱逐**：驱逐一个
    /// batch 只动它贡献过的 key，替代旧的整表扫描。q4 100M 断崖主因
    /// （2026-08-25）：事件跨度 = count×100µs → 100M 跨度 2h46m > over=1h，
    /// time 驱逐每 tick 弹掉一批 bid，旧 `remove_batch` 每批 O(全索引 33M 行)
    /// （retain + max_ts 重算）→ evictor 线程独占一核、EPS 0.27M。registry 只
    /// 保留**在窗** batch 的键（被驱逐 batch 的条目即删），内存 ≈ 窗内去重键
    /// 数（100M 2GB cap ≈ 1.7M 键 × ~24B ≈ 40MB）。
    batch_keys: PLRwLock<crate::match_engine::EngineHashMap<u64, Vec<JoinKey>>>,
}

/// 分片数：2 的幂。64 片时每片持锁临界区 ≈ 570 行/批，竞争摊薄 64×。
const JOIN_INDEX_SHARDS: usize = 64;

/// The indexed rows for one join key, plus the maximum raw timestamp among them
/// (`None` when the key has no timestamped rows).
#[derive(Default)]
struct KeyedRows {
    rows: Vec<IndexedRow>,
    max_ts: Option<i64>,
}

/// A columnar row locator: `(batch, row)` plus the batch-level field index and
/// the row's raw timestamp. The join index holds these instead of materialized
/// `Event`s.
struct IndexedRow {
    ts_nanos: Option<i64>,
    batch: Arc<RecordBatch>,
    row: usize,
    index: Arc<crate::match_engine::FieldIndex>,
    /// Batch seq — M2 seq-cut: pull-mode lookups under a `max_seq` watermark
    /// must not see rows from batches the reader has not pulled yet (2026-08:
    /// 此前索引无 seq 感知，pull 模式被迫回退全量扫描 → q13 等 join 查询
    /// CPU/积压瓶颈）。
    seq: u64,
}

impl JoinIndex {
    /// 选片：`DefaultHasher`（固定密钥，进程内确定性）对 `JoinKey` 散列后取
    /// 低 `mask` 位。与分片内 map 自身的 foldhash 无关——只要求同一 key 恒落
    /// 同一片。
    fn shard_of(key: &JoinKey, mask: usize) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) & mask
    }

    /// Index every row of `batch` by its `key_field` value. Reads the key column
    /// straight from the Arrow batch through the same [`extract_field_value`]
    /// conversion the eager `Event` path uses, so the produced keys are
    /// byte-identical to the previous materialized-index behavior.
    ///
    /// Sharded：先一遍提取 key + 选片（不持任何锁），再逐片短暂持写锁插入——
    /// 每片临界区只有该片分到的行（≈ 36.5k/64），读者最多等自己那片（q4
    /// 100M 单锁阻塞全读者的根因修复）。同时把本批的去重 key 集注册进
    /// `batch_keys`（增量驱逐用，见 struct 注释）。
    fn index_batch(&self, batch: &Arc<RecordBatch>, ts_list: &[Option<i64>], seq: u64) {
        let Ok(col_idx) = batch.schema().index_of(self.key_field.as_str()) else {
            return;
        };
        let schema = batch.schema();
        let field = schema.field(col_idx);
        let col = batch.column(col_idx);
        let index = build_field_index(batch);
        let mut buckets: Vec<Vec<(JoinKey, usize, Option<i64>)>> =
            (0..self.shards.len()).map(|_| Vec::new()).collect();
        for (row, ts) in ts_list.iter().enumerate() {
            if col.is_null(row) {
                continue;
            }
            let Some(value) = extract_field_value(field, col.as_ref(), row) else {
                continue;
            };
            let Some(key) = JoinKey::from_value(&value) else {
                continue;
            };
            let shard = Self::shard_of(&key, self.mask);
            buckets[shard].push((key, row, *ts));
        }
        // 去重本批 key 集（驱逐时按 key 增量清理，需与行去重一致）。Int 键
        // clone 是 memcpy；Str 键只有首次出现才 clone（set 命中后跳过）。
        // 用 foldhash（EngineHashSet）而非 std SipHash——append 热路径每批
        // ~36.5k 行都过这个 set，SipHash 实测把 index_batch 拉到 ~117ns/row
        //（join_index_append_bench），foldhash 约省 2/3。
        let mut batch_key_set: crate::match_engine::EngineHashSet<JoinKey> =
            crate::match_engine::EngineHashSet::default();
        for (shard, rows) in buckets.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let mut map = self.shards[shard].write();
            for (key, row, ts) in rows {
                if !batch_key_set.contains(&key) {
                    batch_key_set.insert(key.clone());
                }
                let kr = map.entry(key).or_default();
                kr.rows.push(IndexedRow {
                    ts_nanos: ts,
                    batch: Arc::clone(batch),
                    row,
                    index: Arc::clone(&index),
                    seq,
                });
                if let Some(t) = ts {
                    kr.max_ts = Some(kr.max_ts.map_or(t, |m| m.max(t)));
                }
            }
        }
        // 空 key 集也注册（空条目）：驱逐时能区分「本批没索引任何行」和
        // 「registry 缺失」，避免空批误触发昂贵的全量回退扫描。
        self.batch_keys
            .write()
            .insert(seq, batch_key_set.into_iter().collect());
    }

    /// Remove every row belonging to batch `seq`, then recompute the per-key
    /// `max_ts` (a removal may have dropped the max).
    ///
    /// **增量**：只动 `batch_keys[seq]` 里的 key（O(受影响 key × 行数)），替代
    /// 旧的整表扫描（O(全索引)——q4 100M 驱逐每 tick 33M 行）。registry 缺失
    /// （防御：set_join_key 前的旧路径等）时回退全量扫描，正确性不变。
    fn remove_batch(&self, seq: u64) {
        let keys: Option<Vec<JoinKey>> = self.batch_keys.write().remove(&seq);
        let Some(keys) = keys else {
            // 回退：全量扫描（防御——batch 未注册：set_join_key 前的旧 batch、
            // key 列缺失被 index_batch 早退的 batch）。按 seq 匹配与旧实现按
            // Arc 指针匹配等价（batch↔seq 一一对应）。
            for shard in &self.shards {
                let mut map = shard.write();
                for kr in map.values_mut() {
                    kr.rows.retain(|r| r.seq != seq);
                    kr.max_ts = kr.rows.iter().filter_map(|r| r.ts_nanos).max();
                }
                map.retain(|_, kr| !kr.rows.is_empty());
            }
            return;
        };
        for key in keys {
            let shard = Self::shard_of(&key, self.mask);
            let mut map = self.shards[shard].write();
            let Some(kr) = map.get_mut(&key) else {
                continue;
            };
            let before = kr.rows.len();
            kr.rows.retain(|r| r.seq != seq);
            if kr.rows.len() != before {
                kr.max_ts = kr.rows.iter().filter_map(|r| r.ts_nanos).max();
            }
            if kr.rows.is_empty() {
                map.remove(&key);
            }
        }
    }

    /// Snapshot-join view: every indexed row for `key` with `seq <= max_seq`
    /// (`None` = all rows), as columnar [`JoinRow`]s. The seq cut is the M2
    /// pull-mode consistency boundary: a reader processing batch N must only
    /// see rows from batches it has pulled (`seq <= N`), never rows the actor
    /// appended past it. 只锁 key 所在片。
    fn lookup(&self, key: &JoinKey, max_seq: Option<u64>) -> Option<Vec<JoinRow>> {
        let map = self.shards[Self::shard_of(key, self.mask)].read();
        map.get(key).map(|kr| {
            kr.rows
                .iter()
                .filter(|r| max_seq.is_none_or(|m| r.seq <= m))
                .map(|r| self.row_to_join_row(r))
                .collect()
        })
    }

    /// Asof-join view: only the timestamped rows for `key` with `seq <= max_seq`.
    fn lookup_timestamped(
        &self,
        key: &JoinKey,
        max_seq: Option<u64>,
    ) -> Option<Vec<(i64, JoinRow)>> {
        let map = self.shards[Self::shard_of(key, self.mask)].read();
        map.get(key).map(|kr| {
            kr.rows
                .iter()
                .filter(|r| max_seq.is_none_or(|m| r.seq <= m))
                .filter_map(|r| r.ts_nanos.map(|ts| (ts, self.row_to_join_row(r))))
                .collect()
        })
    }

    /// Asof lookup: return `key`'s row whose raw timestamp is the greatest
    /// within `[min_ts, event_time]`.
    ///
    /// Fast path — when the key's running `max_ts` already falls inside the
    /// window, that row is the answer and is returned in O(1) (the reverse scan
    /// is guaranteed to find it because `max_ts` comes from `kr.rows`).
    ///
    /// `Miss` when the key is absent, has no timestamped rows, or its max
    /// timestamp is already older than `min_ts` (so no row can qualify).
    ///
    /// When `max_ts > event_time` a smaller row may still qualify, so we do a
    /// single linear scan over `kr.rows` — comparing raw timestamps only — and
    /// materialize just the winning row. This keeps the common asof case on the
    /// index (no `Vec` allocation, no per-row `JoinRow` clone, no redundant
    /// condition re-check), unlike the caller's `asof_candidates` + scan
    /// fallback which is only needed for multi-condition joins and watermarked
    /// reads. `Fallback` remains only for the defensive miss of the fast-path
    /// reverse scan.
    fn lookup_asof_max(
        &self,
        key: &JoinKey,
        event_time: i64,
        min_ts: i64,
        max_seq: Option<u64>,
    ) -> AsofLookup {
        let map = self.shards[Self::shard_of(key, self.mask)].read();
        let Some(kr) = map.get(key) else {
            return AsofLookup::Miss;
        };
        // seq-cut 下 max_ts 缓存可能来自未拉取 batch：`max_seq` 为 Some 时按
        // 过滤后的行重新取最大 ts（域小 + asof 场景少，可接受）；None 用缓存。
        let max_ts = match max_seq {
            None => kr.max_ts,
            Some(m) => kr
                .rows
                .iter()
                .filter(|r| r.seq <= m)
                .filter_map(|r| r.ts_nanos)
                .max(),
        };
        let Some(max_ts) = max_ts else {
            return AsofLookup::Miss;
        };
        if max_ts < min_ts {
            return AsofLookup::Miss;
        }
        let candidates: Vec<&IndexedRow> = kr
            .rows
            .iter()
            .filter(|r| max_seq.is_none_or(|m| r.seq <= m))
            .collect();
        if max_ts <= event_time {
            // min_ts <= max_ts <= event_time: `max_ts` comes from the filtered
            // rows, so the reverse scan is guaranteed to find it.
            let Some(r) = candidates.iter().rev().find(|r| r.ts_nanos == Some(max_ts)) else {
                return AsofLookup::Fallback;
            };
            return AsofLookup::Hit(self.row_to_join_row(r));
        }
        // max_ts > event_time: find the greatest timestamp in [min_ts, event_time].
        let mut best: Option<&IndexedRow> = None;
        let mut best_ts = i64::MIN;
        for r in candidates {
            let Some(ts) = r.ts_nanos else {
                continue;
            };
            if ts <= event_time && ts >= min_ts && ts > best_ts {
                best_ts = ts;
                best = Some(r);
            }
        }
        match best {
            Some(r) => AsofLookup::Hit(self.row_to_join_row(r)),
            None => AsofLookup::Miss,
        }
    }

    fn row_to_join_row(&self, r: &IndexedRow) -> JoinRow {
        JoinRow::Columnar {
            batch: Arc::clone(&r.batch),
            row: r.row,
            index: Arc::clone(&r.index),
            projection: self.projection.clone(),
        }
    }
}

/// A time-ordered buffer of Arrow RecordBatches with eviction support.
///
/// Batches are appended by sequence number and evicted from the front, either
/// by time expiry or memory pressure. The ordered log is a
/// `RwLock<BTreeMap<u64, TimedBatch>>`:
///
/// * **Writers** (the window actor's append path, the periodic evictor, and
///   the inline commit path used by file sources / tests) take the write
///   lock. Removal from a `BTreeMap` drops the `TimedBatch` — and its Arrow
///   batch plus pre-parsed events — **eagerly**, unlike the lock-free
///   `crossbeam-skiplist` log this replaced, whose `remove` only unlinked
///   the node and deferred the value's destructor into crossbeam-epoch
///   garbage bags (a quiet system never advanced the epoch, so ~6M evicted
///   events stayed resident — the 2026-08-16 RSS regression).
/// * **Readers** (cursor-based `events_since`/`read_since`, `snapshot`,
///   join-index setup) take the read lock and clone `Arc` handles out; the
///   lock is held only for the clone, never for downstream processing.
/// * In the production wiring (push-mode rules) no reader touches the log on
///   the hot path at all — the window actor broadcasts parsed events through
///   rule channels — so the write lock is effectively uncontended.
///
/// Lock ordering: a path may hold the log lock and then take `join_index`;
/// the reverse order never occurs (`set_join_key` releases the log lock
/// before indexing into `join_index`).
///
/// The optional join index (only present on windows configured as join
/// targets via [`Self::set_join_key`]) is a hash map that needs interior
/// mutability on both insert and eviction, so it keeps a dedicated fine-grained
/// lock behind an `AtomicBool` fast path — windows that are not join targets
/// (the common case) never touch it.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct Window {
    pub(super) name: String,
    pub(super) schema: SchemaRef,
    pub(super) time_col_index: Option<usize>,
    pub(super) over: Duration,
    pub(super) config: WindowConfig,
    /// Time-ordered append log: batch sequence number → batch. Guarded by an
    /// `RwLock` — see the struct docs for the concurrency contract. Removal
    /// drops the value eagerly (no deferred reclamation).
    ///
    /// ⚠ 已知限制（2026-08-25 review 记录，预先存在）：seq 是**提交顺序**，
    /// 跨 source 乱序提交（ingress instances>1 + parse 并行）下 ≠ 事件时间序。
    /// 时间驱逐（`evict_expired_impl`）按 seq 从最旧提交弹栈——若最旧提交的
    /// 事件时间很新（某 source 的远未来 batch 先落地），弹栈会卡住、后面
    /// seq 更大但事件时间更老的 batch 不被驱逐 → 内存次优（正确性无损：
    /// 多保留）。单源（conns=1）帧有序，无此问题。修复需改驱逐为按事件时间
    /// 扫描（破坏 O(1) 弹栈 + BTreeMap 序），暂不做。
    log: RwLock<BTreeMap<u64, TimedBatch>>,
    /// Next sequence number to assign to an appended batch.
    next_seq: AtomicU64,
    /// Monotonic event-time watermark (`fetch_max` on append).
    watermark_nanos: AtomicI64,
    /// Monotonic raw max event time seen on append (`fetch_max`, **before** the
    /// watermark delay is subtracted). The rule task uses it at flush to know the
    /// global data tail across shards: a shard's state-machine watermark stops at
    /// its last processed row, which can lag the true end-of-data (q11 10M:
    /// 10 shards each stop ~1.8-4.3ms early → a tail session with
    /// `last_event+gap ≤ global end` was misjudged incomplete and dropped).
    max_event_time_nanos: AtomicI64,
    /// 2026-08-25（跨源提交乱序修复）：按 source 记录的**已提交**最大事件时间。
    /// 窗口 actor 对每个 source 按 seq 顺序提交（跨 source 提交顺序自由）——
    /// 全局 `max_event_time_nanos` 可能被任一 source 的晚 batch 提前推高，而
    /// 其它 source 的早期行还没落地（ingress instances=8 + parse 并行派发）。
    /// deferred 评估 gate 若用全局 max，会在右行未提交时提前评估 → 假 miss
    /// （30M q4 over=30m -860 的根因，2026-08-25 实测）。`committed_frontier_ns`
    /// = 各 source 已提交 max 的 **min**——所有 source 的行都提交到该水位，
    /// 才是健全的"右行完整性"判据。非 actor 路径（单测/内嵌，source=None）
    /// 不记录 → 回退全局 max（旧行为）。
    per_source_max_event_time: std::sync::Mutex<std::collections::HashMap<Arc<str>, i64>>,
    /// Aggregate retained content bytes (approximate under concurrency —
    /// exact in the single-writer steady state).
    current_bytes: AtomicUsize,
    /// 存活批次的**实际分配** Arrow 缓冲字节（`get_array_memory_size` 求和）。
    ///
    /// 2026-08-25（会计保真度）：`current_bytes` 只计**逻辑列内容**
    /// （`content_bytes`），不含 null bitmap / offsets；本口径按缓冲去重后累加
    /// **实际引用长度**，补齐这两项。
    ///
    /// 生产实测（q13 30M）：两者**基本相等（1.00×）**——窗口主体是 IPC 零拷贝
    /// 切片，其 bitmap/offsets 占比很小。所以本口径的价值是**证明 content_bytes
    /// 没有系统性低估**（此前曾据单批测量误判为低估 1.75×，见 issue 文档），
    /// 而非揭示新增内存。
    ///
    /// **不计入**：分配器容量宽余（builder 倍增长留在 buffer 尾部的未引用容量，
    /// 单批实测 content 3.45MB → 存活 6.29MB）。那部分属于分配器行为、无法归属
    /// 单个批次；预置容量的修法已被实测否决（峰值 +67%，见 `take_batch` 注释）。
    ///
    /// **专用于可观测性**：驱逐预算（`max_window_bytes`）与 mailbox 预算仍用
    /// `content_bytes` 口径不变——改它会改变所有已调优的容量语义。
    current_alloc_bytes: AtomicUsize,
    /// Aggregate row count (approximate under concurrency).
    total_rows: AtomicUsize,
    /// Number of batches currently in the log.
    batch_count: AtomicUsize,
    /// Monotonic content-generation counter: bumped once per successful append
    /// (which subsumes any accompanying memory eviction — the only other log
    /// mutation). `window.has()` / join snapshot caches key off this to
    /// invalidate stale distinct-value sets without a per-call scan.
    generation: AtomicU64,
    /// Fast path: whether a join index has been configured. Non-join windows
    /// (the common case) skip the join-index lock entirely.
    join_enabled: AtomicBool,
    /// Optional hash index for join lookups (see `set_join_key`). Only
    /// mutated while `join_enabled` is true.
    /// 2026-08-25 q4 100M 分片后：外层锁仅保护 `Option` 配置（`set_join_key`
    /// 一次性写、后续不变），稳态读写都走读锁；真正的并发是 `JoinIndex` 内部
    /// 的 64 片独立 RwLock（`index_batch` 逐片短暂持写锁，查找只锁一片）。
    join_index: PLRwLock<Option<JoinIndex>>,
    /// Optional per-event field whitelist (see `WindowParams`). Immutable
    /// after construction — readers (`Router::route_parse`) access it with no
    /// synchronization at all.
    pub(super) materialize_fields: Option<Arc<HashSet<String>>>,
    /// L2 deferred materialization (see `WindowParams`). Immutable after
    /// construction.
    pub(super) defer_materialization: bool,
    /// Consumption progress (ack floor) for this window, injected by the
    /// registry. Per-window memory eviction respects this floor so a slow
    /// pull consumer never loses unread batches (`None` until the registry
    /// wires it — treated as "no consumers", i.e. everything evictable).
    progress: RwLock<Option<Arc<WindowProgress>>>,
    /// D4: retention pin pre-registered **synchronously** at spawn time for a
    /// deferred join target, parked here until the (async) rule task takes
    /// ownership via [`Self::take_retention_pin`].
    ///
    /// Why park it instead of letting the task register its own: rule tasks are
    /// `tokio::spawn`ed and construct themselves inside the spawned future,
    /// while ingestion starts as soon as `spawn_rule_tasks` returns. A pin
    /// created in the future therefore races with the first appends — nexmark q4
    /// lost 0–6% of its output nondeterministically (5 vs 48 startup eviction
    /// sweeps) until the pin existed before the first batch landed.
    /// parked_pin: RwLock<Option<Arc<std::sync::atomic::AtomicI64>>>,
    parked_pin: RwLock<Option<Arc<std::sync::atomic::AtomicI64>>>,
    /// Shutdown drain state of this window's single-writer actor.
    /// `true` = no actor (or its actor finished committing the queued tail);
    /// `false` = an actor is live and may still append. Defaults to `true`
    /// so windows without an actor (provider / embedded direct-append) are
    /// trivially "drained". The actor flips it to `false` at start and back
    /// to `true` after its shutdown drain. Rule tasks wait on it at full
    /// shutdown so the final flush runs against a complete machine — the
    /// window-tail commit vs rule-flush ordering race (e2e_datagen_brute_force
    /// CI flake: close_all at a stale machine watermark).
    actor_drained: AtomicBool,
}

impl Window {
    /// Create a new empty window.
    pub fn new(params: WindowParams, config: WindowConfig) -> Self {
        let materialize_fields = params.materialize_fields.clone();
        let defer_materialization = params.defer_materialization;
        Self {
            name: params.name,
            schema: params.schema,
            time_col_index: params.time_col_index,
            over: params.over,
            config,
            log: RwLock::new(BTreeMap::new()),
            next_seq: AtomicU64::new(0),
            watermark_nanos: AtomicI64::new(i64::MIN),
            max_event_time_nanos: AtomicI64::new(i64::MIN),
            per_source_max_event_time: std::sync::Mutex::new(std::collections::HashMap::new()),
            current_bytes: AtomicUsize::new(0),
            current_alloc_bytes: AtomicUsize::new(0),
            total_rows: AtomicUsize::new(0),
            batch_count: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            join_enabled: AtomicBool::new(false),
            join_index: PLRwLock::new(None),
            materialize_fields,
            defer_materialization,
            progress: RwLock::new(None),
            parked_pin: RwLock::new(None),
            actor_drained: AtomicBool::new(true),
        }
    }

    /// Mark whether this window's single-writer actor has finished its
    /// shutdown drain (see the field docs).
    pub fn set_actor_drained(&self, drained: bool) {
        self.actor_drained.store(drained, Ordering::Release);
    }

    /// Whether this window's actor has finished committing its queued tail
    /// (or never existed). `true` ⇒ no further appends will arrive.
    pub fn actor_drained(&self) -> bool {
        self.actor_drained.load(Ordering::Acquire)
    }

    /// Wire this window to its consumption-progress table. Called once by the
    /// registry right after construction, so per-window memory eviction can
    /// respect the ack floor (see [`Self::min_acked`]).
    pub(crate) fn set_progress(&self, progress: Arc<WindowProgress>) {
        *self.progress.write().expect("progress lock poisoned") = Some(progress);
    }

    /// Consumption floor for this window: the lowest acked `seq + 1` across
    /// all live consumers, or `u64::MAX` when there are none (everything is
    /// evictable). Per-window memory eviction uses this to avoid dropping a
    /// batch a slow pull rule has not yet read.
    fn min_acked(&self) -> u64 {
        self.progress
            .read()
            .expect("progress lock poisoned")
            .as_ref()
            .map(|p| p.min_acked())
            .unwrap_or(u64::MAX)
    }

    /// Retention frontier for this window (D4): the oldest event time any live
    /// join-target reader still needs, or `i64::MAX` when nothing is pinned.
    ///
    /// Memory eviction (per-window byte cap here, global cap via
    /// [`Self::evict_oldest_acked`]) must not drop a batch that may hold rows at
    /// or after this frontier — a join-target reader owns no consumer slot, so
    /// [`Self::min_acked`] cannot protect it. See [`WindowProgress`] for why the
    /// pin is event-time based and why `over` eviction ignores it.
    pub fn retention_floor_ns(&self) -> i64 {
        self.progress
            .read()
            .expect("progress lock poisoned")
            .as_ref()
            .map(|p| p.min_retention_ns())
            .unwrap_or(i64::MAX)
    }

    /// Register a retention pin on this window (D4), or `None` when the window
    /// is not wired to a progress table (unit-test windows).
    ///
    /// Called by a rule task that uses this window as a **join target**: it
    /// publishes the oldest event time its pending evaluations can still need,
    /// and memory eviction then refuses to drop those rows.
    pub fn register_retention_pin(&self) -> Option<Arc<std::sync::atomic::AtomicI64>> {
        self.progress
            .read()
            .expect("progress lock poisoned")
            .as_ref()
            .map(|p| p.register_retention_pin())
    }

    /// Pre-register a retention pin at spawn time and park it (see
    /// [`Self::parked_pin`]). Idempotent — a second call while a pin is parked is
    /// a no-op, so re-declaring the same join target does not stack pins.
    ///
    /// The pin starts fully pinned (`i64::MIN`): from the very first append the
    /// window keeps everything until the rule task publishes a real frontier.
    pub fn preregister_retention_pin(&self) {
        let mut parked = self.parked_pin.write().expect("parked pin lock poisoned");
        if parked.is_none() {
            *parked = self.register_retention_pin();
        }
    }

    /// Take ownership of the parked pin, or register a fresh one when none was
    /// pre-registered (direct construction in tests, extra shards of a sharded
    /// rule). The caller must keep the returned handle alive for as long as it
    /// needs the rows, and publish `i64::MAX` when done.
    pub fn take_retention_pin(&self) -> Option<Arc<std::sync::atomic::AtomicI64>> {
        if let Some(pin) = self
            .parked_pin
            .write()
            .expect("parked pin lock poisoned")
            .take()
        {
            return Some(pin);
        }
        self.register_retention_pin()
    }

    /// Configure this window as a join target: build a hash index on `key_field`
    /// and index any rows already buffered. Called by the runtime after rules
    /// are loaded (join target windows are only known from rule plans).
    /// Idempotent: a second call with the same/different key is a no-op (the
    /// first join condition's right field wins — consistent with
    /// `first_join_key`).
    pub fn set_join_key(&self, key_field: String) {
        if self.join_enabled.load(Ordering::Acquire) {
            return; // 已配置（首个 join 条件的右字段），幂等
        }
        let key_field = SmolStr::new(&key_field);
        let index = JoinIndex {
            key_field,
            projection: self.materialize_fields.clone(),
            shards: (0..JOIN_INDEX_SHARDS)
                .map(|_| PLRwLock::new(crate::match_engine::EngineHashMap::default()))
                .collect(),
            mask: JOIN_INDEX_SHARDS - 1,
            batch_keys: PLRwLock::new(crate::match_engine::EngineHashMap::default()),
        };
        // Read the log under its read lock; the guard is released before the
        // join-index write lock is taken (lock ordering: log → join_index,
        // never the reverse). The index holds columnar row locators — no
        // per-row `Event` materialization.
        let existing: Vec<(Arc<RecordBatch>, Vec<Option<i64>>, u64)> = {
            let log = self.log.read().expect("window log lock poisoned");
            log.values()
                .map(|tb| (Arc::clone(&tb.batch), self.raw_ts_list(tb), tb.seq))
                .collect()
        };
        for (batch, ts_list, seq) in &existing {
            index.index_batch(batch, ts_list, *seq);
        }
        self.join_enabled.store(true, Ordering::Release);
        *self.join_index.write() = Some(index);
    }

    /// Join index 的 key 字段（无索引 → None）。调用方（RegistryLookup）用它
    /// 校验「请求的 key_field == 索引 key_field」：索引只按**首个注册** join 的
    /// 右字段建（`set_join_key` 幂等），多个规则以不同 key join 同一窗口时
    /// （q8 按 seller / q20·q6 按 id），错字段的索引查询返回「索引命中但空」的
    /// 假空（`join_lookup` 返回 Some(空)，调用方不会回退扫描）→ 静默全 miss
    /// （q8 多规则 7565→1 根因，2026-08-29）。
    pub fn join_key_field(&self) -> Option<SmolStr> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        self.join_index
            .read()
            .as_ref()
            .map(|index| index.key_field.clone())
    }

    /// O(1) lookup of rows whose `key_field` equals `key`, as columnar
    /// [`JoinRow`]s. `Some(empty)` if this window is indexed but the key has no
    /// matching rows; `None` if it has no join index (not a join target — the
    /// caller falls back to a snapshot scan).
    ///
    /// `max_seq`（M2 pull 一致性）: 只返回 `seq <= max_seq` 的行（读者只能看到
    /// 自己已拉取的 batch）; `None` = 全量（push 模式）。
    pub fn join_lookup(&self, key: &JoinKey, max_seq: Option<u64>) -> Option<Vec<JoinRow>> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.join_index
                .read()
                .as_ref()?
                .lookup(key, max_seq)
                .unwrap_or_default(),
        )
    }

    /// O(1) timestamped lookup for the asof-join path: rows whose `key_field`
    /// equals `key`, as `(raw_ts_nanos, JoinRow)` — rows without a
    /// `Timestamp(Ns)` time value are skipped. `Some(empty)` when indexed but
    /// the key has no timestamped rows; `None` when there is no join index
    /// (caller falls back to a timestamped snapshot scan).
    pub fn join_lookup_timestamped(
        &self,
        key: &JoinKey,
        max_seq: Option<u64>,
    ) -> Option<Vec<(i64, JoinRow)>> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.join_index
                .read()
                .as_ref()?
                .lookup_timestamped(key, max_seq)
                .unwrap_or_default(),
        )
    }

    /// Asof fast path: return `key`'s row whose timestamp is the maximum
    /// `<= event_time` and `>= min_ts`, using the index's per-key `max_ts` —
    /// O(1), no candidate scan. See [`AsofLookup`] for the three outcomes.
    /// [`AsofLookup::Fallback`] when the window has no join index.
    pub fn join_lookup_asof(
        &self,
        key: &JoinKey,
        event_time: i64,
        min_ts: i64,
        max_seq: Option<u64>,
    ) -> AsofLookup {
        if !self.join_enabled.load(Ordering::Acquire) {
            return AsofLookup::Fallback;
        }
        let guard = self.join_index.read();
        let Some(index) = guard.as_ref() else {
            return AsofLookup::Fallback;
        };
        index.lookup_asof_max(key, event_time, min_ts, max_seq)
    }

    /// Raw `Timestamp(Ns)` time values for every row of a batch, aligned with
    /// the batch's row order (row `i` → `ts_list[i]`). `None` for null / non-Ts
    /// rows (the asof path skips them).
    fn raw_ts_list(&self, tb: &TimedBatch) -> Vec<Option<i64>> {
        match self.time_col_index {
            Some(tc) => (0..tb.batch.num_rows())
                .map(|row| batch_raw_ts_nanos(&tb.batch, tc, row))
                .collect(),
            None => vec![None; tb.batch.num_rows()],
        }
    }

    /// Test-only: whether any buffered batch has materialized its
    /// `parsed_events`. The columnar join index must never trigger this — a
    /// join-target window with no rule subscription stays fully columnar.
    #[cfg(test)]
    fn any_parsed_events_materialized(&self) -> bool {
        let log = self.log.read().expect("log lock poisoned");
        log.values().any(|tb| tb.parsed_events.get().is_some())
    }

    /// Append a RecordBatch to this window.
    ///
    /// Empty batches are silently skipped. Returns an error if the batch
    /// schema does not match the window schema. After appending, memory
    /// eviction runs if `current_bytes > max_window_bytes`.
    pub fn append(&self, batch: RecordBatch) -> CoreResult<()> {
        self.append_inner(batch, None, None, None).map(|_| ())
    }

    /// Append a RecordBatch whose events were already parsed *outside* the
    /// window (by the router). Rule tasks then read the pre-parsed `Arc`
    /// with no `OnceLock` contention among the concurrent rule tasks.
    pub fn append_parsed(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
    ) -> CoreResult<()> {
        self.append_inner(batch, Some(parsed_events), None, None)
            .map(|_| ())
    }

    /// Append a RecordBatch whose events *and content byte size* were precomputed
    /// by the caller (the R2 parse worker), so the O(rows×cols) accounting runs
    /// in parallel rather than on the ordered commit path. Returns the sequence
    /// number assigned to the appended batch.
    pub fn append_parsed_sized(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<u64> {
        self.append_inner(batch, Some(parsed_events), Some(byte_size), shard_rows)
    }

    /// Append a RecordBatch *without* pre-parsed events but *with* a
    /// precomputed content byte size (R2 parse worker) **and** the parse-side
    /// precomputed columnar shard partition (`shard_rows`, the P2 zero
    /// re-partition data). Used by the columnar/deferred commit path (pull
    /// model sharded match rules) where `route_parse` leaves events `None`
    /// but still carries `shard_rows`. The prior `(None, _)` arm of
    /// `append_with_watermark_inner` funnelled here via `self.append(batch)`,
    /// which dropped `shard_rows` — leaving every pull shard to process the
    /// whole batch (Q2 30M pull over-production, ~9×). Returns the sequence
    /// number assigned to the appended batch.
    pub fn append_sized(
        &self,
        batch: RecordBatch,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<u64> {
        self.append_inner(batch, None, Some(byte_size), shard_rows)
    }

    fn append_inner(
        &self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        // Accept batches that contain at least the window's fields (superset OK).
        if !self.schema.fields().iter().all(|f| {
            batch
                .schema()
                .field_with_name(f.name())
                .is_ok_and(|bf| bf.data_type() == f.data_type())
        }) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "schema mismatch: window {:?} expects {:?}, got {:?}",
                    self.name,
                    self.schema,
                    batch.schema()
                ))
                .err();
        }

        let event_time_range = self.extract_time_range(&batch);
        let row_count = batch.num_rows();
        // Account by *content* bytes, not Arrow buffer allocations: IPC decode
        // inflates `get_array_memory_size` with padding (~7x for decoded arrays),
        // so a single padded frame can exceed max_window_bytes and be silently
        // dropped even though its data is small (wp-labs/wp-reactor#18).
        let byte_size = byte_size.unwrap_or_else(|| content_bytes(&batch));
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let parsed_lock = std::sync::OnceLock::new();
        if let Some(events) = parsed_events {
            // Ignore the error: a freshly-created OnceLock is always empty.
            let _ = parsed_lock.set(events);
        }

        self.current_bytes.fetch_add(byte_size, Ordering::Relaxed);
        // 会计保真度（仅可观测性）：实际分配字节。`get_array_memory_size` 求和各列
        // 缓冲容量，O(列数) 而非 O(行数)，每批一次可忽略。
        let alloc_size = allocated_bytes(&batch);
        self.current_alloc_bytes
            .fetch_add(alloc_size, Ordering::Relaxed);
        self.total_rows.fetch_add(row_count, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);

        // Memory eviction: pop oldest batches while over budget.
        let max_bytes = self.config.max_window_bytes.as_bytes();
        // 热路径：绝大多数 append 不超预算——仅在需要驱逐时才取消费前沿与保留
        // 前沿（两把 progress 读锁）。锁序保持 progress.read → log.write，与
        // `evict_oldest_acked` 一致（绝不持 log 锁时取 progress 锁，防死锁）。
        let over_budget = self.current_bytes.load(Ordering::Relaxed) > max_bytes;
        // Per-window eviction is floor-respecting: only drop batches every
        // live consumer has already acked (`seq < ack_floor`). An unacked
        // front batch stops the sweep — the window may transiently exceed
        // `max_window_bytes` rather than lose unread pull data (the periodic
        // evictor reclaims it once consumers advance).
        let ack_floor = if over_budget {
            self.min_acked()
        } else {
            u64::MAX
        };
        // D4 retention pin: a join-target reader (deferred join) owns no consumer
        // slot, so `ack_floor` says "everything evictable" for it. Its published
        // frontier is honoured here instead — without this, the byte cap silently
        // truncates join results (nexmark q9/q4a −62% at 30M, 2026-08-24).
        // `i64::MAX` = no pins → the check is skipped entirely (byte-identical to
        // the pre-pin behaviour, including windows with no time column).
        let retention_ns = if over_budget {
            self.retention_floor_ns()
        } else {
            i64::MAX
        };
        let pinned = retention_ns != i64::MAX;
        let mut evicted_bytes = 0usize;
        let mut evicted_rows = 0usize;
        {
            let mut log = self.log.write().expect("window log lock poisoned");
            log.insert(
                seq,
                TimedBatch {
                    batch: Arc::new(batch),
                    event_time_range,
                    ingested_at: Instant::now(),
                    row_count,
                    byte_size,
                    alloc_size,
                    seq,
                    parsed_events: parsed_lock,
                    shard_rows,
                },
            );
            while self.current_bytes.load(Ordering::Relaxed) > max_bytes {
                let Some((&key, tb)) = log.first_key_value() else {
                    break;
                };
                // Unacked front batch: stop the sweep — never drop a batch a
                // live consumer has not yet read.
                if tb.seq >= ack_floor {
                    break;
                }
                // Pinned rows: this batch may still hold rows a join-target
                // reader needs. Exceed the budget rather than lose them.
                if pinned && tb.event_time_range.1 >= retention_ns {
                    break;
                }
                // `BTreeMap::remove` returns the owned value: dropping it
                // destroys the Arrow batch and parsed events eagerly — no
                // deferred (epoch-GC) reclamation to drive.
                let Some(tb) = log.remove(&key) else {
                    continue;
                };
                let byte_size = tb.byte_size;
                let row_count = tb.row_count;
                let alloc_size = tb.alloc_size;
                self.remove_batch_from_index(&tb);
                drop(tb);
                self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
                self.current_alloc_bytes
                    .fetch_sub(alloc_size, Ordering::Relaxed);
                self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
                self.batch_count.fetch_sub(1, Ordering::Relaxed);
                evicted_bytes += byte_size;
                evicted_rows += row_count;
            }

            // Index the newly appended batch (after eviction, so rows evicted
            // by the incoming batch aren't kept in the index).
            if self.join_enabled.load(Ordering::Acquire)
                && let Some(tb) = log.get(&seq)
                && let Some(idx) = self.join_index.read().as_ref()
            {
                let ts_list = self.raw_ts_list(tb);
                idx.index_batch(&tb.batch, &ts_list, seq);
            }
        }
        if evicted_rows > 0 {
            // The incoming batch was dropped (in whole or part) because it pushed
            // the window over max_window_bytes — e.g. a single oversized Arrow
            // frame exceeds the cap and is silently discarded. Log it so rules
            // that stop seeing events aren't a mystery.
            log::warn!(
                "window `{}` dropped {} row(s) / {} bytes in memory eviction (max_window_bytes={} bytes, incoming batch = {} rows / {} bytes, retention_floor_ns={})",
                self.name,
                evicted_rows,
                evicted_bytes,
                max_bytes,
                row_count,
                byte_size,
                retention_ns,
            );
        }

        // Content changed (append + any accompanying eviction): bump the
        // generation so `window.has()` / snapshot caches invalidate.
        self.generation.fetch_add(1, Ordering::Relaxed);

        Ok(seq)
    }

    /// Monotonic content-generation counter (see the struct field docs).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Remove an evicted batch's rows from the join index (if configured).
    /// 外层只取读锁（索引设置后不变）；`JoinIndex::remove_batch` 内部按
    /// `batch_keys` 增量清理（只动该批贡献过的 key）——q4 100M 断崖修复：
    /// 旧实现每驱逐一批就全索引扫描（33M 行 × 每批），evictor 线程卡死一核。
    fn remove_batch_from_index(&self, evicted: &TimedBatch) {
        if !self.join_enabled.load(Ordering::Acquire) {
            return;
        }
        if let Some(idx) = self.join_index.read().as_ref() {
            idx.remove_batch(evicted.seq);
        }
    }

    /// Return a snapshot of all current batches.
    ///
    /// `RecordBatch::clone()` is Arc-ref-counted — no data copy occurs.
    /// The returned `Vec` remains valid even if the window is subsequently
    /// mutated.
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        let log = self.log.read().expect("window log lock poisoned");
        log.values().map(|tb| tb.batch.as_ref().clone()).collect()
    }

    /// Return a snapshot of the batches with `seq <= max_seq`.
    ///
    /// M2 (seq-watermark consistency, window-actor-pull-model.md §3.5): when a
    /// rule task is processing batch N, its `window_lookup` must see only the
    /// batches this rule already pulled (`seq <= N`), never the batches the
    /// actor may already have appended past it. `None` returns the full log
    /// (identical to [`Window::snapshot`]) — the legacy view used when no
    /// seq watermark is enforced (push mode / no-join rules).
    ///
    /// `RecordBatch::clone()` is Arc-ref-counted — no data copy occurs.
    pub fn snapshot_up_to(&self, max_seq: Option<u64>) -> Vec<RecordBatch> {
        let log = self.log.read().expect("window log lock poisoned");
        match max_seq {
            Some(n) => log
                .range(..=n)
                .map(|(_, tb)| tb.batch.as_ref().clone())
                .collect(),
            None => log.values().map(|tb| tb.batch.as_ref().clone()).collect(),
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// 存活批次的**实际分配** Arrow 缓冲字节（包含 null bitmap / offsets /
    /// 缓冲容量舍入）。与 [`Self::memory_usage`]（逻辑内容口径，驱逐/mailbox
    /// 预算用）并行维护，仅供内存分账与指标。生产实测两者基本相等（1.00×）：
    /// 本口径的作用是**排除"content_bytes 系统性低估"这一假说**，而不是新增账目。
    pub fn allocated_usage(&self) -> usize {
        self.current_alloc_bytes.load(Ordering::Relaxed)
    }

    pub fn max_window_bytes(&self) -> usize {
        self.config.max_window_bytes.as_bytes()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows.load(Ordering::Relaxed)
    }

    pub fn batch_count(&self) -> usize {
        self.batch_count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.batch_count() == 0
    }

    /// Index of the time column in the schema, if present.
    pub fn time_col_index(&self) -> Option<usize> {
        self.time_col_index
    }

    /// Whether rule tasks defer per-row event materialization (L2).
    pub fn defer_materialization(&self) -> bool {
        self.defer_materialization
    }

    /// Field projection used when materializing events from this window's
    /// batches (L2 deferred materialization). `None` materializes every schema
    /// column. Exposed for the pull-model rule tasks, which read the raw
    /// `RecordBatch` and need the same projection the columnar push path uses.
    pub fn materialize_fields(&self) -> Option<&Arc<HashSet<String>>> {
        self.materialize_fields.as_ref()
    }

    // -- private helpers ----------------------------------------------------

    /// Extract the (min, max) event-time range from a batch.
    ///
    /// Returns `(i64::MIN, i64::MAX)` sentinel when there is no time column,
    /// the column cannot be downcast, or all values are null.
    fn extract_time_range(&self, batch: &RecordBatch) -> (i64, i64) {
        let Some(idx) = self.time_col_index else {
            return (i64::MIN, i64::MAX);
        };

        let col = batch.column(idx);
        let Some(ts_array) = col.as_any().downcast_ref::<TimestampNanosecondArray>() else {
            return (i64::MIN, i64::MAX);
        };

        let mut min_val = i64::MAX;
        let mut max_val = i64::MIN;
        let mut found = false;

        for i in 0..ts_array.len() {
            if !ts_array.is_null(i) {
                let v = ts_array.value(i);
                min_val = min_val.min(v);
                max_val = max_val.max(v);
                found = true;
            }
        }

        if found {
            (min_val, max_val)
        } else {
            (i64::MIN, i64::MAX)
        }
    }
}

/// 批次**实际引用**的 Arrow 缓冲字节（按缓冲去重）——内存分账用口径。
///
/// 与另两个口径的区别（2026-08-25 会计保真度）：
/// - [`content_bytes`]（驱逐/mailbox 预算用）：只算**逻辑列内容**，漏掉
///   null bitmap / offsets——新建批次会低估这两项（IPC 切片批次影响很小）。
/// - `RecordBatch::get_array_memory_size()`：按列累加**整个底层分配容量**，
///   而 IPC 解码批次的各列是**同一帧体的零拷贝切片** → 每列重复计一遍
///   （实测 bid_events content 1.58GB → 报 17.97GB，11.4×，甚至超过进程
///   peak_commit）。**不可用于分账。**
///
/// 本函数：递归走所有列（含子数组与 null buffer），**按缓冲起始指针去重**，
/// 累加各缓冲实际引用长度（`Buffer::len`）。共享帧体只计其被引用的部分，
/// 既不重复计也不漏 bitmap/offsets。
///
/// 已知近似：不计底层分配的**容量宽余**（已分配未引用的尾部）——那部分在
/// 多切片共享时无法归给单一批次；也不计跨窗口共享（各流帧独立，实路上
/// 不重叠）。
pub fn allocated_bytes(batch: &RecordBatch) -> usize {
    // 去重集用 `Vec` 线性扫而非 `HashSet`（R3 review）：每批缓冲数量级是
    // 列数 × (1~2 个数据缓冲 + null 缓冲) ≈ 20~35，线性扫比哈希快且只一次
    // 分配。本函数在**单写者提交路径**上（每批 append 一次）。
    let mut seen: Vec<usize> = Vec::with_capacity(32);
    let mut total = 0usize;
    for col in batch.columns() {
        collect_data_buffers(&col.to_data(), &mut seen, &mut total);
    }
    total
}

fn collect_data_buffers(data: &arrow::array::ArrayData, seen: &mut Vec<usize>, total: &mut usize) {
    let count_once = |ptr: usize, len: usize, seen: &mut Vec<usize>, total: &mut usize| {
        if !seen.contains(&ptr) {
            seen.push(ptr);
            *total += len;
        }
    };
    for buf in data.buffers() {
        count_once(buf.as_ptr() as usize, buf.len(), seen, total);
    }
    if let Some(nulls) = data.nulls() {
        let buf = nulls.buffer();
        count_once(buf.as_ptr() as usize, buf.len(), seen, total);
    }
    for child in data.child_data() {
        collect_data_buffers(child, seen, total);
    }
}

/// Estimate the retained *content* bytes of a batch — the actual data size, not
/// the Arrow buffer allocations (which IPC decode inflates with padding).
///
/// Used for window memory accounting so a single padded frame doesn't exceed
/// `max_window_bytes` and get dropped by memory eviction even though its data
/// is small (wp-labs/wp-reactor#18).
pub fn content_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| column_content_bytes(col.as_ref()))
        .sum()
}

fn column_content_bytes(col: &dyn Array) -> usize {
    let n = col.len();
    match col.data_type() {
        DataType::Null => 0,
        DataType::Boolean => bitmap_bytes(n) * 2, // data + validity bitmaps
        // Fixed-width values: width × rows.
        DataType::Int8 | DataType::UInt8 => n,
        DataType::Int16 | DataType::UInt16 => n * 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => n * 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(..)
        | DataType::Duration(_) => n * 8,
        DataType::Interval(IntervalUnit::MonthDayNano) => n * 16,
        DataType::Interval(_) => n * 8,
        DataType::Decimal128(..) => n * 16,
        DataType::Decimal256(..) => n * 32,
        DataType::Utf8 => utf8_content(
            n,
            col.as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column"),
        ),
        DataType::LargeUtf8 => large_utf8_content(
            n,
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8 column"),
        ),
        DataType::Binary => binary_content(
            n,
            col.as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary column"),
        ),
        DataType::LargeBinary => large_binary_content(
            n,
            col.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("large binary column"),
        ),
        DataType::FixedSizeBinary(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("fixed-size binary column");
            n * arr.value_length() as usize
        }
        DataType::Struct(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct column");
            // The struct's own validity bitmap plus children.
            bitmap_bytes(n)
                + arr
                    .columns()
                    .iter()
                    .map(|c| column_content_bytes(c.as_ref()))
                    .sum::<usize>()
        }
        DataType::List(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list column");
            // value(i) slices the child; a null row yields an empty slice → 0 bytes.
            bitmap_bytes(n)
                + offsets_bytes(n, 4)
                + (0..n)
                    .map(|i| column_content_bytes(arr.value(i).as_ref()))
                    .sum::<usize>()
        }
        DataType::LargeList(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list column");
            bitmap_bytes(n)
                + offsets_bytes(n, 8)
                + (0..n)
                    .map(|i| column_content_bytes(arr.value(i).as_ref()))
                    .sum::<usize>()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("fixed-size list column");
            bitmap_bytes(n)
                + (0..n)
                    .map(|i| column_content_bytes(arr.value(i).as_ref()))
                    .sum::<usize>()
        }
        DataType::Map(..) => {
            let arr = col.as_any().downcast_ref::<MapArray>().expect("map column");
            // Offsets + validity, plus the full key/value entries (unreferenced
            // entry slots are included — conservative).
            bitmap_bytes(n)
                + offsets_bytes(n, 4)
                + column_content_bytes(arr.keys().as_ref())
                + column_content_bytes(arr.values().as_ref())
        }
        // Dictionary and anything else: upper-bound estimate (dictionary values
        // are shared, so this overcounts — the safe direction for eviction).
        _ => n * 8,
    }
}

/// Bytes for a bit-packed bitmap over `n` rows.
fn bitmap_bytes(n: usize) -> usize {
    n.div_ceil(8)
}

/// Bytes for an offset buffer of `(n + 1)` entries, `width` bytes each.
fn offsets_bytes(n: usize, width: usize) -> usize {
    (n + 1) * width
}

/// Content bytes of a utf8 column: `(n + 1)` i32 offsets + string payload.
///
/// O(1) payload: `offsets[n] - offsets[0]` (offsets only advance by actual
/// value lengths — null slots carry the previous offset forward), so no
/// per-row `str::len` walk is needed. Called twice per batch on the hot path
/// ([`push_decoded_batch`] + [`Router::route_parse`]); the walk version cost
/// ~2×100k iterator steps per string column per batch at 44M EPS.
fn utf8_content(n: usize, arr: &StringArray) -> usize {
    offsets_bytes(n, 4) + utf8_payload_bytes(arr.value_offsets())
}

fn utf8_payload_bytes(offsets: &[i32]) -> usize {
    let first = offsets.first().copied().unwrap_or(0);
    let last = offsets.last().copied().unwrap_or(first);
    (last as usize).saturating_sub(first as usize)
}

fn large_utf8_content(n: usize, arr: &LargeStringArray) -> usize {
    offsets_bytes(n, 8) + large_utf8_payload_bytes(arr.value_offsets())
}

fn large_utf8_payload_bytes(offsets: &[i64]) -> usize {
    let first = offsets.first().copied().unwrap_or(0);
    let last = offsets.last().copied().unwrap_or(first);
    (last as usize).saturating_sub(first as usize)
}

/// Content bytes of a binary column: `(n + 1)` i32 offsets + payload.
/// O(1) payload via offset span, same as utf8.
fn binary_content(n: usize, arr: &BinaryArray) -> usize {
    offsets_bytes(n, 4) + utf8_payload_bytes(arr.value_offsets())
}

fn large_binary_content(n: usize, arr: &LargeBinaryArray) -> usize {
    offsets_bytes(n, 8) + large_utf8_payload_bytes(arr.value_offsets())
}

// ---------------------------------------------------------------------------
// Parsed-event memory accounting
// ---------------------------------------------------------------------------

/// Estimate the retained bytes of parsed events: each event is an
/// `HashMap<SmolStr, Value>` (a foldhash table). Structured `object` fields
/// decoded from JSON become nested `EngineHashMap`/`Vec` allocations with
/// fixed per-entry overhead (key struct + bucket + hash/ctrl), so a window
/// that also retains these events holds several× the JSON string bytes it
/// accounts for via [`content_bytes`] — memory eviction then fires far past
/// the real water level (wp-labs/wp-reactor#20: `current_bytes` ≈ cap while
/// RSS ran to 2× max).
///
/// The estimate errs toward overcount (safe direction for eviction): it uses
/// `capacity()`-based table sizes and a per-entry hash/ctrl allowance, so a
/// window never retains *more* real memory than its accounting reports.
pub fn events_bytes(events: &[Arc<Event>]) -> usize {
    events.iter().map(|e| event_bytes(e)).sum()
}

/// Retained bytes of one parsed [`Event`]: the `Event`/`HashMap` header, the
/// bucket table, and every nested value's heap payload.
fn event_bytes(e: &Event) -> usize {
    // size_of::<Event>() covers the foldhash table header itself.
    size_of::<Event>()
        + map_heap_bytes(
            e.fields.capacity(),
            size_of::<SmolStr>(),
            size_of::<Value>(),
        )
        + e.fields
            .iter()
            .map(|(k, v)| smol_str_heap_bytes(k) + value_heap_bytes(v))
            .sum::<usize>()
}

/// Extra heap bytes of a [`Value`] *beyond* the enum's inline storage (the enum
/// struct — including an inline `Vec`/`HashMap` header — is already charged by
/// the containing bucket via `map_heap_bytes`). Recurses into nested containers.
fn value_heap_bytes(v: &Value) -> usize {
    match v {
        Value::Number(_) | Value::Bool(_) => 0,
        Value::Str(s) => smol_str_heap_bytes(s),
        Value::Array(items) => {
            items.capacity() * size_of::<Value>()
                + items.iter().map(value_heap_bytes).sum::<usize>()
        }
        Value::Object(fields) => {
            map_heap_bytes(fields.capacity(), size_of::<SmolStr>(), size_of::<Value>())
                + fields
                    .iter()
                    .map(|(k, v)| smol_str_heap_bytes(k) + value_heap_bytes(v))
                    .sum::<usize>()
        }
    }
}

/// Heap allocation of a `HashMap` bucket table + control bytes.
///
/// std's swiss-table layout stores one `u64` hash plus key+value per bucket,
/// one control byte per bucket (SIMD-group padded), and keeps some growth
/// slack. The flat `+ 16` per entry covers control bytes + padding + slack and
/// errs conservative (overcount).
fn map_heap_bytes(capacity: usize, key_size: usize, value_size: usize) -> usize {
    capacity * (size_of::<usize>() + key_size + value_size + 16)
}

/// Heap bytes of a `SmolStr` beyond its inline struct: only strings that
/// outgrew the inline buffer allocate (payload + NUL).
fn smol_str_heap_bytes(s: &SmolStr) -> usize {
    if s.is_heap_allocated() {
        s.len() + 1
    } else {
        0
    }
}
