//! 规则-窗口订阅扇出与分片（rule_shards）：窗口订阅注册、读游标分批、把行
//! 子集分发给各规则 worker；分片键 = 简单字段取模或表达式派生键逐行求值哈希
//! （issue #80，见 partition 相关函数）。测试在 `window/tests/`。

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use wf_lang::ast::{Expr, FieldRef};

use crate::match_engine::event_bridge::{ColumnarEvent, extract_field_value};
use crate::match_engine::{
    Event, ScopeKey, Value, extract_key_simple, extract_scope_key_mixed, field_ref_name,
    scope_key_from_values, scope_key_shard_index,
};
use arrow::record_batch::RecordBatch;

/// A batch of parsed events pushed from one window to its subscribing rules.
///
/// The `window_name` tags which window the events were appended to, so a rule
/// subscribed to multiple windows can map the batch to the correct aliases.
/// `seq` is the window-assigned batch sequence number; consumers ack
/// `seq + 1` on the window's [`WindowProgress`](crate::window::WindowProgress)
/// slot after processing, which gates time-based eviction.
#[derive(Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.RuleFanout")]
pub struct RulePush {
    pub window_name: Arc<str>,
    /// Pre-parsed events, when the producer materialized them. `None` means the
    /// rule task defers materialization and parses only the rows its bind filter
    /// accepts (L2).
    pub events: Option<Arc<Vec<Arc<Event>>>>,
    /// The raw batch these events were parsed from, when the producer has it.
    /// Rule tasks use it for columnar guard evaluation (zero-copy); `None` for
    /// relay pushes (intermediate pipes) that only carry parsed events.
    pub batch: Option<Arc<RecordBatch>>,
    /// Per-event field whitelist the producer used (or would use) when
    /// materializing `events`. Deferred rule tasks use it to materialize the
    /// raw `batch` with the same field set as the eager path, keeping the
    /// event representation (and downstream wfx_id) stable.
    pub materialize_fields: Option<Arc<HashSet<String>>>,
    pub seq: u64,
    /// Only set by a **sharded** broadcast that defers materialization
    /// (`events` is `None`): the batch rows this shard owns (subset of the
    /// raw `batch`, already partitioned by the match key). Unsharded pushes and
    /// row-based (pre-materialized) pushes leave this `None`. The rule task
    /// applies its columnar bind filter over exactly these rows.
    pub shard_rows: Option<Arc<Vec<u32>>>,
}

/// 每窗 fanout 的**分片键规格**（issue #80）：`keys` + 逐位对齐的表达式槽。
///
/// 普通字段/嵌套路径 key：只填 `keys`（`key_exprs` 空），分片走列直读快路径；
/// 表达式派生 key（#80，如 `concat(src,":",dst)` 的 let）：`keys[i]` 保留逻辑名、
/// `key_exprs[i] = Some(expr)`，fanout 对每行事件求值后哈希分片。
///
/// 求值经 `extract_scope_key_mixed`（与机器内 advance 同构：同一 `ScopeKey` →
/// 同一 shard），故同派生 key 的事件必然落在同一 rule task，窗口跨事件聚合
/// 状态不被切碎。
#[derive(Clone, Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.RuleFanout")]
pub struct ShardKeySpec {
    pub keys: Arc<[FieldRef]>,
    /// 表达式槽；空 = 无表达式键（纯字段分片）。非空时与 `keys` 逐位对齐。
    pub key_exprs: Arc<[Option<Expr>]>,
}

impl ShardKeySpec {
    pub fn new(keys: Arc<[FieldRef]>) -> Self {
        Self {
            keys,
            key_exprs: Arc::from([]),
        }
    }

    /// 是否含表达式键位（决定分片是否走逐行求值）。
    pub fn has_exprs(&self) -> bool {
        self.key_exprs.iter().any(Option::is_some)
    }
}

/// 全等比较：冲突检测必须把表达式槽纳入（同 keys、一方带 expr 一方不带 =
/// 分区方式不同，同窗口并存会互相覆盖注册导致状态切碎）。
impl PartialEq for ShardKeySpec {
    fn eq(&self, other: &Self) -> bool {
        self.keys == other.keys && self.key_exprs == other.key_exprs
    }
}

/// A subscription for one window: a single (unsharded) rule channel, N shard
/// channels with a key partition (rule sharding, P2a), or N worker channels
/// with whole-batch round-robin (stateless `on each` sharding, R4).
///
/// Channels are **bounded** so a slow rule consumer backpressures the producer
/// (the window actor's broadcast awaits a full channel) instead of buffering
/// unboundedly — 50M sustained inject with unbounded channels let RSS grow to
/// ~13GB (wp-labs/wp-reactor long-run test, 2026-08-14).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.RuleFanout")]
enum Subscription {
    Single(mpsc::Sender<RulePush>),
    Sharded {
        shards: Vec<mpsc::Sender<RulePush>>,
        spec: ShardKeySpec,
    },
    RoundRobin {
        shards: Vec<mpsc::Sender<RulePush>>,
        /// Next shard index (wraps via modulo on take). Shared across clones
        /// of this subscription so every broadcast advances the same cursor.
        next: Arc<AtomicUsize>,
    },
}

// Manual impl: `AtomicUsize` is not `Clone`, the round-robin cursor is shared
// behind its `Arc` instead.
impl Clone for Subscription {
    fn clone(&self) -> Self {
        match self {
            Subscription::Single(tx) => Subscription::Single(tx.clone()),
            Subscription::Sharded { shards, spec } => Subscription::Sharded {
                shards: shards.clone(),
                spec: spec.clone(),
            },
            Subscription::RoundRobin { shards, next } => Subscription::RoundRobin {
                shards: shards.clone(),
                next: Arc::clone(next),
            },
        }
    }
}

/// Fan-out table mapping window names to per-rule channels.
///
/// The window actor (producer) broadcasts each parsed `Arc<Vec<Arc<Event>>>`
/// to every channel registered for the window it was appended to; rule tasks
/// (consumers) receive those `Arc`s and advance their state machines without
/// taking the window log lock. Registration happens at rule-task spawn time;
/// closed channels (from a drained/cancelled rule) are pruned lazily on the
/// next broadcast.
///
/// A second table, `window_sharding`, carries the *key partition* of a window
/// independent of the delivery channels. The pull-model (window-actor-pull-
/// model.md, M1) does not register delivery channels (rule tasks pull from the
/// window log instead), yet the parse stage still needs to precompute the
/// per-shard row subsets so they can be stored once in the window log (P2
/// zero re-partition). That partition is registered here and consulted by
/// `precompute_shard_rows` even when no delivery `Subscription` exists.
/// Pull-model partition of one window.
///
/// 空键 = **输入行索引分区**（`row % shard_count`, 2026-08-24 q15 空键 stats
/// 输入分片用——按行号均匀切分, 各片独立累加, close 时归并）; 非空 = 按键
/// 哈希分区（`partition_rows_by_key`/表达式分片, 同 key 同片）。
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.RuleFanout")]
pub(crate) struct WindowShardPartition {
    pub spec: ShardKeySpec,
    pub shard_count: usize,
}

/// 输入行索引分区: `row % shard_count` 均匀切分（空键 stats 输入分片）。
/// 行序保持（每片内行索引升序）, 时间分布均匀（各片窗口对齐）。
pub(crate) fn partition_rows_by_index(batch: &RecordBatch, shard_count: usize) -> Vec<Vec<u32>> {
    let n = batch.num_rows();
    let shards = shard_count.max(1);
    let mut per: Vec<Vec<u32>> = vec![Vec::with_capacity(n / shards + 1); shards];
    for i in 0..n {
        per[i % shards].push(i as u32);
    }
    per
}

#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.RuleFanout")]
pub struct RuleFanout {
    table: RwLock<HashMap<String, Vec<Subscription>>>,
    /// window_name → (match keys, shard count) for the key-partitioned
    /// subscription of that window, used by the pull model to precompute
    /// shard row subsets without a delivery channel.
    window_sharding: RwLock<HashMap<String, WindowShardPartition>>,
}

impl RuleFanout {
    /// Create a fresh, empty fan-out table.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether any rule channel is registered for `window_name`.
    ///
    /// `route_parse` uses this to skip event materialization (and its
    /// accounting) for windows no rule consumes — the dominant parse-side
    /// cost when a window has no subscribers.
    pub fn has_subscribers(&self, window_name: &str) -> bool {
        self.table
            .read()
            .expect("fanout lock poisoned")
            .get(window_name)
            .is_some_and(|subs| !subs.is_empty())
    }

    /// Whether `window_name` has **only** round-robin delivery subscriptions
    /// (stateless `on each` sharded consumers) — or none at all.
    ///
    /// True means the producer can skip materializing per-row `Event`s for
    /// the broadcast: every delivery channel reads the raw `batch` (columnar
    /// safe, or falls back to parsing the batch itself), and no row-path
    /// consumer depends on `RulePush::events`. This is what lets the pipe
    /// producer (2026-08-25 q13) drop the ~18MB/批 events payload for sharded
    /// chains (q13a→bid_mod→q13b) that otherwise accumulates in-flight with
    /// sharded backpressure (RSS 28.8GB plateau).
    ///
    /// Single and Sharded subscriptions may consume `events` (row-path
    /// intermediate-window contract, locked by tests), so their presence
    /// forces the events path.
    pub fn round_robin_only(&self, window_name: &str) -> bool {
        let table = self.table.read().expect("fanout lock poisoned");
        match table.get(window_name) {
            None => true,
            Some(subs) => {
                subs.is_empty()
                    || subs
                        .iter()
                        .all(|s| matches!(s, Subscription::RoundRobin { .. }))
            }
        }
    }

    /// 每窗 fanout 通道的**排队批数 / 总容量**（求和到所有订阅与分片）。
    ///
    /// 存在理由（2026-08-26 内存定位）：diag 墙梯把 q13 的 12.5GB 内存增量定位到
    /// **输出链**（floor 3.3GB → rules 3.9GB → full 16.4GB），而窗口会计只解释
    /// 4.1GB。输出链里唯一未被度量的大容器就是规则分片通道：
    /// 10 分片 × `RULE_CHANNEL_CAPACITY` 256 = 2560 槽，若按 bid_mod 批 3.45MB 算
    /// 满队即 ~8.8GB——量级与残差吻合。
    ///
    /// 读的是 **批数**（tokio `max_capacity() - capacity()`）而非字节：通道里是
    /// `Arc<RecordBatch>`，字节可能与窗口共享（未 ack 批次窗口也在留），相加会
    /// 双算——先拿批数判断“通道是否接近满”，再决定是否值得追字节归属。
    pub fn queued_items(&self, window_name: &str) -> Option<(usize, usize)> {
        let table = self.table.read().expect("fanout lock poisoned");
        let subs = table.get(window_name)?;
        let mut queued = 0usize;
        let mut capacity = 0usize;
        let mut acc = |tx: &mpsc::Sender<RulePush>| {
            let max = tx.max_capacity();
            capacity += max;
            queued += max.saturating_sub(tx.capacity());
        };
        for sub in subs.iter() {
            match sub {
                Subscription::Single(tx) => acc(tx),
                Subscription::Sharded { shards, .. } | Subscription::RoundRobin { shards, .. } => {
                    for tx in shards.iter() {
                        acc(tx);
                    }
                }
            }
        }
        Some((queued, capacity))
    }

    /// Register a single (unsharded) rule channel for `window_name`.
    pub fn register(&self, window_name: &str, tx: mpsc::Sender<RulePush>) {
        let mut table = self.table.write().expect("fanout lock poisoned");
        table
            .entry(window_name.to_string())
            .or_default()
            .push(Subscription::Single(tx));
    }

    /// Register N shard channels for `window_name`, partitioned by `keys`.
    ///
    /// Each broadcast batch is split by `hash(extract_key(event)) % shards.len()`
    /// so every event with the same match key lands on the same shard.
    pub fn register_sharded(
        &self,
        window_name: &str,
        shards: Vec<mpsc::Sender<RulePush>>,
        keys: Arc<[FieldRef]>,
    ) {
        self.register_sharded_with_exprs(window_name, shards, ShardKeySpec::new(keys));
    }

    /// [`Self::register_sharded`] 的表达式键变体（issue #80）：`spec.key_exprs`
    /// 非空时，广播按逐行表达式求值结果分片（缺失/求值失败 → shard 0，与
    /// 机器内 skip 语义对齐）。
    pub fn register_sharded_with_exprs(
        &self,
        window_name: &str,
        shards: Vec<mpsc::Sender<RulePush>>,
        spec: ShardKeySpec,
    ) {
        debug_assert!(!shards.is_empty());
        if spec.has_exprs() {
            debug_assert_eq!(
                spec.keys.len(),
                spec.key_exprs.len(),
                "keys 与 key_exprs 必须逐位对齐"
            );
        }
        let mut table = self.table.write().expect("fanout lock poisoned");
        table
            .entry(window_name.to_string())
            .or_default()
            .push(Subscription::Sharded { shards, spec });
    }

    /// Register N worker channels for `window_name` served whole batches in
    /// round-robin order (stateless `on each` sharding).
    ///
    /// Each broadcast sends the **entire** `Arc<Vec<Arc<Event>>>` to the next
    /// worker — zero per-event partitioning cost, zero copies. This is only
    /// correct for stateless consumers (no cross-event rule state): event
    /// ordering across batches is no longer preserved.
    pub fn register_round_robin(&self, window_name: &str, shards: Vec<mpsc::Sender<RulePush>>) {
        debug_assert!(!shards.is_empty());
        let mut table = self.table.write().expect("fanout lock poisoned");
        table
            .entry(window_name.to_string())
            .or_default()
            .push(Subscription::RoundRobin {
                shards,
                next: Arc::new(AtomicUsize::new(0)),
            });
    }

    /// Register the key-partition of `window_name` for the **pull model**
    /// (window-actor-pull-model.md, M1).
    ///
    /// Unlike the `register_*` delivery methods this does **not** create a
    /// delivery channel — rule tasks pull from the window log instead. It only
    /// records `(keys, shard_count)` so the parallel parse stage can
    /// precompute the per-shard row subsets (`precompute_shard_rows`) and the
    /// window can store them once for every sharded rule task to read.
    pub fn register_window_sharding(
        &self,
        window_name: &str,
        keys: Arc<[FieldRef]>,
        shard_count: usize,
    ) {
        self.register_window_sharding_with_exprs(window_name, ShardKeySpec::new(keys), shard_count);
    }

    /// [`Self::register_window_sharding`] 的表达式键变体（issue #80）：pull 模型
    /// 的 parse 预计算分片同样支持逐行表达式求值。
    pub fn register_window_sharding_with_exprs(
        &self,
        window_name: &str,
        spec: ShardKeySpec,
        shard_count: usize,
    ) {
        debug_assert!(shard_count > 0);
        if spec.has_exprs() {
            debug_assert_eq!(
                spec.keys.len(),
                spec.key_exprs.len(),
                "keys 与 key_exprs 必须逐位对齐"
            );
        }
        let mut reg = self
            .window_sharding
            .write()
            .expect("fanout sharding lock poisoned");
        reg.insert(
            window_name.to_string(),
            WindowShardPartition { spec, shard_count },
        );
    }

    /// Whether `window_name` has a key-partitioned (sharded) subscription
    /// registered for the pull model — i.e. the parse stage should precompute
    /// `shard_rows` even though no delivery channel exists.
    pub fn window_is_sharded(&self, window_name: &str) -> bool {
        self.window_sharding
            .read()
            .expect("fanout sharding lock poisoned")
            .contains_key(window_name)
    }

    /// 窗口分片冲突检测（2026-08-29 q11/q6 多规则根因）：`window_sharding` 是
    /// **每窗口单一 (keys) 配置**（覆盖式 insert），多个规则以**不同 keys** 分片
    /// 同一窗口时互相覆盖——后注册规则的 shard 拉取按被覆盖的 key 分片，同 key
    /// 事件分散到不同 shard → 有状态规则状态被切碎（q11 bidder session 单规则
    /// 17081 → all 118234、q6 872913 → 787704）。注册方（spawn）用它判定：窗口
    /// 已被不同 keys 注册 → 本规则回退单 worker（整批处理，正确性优先）。
    /// 同 keys（如 q11/q12 都按 bidder）不算冲突。
    pub fn window_sharding_conflicts(&self, window_name: &str, keys: &[FieldRef]) -> bool {
        let reg = self
            .window_sharding
            .read()
            .expect("fanout sharding lock poisoned");
        match reg.get(window_name) {
            Some(existing) => {
                // keys 不同，或已注册者是表达式分片（分区方式含求值，即使 keys
                // 相同也不兼容——覆盖式注册会切碎状态）→ 冲突。
                existing.spec.keys.as_ref() != keys || existing.spec.has_exprs()
            }
            None => false,
        }
    }

    /// 表达式键感知的冲突检测（issue #80）：分区规格（keys **且** key_exprs）
    /// 全等才算不冲突——同 keys、一方带表达式槽一方不带 = 分区方式不同，若
    /// 误判不冲突会被覆盖式注册切碎状态。
    pub fn window_sharding_conflicts_with_exprs(
        &self,
        window_name: &str,
        spec: &ShardKeySpec,
    ) -> bool {
        let reg = self
            .window_sharding
            .read()
            .expect("fanout sharding lock poisoned");
        match reg.get(window_name) {
            Some(existing) => existing.spec != *spec,
            None => false,
        }
    }

    /// 输入行索引分区注册（空键 stats 输入分片, 2026-08-24 q15）:
    /// `shard_rows[i] = 行号 % shard_count == i` 的行。空键 = index 分区标记。
    pub fn register_window_index_sharding(&self, window_name: &str, shard_count: usize) {
        debug_assert!(shard_count > 0);
        let mut reg = self
            .window_sharding
            .write()
            .expect("fanout sharding lock poisoned");
        reg.insert(
            window_name.to_string(),
            WindowShardPartition {
                spec: ShardKeySpec::default(),
                shard_count,
            },
        );
    }

    /// Broadcast `events` (window batch with sequence `seq`) to every rule
    /// channel registered for `window_name`.
    ///
    /// Unsharded subscriptions receive the whole batch; sharded subscriptions
    /// partition it by match key. Bounded channels: a full channel blocks the
    /// producer (`.await` on send) — backpressure instead of unbounded
    /// buffering, so a slow rule consumer stalls the ingest rather than
    /// growing RSS. Closed channels are pruned lazily here.
    ///
    /// Slow-consumer semantics: each rule's *compute* is independent (own
    /// channel, shared `Arc` batches — a fast rule is not slowed by a slow
    /// one), and two things are gated by the slowest subscriber: (a) the
    /// window's eviction floor (`WindowProgress::min_acked`, so retained
    /// memory waits for the slowest rule) and (b) this broadcast itself — a
    /// full channel blocks the batch's completion (and with it the window
    /// actor's next append) until it drains. Sends to *independent*
    /// subscriptions run concurrently, though, so one full channel only
    /// backpressures its own stream, never the deliveries to the other
    /// subscribers (P1-② head-of-line fix). Per-channel FIFO is unaffected:
    /// each channel receives from exactly one send future per broadcast, and
    /// broadcasts themselves are serialized by the single-writer commit path.
    pub async fn broadcast(&self, window_name: &str, events: &Arc<Vec<Arc<Event>>>, seq: u64) {
        self.broadcast_inner(window_name, Some(events), None, None, None, seq)
            .await;
    }

    /// Like [`Self::broadcast`], but also forwards the raw [`RecordBatch`] the
    /// events were parsed from, so rule tasks can evaluate columnar guards
    /// zero-copy instead of materializing every row first.
    pub async fn broadcast_with_batch(
        &self,
        window_name: &str,
        events: &Arc<Vec<Arc<Event>>>,
        batch: &RecordBatch,
        materialize_fields: Option<&Arc<HashSet<String>>>,
        seq: u64,
    ) {
        self.broadcast_inner(
            window_name,
            Some(events),
            Some(batch),
            materialize_fields,
            None,
            seq,
        )
        .await;
    }

    /// Broadcast only the raw [`RecordBatch`] (L2 deferred materialization):
    /// each rule task materializes only the rows its bind filter accepts.
    /// `shard_rows` is an optional **precomputed** columnar partition of the
    /// batch rows (produced in the parallel parse stage); when present and its
    /// length matches the live subscription's shard count, the actor reuses it
    /// instead of re-partitioning the whole batch on the single-writer path
    /// (Q2 P0-③ wall).
    pub async fn broadcast_batch_only(
        &self,
        window_name: &str,
        batch: &RecordBatch,
        materialize_fields: Option<&Arc<HashSet<String>>>,
        shard_rows: Option<&[Vec<u32>]>,
        seq: u64,
    ) {
        self.broadcast_inner(
            window_name,
            None,
            Some(batch),
            materialize_fields,
            shard_rows,
            seq,
        )
        .await;
    }

    /// Precompute the sharded row partition for a window's batch, in the
    /// parallel parse stage. Returns `None` when the window has no sharded
    /// subscription (nothing to partition). Byte-identical to the partition
    /// [`broadcast_inner`] computes (`partition_rows_by_key`), so moving it
    /// here does not change which rows land on which shard.
    ///
    /// The partition source is resolved as: (1) a fanout `Sharded` delivery
    /// subscription (push model), else (2) the `window_sharding` registry
    /// (pull model — no delivery channel registered). The pull-model case is
    /// what lets `route_parse` precompute `shard_rows` for storage in the
    /// window log without any rule delivery channel.
    pub fn precompute_shard_rows(
        &self,
        window_name: &str,
        batch: &RecordBatch,
    ) -> Option<Arc<[Vec<u32>]>> {
        let (spec, shard_count) = {
            let subs = self.table.read().expect("fanout lock poisoned");
            let fanout = subs.get(window_name).and_then(|list| {
                list.iter().find_map(|s| match s {
                    Subscription::Sharded { shards, spec } => Some((spec.clone(), shards.len())),
                    _ => None,
                })
            });
            if let Some((spec, n)) = fanout {
                (spec, n)
            } else {
                let reg = self
                    .window_sharding
                    .read()
                    .expect("fanout sharding lock poisoned");
                let entry = reg.get(window_name)?;
                (entry.spec.clone(), entry.shard_count)
            }
        };
        let per = if spec.keys.is_empty() {
            // 空键 = 输入行索引分区（q15 空键 stats 输入分片）: 均匀按行号切分。
            partition_rows_by_index(batch, shard_count)
        } else {
            partition_rows(batch, &spec, shard_count).unwrap_or_else(|| {
                // Key column absent from schema → every row missing → all shard 0
                // (matches row-based). 表达式键规则永不走到这里（逐行求值分片）。
                let mut v = Vec::with_capacity(shard_count);
                v.resize_with(shard_count, Vec::new);
                v[0] = (0..batch.num_rows()).map(|r| r as u32).collect();
                v
            })
        };
        Some(per.into())
    }

    async fn broadcast_inner(
        &self,
        window_name: &str,
        events: Option<&Arc<Vec<Arc<Event>>>>,
        batch: Option<&RecordBatch>,
        materialize_fields: Option<&Arc<HashSet<String>>>,
        shard_rows: Option<&[Vec<u32>]>,
        seq: u64,
    ) {
        let subs: Vec<Subscription> = {
            let table = self.table.read().expect("fanout lock poisoned");
            table.get(window_name).cloned().unwrap_or_default()
        };
        if subs.is_empty() {
            return;
        }
        // One shared allocation per broadcast (not per subscription × shard).
        let window_name: Arc<str> = window_name.into();
        // Raw batch shared by non-sharded subscriptions; sharded subscriptions
        // partition events (so their row indices no longer match the whole
        // batch) and get `None` (interpreted fallback).
        let batch_arc: Option<Arc<RecordBatch>> = batch.map(|b| Arc::new(b.clone()));

        let mut sends: Vec<Pin<Box<dyn Future<Output = bool> + Send>>> = Vec::new();
        for sub in &subs {
            match sub {
                Subscription::Single(tx) => {
                    let push = RulePush {
                        window_name: Arc::clone(&window_name),
                        events: events.map(Arc::clone),
                        batch: batch_arc.clone(),
                        materialize_fields: materialize_fields.map(Arc::clone),
                        shard_rows: None,
                        seq,
                    };
                    let tx = tx.clone();
                    sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                }
                Subscription::Sharded { shards, spec } => {
                    match (events, batch_arc.as_ref()) {
                        // Row-based (pre-materialized events), **no batch**:
                        // keep the existing per-event key partition.
                        (Some(events), None) => {
                            sharded_sends(shards, spec, &window_name, events, seq, &mut sends);
                        }
                        // Batch available (with or without pre-materialized
                        // events): partition the raw batch by key and send each
                        // shard the batch + its row subset + the shared events
                        // (columnar consumers like the stats executor read the
                        // batch; row consumers index the events via shard_rows).
                        //
                        // The `events-only` sharded path (pipe relay broadcast
                        // carried no batch) silently starved columnar shard
                        // consumers — q4a→auction_finals→q4b(stats) chain emitted
                        // nothing (2026-08-23). Reuse the parse-side-precomputed
                        // `shard_rows` when it matches the subscription's shard
                        // count (off the actor's serial O(batch) partition work);
                        // otherwise fall back to a defensive re-partition
                        // (config drift / hot reload).
                        (events, Some(batch)) => {
                            let pre = match shard_rows {
                                Some(pre) if pre.len() == shards.len() => Some(pre),
                                _ => None,
                            };
                            let per: Arc<[Vec<u32>]> = match pre {
                                Some(pre) => Arc::from(pre),
                                None => partition_rows(batch, spec, shards.len())
                                    .unwrap_or_else(|| {
                                        // Key column absent from schema → every row
                                        // missing → all shard 0 (matches row-based).
                                        // 表达式键规则永不走到这里。
                                        let mut v = vec![Vec::new(); shards.len()];
                                        v[0] = (0..batch.num_rows()).map(|r| r as u32).collect();
                                        v
                                    })
                                    .into(),
                            };
                            for (i, rows) in per.iter().enumerate() {
                                if rows.is_empty() {
                                    continue;
                                }
                                let push = RulePush {
                                    window_name: Arc::clone(&window_name),
                                    events: events.map(Arc::clone),
                                    batch: batch_arc.clone(), // shared Arc (refcount, zero copy)
                                    materialize_fields: materialize_fields.map(Arc::clone),
                                    shard_rows: Some(Arc::new(rows.clone())),
                                    seq,
                                };
                                let tx = shards[i].clone();
                                sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                            }
                        }
                        // Unreachable: a sharded broadcast with neither events nor
                        // batch (no producer sends a sharded batch-only-without-batch).
                        (None, None) => {
                            debug_assert!(false, "sharded broadcast without events or batch");
                        }
                    }
                }
                Subscription::RoundRobin { shards, next } => {
                    let n = shards.len();
                    let idx = next.fetch_add(1, Ordering::Relaxed) % n;
                    let push = RulePush {
                        window_name: Arc::clone(&window_name),
                        events: events.map(Arc::clone),
                        batch: batch_arc.clone(),
                        materialize_fields: materialize_fields.map(Arc::clone),
                        shard_rows: None,
                        seq,
                    };
                    let tx = shards[idx].clone();
                    sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                }
            }
        }

        let any_closed = join_sends(sends).await;
        if any_closed {
            let mut table = self.table.write().expect("fanout lock poisoned");
            if let Some(subs) = table.get_mut(window_name.as_ref()) {
                for sub in subs.iter_mut() {
                    match sub {
                        Subscription::Single(tx) => {
                            // handled by retain below
                            let _ = tx;
                        }
                        Subscription::Sharded { shards, .. } => {
                            shards.retain(|tx| !tx.is_closed());
                        }
                        Subscription::RoundRobin { shards, .. } => {
                            shards.retain(|tx| !tx.is_closed());
                        }
                    }
                }
                subs.retain(|sub| match sub {
                    Subscription::Single(tx) => !tx.is_closed(),
                    Subscription::Sharded { shards, .. } => !shards.is_empty(),
                    Subscription::RoundRobin { shards, .. } => !shards.is_empty(),
                });
                if subs.is_empty() {
                    table.remove(window_name.as_ref());
                }
            }
        }
    }
}

/// Poll every send future to completion, concurrently (each gets polled on
/// every wake; none waits for an earlier one to finish). Returns whether any
/// channel was closed. Hand-rolled instead of pulling in `futures` — this is
/// the only combinator needed.
async fn join_sends(mut sends: Vec<Pin<Box<dyn Future<Output = bool> + Send>>>) -> bool {
    let mut any_closed = false;
    if sends.is_empty() {
        return any_closed;
    }
    std::future::poll_fn(move |cx: &mut Context<'_>| {
        let mut still: Vec<Pin<Box<dyn Future<Output = bool> + Send>>> =
            Vec::with_capacity(sends.len());
        for mut fut in sends.drain(..) {
            match fut.as_mut().poll(cx) {
                Poll::Ready(closed) => any_closed |= closed,
                Poll::Pending => still.push(fut),
            }
        }
        sends = still;
        if sends.is_empty() {
            Poll::Ready(any_closed)
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Extract a field from a batch at a pre-resolved column index, byte-identical
/// to the row-based `Event.fields.get(name)` used by [`extract_key_simple`].
fn column_scalar(batch: &RecordBatch, col_idx: usize, row: usize) -> Option<Value> {
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return None;
    }
    extract_field_value(batch.schema().field(col_idx), col.as_ref(), row)
}

/// Build a [`ScopeKey`] from a batch column at `row` (columnar key path), **without
/// rounding through [`Value`]** — reads the native Arrow value straight into the
/// typed key. Produces the **same** variant as `ScopeKey::from_value` on the
/// row-based `Value`, so both paths shard identically.
///
/// Returns `None` when the cell is null / missing (row → shard 0). Unsupported
/// column types fall back to reading via [`column_scalar`] → [`ScopeKey::from_value`]
/// so they still shard deterministically.
pub(crate) fn scope_key_from_column(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> Option<ScopeKey> {
    use arrow::datatypes::{DataType, TimeUnit};
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .map(|a| ScopeKey::Int(a.value(row))),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .map(|a| ScopeKey::Int(a.value(row))),
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .map(|a| a.value(row));
            v.map(|f| ScopeKey::from_value(&Value::Number(f)))
        }
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .map(|a| ScopeKey::Str(a.value(row).into())),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .map(|a| ScopeKey::Str(if a.value(row) { "true" } else { "false" }.into())),
        _ => column_scalar(batch, col_idx, row).map(|v| ScopeKey::from_value(&v)),
    }
}

/// Build a [`ScopeKey`] for a row's match-key fields, in plan field order. `None`
/// iff any key column is null / missing (row lands shard 0).
pub(crate) fn scope_key_columnar(
    batch: &RecordBatch,
    col_idx: &[usize],
    row: usize,
) -> Option<ScopeKey> {
    let mut acc: Option<ScopeKey> = None;
    for &ci in col_idx {
        let v = scope_key_from_column(batch, ci, row)?;
        acc = Some(match acc {
            None => v,
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(v)),
        });
    }
    Some(acc.unwrap_or(ScopeKey::Empty))
}

/// Partition a batch's rows by the match key into per-shard row-index subsets,
/// so a sharded rule can be fed the raw batch + a row subset (zero per-event
/// materialization) instead of a fully materialized `Vec<Arc<Event>>`.
///
/// Byte-identical partition to the row-based [`sharded_sends`] via the shared
/// [`ScopeKey`] canonicalization: both build a typed key from the source value
/// and hash it with [`scope_key_shard_index`]. A row whose key column is missing
/// / null / absent from the schema lands on shard 0, exactly like the row-based
/// missing-key fallback.
/// Returns `None` when a key field is absent from the whole schema (then every
/// row is missing → all shard 0).
fn partition_rows_by_key(
    batch: &RecordBatch,
    keys: &[FieldRef],
    shard_count: usize,
) -> Option<Vec<Vec<u32>>> {
    // Resolve each key field to its batch column index once (schema immutable).
    let col_idx: Vec<usize> = keys
        .iter()
        .map(field_ref_name)
        .map(|name| batch.schema().index_of(name).ok())
        .collect::<Option<_>>()?;
    let mut per: Vec<Vec<u32>> = (0..shard_count).map(|_| Vec::new()).collect();
    for row in 0..batch.num_rows() {
        // Missing key (any key column null/absent) → shard 0, same as the
        // row-based fallback.
        let idx = scope_key_columnar(batch, &col_idx, row)
            .map(|key| scope_key_shard_index(&key, shard_count))
            .unwrap_or(0);
        per[idx].push(row as u32);
    }
    Some(per)
}

/// 统一的分片入口（issue #80）：表达式键规则（`spec.has_exprs()`）逐行求值
/// 分片——`ColumnarEvent::new(batch, row)` 把行变成 `FieldSource`，经
/// [`extract_scope_key_mixed`]（与机器内 advance 同构的键构建）得到 `ScopeKey`
/// 后哈希；求值失败/缺字段行 → shard 0（机器在 advance 再按 key 缺失跳过，
/// 不丢行）。表达式分片**永不**因「键列缺失」返回 `None`（表达式键不看列名）。
///
/// 纯字段规则走 [`partition_rows_by_key`] 列直读快路径（保持原行为：整 schema
/// 缺键列 → `None`，调用方回退全 shard 0）。
fn partition_rows(
    batch: &RecordBatch,
    spec: &ShardKeySpec,
    shard_count: usize,
) -> Option<Vec<Vec<u32>>> {
    if !spec.has_exprs() {
        return partition_rows_by_key(batch, spec.keys.as_ref(), shard_count);
    }
    debug_assert_eq!(
        spec.keys.len(),
        spec.key_exprs.len(),
        "keys 与 key_exprs 必须逐位对齐"
    );
    // 批级字段名→列索引提升一次（review：ColumnarEvent::new 无 index 时每次
    // field_value 线性扫 schema，逐行循环会把它放大成 O(rows × cols)）。
    let index = crate::match_engine::build_field_index(batch);
    let mut per: Vec<Vec<u32>> = (0..shard_count).map(|_| Vec::new()).collect();
    for row in 0..batch.num_rows() {
        let ce = ColumnarEvent::with_index(batch, row, Arc::clone(&index));
        let idx = extract_scope_key_mixed(&ce, spec.keys.as_ref(), spec.key_exprs.as_ref(), "")
            .map(|key| scope_key_shard_index(&key, shard_count))
            .unwrap_or(0);
        per[idx].push(row as u32);
    }
    Some(per)
}

/// Partition a batch by match key and push one send future per non-empty
/// shard into `sends`. Awaits full shard channels via the caller's join
/// (backpressure). `spec.has_exprs()` 时逐事件表达式求值（与 [`partition_rows`]
/// 列式逐行同一哈希 → 同 key 同 shard）。
fn sharded_sends(
    shards: &[mpsc::Sender<RulePush>],
    spec: &ShardKeySpec,
    window_name: &Arc<str>,
    events: &Arc<Vec<Arc<Event>>>,
    seq: u64,
    sends: &mut Vec<Pin<Box<dyn Future<Output = bool> + Send>>>,
) {
    let n = shards.len();
    let mut sub_batches: Vec<Vec<Arc<Event>>> = (0..n).map(|_| Vec::new()).collect();
    for event in events.iter() {
        // Missing key / 求值失败 → shard 0; the rule's state machine skips it anyway.
        let idx = if spec.has_exprs() {
            extract_scope_key_mixed(
                event.as_ref(),
                spec.keys.as_ref(),
                spec.key_exprs.as_ref(),
                "",
            )
            .map(|key| scope_key_shard_index(&key, n))
            .unwrap_or(0)
        } else {
            extract_key_simple(event.as_ref(), spec.keys.as_ref())
                .map(|scope_key| scope_key_shard_index(&scope_key_from_values(&scope_key), n))
                .unwrap_or(0)
        };
        sub_batches[idx].push(Arc::clone(event));
    }

    for (i, sub) in sub_batches.into_iter().enumerate() {
        if sub.is_empty() {
            continue;
        }
        let push = RulePush {
            window_name: Arc::clone(window_name),
            events: Some(Arc::new(sub)),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq,
        };
        let tx = shards[i].clone();
        sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_engine::{EngineHashMap, Value};

    fn event(id: &str) -> Event {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), Value::Str(id.into()));
        Event { fields }
    }

    fn keys() -> Vec<FieldRef> {
        vec![FieldRef::Simple("id".into())]
    }

    /// 窗口分片冲突检测（2026-08-29 q11/q6 多规则根因）：window_sharding 是每
    /// 窗口单一 (keys) 配置（覆盖式 insert），多规则不同 key 分片同一窗口互相
    /// 覆盖 → 后注册者必须回退单 worker。同 keys 不算冲突（共享分片）。
    #[test]
    fn window_sharding_conflicts_detects_key_mismatch() {
        let fanout = RuleFanout::new();
        let k_bidder = [FieldRef::Simple("bidder".into())];
        let k_auction = [FieldRef::Simple("auction".into())];

        // 未注册 → 不冲突。
        assert!(
            !fanout.window_sharding_conflicts("bid_events", &k_bidder),
            "未注册窗口不冲突"
        );

        // 注册 bidder 分片。
        fanout.register_window_sharding("bid_events", Arc::from(k_bidder.as_slice()), 10);
        assert!(
            !fanout.window_sharding_conflicts("bid_events", &k_bidder),
            "同 keys（q11/q12 都按 bidder）不冲突，共享分片"
        );
        assert!(
            fanout.window_sharding_conflicts("bid_events", &k_auction),
            "不同 keys（q5/q7 按 auction）冲突 → 后注册者回退单 worker"
        );
        assert!(
            fanout.window_sharding_conflicts("bid_events", &[]),
            "空 keys（stats index 分区）与已有 key 分片同样冲突"
        );

        // 不同窗口互不影响。
        assert!(
            !fanout.window_sharding_conflicts("auction_events", &k_auction),
            "其它窗口独立"
        );
    }

    /// `round_robin_only` 驱动中间窗广播裁剪（2026-08-25 q13 分片内存）：
    /// - 无订阅 / 只有 RoundRobin 订阅 → true（生产者可跳过 events 物化，
    ///   batch-only 广播）
    /// - 存在 Single / Sharded / 混合订阅 → false（row-path 中间窗消费者
    ///   依赖 `RulePush::events`，必须保留）
    #[test]
    fn round_robin_only_classifies_subscriptions() {
        let fanout = RuleFanout::new();
        let (tx, _rx) = mpsc::channel::<RulePush>(8);
        let (tx2, _rx2) = mpsc::channel::<RulePush>(8);
        let (tx3, _rx3) = mpsc::channel::<RulePush>(8);

        // 未注册窗口 → true（广播无订阅者，物化 events 是纯浪费）。
        assert!(fanout.round_robin_only("unregistered"));

        // 只有 Single 订阅 → false（row-path 契约需要 events）。
        fanout.register("win_single", tx.clone());
        assert!(!fanout.round_robin_only("win_single"));

        // 只有 Sharded 订阅 → false。
        fanout.register_sharded("win_sharded", vec![tx2.clone()], Arc::from(keys()));
        assert!(!fanout.round_robin_only("win_sharded"));

        // 只有 RoundRobin 订阅 → true（列式安全，batch-only 广播）。
        fanout.register_round_robin("win_rr", vec![tx3.clone()]);
        assert!(fanout.round_robin_only("win_rr"));

        // 混合：RoundRobin + Single → false（任一 row-path 消费者都需要 events）。
        fanout.register("win_mixed", tx.clone());
        fanout.register_round_robin("win_mixed", vec![tx.clone()]);
        assert!(!fanout.round_robin_only("win_mixed"));
    }

    #[tokio::test]
    async fn broadcast_delivers_same_arc_to_registered_channels() {
        let fanout = RuleFanout::new();
        let (tx, mut rx) = mpsc::channel(8);
        fanout.register("win_a", tx);

        let events: Arc<Vec<Arc<Event>>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events, 0).await;

        let push = rx
            .try_recv()
            .expect("registered channel should receive a push");
        assert_eq!(&*push.window_name, "win_a");
        assert!(
            push.events
                .as_ref()
                .is_some_and(|e| Arc::ptr_eq(e, &events)),
            "should share the same Arc"
        );
    }

    #[tokio::test]
    async fn broadcast_prunes_closed_channels() {
        let fanout = RuleFanout::new();
        let (tx, rx) = mpsc::channel(8);
        fanout.register("win_a", tx);
        drop(rx); // close the channel

        let events: Arc<Vec<Arc<Event>>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events, 0).await;

        let table = fanout.table.read().expect("fanout lock poisoned");
        assert!(
            !table.contains_key("win_a"),
            "closed channel should be pruned on broadcast"
        );
    }

    #[tokio::test]
    async fn sharded_broadcast_partitions_by_key_and_routes_same_key_together() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0, tx1],
            Arc::from(keys().into_boxed_slice()),
        );

        // Two distinct keys; each should land on a single (deterministic) shard.
        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
            Arc::new(event("k1")),
            Arc::new(event("k2")),
            Arc::new(event("k1")),
        ]);
        fanout.broadcast("win_a", &events, 0).await;

        let mut received = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            received.extend(
                push.events
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| e.fields["id"].clone()),
            );
        }
        while let Ok(push) = rx1.try_recv() {
            received.extend(
                push.events
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| e.fields["id"].clone()),
            );
        }

        // Union of the shards == the original batch (no loss, no dup).
        let mut ids: Vec<String> = received
            .into_iter()
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                _ => panic!("expected str"),
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["k1", "k1", "k2"]);

        // Same key (`k1`) must land on the SAME shard across broadcasts.
        let idx = scope_key_shard_index(&ScopeKey::Str("k1".into()), 2);
        let again: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("k1"))]);
        fanout.broadcast("win_a", &again, 1).await;
        let got0 = rx0
            .try_recv()
            .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0))
            .unwrap_or(0);
        let got1 = rx1
            .try_recv()
            .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0))
            .unwrap_or(0);
        if idx == 0 {
            assert_eq!(got0, 1);
            assert_eq!(got1, 0);
        } else {
            assert_eq!(got0, 0);
            assert_eq!(got1, 1);
        }
    }

    #[test]
    fn scope_key_shard_index_is_deterministic_and_in_range() {
        let n = 4;
        for id in ["a", "b", "c", "same", "same"] {
            let idx = scope_key_shard_index(&ScopeKey::Str(id.into()), n);
            assert!(idx < n);
        }
        // Same key → same index, across repeated calls.
        assert_eq!(
            scope_key_shard_index(&ScopeKey::Str("same".into()), n),
            scope_key_shard_index(&ScopeKey::Str("same".into()), n)
        );
    }

    #[test]
    fn scope_key_shard_index_single_shard_is_zero() {
        assert_eq!(
            scope_key_shard_index(&ScopeKey::Str("anything".into()), 1),
            0
        );
    }

    #[tokio::test]
    async fn round_robin_broadcast_delivers_whole_batches_and_shares_arcs() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_round_robin("win_rr", vec![tx0, tx1]);

        // Four distinct batches; round-robin must send each WHOLE batch (same
        // Arc) to alternating workers with no loss / no duplication.
        let mut sent = Vec::new();
        for i in 0..4 {
            let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
                Arc::new(event(&format!("e{i}a"))),
                Arc::new(event(&format!("e{i}b"))),
            ]);
            sent.push(Arc::clone(&events));
            fanout.broadcast("win_rr", &events, 0).await;
        }

        let mut got0 = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            got0.push(push);
        }
        let mut got1 = Vec::new();
        while let Ok(push) = rx1.try_recv() {
            got1.push(push);
        }

        // Exactly one worker per batch, alternating.
        assert_eq!(got0.len(), 2, "worker 0 receives 2 batches");
        assert_eq!(got1.len(), 2, "worker 1 receives 2 batches");
        // Whole batch preserved: 2 events per delivered push, same Arc as sent.
        let all: Vec<&RulePush> = got0.iter().chain(got1.iter()).collect();
        assert!(
            all.iter()
                .all(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0) == 2)
        );
        for push in &all {
            assert!(
                sent.iter()
                    .any(|s| push.events.as_ref().is_some_and(|e| Arc::ptr_eq(s, e))),
                "delivered batch must be one of the sent Arcs (zero copy)"
            );
        }
        assert_eq!(&*all[0].window_name, "win_rr");
    }

    #[tokio::test]
    async fn round_robin_broadcast_prunes_closed_shards() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, rx1) = mpsc::channel(8);
        fanout.register_round_robin("win_rr2", vec![tx0, tx1]);
        drop(rx1); // worker 1 shut down

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("x"))]);
        // Broadcast enough times to hit the closed shard and trigger pruning.
        for _ in 0..2 {
            fanout.broadcast("win_rr2", &events, 0).await;
        }

        // Surviving shard still receives (at least) one delivery.
        let mut delivered = 0;
        while rx0.try_recv().is_ok() {
            delivered += 1;
        }
        assert!(delivered >= 1, "open shard must still receive batches");

        let table = fanout.table.read().expect("fanout lock poisoned");
        let subs = table.get("win_rr2").expect("subscription survives");
        assert!(
            !subs.is_empty(),
            "subscription with one open shard must not be pruned entirely"
        );
    }

    /// P1-② regression: a full (slow-consumer) channel must not head-of-line
    /// block the deliveries to the *other* subscriptions of the same window.
    /// The sends run concurrently, so the fast subscriber receives its copy
    /// immediately even while the slow one's send is still parked.
    #[tokio::test]
    async fn slow_consumer_does_not_block_other_subscribers() {
        let fanout = RuleFanout::new();
        // Slow consumer: capacity 1, never recv'd → its send parks after the
        // first broadcast fills the channel.
        let (slow_tx, _slow_rx_keep) = mpsc::channel::<RulePush>(1);
        // Fast consumer: capacity 8, drained immediately.
        let (fast_tx, mut fast_rx) = mpsc::channel::<RulePush>(8);
        fanout.register("win_hol", slow_tx);
        fanout.register("win_hol", fast_tx);

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("e1"))]);
        fanout.broadcast("win_hol", &events, 0).await;

        // Second broadcast: the slow channel is full and would block a serial
        // send loop before the fast subscriber's delivery. Drive the
        // broadcast future concurrently with the fast recv — the broadcast
        // stays parked on the slow channel, the fast delivery must not wait.
        let events2: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("e2"))]);
        let broadcast = fanout.broadcast("win_hol", &events2, 1);
        tokio::pin!(broadcast);
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            tokio::select! {
                biased;
                r = fast_rx.recv() => r,
                // If the broadcast ever completes while the slow channel is
                // still full and undrained, something is wrong; ignore and
                // let the outer timeout fail the test.
                _ = &mut broadcast => fast_rx.try_recv().ok(),
            }
        })
        .await
        .expect("fast subscriber must receive within timeout")
        .expect("fast channel open");
        assert_eq!(
            got.events.as_ref().map(|e| e.len()).unwrap_or(0),
            1,
            "fast subscriber got the second batch"
        );
    }

    /// Same property for sharded subscriptions: a full shard must not block
    /// the other shards' deliveries of the same broadcast.
    #[tokio::test]
    async fn slow_shard_does_not_block_other_shards() {
        let fanout = RuleFanout::new();
        let (slow_tx, _slow_rx_keep) = mpsc::channel::<RulePush>(1);
        let (fast_tx, mut fast_rx) = mpsc::channel::<RulePush>(8);
        // Two shards partitioned by "id"; keys k1/k2 deterministically split.
        fanout.register_sharded(
            "win_sh",
            vec![slow_tx, fast_tx],
            Arc::from(keys().into_boxed_slice()),
        );

        let idx_k1 = scope_key_shard_index(&ScopeKey::Str("k1".into()), 2);
        let (slow_key, fast_key) = if idx_k1 == 0 {
            ("k1", "k2")
        } else {
            ("k2", "k1")
        };

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event(slow_key))]);
        fanout.broadcast("win_sh", &events, 0).await;

        // Second broadcast: the slow shard's channel is full; the fast shard
        // must still receive its sub-batch without waiting for it. Drive the
        // broadcast future concurrently with the fast shard's recv.
        let events2: Arc<Vec<Arc<Event>>> =
            Arc::new(vec![Arc::new(event(slow_key)), Arc::new(event(fast_key))]);
        let broadcast = fanout.broadcast("win_sh", &events2, 1);
        tokio::pin!(broadcast);
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            tokio::select! {
                biased;
                r = fast_rx.recv() => r,
                _ = &mut broadcast => fast_rx.try_recv().ok(),
            }
        })
        .await
        .expect("fast shard must receive within timeout")
        .expect("fast shard channel open");
        assert_eq!(
            got.events.as_ref().map(|e| e.len()).unwrap_or(0),
            1,
            "fast shard got its sub-batch"
        );
    }

    #[test]
    fn partition_rows_matches_row_based_per_row() {
        // 列式分片（partition_rows_by_key，从 batch 列读 key）必须与行式分片
        // （batch_to_events + extract_key_simple + shard_index）逐行落在同一
        // shard —— Q2 键闭包 + 有状态安全的基础。含 null / UTF8 / 多行 key。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("k1"),
                Some("k2"),
                None, // null key → both should land shard 0
                Some("k3"),
                Some("k1"),
            ])) as ArrayRef],
        )
        .unwrap();

        let keys = vec![FieldRef::Simple("id".into())];
        let shards = 3usize;

        // 列式：每行 → shard
        let per = partition_rows_by_key(&batch, &keys, shards).expect("key col present");
        let col_shard = |row: usize| -> usize {
            per.iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };

        // 行式：每行物化 Event → extract_key_simple → ScopeKey → scope_key_shard_index
        let events = batch_to_events(&batch);
        let row_shard = |row: usize| -> usize {
            extract_key_simple(&events[row], &keys)
                .map(|sk| scope_key_shard_index(&scope_key_from_values(&sk), shards))
                .unwrap_or(0)
        };

        assert_eq!(batch.num_rows(), 5);
        for row in 0..batch.num_rows() {
            assert_eq!(
                col_shard(row),
                row_shard(row),
                "row {row} landed on different shard (columnar vs row-based)"
            );
        }

        // 无丢失、无重复：并集覆盖全部 5 行
        let flat: Vec<u32> = per.iter().flatten().copied().collect();
        let mut flat = flat;
        flat.sort_unstable();
        assert_eq!(flat, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn precompute_shard_rows_equals_partition_rows_by_key() {
        // 方案 A：`precompute_shard_rows`（并行 parse 阶段，读 fanout 的 sharded
        // keys/shard_count）产出的分片，必须与广播内部所用的
        // `partition_rows_by_key` 逐 shard 完全一致（否则提前分片会改变
        // 命中行落子，破坏有状态语义）。逐 shard 比较行子集（含排序后相等）。
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                None,
                Some(1),
            ])) as ArrayRef],
        )
        .unwrap();

        let fanout = RuleFanout::new();
        let shard_count = 3usize;
        let (txs, _rxs): (Vec<_>, Vec<_>) = (0..shard_count)
            .map(|_| mpsc::channel::<RulePush>(8))
            .unzip();
        let keys: Arc<[FieldRef]> =
            Arc::from(vec![FieldRef::Simple("auction".into())].into_boxed_slice());
        fanout.register_sharded("win_p", txs, keys.clone());

        let pre = fanout
            .precompute_shard_rows("win_p", &batch)
            .expect("sharded window");
        let internal = partition_rows_by_key(&batch, &keys, shard_count).expect("key col present");
        assert_eq!(pre.len(), internal.len(), "same shard count");
        for i in 0..shard_count {
            let mut a = pre[i].clone();
            let mut b = internal[i].clone();
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "precompute shard {i} differs from internal partition");
        }
    }

    #[test]
    fn unsharded_precompute_shard_rows_returns_none() {
        // 无 sharded 订阅的窗口：`precompute_shard_rows` 返回 None（不该分片），
        // 广播走原路径。
        use arrow::array::ArrayRef;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let fanout = RuleFanout::new();
        let (tx, _rx) = mpsc::channel::<RulePush>(8);
        fanout.register("win_s", tx);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![Some(1)])) as ArrayRef],
        )
        .unwrap();
        assert!(fanout.precompute_shard_rows("win_s", &batch).is_none());
        assert!(fanout.precompute_shard_rows("missing", &batch).is_none());
    }

    #[test]
    fn scope_key_columnar_matches_row_based() {
        // 2b 对拍：`scope_key_columnar`（从列直读原生值）必须与行式
        // `scope_key_from_values(extract_key_simple)` 逐行构造出 **同一个**
        // `ScopeKey`（相等）——覆盖 Utf8、null（→ 缺失 → shard 0）、Int64
        // <2^53、多列 key。
        // 注：>2^53 的 Int64 行式走 f64 丢精度（`Value::Number(v as f64)`），
        // 与列式精确 i64 是已知语义分歧（既有 extract_field_value 行为），此
        // 测试锁 <2^53 一致 + 断言 >2^53 分歧方向。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("k1"),
                    Some("k2"),
                    None,
                    Some("k3"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(9007199254740993), // 2^53+1
                    Some(-3),
                    None,
                ])),
            ],
        )
        .unwrap();

        let keys = vec![FieldRef::Simple("id".into()), FieldRef::Simple("n".into())];
        let col_idx: Vec<usize> = keys
            .iter()
            .map(field_ref_name)
            .map(|name| batch.schema().index_of(name).unwrap())
            .collect();
        let events = batch_to_events(&batch);

        assert_eq!(batch.num_rows(), 4);
        // 2^53+1 是唯一的分歧 lane（行式 f64 丢精度），其余必须逐行相等。
        for (row, event) in events.iter().enumerate() {
            let col = scope_key_columnar(&batch, &col_idx, row);
            let rw = extract_key_simple(event, &keys).map(|sk| scope_key_from_values(&sk));
            if row == 1 {
                // >2^53：列式 Int(2^53+1) vs 行式 f64 舍入 → 分歧（已知语义）。
                assert!(
                    col != rw,
                    "row {row} 2^53+1 columnar vs row-based should differ (f64 loss)"
                );
                continue;
            }
            assert_eq!(
                col, rw,
                "row {row}: columnar ScopeKey {:?} != row-based ScopeKey {:?}",
                col, rw
            );
        }
    }

    #[tokio::test]
    async fn broadcast_batch_only_sharded_sends_row_subsets() {
        // 列式 sharded 广播（broadcast_batch_only，events=None + batch）：
        // 每个 shard 收到 events=None + batch:Some + shard_rows:Some(本 shard 行子集),
        // 且各 shard 行子集并集 = 全批（不丢、不重）。
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("k1"),
                Some("k2"),
                Some("k1"),
                Some("k3"),
            ])) as ArrayRef],
        )
        .unwrap();

        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0, tx1],
            Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        );

        // (a) Defensive fallback path: no precomputed shard_rows → actor
        // repartitions internally.
        fanout
            .broadcast_batch_only("win_a", &batch, None, None, 0)
            .await;

        let drain = |rx0: &mut mpsc::Receiver<RulePush>, rx1: &mut mpsc::Receiver<RulePush>| {
            let mut seen: Vec<u32> = Vec::new();
            let mut pushed = 0;
            for rx in [rx0, rx1] {
                while let Ok(p) = rx.try_recv() {
                    pushed += 1;
                    assert!(
                        p.events.is_none(),
                        "deferred sharded push must carry no events"
                    );
                    assert!(
                        p.batch.is_some(),
                        "deferred sharded push must carry the batch"
                    );
                    let rows = p.shard_rows.expect("shard_rows set");
                    seen.extend(rows.iter().copied());
                }
            }
            // 非空 shard 各收到一个 push；k3 若单独一个 shard 也各一个。
            assert!((1..=2).contains(&pushed));
            // 并集 = 全批 4 行，无重复。
            seen.sort_unstable();
            assert_eq!(seen, vec![0, 1, 2, 3]);
        };
        drain(&mut rx0, &mut rx1);

        // (b) Parse-side precomputed path: `precompute_shard_rows` (parallel parse
        // stage) must produce a partition that, handed to the broadcast, routes
        // each row to the *same* shard and covers the batch exactly once.
        let pre = fanout
            .precompute_shard_rows("win_a", &batch)
            .expect("sharded");
        let (tx0b, mut rx0b) = mpsc::channel(8);
        let (tx1b, mut rx1b) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0b, tx1b],
            Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        );
        fanout
            .broadcast_batch_only("win_a", &batch, None, Some(pre.as_ref()), 0)
            .await;
        drain(&mut rx0b, &mut rx1b);

        // (c) Defensive fallback on config drift: a precomputed `shard_rows` whose
        // length does not match the live subscription's shard count must be
        // ignored and the full batch repartitioned internally (never drops rows).
        let (tx0c, mut rx0c) = mpsc::channel(8);
        let (tx1c, mut rx1c) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0c, tx1c],
            Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        );
        let stale: Arc<[Vec<u32>]> = Arc::from(vec![vec![0, 1, 2, 3], vec![], vec![], vec![]]); // len 4 != 2 shards
        fanout
            .broadcast_batch_only("win_a", &batch, None, Some(stale.as_ref()), 0)
            .await;
        drain(&mut rx0c, &mut rx1c);
    }

    /// `precompute_shard_rows` is the parse-stage hot path for sharded pull
    /// windows (q5's `bid_events`, ~100k rows/batch partitioned by `auction`).
    /// If it is slow, uneven parse workers delay a batch's `seq`, the actor's
    /// out-of-order `pending` map accumulates, and the append tail never catches
    /// up — the q5 pull-freeze signature. This measures the partition cost as a
    /// diagnostic baseline.
    #[test]
    fn precompute_shard_rows_throughput_is_bounded() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::time::{Duration, Instant};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            false,
        )]));
        let values: Vec<i64> = (0..100_000).map(|i| i % 1024).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();

        let fanout = RuleFanout::new();
        fanout.register_window_sharding(
            "win",
            Arc::from(vec![FieldRef::Simple("auction".into())].into_boxed_slice()),
            10,
        );

        // Warm up (allocations, first-hash).
        let _ = fanout.precompute_shard_rows("win", &batch);

        let n = 100u32;
        let t0 = Instant::now();
        for _ in 0..n {
            let rows = fanout
                .precompute_shard_rows("win", &batch)
                .expect("sharded window must partition");
            assert_eq!(rows.len(), 10);
        }
        let per = t0.elapsed() / n;
        assert!(
            per < Duration::from_millis(200),
            "precompute_shard_rows 100k rows took {per:?}; it is a parse bottleneck"
        );
    }
    /// `queued_items`（2026-08-26 输出链在途量）：报（排队批数, 总容量）。
    ///
    /// 为何需要：diag 墙梯把 q13 的 12.5GB 内存增量定位到**输出链**，而窗口会计只
    /// 解释 4.1GB；规则分片通道（10 分片 × 256 槽）是该段唯一未度量的大容器。
    /// 若该 API 静默失效（恒 0），"通道是否为持有者"就无法判定。
    #[tokio::test]
    async fn queued_items_reports_backlog_across_shards() {
        let fanout = RuleFanout::new();
        assert!(
            fanout.queued_items("nope").is_none(),
            "未注册窗口返回 None（区分'无订阅'与'空队'）"
        );

        // 两个分片，各容量 4 → 总容量 8、初始排队 0。
        let (tx1, mut rx1) = mpsc::channel::<RulePush>(4);
        let (tx2, _rx2) = mpsc::channel::<RulePush>(4);
        fanout.register_round_robin("w", vec![tx1.clone(), tx2.clone()]);
        assert_eq!(fanout.queued_items("w"), Some((0, 8)), "空队 = (0, 8)");

        // 往分片 1 压 3 条（不消费）→ 排队 3。
        let mk = || RulePush {
            window_name: "w".into(),
            events: None,
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: 0,
        };
        for _ in 0..3 {
            tx1.send(mk()).await.unwrap();
        }
        assert_eq!(
            fanout.queued_items("w"),
            Some((3, 8)),
            "压入 3 条未消费 → 排队须为 3（这是判断通道是否接近满的依据）"
        );

        // 消费 2 条 → 排队回落到 1（否则会把已消费的算成在途，虚增分账）。
        rx1.recv().await.unwrap();
        rx1.recv().await.unwrap();
        assert_eq!(fanout.queued_items("w"), Some((1, 8)), "消费后排队须回落");
    }

    // =========================================================================
    // issue #80 — 表达式派生 key 分片（fanout 逐行求值）
    // =========================================================================

    /// `concat(src, ":", dst)` 表达式键规格：keys 保留逻辑名 pair，槽位存表达式。
    fn pair_expr_spec() -> ShardKeySpec {
        use wf_lang::ast::Expr;
        ShardKeySpec {
            keys: Arc::from(vec![FieldRef::Simple("pair".into())].into_boxed_slice()),
            key_exprs: Arc::from(
                vec![Some(Expr::FuncCall {
                    qualifier: None,
                    name: "concat".into(),
                    args: vec![
                        Expr::Field(FieldRef::Qualified("s".into(), "src".into())),
                        Expr::StringLit(":".into()),
                        Expr::Field(FieldRef::Qualified("s".into(), "dst".into())),
                    ],
                })]
                .into_boxed_slice(),
            ),
        }
    }

    /// src/dst UTF8 批：rows = [a:b, a:b, c:d, 缺 src, a:b, 缺 dst]。
    fn expr_batch() -> RecordBatch {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, true),
            Field::new("dst", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("c"),
                    None, // 缺 src → concat None
                    Some("a"),
                    Some("x"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("b"),
                    Some("d"),
                    Some("e"),
                    Some("b"),
                    None, // 缺 dst → concat None
                ])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn expr_partition_rows_matches_row_based_per_row() {
        // 列式表达式分片必须与行式（batch_to_events + extract_scope_key_mixed）
        // 逐行落在同一 shard：缺 src/dst（求值 None）→ 双方都 shard 0。
        use crate::match_engine::batch_to_events;
        let batch = expr_batch();
        let spec = pair_expr_spec();
        let shards = 3usize;

        let per = partition_rows(&batch, &spec, shards).expect("expr 分片永不 None");
        let col_shard = |row: usize| -> usize {
            per.iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };
        let events = batch_to_events(&batch);
        let row_shard = |row: usize| -> usize {
            extract_scope_key_mixed(
                &events[row],
                spec.keys.as_ref(),
                spec.key_exprs.as_ref(),
                "",
            )
            .map(|key| scope_key_shard_index(&key, shards))
            .unwrap_or(0)
        };
        for row in 0..batch.num_rows() {
            assert_eq!(
                col_shard(row),
                row_shard(row),
                "row {row}: 列式表达式分片与行式不一致"
            );
        }
        // 无丢失/重复：覆盖全部 6 行。
        let mut flat: Vec<u32> = per.iter().flatten().copied().collect();
        flat.sort_unstable();
        assert_eq!(flat, vec![0, 1, 2, 3, 4, 5]);
        // 同派生值 a:b 必须同片（行 0/1/4）。
        let s = col_shard(0);
        assert_eq!(col_shard(1), s);
        assert_eq!(col_shard(4), s);
    }

    #[test]
    fn expr_partition_rows_equals_precomputed_field_column() {
        // 最强正确性锁：表达式分片 == 「上游预计算 pair 列 + 纯字段分片」。
        // 同一批行的派生键分片与直接按 pair 列分片逐 shard 一致。
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let expr_b = expr_batch();
        // 预计算列版本：第 3 行 src 缺 → pair = "none"（避免与 expr None 的
        // 缺字段行分片位不同：缺 src/dst 行在 expr 侧求值 None → shard0，
        // 预计算侧若给非空 pair 会落别的片——故对照只在两批**都有值**的行上做）。
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, true),
            Field::new("dst", DataType::Utf8, true),
            Field::new("pair", DataType::Utf8, true),
        ]));
        let pair_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("c"),
                    None,
                    Some("a"),
                    Some("x"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("b"),
                    Some("d"),
                    Some("e"),
                    Some("b"),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("a:b"),
                    Some("a:b"),
                    Some("c:d"),
                    None, // 与 expr 侧 None（shard0）对应
                    Some("a:b"),
                    None, // 同上
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let spec = pair_expr_spec();
        let shards = 3usize;
        let expr_per = partition_rows(&expr_b, &spec, shards).expect("expr partition");
        let field_per =
            partition_rows_by_key(&pair_batch, &[FieldRef::Simple("pair".into())], shards)
                .expect("pair column present");
        let expr_shard = |row: usize| -> usize {
            expr_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };
        let field_shard = |row: usize| -> usize {
            field_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };
        for row in 0..6 {
            assert_eq!(
                expr_shard(row),
                field_shard(row),
                "row {row}: 表达式分片与预计算列分片不一致"
            );
        }
    }

    #[test]
    fn precompute_shard_rows_equals_partition_rows_expr() {
        // pull 模型注册（with_exprs）后 parse 预计算分片 == 广播内部 partition_rows。
        let fanout = RuleFanout::new();
        let batch = expr_batch();
        let spec = pair_expr_spec();
        let (txs, _rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| mpsc::channel::<RulePush>(8)).unzip();
        fanout.register_sharded_with_exprs("win_e", txs, spec.clone());

        let pre = fanout
            .precompute_shard_rows("win_e", &batch)
            .expect("sharded window");
        let internal = partition_rows(&batch, &spec, 3).expect("expr partition");
        assert_eq!(pre.len(), internal.len());
        for i in 0..3 {
            assert_eq!(pre[i].as_ref() as &[u32], internal[i].as_slice());
        }
    }

    #[tokio::test]
    async fn expr_sharded_broadcast_routes_same_key_together() {
        // push 模式：表达式键广播后，同派生 key 的事件必须到同一分片通道。
        use crate::match_engine::batch_to_events;
        let fanout = RuleFanout::new();
        let batch = expr_batch();
        let events: Arc<Vec<Arc<Event>>> =
            Arc::new(batch_to_events(&batch).into_iter().map(Arc::new).collect());
        let (tx0, mut rx0) = mpsc::channel::<RulePush>(16);
        let (_tx1, _rx1) = mpsc::channel::<RulePush>(16);
        fanout.register_sharded_with_exprs("win_b", vec![tx0, _tx1], pair_expr_spec());

        // 每行事件 → 预期 shard（与 fanout 同构计算）。
        let spec = pair_expr_spec();
        let row_shard = |event: &Event| -> usize {
            extract_scope_key_mixed(event, spec.keys.as_ref(), spec.key_exprs.as_ref(), "")
                .map(|k| scope_key_shard_index(&k, 2))
                .unwrap_or(0)
        };
        // 广播只带事件（行式路径 sharded_sends）。
        fanout.broadcast("win_b", &events, 0).await;
        // 收通道 0 的全部：应只含 shard==0 的行（同派生 key 同片）。
        let mut got = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            if let Some(evs) = push.events {
                for e in evs.iter() {
                    got.push(row_shard(e));
                }
            }
        }
        assert!(!got.is_empty(), "shard 0 至少收到行");
        assert!(got.iter().all(|&s| s == 0), "通道 0 只应收到 shard0 的行");
        // 补验：a:b 三行若落在 shard1，则通道 0 为空时 shard1 应有全部——
        // 用实例覆盖断言：所有行要么全在 0 要么全在 1（按预期 shard 归并）。
        let expected_in_0: usize = (0..6)
            .map(|row| row_shard(&batch_to_events(&batch)[row]))
            .filter(|&s| s == 0)
            .count();
        assert_eq!(got.len(), expected_in_0, "通道 0 行数 = 预期 shard0 行数");
    }

    #[test]
    fn window_sharding_conflicts_accounts_expr_slots() {
        let fanout = RuleFanout::new();
        let keys: Arc<[FieldRef]> = Arc::from(vec![FieldRef::Simple("pair".into())]);
        let plain = ShardKeySpec::new(keys.clone());
        let expr = pair_expr_spec();
        // 同 keys、一方带表达式 → 冲突（分区方式不同）。
        fanout.register_window_sharding_with_exprs("w", plain.clone(), 4);
        assert!(
            fanout.window_sharding_conflicts_with_exprs("w", &expr),
            "expr 与纯字段分区方式不同 → 必须判冲突"
        );
        // 相同 spec 再注册 → 不冲突（覆盖式同值，共享分片）。
        let fanout2 = RuleFanout::new();
        fanout2.register_window_sharding_with_exprs("w", expr.clone(), 4);
        assert!(!fanout2.window_sharding_conflicts_with_exprs("w", &expr));
        // keys-only 入口对已注册 expr spec 也判冲突。
        assert!(fanout2.window_sharding_conflicts("w", &[FieldRef::Simple("pair".into())]));
    }

    #[test]
    fn expr_numeric_partition_matches_precomputed_int_column() {
        // review 3：数值表达式键（Int64 相加）分片 == 预计算 Int64 列分片——
        // typed key（Int）在 fanout 层与列直读同构（数字不能因路径不同塌缩出
        // 不同 shard）。
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use wf_lang::ast::{BinOp, Expr};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let expr_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        // 预计算 sum 列版本（缺 a/b 的行 sum 也为 null，与 expr 侧 None→shard0 对应）。
        let sum_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("sum", DataType::Int64, true),
        ]));
        let sum_batch = RecordBatch::try_new(
            sum_schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                    None,
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(11),
                    Some(22),
                    Some(33),
                    None,
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let add = Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "a".into()))),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "b".into()))),
        };
        let spec = ShardKeySpec {
            keys: Arc::from(vec![FieldRef::Simple("sum".into())].into_boxed_slice()),
            key_exprs: Arc::from(vec![Some(add)].into_boxed_slice()),
        };
        let shards = 3usize;
        let expr_per = partition_rows(&expr_batch, &spec, shards).expect("expr partition");
        let field_per =
            partition_rows_by_key(&sum_batch, &[FieldRef::Simple("sum".into())], shards)
                .expect("sum column present");
        for row in 0..5 {
            let es = expr_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap();
            let fs = field_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap();
            assert_eq!(es, fs, "row {row}: 数值表达式分片与预计算 Int 列分片不一致");
        }
    }

    #[test]
    fn expr_mixed_key_partition_matches_row_based() {
        // review 3：混合键（普通字段位 None + 表达式位 Some）列式分片 == 行式
        // 逐行 extract_scope_key_mixed（None 槽按字段读、expr 槽按行求值）。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use wf_lang::ast::Expr;

        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Utf8, true),
            Field::new("port", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("x"),
                    Some("y"),
                    None,
                    Some("z"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        // 表达式槽：port + 10（读 Int64 列，null → None）。
        let plus = Expr::BinOp {
            op: wf_lang::ast::BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "port".into()))),
            right: Box::new(Expr::Number(10.0)),
        };
        let spec = ShardKeySpec {
            keys: Arc::from(
                vec![
                    FieldRef::Simple("grp".into()),
                    FieldRef::Simple("port_k".into()),
                ]
                .into_boxed_slice(),
            ),
            key_exprs: Arc::from(vec![None, Some(plus)].into_boxed_slice()),
        };
        let shards = 3usize;
        let per = partition_rows(&batch, &spec, shards).expect("expr partition");
        let events = batch_to_events(&batch);
        for (row, ev) in events.iter().enumerate() {
            let col_s = per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap();
            let row_s =
                extract_scope_key_mixed(ev, spec.keys.as_ref(), spec.key_exprs.as_ref(), "")
                    .map(|k| scope_key_shard_index(&k, shards))
                    .unwrap_or(0);
            assert_eq!(col_s, row_s, "row {row}: 混合键列式与行式分片不一致");
        }
    }

    /// 100k 行 src/dst UTF8 批（派生 key 值域 1024，避免字符串缓存/热点失真）。
    fn big_expr_batch(n: usize) -> RecordBatch {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, true),
            Field::new("dst", DataType::Utf8, true),
        ]));
        let src: Vec<String> = (0..n)
            .map(|i| format!("10.{}.{}.{}", (i / 65_536) % 256, (i / 256) % 256, i % 256))
            .collect();
        let dst: Vec<String> = (0..n).map(|i| format!("dst{}", i % 1024)).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(src)) as ArrayRef,
                Arc::new(StringArray::from(dst)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// `partition_rows` 表达式分片是 parse/broadcast 单 writer 路径上的逐行
    /// eval 热点（issue #80）——吞吐必须有界，防止逐行 eval 退化（review R1 的
    /// 批级 field index 提升是它的主要杠杆）。与纯字段版
    /// `precompute_shard_rows_throughput_is_bounded` 对称。
    #[test]
    fn expr_partition_rows_throughput_is_bounded() {
        use std::time::{Duration, Instant};
        let batch = big_expr_batch(100_000);
        let spec = pair_expr_spec();
        let _ = partition_rows(&batch, &spec, 8).expect("expr partition");

        let n = 10u32;
        let t0 = Instant::now();
        for _ in 0..n {
            let per = partition_rows(&batch, &spec, 8).expect("expr partition");
            assert_eq!(per.len(), 8);
        }
        let per = t0.elapsed() / n;
        // 预算 = 实测量级 ×3 余量（CI 抖动）；超限说明逐行求值路径出现量级退化。
        assert!(
            per < Duration::from_millis(900),
            "expr 列式分片 100k rows took {per:?}; 逐行 eval 路径异常"
        );
    }

    /// pull parse 预计算路径（`precompute_shard_rows` + 表达式 spec）同样有界。
    #[test]
    fn expr_precompute_shard_rows_throughput_is_bounded() {
        use std::time::{Duration, Instant};
        let batch = big_expr_batch(100_000);
        let fanout = RuleFanout::new();
        fanout.register_window_sharding_with_exprs("win", pair_expr_spec(), 10);
        let _ = fanout.precompute_shard_rows("win", &batch);

        let n = 10u32;
        let t0 = Instant::now();
        for _ in 0..n {
            let rows = fanout
                .precompute_shard_rows("win", &batch)
                .expect("sharded window must partition");
            assert_eq!(rows.len(), 10);
        }
        let per = t0.elapsed() / n;
        assert!(
            per < Duration::from_millis(900),
            "expr precompute_shard_rows 100k rows took {per:?}; parse 阶段逐行 eval 异常"
        );
    }
}
