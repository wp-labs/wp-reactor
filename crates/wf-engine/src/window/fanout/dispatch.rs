//! fanout 分发器行为层（rule_shards）：`RuleFanout` 的注册 / 预分片 / 广播 / 剪枝
//! 实现，外加输入行索引分区（`partition_rows_by_index`, q15 空键 stats 输入分片）
//! 与并发 send 收口（`join_sends`）。
//!
//! 结构/字段定义留在 `super`（mod.rs 类型面）——`impl RuleFanout` 对父模块私有
//! 字段（`table`/`window_sharding`）读写：可见性只向下流，本子模块作为
//! `fanout::dispatch` 是父模块后代，字段/`Subscription`/`WindowShardPartition`
//! 均直接可达，无需提级或 re-export。

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use wf_lang::ast::FieldRef;

use super::partition::{partition_rows, sharded_sends};
use super::{RuleFanout, RulePush, ShardKeySpec, Subscription, WindowShardPartition};
use crate::match_engine::Event;

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
