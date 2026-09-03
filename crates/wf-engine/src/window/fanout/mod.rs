//! 规则-窗口订阅扇出与分片（rule_shards）：窗口订阅注册、读游标分批、把行
//! 子集分发给各规则 worker；分片键 = 简单字段取模或表达式派生键逐行求值哈希
//! （issue #80，见 partition 相关函数）。
//!
//! **文件组织**：本文件是 fanout 的 *类型 / 规格面*（`RulePush`/`ShardKeySpec`/
//! `Subscription`/`WindowShardPartition`/`RuleFanout` 结构定义；`impl RuleFanout`
//! 与测试要读的私有字段在此定义，子模块经 `super` 下溯可达）；分发行为在
//! `dispatch.rs`，列式 scope-key 直读在 `scope_key.rs`，单测在 `tests.rs`
//! （coverage 另见 `window/tests/`）。

mod dispatch;
mod partition;
mod scope_key;

#[cfg(test)]
mod tests;

pub(crate) use scope_key::{scope_key_columnar, scope_key_from_column};

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;
use wf_lang::ast::{Expr, FieldRef};

use crate::match_engine::Event;
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

#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.RuleFanout")]
pub struct RuleFanout {
    table: RwLock<HashMap<String, Vec<Subscription>>>,
    /// window_name → (match keys, shard count) for the key-partitioned
    /// subscription of that window, used by the pull model to precompute
    /// shard row subsets without a delivery channel.
    window_sharding: RwLock<HashMap<String, WindowShardPartition>>,
}
