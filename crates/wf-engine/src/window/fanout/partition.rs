//! fanout 分片行路由（rule_shards）：把批的行按匹配键（字段直读快路径或
//! 表达式键逐行求值，issue #80）分成片内行子集，并生成逐片 send future。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use wf_lang::ast::FieldRef;

use super::{RulePush, ShardKeySpec, scope_key_columnar};
use crate::match_engine::{
    ColumnarEvent, Event, extract_key_simple, extract_scope_key_mixed, field_ref_name,
    scope_key_from_values, scope_key_shard_index,
};

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
pub(super) fn partition_rows_by_key(
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
pub(super) fn partition_rows(
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
pub(super) fn sharded_sends(
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
