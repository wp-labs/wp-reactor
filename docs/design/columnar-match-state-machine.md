# Match 状态机列式化 · 详细设计

> 日期：2026-08-18 · 分支 `feat/columnar-execution`
> 关联：`columnar-execution-design.md`（整体列式执行，vectorized 路线）、
> `columnar-execution-progress.md`（已落地：guard 列式 14.3ns、on-each defer 懒物化）。
> 本文把「sharded match（Q2）免物化」落到可实施：**类型、签名、分片算法、接线、
> 测试矩阵**，让代码能照着写。

## 0. 背景与缺口（一句话）

`columnar-execution-progress.md` L165：懒物化的 `broadcast_batch_only` **排除 sharded
窗口**；Q2 是 `match<auction:10m>` → `Subscription::Sharded`。当前 `message_parse`
的 sharded 分支（`fanout.rs` L270-276）**硬性要求 `events=Some`**（`debug_assert!`），
导致 sharded match 在 parse 阶段全量 `batch_to_events` 物化每行 HashMap → Q2 ~6.2-7.2M（progress M2a 双峰基线；
ingress receive-only ~90M 为上限参照）。本文设计「sharded 也走 `events=None` + 列式分片」，免物化。

## 1. 目标形态（对齐 `columnar-execution-design.md §3` 的 vectorized 路线）

```
route_parse:  Q2 窗口走 defer → broadcast_batch_only(batch, materialize_fields, seq)
                                  │ events=None, batch=Some（零物化）
                                  ▼
RuleFanout::broadcast_inner:  Subscription::Sharded
   ┌─────────────────────────────────────────────────────────────┐
   │ 无 events → 列式分片：                                       │
   │   ① 对每行从 batch 列读 keys（列索引批级解析一次）           │
   │   ② extract_key → shard_index（复用现有 FNV，字节一致）      │
   │   ③ 每 shard 收集「本 shard 的 batch 行索引子集」            │
   └─────────────────────────────────────────────────────────────┘
   每 shard: RulePush { events: None, batch: Some(batch),
                        shard_rows: Some(Vec<u32>), materialize_fields, seq }
                                  ▼
RuleTask::process_batch:  sharded match 走懒物化（分层见 §6）
   只扫本 shard 的 shard_rows：时间列 + bind-filter 掩码 → 命中行子集
   P2（本次）：命中行 materialize_rows_filtered 物化喂状态机（Q2 命中 0.81%）
   P3（后续）：ColumnarEvent::new(batch, row) 直喂状态机（零 HashMap 物化）
```

## 2. 类型/签名改动

### 2.1 `RulePush` 增加 `shard_rows`

`crates/wf-engine/src/window/fanout.rs` L22-39 `RulePush`：

```rust
pub struct RulePush {
    pub window_name: Arc<str>,
    pub events: Option<Arc<Vec<Arc<Event>>>>,   // Some：行式（interpreter/回退）
    pub batch: Option<Arc<RecordBatch>>,        // Some：列式，供 ColumnarEvent 读列
    pub materialize_fields: Option<Arc<HashSet<String>>>,
    pub seq: u64,
    /// 仅 sharded 广播在 `events=None` 时设置：本 shard 拿到 batch 的哪些行。
    /// unsharded / 行式广播不设置（None）。
    pub shard_rows: Option<Arc<Vec<u32>>>,
}
```

- 语义：`events=None && batch=Some && shard_rows=Some(shards)` → **sharded 列式子集**，
  规则任务对 `shard_rows` 里的行做 `ColumnarEvent`（零物化）。

### 2.2 列式分片函数（新，`fanout.rs` 或 `key.rs`）

复用 `shard_index`（`key.rs` L284）的 FNV 与 `make_scope_key_str`（L116）的字节，
但**从 batch 列读 key 原值**，不物化 `Event`/`Value`：

```rust
// key.rs：把 extract_key_simple 的行式实现复制一份"列式"（按列索引读原始值）。
/// 从 batch 的 row 按 keys（列索引已批级解析）提取分片键，等价于
/// extract_key_simple(Event{从 batch 该行物化到 keys 字段}, keys)。
/// 返回 None ⇔ keys 中任一轮列缺失/null（与行式 `fields.get()?` 一致 → shard 0）。
pub(crate) fn extract_key_columnar(
    batch: &RecordBatch,
    col_idx: &[usize],      // keys 逐字段的批级列索引
    row: usize,
) -> Option<Vec<Value>> {
    let mut result = Vec::with_capacity(col_idx.len());
    for &idx in col_idx {
        let col = batch.column(idx);
        if col.is_null(row) { return None; }
        // 复用 extract_field_value(event_bridge.rs) 产出与行式相同的 Value；
        // 只对 keys 字段（通常 1 个）物化，不建整个 HashMap。
        result.push(extract_field_value(batch.schema().field(idx), col.as_ref(), row)?);
    }
    Some(result)
}

/// 对一批行做列式分片，产出 `Vec<Vec<u32>>`（每 shard 的 batch 行索引子集）。
fn partition_rows_by_key(
    batch: &RecordBatch,
    keys: &[FieldRef],
    shard_count: usize,
) -> Option<Vec<Vec<u32>>> {
    let col_idx: Vec<usize> = keys.iter()
        .map(|fr| field_ref_name(fr))
        .map(|name| batch.schema().index_of(name).ok())
        .collect::<Option<_>>()?;              // key 字段不在 schema → 全 0 shard（行式同）
    let mut per = vec![Vec::new(); shard_count];
    for row in 0..batch.num_rows() {
        // 结构与 sharded_sends 行式一致：Missing key → shard 0。
        let idx = extract_key_columnar(batch, &col_idx, row)
            .map(|k| shard_index(&k, shard_count))
            .unwrap_or(0);
        per[idx].push(row as u32);
    }
    Some(per)
}
```

> **字节一致性**：行式分片是 `extract_key_simple(event)` → `Value` → `make_scope_key_str` →
> FNV；列式是 `extract_key_columnar` → 同样 `Value` → 同样 `shard_index`。价值相同、
> FNV 相同 → **同一行落同一 shard**。用一个 `u32` 和 `ExtractKey` 统一抽象可将二者合一
> （对拍见 §8）。

## 3. `fanout.rs`：sharded 分支支持 `events=None`

`broadcast_inner` 的 `Subscription::Sharded` 分支（L270-276）从「要求 events」改为「二者择一」：

```rust
Subscription::Sharded { shards, keys } => {
    match (events, batch_arc.as_ref()) {
        // 行式（有预物化 events）：现状不变 —— 按 extract_key_simple 分片
        (Some(events), _) => {
            sharded_sends(shards, keys, &window_name, events, seq, &mut sends)
        }
        // 列式 + sharded：events=None，按 batch 直接列式分片
        (None, Some(batch)) => {
            if let Some(per) = partition_rows_by_key(batch, keys, shards.len()) {
                for (i, rows) in per.into_iter().enumerate() {
                    if rows.is_empty() { continue; }
                    let push = RulePush {
                        window_name: Arc::clone(&window_name),
                        events: None,
                        batch: batch_arc.clone(),  // 共享本广播的 Arc<RecordBatch>（refcount 增量，零拷贝）
                        materialize_fields: materialize_fields.map(Arc::clone),
                        shard_rows: Some(Arc::new(rows)),
                        seq,
                    };
                    let tx = shards[i].clone();
                    sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
                }
            } else {
                // key 字段缺列 → 全部 shard 0（与行式 Missing-key→0 对齐），仍免物化
                let rows: Vec<u32> = (0..batch.num_rows()).map(|r| r as u32).collect();
                let push = RulePush {
                    window_name: Arc::clone(&window_name),
                    events: None,
                    batch: batch_arc.clone(),
                    materialize_fields: materialize_fields.map(Arc::clone),
                    shard_rows: Some(Arc::new(rows)),
                    seq,
                };
                let tx = shards[0].clone();
                sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
            }
        }
        // 不可达：本广播连 batch 都没有（broadcast() 纯行式入口走 Some 分支）
        (None, None) => { debug_assert!(false, "sharded broadcast without events or batch"); }
    }
}
```

> 注：`batch_arc` 是 `broadcast_inner` 处每广播**一次**构造的 `Option<Arc<RecordBatch>>`
> （L254，Single/RoundRobin 订阅已按 `Arc::clone` 共享）；列式 sharded 分支同样按 shard
> `Arc::clone` 共享它。原稿伪代码中的 `Arc::new(batch.clone())` 会让每个 shard 再克隆
> 一次 RecordBatch 结构（内含 Vec 分配），应避免。

## 4. `route_parse`：对 sharded 也走 defer（放行懒物化）

`crates/wf-engine/src/window/router.rs` L303-311 放宽 defer 条件。先澄清两个事实
（原稿此处描述有误）：

- `win.defer_materialization()`（`buffer/mod.rs` L444，源自 `wf-lang/field_usage.rs`
  L106 `plan_defer_safe`）是**纯 plan 分析位**：该窗口所有 bound 规则满足
  `each_plan.is_some()` 或每个 bind 的 filter `expr_is_columnar`。它**不含**
  non-sharded 语义——「排除 sharded」是 router 侧 `&& !has_sharded_subscribers`
  单独加的合取项，原因只有一个：sharded 广播分支当时不支持 `events=None`
  （§3 已修）。因此**不需要新增 `columnar_match_safe()`，只删这一个合取项**。
- match 的 step guard **不需要新校验**：`branch_guard_masks`（`executor/mod.rs`
  L340-358）只对 `expr_is_columnar` 的 guard 插掩码，非列式 guard 自然无掩码 →
  状态机逐行解释求值（既有回退，语义不变，不阻塞 defer）。

```rust
// 现状（router.rs L303-304）：
//   win.defer_materialization()
//       && !self.rule_fanout.has_sharded_subscribers(&window_name)
// 改为（只删 sharded 排除合取项）：
if win.defer_materialization() {
    windows.push(ParsedWindow {
        events_bytes: 0,
        window_name,
        events: None,   // sharded 也免物化；fanout 列式分片（§3）+ rule task 扫 shard_rows（§5）
    });
    continue;
}
// 非 columnar（函数/路径/类型不匹配）保持 batch_to_events 全量物化回退
```

> `has_sharded_subscribers`（fanout.rs L116）删合取项后无生产调用者——一并删除
> （或降级 `#[cfg(test)]`）。`defer_materialization()` 语义不变；非 columnar sharded
> 规则不受影响（=false → 仍全量物化）。

## 5. `rule_task.rs`：sharded 收到 `events=None` 时的懒物化

`process_batch` 的 match 路径当前只在 `events.is_none() && batch.is_some() && machine.is_some()`
时走懒物化（L456 `defer_materialize`）。注意 sharded 排除**不在 rule task**（原稿「对
`has_sharded_subscribers` 有另判」有误——该判断全库只有 route_parse 一处使用）；§4 放行后
sharded push 以 `events=None` 到达，需两处改动：

**① `process_batch` 签名增加 `shard_rows`**（现签名
`(window_name, batch_seq, events, batch, materialize_fields)` 没有 push 参量，取不到
`shard_rows`——原稿伪代码引用 `push.shard_rows` 不存在于该作用域；由调用方从 `RulePush`
解出后传入）：

```rust
pub(super) async fn process_batch(
    &mut self,
    window_name: &str,
    batch_seq: u64,
    events: Option<&Arc<Vec<Arc<Event>>>>,
    batch: Option<&RecordBatch>,
    shard_rows: Option<&Arc<Vec<u32>>>,     // 新增：仅列式 sharded 广播设置
    materialize_fields: Option<&HashSet<String>>,
)

// 懒物化命中（defer_materialize）：行域改为 shard 子集，其余逻辑不变
let rows_iter: Box<dyn Iterator<Item = usize>> = match shard_rows {
    Some(rows) => Box::new(rows.iter().map(|&r| r as usize)),  // 本 shard 的行
    None => Box::new(0..num_rows),                            // unsharded：整批
};
// 现有逻辑原样作用于该子集：时间列读取 + bind-filter 掩码 → 命中行
// P2（本次）：命中行 materialize_rows_filtered(batch, &hit_rows, materialize_fields)
//             物化喂 machine.advance_at（同现有 deferred 路径，状态机接口不动）
// P3（后续）：ColumnarEvent::new(batch, row) 直喂状态机（免物化）
```

- **watermark 语义不变**：shard 任务只扫本 shard 行的时间——与行式 sharded 一致（每 shard
  一台状态机，水位本就按各自子集推进）；shard_rows 外的非命中行零成本跳过。
- 命中行（Q2 ~0.81%）本次仍物化为 `Event`（`materialize_fields` 投影），P3 再消。

## 6. 分层：先免物化广播（本次），后免物化命中行（P3）

本次一次交付全部管道改动——`extract_key_columnar`/`partition_rows_by_key` +
`RulePush.shard_rows` + `broadcast_inner` 列式分片 + `route_parse` 放行 + `rule_task`
扫 shard_rows——但**不改状态机接口**：状态机仍吃命中行的 `&Event`（命中行仅 0.81%，
`materialize_rows_filtered` 只物化命中行即可，不必一步到位 `FieldView`）。这样：

- **本次交付**：sharded 免物化（broadcast 列式分片 + rule task 只扫 shard_rows + 命中行
  `materialize_rows_filtered`）。收益即 parse 侧全量物化的解放，Q2 EPS 逼近 receive 上限。
- **P3（后续）**：`FieldView` trait + 命中行也免物化（直接 `ColumnarEvent` 喂状态机），
  guard 向量化 kernel —— 把 `columnar-execution-design.md` 的 vectorized 走到底。

## 7. 边界与语义保持

| 边界 | 处理 |
|---|---|
| key 字段不在 batch schema | `partition_rows_by_key` → `None` → 全走 shard 0（与行式 Missing-key→0 对齐） |
| 行式 sharded（有预物化 events） | 现 `sharded_sends` 不变；只有当 producer 广播 batch-only 时才走列式分片 |
| 非 columnar 规则（函数/路径/类型不匹配 filter） | `defer_materialization()`=false → route_parse 仍全量物化，正确性不变 |
| `shard_rows` 为空 sub | `if rows.is_empty() { continue; }` 不发空 batch 给该 shard（省一次 channel send） |
| 事件时间列 | 列式路径 `shard_rows` 行的时间仍从 batch 时间列取（现有 `batch_event_time_nanos_at`） |
| seq / watermark / 回放 | sharded 广播仍是每 shard 一个 send，`seq` 不变；窗口 actor 不感知分片 |
| 命中行物化字段集 | 用 `materialize_fields`（窗口 `field_usage` 投影），命中行 `materialize_rows_filtered` |
| `trigger_event`（emit 路径） | 命中行在命中时物化该事件（`ColumnarEvent` → Event），仅命中行，可接受 |
| match step guard 非列式 | `branch_guard_masks` 只对列式 guard 插掩码 → 无掩码即逐行解释求值（既有回退，不阻塞 defer） |
| key 字段可用性 | `field_usage` 已把 `m.keys` 收进物化投影 → 行式回退同键可提取；列式路径按列名 `index_of`，同列同键 |
| debug 日志开启 | rule task `defer_materialize=false` → eager 分支按 batch 重建整批（现有机制，行式/列式一致） |

## 8. 测试矩阵（逐字节锁定）

| 测试 | 断言 |
|---|---|
| `partition_rows_by_key` vs 行式 `sharded_sends` 分片 | 同 batch，两个函数对每一行落在**同一 shard**（Q2 `<auction:10m>` 键闭包 + 有状态安全） |
| `extract_key_columnar` vs `extract_key_simple` | 对同一批所有行，逐行返回同 `Vec<Value>`（含 null/缺失/2^53/Utf8 lane） |
| 端到端 Q2：懒物化 sharded | EPS 逼近 receive-only 上限（双峰相位配对，见 progress M2a），EMIT `q2_mod_123`=747816 精确（=0.8129%×100M），`[clean]`，窗口 append 100M/100M（ingress 全量口径，勿与 EMIT 混淆） |
| 行式 sharded 回退 | 非 columnar sharded 规则走物化，结果与改造前逐位一致（回归） |
| `rule_task` 只扫 shard_rows | 每 shard 收到的是本 shard 行子集，总 and += 全批；不丢行、不重复 |

## 9. 验收顺序（方向既定，直接做）

1. **本次代码**（§6 全部管道）：`RulePush.shard_rows` + `partition_rows_by_key` +
   `broadcast_inner` sharded 列式分支 + `route_parse` 放宽 defer + `rule_task` 扫 shard_rows
   （`process_batch` 签名加 `shard_rows`）。
2. **对拍**：分片一致性 + extract_key 一致性（§8 前两行）。
3. **端到端 Q2**：EPS 目标逼近 receive-only 上限（~90M 参照，双峰相位配对）、EMIT 精确、`[clean]`。
4. 回归 Q1/Q2/Q3/Q5/Q7/Q9 + seq。
5. （可选 P3）`FieldView` + 命中行免物化 + guard 整列 kernel（vectorized 走完）。

## 10. 风险

- **中**：sharded 广播从「events=Some」转「events=None」改变了 rule task 收到的表示，
  需对拍分片一致性防「某些行漏发 / 多发到错 shard」，这直接破坏有状态语义。
- **中**：`batch.clone()`（Arc 副本）在广播时给每 shard 一份，RSS 是 refcount 增量、
  零拷贝，但需确认 `Arc<RecordBatch>` 生命周期（TimedBatch 已持 batch，Arc 不复制列）。
- **低**：命中行 `materialize_rows_filtered` 仍是物化（0.81%），但相对全批省 99%，
  已是本步收益主体；P3 再消这 0.81%。
