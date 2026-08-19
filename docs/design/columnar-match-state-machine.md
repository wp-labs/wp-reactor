# Match 状态机列式化 · 详细设计

> 日期：2026-08-18 · 分支 `feat/columnar-execution`
> 关联：`columnar-execution-design.md`（整体列式执行，vectorized 路线）、
> `columnar-execution-progress.md`（已落地：guard 列式 14.3ns、on-each defer 懒物化）。
> 本文把「sharded match（Q2）免物化」落到可实施：**类型、签名、分片算法、接线、
> 测试矩阵**，让代码能照着写。

> ## 实现状态（2026-08-19 初版；本次同步到当前实现）
>
> P2（§6 全部管道）已实现并验证：**代码 ✅ · 对拍 ✅ · 完整回归 ✅**。
> Q2 免物化后 EPS 从 ~6-7M（物化基线）提升到 **~34M**（EMIT 精确、`[clean]`）；
> 受第二道墙（窗口 actor 单写者 P0-③）约束，再往上需拆该墙，而非 rule 侧。
>
> 实现机制相对初版已演进，本文其余章节已**按当前实现重写**：
> - **方案A（commit `3481020`）**：按键分片从 window actor 单写者热路径挪到**并行 parse 侧
>   预分片**（`precompute_shard_rows`），经 `ParsedWindow.shard_rows` 带入 actor；actor 仅做
>   零成本复用，仅当预分片长度与 live shard 数不符（config drift / hot reload）才防御性重分片。
>   这是**拆第二道墙（P0-③）**的关键一步，而非单纯免物化。
> - **ScopeKey typed 分片键（commit `b440ed2`）**：列式子集改用 typed `ScopeKey`
>   （`scope_key_columnar` / `scope_key_from_column` / `column_scalar`）直读 Arrow 原生值，
>   不再走 `Value`/`f64` 往返（Int64 键保持 i64，避免 >2^53 丢精度的旧分歧放大）；
>   Q2 从 ~18M 再翻倍到 ~34M。
> - **P3（guard 整列向量化 kernel，commit `d7360f8`）已实现并验证，但对 Q2 吞吐无增益**
>   （第二道墙约束）：把 `columnar.rs` 的 guard 求值从逐行递归树升级为整列 kernel
>   （`CVec` + `eval_vec` 各算子 `cmp_vec/arith_vec/logic_vec/neg_vec/col_vec`），语义逐字节
>   对齐解释器，`eval_guard_columnar` 接口不变。详见末「附录：实现状态与实测」。
>
> > 初版设计的 `extract_key_columnar`（返回 `Vec<Value>`）+ FNV `shard_index` 内联分片已被上述
> > 方案A + ScopeKey 取代。本文删除初版 §2.2/§3 的 FNV 内联描述，改为 typed `ScopeKey` +
> > parse 侧预分片；旧 `partition_rows_by_key` 仅作为 `broadcast_inner` 的**防御性 fallback** 保留。

## 0. 背景与缺口（一句话）

`columnar-execution-progress.md` L165：懒物化的 `broadcast_batch_only` 初版**排除 sharded
窗口**；Q2 是 `match<auction:10m>` → `Subscription::Sharded`。**改造前** `message_parse`
的 sharded 分支（`fanout.rs`）**硬性要求 `events=Some`**（`debug_assert!`），
导致 sharded match 在 parse 阶段全量 `batch_to_events` 物化每行 HashMap → Q2 ~6.2-7.2M（progress M2a 双峰基线）。
本文设计「sharded 也走 `events=None` + 列式分片」，免物化；现已进一步演进为
**parse 侧预分片（方案A）+ typed ScopeKey 分片键**（见 §实现状态）。

## 1. 目标形态（对齐 `columnar-execution-design.md §3` 的 vectorized 路线）

```
route_parse（并行 parse 阶段，方案A 关键）：
   win.defer_materialization() 的 sharded 窗口
      → fanout.precompute_shard_rows(window_name, batch)   ← 在并行 parse 侧完成分片
      → ParsedWindow { events: None, batch: None(此处),     ← 仅携带分片结果
                        shard_rows: Option<Arc<[Vec<u32>]>>, 每 shard 一个行索引子集 }
      │ 共享 Arc<RecordBatch> 随 ParsedRoute 传到 actor
      ▼
actor commit 路径：broadcast_batch_only(window_name, batch, materialize_fields,
                                         shard_rows: Option<&[Vec<u32>]>, seq)
      │ events=None, batch=Some（零物化）
      ▼
RuleFanout::broadcast_inner:  Subscription::Sharded
   ┌─────────────────────────────────────────────────────────────┐
   │ 直接用 parse 侧预分片的 shard_rows：                          │
   │   - 若 pre.len() == shards.len() → 零成本复用（主路径）      │
   │   - 否则（config drift / hot reload）→ partition_rows_by_key  │
   │     防御性重分片（typed ScopeKey 路径，见 §2.2）             │
   │   每 shard 收集「本 shard 的 batch 行索引子集」              │
   └─────────────────────────────────────────────────────────────┘
   每 shard: RulePush { events: None, batch: Some(batch),
                        shard_rows: Some(Arc<Vec<u32>>),  ← 本 shard 行子集
                        materialize_fields, seq }
                                  ▼
RuleTask::process_batch:  sharded match 走懒物化（分层见 §6）
   只扫本 shard 的 shard_rows：时间列 + bind-filter 掩码 → 命中行子集
   P2（历史中间态，已删除）：命中行 materialize_rows_filtered 物化喂状态机（Q2 命中 ~0.81%）
   P3（已落地，当前生产路径）：ColumnarEvent::with_index(batch, row, FieldIndex) 直喂状态机（零 HashMap 物化）
```

## 2. 类型/签名改动

### 2.1 两级 `shard_rows`

设计里有**两层** `shard_rows`，不要混淆：

**（a）parse 侧预分片结果 —— `ParsedWindow.shard_rows: Option<Arc<[Vec<u32>]>>`**
（`crates/wf-engine/src/window/router.rs`）：并行 parse 阶段由 `fanout.precompute_shard_rows`
算出**整批的 per-shard 行索引子集**（外层 `Vec` 长度 = shard 数，内层 `Vec<u32>` = 该
shard 拥有的 batch 行）。随 `ParsedRoute` 进入 actor commit 路径，零成本传给
`broadcast_batch_only`。仅 sharded 且 defer 的窗口有值；unsharded / 行式窗口为 `None`。

**（b）单 shard 推送 —— `RulePush.shard_rows: Option<Arc<Vec<u32>>>`**
（`crates/wf-engine/src/window/fanout.rs` L48 `RulePush`）：actor 把 (a) 的整批 partition
按 shard 拆开，每个 shard 拿到**自己的那一份子集**放进 `RulePush.shard_rows`。语义：
`events=None && batch=Some && shard_rows=Some` → **sharded 列式子集**，规则任务只对本子集
做 `ColumnarEvent` / 懒物化。unsharded / 行式推送为 `None`。

```rust
pub struct RulePush {
    pub window_name: Arc<str>,
    pub events: Option<Arc<Vec<Arc<Event>>>>,   // Some：行式（interpreter/回退）
    pub batch: Option<Arc<RecordBatch>>,        // Some：列式，供 ColumnarEvent 读列
    pub materialize_fields: Option<Arc<HashSet<String>>>,
    pub seq: u64,
    /// 仅 sharded 广播在 `events=None` 时设置：本 shard 拿到 batch 的哪些行（子集）。
    /// unsharded / 行式广播不设置（None）。
    pub shard_rows: Option<Arc<Vec<u32>>>,
}
```

### 2.2 列式分片函数（`fanout.rs`）

列式子集不再走 `Value`/`f64` 往返，而是用 **typed `ScopeKey`** 直读 Arrow 原生值，
与行式 `sharded_sends`（同样走 `ScopeKey`：`scope_key_from_values(extract_key_simple(...))`）
经 **同一个 `scope_key_shard_index`** 哈希 → 双路逐行同 shard。

```rust
// 从 batch 单列直读原生值构 ScopeKey，不经过 Value/f64（Int64 保持 i64）。
// 不支持的列类型回退 column_scalar → ScopeKey::from_value（仍确定性）。
fn scope_key_from_column(batch, col_idx, row) -> Option<ScopeKey> {
    use arrow::datatypes::{DataType, TimeUnit};
    let col = batch.column(col_idx);
    if col.is_null(row) { return None; }          // null/缺失 → 调用方落 shard 0
    match col.data_type() {
        DataType::Int64 | DataType::Timestamp(Nanosecond, _) =>
            Some(ScopeKey::Int(a.value(row))),    // 原生 i64，无 f64 往返
        DataType::Float64 => Some(ScopeKey::from_value(&Value::Number(a.value(row)))),
        DataType::Utf8    => Some(ScopeKey::Str(a.value(row).into())),
        DataType::Boolean => Some(ScopeKey::Str((if a.value(row) {"true"} else {"false"}).into())),
        _ => column_scalar(batch, col_idx, row).map(|v| ScopeKey::from_value(&v)),
    }
}

// 计划字段序下把 row 的 match-key 字段拼成 ScopeKey（多键 → ScopeKey::Pair）。
// 任一 key 列 null/缺失 → None（调用方落 shard 0）。
pub(crate) fn scope_key_columnar(batch, col_idx: &[usize], row) -> Option<ScopeKey> {
    let mut acc = None;
    for &ci in col_idx {
        let v = scope_key_from_column(batch, ci, row)?;
        acc = Some(match acc { None => v, Some(p) => ScopeKey::Pair(Box::new(p), Box::new(v)) });
    }
    Some(acc.unwrap_or(ScopeKey::Empty))
}

// 整批按 key 分片 → 每 shard 一个行索引子集。key 字段整体缺席 schema → None（全落 shard 0）。
fn partition_rows_by_key(batch, keys: &[FieldRef], shard_count) -> Option<Vec<Vec<u32>>> {
    let col_idx: Vec<usize> = keys.iter().map(field_ref_name)
        .map(|name| batch.schema().index_of(name).ok()).collect::<Option<_>>()?;
    let mut per = vec![Vec::new(); shard_count];
    for row in 0..batch.num_rows() {
        let idx = scope_key_columnar(batch, &col_idx, row)
            .map(|key| scope_key_shard_index(&key, shard_count))   // 与行式同哈希
            .unwrap_or(0);
        per[idx].push(row as u32);
    }
    Some(per)
}
```

> **typed 一致性**：行式 `sharded_sends` 走 `extract_key_simple` → `scope_key_from_values`
> → `ScopeKey` → `scope_key_shard_index`；列式子集走 `scope_key_columnar` → `ScopeKey` →
> 同一 `scope_key_shard_index`。两者都落到同一个 typed `ScopeKey` 再哈希，因此**同一行落同
> 一 shard**。`scope_key_columnar` 直读原生值（Int64→`ScopeKey::Int`，不经 `f64`），只有
> 不支持的列类型才回退 `ScopeKey::from_value`（其 f64 路径与行式 `from_value` 完全一致）。
> 唯一的既有分歧是 `>2^53` 的 Int64 键：行式 `from_value` 会经 `f64` 丢精度，列式
> `ScopeKey::Int` 保持 i64——这是**行式路径自身**的语义，非列式引入，对拍见 §8。

## 3. `fanout.rs`：sharded 分支支持 `events=None`

`broadcast_inner` 的 `Subscription::Sharded` 分支从「要求 events」改为「二者择一」。
**主路径复用 parse 侧预分片**：`broadcast_batch_only` 把 `ParsedWindow.shard_rows`
（`Option<&[Vec<u32>]>`）传进来，`broadcast_inner` 在 `pre.len() == shards.len()` 时**零成本
复用**，仅在长度不符（config drift / hot reload）时退回 `partition_rows_by_key` 防御性重分片。

```rust
Subscription::Sharded { shards, keys } => {
    match (events, batch_arc.as_ref()) {
        // 行式（有预物化 events）：现状不变 —— 按 ScopeKey 分片
        (Some(events), _) => {
            sharded_sends(shards, keys, &window_name, events, seq, &mut sends)
        }
        // 列式 + sharded：events=None，复用 parse 侧预分片（或防御性重分片）
        (None, Some(batch)) => {
            let pre = match shard_rows {
                Some(pre) if pre.len() == shards.len() => Some(pre),  // 主路径：零成本复用
                _ => None,
            };
            let per: Arc<[Vec<u32>]> = match pre {
                Some(pre) => Arc::from(pre),                          // 预分片（parse 侧已算好）
                None => partition_rows_by_key(batch, keys, shards.len())  // fallback：typed ScopeKey
                    .unwrap_or_else(|| { /* key 列缺 → 全 shard 0 */ }),
                    .into(),
            };
            for (i, rows) in per.iter().enumerate() {
                if rows.is_empty() { continue; }       // 空 sub 不发（省一次 channel send）
                let push = RulePush {
                    window_name: Arc::clone(&window_name),
                    events: None,
                    batch: batch_arc.clone(),          // 共享 Arc<RecordBatch>（refcount，零拷贝）
                    materialize_fields: materialize_fields.map(Arc::clone),
                    shard_rows: Some(Arc::new(rows.clone())),   // 本 shard 子集
                    seq,
                };
                let tx = shards[i].clone();
                sends.push(Box::pin(async move { tx.send(push).await.is_err() }));
            }
        }
        // 不可达：本广播连 batch 都没有
        (None, None) => { debug_assert!(false, "sharded broadcast without events or batch"); }
    }
}
```

> **`broadcast_batch_only` 签名**：`(window_name, batch, materialize_fields,
> shard_rows: Option<&[Vec<u32>]>, seq)`——`shard_rows` 是整批的 per-shard 行索引子集
> （来自 `precompute_shard_rows`），不是单 shard 的 `Vec<u32>`。
>
> **`batch_arc` 只 clone 一次**：`broadcast_inner` 入口 `let batch_arc = batch.map(|b|
> Arc::new(b.clone()))` 每广播构造**一个** `Arc<RecordBatch>`（仅克隆 RecordBatch 外壳——
> 列都是 `Arc<Array>`，成本极低），各 shard 仅 `batch_arc.clone()`（refcount 增量、零拷贝）。
> 文档初版的「每 shard 一份 batch 副本」是误读——实际每广播一次，非每 shard。

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
// 现状（router.rs，初版伪代码曾写 `win.defer_materialization() &&
// !has_sharded_subscribers(...)`）：删掉 sharded 排除合取项即可（`has_sharded_subscribers`
// 已随本次删除）。
if win.defer_materialization() {
    // 方案A：在并行 parse 阶段就把 sharded 窗口的分片算好，随 ParsedWindow 带入 actor，
    // 避免 actor 单写者热路径上 O(batch) 重分片（拆第二道墙 P0-③）。
    let shard_rows = self.rule_fanout.precompute_shard_rows(&window_name, batch);
    windows.push(ParsedWindow {
        events_bytes: 0,
        window_name,
        events: None,   // sharded 也免物化
        shard_rows,     // 整批 per-shard 行索引子集（unsharded / 行式窗口为 None）
    });
    continue;
}
// 非 columnar（函数/路径/类型不匹配）保持 batch_to_events 全量物化回退
```

> `has_sharded_subscribers` 删合取项后无生产调用者，已删除（或仅 `#[cfg(test)]`）。
> `defer_materialization()` 语义不变；非 columnar sharded 规则不受影响（=`false` → 仍全量物化）。
> `precompute_shard_rows` 对**每个** defer 窗口都会调用一次（含 unsharded），但无 sharded
> 订阅时返回 `None`，开销仅一次 fanout table 读锁查询，可忽略。

## 5. `rule_task.rs`：sharded 收到 `events=None` 时的懒物化

`process_batch` 的 match 路径当前只在 `events.is_none() && batch.is_some() && machine.is_some()`
时走懒物化（L456 `defer_materialize`）。注意 sharded 排除**不在 rule task**（原稿「对
`has_sharded_subscribers` 有另判」有误——该判断全库只有 route_parse 一处使用）；§4 放行后
sharded push 以 `events=None` 到达，需两处改动：

**① `process_batch` 签名增加 `shard_rows`**（现签名
`(window_name, batch_seq, events, batch, materialize_fields)` 没有 push 参量，取不到
`shard_rows`——由调用方从 `RulePush` 解出后传入）：

```rust
pub(super) async fn process_batch(
    &mut self,
    window_name: &str,
    batch_seq: u64,
    events: Option<&Arc<Vec<Arc<Event>>>>,
    batch: Option<&RecordBatch>,
    shard_rows: Option<&[u32]>,        // 新增：仅列式 sharded 广播设置（本 shard 行子集）
    materialize_fields: Option<&HashSet<String>>,
)

// 懒物化命中（defer_materialize）：行域改为 shard 子集（row_domain 均为绝对 batch 行索引）
let row_domain: Vec<usize> = match shard_rows {
    Some(rows) => rows.iter().map(|&r| r as usize).collect(),  // 本 shard 的行
    None => (0..num_rows).collect(),                           // unsharded：整批
};
// 注意 `num_rows` 在 sharded deferred 路径取自 batch.num_rows()（整批），但只用于
// unsharded 分支的 (0..num_rows)；sharded 走 row_domain，主循环也迭代 row_domain —— 不丢不重。
// DeferredRows（times/hit/hit_indices）按 row_domain 相对下标构建，下游需绝对行时由
// row_domain[i] 还原。现有逻辑原样作用于该子集：时间列读取 + bind-filter 掩码 → 命中行
// P2（历史中间态，已删除）：命中行 materialize_rows_filtered(batch, &hit_rows, materialize_fields)
//             物化喂 machine.advance_at（同现有 deferred 路径，状态机接口不动）
// P3（已落地，当前路径）：ColumnarEvent::with_index(batch, row, &FieldIndex) 直喂状态机（免物化）
```

- **watermark 语义不变**：shard 任务只扫本 shard 行的时间——与行式 sharded 一致（每 shard
  一台状态机，水位本就按各自子集推进）；shard_rows 外的非命中行零成本跳过。
- 命中行（Q2 ~0.81%）现在直接以 `ColumnarEvent`（批级 `FieldIndex` O(1) 名解析）喂状态机，
  不再物化 `Event`——`materialize_rows[_filtered]` 已从 deferred 生产路径删除（P3 落地）。

## 6. 分层：先免物化广播，后免物化命中行（两层均已落地）

先交付全部管道改动——`scope_key_columnar`/`partition_rows_by_key` + `precompute_shard_rows`
（parse 侧预分片）+ 两级 `shard_rows`（`ParsedWindow` 全部分片 / `RulePush` 本 shard 子集）+
`broadcast_inner` 复用预分片（fallback 才重分片）+ `route_parse` 放行 + `rule_task` 扫
`row_domain`。命中行免物化（P3）随后落地：状态机接口改为泛型
`advance_at_with_masks<E: FieldSource>`，命中行以 `ColumnarEvent::with_index` 直喂，
`materialize_rows[_filtered]` 已从生产路径删除。这样：

- **P2（历史交付）**：sharded 免物化（broadcast 列式分片 + rule task 只扫 shard_rows + 命中行
  `materialize_rows_filtered`）。收益即 parse 侧全量物化的解放（Q2 从 ~6-7M 提升到
  ~18M）；之后方案A 把分片挪出 actor + ScopeKey typed 直读原生值，再提到 **~34M**；
  再往上受残余第二道墙（窗口 actor 单写者 P0-③ 串行 append/broadcast）约束，需另拆。
  （命中行 `materialize_rows_filtered` 已被 P3 取代并从生产路径删除。）
- **P3（已实现，2026-08-19，commit 后续）**：`FieldSource` trait（`Event`/`ColumnarEvent`
  都实现）+ 状态机 `advance_at_with_masks` 泛型化 → 命中行直接 `ColumnarEvent::with_index`
  （批级 `FieldIndex` O(1) 名解析）直喂状态机，零 HashMap 物化；`defer_materialize` 不再
  物化命中行（`DeferredRows` 只带 batch + index）。同时**放宽 defer 门槛**（field_usage
  `plan_defer_safe` 放行任意 match 规则）——Q5/Q7/qradar 这类无 filter match 也走
  deferred+columnar，消除 eager 整批物化。guard 整列向量化 kernel **已实现**（见附录），
  对 Q2 吞吐无增益——被第二道墙挡住，非 guard 侧。

## 7. 边界与语义保持

| 边界 | 处理 |
|---|---|
| key 字段不在 batch schema | `partition_rows_by_key` → `None` → 全走 shard 0（与行式 Missing-key→0 对齐） |
| 行式 sharded（有预物化 events） | 现 `sharded_sends` 不变；只有当 producer 广播 batch-only 时才走列式分片 |
| 非 columnar 规则（函数/路径/类型不匹配 filter） | `defer_materialization()`=false → route_parse 仍全量物化，正确性不变 |
| `shard_rows` 为空 sub | `if rows.is_empty() { continue; }` 不发空 batch 给该 shard（省一次 channel send） |
| 事件时间列 | 列式路径 `shard_rows` 行的时间仍从 batch 时间列取（现有 `batch_event_time_nanos_at`） |
| seq / watermark / 回放 | sharded 广播仍是每 shard 一个 send，`seq` 不变；窗口 actor 不感知分片 |
| 命中行字段投影 | `materialize_fields`（窗口 `field_usage` 投影）作为 `ColumnarEvent` 的 `projection` 携带，仅 `trigger_event`→`Event` 转换时按需物化命中行；状态机 eval 经批级 `FieldIndex` 直读列（零 HashMap，P3 落地后 `materialize_rows_filtered` 已从生产路径删除） |
| `trigger_event`（emit 路径） | 命中行在命中时物化该事件（`ColumnarEvent` → Event），仅命中行，可接受 |
| match step guard 非列式 | `branch_guard_masks` 只对列式 guard 插掩码 → 无掩码即逐行解释求值（既有回退，不阻塞 defer） |
| key 字段可用性 | `field_usage` 已把 `m.keys` 收进物化投影 → 行式回退同键可提取；列式路径按列名 `index_of`，同列同键 |
| debug 日志开启 | rule task `defer_materialize=false` → eager 分支按 batch 重建整批（现有机制，行式/列式一致） |

## 8. 测试矩阵（逐字节锁定）

> ✅ = 已实现并验证（2026-08-19）。

| 测试 | 断言 | 状态 |
|---|---|---|
| 列式子集 vs 行式子集：逐行同 shard | `scope_key_columnar`(typed) 与 `sharded_sends`(`scope_key_from_values(extract_key_simple)`) 对同一批每行落同一 shard（含 null key → shard0） | ✅ `partition_rows_matches_row_based_per_row` + `scope_key_columnar_matches_row_based` |
| `scope_key_columnar` vs `scope_key_from_values(extract_key_simple)` | 逐行返回同 `ScopeKey`（Utf8 / null / Int64 <2^53；`>2^53` Int64 为行式 `from_value` f64 丢精度的**既有**分歧，已断言方向一致） | ✅ `scope_key_columnar_matches_row_based` |
| 方案A 预分片 == 内联分片 | `precompute_shard_rows`（parse 侧）与 `partition_rows_by_key`（actor fallback）逐字节一致 | ✅ `precompute_shard_rows_equals_partition_rows_by_key` |
| `broadcast_batch_only` 发行子集 | (a) 预分片复用 (b) fallback 重分片 (c) config-drift 长度不符 → 三种都按 shard 发正确子集 | ✅ `broadcast_batch_only_sharded_sends_row_subsets` |
| 端到端 Q2：懒物化 sharded | EMIT `q2_mod_123`=747816 精确（=0.8129%×100M）、`[clean]`、窗口 append 100M/100M | ✅ EMIT 精确 + clean |
| 端到端 Q2：免物化收益 | EPS 较物化基线（~6-7M）提升，且正确性无损（EMIT 精确） | ✅ 实测 ~34M（方案A 拆 P0-③ + ScopeKey typed；受第二道墙残余约束，见 §9 注） |
| 行式 sharded 回退 | 非 columnar sharded 规则走 `sharded_sends` 物化分片，结果与改造前逐位一致（回归） | ✅ `sharded_broadcast_partitions_by_key_and_routes_same_key_together`（行式路径已覆盖） |
| `rule_task` 只扫 shard_rows | 每 shard 收到的是本 shard 行子集，总数 += 全批；不丢行、不重复 | ✅ `push_sharded_only_processes_shard_rows_subset` + Q2 EMIT 精确间接锁定 |

## 9. 验收顺序（方向既定，直接做）

1. **本次代码**（§6 全部管道）：✅ 完成
2. **对拍**：分片一致性 + ScopeKey 一致性 + 方案A 预分片一致性（§8）：✅ 完成
3. **端到端 Q2**：EPS 较物化基线（~6-7M）提升、EMIT 精确、`[clean]`：
   ✅ 完成——EPS ~34M（提升 ~5×），EMIT 747816 精确 + `[clean]`。
4. 回归 Q1/Q2/Q3/Q5/Q7/Q9 + seq：✅ 完成（全 `[clean]`，见附录实测表）
5. (P3) `FieldSource` + 命中行免物化 + guard 整列 kernel：**全部已实现（✅）**——
   guard 整列 kernel（commit `d7360f8`）+ `FieldSource` trait / 命中行 `ColumnarEvent`
   直喂（本 commit）+ defer 门槛放宽（无 filter match 也 deferred）。Q2 吞吐仍顶在
   第二道墙（100m 实测 EPS ~34M），见附录；Q5/Q7 由此消除 eager 整批物化。

> **§9.3 注：为什么 Q2 停在 ~34M（路线回顾）**
>
> Q2 的有状态本质决定它**不可能**逼近 receive-only ~90M（跳过 route_parse + 窗口 +
> 规则的纯无状态解码上限）。本轮分三步拆墙：
> - **第一道墙（parse 侧全量 `batch_to_events` 物化）**：本设计拆除 → Q2 ~6-7M → ~18M。
> - **窗口 actor 单写者热路径上的 O(batch) 重分片（P0-③ 的一部分）**：方案A 把分片
>   挪到并行 parse 侧（`precompute_shard_rows`），actor 仅零成本复用 → 避免串行瓶颈。
> - **Int64 键经 `f64` 往返的逐行开销**：ScopeKey typed 直读原生值消除 → Q2 ~18M → ~34M。
>
> **残余第二道墙**：`columnar-execution-progress.md` Step 8 / §5.2 记录的「Q2 的 EPS 门是
> **窗口 actor 单写者（P0-③）**」——actor 仍按序 `append`/`broadcast` 整批（含非 sharded
> 订阅的 `batch_arc` clone、各 shard 的 `RulePush` 构造）。这是 sharded match 无法用
> 「Q1 旁路窗口 actor」方式拆掉的（Q1 无状态可旁路，Q2 有状态必须保序）。**要继续提 Q2，
> 下一步是拆窗口 actor 单写者这一道墙（actor 内部并行化 / 分片 append），而非 rule 侧**——
> 不是以 90M 为目标的失败，而是两道墙模型下拆到第二道墙残余的正常结果。

## 10. 风险

- **中**：sharded 广播从「events=Some」转「events=None」改变了 rule task 收到的表示，
  需对拍分片一致性防「某些行漏发 / 多发到错 shard」，这直接破坏有状态语义。
  （已用 `scope_key_columnar_matches_row_based` + `precompute_shard_rows_equals_partition_rows_by_key`
  + `broadcast_batch_only_sharded_sends_row_subsets` 三道对拍锁死。）
- **低（初版误述已更正）**：`Arc<RecordBatch>` 在 `broadcast_inner` 入口**每广播一次**
  构造一个（`Arc::new(b.clone())`，仅克隆 RecordBatch 外壳——列都是 `Arc<Array>`，成本极低），
  各 shard 仅 `batch_arc.clone()`（refcount 增量、零拷贝）。**不是每 shard 一份副本**。
  需确认 `Arc<RecordBatch>` 生命周期（TimedBatch 已持 batch，Arc 不复制列）。
- **低（已解决）**：命中行物化已由 P3 消除——`materialize_rows[_filtered]` 从生产路径删除，
  命中行以 `ColumnarEvent::with_index` 直喂状态机（零 HashMap）。
- **中（P3 已实现，收益暂未兑现）**：guard 整列向量化 kernel 对 Q2 无吞吐增益——
  因 Q2 不是 guard CPU 受限，而是窗口 actor 单写者（P0-③）墙受限；待拆该墙后
  guard kernel 的 CPU 收益才可能变现。
- **中（建议）**：`broadcast_inner` 的 `(None, None)` 臂仍是 `debug_assert!(false)`。
  release 下 `debug_assert` 编译为空，若未来某 producer 忘了带 batch，会**静默**发错分片、
  损坏有状态语义而非 panic。建议改为 `log::error!` + 跳过该 shard（与审查历史 N4 建议一致）。

---

## 附录：实现状态与实测（2026-08-19）

### 代码改动（P2 全部管道，与 §6 设计一致）

| 文件 | 改动 |
|---|---|
| `wf-engine/src/window/fanout.rs` | `RulePush.shard_rows`（本 shard 子集）；新增 `column_scalar` / `scope_key_from_column` / `scope_key_columnar`（typed ScopeKey 直读原生值，替代初版 `extract_key_columnar`+FNV）/ `partition_rows_by_key`（typed ScopeKey fallback）/ `precompute_shard_rows`（parse 侧预分片）；`broadcast_inner` Sharded 分支「二者择一」（events=Some 行式 `sharded_sends` 分片 / events=None 复用预分片，长度不符才 `partition_rows_by_key` 防御性重分片；各 shard 共享 `batch_arc`） |
| `wf-engine/src/window/router.rs` | `route_parse`：删 `&& !has_sharded_subscribers` 合取项（sharded 也能 defer，未新增 `columnar_match_safe`）；对 defer 窗口调用 `precompute_shard_rows` 并把整批 per-shard 行索引子集装入 `ParsedWindow.shard_rows`（`has_sharded_subscribers` 已随删除） |
| `wf-engine/src/match_engine/mod.rs` | `pub(crate)` re-export `field_ref_name`、`scope_key_shard_index` 等 |
| `wf-runtime/src/engine_task/rule_task.rs` | `process_batch` 加 `shard_rows: Option<&[u32]>` 形参；`row_domain` 按 shard 子集遍历（行域相对 `DeferredRows` + 绝对行恢复），defer 块 + 主循环一致 |
| `wf-runtime/src/engine_task/tests.rs` | 全部 `RulePush` 构造补 `shard_rows`（None 或本 shard 子集）；新增 `push_sharded_only_processes_shard_rows_subset` 等 |

### 对拍（wf-engine 全绿）

- `partition_rows_matches_row_based_per_row`：列式子集 vs 行式 `sharded_sends` 逐行同 shard（含 null key → shard0）
- `scope_key_columnar_matches_row_based`：逐行同 `ScopeKey`（Utf8 / null / Int64 <2^53 与 >2^53 行式 `from_value` f64 精度分歧方向一致）
- `precompute_shard_rows_equals_partition_rows_by_key`：parse 侧预分片与 actor fallback 逐字节一致

### 完整回归（nexmark_pk 100m，全 `[clean]`、window append 100M/100M）

| 查询 | EPS | 说明 |
|---|---|---|
| Q1 | ~11M | on-each，无回归（另有 Q1 f64 直写优化，见 CHANGELOG） |
| Q2 | **~34M** | 免物化 sharded + 方案A 拆 P0-③ + ScopeKey typed，较物化基线 ~6-7M **~5×**；EMIT 747816 精确 |
| Q3 | ~18M | join，无回归 |
| Q5 | ~3.7M | count 状态，无回归 |
| Q7 | ~3.5M | 窗口 MAX 状态，无回归 |
| Q9 | ~20M | join，无回归 |
| seq（单测） | 全过 | 状态机无副作用 |

> 注：本表 Q2 已更新到方案A + ScopeKey 后的 ~34M 实测；其余查询保留免物化初版量级
> （方案A 仅作用于 sharded 路径，对其余查询无影响，预期无回归，建议重测 baselining）。

### 遗留与后续

1. **Q2 EPS ~34M，残余第二道墙「窗口 actor 单写者（P0-③）」约束**——
   `columnar-execution-progress.md` Step 8 已记录。本轮已拆两层墙（parse 物化 + actor 内
   重分片），并消除 Int64 键 f64 往返；**剩余的是窗口 actor 单写者本身的串行
   `append`/`broadcast`**（含 `batch_arc` clone、各 shard `RulePush` 构造）。
   **注意：以 ~90M（receive-only 无状态上限）为目标不成立**——有状态 match 物理上必须
   经过窗口 actor，见 §9.3 注。继续提升需 actor 内部并行化 / 分片 append。
2. **行式 sharded 回退已单测**（✅ `sharded_broadcast_partitions_by_key_and_routes_same_key_together`）：
   非 columnar sharded 规则走 `sharded_sends` 物化分片，与改造前逐位一致。
3. **P3 guard 整列 kernel 已实现并对 Q2 无吞吐增益**：
   把 `columnar.rs` 的逐行求值升级为整列 kernel（`CVec`{Int/Float/Str/Bool} +
   `eval_vec` + 各整列算子，复用 `compare_scalars`/`arithmetic` 保证语义逐字节一致，
   删去逐行 `eval_cx`/`cx_logic_*`），`eval_guard_columnar` 接口不变；P2 vs P3 在方案A
   后的 Q2 实测 ~34M vs ~34M（噪声带）、CPU/RSS 无明显改善——**无吞吐/CPU 提升**。
   原因：Q2 已是窗口 actor 单写者墙受限，guard CPU 非瓶颈；Q1 无 filter 不走 guard kernel。
   **guard kernel 收益须待拆第二道墙后评估**；优化点：常量列 `lit_vec` 整列物化可改标量融合。

### 实现过程中修复的既有 bug

- `process_batch` 主循环原用 `batch.num_rows()` 定界，relay/Eager push（events=Some, batch=None）
  误得 0 行 → 主循环空转 → `downstream_close_*` 测试 hang。
  改为事件优先（`events.len()`，其次 `batch.num_rows()`），2 个 hang 测试恢复通过。
