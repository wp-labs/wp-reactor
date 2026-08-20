# Window Actor Pull 模型落地设计

> 状态：起草（2026-08-19）
> 关联：`ISSUE_q5_100m_freeze.md`（q5 100M 间发冻结）、`columnar-match-state-machine.md`
> 定位：q5 冻结的**根因级**修复（止血见 ISSUE §4.1 的 `RULE_CHANNEL_CAPACITY=256`）

---

## 0. 背景与动机

q5 100M 的间发冻结（~1/3 概率，1600s 超时卡在 ~99M）根因是 **window actor 单写者把两件正交的事耦合进同一个 `await`**：

1. **写共享窗口状态**：`append` 批、按 `next_window_seqs` 推进 seq —— 必须串行、强一致。
2. **分发批给 N 个独立消费者**：每条 rule 消费速度不同，本该并发。

当前用「(1) 之后 `await` (2) 全完成」（`actor.rs` `commit_append` → `commit::commit_appended_batch` → `fanout.broadcast_batch_only` → `join_sends` 30 路阻塞广播）来**隐式保证 window-lookup 一致性**。这是**用全局串行化换取一致性**：任一条慢 rule（瞬时 GC / 锁 / 残留 `recalibrate_memory` 扫描）使其 channel 填满 → actor 卡死 → mailbox 堆积 → 字节预算耗尽 → receiver 停收 → `append_total` 永远追不平 TOTAL → 冻结。

**止血**（`RULE_CHANNEL_CAPACITY` 32→256）没动根因：256 深度下任一「持续 >3.5s 的单 rule 滞后」仍会卡，且用内存（Arc 保活批）换吞吐余量。

**关键 insight（正确性）**：当前「阻塞保证」其实只是**近似一致**——它只保证「规则*收到*第 N 批时窗口不含 N+1」，并不保证「规则*处理*第 N 批时窗口不含 N+1」（收到后、处理前 actor 可能已 append N+1）。本设计的 seq 水位机制可将其**严格化**。

---

## 1. 目标与非目标

**目标**
- 消除 actor 被任何消费者阻塞（冻结根因消失）。
- 保留 Q2 列式分片收益：**分片计算零重复**（不退回每 shard 自行分片）。
- 内存模型优于 push：未消费批在 window **共享一份** + 消费感知驱逐（ack floor），而非每条 rule channel 各缓冲一份 Arc。
- 一致性可严格化（seq 水位，§3.5）。

**非目标**
- 不动状态机语义（`CepStateMachine` / `FieldSource`）。
- 不重写 window 存储（`RwLock<BTreeMap<u64, TimedBatch>>` 保留）。
- 不消除「非列式安全规则」的整批物化 fallback（结构必需）。

---

## 2. 现状接口（已实测确认，改动面比预期小）

| 位置 | 现状 | 对本设计的意义 |
|---|---|---|
| `buffer/cursor.rs:15` `read_since` | 返回 `Vec<RecordBatch>`（列式，Arc clone 零拷贝） | **列式 pull 接口已存在** |
| `buffer/cursor.rs:45` `events_since` | 返回 `Vec<Arc<Vec<Arc<Event>>>>`（行式） | 当前 pull 路径用的（要换掉） |
| `engine_task/mod.rs:48-52` | `push_rx.take()` 有值走 push，否则走 `run_pull_loop` | 切换点 |
| `engine_task/mod.rs:110` `run_pull_loop` | `task.pull_and_advance().await` + notify 等待 | pull 主循环 |
| `rule_task.rs:391` `pull_and_advance` | `events_since(cursor)` 行式 → `process_batch(..., Some(events), None, None, None)` | **主要改写点** |
| `rule_task.rs:431` | `first_batch_seq = new_cursor - events_list.len()`（反推 seq） | pull 下 seq 可由连续性反推 |
| `rule_task.rs:454` `process_batch` | 已支持 `batch: Option<&RecordBatch>` + `shard_rows: Option<&[u32]>` 列式入口 | **可直接复用**，pull 不必另写列式子集逻辑 |
| `rule_task.rs:176` `cursors` / `:265` `WindowProgress::release` | per-window cursor + ack slot | pull 天然契合消费感知驱逐 |
| `router.rs:165` `next_window_seqs` | 每 (source,window) 连续 seq | seq 水位基础 |
| `router.rs:319` `precompute_shard_rows` | **parse 侧已算好** per-shard 分区（方案A） | 分片零重复的关键（§3.4） |
| `actor.rs:256` `commit_append` | 内联 `await` 广播 | **去阻塞点** |
| `lifecycle/spawn.rs:23` `RULE_CHANNEL_CAPACITY=32→256` | rule channel 深度 | pull 下 channel 不再承载广播，可降回 |

**结论**：列式 pull 接口（`read_since`）、列式处理入口（`process_batch` 的 `batch`+`shard_rows`）、parse 侧预分片（`precompute_shard_rows`）**全已就绪**。核心改动是把 pull 路径从「行式 events」切到「列式 batch + 复用 `process_batch`」，并让 actor 不再 `await` 广播。

---

## 3. 设计

### 3.1 数据模型

```rust
// buffer/types.rs — TimedBatch 增加 per-shard 行索引（来自 parse 侧预分片）
pub struct TimedBatch {
    pub seq: u64,
    pub batch: Arc<RecordBatch>,
    pub shard_rows: Option<Arc<Vec<Vec<u32>>>>>, // [shard] -> 本 shard 行子集；unsharded=None
    // …既有字段
}

// rule_task.rs — RuleTask 增加本实例 shard 索引
pub(super) shard_index: Option<usize>, // spawn 时从 Subscription::Sharded 索引获知；unsharded=None
```

### 3.2 Actor 路径（去阻塞）

`commit_append` 改为：

```
append 批 → 填 TimedBatch.shard_rows（从 ParsedWindow.shard_rows 取，零 await）
         → 推进 committed_seq
         → notify(per-window Notifier)         // 仅通知，不等消费者
// 删除 join_sends / broadcast_batch_only 的 await
```

actor 自此**永不等待任何消费者**。广播语义从「actor push 30 路」变为「notify + 消费者自 pull」。

### 3.3 Rule 路径（列式 pull）

`pull_and_advance` 改写：

```rust
pub(super) async fn pull_and_advance(&mut self) {
    let mut pending = Vec::new();
    for source in &self.sources {
        let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
        // 列式：直接读 RecordBatch，按本实例 shard_index 取子集
        let (batches, shard_rows_per_batch, new_cursor, gap) =
            source.window.read_since_with_shard(cursor, self.shard_index);
        // …gap 处理（同现状）…
        self.cursors.insert(source.window_name.clone(), new_cursor);
        let first_seq = new_cursor.saturating_sub(batches.len() as u64);
        for (i, batch) in batches.iter().enumerate() {
            let seq = first_seq + i as u64;
            let shard_rows = shard_rows_per_batch[i].as_deref(); // 本 shard 行子集（unsharded=None）
            self.process_batch(&source.window_name, seq, None, Some(batch),
                               shard_rows, self.materialize_fields.as_deref()).await;
            if let Some(slot) = self.progress.get(&source.window_name) {
                slot.store(seq + 1, Ordering::Release); // ack（消费感知驱逐，已有）
            }
        }
    }
    self.update_rule_instances_metric();
}
```

`read_since_with_shard(cursor, shard_index: Option<usize>) -> (Vec<RecordBatch>, Vec<Option<Arc<Vec<u32>>>>, u64, bool)`：
- `shard_index=None`（unsharded rule）：返回 `(batches, vec![None; n], …)`，处理整批。
- `shard_index=Some(i)`（sharded rule）：返回每批的 `TimedBatch.shard_rows[i].clone()`（Arc clone，零拷贝），只处理本 shard 行——**零重复分片计算**。

### 3.4 分片零重复（P2：保留预分片，关键）

parse 侧预分片（`router.rs:319 precompute_shard_rows`）已把 per-shard 分区算好。actor `append` 时将其存入 `TimedBatch.shard_rows`，shard 实例 pull 时取本 shard 子集。

> **为什么必须保留预分片（不能每 shard 自行分片）**：若 pull 下每个 shard 实例自行 `scope_key_columnar` 分片，分片计算从 actor 1 次变为 N_shard 次。Q2（`auction:10m`，shard 数≈规则实例数 16-30）的列式 EPS 会从 ~34M 跌回 ~6-7M（方案A 的收益被吃掉）。故采用 P2（预分片结果随批存 window，shard 实例零重复取子集）。

### 3.5 seq 水位一致性（严格化，M2 里程碑）

规则处理第 N 批时，`window_lookup` 用 **`max_seq = N`** 作可见性上限（只见 seq ≤ N 的批）。

> **状态：已实现（M2 完成）。** 落点为：
> - `wf-engine/src/window/buffer/mod.rs` 新增 `Window::snapshot_up_to(max_seq: Option<u64>)`（`None` = 全量，`Some(n)` = `log.range(..=n)`）。
> - `wf-runtime/src/engine_task/window_lookup.rs` 的 `RegistryLookup` 改为携带 `max_seq`；`snapshot` / `snapshot_field_values` / `snapshot_with_timestamps` / `join_lookup` 均按 `max_seq` 过滤（has-cache 与 join index 因不感知 seq，仅在 `max_seq=None` 时启用；有水位时退回扫描）。
> - `wf-runtime/src/engine_task/rule_task.rs` 的 `process_batch` 新增 `lookup_max_seq` 参数：pull 路径传 `Some(batch_seq)`（当前批 seq），push 路径传 `None`（保留 push 的近似一致，不改变生产 push 行为）。
> - 顺带修复：原 `RegistryLookup::join_lookup` 通过 `WindowLookup::join_lookup(self, ..)` 委托默认扫描会**递归回自身**（潜在栈溢出，此前从未被测试覆盖），已改为内联扫描并复用公开的 `values_equal`。

- 当前（近似）：阻塞只保证「收到 N 时不含 N+1」。
- 目标（严格）：`max_seq=N` 保证「处理 N 时只见 ≤N」——消除多算 N+1 的隐患。
- 分期：M1 先消除 actor 阻塞（止血核心），M2 再严格化一致性（正确性增强，非止血必须）。

### 3.6 内存与 ack floor

pull 天然契合**消费感知驱逐**（已在）：每条 rule 独立 cursor + ack（`slot.store(seq+1)`）；慢 rule 不消费 → 该批不被驱逐 → 内存涨（正确代价）。

优于 push：push 下未消费批在**每条** rule channel 各缓冲一份 Arc（保活整批列数据）；pull 下 window **共享一份** + ack floor 精确控制。单条持续滞后 rule 的内存占用远低于 push 的 N 份 Arc 缓冲。

---

## 4. 改动面清单

| 文件 | 改动 |
|---|---|
| `wf-engine/src/window/buffer/types.rs` | `TimedBatch` 加 `shard_rows: Option<Arc<Vec<Vec<u32>>>>`；`append` 填该字段 |
| `wf-engine/src/window/buffer/cursor.rs` | 新增 `read_since_with_shard(cursor, shard_index)`（复用 `read_since` 的读锁视图） |
| `wf-engine/src/window/actor.rs` | `commit_append` 去 `broadcast` await，改 `notify`；删除 `join_sends` 调用 |
| `wf-engine/src/window/commit.rs` | `commit_appended_batch` 不再 await 广播 |
| `wf-engine/src/window/router.rs` | `ParsedWindow.shard_rows` 透传（已有），`append` 时填充 `TimedBatch` |
| `wf-engine/src/window/fanout.rs` | `broadcast_batch_only` 降级为 fallback / 测试专用，生产路径不再调用 |
| `wf-runtime/src/engine_task/rule_task.rs` | `pull_and_advance` 改 `read_since_with_shard` + `process_batch` 列式入口；加 `shard_index` 字段；ack 逻辑已有 |
| `wf-runtime/src/engine_task/mod.rs` | `run_rule_task` 默认走 pull（或 config 开关），保留 push 回退 |
| `wf-runtime/src/lifecycle/spawn.rs` | 不建 push channel（走 pull）；填 `shard_index`；`RULE_CHANNEL_CAPACITY` 可降回（pull 不用 channel） |
| `wf-engine/src/window/buffer/mod.rs` | 新增 `Window::snapshot_up_to(max_seq)`（M2 seq 水位，`None` 全量 / `Some(n)` 读 `log.range(..=n)`） |
| `wf-runtime/src/engine_task/window_lookup.rs` | `RegistryLookup` 加 `max_seq` 字段，`snapshot`/`has`/`join` 按 seq 过滤；`join_lookup` 内联扫描替代递归委托（M2 里程碑） |

---

## 5. push / pull 切换与回退

- **默认 pull**；保留 push 路径（channel + fanout）作为回退。
- config 开关（如 `window_dispatch = "pull" | "push"`）便于 A/B 与紧急回退。
- 切 push 即恢复当前生产行为（256 止血保留，不冲突）。

---

## 6. 风险

- **单批延迟**：pull 的 notify/wait 模型 vs push channel 直推，单批延迟可能略增。需 bench 验证 EPS 不退化（预期不退化——actor 不再阻塞的增益 > notify 开销）。
- **seq 反推依赖连续 seq**：window seq 由 `next_window_seqs` 连续分配，pull 每窗口单 cursor，连续成立。若未来引入空洞，改 `read_since_with_shard` 显式返回 `Vec<u64>` seq。
- **eviction gap**：pull 慢 rule 不消费 → 批不被驱逐 → 极端内存涨。已有 `scan_timeouts` 兜底；`cursor gap` 告警已有。
- **shard_rows 缺失兜底的跨 shard 重复**：`read_since_with_shard` 对 key-partitioned 批在 `shard_rows` 缺失 / 索引越界时返回 `None`，该 shard 实例退为整批处理。若多个 shard 同时命中同一批（热加载保留的历史批、或规则热更新改了 `shard_count`），该批会被 N 个 shard 各整批处理一次 → 跨 shard 重复消费。这是**有损不损、仅重复**的防御权衡（与 at-least-once ack 一致），且 `rule_task::pull_and_advance` 在命中对每个 shard 触发 warn 告警，便于观测；存储正确路径不受影响。

---

## 7. 验证计划

1. `./bench.sh q5 cont 100m` ≥3 次：冻结率 1/3 → **0**。
2. **Q2 EPS 不退化**（~34M）：确认 P2 分片零重复（核心验收）。
3. 正确性对拍：Q2 EMIT=747816 精确 + `[clean]`；Q1/Q3/Q5/Q7/Q9 全 `[clean]`。
4. RSS 对比：pull vs push（256）。
5. 回归：`cargo test -p wf-engine -p wf-runtime` 全套。

---

## 8. 里程碑

- **M1（止血核心）**：actor 去阻塞 + pull 列式（P2 分片零重复）+ 默认 pull。验证冻结消失、Q2 不退化。
- **M2（严格一致）**：`window_lookup` `max_seq` 水位（近似一致 → 严格一致）。**已完成。**
- **M3（清理）**：push 路径降级为 fallback / 测试；删 `RULE_CHANNEL_CAPACITY` 调参；更新 `ISSUE_q5_100m_freeze.md` 标记根因已解。
