# 方案 B 设计稿：Window → Rule 推送模型（Push + 解析 Worker 池）

> **状态更新（2026-08-16）：已落地**——P0/P1（解析 worker 池 + 规则通道化）与
> window actor 化在 commit 5c354fb 合入（q1 30M 4.18M/s，semaphore 等待消失）。
> 落地细节与 v2 拓扑见
> [window-channel-actor-design.md](window-channel-actor-design.md)（实现版）；
> log 结构与驱逐见
> [window-log-eviction-design.md](window-log-eviction-design.md)。本文保留为
> 设计推理与 wp-motor 对照的原始记录。
>
> **原状态：Design（待评审）**
>
> 2026-08-13 · 参考实现：[wp-motor](https://github.com/wp-labs/wp-motor)
> （连接级 picker + pending 缓冲 + 解析 worker 池 round-robin 分发）
> · 关联：[window-push-consumer-model.md](window-push-consumer-model.md)（候选分析）、
> [window-push-implementation-plan.md](window-push-implementation-plan.md)（实施计划）、
> [architecture.md](architecture.md)（现行 Cursor+Notify pull 模型）

---

## 1. 背景与动机

### 1.1 现状瓶颈（Nexmark_PK 实测）

当前 pull 模型（Cursor+Notify）在逐项优化后（metrics 分片、alert MP2C、
内存 base×N、SmolStr、foldhash），matches 2.37M 完成 ~10.6s（基线 ~20s）。
剩余剖面：

- **`semaphore_wait_trap` ≈ 9000**：规则 `events_since` 读锁 vs receiver/evictor
  写锁（窗口 `RwLock`）竞争 —— 非 CPU，是等待；
- 方案 A（route 侧锁外预解析）实测**净负**：OnceLock 竞争只占 semaphore ~4%，
  而解析放接收侧拖慢入口。见 [window-push-consumer-model.md](window-push-consumer-model.md)。

**结论**：锁竞争源于「窗口 `RwLock` + 规则 pull」的共享结构，不是解析位置或
OnceLock。根治必须把规则数据面从窗口锁移出。

### 1.2 wp-motor 的实证

wp-motor 用 wp-core-connectors 的连接级 reader + **push 分发**，效果好：

```
DataSource（wp-core-connectors，每连接 reader）
  → SourceWorker/JMActPicker（每连接，突发拉取到 pending 缓冲）
  → handle_pending_batch → ParseDispatchRouter（round-robin）
  → N × ActorWork（解析 worker，各自 channel）
  → ActParser::parse_events()   ← 解析在 worker 内
```

特征：连接级 picker + pending 缓冲 + worker 池 round-robin 分发 + **全程 channel
解耦、无共享 RwLock**。接收（picker）与处理（worker）被缓冲/channel 解耦。

## 2. 目标架构

wp-reactor 借鉴 wp-motor，但适配 CEP（有状态）差异。核心：**把窗口从规则
数据面移出（只留控制面），引入「解析 worker 池 + channel 分发」**。

```
TCP（wp-core-connectors 每连接 reader）
  │ RecordBatch（Arrow 解码，顺序）
  ▼
解析 channel（pending 式缓冲，round-robin）        ← 新增
  │
  ▼
N × 解析 worker（并行 batch_to_events，每批一次）    ← 新增：解析在这里
  │ Arc<Vec<Event>>（wp-reactor#19 共享，不重复物化）
  ├──────► 广播：写回窗口缓存（Arc，控制面/join/晚订阅用）
  └──────► 每规则一个 channel：规则 worker 直接收 Arc   ← 新增：规则数据面走 channel
              │
              ▼
规则 worker（原 run_rule_task，可从 channel 收 + 保留 timeout 扫描）
  └─► alert（MP2C）→ sink

窗口（保留，控制面）：watermark / timeout / eviction / join / 中间窗口
```

## 3. 详细设计

### 3.1 组件与职责

| 组件 | 现状 | 目标 | 说明 |
|------|------|------|------|
| **source task**（TCP） | receive_batch → route | 仅 receive_batch → 推解析 channel | 解码顺序，不做解析 |
| **解析 channel + pending** | 无 | 新增 | 有界缓冲，突发拉取/分批投递（wp-motor pending 模式） |
| **解析 worker 池** | 无（规则读时 OnceLock 解析） | 新增 N 个 | round-robin 收 batch，`batch_to_events` 并行，一次解析 Arc 广播 |
| **规则 channel** | 无 | 每规则一个 | 解析 worker 广播 Arc 到每规则 |
| **规则 worker** | run_rule_task（pull 窗口） | 从 channel 收 Arc + 保留 timeout 扫描 | CEP 状态机不变 |
| **窗口** | 数据面（规则读）+ 控制面 | **仅控制面** | watermark/timeout/eviction/join/中间窗口 |

### 3.2 解析位置（决策）

**解析在「解析 worker 池」**（wp-motor ActorWork 模式），不在接收侧、不在规则读侧：

- ✅ **不拖慢接收**：source 只解码 RecordBatch，推走；
- ✅ **解析可并行**：N worker 分摊，2M 事件 ÷ N；
- ✅ **一次解析、Arc 广播**：保住 #19（不重复物化）；
- ✅ **规则数据面无窗口锁**：从 channel 收 Arc。

### 3.3 广播分发（与 wp-motor 的关键差异）

wp-motor 每个 batch 只到一个 worker（无共享）；wp-reactor 的 **12 规则都要同一批
事件**（各自 CEP 状态）。所以：

- **解析层**：round-robin 到 N 解析 worker（每批解析一次）—— 与 wp-motor 一致；
- **规则层**：解析结果 **Arc 广播**到每规则 channel（不能 round-robin，规则要全量）；
- **单规则内部**：按 `match key` 哈希分片（`executor_parallelism`，架构候选），
  同 key 事件到同分片（CEP 状态一致）。

### 3.4 窗口：控制面职责

窗口保留 `TimedBatch { batch, parsed_events: Arc }`（双存）：

- `batch`（RecordBatch）：内存控制、schema 校验、join/`has()` 快照、watermark 提取；
- `parsed_events`（Arc）：解析 worker 写回缓存，供 join/晚订阅/close 复用；
- 规则数据面不再经窗口读锁（改走 channel）。

## 4. 必须解决的关键难点

| # | 难点 | 设计决策 |
|---|------|---------|
| 1 | **解析 worker 池大小 / pending 策略** | 复用 `executor_parallelism` 配置；pending 有界（字节/批数），突发拉取 + 分批投递（wp-motor round_pick 模式） |
| 2 | **广播背压** | 某规则慢 → 其 channel 满 → 解析 worker 对它的广播阻塞。需决策：阻塞（全局背压）/ 丢弃（记录 gap）/ 独立缓冲 |
| 3 | **规则分片一致性** | 按 `match key` 哈希；跨分片的 `conv`（sort/top/dedup）、`max_throttle`、规则级指标需聚合设计（架构候选已列风险） |
| 4 | **watermark/lateness** | route/append 到窗口必须按序（watermark 按 append 推进）。解析 worker 并行但**提交到窗口按 seq 重排**，或窗口接受乱序（按时间列排序） |
| 5 | **timeout/close** | 规则 worker 保留 timeout 扫描（现有 `scan_timeouts`），窗口水位经 channel 或共享水位推给规则 |
| 6 | **中间窗口** | 规则输出 → 中间 window → 下游规则：中间窗口作为「生产者」，走同一 push 链（append 后广播） |
| 7 | **hot reload** | 规则重建时，其 channel 重新绑定（解析 worker 广播目标更新，`ParseDispatchRouter.replace` 模式）。语义：reload = 重新开始匹配，存量不回放（见 `wfl-design.md` §11.1） |
| 8 | **shutdown** | 保证 channel 排空 + 规则 flush（现有两阶段 cancel 语义不变） |

## 5. 与 wp-motor 的差异（不可照搬处）

| | wp-motor | wp-reactor |
|--|----------|-----------|
| 数据单元 | `SourceEvent/SourceBatch`（事件批） | `RecordBatch`（Arrow）→ `Event` |
| 处理语义 | 无状态 parse，可 round-robin | **有状态 CEP**，规则要全量事件 + 按 key 分片 |
| 输出 | sink 导向，无共享窗口 | 有窗口（watermark/timeout/join/中间窗口） |
| 广播 | 无（一份数据一条链） | **必须广播**（12 规则共享同一批） |

## 6. 落地步骤（阶段划分）

1. **P0 解析 worker 池**：source 解码后推 channel → N worker 并行解析 → Arc 写回
   窗口缓存（不动规则读路径，先用 `append_parsed` + 规则仍从窗口读 Arc，验证解析
   并行收益与 receiver 解耦）。
2. **P1 规则 channel**：解析 worker 广播 Arc 到每规则 channel，规则改从 channel 收
   （数据面脱离窗口锁），窗口锁只留控制面。此步验证 semaphore 是否消失。
3. **P2 规则内分片**：单规则按 `match key` 哈希到 N shard worker
   （`executor_parallelism`），处理 conv/限流/指标聚合的跨分片一致性。
4. **P3 中间窗口贯通**：push 链路贯穿中间窗口。

每阶段独立可测、可回退。

## 7. 风险

- **改动量大**（架构级，跨 source/解析/规则/窗口/中间窗口/reload）；
- **广播背压**语义需仔细设计（阻塞 vs 丢弃），直接影响端到端延迟与内存；
- **规则分片**的 conv/限流/指标一致性复杂；
- 收益主要来自 semaphore（~9000 样本）与 receiver 解耦，**需 P1 后实测确认**
  （避免类似方案 A 的"理论对、实测无"）。

## 8. 关联

- [window-push-consumer-model.md](window-push-consumer-model.md) —— 候选 A/B 分析、
  方案 A 实测证伪记录
- [architecture.md](architecture.md) —— 现行 pull 模型 + `executor_parallelism` 候选
- [wp-motor](https://github.com/wp-labs/wp-motor) —— 参考实现（picker + pending + worker 池）
