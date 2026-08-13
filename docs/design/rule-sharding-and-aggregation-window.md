# 规则分片 + 管道（Pipe）/ 变换算子 设计

> 状态：Design（草案）；**Pipe 落地进展：P1a/P1b/P1c 已实施**（见「Pipe 落地」一节）
>
> 2026-08-13 · 关联 [window-push-model-design.md](window-push-model-design.md)（push 架构）、
> [window-push-implementation-plan.md](window-push-implementation-plan.md)（实施计划，P2 规则分片）

## Pipe 落地（2026-08-13）

- **P1a**：`wf-engine/src/pipe/` — `Pipe`（name/schema/over）+ `PipeRegistry`。
- **P1b**：bootstrap 从 yield 拓扑构建 PipeRegistry，接线进 rule task；emit 的中间 schema
  优先取 pipe registry。
- **P1c**：**中间 Pipe 纯 relay**——`emit_window_record` 不再 `append_intermediate`（不存窗口），
  直接 `fanout().broadcast(events)` 给下游规则；下游规则的 match 时间窗由自己的
  `CepStateMachine` 承担（水位来自事件时间戳）。测试已从 pull 迁移到 push。
- **待办**：输出/中间窗口从 WindowRegistry 移除（需下游规则 WindowSource 改指 pipe）；变换算子。

---

## 0. 术语（已定）

| 概念 | 名字 | 说明 |
|---|---|---|
| **源数据流** | stream（保留） | 输入侧，`stream_tag` → window |
| **管道** | Pipe | 规则 `yield` 目标 → 订阅者，`\|>` 即管道 |
| **管道订阅者** | 规则 / sink / 变换算子 | 挂在同一 `PipeFanout` 上 |
| 管道属性 | `over` | 见下「over 语义修正」——push 模型下 Pipe 是纯透传 relay，下游 match 由下游规则自己的 `CepStateMachine` 承担 |
| **变换算子** | Transform Operator | 轻量订阅者，攒批 + conv + emit |

> **over 语义修正（2026-08-13）**：原「`over>0` 保留（供 match 时间窗）」在 push 模型下**不成立**。
> 下游规则 `|>` 的 match 时间窗靠自己的 `CepStateMachine`（per-key 实例 + 机器水位来自事件
> 时间戳），**push 模式不读中间窗口 buffer**。中间窗口的 `over`（=下游 match 时长）只服务
> legacy pull / join——push 下冗余。**Pipe = 纯透传 relay**（无 buffer/watermark/eviction）。
> 唯一例外：若下游规则 join/has() 中间 Pipe 才需保留（`|>` 链当前不 join，可编译期拒绝）。

> 之前「输出流 / 聚合流」两个概念合并为**一个「管道」**：conv（聚合）是挂在管道
> 上游的一个**变换算子**预处理，不是第二条流。

## 0.1 已定案（决策日志）

1. **key 提取同源**：编译期单一 `extract_key(Event) -> Option<Key>`，dispatch 分区层与
   状态机共用；`None` = 无 key = 退单 worker。
2. **conv 批语义 = 时间窗**：conv 重新定义为时间窗聚合（`over` 为批触发周期），不是
   per-watermark 批；结果延迟 ≤ over（毫秒级）。
3. **conv 阶段形态 = 变换算子**：轻量 stateless 订阅者（订阅管道、按 over 攒批做
   `apply_conv` + emit），非 CEP 规则。
4. **close_all 协调 = channel-close**：shard 直接持有给变换算子的 sender，shutdown 时
   drop sender → 管道 channel 关闭 → 变换算子 drain + 最终 conv；EOS 由周期 conv 覆盖。
   （放弃 barrier）
5. **conv = fixed 专属**：conv 只对 fixed（tumbling）窗口有意义；变换算子 `over` =
   fixed 的 `dur`（桶长），批 = 整桶结果集，触发 = 桶边界；sliding 不支持 conv。

---

## 1. 背景与目标

push 改造（R1/R2/content_bytes/sink-fanout）已把 ingest 与 alert 投递从锁竞争里解耦，
但吞吐天花板始终 ~12s/2M —— 瓶颈是**单规则 worker 串行推进 CEP 状态机**（`advance_at_with`
每事件每 key 的 match 开销），不是 ingest、不是锁、不是解析、不是 alert 投递。

目标：**把单个规则的 per-key match 并行化（规则分片），并对任意规则（含 conv / 限流）
保持语义正确**——不是只给 PK 打补丁，是给实际使用。

---

## 2. 总体架构

核心洞察：CEP 规则的状态天然分两类，分开处理。

| 状态 | 作用域 | 频率 | 处理 |
|---|---|---|---|
| **实例状态**（count/avg/max 累加器） | per-key | 热（每事件每 key） | **分片并行** |
| **全局状态**（conv 结果、限流计数、实例预算） | 跨 key | 冷（close/周期/close_all） | **聚合窗口 + 共享原子** |

```
源 → window → 按 key 哈希分区 ─┬─► shard 0（per-key match）─┐
                              ├─► shard 1（per-key match）─┤  close 原始输出
                              └─► shard N（per-key match）─┘
                                                          ▼
                                    自动生成的「conv 聚合窗口」（中间窗口）
                                                          ▼
                                    conv 阶段（sort/top/dedup + 限流 + emit）
                                                          ▼
                                                       sink fanout
```

- **分片的**只有 per-key 的 `advance_at`（热路径）。
- **集中的**是跨 key 的 conv/限流/emit（冷路径），以「自动生成的中间窗口 + 下游 conv 阶段」表达，复用 R1 已打通的中间窗口底座。

---

## 3. 分片机制

### 3.1 key 提取（hoist 到 dispatch 层）

现在 `advance_at` 内部才提取 match key（`InstanceKey`）。分片要求**在广播前**就拿到 key。
改动：把 key 提取逻辑从 `CepStateMachine::advance_at` 上提，编译期生成一个
`key_extractor(Event) -> Option<Key>`（复用 `MatchPlan.keys`），挂在 router 的分区步骤上。

- 无 key 的规则（`keys: Vec::new()`）不可按 key 分片，见 §6。

### 3.2 批分区（batch → N 子批）

现在 `Router::route` 广播整批 `Arc<Vec<Event>>` 到规则 channel。分片后广播要变成**分区**：
对批内每个事件算 `hash(key) % N`，拆成 N 个子批，分别投到对应 shard 的 channel。

- 这是新增的一次 O(batch) 分区开销，但比 `advance_at` 的 match 便宜得多。
- 分区在**解析 worker 之后、广播之前**（复用 R2 的 `route_parse`/`route_commit` 中间，
  或独立一个 partition 步骤）。

### 3.3 shard worker（复用 RuleTask）

每个 shard 一个 worker，复用现有 `RuleTask` / `run_rule_task`，只是：
- 收到的 `Arc<Vec<Event>>` 已经只含本 shard 的 key。
- `CepStateMachine` 每 shard 一份（`instances` 只装本 shard 的 key）。
- 全局状态（限流/预算）改成共享原子（§5）。

---

## 4. 自动聚合窗口（conv）

### 4.1 编译器自动生成

规则带 `conv` 子句时，编译期自动：
1. 生成一个**中间窗口**（`over=0`，批语义），schema = conv 表达式引用的字段
   （scope key 字段 + step label 的 measure 值）——类比现有 `materialize_fields` 的
   field_usage 分析，换成 conv 的 field_usage。
2. 生成一个**conv 阶段**：不是完整 CEP 规则，而是一个轻量「变换算子」，读窗口的一批
   close 事件，复用 `apply_conv`（sort/top/dedup/where）+ 限流 + emit。

### 4.2 批语义（关键 subtlety）

现在 `apply_conv` 作用在 `scan_expired_at` 返回的**同一 watermark 那一批** close 上，
以及 `close_all` 的整批。所以 conv 窗口不是普通滑动窗，而是**按批边界触发**：

- 触发点 = watermark 推进 / 周期 scan_timeouts / close_all。
- 语义 = 收集本批 close → 触发下游 conv → 清空（over=0，不保留）。

正好对上窗口已有的 watermark 机制（`append_with_watermark` 本就在推 watermark），
conv 窗口用 watermark 推进当「批边界」，而不是用 `over` 做滑动保留。

### 4.3 close 输出 → 窗口事件

`CloseOutput`（scope_key + step_data）不是窗口 `Event`。复用 `build_eval_context`
（conv.rs 里已有）把 close 输出映射成窗口事件（scope key 名 + step label 名 → measure 值），
字段集合 = conv 表达式实际引用的字段。

---

## 5. 共享状态（throttle / budget / metrics）

这三样不是数据变换，是资源/遥测，不适合「窗口」，用共享原子：

| 需求 | 现状 | 改法 |
|---|---|---|
| `max_throttle` 限流 | `self.emit_count` / `self.emit_window_start`（per-machine） | `Arc<AtomicU64>` + 共享窗口起点 |
| `max_instances`/`max_memory` 预算 | `self.instances.len()` / `estimated_memory_bytes`（per-machine） | `Arc<AtomicUsize>` 共享计数 |
| 指标 | per-rule gauge | shard 求和聚合上报 |

共享原子只在 close/限流检查时访问（低频），竞争可忽略。

---

## 6. 分片可行性判定（哪些规则可分片）

| 规则形态 | 可分片？ | 说明 |
|---|---|---|
| 有 match key（`keys` 非空） | ✅ | per-key 分片 |
| 无 key（`keys` 空） | ❌ | 退单 worker |
| `each` 规则（无状态） | 另一条路 | 不是 key 分片，是事件级并行，另议 |
| 带 conv | ✅（走聚合窗口） | close 输出进 conv 窗口，跨 shard 聚合 |
| 带 joins/has() | 待确认 | join 走 router 控制面，理论上与分片正交，需验证 |

---

## 7. 回退开关

- 规则级开关：`shards=1` 即单 worker（现行为）。
- `executor_parallelism` 作为默认 shard 数。
- 分片不可行的规则（无 key / each）自动退单 worker。

---

## 8. Review 遗漏 / 待定清单（重点）

✅ = 已定案（见 §0.1）；其余待定：

1. ✅ **key 提取同源**：编译期单一 `extract_key`，dispatch 分区层与状态机共用。

2. ✅ **conv 批语义 = 时间窗**（不再是 per-watermark 对齐问题）：conv 重新定义为
   时间窗聚合，跨 shard 对齐由 `over` 周期天然覆盖，不需要 watermark 协调协议。

3. ✅ **close_all 协调 = channel-close**：shard 持 sender，shutdown drop 触发变换算子
   drain + 最终 conv；EOS 由周期 conv 覆盖。

4. **key 倾斜**：热 key（如 PK 的 hot auction）会导致单 shard 过载，分片收益打折。
   需要至少评估倾斜分布，必要时做「热 key 再拆」或接受非均匀。

5. **批分区开销与内存**：`Arc<Vec<Event>>` 拆成 N 个子批，每个子批是一次 Vec 分配 +
   拷贝 Arc。对高基数 key 是净收益，对低基数（key 数 < N）是纯浪费。判定：key 基数
   < N 时退单 worker。

6. **match emit 顺序**：on-event match 是 per-key 直接 emit，跨 shard 顺序非确定。
   sink 是否依赖顺序？当前多规则并发本就不保证全局顺序，但单规则内分片后「同 key 内
   顺序」仍保证（同 key 同 shard），跨 key 顺序不保证——需明确这是可接受的语义。

7. ✅ **conv 触发语义（fixed vs sliding）**：conv 是 fixed 专属，变换算子 `over` =
   桶长、批 = 整桶、触发 = 桶边界；sliding 不支持 conv（§0.1 #5）。

8. ✅ **conv 阶段形态 = 变换算子**：轻量 stateless 订阅者，非 CEP 规则。

9. **热重载与 shard 数变化**：reload 改 `executor_parallelism`（shard 数）会触发 key
   重分布，现有 per-key 实例状态在新旧 shard 之间怎么迁移？最简：reload 时 shard 数
   变化视为「拓扑变更」拒绝（同现有 sink/window 拓扑拦截），否则要做状态迁移。

10. **管道自身背压**：变换算子若慢，其订阅 channel 满 → 上游 shard 的 close 输出背压。
    与 sink 背压同一套语义，默认阻塞保正确。

11. **限流/预算的原子竞争**：正常是低频，但 `close_all`（shutdown）时 N 个 shard 同时
    冲共享原子，可能瞬时竞争。评估是否用「每 shard 本地计数 + 周期合并」代替直接共享原子。

12. **指标聚合的 gauge vs counter**：`instances` 是 gauge（求和或取 max 取决于语义），
    `matches`/`emitted` 是 counter（求和）。聚合方式要按指标类型区分，不能一律 sum。

---

## 9. 落地顺序（建议）

1. **P2a**：分片可行性判定 + key 提取 hoist + 批分区 + N shard worker（只对无 conv 规则）。
2. **P2b**：共享原子（限流/预算）+ 指标聚合。
3. **P2c**：自动 conv 聚合窗口（编译器生成 conv 窗口 + conv 阶段）。
4. 每步独立可测、可回退（`shards=1`）。
