# decode worker 内联 route 架构提案（parse 层移除）

> 状态：**P1+P2 已实施（2026-08-31）**——route 内联进源任务、parse 池与 `PrereadBudget` 移除、
> 配置废弃（`parse_parallelism` / `parse_buffer_bytes` 字段保留 serde 兼容，引擎忽略）。
> 实施取的方案与设计稿草案的差异记录在 §4.3 / D3。剩余：A/B 验证（§8）与 P3 文档更新。
>
> 关联：
> - [`concurrency-scaling.md`](concurrency-scaling.md)（六维并行度模型 + 两道墙）——本文是其"W-PDP 待墙打破后重测"的延续回答；
> - [`window-push-implementation-plan.md`](archive/window-push-implementation-plan.md)（R2 起源：`route` 拆 `route_parse` + `route_commit`）；
> - [`window-channel-actor-design.md`](window-channel-actor-design.md)（单写者 actor + mailbox 字节预算）；
> - [`preread-budget-design.md`](preread-budget-design.md)（第一道墙：深度节流）。
>
> 前情（2026-08-31 qradar_pk 诊断，数据快照见 [`../issues/parse-parallelism-qradar-diagnosis.md`](../issues/parse-parallelism-qradar-diagnosis.md)）：`parse_parallelism` 三线合流——**命名误称**（不是解析）、
> **无正据**（主文档自标"待墙打破后重测"，无任何 A/B 记录证明 p>1 有用）、**默认伤**
> （p=2 实测 -33%，qradar 1M：p=1=75k vs p=2=50k，受控 A/B 多轮复现）。
> 本文提出：**把 route 阶段并入 decode（源）任务，整体移除 parse 池这一层**。

---

## 1. 问题陈述

### 1.1 现状管线

```
源任务（4 TCP handle）         parse worker（N 个）           窗口 actor（单写者）       规则任务（376 个）
┌─────────────────────┐   ┌────────────────────────┐   ┌────────────────────┐   ┌───────────────────┐
│ 1. 解码 wire 帧      │   │ 6. route_parse：        │   │ 9. 按 (source,seq)   │   │ 11. pull 窗口 log   │
│    (arrow/ndjson/csv)│   │    - 路由（订阅窗口）     │   │    重排序（pending）  │   │ 12. process_batch   │
│ 2. 投影 (prepare)    │──►│    - 分片行子集（可选）  │──►│ 10. append + notify  │──►│ 13. 发射告警         │
│ 3. seq/window_seqs   │   │    - 物化事件（可选）    │   │                     │   │                     │
│ 4. 预算记账 → push    │   │ 7. dispatch_parsed     │   │                     │   │                     │
└─────────────────────┘   └────────────────────────┘   └────────────────────┘   └───────────────────┘
         └── 通道 + preread 预算（第一道墙）──┘              └── 第二道墙 ──┘
```

### 1.2 parse 层的三个问题

| 问题 | 证据 |
|---|---|
| **命名误称** | "解析"（wire decode）在源适配器 `DataSourceBatchSource` 里；parse worker 只做路由 + 分片 + **条件物化**。`batch_to_events`（Arrow→Event）是 R2 时代的"解析"，列式化后已退化为非列式窗口才触发。 |
| **无正据** | `concurrency-scaling.md` W-PDP 结论栏原文"待墙打破后重测"；nexmark conf 的 p=10 无 A/B 记录；qradar 探针 10→16 无增益。 |
| **默认伤** | 默认 2（已改 1）+ p=2 实测 -33%（共享 `Arc<Mutex<Receiver>>` 接收串行 + 并发 dispatch 乱序 → actor reorder park）。 |

### 1.3 关键结构事实（本提案的立足点）

1. **seq / window_seqs 已在源任务里分配**（`build_parse_item`，parse_pool.rs L213-243）——"保序契约"在 push 前就已建立，parse worker 不参与；
2. **route_parse 是只读、无共享可变状态的**（router.rs 注释："the parallelizable half of routing"）——可以在任何任务里执行；
3. **decode 已经在源任务里且天然并行**（4 连接 key 分片均衡）——把 route 并进去是白赚；
4. **`spawn_window_actors` 在 daemon 与 batch 模式都无条件执行**（lifecycle/mod.rs L397）→ mailbox 总是注册 → **actor 模式是唯一生产路径**；sync 模式（无 mailbox 的 commit worker）仅测试/embedded。

---

## 2. 目标管线

```
源任务（4 TCP handle / file replay）                窗口 actor（单写者）       规则任务（376 个）
┌──────────────────────────────────────┐   ┌────────────────────┐   ┌───────────────────┐
│ 1. 解码 wire 帧                        │   │ 5. append + notify   │   │ 7. pull 窗口 log   │
│ 2. 投影 (prepare)                      │──►│    （reorder 几乎消失）│──►│ 8. process_batch   │
│ 3. seq/window_seqs                    │   │                     │   │ 9. 发射告警         │
│ 4. route_parse + dispatch_parsed（内联）│   │                     │   │                     │
└──────────────────────────────────────┘   └────────────────────┘   └───────────────────┘
        └── 无中间队列：背压 = mailbox 预算直达 ──┘
```

**变化**：parse 池、通道、`Arc<Mutex<Receiver>>`、`parse_parallelism` 配置、`ParseItem` 传输对象整体消失；
第一道墙（preread 深度节流）消融，第二道墙（actor 单写者）保留。

---

## 3. 结构论证（为什么可行）

### 3.1 功的构成（每批）

| 环节 | 每批功 | 合并后位置 |
|---|---|---|
| decode | O(rows)，已并行 | 不变（源任务） |
| route_parse | 路由 O(订阅数) + 分片 O(rows)（仅分片时）+ 物化 O(rows)（仅非列式时） | **并入源任务，天然并行 × 连接数** |
| dispatch | await mailbox 预算 + send | 并入源任务（await 天然可用） |
| actor append | O(rows) 串行 | 不变（第二道墙） |
| 规则求值 | O(rows) × 规则数，不可摊还 | 不变（规则墙恒在） |

### 3.2 保序论证

- 每个源任务是一个**单循环**：`receive_batch → route → dispatch → 下一帧`；
- 同一入口的批次按严格顺序经同一任务投递 → actor 的 `(source, seq)` 重排序收到**有序输入**，`pending` park 从"跨 worker 竞态"消失；
- 残留的唯一乱序源 = 同一入口多 handle 的 `fetch_add` 竞态（handle A 取 seq N+1 先投，handle B 取 N 后投）——**与现状 p=1 相同且罕见**，actor 重排序保留作为兜底（正确性不依赖内联保序）。

### 3.3 背压与内存上界

- 中间队列（parse 通道）消失 → `preread` 预算失去对象；
- 新背压链：`dispatch_parsed` 的 mailbox 预算（`window_buffer_bytes`，默认 64MiB/窗）直接从源任务传导到 TCP 读；
- 内存上界 = Σ源任务 × 1 批在途 + Σwindow 预算（由构造有界，无需额外预算）。

### 3.4 并行度语义变化

| 场景 | 现状 | 合并后 |
|---|---|---|
| 4 TCP 连接（key 分片） | decode 4 路并行，route 池 N 路 | **decode+route 4 路并行**（连接即均衡） |
| 单连接 | route 池仍可 N 路 | route 串行（形态 B 单连接不友好——**无正据场景，接受**） |
| file 回放 | 单任务 push，route 池消化 | 单任务内联（同样单线程，无退化） |

---

## 4. 设计要点

### 4.1 源侧新循环（替代 `push_decoded_batch` 的 push 部分）

```rust
// 每个源任务（TCP handle / file replay）内部：
loop {
    let batch = receive_batch().await?;              // decode（不变）
    // perf-diag 门控（位置不变）：实际符号为 perf_diag::perf_cut_append() + 哨兵流豁免
    //（现状见 parse_pool.rs push_decoded_batch 开头，L271-274）
    if crate::perf_diag::perf_cut_append() && stream != PERF_SENTINEL_STREAM { continue; }
    if let Some(l) = limiter { l.acquire(rows).await; }  // 限速（位置不变）
    let (seq, window_seqs) = alloc_seqs();           // 保序契约（原 build_parse_item）
    metrics.receiver_frame(...);                     // 指标（位置不变）
    let parsed = router.route_parse(&stream, &batch);   // ← 从 parse worker 移入
    router.dispatch_parsed(src, seq, window_seqs, batch, parsed).await; // ← 移入
}
```

### 4.2 sync 模式（测试/embedded）

生产（daemon + batch）总是 actor 模式（§1.3-4）。sync 模式的 commit worker（per-source seq 重排序 + `route_commit`）**保留不动**，仅作为测试/embedded 的辅助路径；`spawn_parse_pool`（测试入口）改为 spawn 1 个内联消费者即可。

### 4.3 file replay 路径（已实施，含一处方案偏离）

- `replay_arrow_framed_file`（async）：直接调用 4.1 的 `route_and_dispatch`；
- `replay_arrow_ipc_file`（`spawn_blocking`）：**唯一需要特殊处理**——IPC 解析在 blocking
  池线程做，而 `dispatch_parsed` 是 async。原草案给了两个方案：

  > a. 保留 `spawn_blocking`，用 `mailbox.tx.blocking_send`（mpsc 提供）——最省事；
  > b. 把 IPC 解析也改成 async 批处理（改动大，不推荐 P1）。

  **实施两者皆未取**，而是折中：新增 `route_and_dispatch_blocking`（`ingest.rs`），
  在 blocking 线程用 `Handle::block_on(route_and_dispatch(...))` 驱动**完整** dispatch。

  **为什么不能只 `blocking_send`**：`dispatch_parsed` ≠ 一次 send，它还包含
  ① 每窗 mailbox 字节预算的 async `acquire_window_budget` 等待（**唯一的 ingest 背压**，
  parse 池移除后背压链是 `window_buffer_bytes` 预算 → 源任务 → TCP 读）；
  ② 无 mailbox 窗口的 `commit_window` 内联兜底（sync 模式 / 热加窗）。
  裸 send 跳过预算等待 = 丢背压、上界变无界；且 sync 路径整个漏掉——两者都是语义损失。

  **为什么 `block_on` 无死锁**：`spawn_blocking` 线程**不是 runtime worker 线程**，
  其上的 `block_on` 只驱动 dispatch 这一个 future，不占用 runtime 调度。它 await 的
  预算信号量由 window actor 在 runtime 线程消费（append）后释放——actor 的推进不依赖
  源线程做任何事，等待链无环：`源 blocking 线程 → 等预算 ← actor append → 释放预算`。
  等待期间 runtime 线程照常调度其它任务。代价与方案 (a) 相同（dispatch 期间该 blocking
  池线程被占用），但**保住了预算背压与 sync 兜底两条语义**——这正是方案 (a) 会丢掉的部分。

### 4.4 热重载

`route_parse` 每批读 router 当前状态（fanout / sharding registry / window metadata）→ 规则/窗口 reload **不需要重起源任务**（与现状一致）。

### 4.5 配置与命名

- `parse_parallelism`：**废弃**（保留字段 + `#[serde(default)]`，引擎忽略；已提交默认 1，兼容旧 conf）；
- `parse_pool` / `ParseItem` / `run_parse_worker*` 词汇随实现删除；
- `PrereadBudget` 若保留，仅作内存护栏（决策点 D1）。

---

## 5. 正确性论证

| 关注点 | 结论 |
|---|---|
| 窗口保序 | 单源单任务内联 → actor 输入有序；actor 重排兜底保留（§3.2） |
| 关闭路径 | actor mailbox 关闭时 drain（现逻辑不变）；EOS flush 计数不变 |
| 窗口 miss 上报 | `subscribers_of` 为空时的 `report_window_miss` 在源侧（位置不变） |
| 驱逐/ack | actor 侧不变；pull 规则任务不变 |
| 热重载 | route 读当前状态，无缓存一致性风险 |
| 乱序兜底 | handle 级 `fetch_add` 竞态：现状 p=1 已存在、有界，actor 处理 |

---

## 6. 改动清单

**删除**
- `parse_pool.rs` 的 worker 池 / 通道 / `run_parse_worker` / `run_parse_worker_direct` / `ParseItem` 通道路径；
- `spawn_receiver_task` 中的 `spawn_parse_pool_with_preread` 调用；
- `parse_parallelism` 配置语义（字段保留兼容）；
- metrics `parse.inflight_bytes` gauge（决策点 D2）。

**修改**
- `push_decoded_batch` → 源侧内联 `route_and_dispatch`（保留门控/限速/指标/seq 分配）；
- `spawn_external_source_tasks` / file 分支 / `replay_*`：签名去掉 `parse_tx`/`preread`，保留 `router`；
- `wf-config` validate.rs L14-16：`parse_parallelism must be > 0` 校验随 P2 语义废弃同步移除（否则校验悬空）；
- `parse_buffer_bytes` 配置项：与决策点 D1 联动——删除，或重定义为 sync 模式/全局内存护栏的参数；
- `receiver/tests.rs` / `parse_pool.rs` 测试：按新路径改造。

**保留**
- `build_parse_item` 的投影/metrics/seq 逻辑（并入源侧）；
- mailbox 字节预算（`window_buffer_bytes`）= 唯一背压；
- actor 模式（生产）与 sync 模式（测试）双路径的 actor 侧。

---

## 7. 风险与对策

| 风险 | 对策 |
|---|---|
| IPC replay 的 spawn_blocking + async dispatch 冲突 | 已实施：`route_and_dispatch_blocking`（`Handle::block_on` 驱动完整 dispatch，含预算背压与 sync 兜底）——论证见 §4.3 |
| 删除 preread 预算后内存对账口径变化 | 决策点 D2：重定义或删除 `parse.inflight_bytes`；peak_commit 对账更新 |
| 单连接形态 B（物化重负载）route 串行 | 无正据场景（W-PDP 待重测），接受并在文档标注 |
| 改动面大（receiver/parse_pool/生命周期/测试） | 分步实施（§9），每步 A/B + 正确性门禁 |
| 隐藏的 sync 模式调用方 | 保留 commit worker 路径，仅删 actor 模式的池 |

---

## 8. 验证口径

1. **正确性**：现有 receiver/parse_pool 测试改造后全绿；qradar `run.sh` #18 门禁（eviction=0 + emitted 阈值）；nexmark `verify_daemon.sh` 三层对拍；
2. **性能**：qradar 1M A/B——EPS 预期 ≥ 75k（现状），**RSS 预期下降**（少一层 2GB 预算缓冲与在途批次）；nexmark 30M 对比不劣化；perf-diag `decode→floor` 增量（parse 环节并入 decode 后该增量应变小）；
3. **内存**：`peak_commit − Σwindow_bytes` 对账（删除 preread 预算后未归因应消失）。

---

## 9. 分步实施

- **P0（已完成，2026-08-31）**：`rule_parallelism → rule_shards`（默认 1）；`parse_parallelism` 默认 2 → 1（止损）；
- **P1（本提案）**：actor 模式内联——源任务直接 `route_parse + dispatch_parsed`；`push_decoded_batch` 只保留门控/限速/指标/seq；测试改造；A/B 验证；
- **P2**：`parse_pool.rs` 拆除（worker 池/通道/词汇）、`parse_parallelism` 配置废弃（alias 兼容）、metrics 对账；
- **P3**：`concurrency-scaling.md` 更新——两道墙 → **一道墙（actor 单写者）+ 规则墙**；W-PDP 行从"待重测"改为"已移除（并入 W-RDP）"。
- **P4（独立演进，见 §11）**：物化列式化——`batch_to_events` 逐类退役，端到端纯列式。

---

## 10. 决策点（评审待定）

- **D1**：`PrereadBudget` 删 vs 保留为全局内存护栏（同步模式还需要）；
- **D2**：`parse.inflight_bytes` gauge 删除 vs 重定义为"源在途 + mailbox 在途"；
- **D3（已定，2026-08-31 实施）**：IPC replay 未取 blocking_send（方案 a，丢预算背压与 sync 兜底）也未改 async（方案 b，改动大）——采用 `route_and_dispatch_blocking`：`Handle::block_on` 驱动完整 dispatch，论证见 §4.3。
- **D4**：sync 模式（测试/embedded）是否同步内联，还是保留 commit worker 不动（P1 建议不动）。

---

## 11. 物化单轨化路线图（P4，独立于 merge）

> 前情（2026-09-01 基线实测后重定向）：`batch_to_events`（Arrow → `Event`）的存在 =
> **列式引擎未覆盖面的清单**。**动机从性能改为一致性**：
> 1. **已存在语义分歧**：`>2^53` 整数 / 纳秒时间戳，列式路径原生 i64 精确、行式路径
>    `Value::Number(f64)` 丢精度（CHANGELOG 1.1.0 "documented semantic divergence"——
>    同一查询两条路径可能给出不同结果，`2^53 == 2^53+1` 列式 false / 行式 true）；
> 2. **双路径对拍约束**：`ColumnarEvent` 处处背 "byte-identical to eager path"——维护负担 + 漂移风险；
> 3. **新功能双实现**：新增表达式构造要列式编译 + 行式解释各做一遍（触发面 8 门控即双实现清单）。
> 性能基线否定（nexmark 10m 墙梯）：q4/q9（each+reduce join，回退 eager）rules 增量
> +60~108ns/事件，但 q4 为等墙（CPU 14%、RSS 5.3GB 堆积）——**列式化对吞吐无收益**。
> 故本路线图以**一致性**为判据：生产只保留一条执行路径。

### 11.1 目标终态：生产单轨化（非“删行式解释器”）

```
生产路径：全列式（ColumnarEvent / 列式 join / 列式 let）——唯一执行轨
行式解释器：保留，降级为「测试对拍 golden」——只活在测试里，不进生产
```

一致性由**构造**保证（一条数据一种读法、一个结果），不再靠 "byte-identical 约束 + 对拍" 维持。
`Event` / `batch_to_events` / 窗口 log 惰性 `OnceLock` 从生产路径退役（代码可保留为测试参考）。

### 11.2 覆盖清单（触发面分析结论，2026-09-01）

已列式（无需动）：

| 形态 | 证据 |
|---|---|
| match 规则（P3 FieldView 列式喂状态机） | qradar 376 条全走此路径，无物化 |
| each + **Snapshot** join（q13b/q20） | `parse_each_join_columnar` + `emit_each_direct_batch_columnar_join` |
| each + **let**（q22：split/mvindex/concat） | 2026-08-25 层 2：let RHS 列式编译 + yield 引用内联 |

剩余缺口（生产仍走行式）——each 门控 `each_plan_columnar_safe()`（each_exec.rs L695-827）的 8 条件：

| # | 缺口 | 触发查询 | 量级 | 状态 |
|---|---|---|---|---|
| 1 | **reduce maxrow join**（非 Snapshot） | q4/q9 | **大**（新执行器：右窗 per-key 窗口化归约 + 输出整行） | ✅ **2026-09-02 收口**——deferred 驱动列式挂起（`DeferredPending.left` → `DeferredLeft`） |
| 2 | **within/interval join** | q8 | **大**（列式 interval-join 结构） | ✅ **2026-09-02 收口**（同上；区间过滤本就列式，剩余是驱动行物化） |
| 3 | each + 后置 `where`（无 join 时 where 非空） | q15/q16/q17 形态 | 中 | ✅ **2026-09-02 收口**——无活 join + 可列式驱动列 where（批级守卫掩码；真实可达形状 = 死 join + 驱动列 where） |
| 4 | each filter / bind filter 非列式 | 通用 | 中 | ✅ **2026-09-02 收口**——非列式 each filter 逐行 `passes_each_filter` 回退；非列式 bind filter 命中循环逐行 `event_matches_alias`（不再 `hit.fill(true)` 丢 filter） |
| 5 | **list-index 输出字段**（`c.tags[0]`，Path=[Field,Index]） | qradar 形态（tags/categories 下标） | 小-中 | ✅ **2026-09-02 收口**——无活 join 时编译 ListIndex cvec（Field 快通道 `value_at` 只读 flat 列，索引元素需 offset 读；root 引用 let 拒绝）；**有活 join 的非限定/歧义裸名仍行式**（门控 out_shape 保持限定，残项） |
| 6 | score 非「常量 \| 常量×flat 字段」（裸 flat 字段 / 字段×字段 / 复合算数 / 常量×list-index） | 通用 | 小-中 | ✅ **2026-09-02 收口**——无活 join 的可列式 score 编译批级 score_cvec（逐行 cell → Number → clamp；读结构化列/编译失败逐行 eval_score 回退；非数值/缺失 → 整行 failed 与行式一致）；活 join 仍只允许常量 |
| 7 | entity 非字面量 / flat 字段（list-index / flat 组件复合表达式） | 通用 | 小 | ✅ **2026-09-02 收口**——无活 join 的可列式 entity 编译批级 entity_cvec（cell → Value → `value_to_string`，与快通道 Generic 渲染同源；编译失败逐行 eval_entity_id 回退）；**非列式（对象/数组嵌套 Path）仍行式** |
| 8 | yield 非字面量 / flat / 列式输出函数（有 join 更严） | 通用 | 中 | 开放 |

### 11.3 排序与验证口径

> 状态（2026-09-02）：**断言基座已落地**——生产路径判定抽为单一事实源
> `RuleExecutor::execution_path`（`executor/execution_path.rs`），`process_batch`
> 是唯一消费方（断言的路径 ≡ 执行的路径）；矩阵测试按下表逐形状断言
> `DeferredMachine / DeferredPending / ColumnarEach / EagerRows`，缺口收敛时
> 翻转向量即可回归。
>
> **gap 1/2 已收口（2026-09-02）**：deferred join（q4/q8/q9）驱动事件列式
> 挂起——`DeferredPending.left` 从 `Event`（每驱动行 HashMap 物化）降为
> `DeferredLeft::Columnar`（`JoinRow::Columnar`：Arc batch + 行号 + 索引 +
> 投影，同批共享），挂起队列与注入期分配随之下降。有 let 绑定回退物化一次
> （字节一致由构造保证）。`execution_path` 新增 `DeferredPending` 变体（无
> machine + 有 emit_at join + raw batch + 非 DEBUG），矩阵断言 gap 1/2 真实
> 形态（reduce/within + emit at）翻转为 `DeferredPending`；对拍测试
> `deferred_pending_columnar_matches_eager` / `execute_columnar_matches_eager_output`
> / `columnar_projection_shadows_unprojected_fields` / `multi_cond_recheck` /
> `with_lets_materializes_once` 锁定列式 == eager 字节一致。
>
> **gap 3 已收口（2026-09-02）**：each + 后置 `where`（无活 join）列式化——
> `EachBatchVecs.where_cvec`（批级守卫掩码，compile_guard 同 filter 机制，
> 编译失败/结构化参数逐行 `where_ok` 回退）；gate 放行「无活 join + where
> 可列式」（真实可达形状 = 死 join + 驱动列 where——checker 要求 where 必须
> 有 ≥1 join，where 只读驱动列 → join 死消除）。**顺带修复行式批路径 latent
> bug**：`execute_each_direct_batch` 的借用短路（`live_joins 空 && lets 空`）
> 会静默跳过 `where_ok`——死 join 形状下生产 batched 路径 where 失效（与
> 逐事件/oracle 的 plan.joins 判定分叉），已补 `where.is_none()` 前提。对拍：
> `each_columnar_where_matches_row_path` / `_after_filter` /
> `_missing_column_rejects_all_parity`。矩阵断言 gap 3 从 `EagerRows` 翻转
> 为 `ColumnarEach`。
>
> **gap 4 已收口（2026-09-02）**：非列式 each filter / bind filter 不再让整条
> 规则回退行式——each filter 经 `execute_*_batch_columnar` 的 `filter_cvec=None`
> 分支逐行 `passes_each_filter` 解释（与行式字节一致）；**bind filter 是关键修复**：
> process_batch 的 columnar_each 命中循环对非列式 bind filter 原 `hit.fill(true)`
> 全放行（静默丢过滤子集）——改为逐行 `event_matches_alias` 解释（ColumnarEvent
> 视图直读列，批级 FieldIndex，语义与行式一致）。矩阵断言 gap 4 从 `EagerRows`
> 翻转为 `ColumnarEach`（pipe + filter / 活 join + filter 残项仍 `EagerRows`）。
> 对拍：`each_columnar_nonexpr_each_filter_matches_row_path` /
> `each_columnar_nonexpr_filter_and_bind_matches_row_path`（executor 层）+
> `each_noncolumnar_bind_filter_columnar_hit_matches_row_path`（引擎层：
> columnar 命中循环 vs 行式 event_matches_alias）。
>
> **gap 5 已收口（2026-09-02）**：无活 join 的 list-index 输出字段（`c.tags[0]`，
> Path=[Field,Index]）不再让整条规则回退行式——`compile_yield_cvec` 对 list-index
> Field 编译 `ListIndex` cvec（快通道 `value_at` 只读 flat 列，索引元素需 offset
> 读），yield 分类从 `YieldKind::Field` 改 `YieldKind::General`（批级槽位取 cell，
> 编译失败逐行 `to_event` 回退与行式一致）。gate 只放行**无活 join**（有活 join
> 的 out_shape 限定不变）；root 引用 let（`x[0]`）拒绝（列式无 let 视图，编译
> root 缺失 → 全 null 静默失真）。矩阵断言 gap 5 从 `EagerRows` 翻转为
> `ColumnarEach`（+ 有活 join / let-root 负向边界仍 `EagerRows`）。对拍：
> `each_columnar_list_index_yield_matches_row_path`（原生 List，含 null 元素/
> 空列表/越界）+ `each_columnar_list_index_json_array_yield_matches_row_path`
> （JsonArray-metadata Utf8 = qradar 帧 `array/…` 真实存储形态，含 null-drop
> 探针 `["a",null,"b"][1]`→"b"）。
>
> **gap 6/7 已收口（2026-09-02）**：score / entity 的非「常量/常量×flat」/非
> 字面量-flat 形态不再整条回退行式——无活 join 的可列式 score / entity 表达式
> 编译批级 `score_cvec` / `entity_cvec`（`each_batch_prepare` 与 where/filter
> 同机制，读结构化列不编译——列式读原始 JSON 文本 vs 解释器解析可分叉），逐
> 行 cell 求值：score → Number → clamp（非数值/缺失 → 整行 failed，与行式
> `eval_score` Err 一致）；entity → Value → `value_to_string`（null/缺失 → 空
> 串，不拒行）。快通道保留：score 常量 / 常量×flat（f64 直读）、entity
> StringLit / flat Field（typed 列 lane）。编译失败 → 逐行 eval_score /
> eval_entity_id（Event 视图）回退。矩阵断言 gap 6（字段×字段、裸 flat 字段
> score）与 gap 7（list-index / Add 复合 entity）翻转为 `ColumnarEach`（非列式
> upper() score / 对象嵌套 Path entity 负向边界仍 `EagerRows`）。对拍：
> `each_columnar_general_score_matches_row_path`（int×int clamp 上下界 + null
> 操作数 failed 行）/ `_null_and_bad_cells_...`（裸字段 null cell、Utf8 非数值
> 全 failed、schema 外字段全 failed）/ `each_columnar_general_entity_...`
> （list-index 原生 List null/空/越界 + 复合 Add 的 number_to_string 渲染）。
> **review 补测（2026-09-02）**：常量×list-index score（`2.0*c.tags[0]`）原被
> `score_shape` 归 MulConst 而 gate 只放行 flat 字段 → 未单轨；且执行器原以
> 「ScorePlan 存在」判快通道，一旦放行会对 list-index 跑 value_at（读整列而非
> 元素）。已修：`score_is_general` 以 field 是否 flat 分类（MulConst×list-index
> 归一般 cvec），gate/执行器/prepare 同 key。补测：`each_columnar_score_const_
> times_list_index_matches_row_path` / `each_columnar_general_score_nested_arith_
> matches_row_path`（`a*b+c` 复合）/ `each_columnar_entity_list_index_json_array_
> fallback_matches_row_path`（JsonArray-metadata entity → 逐行 eval_entity_id
> 回退路径）+ 矩阵/ gate 分支断言。

1. **判据 = 生产路径是否还走行式**（不再看性能收益）：每收一项，该类规则的生产执行轨
   切换为列式，行式路径保留为测试对拍 golden（从“生产实现”变“测试参考”）；
2. **每完成一项**：
   - 该形状规则的**生产执行路径断言为列式**（`execution_path` 矩阵测试翻转向量：
     对应断言从 `EagerRows` 改为 `DeferredMachine` / `ColumnarEach`）;
   - 行式/列式对拍测试**保留**（行式变 golden，继续锁定语义）;
   - `compute_window_field_usage` 的非 defer 集合收缩断言;
3. **终点判定**：生产执行路径 `batch_to_events` / `materialize_rows` / 窗口惰性 `OnceLock`
   调用点归零（仅测试引用）；`Event` 类型只存在于测试对拍与 hot-reload 兜底。

### 11.4 物化代码现状（2026-09-02 盘点，gap 1/2/3 收口后）

生产热路径物化点（按触发条件）：

| # | 位置 | 触发条件 | 量级 | 状态/对应缺口 |
|---|---|---|---|---|
| 1 | `router.rs` route_parse L361-380 | 窗口 `defer_materialization=false`（有规则走行式）且非分片 | 全批 `batch_to_events[_filtered]` | **剩余面**：gap 4-8 的 match 侧 + 行式 stats 窗口（qradar 全 defer → 不触发） |
| 2 | `rule_task.rs` L1249 eager 兜底 | `ExecutionPath::EagerRows`（非 deferred / 非 columnar each / 非 deferred-pending） | 全批 | **剩余面**：gap 4-8 的 each 侧 + debug 模式（2026-09-02 已收窄：deferred join 免 eager） |
| 3 | `rule_task.rs` L4610 `PipeBatchStager::take_events` | pipe flush 且目标有 Single/Sharded（**row-path 中间窗消费者**）订阅 | 每批一次 | **条件物化**：纯列式消费者（q4b stats 从窗口读、无事件订阅）→ `take_batch` 不物化；row-path 消费者存在时才物化 |
| 4 | `stats_task.rs` L411 | stats 列式段 `process_batch_rows` 返回 false（where 非列式 / distinct 不支持等）→ 行式回退 | 行域内行 | **剩余面**：stats 行式回退（主路径 q15-q19 已列式，回退是 rare） |
| 5 | 窗口 log `OnceLock`（buffer/mod.rs L844） | route_parse 预置 events 的窗口；hot-reload 新订阅者 `events_since()` | 惰性 | hot-reload 兜底，生产 pull 已不用 |

已退役（gap 收口移除）：

| 位置 | 状态 |
|---|---|
| `deferred_exec.rs` `DeferredPending.left` 每行 Event | ✅ gap 1/2：`DeferredLeft`（`JoinRow::Columnar` + 投影遮蔽；有 let 回退物化一次） |
| each + 无活 join where 的 eager 物化 | ✅ gap 3：批级 `where_cvec` 守卫掩码 |
| each list-index 输出字段（`c.tags[0]`）的整条回退 | ✅ gap 5：`compile_yield_cvec` 编译 `ListIndex` cvec + yield 分类 `General` |
| each 非快通道 score / entity 的 eager 物化 | ✅ gap 6/7：批级 `score_cvec` / `entity_cvec`（编译失败逐行解释回退，不整批物化） |

受控「反物化」（emit/到期时**按命中行**物化，非全批——保留，属输出路径）：
`DeferredLeft::to_event` / `ColumnarEvent::to_event` / `JoinRow::to_event` / `RowEvent::to_event`。

转换核心（代码保留，测试 golden + 回退）：`event_bridge.rs` `batch_to_events[_with]` /
`materialize_rows[_with]`；`ColumnarEvent` / `JoinRow::Columnar` 为反物化的现成方案。

> 与 §11.2 缺口对应：gap 8（yield 非列式函数/表达式）→ 触 #2（each 侧）/
> #1（match 侧）；stats 行式回退 → #4。（gap 1-7 已收口：deferred pending /
> each where / 非列式 filter / list-index 输出 / 一般 score·entity 均已列式或
> 逐行解释回退。）
> 终点判定（§11.3-3）不变：`batch_to_events` / `materialize_rows` /
> `OnceLock` 调用点归零（仅测试引用）。

### 11.6 终态通用机制：按读集物化 → FieldSource 直读（M1/M2/M3 已落 2026-09-02）

**动机（实测）**：qradar rules 段 CPU 采样 + census 显示——376 条 match 规则**全走
deferred 列式路径**（无行式整批物化），但 138 条（36%）`trigger_event_needed=true`、
其 fires 占 40.1%；每 fire `ColumnarEvent::to_event()` 按**窗口并集**投影物化全行
（conn_events 的 `conn_info: object`/`tags: array` JSON 解析），而 ctx 只读
`close_ctx_fields::Named` 读集 → 未引用结构化列的 JSON 解析是纯浪费（采样：
serde_json 2.4k → 46 权重，-98%）。

**机制 = 三件套（已存在，M1 参数化到规则粒度）**：
1. 编译期读集：`plan_close_ctx_fields`（Named 窄化，覆盖 score/entity/yield/lets/
   join 左字段/where）；
2. 物化投影：`ColumnarEvent.projection`（`to_event` 只遍历投影列；`field_value`
   不看投影 → step/guard 逐事件读列不受影响）；
3. 运行时接线：rule_task 的 match 行 `RowEvent::Columnar` 用
   `RuleExecutor::fire_trigger_projection()`（= Named 集）替代窗口并集
   `materialize_fields`；All（含 `_step_*`/合成字段引用）→ 回退窗口并集（现状）。

字节一致由构造保证：ctx 从 trigger Event 只读 Named 集（`build_eval_context` 按
`needed.wants` 过滤；ctx-free 快路径直读的 entity/yield 字段也在 Named 内）→
投影到 Named = 求值需要的全集。

**M1 落地**：`RuleExecutor.fire_trigger_projection`（executor 字段+访问器）、
rule_task RowEvent 投影选择。对拍：`fire_trigger_projection_narrows_to_rule_read_set`
（读集入投影/未引用不入 + 结构化列批上 to_event 只物化读集）/`_none_when_ctx_
untrackable`（合成字段 → All → None）。实测 qradar rules 段 serde_json 权重
-98%（to_event 不再解析未引用结构化列）。

> **M1 review ① 审计（2026-09-02，结论=不破 produce）**：multi-alias/Path 读集
> 正确性。`field_ref_name` 剥 alias、Path 取 root → Named 含跨 alias 裸名，疑点
> 是「跨窗裸名投影是否错物化/漏物化」。实证三不变式成立：
> (1) 触发行 `RowEvent::Columnar` 的 batch 恒为**单窗**（`DeferredRows.batch`），
> to_event 遍历该批 FieldIndex——任何投影只物化本批列；
> (2) Named ⊇ eval 从 ctx 读的每个裸名（visit_expr_fields 穷尽 + 函数/合成
> 字段 force_all 兜底）→ 被投影裁掉的列必然不可读；
> (3) All/Named 决策在 build_eval_context 注入门控与 fire 投影间同源（None ↔ All）。
> 对拍：`plan_close_ctx_fields_multi_alias_and_path_collapse_to_bare_roots`（剥
> alias/Path root/同名折叠入集，不误收 alias/叶子段）/
> `fire_projection_multi_window_bare_names_no_phantom_and_path_root_materialized`
> （跨窗裸名非本批列 → to_event 无幻影键；Path root 整列物化；未读结构化列
> 不解析）。

**M2 已落 2026-09-02**：`executor::eval` 泛型化——L3/输出 eval 族
（`eval_yield_expr*` / `eval_bool_expr` / `eval_expr_with_l3` /
`eval_score` / `eval_entity_id` + builtins/step_data/utils）ctx 从 `&Event` 改
`&dyn FieldSource`（FieldSource 名协议：`field_value` / `field_names`）：

- step/bind 历史访问器（step_data.rs）与 stat 标签读取全部走
  `field_value`（owned），`step_indices` 枚举改 `field_names()`——eval 不再
  依赖具体 `Event` map（`_step_*` / `_bind_*` 合成字段协议保留为**名称解析
  契约**：任何按同一协议实现 FieldSource 的 ctx 字节一致）；
- `Expr::Field` 改 `eval_field_value_src`（与 map 版逐字节同构，key.rs）；
- owned 读取代价：series 访问每次多一次中间 `Vec` 拷贝（原借用+逐元素 clone
  → clone + 移动），仅 per-fire 输出路径、量级与既有 to_event/ctx 构建同阶；
- 对拍：`coverage_m2` 三件套——非-Event `RowSource`（镜像 Event 名协议）逐
  表达式/入口字节一致；`RowOnlySource`（裸行，无合成条目）= 无合成条目的
  Event（L3 空历史语义）；field_names 枚举契约锁。

**M3 已落 2026-09-02；§11.3 终点达成**：`MatchedContext.trigger_event`
`Option<Arc<Event>>` → `Option<TriggerEvent>`（owned 列式行引用：`Event` 变体
= row-mode/回退物化；`Columnar` 变体 = Arc batch + row + FieldIndex + 投影）：

- `TriggerEvent` 实现 `FieldSource`（field_value 直读列，field_names 尊重
  投影，to_event 复用 ColumnarEvent 物化语义）——M2 的 FieldSource 化消费方
  （build_eval_context / ctx-free resolve_field / resolve_match_field）零改读；
- 机器捕获外置：`advance_at_with_masks_key_capture` 带 owned trigger capture
  （deferred 路径 = rule_task 每命中行预建列式快照，Arc clone×3，仅
  trigger_event_needed 时）；默认 None 回退机器内 `event.to_event()`（row-mode
  / 测试不变）；
- rule_task deferred 命中行：`DeferredRows` 携每批 `batch_arc`，命中行构造
  快照直接入机器——**fire 不再 to_event**（HashMap + 结构化列 JSON 解析归零），
  ctx 按需经 field_value 直读列（结构化字段仍按读集解析一次）；
- 行为零变化保证：投影一致性（快照投影 == M1 fire 投影/窗口并集）、读集
  闭合（输出 Field 读名 ⊆ Named ⊆ 投影 → 快照/物化 map 无分歧）、null/缺失
  语义一致；对拍：`trigger_event_columnar_matches_event_fieldsource_and_
  projection`（双变体 field_value/to_event/投影窄化字节一致）/
  `machine_capture_trigger_columnar_skips_fire_materialization`（捕获路径
  ctx 携 Columnar 变体、回退路径仍 Event 变体，两者 to_event 一致）。

M2+M3 到达 §11.3 终点（生产路径每 fire `Event` 物化归零——deferred 列式
匹配路径不再有任何逐事件 HashMap/JSON 解析）。

### 11.5 与 merge 的关系

- merge（P1-P3）把 route 并入源任务后，非 defer 窗口的物化仍在源任务执行（每批一次、Arc 共享）——
  不构成 merge 的阻塞项;
- P4 与 merge 可独立合并/独立验证；P4 完成后 merge 后的 route 分支收敛为纯路由 + 分片。

---

## 附：与现有结论的印证

- qradar 实测规则求值仅 ~1 核、12 核被"机制开销"喂满 → 移除一个中间层（少 ~2 个 worker + 通道泵送 + 锁）正是去机制开销；
- 本提案与"并行旋钮 = 默认安全 + 无用自动失效"原则一致：`parse_parallelism` 直接不存在，比钳制更彻底；
- 合并后剩余的可并行维度：连接数（C-UCP/W-RDP，已验证）、规则任务数（tokio 调度，吃满核）、sink parallel（已配 8）。
- **无状态 each 规则的分片语义（2026-08-31 q1 实测）**：`rule_shards > 1` 对无 match key 的 each 规则不做 key 分片，而是整批 round-robin 轮转（每批整批进一个 shard，批内顺序保持）——此时分片的实际收益是**输出链（告警构建）并行**（每个 shard 是完整规则任务，自带 `AlertColumnBuilder`）。q1 full 档 6.5M → 22.7M EPS（增量 +115.6 → +1.6 ns/事件）。「分片」命名对 match/stats 的 key 分片直观，对无状态 each 规则是隐式的输出并行旋钮（config `rule_shards` 字段注释同源说明）。
