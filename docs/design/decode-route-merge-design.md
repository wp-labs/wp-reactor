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

## 11. 物化列式化路线图（P4，独立于 merge）

> 前情：`route_parse` 里的 `batch_to_events`（Arrow → `Event`）只对**非 defer 窗口**执行
> （`compute_window_field_usage`，wf-lang/field_usage.rs）。它的存在 = **列式引擎未覆盖面的清单**。
> qradar 已全 defer（实测 p=2 伤即 route 无活）；真正触发物化的是 join 类查询
> （nexmark q3 的 post-join where / q13 等 deferred reduce）。
> 本路线图与 merge（P1-P3）正交：merge 不依赖它，它也不依赖 merge。

### 11.1 目标终态

端到端纯列式：`Row batch → Arrow 列 → 列式过滤/状态机/输出`，`Event` / `batch_to_events` /
窗口 log 的惰性 `OnceLock` 物化全部退役。物化不再是"可选环节"，而是不存在的环节。

### 11.2 覆盖清单（物化触发源 → 列式化项）

窗口能否 defer 的判定（field_usage.rs L109-116 的局部变量 `plan_defer_safe`，非独立函数）：
`each 规则 || match 有 event/close/seq 步骤 || 全部 bind filter 列式`。
非 defer 触发源 = 需要行级 eval context 的表达式：

| 触发源 | 当前实现 | 列式化 | 量级 | 注 |
|---|---|---|---|---|
| bind filter | 部分列式（scope-key 直读 `0c989f7`） | 剩余 case 逐个覆盖 | 小 | qradar 已全 defer |
| entity_id / score / yield 表达式 | eval context 逐行 eval | 每批对命中行算输出列 | 小-中 | qradar 的 `score(50.0)` 常量 + `entity(c.sip)` 字段引用很轻 |
| join 后 `where`（读 join 侧字段） | join lookup 产生行级 Event | join 结果列式化 | **大** | q3 回归记录（field_usage L161-165） |
| deferred join（`emit at`）reduce 字段 | eval context | 与上同源 | **大** | L481-500 测试 |
| MACHINE_ID | 已是列字段 | — | ✅ | |

### 11.3 排序与验证口径

1. **按量级从小到大推进**：bind filter 剩余 case → entity/score/yield → join 侧 eval（最后，最大）;
2. **每完成一项**：
   - `compute_window_field_usage` 的非 defer 集合应**收缩**（新增断言：某窗口从 needs_all/非 defer 落入 defer_materialization）;
   - 对应正确性测试（q3/q13 join 对拍、deferred 归并）全绿;
   - perf-diag 实测该形态窗口不再是墙（`decode→floor` 增量随物化消失而下降）;
3. **终点判定**：全仓 `defer_materialization` 集合 = 所有被消费窗口；`batch_to_events`/`OnceLock` 死代码删除。

### 11.4 与 merge 的关系

- merge（P1-P3）把 route 并入源任务后，**物化仍在源任务执行**（每批一次、Arc 共享、跨连接并行）——
  不构成 merge 的阻塞项;
- P4 完成后 merge 后的 route 分支进一步收敛为纯路由 + 分片（都无物化分支），
  但两者可独立合并/独立验证。

---

## 附：与现有结论的印证

- qradar 实测规则求值仅 ~1 核、12 核被"机制开销"喂满 → 移除一个中间层（少 ~2 个 worker + 通道泵送 + 锁）正是去机制开销；
- 本提案与"并行旋钮 = 默认安全 + 无用自动失效"原则一致：`parse_parallelism` 直接不存在，比钳制更彻底；
- 合并后剩余的可并行维度：连接数（C-UCP/W-RDP，已验证）、规则任务数（tokio 调度，吃满核）、sink parallel（已配 8）。
