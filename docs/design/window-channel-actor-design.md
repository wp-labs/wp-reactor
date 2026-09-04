# 窗口通道化设计：单写者 Window Actor（订阅模型）

> **状态更新（2026-08-16 晚）：拓扑已落地**（A.1/A.2，commit 5c354fb；q1 30M
> 4.18M/s）。**log 结构最终未采用本文的 LF SkipMap 变体**——RSS 回归 11-12GB 的
> 根因是 crossbeam-epoch 延迟析构（本文 §1 诊断链正是它的发现过程），最终方案
> 为 `RwLock<BTreeMap<u64, TimedBatch>>`，见
> [window-log-eviction-design.md](window-log-eviction-design.md)。通道拓扑、
> 有序性不变量、内存上界账本（§2-§6）仍然有效。

状态：草案 v2（待评审）
日期：2026-08-16
分支基线：columnar-emit（C2, 40ba71d）之上

v2 变更：**取消 commit worker**。reorder 下沉到 window actor
（per-source 游标），parse worker 直接发送到窗口通道——"window 订阅
stream"字面落地：每个窗口 actor 持有它所订阅 stream 的接收端。

---

## 1. 背景与动机

C2（on-each 直写列）之后，采样定位的吞吐断点是窗口 `std::sync::RwLock` 争用
（route_parse 读 vs parse_pool::commit 写，lock_contended 683 样本）。

第一版尝试（LF，共享式无锁）：`SkipMap<u64, TimedBatch>` + 全原子账本，所有方法 `&self`。
A/B 结果：EPS 3.62-3.71M → **4.14-4.18M（+12-15%）**，但 RSS 2.9-3.4GB → **11-12GB**。

RSS 回退诊断链（全部实验留档）：

| 实验 | 结果 | 排除/证实 |
|---|---|---|
| 窗口 gauge 对比 | 两版一致（2.33GB / 5.6M rows / 73 批） | 回退不在窗口账本内 |
| 限速 3.6M/s 复现 | RSS 仍 11.2GB | 排除消费排队 |
| mimalloc 退出统计 | abandoned 页 2.0→8.8GiB，arena 3→11 | 跨线程 free 放大 |
| jemalloc + 零 decay | 仍 12GB | 排除 allocator 特有 → 瞬时活数据 |
| ingest 后观察 | RSS 回落 2.7GB | 非泄漏，是 in-flight 膨胀 |
| 手动 1MiB 帧跑 | peak 仅 3.1GB，回退未复现 | **与 8MiB 帧强相关** |
| MIMALLOC_PURGE_DELAY=0 | RSS 降但 EPS 掉回 3.67M | allocator 旋钮非正解 |

**根因结论：窗口写锁一直在充当事实上的串行化闸门/背压。** 无锁化后 10 个 parse
线程的 commit 完全并发，8MiB 帧下每批是巨型 Arrow 块 + 全量事件物化，in-flight
批次并发膨胀出 ~8GB 瞬时水位；跨线程释放再被 allocator 放大为页滞留。

**正确方向（用户指示）：归属权 + 消息传递的无锁，而非共享数据结构的无锁。**

> window 向 stream 订阅，规则向 window/pipe 订阅，通过 channel 串联。

即：每个窗口是一个**单写者 actor**（自己的任务、独占自己的状态），上下游全靠
有界 channel 连接——背压天然存在，写者唯一，无共享写竞争。

LF 版的代码不是弯路中的废料：`&self` 窗口 API（SkipMap + 原子）恰好是
"actor 独占写 + 并发 lock-free 读"所需的底座，全部复用。

---

## 2. 目标拓扑（v2：无 commit worker）

```
 source (TCP/file)          每 source 一把 seq（接收循环内 fetch_add+send，
   │ decoded batch          单循环串行 → per-source 顺序天然可靠）
   ▼
 [parse pool ×N] ──── PrereadBudget 256MiB（不变）────┐
   route_parse() 纯函数，  parse ch (count 1024)      │ 解析完成即按订阅表
   只读窗口元数据 Arc     （workers 经 Mutex recv，   │ 直接发往各窗口通道
                          per-source 顺序到 parse 为止）│
                                                       ▼
      ┌──────────────────── per-window 窗口通道（有界，字节预算）────────────────┐
      │                          │                          │                    │
      ▼                          ▼                          ▼                    │
┌─────────────┐          ┌─────────────┐          ┌─────────────┐               │
│ Window      │          │ Window      │          │ Window      │               │
│ Actor A     │          │ Actor B     │          │ Actor C     │               │
│ 单写者：     │          │             │          │             │               │
│  reorder    │          │             │          │             │               │
│  (per-src)  │          │             │          │             │               │
│  append     │          │             │          │             │               │
│  watermark  │          │             │          │             │               │
│  broadcast  │          │             │          │             │               │
│  evict      │          │             │          │             │               │
└──────┬──────┘          └─────────────┘          └─────────────┘               │
  rule ch(32)│ ← RulePush（不变）                                               │
       ▼                pipe ch(有界)│                                           │
  [rule task ×N] ──emit──▶ [pipe Window Actor]                                   │
       │    ▲                                                               │   │
       │    │ pull/join/snapshot：直读 Arc<Window>（LF 底座，Phase A 不动）    │   │
       └─▶ [sink ×8]                                                          │   │
                          ◀───────────────────────────────────────────────────┘   │
```

（说明：多 rule 写同一 pipe 时投递到该 pipe 窗口通道，图中省略汇流箭头。）

三种订阅关系全部落在有界 channel 上：

1. **window 订阅 stream**：parse worker 解析完成后，按订阅表把每窗口消息
   直接发进该窗口通道（新增，字节有界）；
2. **rule 订阅 window**：既有 RulePush fanout（通道 32），不变；
3. **rule 写 pipe**：`append_intermediate` 改为投递 pipe 窗口通道（新增），
   多个 rule 写者经通道汇成 pipe actor 的单写者。

---

## 3. 设计原则

1. **单写者**：每窗口一个 actor 任务，窗口数据面只有该任务写。
   不再需要"多写者并发安全"，但保留 LF 的并发**读**能力。
2. **通道按字节有界**：背压显式化、可配置、可观测。锁的"隐形背压"
   由通道预算显式接管——这是修复 RSS 回退的核心机制。
3. **读路径 lock-free**：rule pull（events_since cursor）、join_lookup、
   snapshot、metrics 采样继续直读 `Arc<Window>`（SkipMap 无锁读），
   与 actor 写并发安全。
4. **per-source 保序**：真正的顺序 invariant 是"同一 source 内批次有序"
   （event-time 单调性、回放确定性、watermark 正确性都由它保证）。
   现状的全局 seq 全序是 ingress 分配竞态下的伪保证（见 4.2），
   reorder 随之按 (window, source) 维度下沉到 actor。

---

## 4. 组件设计

### 4.1 WindowActor

每窗口一个 tokio 任务，持有 `Arc<Window>` 的独占写权（独占 = 只有它调用
append/evict 类方法；对象本身仍是 `&self` API，供读者并发访问）。

**reorder 下沉**：actor 为每个向它供数的 source 维护一个游标
`next_seq[source]`，乱序到达的批次进 `pending: BTreeMap<(source, seq), _>`，
游标推进时依序落账：

```rust
struct SourceCursor { next_seq: u64 }

enum WindowMsg {
    /// 源数据批（parse worker 直接投递；同 source 保证 seq 连续递增）
    Append {
        source: Arc<str>,
        seq: u64,
        batch: RecordBatch,
        events: Option<Arc<Vec<Arc<Event>>>>,   // None = 无 rule 订阅的快路径
        content_bytes: usize,
        events_bytes: usize,
    },
    /// 中间窗口：上游 rule task 的 emit 产物（无 seq，FIFO 即序）
    Intermediate { batch: RecordBatch },
    /// 订阅表变更（rule spawn / 关停裁剪）
    Subscribe { sub: Subscription },
    Shutdown { reply: oneshot::Sender<()> },
}

async fn run_window_actor(
    win: Arc<Window>,
    fanout: Arc<RuleFanout>,
    mut rx: mpsc::Receiver<WindowMsg>,
    evict_interval: Duration,
    metrics: Option<Arc<RuntimeMetrics>>,
) {
    let mut cursors: HashMap<Arc<str>, SourceCursor> = HashMap::new();
    let mut pending: BTreeMap<(Arc<str>, u64), PendingAppend> = BTreeMap::new();
    let mut ticker = tokio::time::interval(evict_interval);
    loop {
        tokio::select! {
            biased;
            Some(msg) = rx.recv() => match msg {
                WindowMsg::Append { source, seq, .. } => {
                    pending.insert((source.clone(), seq), item);
                    // 推进该 source 的连续序列；注意：**缺序不阻塞 recv**，
                    // 缺口批次之后的内容都进 pending（见下方"无死锁论证"）
                    while let Some(item) = pending.remove(&(source.clone(), cursor.next_seq)) {
                        let (outcome, seq) = win.append_with_watermark_...(...)?;
                        if appended { fanout.broadcast(...).await; win.notify_waiters(); }
                        cursor.next_seq += 1;
                    }
                }
                WindowMsg::Intermediate { batch } => { /* parse+append+broadcast */ }
                ...
            },
            _ = ticker.tick() => { win.evict_expired(); /* ack floor 语义不变 */ }
        }
    }
}
```

关键性质：

- **无死锁论证**：actor 即使在等某个缺口 seq，也**继续 dequeue** 进
  pending，窗口通道不会因 reorder 等待而停止消费 → 上游 send 永远可
  排空。缺口批次只可能滞留在：parse 通道（PrereadBudget 有界）、
  某 parse worker（数量 = worker 数）、或已在本 actor pending 里——
  总量被上游预算**传递性封顶**，pending 不需要独立上限。
  source 正常关闭时其最后一个 seq 之后不会再有缺口；parse worker
  异常中止产生的真缺口，沿用现状语义（关停期 warn + drain，见
  run_commit_worker 的既有处理）。
- **broadcast 的 await 背压已从全局路径剥离**：慢 rule 只阻塞其所在
  窗口 actor 的循环，窗口通道提供缓冲深度，之后才反压 parse worker
  对该窗口的 send——影响范围从"全局所有窗口"缩小为"同流窗口链"。
- **actor 吞吐量估算**：单次循环 = reorder BTreeMap 取出 + SkipMap
  insert（亚 µs）+ 10 路 Arc channel send（µs 级）。q1 峰值 ~110 批/s，
  预算三个数量级余量；append 本身不是热点（C2 采样已证实）。
- **late 判定/路由报表**：actor 把 per-window 计数（delivered/late）
  写入 metrics（原子），不再有全局聚合返回值。

### 4.2 取消 commit worker（v2 核心变更）

现状 `run_commit_worker`（单任务串行漏斗）承担两件事：
BTreeMap 全局 seq reorder + `route_commit`（append + broadcast await）。
v2 将其整体取消：

- **reorder 不再需要全局维度**。审查发现全局 seq（parse_pool.rs
  `parse_seq: AtomicU64`，`build_parse_item` 中 fetch_add）在多 source
  并发下 fetch_add 与 channel send 之间存在固有竞态——今天的"全局
  全序"本来就是 best-effort。真正可靠的顺序保证只存在于单 source
  的接收循环内（串行 fetch_add + send）。因此：
  - seq 改为 **per-source**（每 source 接收循环一把 `AtomicU64`，
    赋值点不变，只是粒度变化）；
  - reorder 按 (window, source) 在 actor 内完成（4.1）。
  - 语义影响：跨 source 的到达交织从"伪全序"变为显式 per-source
    序。跨 source 顺序本来就非确定（多个 TCP 连接的到达顺序），
    watermark 单调性（fetch_max）与 late 判定不受影响；文件回放
    确定性由单 source 内保序完整保留。
- **parse worker 直接分发**：`route_parse` 产出 `ParsedRoute` 后，
  parse worker 遍历 `parsed.windows`，对每个窗口：
  1. acquire 该窗口的字节预算（见 4.3）；
  2. `window_tx.send(WindowMsg::Append { source, seq, ... }).await`。
  多窗口批次共享的 `RecordBatch`/events 都是 Arc，重复 send 零拷贝。
- **PrereadBudget 许可移交**：现状在 commit 完成后释放；v2 改为
  parse worker 完成对**所有**订阅窗口的 send 后释放（封装为
  `Arc<PermitGuard>`，最后一个引用 drop 时归还）——预算责任转移给
  窗口通道字节预算接管，避免同一批字节在两本账里重复占额。
- **`Router::route()`（file 回放、测试用同步路径）保留直写模式**：
  构造时未挂 actor 的 Router 走原 route_commit 逻辑，测试零改动。
  （file 回放在生产 spawn 路径同样经 parse pool → actor，与 TCP 一致。）

**收益**：全局串行漏斗（C2 采样中 9ms/loop 的锁+await 延迟点）彻底
消失，数据面 parse→window→rule 全程无全局串行点；跳数减一。

### 4.3 窗口通道与字节预算

复用 PrereadBudget 的成熟模式（acquire 分块、spawn_blocking 慢路径）：

- 每窗口一个 `Arc<Semaphore>` 字节预算，config 新增
  `window_buffer_bytes`（默认 **64MiB**，下限 4MiB）；
- parse worker / pipe 写者 send 前按 `content_bytes + events_bytes`
  acquire，actor 完成 append（或判定 late 丢弃）后 release；
- 深度（条数）`mpsc::channel(WINDOW_CHANNEL_DEPTH)`，默认 16——
  与字节预算共同上界 in-flight：8MiB 帧下每窗口最多 64MiB 在途，
  替代被移除的锁串行化。

**内存总量对照**（q1 三窗口）：原锁版隐性 in-flight（锁队列深度不可控，
实测膨胀 ~8GB）→ 显式上界 3 × 64MiB = 192MiB。

### 4.4 中间窗口（pipe）——rule 侧通道化（A.2 as-built）

现状（C2 基线）：rule 对 pipe 的 emit 走逐行路径——每行
`build_pipeline_batch`（单行 RecordBatch）+ `batch_to_events` +
一次 fanout channel send（每行一次 await）。

改法（as-built，已实现）：**pipe 不起窗口 actor、不落窗口存储**，
保持纯中继语义；emit 侧改为列批暂存 + 单广播：

- `PipeState{Uninit/Staging/Dead}`（std::Mutex，emit 保持 `&self`；
  形状解析失败 → warn + Dead，与旧路径失败语义一致）；
- `PipeBatchStager`：per schema 字段的类型化列缓冲
  （Int64/Float64/Bool/Utf8/Timestamp/Null），coercion 矩阵与
  `value_to_single_row_array` 完全一致（含 `PIPE_EVENT_TIME_FIELD`、
  time_col 回退、optional 字段缺省 null）；
- flush 挂点与 sink alert 批交付同界：`process_batch` 末尾、
  `scan_timeouts` 末尾、`flush()` 末尾——交付延迟上界不变；
- flush = 一个 N 行 RecordBatch + 一次 `batch_to_events` + 一次
  `fanout.broadcast(target, events, seq=u64::MAX)`（纯中继语义，
  消费侧 `saturating_add` ack 不回退游标）；sharded 订阅下
  `broadcast_sharded` 按 key 分区、每 shard 单次整批 send；
- 背压：broadcast await 满 shard 通道（RULE_CHANNEL_CAPACITY=32），
  规则 emit 反压进既有通道容量语义；
- 锁纪律：stager 的 Mutex 只覆盖同步暂存/取批，broadcast await
  在锁外——锁内无 await，无跨任务死锁面。

原"pipe 窗口起 actor + `WindowMsg::Intermediate`"方案搁置：pipe 数据
无窗口驻留需求（无 join/agg 读、无驱逐语义），actor 化只增加一跳
与一份存储；如 Phase B 需要 pipe 侧聚合再评估。

### 4.5 驱逐与 ack floor

- 独立 evictor 任务取消，驱逐成为 actor 的 ticker 分支
  （`evict_interval` 配置原样生效，bench 用 1s）；
- **WindowProgress ack floor 机制原样保留**：rule 处理完批 ack seq+1，
  actor 的 `evict_expired` 取 live slots 的 min 作 floor——
  "事件时间过期 && 全消费者已 ack" 的消费感知驱逐语义零变化；
- Phase2 内存驱逐（显式有损兜底）同样在 ticker 内执行；
- rule task Drop 时 release slot → u64::MAX 的路径不变。

### 4.6 读路径（Phase A 全部不动）

| 读路径 | 调用方 | Phase A 方案 |
|---|---|---|
| pull（events_since cursor） | rule task 首拉/补拉 | 直读 `Arc<Window>`，不变 |
| snapshot_with_timestamps | 聚合规则 | 直读，不变 |
| join_lookup | join 规则执行期 | 直读，不变 |
| metrics 采样（gauge） | sampling 任务 | 直读原子，不变 |
| schema/materialize 元数据 | route_parse/receiver | 构造期不可变 Arc，不变 |

LF 结构（SkipMap + 原子）保证这些读者与 actor 写者并发安全。**这是
LF 工作的全部复用点，也是增量迁移可行性的关键。**

Phase B（可选，另行评审）：join 索引归属迁移到订阅 rule task
（规则从 RulePush 自建 join 索引），实现"零跨任务读"的纯订阅模型；
pull 路径收缩为 bootstrap 补拉。收益是归属更纯净，代价是内存重复
与迁移面大，暂不排期。

### 4.7 订阅注册

- `RuleFanout` 表（冷路径 RwLock）保留原样：rule spawn 时 register，
  actor broadcast 时读——注册频率 = 任务数，无争用；
- `route_parse` 的 `has_subscribers` 判断改为窗口上的
  `AtomicBool has_rule_subscribers`（register/unregister 时维护），
  parse worker 读原子不进 fanout 读锁。

---

## 5. 有序性与正确性不变量

1. **per-source 序（v2 起 invariant）**：source 接收循环分配 seq →
   parse 通道 FIFO（Mutex recv）→ parse 完成乱序 → 窗口通道 →
   actor per-source reorder → 按序 append。event-time 单调性前提、
   回放确定性、watermark 推进单调性（fetch_max）全部由它保证。
   跨 source 顺序显式声明为非确定（原本即如此，见 4.2）。
2. **RulePush 语义**：seq = 窗口批序号，at-least-once cursor、
   ack seq+1、`cursor_gap` 观测口径全部不变。
3. **pipe 序**：per-emitter FIFO，跨 emitter 交错（同现状）。
4. **背压链完整性**：source →(PrereadBudget)→ parse →(窗口字节预算)→
   actor →(rule 通道 32)→ rule →(pipe 字节预算)→ pipe actor；
   rule →(sink 通道 2048)→ sink。任何一段慢，压力最终回传 TCP 源，
   不再依赖任何隐式串行化，且无全局串行漏斗。

---

## 6. 内存上界账本（q1, 8MiB 帧）

| 段 | 上界 | 机制 |
|---|---|---|
| parse + commit 通道 | 256MiB | PrereadBudget（不变） |
| 窗口通道 in-flight | 3 × 64MiB = 192MiB | window_buffer_bytes（新） |
| actor reorder pending | ≤ 上游在途总量（传递性有界） | PrereadBudget + worker 数 |
| 窗口驻留 | 消费 backlog 决定 | ack floor 驱逐（不变） |
| rule 通道 | 32 批 × N rule（Arc 共享，钉在窗口账本内） | 计数有界（不变） |
| pipe 通道 | 64MiB/pipe | window_buffer_bytes（新） |
| sink 通道 | 2048 批 ×8（列批 Arc） | 计数有界（不变） |

理论 RSS 峰值回到 ~3GB 水平（窗口驻留主导），且每段可解释、可配置。

---

## 7. 迁移步骤（增量提交）

### A.1 actor 化 + parse 直发（✅ 已完成，commit 46b99bb）

- 新增 `WindowActor` + `WindowMsg` + 窗口通道/字节预算 + per-source
  seq/reorder；
- 取消 commit worker：parse worker 遍历订阅窗口直接 send；
  PrereadBudget 许可改 Arc 倒计时、send 完成后释放；
- Router 保留同步直写模式（测试/file 回放 R2 路径）；
- wf-runtime spawn 处为每个窗口起 actor，shutdown 对接两阶段框架
  （先关 parse，窗口通道 drain 完毕后 actor 自然退出——复用
  graceful-shutdown.md 的 join 框架）。

**结果：q1 30M 6 轮交错 A/B 打平 C2 基线（3.641M vs 3.643M EPS，
-0.06%），RSS ~2.3GB，alerts 27.6M 一致，cursor_gap=0。**

### A.2 rule 侧通道化（pipe 列批暂存）（实现完成，集成验证进行中）

- §4.4 as-built：`PipeBatchStager` 列暂存 + 每输入批一次 broadcast；
- 局部正确性：workspace 全量 **1244 测试全绿**（`pure_relay` 用例
  更新为"单 push 含多事件"批语义断言 + 第二批 flush 边界断言）；
- 集成验证（提交门槛，均未过）：
  - q1p（管道压测查询，`models/queries/q1p.wfl`）在新二进制上
    确定性停滞——已定位停滞点在 rule 侧**上游**（rx_rows 冻结
    2,714,786 / routed=0 / 零 P-DIAG，process_batch 从未被调用），
    根因待查，见 §8 Q4；
  - q1 单轮回归（确认无回退）；
- 提交前移除 P-DIAG 诊断面包屑（rule_task.rs 三处 wf_warn；该文件 2026-09 P4 已目录化为 `engine_task/rule_task/`）。

### A.3 驱逐入 actor（未排期）

- 现状仍为独立 evictor 任务（`wf-runtime/src/evictor_task.rs`）；
  正确性已由消费感知驱逐 + ack floor（2563b8a）保证，此项仅为
  归属纯净化，非阻塞项。

### A.4 观测补强（未排期，原 A.3）

- 新 gauge：窗口通道深度/字节水位、actor reorder pending 深度、
  actor 循环延迟（p50/p99）；
- counter：窗口通道 full 次数（背压可视化）、reorder 缺口等待
  （stall 次数/最长等待，监测 parse straggler）。

每步门槛：wf-engine + wf-runtime 全量测试绿；q1 30M 不限速 A/B
（与 C2 基线 3 对交错）无正确性回退（emitted=27.6M、cursor_gap=0、
aborted=0 限速口径）。

---

## 8. 风险与开放问题

| # | 风险/问题 | 评估 |
|---|---|---|
| R1 | 慢 rule 经窗口通道反压 parse worker，仍可能跨窗口耦合（parse worker 逐窗口顺序 send，头窗口满则后续窗口等待） | 64MiB/窗口缓冲；且 send 顺序可按"先可用后拥塞"重排；与现状 broadcast await 的耦合范围一致，可观测（A.3）后按需调 |
| R2 | parse straggler 使某窗口 pending 堆积、append 可见性延迟 | 与现状 commit worker 等缺口**语义完全一致**（今天同样会 pending 等待），且 pending 传递性有界；A.3 加 stall 观测 |
| R3 | actor 单线程成为新漏斗 | append 非热点（采样证实），预算三个数量级；若未来成为瓶颈，窗口已天然各自并行 |
| R4 | shutdown 漏 drain 丢尾批 | 复用 two-phase join 框架；用 aborted=0 限速口径验收 |
| R5 | per-source seq 改造遗漏多 source 共写同 stream 的场景 | 若多个 source 喂同一 stream：per-source 序 + 显式交织声明（现状全局 seq 在该场景本就有分配竞态，不构成回退）；ingress 只改 seq 粒度，赋值点不变 |
| Q1 | rule 通道（32 批）是否也改字节有界？ | 暂不改：RulePush 是 Arc，实际内存由窗口 ack floor 钉住，双计无益。8MiB 帧下如需再评估 |
| Q2 | `append_intermediate` 返回值改 fire-and-forget 可接受？ | 调用方仅遥测用途，建议接受 |
| Q3 | Phase B（join 归属 rule）是否排期 | 暂不排期，本设计 Phase A 已达成目标 |
| Q4 | **q1p 确定性停滞（已定位并修复，2026-08-16）**：A.1 窗口字节预算的 **acquire 侧两处死锁**，与 A.2 pipe 改动无关（q1p 只是首个触发者：bid 单窗批 117MB > 64MiB 预算）。① 超预算批 chunk 式分批获取 → dispatcher 自持部分许可等剩余，actor 等永不出现的消息（首份大消息无 earlier releases）；② 修复①引入钳制后并发 dispatcher 交错瓜分预算（哲学家就餐）；③ reorder pending 持预算许可停车，而持缺口 seq 的 dispatcher 正卡在 acquire 等该预算。修复：acquire 一次原子获取 min(bytes, capacity)（超限批独占整个预算直至 actor 消费），消息进 pending 前释放许可（pending 字节由 parse 侧在途预算天然有界）。回归测试 3 个：oversized_batch_acquisition_clamps_to_capacity / concurrent_oversized_acquisitions_do_not_interleave / parked_out_of_order_message_releases_budget | 已解决：q1p 865K→1.32M EPS(+53%)、RSS -24%；q1 无回退 |

---

## 9. 验证计划

1. **单元/集成**：workspace 全量 **1244 测试全绿**（A.1+A.2 当前
   工作区实测）；新增：
   - actor per-source reorder 测试（乱序投递 → 按序落账；双 source 独立游标）；
   - 窗口预算背压测试（占满预算 → send 挂起 → append 释放）；
   - 无死锁验证：缺口期持续投递后续批次（pending 吸收、通道不堵死）；
   - pipe 多写者 FIFO 测试、shutdown drain 测试；
   - A.2 补充：pipe 列批单广播语义（单 push 含多事件、第二批
     flush 边界）——已落（`pure_relay_broadcasts_to_sharded_downstream`）。
2b. **A.2 集成门槛（提交前）**：
   - q1p 30M A/B：新二进制显著快于 C2 基线 865K EPS，且无停滞
     （已通过，见 §8 Q4）；
   - q1 30M 单轮：与 A.1 基线（3.64M EPS）无回退。
2. **A/B 口径**：q1 30M 8MiB 帧（bench 默认）不限速，3 对交错，
   `/tmp/wfusion.c2` vs 新二进制。**验收：EPS ≥ 4.0M 且
   RSS_peak ≤ 4GB，emitted/cursor_gap/aborted 与 C2 持平。**
3. **加测 1MiB 帧一轮**：确认两种帧口径下 RSS 均受控
   （修复必须不依赖帧大小这个混淆变量）。

---

## 附录：已否决/搁置路径存档

- **mimalloc/jemalloc 旋钮调优**（PURGE_DELAY=0 / ABANDONED_RECLAIM /
  零 decay）：治标且掉速或无效，RSS 根因是 in-flight 无界；
- **共享 SkipMap 多写者**：吞吐有效但失去背压归属，保留其代码作为
  actor 模式的并发读底座；
- **commit worker 薄化保留（v1 方案）**：v2 用 per-source reorder
  下沉取代——全局漏斗彻底消失，reorder 语义从"伪全序"改为显式
  per-source 序（不变量更强、更诚实）。
