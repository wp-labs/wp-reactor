# 第一部分：核心概念与处理过程

## 1. WarpFusion 是什么

WarpFusion 是一个**实时关联计算引擎**。它的输入是结构化事件流，输出是风险告警。

它解决的核心问题是：**跨多条事件、跨时间窗口的模式匹配**。例如"同一 IP 在 5 分钟内登录失败 3 次，然后发起端口扫描"——传统无状态管道做不到，WarpFusion 通过维护有状态窗口和 CEP 状态机来做到。

关键定位：

| 是 | 不是 |
|---|---|
| 实时事件关联检测引擎 | 通用流计算平台 |
| 声明式规则驱动 | SQL 查询引擎 |
| 单二进制、零外部依赖 | 任意 DAG 编排工具 |
| 单机起步，可分布式扩展 | 批处理框架 |

---

## 2. 三文件架构

每个 WarpFusion 项目由三类文件组成，职责严格分离：

```
my-project/
├── wfusion.toml          # 物理参数：怎么跑
├── schemas/
│   └── security.wfs      # 数据定义：数据长什么样
├── rules/
│   └── brute_force.wfl   # 检测逻辑：匹配什么模式
└── sinks/                # 输出配置：告警发到哪里
```

| 文件 | 职责 | 典型内容 |
|------|------|---------|
| `.wfs` | 逻辑数据定义 | window 名、订阅的 stream、时间字段、保持时长(over)、字段 schema |
| `.wfl` | 检测规则 | 事件绑定、匹配条件、评分、实体、输出 |
| `.toml` | 物理约束 | 监听地址、内存上限、watermark 延迟、sink 路由；窗口默认值与按窗口覆盖放在独立 `windows.toml`（由顶层 `windows = ...` 引用） |

依赖方向：`.wfs` ← `.wfl` ← `.toml`

- `.wfl` 通过 `use "security.wfs"` 引用 window 定义
- `.toml` 通过 `schemas` / `rules` 字段指向文件路径
- `.wfs` 变更需重启引擎；`.wfl` 和 `[vars]` 支持热加载

---

## 3. 核心概念

### 3.1 Window — 带订阅声明的时间窗口

Window 是 WarpFusion 的核心抽象。它不是一个被动的缓冲区，而是一个**带订阅条件的、时间有序的数据持有者**。

```wfs
window auth_events {
    stream_tag = "syslog"       # 订阅的数据流
    time = event_time       # 事件时间字段
    over = 5m               # 数据保留时长

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}
```

每个 Window 回答三个问题：
- **数据从哪来**：`stream_tag` 声明订阅的逻辑流 tag，支持多 stream tag
- **数据保留多久**：`over` 定义窗口时间跨度。`over = 0` 表示静态集合（不入队，类似快照表）
- **字段长什么样**：`fields` 定义 schema，输入字段使用 scalar 类型（`chars`、`digit`、`float`、`bool`、`time`、`ip`、`hex`）；输出和中间 window 还可以使用 `object`、`array`、`array/T` 承载结构化结果

一个 stream tag 可以被多个 Window 订阅。例如 `syslog` 可以同时被 `auth_events`（5 分钟窗口）和 `auth_stats`（1 小时窗口）订阅，各自独立维护数据。

### 3.2 事件时间与 Watermark

Window 基于**事件时间**（event time）管理数据生命周期，而非处理时间（系统收到数据的时间）。

```
时间轴：
  ──────────┬───────────────┬──────────────┬──────────→
            │               │              │
       watermark      watermark +      当前最新
                    allowed_lateness   event_time
```

三个核心概念：

| 概念 | 含义 | 示例默认 |
|------|------|--------|
| **Watermark** | 当前认定"不会再收到更早数据"的时间点。计算方式：`max(已收到事件时间) - watermark_delay` | `5s` |
| **Allowed Lateness** | watermark 之后仍允许的迟到时间窗口 | `0s`（不接受迟到） |
| **Late Policy** | 超过允许范围的数据如何处理 | `drop`（丢弃） |

这些值来自 `[window_defaults]` 配置（所有字段均为必填，无隐式默认）；上面列出的 `5s / 0s / drop` 是示例配置的取值。

迟到策略选择：
- `drop`：直接丢弃。适用于数据基本有序的场景（绝大多数）
- `revise`：追加到窗口，可能触发规则重算。适用于乱序严重但精度要求高的场景
- `side_output`：当前实现尚未落地，运行时等价于 `drop`（保留策略枚举用于前瞻）

### 3.3 Match — 模式匹配的核心

`match` 子句是规则的心脏，定义"在什么条件下触发告警"。

```wfl
match<sip:5m> {
    on event {
        fail | count >= 3;
    }
    and close {
        fail | count >= 1;
    }
} -> score(70.0)
```

结构拆解：

| 部分 | 含义 | 示例 |
|------|------|------|
| `<sip:5m>` | scope key + 窗口时长。按 `sip` 分组，每组独立维护 5 分钟滑动窗口 | 不同 IP 的事件互相隔离 |
| `on event { ... }` | 事件驱动触发。条件满足时推进状态并可能产出告警 | 累计 3 次失败 → 命中 |
| `on close { ... }` | 窗口关闭时触发（OR 模式）。事件路径独立触发，关闭路径独立评估 | 窗口结束时若没达到阈值也告警 |
| `and close { ... }` | AND 模式关闭。必须 `event_ok && close_ok` 才产出告警 | 事件先满足 + 关闭时额外条件也满足 |

**关闭触发三种方式**：Timeout（超过 `over` 时长）、Flush（引擎关闭时）、Eos（数据流结束）。

**排序模式（`on event seq` / `on event any`）**：

`on event` 的 `seq` / `any` 修饰符声明步骤的**排序模式**。裸 `on event { ... }` 等价 `seq`，向后兼容：

- `on event seq { ... }` — 有序序列：步骤按书写顺序完成（step i+1 只在 step i 完成后评估），
  支持 `has <alias>`（存在性，隐式 `count >= 1`）、`within <dur>`（步间时间 gap）、
  `not has <alias> within <dur>`（否定步）、`consec`（严格相邻）。`skip` 可解析，
  但运行时当前只实现默认的 `past_last`（失败后回退到上一步），`skip = to_next` 延后到 L3。
- `on event any { ... }` — 无序共现：所有步骤并行评估，全部满足即触发，顺序无关
  （不支持 `within` / `not` / `consec` / `skip`，编译期拒绝）。

```wfl
match<sip:30m> {
    on event seq {
        has scan;                    # 存在性：scan 至少一次
        has login within 10m;        # login 必须在 scan 后 10m 内
        not has failed within 5m;    # 否定：scan 后 5m 内无失败登录
    }
}
```

**实例过期（窗口 TTL）**：每条规则的实例按窗口时长保留（`match<sip:5m>` → 距最后事件 5m）。
运行时周期扫描会按墙钟时间推进有效水位（`watermark + 距上次处理事件的墙钟时间`），因此
**输入静默时实例也会按窗口 TTL 自动过期释放**，符合窗口的时间语义——无需新事件触发。

### 3.4 CEP 状态机 — 怎么执行 match

每条规则编译为一个 `MatchPlan`，运行时由 `CepStateMachine` 驱动。状态机按 scope-key 维护独立的 `Instance`。

```
事件到达 → advance(alias, event)
  ├─ Accumulate → 事件已消费，未跨越步骤边界（等待更多事件）
  ├─ Advance    → 步骤条件满足，推进到下一步
  └─ Matched    → 所有步骤完成，规则命中 → 生成 AlertRecord
```

多步骤 OR 分支：

```wfl
match<sip:5m> {
    on event {
        fail | count >= 3;           # 步骤 1
        scan : firewall_logs &&      # 步骤 2（OR 分支）
              action == "port_scan" | count >= 1;
    }
}
```

`fail | count >= 3` 满足后推进一步，然后有两条可能的路径——`|| scan | count >= 1` 任一满足即命中。

**聚合度量**：`count`、`sum`、`avg`、`min`、`max`。**变换**：`distinct` 在累积阶段去重（通过管道 `| distinct |` 使用，不是独立函数）。

### 3.5 OutputRecord — 告警输出

规则命中后，`RuleExecutor` 求值 score 和 entity 表达式，生成 `OutputRecord`：

```
OutputRecord {
    wfx_id      # 确定性输出 ID：SHA-256 内容哈希（16 hex），非字面拼接
    rule_name   # 规则名
    score       # 风险评分 [0, 100]
    entity_type # 实体类型（来自 entity(type, ...) 声明）
    entity_id   # 实体 ID
    origin      # Event | Close { reason }，reason 取 Timeout | Flush | Eos
    fired_at    # 基于事件时间或 close 水位生成的业务时间
    emit_time   # 发出时间
    summary     # 引擎生成的摘要
    yield_target# yield 目标 window
    fields      # yield 字段
}
```

`yield` 字段可以是 scalar，也可以是结构化值。结构化值用 WFL 字面量构造：

```wfl
yield security_alerts (
    risk_context = object {
        score = @score;
        source = e.sip;
        tags = array ["bruteforce", "ssh", e.action];
    }
)
```

`object` / `array` 是 WFL 内部的结构化值，不是要求用户手写 JSON 字符串。最终 JSON、XML、文本或 CSV 如何表达由 sink 决定；中间 window 为了继续被下游规则消费，会把结构化值按 UTF-8 JSON 字符串桥接。

输出业务时间时，优先把时间系统变量显式映射为普通业务字段，而不是依赖内部 `__wfu_*` 元数据字段：

```wfl
yield security_alerts (
    first_seen = @event_first_time,
    last_seen = @event_last_time,
    evidence_start_time = @evidence_start_time,
    evidence_end_time = @evidence_end_time,
    rule_window_start = @window_start_time,
    rule_window_end = @window_end_time,
    latest_analysis_time = @emit_time
)
```

这些字段通常在输出 window 中声明为 `time`。`@event_first_time` / `@event_last_time` 表达本次命中证据的首尾事件时间，`@window_start_time` / `@window_end_time` 表达规则窗口边界，`@emit_time` 表达本条输出记录的稳定产出时间。

输出规则触发原因时，优先使用稳定统计上下文，而不是依赖内部字段名或临时聚合表达式：

```wfl
yield security_alerts (
    window_events = stat.count(window_event(auth)),
    matched_events = stat.count(match_event(failures)),
    distinct_users = stat.count(match_distinct(user_spray)),
    trigger_count = stat.value(trigger(user_spray)),
    final_count = stat.value(final(final_users))
)
```

这些 selector 只在 `yield` 中作为 `stat.count(...)` 或 `stat.value(...)` 参数使用。`window_event(alias)` 引用 source alias；`match_event(label)`、`match_distinct(label)`、`trigger(label)` 引用 `on event` label；`final(label)` 引用 `and close` label。参数是静态符号，不加引号，缺失或阶段不匹配会在编译期报错。

`OutputRecord` 序列化后，由 `SinkDispatcher` 按 `yield_target` 路由到配置的 sink 组。

### 3.6 Sink 路由 — 告警发到哪里

告警通过 Connector 模式输出：

```
OutputRecord → to_data_record() → DataRecord → SinkDispatcher.dispatch(yield_target, DataRecord)
                        ├─ 匹配 business group (按 yield_target 通配符)
                        ├─ 无匹配 → default group (兜底)
                        └─ 写入失败 → error group (容错)
```

配置结构（`sinks = "sinks"` 指向 `sinks/` 目录，Connector 定义放在同级 `connectors/` 目录下）：

```
sinks/
├── defaults.toml       # 全局默认值
├── business.d/         # 业务路由组（window 通配符匹配）
│   └── security.toml
└── infra.d/            # 基础设施组
    ├── default.toml    # 兜底
    └── error.toml      # 容错
connectors/
└── sink.d/             # Connector 定义（type + 默认参数）
    └── file_json.toml
```

---

## 4. 端到端处理流程

下面追踪一条数据从进入到输出的完整路径。

### 阶段 1：启动 Bootstrap

```
1. 加载 .wfs 文件 → WindowSchema[]
2. 加载 .toml → FusionConfig（物理参数、sink 配置）
3. 加载 .wfl 文件 → 变量预处理 → 解析 → 语义检查 → 编译 → RulePlan[]
4. schema_bridge: WindowSchema × WindowConfig → WindowDef[]
5. WindowRegistry::build(defs) → 创建 Window 实例 + 订阅表 + Notify
6. Router::new(registry)
7. 构建 RunRule[]（预计算 stream_name → alias 路由）
8. 构建 SinkDispatcher（从 sinks/ 目录加载）
```

### 阶段 2：任务启动（LIFO：消费者先于生产者）

```
启动顺序: alert → evictor → rules → receiver

TaskGroup: alert
  └─ run_alert_dispatcher (×1) ← mpsc channel 消费者

TaskGroup: evictor
  └─ run_evictor (×1) ← 定时扫描过期窗口数据

TaskGroup: rules
  └─ RuleTask (×N, 每条规则一个) ← Notify 唤醒 + cursor 拉取

TaskGroup: receiver
  └─ Receiver accept loop (×1) + per-connection task (×N)
```

### 阶段 3：数据输入与路由

```
TCP 客户端发送: [4B BE len][stream_name][Arrow IPC RecordBatch]

Receiver:
  1. accept TCP 连接 → per-connection task
  2. read_frame() → 解析长度前缀 → 提取 stream_name + Arrow IPC payload
  3. decode_ipc() → RecordBatch（零反序列化）
  4. Router.route(stream_name, batch)

Router:
  1. 查询订阅表: WindowRegistry.subscribers_of(stream_name)
     → 找到订阅该 stream 的所有 Window
  2. 对每个 Window:
     a. 迟到检查: min_event_time < watermark - allowed_lateness?
        → Drop / Revise / SideOutput
     b. append_with_watermark(batch) → 追加到 Window 的 VecDeque
     c. 推进 watermark = max(current, max_t - delay)
     d. 释放写锁
     e. Notify.notify_waiters() → 唤醒关联 RuleTask

如有 stream_name 无订阅者 → 静默丢弃
```

### 阶段 4：规则执行

```
RuleTask 主循环:

1. pull_and_advance():
   - window.read_since(cursor) → (batches, new_cursor, gap_detected)
   - 更新 cursor
   - batch_to_events(batch) → Vec<Event>
   - for event in events:
       for alias in stream_aliases[stream_name]:
         match machine.advance(alias, event):
           Accumulate → 继续
           Advance    → 继续
           Matched(ctx) →
             alert = executor.execute_match(ctx)
             alert_tx.send(alert)

2. select! 等待:
   ├─ Notify 触发 → 回到步骤 1
   ├─ timeout_scan_interval.tick() → scan_expired() → 处理超时关闭
   └─ rule_cancel.cancel() → 最终 drain + close_all(Flush) → 退出
```

Cursor 机制：
```
Window batches:  [seq=3] [seq=4] [seq=5] [seq=6]
                                    ▲
                              cursor = 5

read_since(5) → 返回 [seq=5, seq=6], new_cursor=7
```

RuleTask 初始化 cursor = `window.next_seq()`，只处理启动后新到达的数据。

### 阶段 5：告警输出

```
RuleTask.alert_tx.send(OutputRecord)
        │
        ▼
alert_dispatcher (mpsc channel 消费):
  1. OutputRecord → to_data_record() → DataRecord
  2. SinkDispatcher.dispatch(yield_target, DataRecord)
  3. 匹配到的 business group 中的所有 SinkRuntime 顺序写入（逐个 await）
  4. 无 business group 匹配 → default_group
  5. 写入失败 → error_group
```

### 阶段 6：优雅关闭（LIFO：生产者先于消费者）

```
Phase 1: 停止数据输入
  cancel.cancel()
  → Receiver 停止 accept，现有连接完成当前帧后退出
  → join receiver

Phase 2: 清空规则管道
  rule_cancel.cancel()
  → 每个 RuleTask:
    ① 最后一次 pull_and_advance()（处理 Window 剩余数据）
    ② machine.close_all(Flush)（flush 所有状态机实例）
    ③ drop alert_tx（关闭 channel sender）
  → join rules
  → join evictor
  → alert channel 关闭 → alert dispatcher 自动退出 → join alert

关闭顺序保证: 上游（receiver）先退出，下游（alert）最后退出 → 零告警丢失
```

---

## 5. 并发模型要点

| 原则 | 说明 |
|------|------|
| **Channel-close-driven** | Alert dispatcher 通过 channel 关闭信号自然退出，不用 CancelToken |
| **Pull-based** | RuleTask 通过 cursor 主动拉取 Window 数据，而非被动接收推送 |
| **Notify 唤醒** | Router append 后通过 `Notify::notify_waiters()` 唤醒关联 RuleTask |
| **Per-rule 独立** | 每条规则一个独立的 tokio task，互不阻塞 |
| **无锁引擎状态** | 每个 RuleTask 独占 `CepStateMachine`，无需 `Arc<Mutex>` |
| **两阶段取消** | `cancel`（停止 Receiver）+ `rule_cancel`（停止 RuleTask），防止关闭时丢数据 |

---

## 6. 关键设计决策

| 决策 | 原因 |
|------|------|
| Arrow IPC 作为传输格式 | 接收端零反序列化，类型保真，DataFusion 原生支持 |
| 规则引用 Window 而非 Stream | 同一 Stream 可被不同 Window 以不同方式（mode、over）订阅 |
| CEP 状态机而非 SQL | 时序模式匹配（A 发生 → B 发生）不适合 SQL 语义 |
| cursor-based 拉取 | 解耦 Router 写入和 RuleTask 处理，无需全局调度器 |
| LIFO 启停顺序 | 消费者先启动后退出，保证管道中没有数据丢失 |
| 单通道风险评分 | 每次命中产出单一 score [0, 100]，下游聚合简单可控 |

---

## 7. 下一步

- [快速开始](./quick-start.md) — 搭建第一个项目
- [WFL 语言参考](./language-reference.md) — 完整语法手册
- [运行时配置](./runtime-config.md) — TOML 配置详解
