# WarpFusion — 实时关联计算引擎设计方案
<!-- 角色：架构师 / 技术决策者 | 状态：v2.1 对齐中 | 创建：2026-02-13 | 更新：2026-02-20 -->

## 1. 背景与动机

### 1.1 现状

WarpParse（wp-motor）是一个高性能日志解析引擎，核心能力是：

- 通过 WPL 规则将非结构化日志解析为结构化 `DataRecord`
- 支持多种数据源（TCP、文件、Syslog）和输出目标（文件、TCP、Kafka）
- 单条解析，无状态，解析即输出

### 1.2 缺失能力

安全分析场景需要跨数据源的实时关联计算，例如：

- 检测"5 分钟内登录失败 3 次后发起端口扫描"的 IP（需要 JOIN auth_logs 和 firewall_logs）
- 统计"每分钟来自同一 IP 的不同目的端口数"（需要时间窗口聚合）
- 关联"DNS 查询异常域名后发起 HTTP 外联"（需要多流模式匹配）

这些能力超出 WarpParse 的设计范围——它是无状态的线性管道，无法缓存历史数据、执行 JOIN 或窗口聚合。

### 1.3 设计目标

构建 **WarpFusion**，一个独立的轻量级实时关联计算引擎：

| 目标 | 说明 |
|------|------|
| 独立部署 | 与 WarpParse 分离，独立进程 |
| 单机运行 | 无外部依赖（不依赖 Kafka、etcd、数据库） |
| 轻量 | 资源占用低（目标 <100MB 内存 / 空载） |
| WFL 驱动 | 用声明式 WFL 语言定义关联规则，编译为 Core IR 执行 |
| 实时 | 秒级关联延迟 |
| 可分布 | 单机和分布式统一模型，通过 Window 分布声明平滑扩展 |

### 1.4 术语约定（全文统一）

- **事件（Event）**：输入到 WarpFusion 的原始结构化日志记录（RecordBatch 行）。
- **规则命中（Hit）**：规则在某个窗口实例上满足 `on event`/`on close` 条件。
- **风险告警（Risk Alert）**：规则命中后输出的业务结果，核心字段为 `rule_name + score + entity_type/entity_id`（可含 `close_reason`、`score_contrib`）。
- **运维告警（Ops Alert）**：对系统运行状态（断连、积压、丢弃率）的监控告警，区别于业务风险告警。
- 代码中的 `AlertRecord` / `AlertSink` 保留原命名，但语义统一指“风险告警记录/输出通道”。

### 1.5 与 WFL v2.1 的实现对齐范围（本次更新）

- 规则治理：强制 `meta.lang`、`meta.contract_version`、`limits`、`yield@vN`。
- 语义收敛：`join` 强制 `snapshot/asof` 模式，不再允许隐式时点。
- 正确性门禁：发布前必须通过 `contract + shuffle + scenario verify` 三层校验。
- 可靠性分级：传输层从单一 best-effort 升级为可配置分级（默认仍为 best-effort）。

## 2. 整体架构

### 2.1 核心概念：Window 即订阅者

WarpFusion 的核心抽象是 **Window**——它不是被动的缓冲区，而是带条件的数据订阅者。

每个 Window 的定义分布在三个文件中：

1. **Window Schema (.wfs)**：逻辑属性——订阅哪个 stream、时间字段、保持时长（over）、字段 schema
2. **TOML Config (.toml)**：物理属性——分布模式（mode）、内存上限、over 能力上限（over_cap）
3. **WFL Rule (.wfl)**：使用方——通过 window 名称引用数据，定义检测逻辑

```
三文件架构：.wfs (数据定义) → .wfl (检测逻辑) → .toml (物理参数)

Window Schema (.wfs)  定义 stream 来源、字段 schema、over 时长
WFL Rule (.wfl)      引用 window 名称，定义 events/match/join/yield
TOML Config (.toml)  定义 mode(local/replicated/partitioned)、max_bytes、over_cap
```

**Window 分布模式（TOML 中配置）：**

| 模式 | 含义 | 路由行为 | 适用场景 |
|------|------|---------|---------|
| `partitioned(key)` | 按 key 分区，每节点持有子集 | hash(key) % N → 单节点 | 等值 JOIN 的主表 |
| `replicated` | 全局复制，每节点持有完整副本 | 广播 → 所有节点 | 小表、字典表、威胁情报 |
| `local` | 仅本机数据，不参与分布 | 不路由，就地消费 | 单机统计、调试 |

单机模式下所有 Window 实质上都是 `local`，无网络开销。分布式模式下 Router 根据 Window 的 mode 声明自动推导路由策略。**Window Schema 和 WFL Rule 不需要改变**——只需调整 TOML 中的 mode。

### 2.2 系统全景

```
WarpParse (wp-motor) ×N                WarpFusion (wp-reactor，独立进程)
┌──────────────────┐                  ┌──────────────────────────────────────────┐
│ Sources          │                  │                                          │
│   ↓              │  TCP             │  Receiver (accept 多连接)                │
│ Parser (WPL)     │  Arrow IPC       │    ↓                                     │
│   ↓              │  单向推送        │  Router (根据 Window 订阅声明路由)        │
│ Router           │  ────────────→   │    ↓                                     │
│   ↓              │                  │  ┌→ Window["auth_events"]    local        │
│ Sinks            │                  │  ├→ Window["fw_events"]      local        │
│   ├ File         │                  │  └→ Window["ip_blocklist"]   replicated   │
│   ├ TCP          │                  │                                          │
│   └ Arrow (新增) │                  │  Scheduler (事件驱动 + 超时扫描)           │
└──────────────────┘                  │    ↓                                     │
                                      │  RuleExecutor ×N (CEP 状态机 + Core IR) │
wp-reactor Node X ─TCP─┐              │    ↓                                     │
wp-reactor Node Y ─TCP─┼──→ 同一端口  │  AlertSink (风险告警输出: File/HTTP/Syslog)            │
                       └──→           └──────────────────────────────────────────┘
```

多个 wp-motor 实例和分布式模式下的其他 wp-reactor 节点，均通过 TCP 连入同一个监听端口。

### 2.3 传输协议

WarpParse → WarpFusion 之间使用 **Arrow IPC Streaming** 协议，通过 **TCP 单向推送**：

- 基于 Arrow 官方 Streaming 格式，带 schema + 数据块
- 统一使用 TCP 传输（同机走 `127.0.0.1`，跨机改为实际地址，零代码改动）
- 每个消息携带 `stream_name` 标识数据流（如 `auth`、`firewall`）
- 接收端零反序列化——Arrow IPC 直接映射为内存中的 RecordBatch
- 无应用层 ACK，背压依赖 TCP 流控（send buffer 满 → write 阻塞 → 发送端自然减速）

选择 Arrow IPC 而非 JSON/Protobuf 的原因：

| 维度 | Arrow IPC | JSON | Protobuf |
|------|-----------|------|----------|
| 接收后可直接计算 | 是（零反序列化） | 否（需解析） | 否（需解析） |
| DataFusion 原生支持 | 是 | 否 | 否 |
| 类型保真 | 完整（Int64/Float64/Timestamp/...） | 弱（数字精度丢失） | 需 .proto 定义 |
| 传输体积 | 中 | 大 | 小 |

### 2.4 传输可靠性语义

**投递保证：可靠性分级（默认 Best-Effort）。**

v2.1 起，WarpFusion 支持 `best_effort / at_least_once / exactly_once` 三档传输语义。默认仍为 `best_effort`，以保持低复杂度和低延迟。

#### 2.4.1 帧格式

每条 Arrow IPC 消息使用长度前缀分帧：

```
[4 字节 BE u32: payload 长度][payload: stream_name + Arrow IPC RecordBatch]
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `frame_len` | `u32` (big-endian) | payload 总长度 |
| `stream_name` | length-prefixed String | 数据流标识（如 `auth`、`firewall`） |
| `payload` | bytes | Arrow IPC Streaming 格式的 RecordBatch |

#### 2.4.2 可靠性等级

| 模式 | 语义 | 成本 | 适用场景 |
|------|------|------|----------|
| `best_effort` | At-Most-Once，断连期间允许丢失 | 最低 | 默认在线检测场景 |
| `at_least_once` | WAL + ACK，断连后重放，可能重复 | 中 | 需要可补算能力 |
| `exactly_once` | 幂等键/事务 sink，端到端去重一致 | 最高 | 高审计要求场景 |

通用保障仍包含：
- 背压：TCP 流控 + 本地有界队列。
- 断连：指数退避重连；非 `best_effort` 模式下配合 replay 追平。

#### 2.4.3 设计取舍

1. 默认 `best_effort`，优先保障吞吐、延迟与部署简单性。  
2. 对关键链路按需升级到 `at_least_once`，避免“一刀切”复杂化。  
3. `exactly_once` 仅在高价值链路启用，并配套幂等 sink。  
4. 三档模式共享同一 WFL/RulePlan，不影响规则语义与编译结果。  

> 默认模式下断连期间仍可能丢失数据；系统暴露 `connection_drops`、`replay_lag_seconds`、`reconnect_latency` 等指标供运维告警。

### 2.5 核心处理流程

```
1. 启动：加载 .wfs 文件注册 Window Schema，加载 .toml 绑定物理参数，加载 .wfl 编译规则
2. Receiver 从 Socket 接收 Arrow IPC 消息，解析 stream tag
3. Router 查询 WindowRegistry，找到订阅该 stream 的所有 Window
4. 按各 Window 的分布模式路由数据（单机直接本地分发）
5. Window 追加 RecordBatch，按时间窗口策略维护活跃数据
6. Scheduler 以事件驱动方式将新事件分发到相关规则的 match 引擎
7. RuleExecutor 推进 `on event/on close` 双阶段求值，命中后执行 `join(snapshot/asof)` 并计算 `score`（可输出 `score_contrib`）
8. 结合 `entity(type,id_expr)` 生成实体键，`yield target@vN` 写入目标 window（含系统字段）→ conv 后处理（如有）→ window sinks 输出
9. Evictor 定期淘汰过期窗口数据
```


## 3. 工程结构

### 3.1 代码仓库布局

WarpFusion 作为独立工程，与 wp-motor 平级。通过共享 crate `wf-arrow` 桥接。

```
wp-labs/
├── wp-motor/                  # 现有解析引擎（不动）
│
├── wf-arrow/                  # 共享 crate：Arrow 转换层
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── schema.rs          # DataType → Arrow DataType 映射
│       ├── convert.rs         # Vec<DataRecord> → RecordBatch
│       ├── reverse.rs         # RecordBatch → Vec<DataRecord>
│       └── ipc.rs             # Arrow IPC StreamWriter/StreamReader 封装
│
└── warp-fusion/               # 新工程：关联计算引擎
    ├── Cargo.toml             # workspace 根
    ├── src/                   # 主二进制
    ├── crates/                # 内部 crate
    ├── config/                # 配置示例
    ├── tests/                 # 集成测试
    └── docs/                  # 工程文档
```

### 3.2 wf-arrow（共享 crate）

独立仓库，wp-motor 和 warp-fusion 均通过 path/git 依赖引用。

```
wf-arrow/
├── Cargo.toml
│   # 依赖: arrow, parquet, wp-model-core
└── src/
    ├── lib.rs
    ├── schema.rs              # 类型映射
    │   └── to_arrow_type(DataType) → arrow::DataType
    │   └── to_arrow_schema(Vec<FieldDef>) → Schema
    ├── convert.rs             # 行转列
    │   └── records_to_batch(Vec<DataRecord>, Schema) → RecordBatch
    │   └── batch_to_records(RecordBatch) → Vec<DataRecord>
    └── ipc.rs                 # IPC 编解码
        └── encode_ipc(tag, RecordBatch) → Bytes
        └── decode_ipc(Bytes) → IpcFrame { tag, RecordBatch }
```

**类型映射表：**

| wp-model-core DataType | Arrow DataType | 说明 |
|------------------------|----------------|------|
| Chars | Utf8 | |
| Digit | Int64 | |
| Float | Float64 | |
| Bool | Boolean | |
| Time | Timestamp(Nanosecond, None) | NaiveDateTime → i64 纳秒 |
| IP | Utf8 | IpAddr → 字符串（便于 SQL 操作） |
| Hex | Utf8 | |
| Array(T) | List(T) | 递归映射内部类型 |

### 3.3 warp-fusion（主工程）

```
warp-fusion/
├── Cargo.toml                      # workspace 根
├── src/
│   ├── main.rs                     # 入口
│   ├── lib.rs                      # 库根
│   ├── cli.rs                      # clap CLI 参数定义
│   ├── types.rs                    # 全局类型别名
│   │
│   ├── runtime/                    # 运行时
│   │   ├── mod.rs
│   │   ├── actor/                  # actor 基础设施（复用 wp-motor 模式）
│   │   │   ├── mod.rs
│   │   │   ├── group.rs            # TaskGroup: JoinHandle 管理
│   │   │   ├── channel.rs          # 有界 mpsc channel 封装
│   │   │   └── command.rs          # 控制命令: Start/Stop/Reload/Drain
│   │   ├── receiver.rs             # 接收 task: 监听 Socket，解码 Arrow IPC
│   │   ├── scheduler.rs            # 调度 task: 事件驱动分发 + 超时扫描
│   │   └── lifecycle.rs            # 启停管理: 信号处理、优雅关闭
│   │
│   └── facade/                     # 外部接口
│       ├── mod.rs
│       └── health.rs               # 健康检查（可选 HTTP 端点）
│
├── crates/
│   ├── wf-core/                    # 核心引擎
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── window/             # Window 订阅者模型
│   │       │   ├── mod.rs
│   │       │   ├── types.rs        # WindowSchema, WindowRtConfig, DistMode, Window
│   │       │   ├── buffer.rs       # Window 时间窗口缓冲实现
│   │       │   ├── registry.rs     # WindowRegistry: 订阅表 + 路由推导
│   │       │   ├── router.rs       # Router: 根据订阅声明分发数据
│   │       │   └── evictor.rs      # 过期淘汰
│   │       ├── rule/               # 关联规则
│   │       │   ├── mod.rs
│   │       │   ├── executor.rs     # RuleExecutor: 状态机驱动 + DataFusion join
│   │       │   ├── match_engine.rs  # MatchEngine: CEP 状态机驱动
│   │       │   └── timeout.rs     # 超时管理: on close 求值 / maxspan 过期清理
│   │       └── alert/              # 风险告警输出
│   │           ├── mod.rs
│   │           ├── types.rs        # AlertRecord（score/entity）定义
│   │           └── sink.rs         # AlertSink trait + File/HTTP/Syslog 实现
│   │
│   └── wf-config/                  # 配置管理
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── fusion.rs           # FusionConfig 主配置（加载 .toml）
│           ├── window_rt.rs        # WindowRtConfig: 运行时物理参数（mode, max_bytes, over_cap）
│           └── limits.rs           # 常量: channel 容量、默认窗口大小
│
├── config/                         # 配置示例
│   ├── wfusion.toml                 # 主配置文件（运行时物理参数）
│   ├── schemas/
│   │   └── security.wfs             # Window Schema 文件
│   └── rules/
│       ├── brute_scan.wfl          # 安全关联规则（WFL）
│       └── traffic.wfl             # 流量分析规则（WFL）
│
└── tests/                          # 集成测试
    ├── e2e_basic.rs                # 端到端: 发送数据 -> 触发规则 -> 验证风险告警
    └── window_test.rs              # 窗口缓冲与淘汰测试
```


## 4. 核心组件设计

### 4.1 Window — 带订阅声明的时间窗口

Window 是 WarpFusion 的核心抽象，兼具数据订阅声明和时间窗口缓冲两个职责。

```rust
/// Window 逻辑定义（来自 .wfs 文件）
pub struct WindowSchema {
    pub name: String,                     // Window 名称
    pub streams: Vec<String>,             // 订阅的 stream name（支持多个）
    pub time_field: String,               // 事件时间字段
    pub over: Duration,                   // 窗口保持时长（需求侧）
    pub fields: Vec<FieldDef>,            // 字段 schema
}

/// Window 运行时配置（来自 .toml 文件）
pub struct WindowRtConfig {
    pub mode: DistMode,                   // 分布模式
    pub max_window_bytes: usize,          // 单窗口内存上限（字节）
    pub over_cap: Duration,               // over 能力上限（over ≤ over_cap）
    pub watermark: Duration,              // Watermark 延迟（默认 5s）
    pub allowed_lateness: Duration,       // 允许迟到时长（默认 0）
    pub late_policy: LatePolicy,          // 迟到数据处理策略
}

/// 迟到数据处理策略
pub enum LatePolicy {
    /// 丢弃迟到数据，不进入窗口（默认；最简单，适合大多数场景）
    Drop,
    /// 接受迟到数据并修正窗口（可能导致已触发的规则重新计算）
    Revise,
    /// 写入旁路输出（late_sink），不影响主窗口
    SideOutput { sink: String },
}

/// 分布模式
pub enum DistMode {
    /// 按 key hash 分区，同 key 的数据保证在同一节点
    Partitioned { key: String },
    /// 全局复制，每个节点持有完整副本
    Replicated,
    /// 仅本机，不参与分布式路由
    Local,
}

/// Window 运行时实例：持有实际数据
pub struct Window {
    ws: WindowSchema,                     // 逻辑定义（from .wfs）
    rt: WindowRtConfig,                   // 运行时配置（from .toml）
    schema: SchemaRef,                    // Arrow Schema（从 ws.fields 映射）
    batches: VecDeque<TimedBatch>,        // 带时间戳的批数据队列
    current_bytes: usize,                 // 当前已用字节数
    total_rows: usize,                    // 当前总行数（监控用）
}

struct TimedBatch {
    batch: RecordBatch,
    event_time_range: (i64, i64),         // 该批数据的事件时间范围
    ingested_at: Instant,                 // 接收时间
    row_count: usize,
    byte_size: usize,                     // 该 batch 的内存占用（arrow get_array_memory_size）
}

impl Window {
    /// 追加数据；若追加后超出 max_window_bytes 则先淘汰最早的 batch
    pub fn append(&mut self, batch: RecordBatch);
    /// 获取当前窗口内的只读快照（不阻塞写入）
    pub fn snapshot(&self) -> Vec<RecordBatch>;
    /// 淘汰过期数据
    pub fn evict_expired(&mut self, now: i64);
    /// 返回当前窗口的内存占用（字节）
    pub fn memory_usage(&self) -> usize;
}
```

**事件时间模型：**

Window 基于 **事件时间**（event time）而非处理时间（processing time）管理数据生命周期。核心机制：

| 概念 | 含义 | 默认值 |
|------|------|--------|
| **Watermark** | 当前窗口认定"不会再收到比此时间更早的数据"的时间点。计算方式：`max(event_time) - watermark_delay` | 5s |
| **Allowed Lateness** | Watermark 之后仍允许接收的迟到时间余量。在 `watermark..watermark + allowed_lateness` 区间内到达的数据仍可进入窗口 | 0（不接受） |
| **Late Policy** | 超过 allowed_lateness 的数据如何处理 | Drop |

```
时间轴：
  ──────────┬───────────────┬──────────────┬──────────→
            │               │              │
       watermark      watermark +      当前最新
                    allowed_lateness   event_time
            │               │
   此前数据视为迟到    │
            │         此区间的迟到数据
            │         按 late_policy 处理
```

**Late Policy 选择指南：**

| 策略 | 行为 | 适用场景 | 代价 |
|------|------|---------|------|
| `Drop` | 直接丢弃，不入窗口 | 数据源时间基本有序（绝大多数场景） | 无 |
| `Revise` | 追加到窗口，可能触发规则重算 | 数据源乱序严重，且检测精度要求高 | 规则可能重复触发，需下游去重 |
| `SideOutput` | 写入旁路 sink（文件/队列），主窗口不受影响 | 需要保留迟到数据用于事后分析或审计 | 额外 I/O |

**窗口策略：**

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| Tumbling(duration) | 固定大小不重叠窗口 | 定时聚合报表 |
| Sliding(size, slide) | 滑动窗口，可重叠 | 实时异常检测 |

**一个 Stream 可被多个 Window 订阅**——例如 `firewall` 流可以同时被 `fw_by_sip`（partitioned by sip）和 `fw_global_stats`（replicated）订阅，Router 按各自的 mode 分别路由。

### 4.2 WindowRegistry — 窗口注册与路由

维护所有 Window 实例，并根据 Window 的订阅声明推导路由表。

```rust
pub struct WindowRegistry {
    /// Window 名称 → 运行时实例
    windows: HashMap<String, Arc<RwLock<Window>>>,
    /// Stream name → 订阅该 stream 的 Window 列表（路由表）
    subscriptions: HashMap<String, Vec<Subscription>>,
}

struct Subscription {
    window_name: String,
    mode: DistMode,
}
```

#### 4.2.1 数据流全景

```
                         ┌─────────────────────────────────┐
                         │         BOOT 阶段                │
                         │   .wfs + wfusion.toml → 订阅表    │
                         └───────────────┬─────────────────┘
                                         │
                                         ▼
 ┌───────────┐    TCP 帧     ┌───────────────────┐
 │ wp-motor  │──────────────▶│     Receiver       │
 │ (sender)  │ [len][name]   │ decode_ipc(payload)│
 └───────────┘ [RecordBatch] └─────────┬─────────┘
                                       │
                          frame.tag ──▶ stream_name
                          frame.batch ─▶ RecordBatch
                                       │
                          ┌────────────┴────────────┐
                          │                         │
                          ▼                         ▼
                 ┌─────────────────┐     ┌──────────────────────┐
                 │ Router.route()  │     │ event_tx.send()      │
                 │ → Window 写入   │     │ → Scheduler 消费     │
                 └────────┬────────┘     └──────────┬───────────┘
                          │                         │
                          ▼                         ▼
                 ┌─────────────────┐     ┌──────────────────────┐
                 │ Window Buffer   │     │ CEP StateMachine     │
                 │ (时间窗口存储)   │     │ (规则状态推进)        │
                 └─────────────────┘     └──────────────────────┘
```

Router 和 Scheduler 并行消费同一帧数据：Router 负责将 batch 写入 Window 缓冲区供 join/snapshot 使用；Scheduler 负责驱动 CEP 状态机进行规则匹配。

#### 4.2.2 启动阶段：订阅表构建

```
.wfs 定义:                             wfusion.toml 配置:
┌──────────────────────┐              ┌─────────────────────┐
│ window auth_events { │              │ [window.auth_events] │
│   stream = "syslog"  │              │ mode = "local"       │
│   time = event_time  │              │ over_cap = "30m"     │
│   over = 5m          │              └──────────┬──────────┘
│   fields { ... }     │                         │
└──────────┬───────────┘                         │
           │                                     │
           ▼                                     ▼
┌────────────────────────────────────────────────────────┐
│            schema_bridge::schema_to_window_def          │
│  WindowSchema + WindowConfig → WindowDef {              │
│    params:  { name: "auth_events", schema, over, ... }  │
│    streams: ["syslog"],         ← 来自 .wfs             │
│    config:  { mode: Local, ... } ← 来自 wfusion.toml     │
│  }                                                      │
└────────────────────────┬───────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────┐
│            WindowRegistry::build(defs)                  │
│                                                         │
│  for def in defs:                                       │
│    windows["auth_events"] = Window::new(...)             │
│    for stream_name in def.streams:                      │
│      subscriptions["syslog"]                            │
│        .push(Subscription {                             │
│          window_name: "auth_events",                    │
│          mode: Local,                                   │
│        })                                               │
│                                                         │
│  生成的订阅表 (HashMap<String, Vec<Subscription>>):      │
│  ┌────────────────────────────────────────────────┐     │
│  │ "syslog"  → [ auth_events(Local) ]             │     │
│  │ "netflow" → [ fw_events(Local),                │     │
│  │              ip_stats(Replicated) ]             │     │
│  └────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────┘
```

多对多关系：

```
stream_name         subscriptions            window
───────────         ─────────────            ──────
                 ┌→ auth_events (Local)
"syslog"    ─────┤                            扇出: 1 stream → N windows
                 └→ all_logs    (Local)

"syslog"    ─────┐
                 ├→ all_logs    (Local)       聚合: N streams → 1 window
"winlog"    ─────┘
```

#### 4.2.3 运行阶段：Router 路由逻辑

```
Router.route(stream_name, batch):
│
├─ ① 查订阅表
│    subs = registry.subscribers_of(stream_name)
│    无订阅者 → 静默丢弃（RouteReport 全零）
│
├─ ② 遍历订阅者
│    for (window_name, mode) in subs:
│    │
│    ├─ mode != Local
│    │   → skipped_non_local += 1（Replicated/Partitioned 暂不处理）
│    │
│    └─ mode == Local
│        → window.append_with_watermark(batch)
│          │
│          ├─ ③ 提取时间范围
│          │    (min_ts, max_ts) = extract_time_range(batch)
│          │
│          ├─ ④ 推进 Watermark
│          │    watermark = max(watermark, max_ts - delay)
│          │
│          └─ ⑤ 迟到检查
│               cutoff = watermark - allowed_lateness
│               min_ts < cutoff ?
│               ├─ YES → late_policy:
│               │   ├─ Drop       → DroppedLate（丢弃）
│               │   └─ Revise     → 仍写入窗口
│               └─ NO  → append(batch) → Appended ✓
│
└─ 返回 RouteReport { delivered, dropped_late, skipped_non_local }
```

#### 4.2.4 运行阶段：Scheduler 分发逻辑

Scheduler 消费 `(stream_name, RecordBatch)` 元组，通过预计算的 `stream_aliases` 映射将事件分发到对应的 CEP 状态机。

```
启动时预计算 (build_stream_aliases):
  WFL 规则: events { fail : auth_events && action == "failed" }
  .wfs 定义: window auth_events { stream = "syslog" }
  → stream_aliases["syslog"] = ["fail"]

运行时分发:
Scheduler.dispatch_batch(stream_name, batch):
│
├─ events = batch_to_events(batch)
│  空 → 返回
│
└─ for engine in engines:       // 并行, 受 exec_semaphore 限制
     aliases = engine.stream_aliases.get(stream_name)
     ├─ None → skip（此规则不关心该 stream）
     └─ Some(["fail"]) →
          for event in events:
            for alias in aliases:
              machine.advance(alias, event)
              └─ Matched(ctx) → executor.execute_match(ctx)
                                  → alert_tx.send(alert)
```

> **注意**：行级过滤（`events { alias : window && filter }` 中的 `&&` 条件）不在路由层执行——路由层负责将整个 RecordBatch 分发到 Window，过滤在 RuleExecutor 的事件匹配阶段执行。这保证了同一 Window 被多条规则以不同过滤条件引用时，数据只存储一份。

**Router 分布式路由（收到数据时）：**

```
收到 (stream_name, RecordBatch):
  for sub in subscriptions[stream_name]:
    match sub.mode:
      Local | 单机模式  → 直接 window.append(batch)
      Replicated        → broadcast batch → 所有节点

      Partitioned(key)  → 行级分桶路由:
        // ① 取出 key 列，逐行计算 hash
        buckets: HashMap<NodeId, Vec<row_index>> = {}
        for row in 0..batch.num_rows():
          node = hash(batch.column(key)[row]) % N
          buckets[node].push(row)
        // ② 按目标节点组装子 batch
        for (node, indices) in buckets:
          sub_batch = batch.take(indices)   // Arrow take 内核，gather 拷贝
          send(node, sub_batch)
```

> **设计说明**：`Partitioned` 不能按整个 RecordBatch 做 hash——同一 batch 可能包含多个不同 key 值的行。必须逐行提取 key、分桶，再按目标节点重组子 batch 发送。Arrow 的 `take` 内核按索引 gather 行数据，**会产生内存拷贝**（非零拷贝），单机模式下不涉及此路径。分布式模式下需关注其 CPU/内存开销，优化手段：
>
> - **预排序优化**：若上游 batch 已按 key 排序（WarpParse Sink 侧可选排序），则同 key 行连续，可用 `slice()` 做零拷贝切片代替 `take()` gather
> - **batch 尺寸控制**：控制单个 batch 的行数（如 ≤4096 行），限制单次 gather 的开销
> - **单机跳过**：单机模式下所有 Window 实质为 `local`，Router 直接 append 原始 batch，不触发分桶逻辑

### 4.3 RuleExecutor — 规则执行器

每条规则由 `wf-lang` 编译为 `RulePlan(Core IR)`。事件到达时推进状态机，在 `on event`/`on close` 条件满足后按 `JoinPlan(snapshot/asof)` 执行关联、计算 `score`，并按 `entity(type,id_expr)` 产出风险告警写入目标 window。

```rust
/// 编译后的规则执行计划（由 wf-lang 编译器生成）
/// 所有 WFL 规则统一编译为 CEP 状态机
pub struct RulePlan {
    pub name: String,
    pub lang: String,                      // v2.1
    pub contract_version: u32,             // 对应 yield@vN
    pub windows: Vec<String>,             // 引用的 Window 名称列表
    pub state_machine: CepStateMachine,    // 含 on event / on close
    pub joins: Vec<JoinPlan>,              // join(snapshot|asof)
    pub limits_plan: LimitsPlan,           // 规则资源预算
    pub score_plan: ScorePlan,             // score(expr) 或 score { ... }
    pub entity_plan: EntityPlan,           // entity(type, id_expr)
    pub yield_plan: YieldPlan,             // yield target@vN(...)
}

pub struct RuleExecutor {
    plan: RulePlan,
    ctx_template: SessionConfig,           // DataFusion 配置模板（用于 JoinExecutor）
}

impl RuleExecutor {
    /// 状态机驱动：事件到达时推进 on event/on close
    pub fn on_event(&self, event: &DataRecord) -> StepResult {
        self.plan.state_machine.advance(event)
    }

    /// 全部步骤完成后，按 JoinPlan 执行关联并产出告警
    pub async fn execute_join_plan(
        &self,
        registry: &WindowRegistry,
        matched: &MatchedContext,
    ) -> Result<AlertRecord> {
        let ctx = SessionContext::new_with_config(self.ctx_template.clone());
        let joined = JoinExecutor::run(&ctx, registry, matched, &self.plan.joins).await?;
        let scored = ScoreEvaluator::eval(&self.plan.score_plan, &joined)?;
        AlertBuilder::build(&self.plan, joined, scored)
    }
}
```

**关键设计决策：**

- WFL 规则由 `wf-lang` 编译器统一编译为 `RulePlan`（CEP 状态机），RuleExecutor 以事件驱动方式执行
- Rule 引用 Window 名称（定义在 .wfs 中），而非 Stream 名称——同一个 Stream 可被不同 Window 以不同方式订阅
- Join 执行使用 `JoinPlan`（`snapshot/asof`）而不是“规则内 SQL 字符串”
- 每次 Join 执行创建新的 `SessionContext`，避免状态污染
- 窗口快照是只读的（`snapshot()` 返回 `Vec<RecordBatch>` 的 clone/Arc），不阻塞写入
- 多条规则可并行执行（各自独立的 SessionContext）
- **空窗口安全**：Window 无数据时注册 `RecordBatch::new_empty(schema)` 为临时表，Join 正常执行返回零行，不会因 "table not found" 报错

### 4.4 Scheduler — 规则调度器

```rust
pub struct Scheduler {
    rules: Vec<ManagedRule>,
    exec_semaphore: Arc<Semaphore>,       // 全局并发上限
}

struct ManagedRule {
    executor: RuleExecutor,
    alert_sink: Arc<dyn AlertSink>,
    exec_timeout: Duration,               // 单次 join/score/yield 执行超时
}
```

**调度循环（事件驱动）：**

```
loop {
    tokio::select! {
        // 事件到达：从 Window 接收新事件
        event = event_rx.recv() => {
            for rule in &rules {
                // 只分发到引用该 window 的规则
                if !rule.executor.accepts(&event.window) { continue }

                // 全局并发上限（背压）
                let permit = exec_semaphore.clone().try_acquire_owned();
                let Ok(permit) = permit else {
                    tracing::warn!("executor concurrency limit reached");
                    continue;
                };

                let result = rule.executor.on_event(&event);
                match result {
                    StepResult::Accumulate => {} // 继续累积，等待下一个事件
                    StepResult::Advance => {}    // 步骤条件满足，推进到下一步
                    StepResult::Matched(ctx) => {
                        // 全部步骤完成 -> join(snapshot/asof) -> score/entity -> yield -> alert
                        let _permit = permit;
                        match timeout(rule.exec_timeout, rule.executor.execute_join_plan(&registry, &ctx)).await {
                            Ok(Ok(alert)) => rule.alert_sink.emit(&alert).await,
                            Ok(Err(e)) => tracing::error!("join error: {e}"),
                            Err(_) => tracing::error!("join timeout"),
                        }
                    }
                }
            }
        }
        // 超时检查：定期扫描过期的状态机实例并执行 on close 求值
        _ = timeout_interval.tick() => {
            for rule in &rules {
                let expired = rule.executor.check_timeouts(now);
                for ctx in expired {
                    // on close：窗口关闭（timeout/flush/eos）时求值
                    // maxspan 过期：重置状态机实例
                    rule.executor.handle_timeout(ctx);
                }
            }
        }
        // 控制命令
        cmd = cmd_rx.recv() => {
            match cmd {
                Reload => reload_rules(),
                Stop   => break,
            }
        }
    }
}
```

**调度保护机制：**

| 机制 | 说明 |
|------|------|
| 事件驱动 | 事件到达时分发到相关规则，推进状态机；非定时轮询 |
| 超时扫描 | 定期检查 `on close` 条件和 maxspan 超期的状态机实例 |
| 全局并发上限 | `Semaphore(executor_parallelism)`，防止大量规则同时执行耗尽 CPU |
| 执行超时 | `tokio::time::timeout` 包裹 join/score/yield 主路径，超时自动取消 |

### 4.5 AlertSink — 风险告警输出

```rust
pub struct AlertRecord {
    pub alert_id: String,                      // 幂等键：hash(rule_name + scope_key + window_range)
    pub rule_name: String,
    pub score: f64,                            // 统一风险分数 [0,100]
    pub entity_type: String,                   // 由 entity(type, id_expr) 注入
    pub entity_id: String,
    pub close_reason: Option<String>,          // timeout | flush | eos | null
    pub score_contrib: Option<JsonValue>,      // score { ... } 时输出
    pub fired_at: DateTime<Utc>,
    pub matched_rows: Vec<RecordBatch>,        // 命中的事件数据
    pub summary: String,                       // 规则执行摘要
}

#[async_trait]
pub trait AlertSink: Send + Sync {
    async fn emit(&self, alert: &AlertRecord) -> Result<()>;
}
```

**风险告警去重（幂等保证）：**

无论 `best_effort` 还是 `at_least_once`，都可能出现重复风险告警（上游重复发送、重放、多实例并发输出）。通过 `alert_id` 实现幂等：

```
alert_id = sha256(rule_name + scope_key_value + window_start + window_end)
```

| 组件 | 职责 |
|------|------|
| `alert_id` 生成 | RuleExecutor 在产出风险告警时计算，相同规则 + 相同关联 key + 相同窗口区间 -> 相同 ID |
| 本地去重缓存 | AlertSink 维护最近 N 分钟（可配置，默认 `2 × max(rule.maxspan)`）的 `alert_id` 集合，重复 ID 直接跳过 |
| 下游幂等 | `alert_id` 随风险告警一起输出，下游系统可据此做最终去重（防止 WarpFusion 多实例场景的分布式重复） |

内置实现：
- `FileAlertSink` — JSON Lines 写入文件（含 `alert_id` 字段）
- `HttpAlertSink` — POST 到 HTTP 端点（`alert_id` 作为幂等键 header）
- `SyslogAlertSink` — 发送到 Syslog（`alert_id` 写入 structured data）


## 5. 并发模型

### 5.1 复用 wp-motor 模式

| 模式 | wp-motor 实现 | WarpFusion 复用方式 |
|------|--------------|-------------------|
| TaskGroup | `runtime/actor/group.rs` | 直接复刻，管理 Receiver/Scheduler/Evictor |
| 有界 channel | `tokio::mpsc` + 容量常量 | 相同，Receiver→Router、Scheduler→Executor |
| broadcast 控制 | `async_broadcast` | 相同，用于 Stop/Reload 全局广播 |
| 优雅关闭 | Drain + Timeout | 相同，先停 Receiver，等规则执行完毕 |
| 对象池 | `SinkRecUnitPool` | 适配为 RecordBatch 缓冲池 |

### 5.2 WarpFusion 任务拓扑

```
TaskGroup: receiver
  └─ Receiver task (×1)
       ↓ mpsc channel (容量 256)
       ↓ (tag, RecordBatch)
TaskGroup: router
  └─ Router task (×1)
       ↓ 根据 WindowRegistry 订阅表分发到各 Window
       ↓ (事件到达时通知 Scheduler)

TaskGroup: scheduler
  └─ Scheduler task (×1)
       ↓ tokio::spawn 执行规则
       ↓ mpsc channel (容量 64)

TaskGroup: executor
  └─ RuleExecutor tasks (×N, 可配并行度)
       ↓ 执行结果
       ↓ mpsc channel (容量 64)

TaskGroup: alert
  └─ AlertSink task (×1)
       ↓ 写文件 / HTTP / Syslog

TaskGroup: maintenance
  └─ Evictor task (×1，定时清理过期窗口数据)
  └─ Monitor task (×1，统计指标输出)
```

**启动顺序（复用 wp-motor 原则：先消费者后生产者）：**

1. AlertSink 启动（持有接收端）
2. Executor 启动（持有接收端）
3. Scheduler 启动
4. Evictor / Monitor 启动
5. Router 启动（持有接收端）
6. Receiver 最后启动（开始接收数据）


## 6. 配置设计

### 6.1 主配置 wfusion.toml

三文件架构下，TOML 仅负责**运行时物理参数**。数据 schema 在 `.wfs` 文件中定义，检测逻辑在 `.wfl` 文件中定义。

```toml
[server]
listen = "tcp://127.0.0.1:9800"                   # 监听地址（多个 wp-motor + 分布式节点共用同一端口）

[runtime]
executor_parallelism = 2                       # 规则执行并行度（Semaphore 上限）
rule_exec_timeout = "30s"                      # 单条规则执行超时

# 文件引用
window_schemas = ["security.wfs"]               # Window Schema 文件列表
rule_packs     = ["pack/security.yaml"]         # RulePack 入口（推荐）
wfl_rules      = ["brute_scan.wfl", "traffic.wfl"]  # 兼容模式（conformance=compat）

[language]
version = "wfl-2.1"
conformance = "strict"                          # strict | compat

[window_defaults]
evict_interval = "30s"                         # 淘汰检查间隔
max_window_bytes = "256MB"                     # 单窗口内存上限（默认）
max_total_bytes = "2GB"                        # 全局窗口内存上限
evict_policy = "time_first"                    # 淘汰策略: time_first | lru
watermark = "5s"                               # 默认 watermark 延迟
allowed_lateness = "0s"                        # 默认不接受迟到数据
late_policy = "drop"                           # drop | revise | side_output

# 每个 window 的物理参数（覆盖默认值）
[window.auth_events]
mode = "local"                                 # 分布模式: local | replicated | partitioned
max_window_bytes = "256MB"
over_cap = "30m"                               # over 能力上限（.wfs 中 over ≤ over_cap）

[window.fw_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
watermark = "10s"                              # 防火墙日志乱序较多，放宽 watermark
allowed_lateness = "30s"
late_policy = "drop"

[window.ip_blocklist]
mode = "replicated"                            # 全局复制（分布式下每节点有全量）
max_window_bytes = "64MB"
over_cap = "48h"

[transport]
reliability = "best_effort"                    # best_effort | at_least_once | exactly_once

[transport.replay]
wal_dir = "/var/lib/warpfusion/transport-wal"  # reliability != best_effort 时启用
ack_timeout = "5s"
max_inflight = 10000

[alert]
sinks = [
    "file:///var/log/wf-alerts.jsonl",
    # "http://alert-api:8080/v1/alerts",
]
```

**配置分层原则：**

| 层级 | 文件 | 内容 | 示例 |
|------|------|------|------|
| 数据定义 | `.wfs` | stream 来源、time 字段、over 时长、字段 schema | `over = 5m` |
| 检测逻辑 | `.wfl` | 事件绑定、模式匹配、条件、输出、`limits` | `yield risk_scores@v2 (...)` |
| 规则入口 | `pack.yaml` | `language/features/rules/runtime` 统一入口 | `conformance: strict` |
| 物理约束 | `.toml` | mode、max_bytes、over_cap、watermark、reliability | `reliability = "best_effort"` |

**over vs over_cap 校验：** 启动时检查每个 window 的 `.wfs` 中 `over` ≤ `.toml` 中 `over_cap`，不满足则报错拒绝启动。

### 6.2 关联规则

关联检测规则使用 WFL 语言编写，存储在 `.wfl` 文件中。完整语法和语义模型见 [WFL v2.1 设计方案](wfl-desion.md)，与主流 DSL 的对比分析见 [WFL DSL 对比](wfl-dsl-comparison.md)。

v2.1 实现中，规则发布默认使用 `conformance=strict`，并强制校验：
- `meta.lang = "2.1"`
- `meta.contract_version`
- `join snapshot/asof`
- `limits { ... }`
- `yield target@vN (...)`

WFL 规则中的数据源名称引用 **Window Schema (.wfs) 中定义的 window 名称**，不直接引用 stream tag。这使得同一个 stream 可以以不同方式（不同 mode、不同 over）被多个 window 引用，规则按需选择合适的 window。

**规则文件示例（brute_scan.wfl）：**

```wfl
use "security.wfs"

rule brute_force_then_scan {
    meta {
        lang        = "2.1"
        contract_version = "2"
        description = "Login failures followed by port scan from same IP"
        mitre       = "T1110, T1046"
    }

    features [l1, l2]

    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }

    match<:5m> {
        key {
            src = fail.sip;
        }
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)

    join ip_blocklist snapshot on fail.sip == ip_blocklist.ip

    entity(ip, fail.sip)

    yield security_alerts@v2 (
        sip        = fail.sip,
        fail_count = count(fail),
        port_count = distinct(scan.dport),
        message    = fmt("{}: brute force then port scan detected", fail.sip)
    )

    limits {
        max_state = "512MB";
        max_cardinality = 200000;
        max_emit_rate = "1000/m";
        on_exceed = "throttle";
    }
}
```


## 7. 依赖清单

### 7.1 wf-arrow

```toml
[dependencies]
wp-model-core = "0.8"
arrow = { version = "54", features = ["ipc"] }
parquet = { version = "54", optional = true }
bytes = "1.10"
```

### 7.2 wf-core

```toml
[dependencies]
wf-arrow = { path = "../../wf-arrow" }
datafusion = "45"
arrow = { version = "54" }
tokio = { version = "1.48", features = ["sync", "time"] }
tracing = "0.1"
anyhow = "1.0"
chrono = "0.4"
```

### 7.3 wf-config

```toml
[dependencies]
serde = { version = "1.0", features = ["derive"] }
toml = "0.9"
anyhow = "1.0"
```

### 7.4 warp-fusion（主二进制）

```toml
[dependencies]
wf-core = { path = "crates/wf-core" }
wf-arrow = { path = "../wf-arrow" }
wf-config = { path = "crates/wf-config" }
tokio = { version = "1.48", features = ["full"] }
clap = { version = "4.5", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = "0.3"
anyhow = "1.0"
async-broadcast = "0.7"
```


## 8. wp-motor 侧改动

wp-motor 需要新增一个 Arrow IPC Sink，将解析后的 DataRecord 通过 TCP 单向推送给 WarpFusion。

### 8.1 新增依赖

```toml
# wp-motor/Cargo.toml
wf-arrow = { path = "../wf-arrow" }
```

### 8.2 Sink 实现

位置：`wp-motor/src/sinks/backends/arrow_ipc.rs`

#### 核心结构

```rust
/// 连接状态机
enum ConnState {
    Connected { writer: OwnedWriteHalf },     // 活跃连接
    Disconnected { next_attempt: Instant, backoff: Duration },  // 断连退避中
    Stopped,                                  // 终态
}

pub struct ArrowIpcSink {
    conn: ConnState,
    target: SocketAddr,               // TCP 地址，用于重连
    tag: String,
    field_defs: Vec<FieldDef>,
}
```

#### send_batch() 流程

```
1. records → records_to_batch → encode_ipc → payload
2. 组帧：[4B BE len][stream_name][payload]
3. if Connected → try write, error → enter_disconnected()
   if Disconnected → if now >= next_attempt → try_reconnect()
4. return Ok(())
```

默认（`best_effort`）下：**写入失败时数据丢弃**，不缓冲。  
当启用 `at_least_once/exactly_once` 时：进入本地 WAL 并等待 ACK/重放，不走“直接丢弃”路径。

#### 重连状态机

```
[Connected] ──write error──→ [Disconnected]
     ↑                            │
     │   reconnect success        │ backoff 1s→2s→4s→…→30s (cap)
     └───────────────────────────←┘
                                   │
                      stop() → [Stopped]
```

- **`try_reconnect()`**：`TcpStream::connect(target)` → 进入 `Connected`
- **`reconnect()`**（trait 方法）：重置 backoff，立即尝试

### 8.3 配置示例

```toml
# wp-motor sink 配置
[[sink]]
name = "to-fusion"
type = "arrow-ipc"
target = "tcp://127.0.0.1:9800"
tag = "firewall"
reliability = "best_effort"                       # best_effort | at_least_once | exactly_once
retry_interval = "1s"                             # 初始重试间隔（指数退避）
retry_max_interval = "30s"                        # 最大重试间隔
```


## 9. 开发路线

| 阶段 | 内容 | 交付物 | 依赖 |
|------|------|--------|------|
| **P0** | wf-arrow: schema 映射 + 行列转换 + IPC 编解码 | wf-arrow crate 可用 | 无 |
| **P1** | wp-motor: 新增 Arrow IPC Sink | WarpParse 可输出 Arrow IPC | P0 |
| **P1** | wf-config: RulePack + conformance + reliability 配置解析 | wfusion.toml/pack.yaml 可加载 | 无 |
| **P2** | wf-core/window: Window + WindowRegistry + Router | 能接收多流、按订阅声明路由并缓存 | P0, P1-config |
| **P3** | wf-core/rule: Loader + Executor(Core IR) | 支持 `join snapshot/asof` + `yield@vN` | P2 |
| **P3** | runtime: Receiver + Scheduler + Lifecycle | 主进程可运行 | P2, P3-rule |
| **P4** | wf-core/alert: AlertSink 实现 + score_contrib 透传 | 完整单机闭环 | P3 |
| **P5** | 可证明正确性门禁：contract + shuffle + datagen verify | 发布前一致性可验证 | P4 |
| **P6** | 可靠性分级：best_effort/at_least_once/exactly_once | 按场景切换可靠性档位 | P4 |
| **P7** | 分布式 V3: 两阶段聚合汇总 | 全局 GROUP BY | P6 |

**P0 和 P1 可并行**——wf-arrow 完成后，wp-motor 侧的 Sink 和 warp-fusion 侧的 Receiver 可同时开发。


## 9.1 执行计划

详细的 30 里程碑执行计划已独立为专属文档，详见 → [wf-execution-plan.md](wf-execution-plan.md)

计划将引擎基建（P0–P7）与 WFL 语言实现（Phase A–D）统一拆分为 **M01–M30**，分属十个阶段：

| 阶段 | 里程碑 | 阶段目标 |
|------|--------|---------|
| **I 数据基建** | M01–M05 | wp-motor 能通过 Arrow IPC 推送数据，WarpFusion 可接收路由 |
| **II 配置与窗口** | M06–M10 | 配置可加载、Window 能接收路由并缓存数据 |
| **III WFL 编译器** | M11–M13 | .wfs + .wfl 编译为 RulePlan |
| **IV 执行引擎** | M14–M16 | CEP 状态机 + DataFusion join 可执行 |
| **V 运行时闭环** | M17–M20 | **单机 MVP：数据接收 -> 规则执行 -> 风险告警输出** |
| **VI 生产化** | M21–M24 | 热加载、多通道风险告警、监控、工具链、CostPlan |
| **VII L2 增强** | M25–M26 | join / baseline / 条件表达式 / 函数 |
| **VIII 正确性门禁** | M27–M28 | contract + shuffle + scenario verify |
| **IX 可靠性分级** | M29 | best_effort/at_least_once/exactly_once |
| **X 分布式** | M30 | 多节点分布式部署 |


## 10. 风险与约束

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| DataFusion 每次建 SessionContext 的开销 | 规则频繁执行时延迟增加 | 复用 SessionContext + 仅替换表数据；benchmark 验证 |
| 窗口数据量超内存 | OOM | max_window_bytes + max_total_bytes 硬限 + evict_policy 自动淘汰 + 监控告警 |
| Arrow IPC 传输断连 | 断连期间数据丢失 | TCP 可靠传输 + 指数退避重连；`connection_drops` 监控告警（见 2.4 节） |
| 上游重复发送/多实例并发导致重复风险告警 | 风险告警风暴 | alert_id 幂等去重 + AlertSink 本地去重缓存 + 下游幂等键（见 4.5 节） |
| DataFusion 版本升级不兼容 | 编译失败 | workspace 锁定版本；跟进 DataFusion 发布周期 |
| 规则表达式/Join 模式写错导致高开销 | CPU 飙升 | 执行超时 + `wf lint --strict` + CostPlan 风险阻断 |
| replicated 窗口数据量过大 | 所有节点内存膨胀 | replicated 仅用于小表（字典/情报），配置层限制最大行数 |
| 分布式下 key 热点 | 单节点负载倾斜 | 监控各节点 Window 大小；极端情况拆分子 key |


## 11. WFL 语言设计

WFL 语言设计已独立为专属文档，详见：

- **WFL v2.1 设计方案** → [wfl-desion.md](wfl-desion.md)
  - 三文件架构（.wfs / .wfl / .toml）
  - v2.1 规则治理（`meta.lang` / `meta.contract_version` / `limits` / `yield@vN`）
  - 固定执行链与语义模型（BIND / SCOPE / JOIN / ENTITY / YIELD / CONV）
  - Join 时点语义一等化（`snapshot` / `asof`）
  - 单通道风险输出（score）+ 实体建模（entity）+ 贡献明细（score_contrib）
  - Window Schema (.wfs) 文法与语义约束
  - WFL (.wfl) EBNF 文法、类型系统与语义约束（含 on close / close_reason）
  - Conformance 门禁（contract / shuffle / scenario verify）
  - 行为分析扩展（session window、baseline、score、collect 函数）
  - 编译模型（源码 → AST → RulePlan → CEP 状态机）
  - 完整示例与开发分期

- **WFL 与主流 DSL 对比分析** → [wfl-dsl-comparison.md](wfl-dsl-comparison.md)
  - 与 YARA-L 2.0 / Elastic EQL / Sigma / Splunk SPL / KQL 的能力对比



## 附录 A: 分布式架构

### A.1 Window 分布模式如何解决分布式 JOIN

分布式实时关联的三大难题：窗口分区、分布式 JOIN、状态容错。Window 订阅者模型通过声明式分布模式，自然化解前两个问题。

**示例：三表 JOIN（2 个 partitioned + 1 个 replicated）**

```sql
SELECT a.sip, a.username, b.threat_level, f.distinct_ports
FROM auth_by_sip a
JOIN ip_blocklist b ON a.sip = b.ip        -- partitioned JOIN replicated
JOIN fw_by_sip f ON a.sip = f.sip          -- partitioned JOIN partitioned (同 key)
```

集群中每个节点的数据分布：

```
Node 0 (sip hash 0..N/2):
  auth_by_sip:  sip ∈ {1.1.1.1, 3.3.3.3, ...}   ← partitioned 子集
  fw_by_sip:    sip ∈ {1.1.1.1, 3.3.3.3, ...}   ← 同 key，保证共存
  ip_blocklist: 全量                               ← replicated

Node 1 (sip hash N/2..N):
  auth_by_sip:  sip ∈ {2.2.2.2, 4.4.4.4, ...}   ← partitioned 子集
  fw_by_sip:    sip ∈ {2.2.2.2, 4.4.4.4, ...}   ← 同 key，保证共存
  ip_blocklist: 全量                               ← replicated
```

**每个节点独立执行 SQL，零跨节点通信**。三个表在本地都有 Rule 所需的数据：
- 两个 `partitioned(sip)` 窗口按同 key 分区 → 同 sip 在同节点
- `replicated` 窗口每节点有全量 → JOIN 小表本地完成

### A.2 路由拓扑（分布式模式）

```
wp-motor A ──TCP──┐                  ┌─ Fusion Node 0
wp-motor B ──TCP──┤                  │  Window(partitioned:sip) 子集 0
wp-motor C ──TCP──┼──→ 同一监听端口 ─┤  Window(replicated) 全量
                  │   按 Window mode │
Fusion Node 1 ─TCP┤   路由          ├─ Fusion Node 1
Fusion Node 2 ─TCP┘                  │  Window(partitioned:sip) 子集 1
                                     │  Window(replicated) 全量
                                     │
                                     └─ Fusion Node 2
                                        Window(partitioned:sip) 子集 2
                                        Window(replicated) 全量
```

wp-motor 和其他 Fusion 节点均通过 TCP 连入同一监听端口。Receiver 不区分来源类型，统一按帧 payload 中的 `stream_name` 路由到目标 Window。

路由逻辑可在 WarpParse 的 Sink 侧或 WarpFusion 的 Receiver 侧完成。`partitioned` 模式下需按行级 key hash 分桶（见 4.2 节 Router 伪代码），不能按整个 batch 路由。

### A.3 分布式演进路线

不需要一步到位，按阶段演进：

| 阶段 | 架构 | 支持能力 | 复杂度 |
|------|------|---------|--------|
| **V1** | 单机 | 全部 SQL，所有 Window 实质为 local | 低 |
| **V2** | 按 key 分区，多实例 | 等值 JOIN + 本地聚合，每实例是独立的单机版 | 中 |
| **V3** | V2 + 聚合汇总节点 | V2 + 全局 GROUP BY（两阶段聚合） | 中+ |
| **V4** | 引入 exchange 层 | 非等值 JOIN / 多 key JOIN（需 shuffle） | 高 |
| **V5** | V4 + checkpoint | V4 + 节点故障恢复 | 很高 |

**V2 对安全关联场景已够用**——绝大多数规则是 `ON a.sip = b.sip` 等值 JOIN。V2 的本质是多个独立 WarpFusion 实例各管一部分 key，WarpFusion 代码本身无需改动，路由在 WarpParse Sink 侧完成。

### A.4 已知局限

| 局限 | 说明 | 缓解 |
|------|------|------|
| 非等值 JOIN | `ON a.bytes > b.threshold` 无法按 key 分区 | V4 阶段引入 shuffle exchange |
| 多 key JOIN | `ON a.sip = b.sip AND a.dport = c.port`，两个 key 分区策略冲突 | 拆为两步 JOIN，或其中一方声明为 replicated |
| 全局聚合 | `SELECT count(*) FROM fw_by_sip` 需汇总所有节点 | V3 阶段两阶段聚合：各节点局部聚合 → 汇总节点合并 |
| 热点 key | 某个 sip 数据量极大，单节点负载高 | 监控 + 告警；极端情况可拆分为子 key（sip + 时间片） |


## 附录 B: 业界验证

Window 订阅者模型的三种分布模式（partitioned / replicated / local）与主流分布式流引擎的核心机制一致：

| WarpFusion | Apache Flink | Kafka Streams | Esper (CEP) |
|------------|-------------|---------------|-------------|
| `partitioned(key)` | `keyBy(sip)` + Keyed State | `KTable`（按 key 分区） | `context partition by sip` |
| `replicated` | `broadcast()` + Broadcast State | `GlobalKTable`（全实例全量） | — |
| `local` | Operator State | 单实例 KStream | 默认行为 |

**与 Flink 的差异：**

| 维度 | Flink | WarpFusion |
|------|-------|-----------|
| 表达方式 | 命令式 API（代码中写 `keyBy` / `broadcast`） | 声明式配置（TOML 中定义 Window mode） |
| 分区绑定 | 绑定在算子/流上 | 绑定在 Window 上 |
| 灵活度 | 高（DAG 中每个算子可独立分区） | 中（Window 粒度，更简单直观） |
| 复杂度 | 高（需理解算子拓扑） | 低（配置即分布策略） |

WarpFusion 的声明式模型更接近 Esper 的 `PARTITION BY ... FROM ...` 语法，但进一步将分区声明和窗口定义合为一体，对 SQL 规则场景表达力足够，同时大幅降低配置和理解成本。


## 附录 C: 与外部流计算引擎的选型对比

本方案选择自建而非使用外部服务（RisingWave/Arroyo/Flink）的原因：

| 维度 | 外部服务 | 自建 WarpFusion |
|------|---------|----------------|
| 外部依赖 | 需要 Kafka + 流引擎 + 可能的存储 | 零外部依赖 |
| 部署复杂度 | 至少 2-3 个组件 | 单二进制 |
| 资源占用 | 数百 MB 至 GB 级 | <100MB |
| 可控性 | 受限于引擎能力 | 完全可控 |
| 与 WarpParse 集成 | 需 Kafka 中转 | TCP 直连（同机 loopback / 跨机直连） |
| 适用规模 | 分布式大规模 | 单机起步，可分布式扩展 |

本方案适用于单机部署、中小数据量（万级 EPS 以内）的实时关联场景。通过 Window 分布模式声明，可平滑扩展到多节点分布式架构。
