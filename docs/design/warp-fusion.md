# WarpFusion — 实时关联计算引擎设计方案
<!-- 角色：架构师 / 技术决策者 | 状态：L1 已实现 | 创建：2026-02-13 | 更新：2026-02-24 -->

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
- 代码中的 `AlertRecord` 保留原命名，但语义统一指”风险告警记录”。

### 1.5 当前实现状态（L1）

截至 M24，WarpFusion 已完成 L1（单机 MVP）闭环：

- **WFL 编译器**：.wfs + .wfl 解析 → 语义检查 → 编译为 `RulePlan`（CEP 状态机 + score/entity/yield 求值计划）
- **CEP 引擎**：基于 scope-key 的状态机实例管理，支持 `on event` / `on close` 双阶段求值
- **运行时闭环**：TCP 接收 Arrow IPC → Window 缓冲 → 规则执行 → 风险告警输出（Connector SinkDispatcher）
- **告警输出**：Connector 模式（yield-target 路由 SinkDispatcher，基于 wp-connector-api）
- **开发者工具**：`wfl`（explain / lint / fmt）、`wfgen`（测试数据生成 + oracle + verify）
- **传输层**：Best-Effort 模式，Arrow IPC over TCP

**L1 未实现**（计划 L2+）：
- `join snapshot/asof`（JoinPlan 为空）
- DataFusion SQL 执行
- `limits` 资源预算执行
- `at_least_once` / `exactly_once` 传输可靠性
- 分布式路由（Replicated / Partitioned）

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
│ Parser (WPL)     │  Arrow IPC       │    ↓ 路由到 Window                       │
│   ↓              │  单向推送        │  Router (根据 Window 订阅声明路由)        │
│ Router           │  ────────────→   │    ↓ Notify 通知                         │
│   ↓              │                  │  ┌→ Window["auth_events"]    local        │
│ Sinks            │                  │  └→ Window["security_alerts"] local       │
│   ├ File         │                  │                                          │
│   ├ TCP          │                  │  RuleTask ×N (per-rule, pull-based)       │
│   └ Arrow (新增) │                  │    ↓ cursor-based read_since             │
└──────────────────┘                  │  CepStateMachine + RuleExecutor           │
                                      │    ↓                                     │
wp-reactor Node X ─TCP─┐              │  SinkDispatcher (风险告警输出)          │
                       │              │    └ yield-target 路由                 │
wp-reactor Node Y ─TCP─┼──→ 同一端口  │                                          │
                       └──→           │  Evictor (定期淘汰过期窗口数据)            │
                                      └──────────────────────────────────────────┘
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
2. 构建 WindowRegistry（schema → WindowDef → Window 实例 + 订阅表）
3. 按 LIFO 顺序启动任务：alert → evictor → rules → receiver
4. Receiver 从 Socket 接收 Arrow IPC 消息，解析 stream tag
5. Router 查询 WindowRegistry，找到订阅该 stream 的所有 Window
6. 按各 Window 的分布模式路由数据（单机直接本地 append），推进 watermark
7. append 成功后通过 Notify 唤醒关联的 RuleTask
8. RuleTask 通过 read_since(cursor) 拉取新数据，转换为 Event 后推进 CepStateMachine
9. 状态机 on event 命中后，RuleExecutor 求值 score/entity，生成 AlertRecord
10. AlertRecord 通过 mpsc channel 发送到 alert dispatcher 任务：
    - 序列化为 JSON，由 SinkDispatcher 按 yield_target 路由到匹配的 sink 组
11. Evictor 定期淘汰过期窗口数据（时间淘汰 + 内存淘汰）
12. 关闭：先停 Receiver，通过 rule_cancel 触发规则最终 drain+flush，LIFO 逆序 join
```


## 3. 工程结构

### 3.1 代码仓库布局

WarpFusion 使用 Cargo workspace 组织，工程名 `wp-reactor`，与 wp-motor 平级。通过共享 crate `wp-arrow` 桥接 Arrow IPC 传输。

```
wp-labs/
├── wp-motor/                  # 现有解析引擎（不动）
├── wp-arrow/                  # 共享 crate：Arrow IPC 编解码
└── wp-reactor/                # WarpFusion 关联计算引擎
    ├── Cargo.toml             # workspace 根
    ├── crates/
    │   ├── wf-lang/           # WFL/WFS 语言编译器（纯同步，零运行时依赖）
    │   ├── wf-config/         # 配置解析（.toml + 项目工具共享函数）
    │   ├── wf-core/           # 核心引擎（Window + CEP + Alert）
    │   ├── wf-runtime/        # 异步运行时（Receiver + RuleTask + Lifecycle）
    │   ├── wf-engine/         # 引擎二进制 → wfusion（面向运维）
    │   ├── wfl/               # 项目工具二进制 → wfl（面向规则开发者）
    │   └── wfgen/        # 测试数据生成二进制 → wfgen
    ├── examples/              # 配置与规则示例
    │   ├── wfusion.toml
    │   ├── schemas/
    │   │   └── security.wfs
    │   ├── rules/
    │   │   └── brute_force.wfl
    │   ├── scenarios/
    │   │   └── brute_force.wfg
    │   └── sinks/             # Connector-based sink 路由配置
    │       ├── defaults.toml
    │       ├── sink.d/        # Connector 定义
    │       ├── business.d/    # 业务路由组（按 yield-target 匹配）
    │       └── infra.d/       # 基础设施组（default / error）
    └── docs/                  # 工程文档
```

**三个二进制：**

| Crate | 二进制名 | 用途 |
|-------|---------|------|
| wf-engine | `wfusion` | 运行时引擎，`run --config wfusion.toml` |
| wfl | `wfl` | 开发者工具：`explain` / `lint` / `fmt` |
| wfgen | `wfgen` | 测试数据生成：`gen` / `lint` / `verify` / `bench` |

### 3.2 Crate 依赖关系

```
wf-lang  (纯同步，无运行时依赖)
  ↑
wf-config  (依赖 wf-lang + wp-connector-api，配置解析 + sink 路由配置)
  ↑
wf-core  (依赖 wf-config + wf-lang + wp-connector-api，核心引擎逻辑 + sink 调度)
  ↑
wf-runtime  (依赖 wf-core + wp-arrow + wp-connector-api，异步运行时 + sink 工厂)
  ↑
wf-engine  (依赖 wf-config + wf-runtime，引擎二进制)

wfl  (依赖 wf-config + wf-lang + tree-sitter，开发者工具)
wfgen  (依赖 wf-core + wf-lang，测试数据生成)
```

### 3.3 wf-lang（WFL 编译器）

纯同步 crate，零运行时依赖。使用 winnow 解析器组合子。

```
wf-lang/
└── src/
    ├── lib.rs                 # 公开 API：parse_wfl, parse_wfs, check_wfl, lint_wfl,
    │                          #           compile_wfl, preprocess_vars, preprocess_vars_with_env
    ├── ast.rs                 # WFL AST 类型（WflFile, RuleDecl, Expr, ContractBlock...）
    ├── plan.rs                # 编译产物：RulePlan, MatchPlan, BindPlan, ScorePlan...
    ├── schema.rs              # WindowSchema, FieldDef, BaseType, FieldType
    ├── wfl_parser/            # .wfl 解析器（winnow）
    ├── wfs_parser/            # .wfs 解析器（winnow）
    ├── checker/               # 语义检查
    │   ├── mod.rs             # check_wfl: L1 语义错误检查
    │   ├── lint.rs            # lint_wfl: 最佳实践警告（W001–W006）
    │   ├── rules.rs           # 规则级检查逻辑
    │   ├── contracts.rs       # contract 块检查逻辑
    │   └── types.rs           # CheckError, Severity
    ├── compiler/              # AST → RulePlan 编译
    │   └── mod.rs             # compile_wfl: 语义检查 + 编译
    ├── explain.rs             # 规则人类可读解释 + 字段血缘分析
    ├── preprocess.rs          # 变量替换预处理（支持环境变量回退）
    └── parse_utils.rs         # 解析工具函数
```

### 3.4 wf-core（核心引擎）

Window 缓冲、CEP 状态机、规则执行、告警输出、sink 调度。同步与异步混合 API。

```
wf-core/
└── src/
    ├── lib.rs
    ├── window/
    │   ├── mod.rs
    │   ├── buffer.rs          # Window: 带 watermark 的时间窗口缓冲 + cursor-based 读取
    │   ├── registry.rs        # WindowRegistry: 窗口注册 + stream→window 订阅表
    │   ├── router.rs          # Router: watermark-aware 路由 + Notify 通知
    │   └── evictor.rs         # Evictor: 时间淘汰 + 内存淘汰
    ├── rule/
    │   ├── mod.rs
    │   ├── match_engine.rs    # CepStateMachine: scope-key 状态机 + 表达式求值
    │   ├── executor.rs        # RuleExecutor: score/entity 求值 → AlertRecord
    │   └── event_bridge.rs    # batch_to_events: RecordBatch → Vec<Event>
    ├── alert/
    │   ├── mod.rs
    │   └── types.rs           # AlertRecord（alert_id, score, entity, fired_at, yield_target...）
    ├── sink/
    │   ├── mod.rs
    │   ├── dispatch.rs        # SinkDispatcher: yield-target 路由引擎（connector 模式）
    │   └── runtime.rs         # SinkRuntime: 封装 wp-connector-api SinkHandle
    └── error.rs               # CoreError（基于 orion-error 结构化错误）
```

### 3.5 wf-runtime（异步运行时）

Tokio 异步运行时，管理任务生命周期。

```
wf-runtime/
└── src/
    ├── lib.rs
    ├── lifecycle/             # Reactor 生命周期管理
    │   ├── mod.rs             # Reactor: 启动引导 + 任务编排 + 两阶段关闭
    │   ├── bootstrap.rs       # load_and_compile: 配置加载 + 规则编译
    │   ├── compile.rs         # 模式编译: schemas/rules 构建
    │   ├── spawn.rs           # 任务组创建: alert/evictor/rules/receiver
    │   ├── signal.rs          # wait_for_signal: SIGINT + SIGTERM
    │   └── types.rs           # TaskGroup, RunRule, BootstrapData
    ├── receiver.rs            # Receiver: TCP 监听 + Arrow IPC 解码 + 路由
    ├── engine_task/           # RuleTask: per-rule pull-based 规则执行循环
    │   ├── mod.rs
    │   ├── rule_task.rs       # run_rule_task: Notify + cursor 驱动的事件处理
    │   ├── task_types.rs      # RuleTaskConfig, WindowSource
    │   └── window_lookup.rs   # 窗口查找辅助
    ├── alert_task.rs          # run_alert_dispatcher（connector sink 路由）
    ├── evictor_task.rs        # run_evictor: 定时淘汰
    ├── sink_build.rs          # SinkFactoryRegistry + build_sink_dispatcher
    ├── sink_factory/          # Sink 工厂实现
    │   ├── mod.rs
    │   └── file.rs            # FileSinkFactory: 异步文件写入（tokio BufWriter）
    ├── schema_bridge.rs       # WindowSchema + WindowConfig → WindowDef 转换
    └── error.rs               # RuntimeError（基于 orion-error）
```

### 3.6 wf-config（配置管理）

```
wf-config/
└── src/
    ├── lib.rs                 # 公开 API 统一导出
    ├── fusion.rs              # FusionConfig: 加载 wfusion.toml
    ├── server.rs              # ServerConfig: listen 地址
    ├── runtime.rs             # RuntimeConfig: schemas/rules glob、并行度
    ├── window.rs              # WindowConfig, WindowDefaults, WindowOverride
    ├── sink/                  # Connector-based sink 路由配置
    │   ├── mod.rs             # 模块导出
    │   ├── connector.rs       # ConnectorDefRaw, ConnectorTomlFile: sink.d/*.toml 解析
    │   ├── route.rs           # RouteFile, RouteGroup, RouteSink: business.d/*.toml 解析
    │   ├── defaults.rs        # DefaultsBody: defaults.toml 全局默认值
    │   ├── expect.rs          # GroupExpectSpec, SinkExpectOverride: 投递保证
    │   ├── group.rs           # FlexGroup（业务组）, FixedGroup（基础设施组）
    │   ├── build.rs           # 参数合并（allow_override 白名单）、标签三级合并
    │   ├── io.rs              # SinkConfigBundle, load_sink_config: 目录加载
    │   ├── types.rs           # StringOrArray, WildArray, ParamMap
    │   └── validate.rs        # validate_sink_coverage: yield-target 覆盖校验
    ├── logging.rs             # LoggingConfig: level, format, file, modules
    ├── types.rs               # HumanDuration, ByteSize, DistMode, LatePolicy
    ├── validate.rs            # over vs over_cap 校验
    └── project.rs             # 项目工具共享函数: load_wfl, load_schemas, parse_vars
```

### 3.7 wfl（开发者工具）

同步 CLI，不依赖 tokio。使用 tree-sitter-wfl 做代码格式化。

```
wfl/
└── src/
    ├── main.rs                # CLI: Explain / Lint / Fmt 子命令
    ├── cmd_explain.rs         # 编译规则并输出人类可读解释
    ├── cmd_lint.rs            # 语义检查 + lint 检查，输出诊断信息
    └── cmd_fmt.rs             # tree-sitter 语法验证 + 行级缩进规范化
```

### 3.8 wfgen（测试数据生成）

```
wfgen/
└── src/
    ├── main.rs                # CLI: Gen / Lint / Verify / Bench 子命令
    ├── lib.rs
    ├── wfg_ast.rs             # .wfg 场景文件 AST
    ├── wfg_parser.rs          # .wfg 解析器（winnow）
    ├── loader.rs              # 场景加载: .wfg → schemas + rules + plans
    ├── datagen.rs             # 数据生成: stream/field/inject/fault
    ├── oracle.rs              # Oracle: 在生成数据上运行规则，输出期望告警
    ├── validate.rs            # 场景文件校验
    ├── verify.rs              # 告警验证: expected vs actual → VerifyReport
    └── output.rs              # 输出格式: Arrow IPC / JSON Lines
```


## 4. 核心组件设计

### 4.1 Window — 带订阅声明的时间窗口

Window 是 WarpFusion 的核心抽象，兼具数据订阅声明和时间窗口缓冲两个职责。

```rust
/// Window 逻辑定义（来自 .wfs 文件，由 wf-lang 解析）
pub struct WindowSchema {
    pub name: String,                     // Window 名称
    pub streams: Vec<String>,             // 订阅的 stream name（空 = yield-only 窗口）
    pub time_field: Option<String>,       // 事件时间字段（over > 0 时必须）
    pub over: Duration,                   // 窗口保持时长（Duration::ZERO = 静态集合）
    pub fields: Vec<FieldDef>,            // 字段 schema
}

/// Window 运行时配置（来自 .toml 文件，经 WindowDefaults 合并后）
pub struct WindowConfig {
    pub name: String,
    pub mode: DistMode,                   // 分布模式
    pub max_window_bytes: ByteSize,       // 单窗口内存上限
    pub over_cap: HumanDuration,          // over 能力上限（over ≤ over_cap）
    pub evict_policy: EvictPolicy,        // 淘汰策略
    pub watermark: HumanDuration,         // Watermark 延迟（默认 5s）
    pub allowed_lateness: HumanDuration,  // 允许迟到时长（默认 0）
    pub late_policy: LatePolicy,          // 迟到数据处理策略
}

/// 分布模式
pub enum DistMode {
    Local,
    Replicated,
    Partitioned { key: String },
}

/// Window 运行时实例：带 watermark 的时间窗口缓冲
pub struct Window {
    // 内部字段（private）
    params: WindowParams,                 // name, schema, time_col_index, over
    config: WindowConfig,                 // 运行时配置
    batches: VecDeque<TimedBatch>,        // 带时间戳的批数据队列
    current_bytes: usize,                 // 当前已用字节数
    total_rows: usize,                    // 当前总行数
    watermark_nanos: i64,                 // 当前 watermark 时间戳
    next_seq: u64,                        // 单调递增序列号（用于 cursor）
}

struct TimedBatch {
    batch: RecordBatch,
    event_time_range: (i64, i64),         // 该批数据的事件时间范围
    ingested_at: Instant,                 // 接收时间
    row_count: usize,
    byte_size: usize,
    seq: u64,                             // 分配的序列号
}

impl Window {
    /// 追加数据（不检查 watermark）
    pub fn append(&mut self, batch: RecordBatch) -> Result<()>;
    /// 追加数据并检查 watermark；迟到数据按 late_policy 处理
    pub fn append_with_watermark(&mut self, batch: RecordBatch) -> Result<AppendOutcome>;
    /// 获取当前窗口内的只读快照
    pub fn snapshot(&self) -> Vec<RecordBatch>;
    /// cursor-based 读取：返回 (batches, new_cursor, gap_detected)
    pub fn read_since(&self, cursor: u64) -> (Vec<RecordBatch>, u64, bool);
    /// 淘汰过期数据（基于事件时间）
    pub fn evict_expired(&mut self, now_nanos: i64);
    /// 淘汰最早的一个 batch（内存压力时使用）
    pub fn evict_oldest(&mut self) -> Option<usize>;
    /// 返回当前窗口的内存占用（字节）
    pub fn memory_usage(&self) -> usize;
}

pub enum AppendOutcome {
    Appended,
    DroppedLate,
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

### 4.2 WindowRegistry + Router — 窗口注册与路由

WindowRegistry 维护所有 Window 实例及订阅表。Router 封装 WindowRegistry 提供 watermark-aware 路由。

```rust
pub struct WindowDef {
    pub params: WindowParams,
    pub streams: Vec<String>,     // 此窗口订阅的 stream 名称
    pub config: WindowConfig,
}

pub struct WindowRegistry {
    /// Window 名称 → 运行时实例
    windows: HashMap<String, Arc<RwLock<Window>>>,
    /// Stream name → 订阅该 stream 的 Window 列表（路由表）
    subscriptions: HashMap<String, Vec<Subscription>>,
    /// Window 名称 → Notify handle（用于唤醒 RuleTask）
    notifiers: HashMap<String, Arc<Notify>>,
}

impl WindowRegistry {
    pub fn build(defs: Vec<WindowDef>) -> CoreResult<Self>;
    pub fn get_window(&self, name: &str) -> Option<&Arc<RwLock<Window>>>;
    pub fn get_notifier(&self, name: &str) -> Option<&Arc<Notify>>;
    pub fn snapshot(&self, name: &str) -> Option<Vec<RecordBatch>>;
    pub(crate) fn subscribers_of(&self, stream_name: &str) -> Vec<(&str, &DistMode)>;
}

pub struct Router {
    registry: WindowRegistry,
}

pub struct RouteReport {
    pub delivered: usize,
    pub dropped_late: usize,
    pub skipped_non_local: usize,
}

impl Router {
    pub fn new(registry: WindowRegistry) -> Self;
    /// 路由 batch 到所有订阅该 stream 的 Local 窗口，append 成功后 Notify 唤醒 RuleTask
    pub fn route(&self, stream_name: &str, batch: RecordBatch) -> Result<RouteReport>;
    pub fn registry(&self) -> &WindowRegistry;
}
```

#### 4.2.1 数据流全景

```
                         ┌─────────────────────────────────┐
                         │         BOOT 阶段                │
                         │   .wfs + wfusion.toml → 订阅表    │
                         │   .wfl → RulePlan → RunRule       │
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
                          │   Router.route()         │
                          │   → Window.append_with_  │
                          │     watermark(batch)      │
                          │   → Notify.notify_        │
                          │     waiters()             │
                          └────────────┬─────────────┘
                                       │
                          ┌────────────┴────────────┐
                          │                         │
                          ▼                         ▼
                 ┌─────────────────┐     ┌──────────────────────┐
                 │ Window Buffer   │     │ RuleTask (pull-based) │
                 │ (时间窗口存储)   │←────│ read_since(cursor)    │
                 └─────────────────┘     │ batch_to_events()     │
                                         │ machine.advance()     │
                                         └──────────┬───────────┘
                                                    │
                                                    ▼
                                         ┌──────────────────────┐
                                         │ SinkDispatcher       │
                                         │   → yield-target 路由│
                                         └──────────────────────┘
```

Router 负责将 batch 写入 Window 缓冲区并通过 Notify 唤醒 RuleTask。RuleTask 通过 cursor-based `read_since()` 拉取新数据，转换为事件后推进 CEP 状态机。

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
│          schema_bridge::schema_to_window_def            │
│  WindowSchema + WindowConfig → WindowDef {              │
│    params:  { name: "auth_events", schema, over, ... }  │
│    streams: ["syslog"],         ← 来自 .wfs             │
│    config:  { mode: Local, ... } ← 来自 wfusion.toml    │
│  }                                                      │
└────────────────────────┬───────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────┐
│            WindowRegistry::build(defs)                  │
│                                                         │
│  for def in defs:                                       │
│    windows["auth_events"] = Arc::new(RwLock::new(       │
│      Window::new(params, config)))                      │
│    notifiers["auth_events"] = Arc::new(Notify::new())   │
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
│  └────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────┘
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
│    │   → skipped_non_local += 1（Replicated/Partitioned L1 暂不处理）
│    │
│    └─ mode == Local
│        → window.append_with_watermark(batch)
│          │
│          ├─ ③ 提取时间范围
│          │    (min_ts, max_ts) = extract_time_range(batch)
│          │
│          ├─ ④ 迟到检查（在推进 watermark 之前）
│          │    cutoff = watermark - allowed_lateness
│          │    min_ts < cutoff ?
│          │    ├─ YES → late_policy:
│          │    │   └─ Drop → DroppedLate
│          │    └─ NO  → append(batch) → Appended ✓
│          │
│          ├─ ⑤ 推进 Watermark
│          │    watermark = max(watermark, max_ts - delay)
│          │
│          └─ ⑥ 唤醒 RuleTask
│               notifiers[window_name].notify_waiters()
│               （在释放 write lock 之后）
│
└─ 返回 RouteReport { delivered, dropped_late, skipped_non_local }
```

#### 4.2.4 RuleTask：Pull-Based 规则执行

取代了原设计中的 Scheduler，每条编译后的规则对应一个独立的 `RuleTask`，通过 Notify + cursor 机制拉取数据。

```rust
pub(crate) struct RuleTaskConfig {
    pub machine: CepStateMachine,
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    pub stream_aliases: HashMap<String, Vec<String>>,  // stream_name → Vec<alias>
    pub alert_tx: mpsc::Sender<AlertRecord>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
}

pub(crate) struct WindowSource {
    pub window_name: String,
    pub window: Arc<RwLock<Window>>,
    pub notify: Arc<Notify>,
    pub stream_names: Vec<String>,
}
```

**Pull-based 执行循环：**

```
RuleTask main loop:
│
├─ 初始化 cursors: window_name → window.next_seq()
│  （跳过历史数据，仅处理新 batch）
│
├─ loop:
│    ├─ ① 注册 Notify futures（调用 .enable() 后再读数据，防止丢失通知）
│    │
│    ├─ ② pull_and_advance()
│    │    for source in window_sources:
│    │      (batches, new_cursor, gap) = window.read_since(cursor)
│    │      cursor = new_cursor
│    │      for batch in batches:
│    │        events = batch_to_events(batch)
│    │        for event in events:
│    │          for alias in stream_aliases[source.stream_name]:
│    │            match machine.advance(alias, event):
│    │              Accumulate → (继续)
│    │              Advance    → (继续)
│    │              Matched(ctx) →
│    │                alert = executor.execute_match(ctx)
│    │                alert_tx.send(alert)
│    │
│    ├─ ③ select!:
│    │    ├─ any Notify triggered → continue loop
│    │    ├─ timeout_interval.tick() →
│    │    │    expired = machine.scan_expired()
│    │    │    for close in expired:
│    │    │      if let Some(alert) = executor.execute_close(close):
│    │    │        alert_tx.send(alert)
│    │    └─ cancel triggered →
│    │         final pull_and_advance()  // 处理剩余数据
│    │         machine.close_all(Eos)    // flush 所有实例
│    │         break
```

> **设计要点**：RuleTask 的 cursor 在创建时设置为 `window.next_seq()`，只处理新到达的数据。如果 cursor 落后于 eviction（`gap_detected = true`），RuleTask 会记录日志但继续处理。

### 4.3 CepStateMachine — CEP 状态机

每条规则编译为一个 `MatchPlan`，由 `CepStateMachine` 在运行时驱动。状态机按 scope-key 维护独立的 `Instance`，支持多步骤 OR 分支和聚合管道。

```rust
/// CEP 状态机
pub struct CepStateMachine {
    rule_name: String,
    plan: MatchPlan,
    instances: HashMap<String, Instance>,  // scope-key 序列化值 → 实例
    time_field: Option<String>,
    watermark_nanos: i64,
}

/// 事件
pub struct Event {
    pub fields: HashMap<String, Value>,
}

pub enum Value {
    Number(f64),
    Str(String),
    Bool(bool),
}

/// 步骤推进结果
pub enum StepResult {
    Accumulate,                    // 事件已消费，未跨越步骤边界
    Advance,                      // 步骤条件满足，推进到下一步
    Matched(MatchedContext),       // 所有步骤完成，规则命中
}

/// 命中上下文
pub struct MatchedContext {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub step_data: Vec<StepData>,
    pub event_time_nanos: i64,
}

/// 窗口关闭输出
pub struct CloseOutput {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub close_reason: CloseReason,  // Timeout | Flush | Eos
    pub event_ok: bool,             // on event 是否满足
    pub close_ok: bool,             // on close 是否满足
    pub event_step_data: Vec<StepData>,
    pub close_step_data: Vec<StepData>,
    pub watermark_nanos: i64,
}

impl CepStateMachine {
    pub fn advance(&mut self, alias: &str, event: &Event) -> StepResult;
    pub fn close(&mut self, scope_key: &[Value], reason: CloseReason) -> Option<CloseOutput>;
    pub fn scan_expired(&mut self) -> Vec<CloseOutput>;      // 按 watermark 扫描过期实例
    pub fn close_all(&mut self, reason: CloseReason) -> Vec<CloseOutput>;  // shutdown flush
    pub fn instance_count(&self) -> usize;
}
```

**内部实例状态：**

```rust
struct Instance {
    scope_key: Vec<Value>,
    created_at: i64,           // 纳秒时间戳
    current_step: usize,       // on event 当前步骤索引
    event_ok: bool,            // on event 所有步骤是否已完成
    step_states: Vec<StepState>,        // on event 步骤状态
    completed_steps: Vec<StepData>,     // on event 已完成步骤数据
    close_step_states: Vec<StepState>,  // on close 步骤状态
}

struct BranchState {
    count: usize, sum: f64, min: f64, max: f64,
    min_val: Option<Value>, max_val: Option<Value>,
    avg_sum: f64, avg_count: usize,
    distinct_set: HashSet<String>,  // Distinct transform
}
```

**表达式求值器**：`eval_expr` 支持字面量、字段引用、二元运算（And/Or/比较/算术）、取反、InList、函数调用（`contains`/`lower`/`upper`/`len`）。AND/OR 使用三值 SQL NULL 语义。

**聚合度量**：Count、Sum、Avg、Min、Max。**变换**：Distinct（在累积阶段去重）。

### 4.4 RuleExecutor — 规则执行器

RuleExecutor 从 CEP 状态机的 match/close 输出中求值 score/entity 表达式，生成 AlertRecord。

```rust
pub struct RuleExecutor {
    plan: RulePlan,
}

impl RuleExecutor {
    /// 事件命中后求值，生成告警
    pub fn execute_match(&self, matched: &MatchedContext) -> CoreResult<AlertRecord>;
    /// 窗口关闭后求值，仅当 event_ok && close_ok 时生成告警
    pub fn execute_close(&self, close: &CloseOutput) -> CoreResult<Option<AlertRecord>>;
}
```

**编译后的规则计划（RulePlan）：**

```rust
pub struct RulePlan {
    pub name: String,
    pub binds: Vec<BindPlan>,          // 事件绑定
    pub match_plan: MatchPlan,         // CEP 状态机计划
    pub joins: Vec<JoinPlan>,          // L1 为空；L2 支持 snapshot/asof
    pub entity_plan: EntityPlan,       // entity(type, id_expr)
    pub yield_plan: YieldPlan,         // yield target(fields...)
    pub score_plan: ScorePlan,         // score(expr)
    pub conv_plan: Option<ConvPlan>,   // L1 为 None
}
```

**关键设计决策：**

- WFL 规则由 `wf-lang` 编译器统一编译为 `RulePlan`（CEP 状态机），RuleExecutor 以事件驱动方式执行
- Rule 引用 Window 名称（定义在 .wfs 中），而非 Stream 名称——同一个 Stream 可被不同 Window 以不同方式订阅
- L1 不含 Join 执行（JoinPlan 为空），L2 将引入 DataFusion 执行 `join snapshot/asof`
- Score 求值直接从 `ScorePlan.expr` 计算，结果 clamp 到 [0, 100]
- Entity ID 从 MatchedContext 中的 scope-key 或 step-data 提取
- `on close` 仅在 `event_ok && close_ok` 时生成告警（部分满足的实例不输出）
- AlertRecord 的 `yield_target` 字段（`serde(skip)`）来自 `YieldPlan.target`，用于 sink 路由

### 4.5 告警输出 — Connector-based Sink 路由

WarpFusion 使用基于 `wp-connector-api` 的 Connector sink 路由系统输出告警。AlertRecord 序列化为 JSON 后，由 `SinkDispatcher` 按 `yield_target`（yield 目标窗口名）匹配路由到对应 sink 组。通过 `sinks = "sinks"` 在 `wfusion.toml` 中配置 sink 目录路径。

#### 4.5.1 AlertRecord

```rust
#[derive(Debug, Clone, serde::Serialize)]
pub struct AlertRecord {
    pub alert_id: String,                      // "rule|key1\x1fkey2|fired_at#seq"
    pub rule_name: String,
    pub score: f64,                            // [0, 100]
    pub entity_type: String,                   // 由 entity(type, id_expr) 求值
    pub entity_id: String,
    pub close_reason: Option<String>,          // "timeout" | "flush" | "eos" | None
    pub fired_at: String,                      // ISO 8601 UTC（无 chrono 依赖，内置实现）
    #[serde(skip)]
    pub matched_rows: Vec<RecordBatch>,        // L1 为空
    pub summary: String,                       // 规则执行摘要
    #[serde(skip)]
    pub yield_target: String,                  // yield 目标窗口名，用于 sink 路由
}
```

**Alert ID 格式**：`"rule|key1\x1fkey2|fired_at#seq"`，使用 `%` 编码分隔符，`seq` 为进程级 `AtomicU64`。

#### 4.5.2 Sink 路由（yield-target 路由）

```
告警路由流程：

AlertRecord → serde_json::to_string → SinkDispatcher.dispatch(yield_target, json)
                                        │
                                        ├─ ① 遍历 BusinessGroup
                                        │    windows.matches(yield_target)?
                                        │    ├─ YES → 发送到该组所有 SinkRuntime
                                        │    └─ NO  → 继续下一个组
                                        │
                                        ├─ ② 无 BusinessGroup 匹配
                                        │    → 发送到 default_group（如已配置）
                                        │
                                        └─ ③ 任何 sink 写入失败
                                             → 额外发送到 error_group（如已配置）
```

**核心类型：**

```rust
/// yield-target 路由引擎
pub struct SinkDispatcher {
    business: Vec<BusinessGroup>,           // 业务路由组（带 window 通配符匹配）
    default_group: Option<SinkGroup>,       // 兜底组（无匹配时使用）
    error_group: Option<SinkGroup>,         // 错误组（写入失败时使用）
}

/// 单个 sink 实例的运行时状态
pub struct SinkRuntime {
    pub name: String,
    pub spec: ResolvedSinkSpec,             // 来自 wp-connector-api
    pub handle: Mutex<SinkHandle>,          // 异步 sink 句柄
    pub tags: Vec<String>,
}
```

`SinkDispatcher` 任务（`run_alert_dispatcher`）通过 mpsc channel（容量 64）接收 AlertRecord，channel-close-driven 退出。当所有 RuleTask drop 了 `alert_tx` sender 后，接收端返回 `None`，任务自动退出。退出前调用 `dispatcher.stop_all()` 优雅关闭所有 sink。

#### 4.5.3 Sink 工厂

`SinkFactoryRegistry` 管理 sink 类型（kind）到工厂的映射。`build_sink_dispatcher` 从 `SinkConfigBundle` 构建 `SinkDispatcher`。

```rust
/// Sink 工厂注册表
pub struct SinkFactoryRegistry {
    factories: HashMap<String, Arc<dyn SinkFactory>>,
}
```

L1 内置工厂：

| Kind | 工厂 | 实现 | 说明 |
|------|------|------|------|
| `"file"` | `FileSinkFactory` | `AsyncFileSink` | 异步文件写入（`tokio::io::BufWriter`），自动创建父目录 |

`SinkFactory` trait 来自 `wp-connector-api`，定义 `kind()` / `validate_spec()` / `build()` 三个方法。`AsyncFileSink` 实现 `AsyncCtrl`（stop/reconnect）和 `AsyncRawDataSink`（sink_str/sink_bytes/batch）接口。

#### 4.5.4 Sink 配置结构

```
sinks/                         # sinks 根目录（wfusion.toml 中 sinks = "sinks" 指向此处）
├── defaults.toml              # 全局默认值（tags、expect）
├── sink.d/                    # Connector 定义（sink 类型 + 默认参数 + 可覆盖白名单）
│   └── file_json.toml
├── business.d/                # 业务路由组（window 通配符匹配 + sink 列表）
│   ├── security.toml
│   └── catch_all.toml
└── infra.d/                   # 基础设施组
    ├── default.toml           # 兜底组（yield-target 无匹配时）
    └── error.toml             # 错误组（sink 写入失败时）
```

**Connector 定义**（`sink.d/file_json.toml`）：

```toml
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["path"]

[connectors.params]
path = "alerts/default.jsonl"
```

**业务路由组**（`business.d/security.toml`）：

```toml
version = "1.0"

[sink_group]
name = "security_output"
windows = ["security_*"]          # yield-target 通配符匹配

[[sink_group.sinks]]
connect = "file_json"             # 引用 connector id
name = "sec_file"

[sink_group.sinks.params]
path = "alerts/security_alerts.jsonl"   # 覆盖 connector 默认参数
```

**基础设施组**（`infra.d/default.toml`）：

```toml
[sink_group]
name = "__default"

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
path = "alerts/unrouted.jsonl"
```

**配置解析产物：**

```rust
pub struct SinkConfigBundle {
    pub connectors: BTreeMap<String, ConnectorDef>,  // id → connector 定义
    pub defaults: DefaultsBody,                      // 全局默认值
    pub business: Vec<FlexGroup>,                    // 业务路由组（已解析）
    pub infra_default: Option<FixedGroup>,           // 兜底组
    pub infra_error: Option<FixedGroup>,             // 错误组
}
```

**参数合并规则：**
- **allow_override 白名单**：sink 级参数覆盖仅允许 connector 声明的 `allow_override` 中列出的 key
- **标签三级合并**：defaults（最低）→ group → sink（最高），同 key 前缀（`:`前部分）的标签后者覆盖前者
- **sink coverage 校验**：启动时检查所有 yield-target 至少被一个 business group 或 infra_default 覆盖


## 5. 并发模型

### 5.1 设计原则

| 原则 | 说明 |
|------|------|
| Pull-based | RuleTask 主动拉取 Window 数据（通过 cursor），而非被动接收推送 |
| Notify 唤醒 | Router append 后通过 `Notify::notify_waiters()` 唤醒关联 RuleTask |
| Per-rule 独立 | 每条规则一个独立的 tokio task，互不阻塞 |
| LIFO 生命周期 | 启动顺序：消费者先于生产者；关闭顺序：生产者先于消费者 |
| Channel-close-driven | Alert dispatcher 任务通过 channel 关闭信号自然退出 |
| CancellationToken | 两级取消：`cancel`（全局）+ `rule_cancel`（规则级，延迟触发） |

### 5.2 任务拓扑

```
TaskGroup: alert
  └─ run_alert_dispatcher (×1)
       ↑ mpsc channel (容量 64)
       │ AlertRecord → JSON → SinkDispatcher.dispatch(yield_target, json)
       │ 退出时调用 dispatcher.stop_all()

TaskGroup: evictor
  └─ run_evictor (×1，定时清理过期窗口数据)

TaskGroup: rules
  └─ run_rule_task (×N, 每条规则一个)
       ← Notify 唤醒
       ← cursor-based read_since()
       → alert_tx.send(AlertRecord)

TaskGroup: receiver
  └─ Receiver::run (×1)
       ↓ per-connection task (tokio::spawn)
       ↓ decode Arrow IPC → Router.route()
       ↓ → Window.append_with_watermark()
       ↓ → Notify.notify_waiters()
```

### 5.3 启动与关闭顺序

**启动顺序（先消费者后生产者）：**

1. `alert` — AlertSink 启动，持有 `alert_rx` 接收端
2. `evictor` — Evictor 启动，定时清理（初始延迟 = interval，避免清理新数据）
3. `rules` — RuleTask ×N 启动，各自初始化 cursor 为 `window.next_seq()`
4. `receiver` — Receiver 最后启动，开始接收网络数据

**关闭顺序（两阶段，LIFO join）：**

```
Phase 1: 停止数据输入
  cancel.cancel()
  → Receiver 停止 accept，现有连接完成当前帧后退出
  → join receiver task group

Phase 2: 清空规则管道
  rule_cancel.cancel()
  → 每个 RuleTask:
    ① 最后一次 pull_and_advance()（处理 Window 中剩余数据）
    ② machine.close_all(Eos)（flush 所有状态机实例）
    ③ drop alert_tx（关闭 channel sender）
  → join rules task group
  → alert_rx 收到 None，alert dispatcher 自动退出
  → join alert task group
  → join evictor task group
```

### 5.4 Reactor — 生命周期管理

```rust
pub struct Reactor {
    cancel: CancellationToken,
    rule_cancel: CancellationToken,  // receiver 停止后延迟触发
    groups: Vec<TaskGroup>,          // 按启动顺序存储
    listen_addr: SocketAddr,
}

impl Reactor {
    pub async fn start(config: FusionConfig, base_dir: &Path) -> RuntimeResult<Self>;
    pub fn shutdown(&self);           // 触发 cancel
    pub async fn wait(mut self) -> RuntimeResult<()>;  // LIFO join all groups
}

pub async fn wait_for_signal(cancel: CancellationToken);  // SIGINT + SIGTERM
```

**Bootstrap 阶段（`Reactor::start` 内部）：**

1. 加载 `.wfs` → `Vec<WindowSchema>`
2. 预处理 + 解析 + 编译 `.wfl` → `Vec<RulePlan>`
3. 校验 `over` ≤ `over_cap`
4. `schema_bridge`: `WindowSchema × WindowConfig` → `Vec<WindowDef>`
5. `WindowRegistry::build(defs)`
6. `Router::new(registry)`
7. 构建 `RunRule`（预计算 stream_name → alias 路由）
8. 构建 SinkDispatcher（从 sinks/ 配置目录加载）
9. 按顺序启动 4 个 TaskGroup


## 6. 配置设计

### 6.1 主配置 wfusion.toml

三文件架构下，TOML 负责**运行时物理参数**。数据 schema 在 `.wfs` 文件中定义，检测逻辑在 `.wfl` 文件中定义。

```toml
# Connector-based sink routing（指向 sinks/ 配置目录）
sinks = "sinks"

[server]
listen = "tcp://127.0.0.1:9800"                   # 监听地址

[runtime]
executor_parallelism = 2                       # 规则执行并行度
rule_exec_timeout = "30s"                      # 单条规则执行超时
schemas = "schemas/*.wfs"                      # Window Schema 文件 glob 模式
rules   = "rules/*.wfl"                        # WFL 规则文件 glob 模式

[window_defaults]
evict_interval = "30s"                         # 淘汰检查间隔
max_window_bytes = "256MB"                     # 单窗口内存上限
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

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"

[vars]                                         # WFL $VAR 变量替换
FAIL_THRESHOLD = "3"

[logging]
level = "info"                                 # 全局日志级别
format = "plain"                               # plain | json
file = "logs/wf-engine.log"                    # 日志文件路径
[logging.modules]                              # 模块级日志级别覆盖
"wf_runtime::receiver" = "debug"
```

**配置分层原则：**

| 层级 | 文件 | 内容 | 示例 |
|------|------|------|------|
| 数据定义 | `.wfs` | stream 来源、time 字段、over 时长、字段 schema | `over = 5m` |
| 检测逻辑 | `.wfl` | 事件绑定、模式匹配、条件、输出 | `yield security_alerts (...)` |
| 物理约束 | `.toml` | mode、max_bytes、over_cap、watermark | `watermark = "5s"` |
| 变量替换 | `.toml [vars]` | WFL `$VAR` 替换值（也支持环境变量回退） | `FAIL_THRESHOLD = "3"` |
| 告警输出 | `sinks/` 目录 | Connector 路由配置 | `sinks = "sinks"` |

**over vs over_cap 校验：** 启动时检查每个 window 的 `.wfs` 中 `over` ≤ `.toml` 中 `over_cap`，不满足则报错拒绝启动。

**变量预处理：** `.wfl` 文件中的 `$VAR` 或 `${VAR}` 引用按以下优先级解析：
1. `.toml` 中 `[vars]` 显式定义
2. 环境变量（`std::env::var`）
3. 均未定义则报错

**Glob 解析：** `schemas` 和 `rules` 字段支持 glob 模式（如 `schemas/*.wfs`），相对于 `.toml` 文件所在目录解析。无匹配文件时报错。

### 6.2 关联规则

关联检测规则使用 WFL 语言编写，存储在 `.wfl` 文件中。完整语法和语义模型见 [WFL v2.1 设计方案](wfl-desion.md)，与主流 DSL 的对比分析见 [WFL DSL 对比](wfl-dsl-comparison.md)。

WFL 规则中的数据源名称引用 **Window Schema (.wfs) 中定义的 window 名称**，不直接引用 stream tag。这使得同一个 stream 可以以不同方式（不同 mode、不同 over）被多个 window 引用，规则按需选择合适的 window。

**Window Schema 示例（security.wfs）：**

```wfs
window auth_events {
    stream = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
```

**规则文件示例（brute_force.wfl）：**

```wfl
use "security.wfs"

rule brute_force_then_scan {
    events {
        fail : auth_events && action == "failed"
    }

    match<sip:5m> {
        on event {
            fail | count >= $FAIL_THRESHOLD;
        }
        on close {
            fail | count >= 1;
        }
    } -> score(70.0)

    entity(ip, fail.sip)

    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
```

**L1 规则编译管线（wf-lang）：**

```
.wfl 源码
  → preprocess_vars_with_env（变量替换，支持环境变量回退）
  → parse_wfl（winnow 解析器 → WflFile AST）
  → check_wfl（语义检查，返回 Vec<CheckError>）
  → compile_wfl（AST → Vec<RulePlan>）
    ├─ compile_binds → Vec<BindPlan>
    ├─ compile_match → MatchPlan（keys, WindowSpec::Sliding, event_steps, close_steps）
    ├─ compile_entity → EntityPlan
    ├─ compile_score → ScorePlan
    ├─ compile_yield → YieldPlan
    └─ joins: vec![], conv_plan: None（L1 未实现）
```


## 7. 依赖清单

### 7.1 Workspace 共享依赖

```toml
[workspace.package]
edition = "2024"
license = "Apache-2.0"

[workspace.dependencies]
serde = { version = "1.0", features = ["derive"] }
toml = "0.9"
anyhow = "1.0"
winnow = "0.7"                   # 解析器组合子（替代 nom）
wp-connector-api = "0.8"        # Sink 抽象层（SinkFactory, SinkHandle, ConnectorDef）
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json", "fmt"] }
tracing-appender = "0.2"
orion-error = { version = "0.6", default-features = false, features = ["tracing"] }  # 结构化错误
derive_more = { version = "2.1", features = ["from"] }
thiserror = "2.0"
```

### 7.2 wf-lang

```toml
[dependencies]
winnow.workspace = true          # .wfl + .wfs 解析
anyhow.workspace = true
```

### 7.3 wf-config

```toml
[dependencies]
wf-lang = { path = "../wf-lang" }  # 用于 parse_wfs 和 preprocess_vars
serde.workspace = true
toml.workspace = true
anyhow.workspace = true
glob = "0.3"                     # 文件 glob 匹配
wp-connector-api.workspace = true  # ConnectorDef, SinkSpec, ParamMap
serde_json = "1.0"               # ParamMap (JSON 值) 序列化
wildmatch = "2"                  # yield-target 通配符匹配
```

### 7.4 wf-core

```toml
[dependencies]
wf-config = { path = "../wf-config" }
wf-lang = { path = "../wf-lang" }
wp-connector-api.workspace = true  # SinkHandle, SinkSpec (用于 SinkRuntime)
arrow = { version = "54", default-features = false, features = ["ipc"] }
anyhow.workspace = true
serde.workspace = true
serde_json = "1.0"
async-trait = "0.1"
tokio = { version = "1", features = ["sync"] }  # Notify, RwLock, Mutex
log = "0.4"
orion-error.workspace = true
derive_more.workspace = true
thiserror.workspace = true
regex = "1"
```

### 7.5 wf-runtime

```toml
[dependencies]
wf-core = { path = "../wf-core" }
wf-config = { path = "../wf-config" }
wf-lang = { path = "../wf-lang" }
wp-connector-api.workspace = true  # SinkFactory, SinkBuildCtx, SinkHandle
wp-arrow = "0.1"                 # Arrow IPC 编解码
wp-model-core = "0.8"            # DataRecord（FileSinkFactory 需要 AsyncRecordSink）
arrow = { version = "54", default-features = false, features = ["ipc"] }
tokio = { version = "1", features = ["net", "io-util", "sync", "macros", "rt-multi-thread", "signal", "time", "fs"] }
tokio-util = { version = "0.7", features = ["rt"] }
anyhow.workspace = true
serde_json = "1.0"
log = "0.4"
async-trait = "0.1"
tracing.workspace = true
tracing-subscriber.workspace = true
tracing-appender.workspace = true
orion-error.workspace = true
derive_more.workspace = true
thiserror.workspace = true
```

### 7.6 wf-engine（二进制 `wfusion`）

```toml
[dependencies]
wf-config = { path = "../wf-config" }
wf-runtime = { path = "../wf-runtime" }
anyhow.workspace = true
clap = { version = "4", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
tracing.workspace = true
```

### 7.7 wfl（二进制 `wfl`）

```toml
[dependencies]
wf-config = { path = "../wf-config" }
wf-lang = { path = "../wf-lang" }
tree-sitter = "0.22"             # WFL 代码格式化
tree-sitter-wfl = { git = "https://github.com/wp-labs/tree-sitter-wfl.git", branch = "main" }
anyhow.workspace = true
clap = { version = "4", features = ["derive"] }
```

### 7.8 wfgen（二进制 `wfgen`）

```toml
[dependencies]
wf-core = { path = "../wf-core" }
wf-lang = { path = "../wf-lang" }
winnow.workspace = true
anyhow.workspace = true
serde.workspace = true
serde_json = "1.0"
clap = { version = "4", features = ["derive"] }
rand = "0.9"
chrono = { version = "0.4", features = ["serde"] }
arrow = { version = "54", default-features = false, features = ["ipc"] }
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

### 9.1 里程碑总览

| 阶段 | 内容 | 状态 |
|------|------|------|
| **P0** | wp-arrow: schema 映射 + 行列转换 + IPC 编解码 | 已完成 |
| **P1** | wp-motor: Arrow IPC Sink | 已完成 |
| **P1** | wf-config: 配置解析 | 已完成 |
| **P2** | wf-core/window: Window + WindowRegistry + Router | 已完成 |
| **P3** | wf-core/rule: CEP 状态机 + RuleExecutor | 已完成 |
| **P3** | wf-runtime: Receiver + RuleTask + Lifecycle | 已完成 |
| **P4** | wf-core/alert: AlertRecord；wf-core/sink: SinkDispatcher + SinkRuntime；wf-config/sink: 路由配置；wf-runtime: SinkFactoryRegistry + FileSinkFactory | 已完成 |
| **P5** | wf-lang: 编译器 + checker + lint | 已完成 |
| **P6** | wfl: explain / lint / fmt 工具 | 已完成 |
| **P7** | wfgen: 数据生成 + oracle + verify | 已完成 |
| **P8** | L2 增强: join / baseline / 条件表达式 / 函数 | 计划中 |
| **P9** | 正确性门禁: contract + shuffle + scenario verify | 计划中 |
| **P10** | 可靠性分级: at_least_once / exactly_once | 计划中 |
| **P11** | 分布式 V2+: 多节点部署 | 计划中 |

### 9.2 执行计划

详细的里程碑执行计划已独立为专属文档，详见 → [wf-execution-plan.md](wf-execution-plan.md)

**已完成阶段（截至 M24）：**

| 阶段 | 里程碑 | 阶段目标 | 状态 |
|------|--------|---------|------|
| **I 数据基建** | M01–M05 | Arrow IPC 传输可用 | 已完成 |
| **II 配置与窗口** | M06–M10 | 配置加载、Window 接收路由缓存 | 已完成 |
| **III WFL 编译器** | M11–M13 | .wfs + .wfl → RulePlan | 已完成 |
| **IV 执行引擎** | M14–M16 | CEP 状态机 + RuleExecutor | 已完成 |
| **V 运行时闭环** | M17–M20 | **单机 MVP** | 已完成 |
| **VI 生产化** | M21–M24 | wfl + wfgen + lint + fmt | 已完成 |

**计划阶段：**

| 阶段 | 里程碑 | 阶段目标 |
|------|--------|---------|
| **VII L2 增强** | M25–M26 | join snapshot/asof（DataFusion）、条件表达式增强 |
| **VIII 正确性门禁** | M27–M28 | contract + shuffle + scenario verify |
| **IX 可靠性分级** | M29 | at_least_once / exactly_once 传输 |
| **X 分布式** | M30 | 多节点分布式部署 |


## 10. 风险与约束

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 窗口数据量超内存 | OOM | max_window_bytes + max_total_bytes 硬限 + Evictor 两阶段淘汰（时间 + 内存） |
| Arrow IPC 传输断连 | 断连期间数据丢失 | TCP 可靠传输 + 指数退避重连；L1 为 best-effort |
| 规则状态机实例膨胀 | 高基数 scope-key 导致内存增长 | W003 lint 警告（≥4 key 字段）；L2 规划 limits.max_instances |
| cursor 落后于 eviction | RuleTask 丢失历史数据 | read_since 返回 gap_detected=true，记录日志继续处理 |
| CEP 状态机未做 scope-key 限制 | 恶意/异常数据产生大量实例 | L2 规划 limits 资源预算执行 |
| DataFusion 引入后的性能开销 | L2 join 执行时延迟增加 | L2 阶段 benchmark 验证；复用 SessionContext 策略 |
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

**每个节点独立执行关联，零跨节点通信**。三个表在本地都有 Rule 所需的数据：
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
| **V1** | 单机 | CEP 状态机 + score/entity 求值，所有 Window 为 local | 低（**L1 已实现**） |
| **V2** | 按 key 分区，多实例 | 等值 JOIN + 本地聚合（需 L2 join 实现） | 中 |
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
