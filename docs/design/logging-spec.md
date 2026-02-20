# WarpFusion 日志规范

本文档定义 WarpFusion 项目的日志分类、分级和字段规范。所有 `wf-*` crate 中的日志记录必须遵循本规范。

## 1. 关注域（Domain）

日志按 **可观测性关注域** 分类，而非按 Rust 模块路径。每条日志必须归属一个域。

| 域标识 | 含义 | 关注者 | 典型事件 |
|--------|------|--------|---------|
| `sys` | 进程生命周期 | 运维 / SRE | 启动、停止、信号处理、task group 状态 |
| `conn` | 网络连接管理 | 运维 / SRE | TCP accept/close、读写错误、连接数变化 |
| `pipe` | 数据处理路径 | 运维 + 开发 | batch 到达、路由、rule match、alert emit |
| `res` | 资源使用与回收 | 运维 / SRE | eviction、内存压力、channel 背压 |
| `conf` | 配置与编译 | 开发 | 文件加载、schema/rule 编译、校验结果 |

### 代码中的域标注

通过 `domain` 字段标注（由 `wf_log!` 系列宏自动注入）：

```rust
// 宏展开后等价于：
tracing::info!(domain = "sys", schemas = 1, rules = 2, "engine bootstrap complete");
```

`tracing` 默认的 `target` 字段仍为 Rust module path（`wf_runtime::lifecycle`），用于开发时按模块过滤（`RUST_LOG=wf_runtime::receiver=debug`）。`domain` 字段用于生产环境日志聚合系统按关注域过滤。

## 2. 级别语义

级别按 **受众 × 场景** 定义，不按主观严重程度。

### ERROR — 需要人立即介入

- **受众**：On-call 值班
- **开启场景**：永远开启
- **判断标准**：系统无法自愈，影响数据正确性或可用性
- **日志量**：极少，每个 ERROR 都应该触发告警

```
✓ alert sink 写盘失败（告警数据丢失）
✓ schema/rule 编译失败导致引擎无法启动
✗ 单次 IPC decode 失败 → 这是 WARN
```

### WARN — 系统自愈但需关注

- **受众**：运维团队
- **开启场景**：永远开启
- **判断标准**：系统自动恢复了，但持续出现说明有问题
- **日志量**：低频；持续高频 WARN 本身就是需要修复的问题

```
✓ execute_match error（单次失败，引擎继续运行）
✓ engine dispatch timeout（超时取消，不阻塞后续 batch）
✓ route error（单条数据路由失败）
✓ connection read error（连接异常断开）
✗ 正常连接关闭 → 这是 DEBUG
```

### INFO — 生产常态可见性

- **受众**：运维团队
- **开启场景**：生产环境常驻
- **判断标准**：引擎运行的关键状态变更
- **日志量约束**：**必须与业务吞吐量无关**。每秒处理 1M 事件和 1K 事件时，INFO 日志行数应相同。

```
✓ 引擎启动 / 停止（一次性事件）
✓ 配置摘要：schema 数、rule 数、window 数
✓ 收到 shutdown 信号
✗ accepted connection → 与连接数成正比，应为 DEBUG
✗ connection closed → 同上
✗ 每个 batch 的处理结果 → 与吞吐量成正比，应为 DEBUG
```

### DEBUG — 排查时临时开启

- **受众**：开发者
- **开启场景**：排查问题时通过 `RUST_LOG` 或 `[logging.modules]` 开启
- **判断标准**：定位问题需要的上下文，但量太大不适合常驻

```
✓ accepted connection / connection closed
✓ task group 等待 / 完成
✓ evictor sweep 结果
✓ dispatch_batch 耗时
✓ rule match/miss 的 per-batch 摘要
✓ channel 水位采样
```

### TRACE — 仅限本地开发

- **受众**：开发者
- **开启场景**：本地开发 / 单元测试，生产环境**绝不开启**
- **判断标准**：逐事件、逐帧级别的细节

```
✓ 每条 event 的 CEP 状态机步进（advance/match/skip）
✓ 每个 IPC frame 的 decode 细节（stream_name, num_rows, bytes）
✓ EnvFilter 指令解析细节
```

## 3. 标准字段命名

相同语义的字段在所有日志中使用相同名称。

### 必选字段

| 字段 | 类型 | 语义 | 示例 |
|------|------|------|------|
| `domain` | `&str` | 关注域 | `"sys"`, `"pipe"` |

### 按域字段

#### sys

| 字段 | 类型 | 语义 |
|------|------|------|
| `task_group` | `&str` | task group 名称 |
| `listen` | `Display` | 监听地址 |
| `signal` | `&str` | 信号名（`"SIGINT"`, `"SIGTERM"`） |

#### conn

| 字段 | 类型 | 语义 |
|------|------|------|
| `peer` | `Display` | 远端 socket 地址 |

#### pipe

| 字段 | 类型 | 语义 |
|------|------|------|
| `stream` | `&str` | 数据流名称 |
| `rule` | `&str` | 规则名称 |
| `rows` | `usize` | batch 行数 |
| `alerts` | `usize` | 本次产出告警数 |

#### res

| 字段 | 类型 | 语义 |
|------|------|------|
| `scanned` | `usize` | 扫描的 window 数 |
| `time_evicted` | `usize` | 按时间淘汰的 batch 数 |
| `memory_evicted` | `usize` | 按内存淘汰的 batch 数 |

#### 通用字段

| 字段 | 类型 | 语义 | 说明 |
|------|------|------|------|
| `error` | `Display` | 错误信息 | 始终用 `%e` 格式 |
| `duration_ms` | `u64` | 操作耗时（毫秒） | 数值类型，便于聚合 |
| `timeout` | `Debug` | 超时时长 | 用 `?` 格式显示 Duration |
| `count` | `usize` | 通用计数 | 仅在无更具体字段时使用 |

## 4. Span 命名

`#[tracing::instrument]` 的 span 名使用 `模块.动作` 格式：

| span 名 | 附加字段 | 所在函数 |
|---------|---------|---------|
| `engine.start` | `listen` | `FusionEngine::start()` |
| `receiver` | — | `Receiver::run()` |
| `handle_connection` | `peer` | `handle_connection()` |
| `scheduler` | — | `Scheduler::run()` |
| `alert_sink` | — | `run_alert_sink()` |
| `evictor` | — | `run_evictor()` |

## 5. 域宏使用

`wf-runtime` 提供 `wf_log!` 系列便捷宏，自动注入 `domain` 字段：

```rust
use crate::log_macros::*;

// sys 域
wf_info!(sys, task_group = "receiver", "task group finished");
wf_debug!(sys, task_group = name, "waiting for task group");

// conn 域
wf_debug!(conn, peer = %peer, "accepted connection");
wf_warn!(conn, peer = %peer, error = %e, "connection read error");

// pipe 域
wf_warn!(pipe, error = %e, "execute_match error");
wf_warn!(pipe, timeout = ?exec_timeout, "dispatch_batch engine timed out");

// res 域
wf_debug!(res, scanned = report.windows_scanned, "evictor sweep");

// conf 域
wf_info!(conf, schemas = n, rules = m, "loaded schema and rule files");
```

宏展开后等价于：

```rust
tracing::info!(domain = "sys", task_group = "receiver", "task group finished");
```

## 6. 禁止事项

1. **禁止在 INFO 级别记录与吞吐量成正比的事件**（连接、batch、event）
2. **禁止在日志消息中重复字段值**：用结构化字段而非插值
   ```rust
   // ✗ 消息中重复了 peer
   tracing::info!("accepted connection from {peer}");
   // ✓ peer 作为字段
   wf_debug!(conn, peer = %peer, "accepted connection");
   ```
3. **禁止使用 `log::` 宏**：全部使用 `tracing::` 或 `wf_log!` 系列宏
4. **禁止自定义 `domain` 值**：只使用本文档定义的五个域
5. **禁止在非测试代码中使用 `println!` / `eprintln!`**

## 7. 扩展域的流程

如需新增关注域：

1. 在本文档 §1 表格中添加域定义
2. 在 `log_macros.rs` 中确认域标识已被宏覆盖（宏支持任意域标识，无需修改代码）
3. 更新 `.claude/skills/rust-logging.md` 中的域列表
4. 提交 PR 并在 commit message 中注明新域的理由
