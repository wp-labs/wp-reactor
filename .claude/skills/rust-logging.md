# rust-logging

在 WarpFusion 代码中添加、审查或修改日志记录，遵循 `docs/design/logging-spec.md` 规范。

## 输入

用户可能提供：
- 要添加日志的模块或函数
- 要审查现有日志的文件
- 新功能需要规划日志埋点
- "日志级别不对" 或 "缺少关键日志"

## 前置知识

先阅读日志规范：

```bash
cat docs/design/logging-spec.md
```

核心规则速查：

### 五个关注域

| 域 | 用途 |
|---|------|
| `sys` | 启动、停止、信号、task group |
| `conn` | TCP accept/close、读写错误 |
| `pipe` | batch 路由、rule match、alert emit |
| `res` | eviction、内存、channel 背压 |
| `conf` | 文件加载、编译、校验 |

### 级别判断

| 级别 | 判断标准 |
|------|---------|
| ERROR | 需要人立即介入，影响正确性（如 alert 写盘失败） |
| WARN | 系统自愈但需关注（如单次 timeout、decode 失败） |
| INFO | 生产常态，**必须与吞吐量无关**（如启动/停止） |
| DEBUG | 排查用，可与吞吐量相关（如连接、batch 摘要） |
| TRACE | 仅限本地开发（如逐事件状态机步进） |

### 域宏

在 `wf-runtime` 中使用 `wf_*!` 宏（定义在 `crates/wf-runtime/src/log_macros.rs`）：

```rust
wf_info!(sys, listen = %addr, "engine started");
wf_warn!(pipe, error = %e, "execute_match error");
wf_debug!(conn, peer = %peer, "accepted connection");
wf_debug!(res, scanned = n, "evictor sweep");
wf_error!(pipe, error = %e, "alert sink write failed");
```

宏自动注入 `domain` 字段。在 `wf-runtime` 之外的 crate 直接使用 `tracing::` 并手动加 `domain`：

```rust
tracing::info!(domain = "conf", schemas = n, "loaded schemas");
```

## 工作流程

### 场景 A：为新功能添加日志

#### 第一步：确定关注域

根据功能归属选择域。如果不确定，问自己："谁需要看这条日志？"

- 运维需要看 → `sys` / `conn` / `res`
- 开发需要看 → `pipe` / `conf`

#### 第二步：确定级别

逐条检查：

1. 这条日志在生产环境是否**始终需要**？→ INFO 或更高
2. 日志量是否**与吞吐量成正比**？→ 不能是 INFO，降为 DEBUG
3. 系统能否自愈？→ 能 = WARN，不能 = ERROR
4. 是否仅在排查时有用？→ DEBUG
5. 是否逐事件/逐帧级别？→ TRACE

#### 第三步：选择结构化字段

查阅 `docs/design/logging-spec.md` §3 的标准字段表。使用规范定义的字段名，不要自造。

#### 第四步：编写日志

```rust
// 在 wf-runtime 中
wf_info!(sys, task_group = "receiver", "task group finished");

// 在其他 crate 中
tracing::warn!(domain = "pipe", error = %e, "rule compilation failed");
```

#### 第五步：验证

```bash
cargo check -p <crate> 2>&1
cargo test -p <crate> 2>&1
```

### 场景 B：审查现有日志

#### 第一步：扫描当前日志

```bash
# 查找所有日志调用
grep -rn 'wf_\(info\|warn\|debug\|error\|trace\)!' crates/<crate>/src/ --include="*.rs"
grep -rn 'tracing::\(info\|warn\|debug\|error\|trace\)!' crates/<crate>/src/ --include="*.rs"

# 检查是否有遗留的 log:: 调用
grep -rn 'log::' crates/<crate>/src/ --include="*.rs"

# 检查是否有 println/eprintln
grep -rn 'println!\|eprintln!' crates/<crate>/src/ --include="*.rs"
```

#### 第二步：逐条审查

对每条日志检查：

- [ ] 域是否正确（五个域之一）
- [ ] 级别是否符合规范（特别关注 INFO 是否与吞吐量无关）
- [ ] 字段名是否使用标准命名
- [ ] 消息中是否重复了字段值（应使用结构化字段）
- [ ] 错误是否用 `%e`（Display）而非 `?e`（Debug）

#### 第三步：输出审查报告

列出发现的问题，按严重程度排序：

1. **级别错误**：与吞吐量相关的 INFO 日志
2. **域缺失**：未标注 domain 的 tracing 调用
3. **字段不规范**：自造字段名
4. **遗留 API**：`log::` 或 `println!`

## 标准字段速查

| 字段 | 类型 | 域 |
|------|------|-----|
| `domain` | `&str` | 所有（宏自动注入） |
| `peer` | `Display` | conn |
| `stream` | `&str` | pipe |
| `rule` | `&str` | pipe |
| `rows` | `usize` | pipe |
| `alerts` | `usize` | pipe |
| `task_group` | `&str` | sys |
| `listen` | `Display` | sys |
| `signal` | `&str` | sys |
| `scanned` | `usize` | res |
| `time_evicted` | `usize` | res |
| `memory_evicted` | `usize` | res |
| `error` | `Display` | 通用，用 `%e` |
| `duration_ms` | `u64` | 通用 |
| `timeout` | `Debug` | 通用，用 `?dur` |

## 反模式

| 反模式 | 问题 | 改正 |
|--------|------|------|
| `tracing::info!("got {n} events")` | INFO 与吞吐量相关 | 降为 DEBUG |
| `wf_info!(pipe, "error: {e}")` | 消息中内嵌错误 | `error = %e` 字段 |
| `tracing::warn!(...)` 无 domain | 缺少域标注 | 用 `wf_warn!` 宏 |
| `log::info!(...)` | 遗留 API | 改用 `wf_info!` |
| `wf_warn!(pipe, err = %e, ...)` | 字段名 `err` 非标准 | 用 `error` |
| `eprintln!("debug: ...")` | 绕过日志系统 | 用 `wf_debug!` |
