# wp-reactor 错误处理设计

> 基于 orion-error 0.6.0，结合 wp-motor 实战经验

---

## 一、现状与动机

### 1.1 当前错误处理

wp-reactor 当前以 `anyhow::Result<T>` 作为唯一错误类型，贯穿所有 6 个 crate。

**三种错误模式并存**:

| 模式 | 用途 | 示例 |
|------|------|------|
| `Result<T, anyhow::Error>` | 首个错误即终止 | 配置加载、解析、编译 |
| `Vec<XxxError>` | 收集全部错误再返回 | `check_wfl()`, `validate_wfg()` |
| log + continue | 运行时吞掉错误继续处理 | 连接断开、执行超时、通道关闭 |

**自定义错误类型**（均为简单 struct，未接入统一框架）:

| 类型 | 位置 | 字段 |
|------|------|------|
| `CheckError` | `wf-lang/src/checker/mod.rs:11-15` | `rule`, `contract`, `message` |
| `PreprocessError` | `wf-lang/src/preprocess.rs` | `position`, `message` |
| `ValidationError` | `wfgen/src/validate.rs` | `code`, `message` |

### 1.2 为什么引入 orion-error

**1. 启动阶段错误信息不够结构化**

`lifecycle.rs:91-105` 中，多种不同性质的错误全部打平为 `anyhow::Error`，上层无法区分错误类别：

```rust
// 三种完全不同性质的错误表现为相同的 anyhow::Error
let content = std::fs::read_to_string(full_path)
    .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", full_path.display()))?;
let preprocessed = wf_lang::preprocess_vars(&raw, &config.vars)
    .map_err(|e| anyhow::anyhow!("preprocess error in {}: {e}", full_path.display()))?;
let wfl_file = wf_lang::parse_wfl(&preprocessed)?;
```

**2. 操作上下文手工拼接**

`map_err` 手动拼接上下文字符串，格式不一致。orion-error 的 `OperationContext` 提供统一的上下文记录。

**3. 无错误码体系**

没有可枚举的错误类别和错误码。orion-error 0.6.0 的 `ErrorCode` trait 和 `UvsReason` 的分层编码可直接复用。

**4. 运行时错误无法按策略处理**

wp-motor 验证了一个关键场景：reason 类型化不只是日志装饰，而是**运行时控制流的一部分**。wp-motor 基于 `StructError` 的 reason 实现了三模式错误策略（Debug/Normal/Strict），不同模式对同一错误返回不同处理策略。wp-reactor 当前的 log+continue 模式是硬编码的，无法按部署模式灵活调整。

---

## 二、设计原则

### 2.1 不是全量替换

错误处理方案应匹配错误的生命周期：

| 错误生命周期 | 适用方案 | 理由 |
|-------------|---------|------|
| 产生后立即被 match 或上报 | `StructError<R>` | 需要类型化的 reason 做区分 |
| 产生后被收集到 Vec 中 | 保持现有 `Vec<CheckError>` | 非 Result 模式，orion-error 不适用 |
| 产生后立即 log + continue | 保持 `anyhow` 或直接 log | 错误被吞掉，不需要类型化 |

### 2.2 边界转换原则

orion-error 的 `ErrorOwe` / `ErrorConv` 应用在 **crate 边界**，而非每个函数内部：

```
wf-lang (anyhow) ──owe──▶ wf-runtime (StructError<RuntimeReason>)
wf-config (anyhow) ──owe──▶ wf-runtime (StructError<RuntimeReason>)
wf-core (StructError<CoreReason>) ──err_conv──▶ wf-runtime (StructError<RuntimeReason>)
```

### 2.3 运行时循环——当前 log+continue，预留策略扩展

`scheduler.rs` 和 `receiver.rs` 内部的错误处理当前是 log + continue 模式。引入 StructError 后，wf-core 的公共 API 返回类型化错误，调用侧可按需扩展为策略化处理（参考 wp-motor 的 ErrorHandlingPolicy 模式），但**初始实现保持 log+continue**。

---

## 三、分层设计

### 3.1 总体架构

```
                     ┌─────────────┐
                     │  wf-engine  │  直接使用 RuntimeReason
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │ wf-runtime  │  定义 RuntimeReason（收敛层）
                     └──┬───┬───┬──┘
                        │   │   │
              ┌─────────┘   │   └─────────┐
              │             │             │
       ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
       │  wf-config  │ │wf-lang │ │   wf-core   │
       │  (anyhow)   │ │(anyhow)│ │ CoreReason  │
       └─────────────┘ └────────┘ └─────────────┘
```

### 3.2 各 crate 职责

| Crate | 错误方案 | DomainReason | 理由 |
|-------|---------|-------------|------|
| **wf-lang** | 保持 `anyhow` + 现有自定义类型 | 不定义 | 解析库，错误是位置+消息，不需要分类 |
| **wf-config** | 保持 `anyhow` | 不定义 | 配置错误全部 fatal，不需要分类 |
| **wf-core** | 引入 `StructError<CoreReason>` | `CoreReason` | 业务层，错误需要分类（窗口/规则/数据） |
| **wf-runtime** | 引入 `StructError<RuntimeReason>` | `RuntimeReason` | 收敛层，统一下层错误 + 自有运行时错误 |
| **wf-engine** | 直接使用 `RuntimeReason` | 不定义 | 入口，格式化输出即可 |
| **wfgen** | 保持 `anyhow` + `ValidationError` | 不定义 | 独立工具，不需要 |

---

## 四、DomainReason 定义

### 4.1 CoreReason（wf-core）

```rust
// wf-core/src/error.rs

use orion_error::{UvsReason, ErrorCode, StructError};
use derive_more::From;

#[derive(Debug, Clone, PartialEq, thiserror::Error, From)]
pub enum CoreReason {
    /// 窗口构建错误（重复窗口名、参数无效）
    #[error("window build error")]
    WindowBuild,

    /// 规则执行错误（match/close 阶段）
    #[error("rule execution error")]
    RuleExec,

    /// 数据格式错误（Arrow IPC 解码、schema 不匹配）
    #[error("data format error")]
    DataFormat,

    /// 统一收敛
    #[error("{0}")]
    Uvs(UvsReason),
}

impl ErrorCode for CoreReason {
    fn error_code(&self) -> i32 {
        match self {
            Self::WindowBuild => 1001,
            Self::RuleExec    => 1002,
            Self::DataFormat  => 1004,
            Self::Uvs(u)      => u.error_code(),
        }
    }
}

/// 类型别名，简化函数签名（参考 wp-motor 惯例）
pub type CoreError = StructError<CoreReason>;
pub type CoreResult<T> = Result<T, CoreError>;
```

**设计依据**:

- `WindowBuild` — 对应 `WindowRegistry::build()` 中 `bail!("duplicate window name")`
- `RuleExec` — 对应 `RuleExecutor::execute_match/close()` 的错误路径
- `DataFormat` — 对应 `wp_arrow::ipc::decode_ipc()` 的解码错误
- `Uvs(UvsReason)` — 启用 `ErrorOwe` 的 `.owe_sys()` / `.owe_data()` 等便捷方法。wp-motor 验证了 Uvs 变体的核心价值：错误策略 match 中，域特定变体先匹配，兜底通过 Uvs 委托给通用策略

### 4.2 RuntimeReason（wf-runtime）

```rust
// wf-runtime/src/error.rs

use orion_error::{UvsReason, ErrorCode, StructError};
use derive_more::From;
use wf_core::error::CoreReason;

#[derive(Debug, Clone, PartialEq, thiserror::Error, From)]
pub enum RuntimeReason {
    /// 引擎启动失败（配置加载、schema/rule 编译、资源创建）
    #[error("bootstrap error")]
    Bootstrap,

    /// 优雅关闭失败（任务 join 错误）
    #[error("shutdown error")]
    Shutdown,

    /// wf-core 错误收敛（保留原始分类，不丢失信息）
    #[error("{0}")]
    Core(CoreReason),

    /// 统一收敛
    #[error("{0}")]
    Uvs(UvsReason),
}

impl ErrorCode for RuntimeReason {
    fn error_code(&self) -> i32 {
        match self {
            Self::Bootstrap => 2001,
            Self::Shutdown  => 2002,
            Self::Core(c)   => c.error_code(),
            Self::Uvs(u)    => u.error_code(),
        }
    }
}

pub type RuntimeError = StructError<RuntimeReason>;
pub type RuntimeResult<T> = Result<T, RuntimeError>;
```

**Core(CoreReason) 的设计理由**:

wp-motor 中 `OMLCodeReason → RunReason` 的转换将所有 OML 错误映射为 `UvsReason::ConfigError`，丢失了原始分类。wp-reactor 通过 `Core(CoreReason)` 包装变体保留完整的错误链，上层可以穿透 match 到具体的 CoreReason 变体。

### 4.3 错误码规划

```
  错误码范围          含义
  ─────────────────────────────────
  100-199            UvsReason 业务层（validation, business, not_found...）
  200-299            UvsReason 基础设施层（data, system, network, timeout...）
  300-399            UvsReason 配置/外部层（config, external）

  1001-1099          CoreReason（窗口、规则、数据）
  2001-2099          RuntimeReason（启动、关闭）
```

---

## 五、错误流转

### 5.1 启动路径（lifecycle.rs）

引入 orion-error 收益最大的路径。结合 wp-motor 的 OperationContext RAII 模式。

**改造后**:

```rust
use orion_error::prelude::*;

pub async fn start(config: FusionConfig, base_dir: &Path)
    -> RuntimeResult<Self>
{
    // OperationContext RAII 日志：函数结束时自动 log，? 提前返回也不遗漏
    let mut op = op_context!("engine-bootstrap").with_auto_log();
    op.record("listen", config.server.listen.as_str());
    op.record("base_dir", &base_dir.display().to_string());

    let cancel = CancellationToken::new();

    // 1. 加载 .wfs 文件
    let wfs_paths = resolve_glob(&config.runtime.schemas, base_dir)
        .owe_conf()?;

    let mut all_schemas = Vec::new();
    for full_path in &wfs_paths {
        let content = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())
            .with(&op)?;
        let schemas = wf_lang::parse_wfs(&content)
            .owe(RuntimeReason::Bootstrap)
            .position(full_path.display().to_string())?;
        all_schemas.extend(schemas);
    }

    // 2. 预处理 + 解析 .wfl 文件
    let wfl_paths = resolve_glob(&config.runtime.rules, base_dir)
        .owe_conf()?;
    let mut all_rule_plans = Vec::new();
    for full_path in &wfl_paths {
        let raw = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())
            .with(&op)?;
        let preprocessed = wf_lang::preprocess_vars(&raw, &config.vars)
            .owe_data()
            .position(full_path.display().to_string())?;
        let wfl_file = wf_lang::parse_wfl(&preprocessed)
            .owe(RuntimeReason::Bootstrap)
            .position(full_path.display().to_string())?;
        let plans = wf_lang::compile_wfl(&wfl_file, &all_schemas)
            .owe(RuntimeReason::Bootstrap)?;
        all_rule_plans.extend(plans);
    }

    // 3. 配置校验
    wf_config::validate_over_vs_over_cap(&config.windows, &window_overs)
        .owe_conf()?;

    // 4. 构建窗口注册表（StructError<CoreReason> → StructError<RuntimeReason>）
    let registry = WindowRegistry::build(window_defs)
        .err_conv()?;

    // 5. 构建 sink dispatcher
    let dispatcher = build_sink_dispatcher(&bundle, &registry, &work_root)
        .await
        .owe(RuntimeReason::Bootstrap)?;

    // ... 启动任务组 ...

    op.mark_suc();
    Ok(Self { cancel, groups, listen_addr, cmd_tx })
}
```

**OperationContext 使用模式**（参考 wp-motor loader.rs、build_sinks.rs）:

```rust
// 模式：want + with_auto_log + record + mark_suc
let mut op = op_context!("操作名").with_auto_log();
op.record("key", "value");
// ... 执行操作，? 可能提前返回 ...
// → 如果 ? 返回，op drop 时 exit_log=true 且 result=Fail，自动 log error
// → 如果成功，mark_suc() 后 op drop 时 log info
op.mark_suc();
```

### 5.2 wf-core 内部（公共 API 改造）

**WindowRegistry::build()**

```rust
pub fn build(defs: Vec<WindowDef>) -> CoreResult<Self> {
    for def in &defs {
        if windows.contains_key(&name) {
            return StructError::from(CoreReason::WindowBuild)
                .with_detail(format!("duplicate window name: {:?}", name))
                .err();
        }
    }
    // ...
}
```

**RuleExecutor::execute_match/close()**

```rust
pub fn execute_match(&self, ctx: &MatchContext) -> CoreResult<AlertRecord> {
    // ...内部错误用 .owe(CoreReason::RuleExec) 包装
}
```

### 5.3 运行时循环（保持 log+continue，类型化预留策略扩展）

`scheduler.rs` 和 `receiver.rs` 的内部循环保持现有模式。但因为 wf-core API 现在返回 `CoreResult<T>`，调用侧拿到的是 `StructError<CoreReason>`，可以在日志中输出结构化信息，未来也可扩展为策略化处理：

```rust
// scheduler.rs — 当前：log + continue
match core.executor.execute_match(&ctx) {
    Ok(record) => alerts.push(record),
    Err(e) => {
        // 现在 e 是 StructError<CoreReason>，日志自动包含 error_code 和 reason
        wf_warn!(pipe, error = %e, code = e.error_code(), "execute_match error");
    }
}

// receiver.rs — 当前：log + break
Err(e) => {
    wf_warn!(conn, error = %e, "connection read error");
    break;
}
```

**未来策略化扩展方向**（参考 wp-motor ErrorHandlingPolicy）:

当 wp-reactor 需要区分 Debug/Normal/Strict 模式时，可以基于 `CoreReason` 的 match 来决定 Ignore/Throw/Retry。wp-motor 验证了这个模式：

```rust
// 未来可选——错误策略 trait（参考 wp-motor）
// match err.reason() {
//     CoreReason::RuleExec => ErrStrategy::Ignore,   // 规则执行错误可忽略
//     CoreReason::DataFormat => ErrStrategy::Ignore,  // 数据格式错误可忽略
//     CoreReason::Uvs(u) => universal_strategy(u),    // Uvs 穿透到通用策略
//     _ => ErrStrategy::Throw,
// }
```

### 5.4 CLI 入口

```rust
// main.rs
#[tokio::main]
async fn main() -> Result<()> {
    // ...
    let engine = Reactor::start(fusion_config, base_dir).await
        .map_err(|e| {
            // StructError 的 Display 自动输出 [error_code] reason + position + detail + context
            anyhow::anyhow!("{e}")
        })?;
    // ...
}
```

---

## 六、OperationContext 使用

### 6.1 与 tracing 的关系

wp-reactor 使用 tracing。orion-error 0.6.0 的 tracing feature 已完整实现，OperationContext Drop 时使用 `tracing::info!/error!/warn!` 输出。

**分工**:

| 机制 | 用途 |
|------|------|
| tracing `#[instrument]` | 函数级 span 追踪（已有，不改） |
| `OperationContext` with_auto_log | 操作级 RAII 日志（成功/失败/取消自动记录） |
| `OperationContext` 作为 StructError 上下文 | 错误上下文栈（附加到 StructError，跟随错误传播） |

orion-error 0.6.0 的 tracing 分支已实现结构化输出：

```rust
// Drop 时自动输出（orion-error 0.6.0 context.rs:67-93）
tracing::info!(target: "domain", mod_path = %self.mod_path, "suc! {ctx}");
tracing::error!(target: "domain", mod_path = %self.mod_path, "fail! {ctx}");
```

### 6.2 典型用法

**启动路径——RAII 日志 + 错误上下文**（参考 wp-motor loader.rs 模式）:

```rust
use orion_error::{op_context, ContextRecord, ErrorOwe, ErrorWith};

pub async fn start(config: FusionConfig, base_dir: &Path) -> RuntimeResult<Self> {
    // op_context! 宏在调用处展开 module_path!()，mod_path 指向正确模块
    let mut op = op_context!("engine-bootstrap").with_auto_log();
    op.record("base_dir", &base_dir.display().to_string());
    op.record("schema_count", wfs_paths.len().to_string());

    for full_path in &wfs_paths {
        let content = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())
            .with(&op)?;  // 错误携带 "engine-bootstrap" 上下文
        // ...
    }

    op.mark_suc();  // 只在成功时 mark，? 提前返回则 Drop 自动 log error
    Ok(...)
}
```

**构建资源——独立操作上下文**（参考 wp-motor build_sinks.rs 模式）:

```rust
async fn build_sink_dispatcher(config: &FusionConfig, base_dir: &Path) -> RuntimeResult<Arc<SinkDispatcher>> {
    let mut op = op_context!("build-sink-dispatcher").with_auto_log();
    let sinks_dir = base_dir.join(&config.sinks);
    let bundle = wf_config::sink::load_sink_config(&sinks_dir).owe_conf()?;
    op.record("sinks_dir", &sinks_dir.display().to_string());
    // ...
    op.mark_suc();
    Ok(...)
}
```

---

## 七、模式与约定

### 7.1 边界转换三板斧

```rust
// 1. 外部 error → StructError 便捷分类（ErrorOwe，需要 Uvs 变体）
std::fs::read_to_string(path).owe_sys()?;     // IO 错误 → UvsReason::SystemError
toml::from_str(s).owe_conf()?;                // 配置解析 → UvsReason::ConfigError
serde_json::to_string(v).owe_data()?;         // 数据序列化 → UvsReason::DataError
tcp_listener.accept().await.owe_net()?;        // 网络 → UvsReason::NetworkError

// 2. 外部 error → StructError 自定义 reason（ErrorOweBase，不需要 Uvs 变体）
wf_lang::parse_wfl(input).owe(RuntimeReason::Bootstrap)?;

// 3. StructError<R1> → StructError<R2>（ErrorConv）
WindowRegistry::build(defs).err_conv()?;       // CoreReason → RuntimeReason
```

**0.6.0 改进**: `ErrorOweBase` 的 `.owe(reason)` 不再被 `From<UvsReason>` 约束牵连。不需要 Uvs 收敛的 reason 类型也可以使用 `.owe()`，无需像 wp-motor 那样自定义 `RunErrorOwe` 之类的 workaround。

### 7.2 position 使用约定

`position` 字段记录错误**发生的位置**：

```rust
// 文件路径
.position(full_path.display().to_string())

// 数据位置（行号）
.position(format!("row {}", row_index))

// 源码位置（调试用）
.position(orion_error::location!())
```

### 7.3 哪些错误用哪个 owe

| 原始错误来源 | owe 方法 | 映射到 UvsReason |
|-------------|---------|-----------------|
| `std::io::Error` (文件读写) | `.owe_sys()` | `SystemError` (201) |
| `std::io::Error` (网络) | `.owe_net()` | `NetworkError` (202) |
| `toml::de::Error` | `.owe_conf()` | `ConfigError` (300) |
| `serde_json` 错误 | `.owe_data()` | `DataError` (200) |
| `arrow` 错误 | `.owe_data()` | `DataError` (200) |
| wf-lang 解析错误 | `.owe(RuntimeReason::Bootstrap)` | 自定义 reason |
| 业务逻辑错误 | `.owe_logic()` | `LogicError` (104) |
| 超时 | `.owe_timeout()` | `TimeoutError` (204) |

**0.6.0 改进**: `UvsReason` 变体不再携带 String 参数，消息统一存在 `detail` 字段。Display 输出不再重复。

### 7.4 类型别名约定

所有定义 DomainReason 的 crate 统一提供类型别名（参考 wp-motor 惯例）：

```rust
// wf-core/src/error.rs
pub type CoreError = StructError<CoreReason>;
pub type CoreResult<T> = Result<T, CoreError>;

// wf-runtime/src/error.rs
pub type RuntimeError = StructError<RuntimeReason>;
pub type RuntimeResult<T> = Result<T, RuntimeError>;
```

### 7.5 不使用 StructError 的场景

以下场景保持现有方案：

- `scheduler.rs` 中的 `execute_match` / `execute_close` 错误 → log warn + continue
- `receiver.rs` 中的连接/解码错误 → log warn + break/continue
- `check_wfl()` 返回 `Vec<CheckError>` → 收集模式不变
- `validate_wfg()` 返回 `Vec<ValidationError>` → 收集模式不变
- wfgen 全部 → 独立工具，保持 anyhow

---

## 八、依赖配置

### 8.1 wf-core/Cargo.toml

```toml
[dependencies]
orion-error = { version = "0.6", features = ["tracing"] }
# ...现有依赖不变
```

### 8.2 wf-runtime/Cargo.toml

```toml
[dependencies]
orion-error = { version = "0.6", features = ["tracing"] }
# ...现有依赖不变
```

使用 `tracing` feature 而非默认的 `log`，与 wp-reactor 现有的 tracing 集成对齐。

### 8.3 不引入 orion-error 的 crate

wf-lang、wf-config、wf-engine、wfgen **不**添加 orion-error 依赖。

wf-engine 通过 wf-runtime 的 re-export 使用 `StructError<RuntimeReason>`。

---

## 九、迁移路径

### 阶段 1: 定义 DomainReason

- 在 `wf-core` 新建 `src/error.rs`，定义 `CoreReason` + `CoreError` + `CoreResult`
- 在 `wf-runtime` 新建 `src/error.rs`，定义 `RuntimeReason` + `RuntimeError` + `RuntimeResult`
- 添加 orion-error 0.6 依赖（features = ["tracing"]）
- **不改任何现有代码**，仅新增文件

### 阶段 2: 改造 wf-core 公共 API

- `AlertSink::send()` → `CoreResult<()>`（已移除，告警输出通过 SinkDispatcher 异步处理）
- `WindowRegistry::build()` → `CoreResult<Self>`
- `RuleExecutor::execute_match/close()` → `CoreResult<T>`
- 内部实现用 `.owe()` / `.owe_xxx()` 包装外部错误
- **rule_task/receiver 内部的 log + continue 不改**，但日志输出可利用 StructError 的结构化信息

### 阶段 3: 改造 wf-runtime lifecycle

- `Reactor::start()` → `RuntimeResult<Self>`
- `Reactor::wait()` → `RuntimeResult<()>`
- 对 wf-lang/wf-config 的 anyhow 错误用 `.owe_xxx()` / `.owe()` 转换
- 对 wf-core 的 StructError 用 `.err_conv()` 转换
- 添加 `OperationContext` RAII 日志（`op_context!` + `with_auto_log` + `record` + `mark_suc`）

### 阶段 4: 适配 wf-engine

- `main()` 处理 `StructError<RuntimeReason>` 的格式化输出

---

## 十、与 wp-motor 设计的对比

### 10.1 采纳的模式

| 模式 | wp-motor 来源 | wp-reactor 应用 |
|------|-------------|----------------|
| 类型别名 | `RunError` / `RunResult<T>` | `CoreError` / `CoreResult<T>` / `RuntimeError` / `RuntimeResult<T>` |
| OperationContext RAII | `want() + with_auto_log() + record() + mark_suc()` | 启动路径和资源构建函数 |
| Uvs 穿透 | reason match 先匹配域变体，`Uvs(e)` 兜底委托通用策略 | CoreReason/RuntimeReason 的 Uvs 变体保持穿透能力 |
| Core 包装变体 | — | `RuntimeReason::Core(CoreReason)` 保留完整错误链，不丢失信息 |
| 策略化处理预留 | `ErrorHandlingPolicy` trait + Debug/Normal/Strict 三模式 | 当前 log+continue，reason 类型化为未来策略扩展做准备 |

### 10.2 未采纳的模式

| 模式 | wp-motor 做法 | wp-reactor 不采纳理由 |
|------|-------------|---------------------|
| DomainReason 集中到独立 crate | wp-error 独立仓库 | wp-reactor crate 间依赖简单，每 crate 定义自己的 reason 更清晰 |
| 自定义 owe trait | `RunErrorOwe` (owe_sink/owe_source) | 0.6.0 的 `ErrorOweBase.owe(reason)` 不再受 `From<UvsReason>` 约束，不需要 workaround |
| ResultExt 辅助 trait | `to_run_err("context")` | 0.6.0 的 `.owe(reason)` 可直接覆盖此场景 |
| From impl 信息丢失 | `OMLCodeReason → RunReason` 全映射为 ConfigError | `Core(CoreReason)` 包装变体保留完整信息 |
