# wp-reactor 错误处理设计

> 基于 `orion-error` 0.8.x。目标是让核心 API、运行时 task 边界、配置加载和 CLI 边界都返回结构化错误，具备稳定 reason、identity、上下文和 source chain。

## 设计目标

wp-reactor 的错误分三类处理：

| 生命周期 | 机制 | 说明 |
|---|---|---|
| 跨 crate 返回、需要上层决策 | `StructError<R>` | 用 typed reason 保留分类、上下文和 source |
| DSL 检查、lint、批量诊断 | 保持 `Vec<CheckError>` | 这类错误天然是多诊断集合，不适合强行塞进单个 `Result` |
| task 内部可恢复错误 | log + continue | 当前保持既有行为，后续可基于 reason 加策略 |

当前落点：

| Crate | 错误边界 |
|---|---|
| `wf-engine` | 定义 `CoreReason` 和 `CoreResult<T>`（`src/error.rs`） |
| `wf-runtime` | 定义 `RuntimeReason` 和 `RuntimeResult<T>`，task 边界使用 `RuntimeResult`（`src/error.rs`） |
| `wf-runtime` | CLI 边界用 `EngineReason` / `EngineResult`，直接渲染 `DiagnosticReport`（`src/cli/error.rs`） |
| `wf-config` | 定义 `ConfigReason` 和 `ConfigResult<T>`，配置加载、校验、sink 配置边界结构化（`src/error.rs`）；变量展开用 `VarsReason`（`src/vars/error.rs`） |
| `wf-lang` | 定义 `LangReason` 和 `LangResult<T>`，解析/编译核心 API 结构化（`src/error.rs`） |

## Reason 建模

领域 reason 使用 `#[derive(OrionError)]`，业务变体必须有稳定 `identity`。动态信息不放在 enum payload 里，而是放到 `StructError` 的 detail、position、context 或 source。

`wf-engine`：

```rust
#[derive(Debug, Clone, PartialEq, OrionError)]
pub enum CoreReason {
    #[orion_error(message = "window build error", identity = "logic.wf_core.window_build")]
    WindowBuild,
    #[orion_error(message = "rule execution error", identity = "logic.wf_core.rule_exec")]
    RuleExec,
    #[orion_error(message = "data format error", identity = "sys.wf_core.data_format")]
    DataFormat,
    #[orion_error(transparent)]
    General(UnifiedReason),
}
```

`wf-runtime`：

```rust
#[derive(Debug, Clone, PartialEq, OrionError)]
pub enum RuntimeReason {
    #[orion_error(message = "bootstrap error", identity = "sys.wf_runtime.bootstrap")]
    Bootstrap,
    #[orion_error(message = "shutdown error", identity = "sys.wf_runtime.shutdown")]
    Shutdown,
    #[orion_error(transparent)]
    Core(CoreReason),
    #[orion_error(transparent)]
    General(UnifiedReason),
}
```

`General(UnifiedReason)` 作为通用基础设施错误兜底，并通过 derive 生成 `RuntimeReason::system_error()`、`RuntimeReason::core_conf()`、`RuntimeReason::data_error()` 等委托构造器。

## 边界转换规则

`orion-error` 0.8 的主路径：

| 上游错误 | 转换方式 | 用途 |
|---|---|---|
| `Result<T, StructError<R1>>` 且只改变 reason 类型 | `.conv_err()` | `CoreReason -> RuntimeReason` |
| `Result<T, E>`，`E` 有 `UnstructuredSource` 支持 | `.source_err(reason, detail)` | `io`、`toml`、`serde_json` |
| 第三方 `StdError` 无专用 bridge | `.source_raw_err(reason, detail)` | 外部 SDK / connector 错误 |
| 单个 reason 构造错误 | `reason.to_err()` | 手写业务失败 |

示例：

```rust
let schemas = wf_lang::parse_wfs(&content)
    .source_err(RuntimeReason::Bootstrap, "parse schema file")
    .position(path.display().to_string())?;

let registry = WindowRegistry::build(window_defs).conv_err()?;

return RuntimeReason::Bootstrap
    .to_err()
    .with_detail("no enabled sources configured")
    .err();
```

## Identity

对外稳定标识使用 `identity_snapshot().code` / `stable_code()`。不要为领域 reason 手工分配数字 `code`；需要分类时依赖 `identity` 前缀、`ErrorCategory` 和 typed reason。

## 后续扩展

后续如果要做运行时策略，可以基于 `RuntimeReason` match 出 Debug / Normal / Strict 等模式：

- `Bootstrap`：启动期 fatal。
- `Shutdown`：优雅退出阶段记录首个失败。
- `Core(WindowBuild | RuleExec | DataFormat)`：区分业务、规则执行和数据格式。
- `General(system/network/timeout/resource)`：按基础设施类别决定重试、降级或退出。

策略层应建立在 typed reason 上，不应重新解析错误字符串。
