# rust-error-handling

wp-reactor 基于 orion-error 0.6.0 的分层错误处理。理解何时使用 StructError、何时保留 anyhow、如何做边界转换。

## 架构概览

```
                     ┌─────────────┐
                     │  wf-engine  │  map_err → anyhow
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │ wf-runtime  │  RuntimeReason（收敛层）
                     └──┬───┬───┬──┘
                        │   │   │
              ┌─────────┘   │   └─────────┐
              │             │             │
       ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
       │  wf-config  │ │wf-lang │ │   wf-core   │
       │  (anyhow)   │ │(anyhow)│ │ CoreReason  │
       └─────────────┘ └────────┘ └─────────────┘
```

| Crate | 方案 | 理由 |
|-------|------|------|
| wf-core | `StructError<CoreReason>` | 业务层，错误需要分类 |
| wf-runtime | `StructError<RuntimeReason>` | 收敛层，统一下层错误 |
| wf-engine | anyhow（入口） | 格式化输出即可 |
| wf-lang | anyhow + `Vec<CheckError>` | 解析库，不需要分类 |
| wf-config | anyhow | 配置错误全部 fatal |
| wf-datagen | anyhow + `Vec<ValidationError>` | 独立工具 |

## DomainReason 定义

### CoreReason — `wf-core/src/error.rs`

```rust
use derive_more::From;
use orion_error::{ErrorCode, StructError, UvsReason};

#[derive(Debug, Clone, PartialEq, thiserror::Error, From)]
pub enum CoreReason {
    #[error("window build error")]
    WindowBuild,          // 1001
    #[error("rule execution error")]
    RuleExec,             // 1002
    #[error("alert sink error")]
    AlertSink,            // 1003
    #[error("data format error")]
    DataFormat,           // 1004
    #[error("{0}")]
    Uvs(UvsReason),       // 委托 UvsReason 编码
}

pub type CoreError = StructError<CoreReason>;
pub type CoreResult<T> = Result<T, CoreError>;
```

### RuntimeReason — `wf-runtime/src/error.rs`

```rust
#[derive(Debug, Clone, PartialEq, thiserror::Error, From)]
pub enum RuntimeReason {
    #[error("bootstrap error")]
    Bootstrap,            // 2001
    #[error("shutdown error")]
    Shutdown,             // 2002
    #[error("{0}")]
    Core(CoreReason),     // 保留完整错误链
    #[error("{0}")]
    Uvs(UvsReason),       // 委托 UvsReason 编码
}

pub type RuntimeError = StructError<RuntimeReason>;
pub type RuntimeResult<T> = Result<T, RuntimeError>;
```

### 错误码范围

```
100-199    UvsReason 业务层（validation, business, not_found...）
200-299    UvsReason 基础设施层（data, system, network, timeout...）
300-399    UvsReason 配置/外部层（config, external）
1001-1099  CoreReason（窗口、规则、告警、数据）
2001-2099  RuntimeReason（启动、关闭）
```

## 三种边界转换模式

### 1. 外部 error → StructError（便捷分类，需 Uvs 变体）

```rust
std::fs::read_to_string(path).owe_sys()?;     // IO → SystemError (201)
toml::from_str(s).owe_conf()?;                // 配置 → ConfigError (300)
serde_json::to_string(v).owe_data()?;         // 数据 → DataError (200)
tcp_listener.accept().await.owe_net()?;        // 网络 → NetworkError (202)
```

### 2. 外部 error → StructError（自定义 reason）

```rust
wf_lang::parse_wfl(input)
    .owe(RuntimeReason::Bootstrap)
    .position(full_path.display().to_string())?;
```

### 3. StructError<R1> → StructError<R2>（跨层转换）

```rust
// CoreReason → RuntimeReason（需要 RuntimeReason: From<CoreReason>）
WindowRegistry::build(window_defs).err_conv()?;
```

## 导入约定

```rust
// wf-core 内部
use orion_error::prelude::*;
use crate::error::{CoreReason, CoreResult};

// wf-runtime 内部
use orion_error::op_context;
use orion_error::prelude::*;
use crate::error::{RuntimeReason, RuntimeResult};
```

## 构造域特定错误

当需要主动构造错误（替代 `bail!`）时：

```rust
// 返回 Err
return StructError::from(CoreReason::WindowBuild)
    .with_detail(format!("duplicate window name: {:?}", name))
    .err();

// 也可链式 position
return StructError::from(CoreReason::RuleExec)
    .with_detail("score expression evaluated to None")
    .err();
```

## OperationContext RAII 模式

用于启动路径和资源构建函数，自动记录操作成功/失败：

```rust
use orion_error::op_context;

pub async fn start(config: FusionConfig, base_dir: &Path) -> RuntimeResult<Self> {
    let mut op = op_context!("engine-bootstrap").with_auto_log();
    op.record("listen", config.server.listen.as_str());
    op.record("base_dir", base_dir.display().to_string().as_str());

    // ... 执行操作，? 可能提前返回 ...
    // → 提前返回：op drop 时 result=Fail，自动 log error
    // → 成功：mark_suc() 后 op drop 时 log info

    op.mark_suc();
    Ok(...)
}
```

注意：`op.record()` 的值参数接受 `&str`、`String`、`&Path`、`&PathBuf`，不接受 `&String`。需要 `.as_str()` 转换。

## 不使用 StructError 的场景

| 场景 | 保持方案 | 理由 |
|------|----------|------|
| scheduler.rs execute_match/close 错误 | log warn + continue | 错误被吞掉，不传播 |
| receiver.rs 连接/解码错误 | log warn + break | 错误被吞掉，不传播 |
| `check_wfl()` 返回 `Vec<CheckError>` | 收集模式 | 非 Result 模式 |
| `validate_wfg()` 返回 `Vec<ValidationError>` | 收集模式 | 非 Result 模式 |
| wf-datagen 全部 | anyhow | 独立工具 |
| wf-lang / wf-config 全部 | anyhow | 不需要错误分类 |

## CLI 入口桥接

wf-engine 不引入 orion-error 依赖，通过 `map_err` 转回 anyhow：

```rust
let engine = FusionEngine::start(fusion_config, base_dir)
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

engine.wait().await.map_err(|e| anyhow::anyhow!("{e}"))?;
```

## 添加新错误变体的流程

### 在 wf-core 添加新 CoreReason 变体

1. 编辑 `crates/wf-core/src/error.rs`，添加变体和 `#[error("...")]`
2. 在 `ErrorCode::error_code()` 分配 1001-1099 范围的码
3. 在公共 API 中使用 `.owe(CoreReason::NewVariant)` 或 `StructError::from(...).err()`

### 在 wf-runtime 添加新 RuntimeReason 变体

1. 编辑 `crates/wf-runtime/src/error.rs`，添加变体和 `#[error("...")]`
2. 在 `ErrorCode::error_code()` 分配 2001-2099 范围的码

### 新 crate 需要 StructError

1. 在 crate 的 `Cargo.toml` 添加 workspace 依赖：orion-error、derive_more、thiserror
2. 创建 `src/error.rs`，定义 DomainReason 枚举（含 Uvs 变体）
3. 实现 `ErrorCode` trait
4. 提供 `type XxxError = StructError<XxxReason>` 和 `type XxxResult<T>` 别名
5. 在 `lib.rs` 添加 `pub mod error;`

## 验证

```bash
cargo check -p <crate>
cargo test -p <crate>
cargo clippy -p <crate>
```
