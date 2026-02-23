# wp-motor 错误处理设计分析

> 基于 wp-motor 对 orion-error 0.5.x 的实际使用，评估优缺点与可参考模式

---

## 一、项目错误类型全景

### 1.1 DomainReason 定义分布

wp-motor 生态有 **6 个 DomainReason**，分布在 3 个仓库中：

| Reason | 位置 | 变体数 | 有 Uvs? | DomainReason impl |
|--------|------|--------|---------|-------------------|
| `RunReason` | wp-error/src/run_error.rs:34-42 | 3 | 是 | blanket (From\<UvsReason\>) |
| `KnowledgeReason` | wp-error/src/knowledge.rs:5-11 | 2 | 是 | blanket |
| `OMLCodeReason` | wp-error/src/parse_error.rs:41-50 | 3 | 是 | blanket |
| `WplCodeReason` | wp-lang/src/parser/error.rs:87-101 | 5 | 是 | blanket |
| `WparseReason` | wp-lang (wpl) | 4 | 是 | blanket |
| `OMLRunReason` | wp-oml/src/core/error.rs:4-8 | 1 | **否** | **手动 impl** |

**发现**: 6 个 DomainReason 中 5 个通过 blanket impl 自动满足（携带 `Uvs(UvsReason)` 变体），只有 `OMLRunReason` 需要手动 `impl DomainReason`。

### 1.2 类型别名惯例

wp-motor 统一使用类型别名简化签名：

```rust
pub type RunError = StructError<RunReason>;
pub type RunResult<T> = Result<T, RunError>;

pub type KnowledgeError = StructError<KnowledgeReason>;
pub type KnowledgeResult<T> = Result<T, StructError<KnowledgeReason>>;

pub type OMLCodeError = StructError<OMLCodeReason>;
pub type OMLCodeResult<T> = Result<T, OMLCodeError>;
```

---

## 二、优点分析

### 2.1 Error Strategy 模式（值得参考）

**位置**: `wp-motor/src/core/error/strategies/`

wp-motor 实现了一个**策略化错误处理框架**，根据运行模式（Debug/Normal/Strict）对同一个错误返回不同的处理策略：

```rust
// strategies/mod.rs:19-25
pub trait ErrorHandlingPolicy {
    fn err4_send_to_sink(&self, err: &SinkError) -> ErrorHandlingStrategy;
    fn err4_load_oml(&self, err: &OMLCodeError) -> ErrStrategy;
    fn err4_load_wpl(&self, err: &WplCodeError) -> ErrStrategy;
    fn err4_engine_parse_data(&self, err: &WparseError) -> ErrorHandlingStrategy;
    fn err4_dispatch_data(&self, err: &SourceError) -> ErrorHandlingStrategy;
}
```

三种策略实例在编译期构造（`const fn init()`），通过运行时配置选择：

```rust
// strategies/mod.rs:27-36
static ERR_STRATEGY_DEV: Err4Debug = Err4Debug::init();
static ERR_STRATEGY_BETA: Err4Normal = Err4Normal::init();
static ERR_STRATEGY_STOIC: Err4Stoic = Err4Stoic::init();

pub fn current_error_policy() -> &'static dyn ErrorHandlingPolicy {
    match sys_robust_mode() {
        RobustnessMode::Debug => &ERR_STRATEGY_DEV,
        RobustnessMode::Normal => &ERR_STRATEGY_BETA,
        RobustnessMode::Strict => &ERR_STRATEGY_STOIC,
    }
}
```

**核心价值**: 这个模式充分利用了 StructError 的类型化 reason——策略 match reason 变体决定是 Ignore、Throw 还是 Retry：

```rust
// err_4normal.rs:18-29
fn err_4universal(&self, reason: &UvsReason) -> ErrStrategy {
    match reason {
        UvsReason::DataError(_, _) => ErrStrategy::Ignore,
        UvsReason::RunRuleError(_) => ErrStrategy::Ignore,
        _ => ErrStrategy::Throw,
    }
}

// err_4normal.rs:52-57
fn err4_load_oml(&self, err: &OMLCodeError) -> ErrStrategy {
    match err.reason() {
        OMLCodeReason::Syntax(_) => ErrStrategy::Ignore,
        OMLCodeReason::NotFound(_) => ErrStrategy::Ignore,
        OMLCodeReason::Uvs(e) => self.err_4universal(e),
    }
}
```

**Debug 模式下同一个错误的不同策略**:

```rust
// err_4debug.rs:55-60 — Debug 模式对语法错误 Throw（暴露问题）
fn err4_load_oml(&self, err: &OMLCodeError) -> ErrStrategy {
    match err.reason() {
        OMLCodeReason::Syntax(_) => ErrStrategy::Throw,   // Debug: 抛出
        OMLCodeReason::NotFound(_) => ErrStrategy::Ignore,
        OMLCodeReason::Uvs(e) => self.err_4universal(e),
    }
}

// err_4normal.rs:52-57 — Normal 模式对语法错误 Ignore（容忍）
fn err4_load_oml(&self, err: &OMLCodeError) -> ErrStrategy {
    match err.reason() {
        OMLCodeReason::Syntax(_) => ErrStrategy::Ignore,  // Normal: 忽略
        OMLCodeReason::NotFound(_) => ErrStrategy::Ignore,
        OMLCodeReason::Uvs(e) => self.err_4universal(e),
    }
}
```

**可参考点**:
- 证明了 StructError 的 reason 类型化在运行时决策中的实际价值
- `Uvs(e)` 变体的"穿透"模式——域特定变体先 match，兜底通过 Uvs 委托给通用策略
- 这是 orion-error 最有说服力的使用场景，说明 reason 不只是日志装饰，而是运行时控制流的一部分

### 2.2 OperationContext 的 RAII 日志模式

**位置**: `wp-motor/src/orchestrator/config/build_sinks.rs:50-83`

OperationContext 在 wp-motor 中形成了清晰的使用模式：

```rust
pub async fn build_sink_target(s_conf: &SinkInstanceConf, ...) -> RunResult<SinkBackendType> {
    let mut op = OperationContext::want("build-sink-instance").with_auto_log();
    op.record("sink_name", s_conf.name().as_str());
    op.record("sink_kind", kind.as_str());
    // ... 执行操作 ...
    let init = factory.build(&spec, &ctx).await
        .owe(RunReason::Dist(DistFocus::SinkError(kind)))?;
    op.mark_suc();   // ← 只在成功时 mark
    Ok(SinkBackendType::Proxy(init.sink))
}
// 函数结束 → op drop → 如果没 mark_suc，自动 log error
```

**位置**: `wp-knowledge/src/loader.rs:146-171`

```rust
pub fn build_authority_from_knowdb(...) -> KnowledgeResult<Vec<String>> {
    let mut opx = OperationContext::want("build authority from knowdb").with_auto_log();
    let (conf, conf_abs, base_dir) = parse_knowdb_conf(root, conf_path, dict)?;
    opx.record("conf", &conf_abs);
    opx.record("base_dir", &base_dir);
    let db = open_authority(authority_uri)?;
    for t in &conf.tables {
        // ...
        load_one_table(&db, &base_dir, t, &conf.csv, &conf.default)?;
    }
    opx.mark_suc();
    Ok(loaded_names)
}
```

**可参考点**:
- `want("操作名") + with_auto_log()` 是统一的入口模式
- `record()` 记录关键参数，用于错误时的上下文回溯
- 通过 RAII Drop 保证：即使 `?` 提前返回也会输出 "fail!" 日志
- 只需 `mark_suc()` 一处调用，代码不需要在每个错误路径添加日志

### 2.3 ErrorOwe + ErrorWith 的流式链

wp-motor 形成了 `.owe_xxx().want("描述").with(&ctx)` 的惯用链式写法：

```rust
// wp-proj/src/connectors/templates.rs:21-24
fs::create_dir_all(&dir)
    .owe_res()
    .want("create connector template dir")
    .with(&dir)?;

// wp-lang/src/util.rs:15-20
let files = find_conf_files(path, target).owe_conf().with(&ctx)?;
let mut f = File::open(f_name).owe_conf().with(&ctx)?;
f.read_to_end(&mut buffer).owe_conf().with(&ctx)?;
```

**可参考点**:
- `.owe_xxx()` 分类 + `.want()` 描述意图 + `.with()` 附加上下文 = 三层信息叠加
- 链式 API 可读性好，`?` 自然终止传播

### 2.4 类型安全的跨 crate 错误收敛

wp-motor 通过 `From` trait + `err_conv()` 实现跨 crate 错误收敛：

```rust
// wp-error/src/run_error.rs:116-128 — SourceReason → RunReason
impl From<SourceReason> for RunReason {
    fn from(e: SourceReason) -> Self {
        match e {
            SourceReason::NotData => Self::Source(SourceFocus::NoData),
            SourceReason::EOF => Self::Source(SourceFocus::Eof),
            SourceReason::SupplierError(info) => Self::Source(SourceFocus::SupplierError(info)),
            SourceReason::Disconnect(info) => Self::Source(SourceFocus::Disconnect(info)),
            SourceReason::Other(info) => Self::Source(SourceFocus::Other(info)),
            SourceReason::Uvs(uvs) => Self::Uvs(uvs),     // UvsReason 穿透
        }
    }
}
```

调用侧只需 `.err_conv()?`：

```rust
// wp-proj/src/sinks/sink.rs:52-55
pub fn check(&self, dict: &EnvDict) -> RunResult<CheckStatus> {
    sinks_core::validate_routes(...).err_conv()?;
    Ok(CheckStatus::Suc)
}
```

**可参考点**:
- `err_conv()` 在 crate 边界使用，清晰标识错误类型转换点
- `Uvs(uvs)` 变体的穿透——底层的 UvsReason 可以不丢失地传到上层

---

## 三、缺点与问题

### 3.1 DomainReason 定义散落在外部 crate（wp-error）

wp-motor 的大部分 DomainReason（`RunReason`, `KnowledgeReason`, `OMLCodeReason`）定义在 **wp-error** 这个独立的基础设施 crate 中，而不是在使用它的业务 crate 里。

**问题**: 修改一个 reason 枚举需要同时改 wp-error + 使用方，增加了跨仓库的耦合。这是因为多个 crate 需要共享同一个 reason 类型——wp-error 作为公共类型仓库存在。

**对 orion-error 的启示**: 这说明在实际大型项目中，DomainReason 倾向于被"提升"到共享层，而不是各 crate 独立定义再 `err_conv()`。orion-error 的文档可以提供关于何时用共享 reason 仓库、何时用每 crate 独立 reason 的指导。

### 3.2 Uvs 变体中 String 参数的实际使用暴露重复问题

在 0.5.x 版本中，UvsReason 变体携带 String（如 `DataError(String, Option<usize>)`），而 `map_err_with` 同时将消息存入 reason 和 detail：

```rust
// err_4normal.rs:22-26 — 策略匹配时忽略 String 内容
UvsReason::DataError(_, _) => ErrStrategy::Ignore,
UvsReason::RunRuleError(_) => ErrStrategy::Ignore,
```

**证据**: 在错误策略 match 中，所有 `UvsReason` 变体的 String 参数都用 `_` 忽略。没有一处代码读取 `UvsReason::DataError` 中的 String 值——它们只 match 变体名称。

**结论**: 这直接证实了提案问题 3（消息冗余）——UvsReason 变体中的 String 在实际使用中完全被忽略，reason 只用于分类，消息始终从 detail 读取。支持 0.6 将 UvsReason 变体简化为无参数枚举。

### 3.3 自定义 owe 方法绕过 ErrorOwe 体系

**位置**: `wp-error/src/run_error.rs:72-101`

wp-motor 在 wp-error 中自定义了 `RunErrorOwe` trait：

```rust
pub trait RunErrorOwe<T> {
    fn owe_sink(self) -> RunResult<T>;
    fn owe_source(self) -> RunResult<T>;
}

impl<T, E> RunErrorOwe<T> for Result<T, E>
where E: std::fmt::Display,
{
    fn owe_sink(self) -> RunResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(StructError::from(RunReason::Dist(DistFocus::SinkError(
                "sink fail".into(),
            ))).with_detail(e.to_string())),
        }
    }
    fn owe_source(self) -> RunResult<T> {
        // 类似模式...
    }
}
```

**问题**:
- 这些自定义 owe 方法和 orion-error 的 `ErrorOwe` trait 方法（`.owe_sys()`, `.owe_conf()` 等）**风格相同但机制不同**
- 存在原因：`ErrorOwe` 的 `.owe_xxx()` 只支持 UvsReason 分类，无法映射到 `RunReason::Dist(DistFocus::SinkError(...))`
- 这是因为 `.owe(reason)` 被 `From<UvsReason>` 约束牵连（提案问题 2）

**对 orion-error 的启示**: 拆分 `ErrorOweBase`（只含 `.owe(reason)`，不要求 `From<UvsReason>`）后，用户可以直接用：
```rust
// 改进后不需要自定义 RunErrorOwe
factory.build(&spec, &ctx).await
    .owe(RunReason::Dist(DistFocus::SinkError(kind)))?;
```
事实上 wp-motor 的 `build_sinks.rs:75` 已经在用 `.owe()` 做这件事，说明 `.owe()` 本身已够用，`RunErrorOwe` 的存在是因为在某些上下文中无法访问 `ErrorOweBase`（它和 `ErrorOwe` 在同一个 impl 块中，受限于 `From<UvsReason>` bound）。

### 3.4 ResultExt 辅助 trait 反映边界转换的摩擦

**位置**: `wp-proj/src/utils/error_conv.rs:38-87`

wp-motor 在 wp-proj 中定义了 `ResultExt` trait 来处理 `anyhow::Result` 和其他非 StructError 类型到 `RunResult` 的转换：

```rust
pub trait ResultExt<T, E> {
    fn to_run_err(self, context: &str) -> RunResult<T>;
    fn to_run_err_with<F>(self, f: F) -> RunResult<T>
        where F: FnOnce(&E) -> String;
}

impl<T, E: std::fmt::Display> ResultExt<T, E> for Result<T, E> {
    fn to_run_err(self, context: &str) -> RunResult<T> {
        self.map_err(|e| RunReason::from_conf(format!("{}: {}", context, e)).to_err())
    }
}
```

**问题**:
- `.to_run_err("context")` 内部调用 `RunReason::from_conf(msg)`，所有非 StructError 的错误都被归类为 `ConfigError`——这不一定准确
- 与 ErrorOwe 的 `.owe_conf()` / `.owe_sys()` 功能重叠
- `ResultExt` 的存在说明 orion-error 的 `ErrorOwe` 在处理 `anyhow::Result` 和任意 `Display` 错误时不够顺畅

**对 orion-error 的启示**: `ErrorOweBase` 拆分后，`.owe(reason)` 可以覆盖这个场景，减少用户自定义辅助 trait 的需要。

### 3.5 From impl 的手工转换链过长

**位置**: `wp-error/src/run_error.rs:50-70`

```rust
impl From<ConfReason<ConfCore>> for RunReason {
    fn from(value: ConfReason<ConfCore>) -> Self {
        Self::Uvs(UvsReason::from_conf(ConfErrReason::Core(value.to_string())))
    }
}
impl From<OMLCodeReason> for RunReason {
    fn from(value: OMLCodeReason) -> Self {
        Self::Uvs(UvsReason::from_conf(value.to_string()))
    }
}
impl From<OrionSecReason> for RunReason {
    fn from(value: OrionSecReason) -> Self {
        match value {
            OrionSecReason::Sec(sec_reason) => {
                Self::Uvs(UvsReason::from_conf(sec_reason.to_string()))
            }
            OrionSecReason::Uvs(uvs_reason) => Self::Uvs(uvs_reason),
        }
    }
}
```

**问题**:
- 每个底层 reason 到上层 reason 的 `From` impl 都需要手写
- `OMLCodeReason → RunReason` 的转换丢失了原始分类信息——所有 OML 错误都被打包为 `UvsReason::ConfigError`
- 这是 orion-error `From<R1> for R2` 模式的固有成本，`err_conv()` 也依赖这些 impl

**对 orion-error 的启示**: 这是否值得提供 derive macro 来自动生成？当前的手写 `From` 有信息丢失风险（如 Syntax 错误变成 ConfigError），但自动生成也难以推断正确映射。**这可能不是框架能解决的问题**。

### 3.6 OMLRunReason 手动 impl DomainReason 的摩擦

**位置**: `wp-oml/src/core/error.rs:4-9`

```rust
#[derive(Error, Debug, Clone, PartialEq, Serialize)]
pub enum OMLRunReason {
    #[error("format conv fail{0}")]
    FmtConv(String),
}
impl DomainReason for OMLRunReason {}
```

`OMLRunReason` 只有一个变体，不需要 UvsReason 收敛，因此没有 `Uvs(UvsReason)` 变体。但因为不满足 blanket impl 的 `From<UvsReason>` 约束，必须手动 `impl DomainReason`。

**问题**: 这不是 bug，但增加了"为什么编译不过"的学习成本。新用户不理解为什么自己的 enum 不自动满足 DomainReason。

**对 orion-error 的启示**: 0.6 拆分 ErrorOweBase 后，没有 `Uvs` 变体的 reason 也能用 `.owe(reason)`，降低了不携带 Uvs 的代价。但 blanket impl 的 `From<UvsReason>` 约束是否应该去掉值得考虑——去掉后所有 DomainReason 都需要手动 impl（或改为 derive macro）。

### 3.7 三种策略实现高度重复

**位置**: `err_4debug.rs`, `err_4normal.rs`, `err_4stoic.rs`

三个文件结构几乎相同（每个约 100 行），差异仅在于个别分支返回 Throw 还是 Ignore：

```rust
// Debug: OMLCodeReason::Syntax(_) => ErrStrategy::Throw
// Normal: OMLCodeReason::Syntax(_) => ErrStrategy::Ignore
// Stoic: OMLCodeReason::Syntax(_) => ErrStrategy::Ignore
```

`err4_send_to_sink`、`err4_dispatch_data` 在三个策略中**完全相同**（约 60% 代码重复）。

**问题**: 这不是 orion-error 的问题，而是 wp-motor 自身的实现选择。但它暴露了一个潜在需求：**错误策略的声明式配置**，而不是命令式 match。

---

## 四、模式总结与建议

### 4.1 wp-motor 验证了的 orion-error 核心价值

| 特性 | 验证场景 | 结论 |
|------|---------|------|
| reason 类型化 match | Error Strategy 三模式切换 | **核心价值**——reason 不只是日志，是运行时控制流 |
| OperationContext RAII | loader.rs, build_sinks.rs 的 want/record/mark_suc | **实用**——减少遗漏日志 |
| ErrorOwe 链式 API | `.owe_res().want().with()` | **好用**——流式可读 |
| err_conv 跨 crate 转换 | sinks_core → wp-proj | **必要**——crate 边界的类型安全 |
| UvsReason 收敛 | Uvs 变体穿透到上层统一处理 | **必要**——没有它，错误策略无法统一 |

### 4.2 wp-motor 暴露的 orion-error 改进需求

| 问题 | wp-motor 证据 | 改进方向 |
|------|-------------|---------|
| UvsReason 变体中 String 无用 | 策略 match 全用 `_` 忽略 | 0.6: 变体去 String |
| ErrorOwe 绑 From\<UvsReason\> | 自定义 RunErrorOwe 绕过 | 拆分 ErrorOweBase |
| 手动 impl DomainReason | OMLRunReason 需手写 | 降低 blanket impl 门槛 |
| 非 StructError 的转换摩擦 | ResultExt 辅助 trait | ErrorOweBase 覆盖此场景 |
| Serialize 硬约束 | 所有 reason 必须 derive(Serialize) | 0.6: serde feature gate |

### 4.3 可参考到 wp-reactor 设计的模式

1. **Error Strategy 模式**: wp-reactor 当前用 log+continue 处理运行时错误（scheduler.rs, receiver.rs）。如果未来需要更精细的错误处理策略（如区分可重试错误 vs 致命错误），可参考 wp-motor 的 ErrorHandlingPolicy trait 模式。但当前 wp-reactor 的 log+continue 模式已足够，不需要过早引入。

2. **类型别名惯例**: `type CoreError = StructError<CoreReason>; type CoreResult<T> = Result<T, CoreError>;` 应统一采用。

3. **OperationContext 在启动路径使用**: wp-reactor 的 `lifecycle.rs::start()` 是引入 OperationContext 收益最大的位置，参考 wp-motor 的 `loader.rs` 模式——want + record + mark_suc。

4. **`Uvs(UvsReason)` 穿透模式**: wp-reactor 的 `CoreReason` 和 `RuntimeReason` 都应保留 Uvs 变体，使底层的 IO/网络/配置错误可以不丢失地传到上层。

### 4.4 wp-motor 中不建议参考的模式

1. **DomainReason 集中在独立 crate (wp-error)**: wp-reactor 的 crate 间依赖更简单（wf-core → wf-runtime → wf-engine），每个 crate 定义自己的 reason 再 err_conv 更清晰。

2. **自定义 owe trait (RunErrorOwe)**: 这是 ErrorOwe 约束过强的 workaround，orion-error 0.6 拆分 ErrorOweBase 后不再需要。

3. **ResultExt 辅助 trait**: 同上，ErrorOweBase 的 `.owe(reason)` 可以覆盖。

4. **From impl 丢失信息的转换**: `OMLCodeReason → RunReason` 全部映射为 ConfigError 丢失了原始错误分类。wp-reactor 应在 `CoreReason → RuntimeReason` 的 From impl 中保留 `Core(CoreReason)` 包装变体，不丢失信息。
