# WarpFusion Reactor

WarpFusion Reactor 是一个基于 Rust 构建的安全事件流处理引擎，专注于实时关联检测、告警归并与实体行为分析。

## 项目结构

```
wp-reactor/
├── Cargo.toml              # Workspace 根配置
├── crates/
│   ├── wf-lang/            # Window Schema (.wfs) 解析器
│   └── wf-config/          # wfusion.toml 配置管理与校验
└── docs/
    └── design/             # 设计文档
```

## Crates

### wf-lang

Window Schema 解析器，负责解析 `.wfs` 文件中的窗口定义 DSL。

支持的字段类型：`chars` | `digit` | `float` | `bool` | `time` | `ip` | `hex` | `array/T`

示例 `.wfs` 文件：

```
window auth_events {
    stream = "auth"
    time = event_time
    over = 30m
    fields {
        username: chars
        sip: ip
        event_time: time
    }
}
```

### wf-config

运行时配置管理，负责加载、解析和校验 `wfusion.toml` 配置文件。

核心模块：

- **types** — 自定义类型（`HumanDuration`、`ByteSize`、`DistMode`、`EvictPolicy`、`LatePolicy`）
- **window** — 窗口配置（全局默认值 + 逐窗口覆盖 → 合并解析）
- **server** — 服务端监听配置
- **runtime** — 执行器并行度、规则超时、schema/rule 文件路径
- **fusion** — 顶层配置组装与解析入口
- **validate** — 跨文件语义校验

示例 `wfusion.toml`：

```toml
sinks = "sinks"

[server]
listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
window_schemas = ["security.wfs"]
wfl_rules = ["brute_scan.wfl"]

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.auth_events]
mode = "local"
over_cap = "30m"
```

## 三文件模型

| 文件 | 职责 |
|------|------|
| `.wfs` | 逻辑数据定义（window、field、time、over） |
| `.wfl` | 检测规则（bind / match / join / yield） |
| `wfusion.toml` | 物理参数（mode、max_bytes、watermark、sinks） |

## 构建

```bash
cargo build
```

## 测试

```bash
cargo test
```

## 依赖

| 依赖 | 用途 |
|------|------|
| `serde` | 序列化 / 反序列化 |
| `toml` | TOML 配置解析 |
| `anyhow` | 错误处理 |
| `winnow` | Parser combinator |

## 许可证

[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)
