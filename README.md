# WarpFusion Reactor

<div align="center">

[![CI](https://github.com/wp-labs/wp-reactor/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wp-labs/wp-reactor/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/wp-labs/wp-reactor/graph/badge.svg?branch=main&token=6SVCXBHB6B)](https://codecov.io/gh/wp-labs/wp-reactor)
[![Release](https://img.shields.io/github/v/release/wp-labs/wp-reactor?label=release&sort=semver)](https://github.com/wp-labs/wp-reactor/releases)
[![License: Elastic-2.0](https://img.shields.io/badge/license-Elastic--2.0-blue.svg)](https://github.com/wp-labs/wp-reactor/blob/main/LICENSE)
[![Test matrix](https://img.shields.io/badge/test%20matrix-linux%20%C2%B7%20macOS%20%C2%B7%20stable%20%26%20beta-informational)](https://github.com/wp-labs/wp-reactor/blob/main/.github/workflows/ci.yml)
[![Rust: edition 2024](https://img.shields.io/badge/rust-edition%202024-dea584.svg?logo=rust&logoColor=white)](https://doc.rust-lang.org/edition-guide/rust-2024/)

WarpFusion Reactor 是一个基于 Rust 构建的安全事件流处理引擎，专注于实时关联检测、告警归并与实体行为分析。

</div>

## 项目结构

```
wp-reactor/
├── Cargo.toml              # Workspace 根配置
├── crates/
│   ├── wf-lang/            # Window Schema / Rule 编译器
│   ├── wf-config/          # wfusion.toml 配置管理与校验
│   ├── wf-core/            # 核心 CEP / Window / Alert 逻辑
│   ├── wf-runtime/         # 运行时生命周期 / Receiver / Scheduler
│   └── wf-engine/          # `wfusion` CLI 逻辑库（实际二进制在 ../warp-fusion）
└── docs/
    ├── design/             # 设计文档
    └── user-guide/         # 面向使用者的指南
```

CLI / 工具 workspace 位于相邻仓库 `../warp-fusion`，负责产出 `wfusion`、`wfgen`、`wfl` 三个二进制。

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
- **source** — 输入源配置（`tcp` / `file`，支持 `ndjson` / `arrow_framed` / `arrow_ipc`）
- **runtime** — 执行器并行度、规则超时、schema/rule 文件路径
- **fusion** — 顶层配置组装与解析入口
- **validate** — 跨文件语义校验

示例 `wfusion.toml`：

```toml
mode = "daemon"
sinks = "sinks"

[[sources]]
type = "tcp"
name = "ingress_tcp"
listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

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

批处理文件输入示例：

```toml
mode = "batch"
sinks = "sinks"

[[sources]]
type = "file"
path = "data/events.arrowf"
data_format = "arrow_framed"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"
```

## 三文件模型

| 文件 | 职责 |
|------|------|
| `.wfs` | 逻辑数据定义（window、field、time、over） |
| `.wfl` | 检测规则（bind / match / join / yield） |
| `wfusion.toml` | 物理参数（mode、max_bytes、watermark、sinks） |

用户文档入口见 [docs/user-guide/index.md](docs/user-guide/index.md)。

## 构建

```bash
cargo build
```

构建 CLI：

```bash
cargo build --manifest-path ../warp-fusion/Cargo.toml
```

## 测试

```bash
cargo test
```

运行 CLI / 工具测试：

```bash
cargo test --manifest-path ../warp-fusion/Cargo.toml
```

## 依赖

| 依赖 | 用途 |
|------|------|
| `serde` | 序列化 / 反序列化 |
| `toml` | TOML 配置解析 |
| `orion-error` | 结构化错误处理 |
| `winnow` | Parser combinator |

## 许可证

[Elastic License 2.0](https://www.elastic.co/licensing/elastic-license)（SPDX：`Elastic-2.0`）——全文见 [LICENSE](LICENSE)。
