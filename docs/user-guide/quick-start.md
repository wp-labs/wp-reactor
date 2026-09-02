# 快速开始

## 概述

WFL 是 WarpFusion 的检测领域专用语言（DSL），用于编写安全关联检测规则、风险告警归并与实体行为分析逻辑。

核心设计理念：

- 简洁可读
- 显式优先
- 可解释可调试

WFL 不是通用流计算 SQL，也不是任意 DAG 引擎。

## 第一个项目

一个典型的 WarpFusion 项目包含三类文件：

```text
my-project/
├── fusion.toml
├── windows.toml
├── schemas/
│   └── security.wfs
├── rules/
│   └── brute_force.wfl
└── sinks/
```

## 第一个规则

第 1 步，定义数据窗口 `schemas/security.wfs`：

```wfs
window auth_events {
    stream_tag = "syslog"
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
        window_events: digit
        fail_count: digit
        first_seen: time
        last_seen: time
        rule_window_start: time
        rule_window_end: time
        latest_analysis_time: time
        message: chars
    }
}
```

第 2 步，编写规则 `rules/brute_force.wfl`：

```wfl
use "security.wfs"

rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }

    match<sip:5m> {
        on event {
            failed_hits: fail | count >= 3;
        }
    } -> score(70.0)

    entity(ip, fail.sip)

    yield security_alerts (
        sip = fail.sip,
        window_events = stat.count(window_event(fail)),
        fail_count = stat.count(match_event(failed_hits)),
        first_seen = @event_first_time,
        last_seen = @event_last_time,
        rule_window_start = @window_start_time,
        rule_window_end = @window_end_time,
        latest_analysis_time = @emit_time,
        message = fmt("{} brute force detected", fail.sip)
    )
}
```

第 3 步，配置运行时 `fusion.toml`：

```toml
mode = "daemon"
sinks = "sinks"
windows = "windows.toml"

[[sources]]
connect = "tcp_src"
name = "ingress_tcp"
addr = "127.0.0.1"
port = "9800"
framing = "len"
data_format = "arrow_framed"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

[vars]
FAIL_THRESHOLD = "3"
```

窗口默认值与覆盖不写在 `fusion.toml`，而是放在独立的 `windows.toml` 中，由顶层 `windows = "windows.toml"` 引用：

```toml
# windows.toml
[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"
```

第 4 步，启动引擎：

```bash
wfusion daemon --config fusion.toml
```

如需直接看到指标快照：

```bash
wfusion daemon --config fusion.toml --metrics --metrics-interval 2s
```

## 三文件模型

WFL 采用职责分离的三文件模型：

| 文件 | 扩展名 | 职责 | 热加载 |
|------|--------|------|:------:|
| Window Schema | `.wfs` | 逻辑数据定义（window、field、time、over） | 否 |
| 检测规则 | `.wfl` | 检测逻辑（events/match/join/yield） | 是 |
| 运行时配置 | `.toml` | 物理参数（mode、sources、watermark、sinks） | 仅 `[vars]` 与 `[output]` |

依赖关系如下：

```text
.wfs
  ↑
.wfl
  ↑
.toml
```

- `.wfl` 仅能引用 `use` 导入的 window
- `.toml` 只管物理参数，不写业务规则
- `.wfs` 变更需要重启引擎

## 输出统计字段

在 `yield` 中输出规则触发原因时，优先使用稳定统计上下文：

- `stat.count(window_event(alias))`：当前 rule instance/window 内，source alias 进入窗口的候选事件数
- `stat.count(match_event(label))`：已命名 `on event` step label 接受为证据的命中事件数
- `stat.count(match_distinct(label))`：`distinct | count` 分支的精确 distinct 数量
- `stat.value(trigger(label))`：`on event` label 第一次满足阈值时的聚合值
- `stat.value(final(label))`：`and close` label 在 close/flush 输出时的最终聚合值

selector 参数是静态符号，不加引号。详细规则见 [规则编写指南](./rule-writing.md#16-输出稳定统计上下文) 和 [语言参考](./language-reference.md#稳定统计上下文)。

## 输出时间字段

告警输出不要依赖内部 `__wfu_*` 元字段来表达业务时间。需要首尾事件时间、窗口边界或分析时间时，在输出 window 中声明普通 `time` 字段，并在 `yield` 中显式赋值：

- `first_seen = @event_first_time`：窗口内候选事件（进入实例的被接受事件）的第一条事件时间
- `last_seen = @event_last_time`：候选事件的最后一条事件时间
- `evidence_start_time = @evidence_start_time` / `evidence_end_time = @evidence_end_time`：本次命中证据（命中依据事件）的范围起止时间
- `rule_window_start = @window_start_time` / `rule_window_end = @window_end_time`：规则窗口边界
- `latest_analysis_time = @emit_time`：本条输出记录的稳定产出时间

时间变量只允许在 `yield` 表达式中使用。详细规则见 [规则编写指南](./rule-writing.md#15-输出证据时间和窗口时间) 和 [语言参考](./language-reference.md#时间系统变量)。

## 模式说明

- `mode = "daemon"`：常驻运行，至少需要一个启用的 source（任意类型）
- `mode = "batch"`：批处理回放，至少需要一个启用的 `file` source，且不允许启用任何非 `file` 类型的 source

## Source 约定

`wfusion` 的输入统一通过 `[[sources]]` 声明：

- `connect = "tcp_src"`：基于 TCP 接收数据（`addr` + `port`）
- `connect = "file_src"` 或 `type = "file"`：基于文件回放数据

不再使用 `[server]` 配置块。

更多配置见 [运行时配置](./runtime-config.md)。

## 逐条评分到下游聚合

如果你的需求是“先对每条事件做语义打分，再在下游窗口聚合”，优先考虑 `on each`，而不是一开始就把全部逻辑写进 `match`。

示例：

```wfl
rule enrich_each_event {
    events {
        e : auth_events
    }

    on each e -> score(if e.action == "failed" then 70.0 else 10.0)

    entity(ip, e.sip)

    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip,
        username = e.username
    )
}
```

下游再聚合：

```wfl
rule final_risk {
    events {
        x : enriched_events
    }

    match<sip:5m> {
        on event {
            x | count >= 1;
        }
    } -> score(avg(x.__wfu_score) + 10.0)

    entity(ip, x.sip)

    yield final_out (
        sip = x.sip
    )
}
```

说明：

- `x.__wfu_score` 可直接在下游规则中使用；中间 window 被下游消费时，编译器会自动把这些 `__wfu_*` 系统字段视为可用列
- 如果 `enriched_events` 定义了 time 列，而上游 `yield` 没显式赋值，runtime 会自动继承输入事件时间；如果显式赋值，则以用户写的值为准
- 中间 window 链路必须无环，不能把输出再写回自己或互相回写
- 如果这一步只是把原始事件逐条整理成语义事件，例如 `raw_events -> semantic_events`，且你的上游已有 OML/投影层，优先在 OML 完成；WFL 更适合保留窗口聚合和告警规则

详细约定见 [On Each 与逐条打分](./on-each.md)。
