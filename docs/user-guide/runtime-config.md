# 运行时配置

## 完整配置示例

```toml
mode = "daemon"                              # daemon | batch
sinks = "sinks"
windows = "windows.toml"                     # 窗口默认值与覆盖（外部文件）

[output]
time_format = "%Y-%m-%d %H:%M:%S%.3f"
time_zone = "utc"

[[sources]]
connect = "tcp_src"                          # TCP 接入（addr + port 由 connector 解析）
name = "ingress_tcp"
addr = "127.0.0.1"
port = "9800"
framing = "len"
data_format = "arrow_framed"              # ndjson | arrow_framed | arrow_ipc

[[sources]]
type = "file"
name = "seed_auth"
path = "data/auth_events.ndjson"
stream_tag = "syslog"
data_format = "ndjson"                     # ndjson | csv | arrow_framed | arrow_ipc

[[sources]]
type = "file"
name = "seed_arrow_framed"
path = "data/auth_events.arrowf"
data_format = "arrow_framed"               # wp_arrow 分帧格式

[[sources]]
type = "file"
name = "seed_arrow_ipc"
path = "data/auth_events.arrow"
stream_tag = "syslog"
data_format = "arrow_ipc"                  # 标准 Arrow IPC file

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

[vars]
FAIL_THRESHOLD = "3"
SCAN_THRESHOLD = "10"
```

窗口默认值与按窗口覆盖不写在 `wfusion.toml`，而是放在独立文件（顶层 `windows = "windows.toml"` 引用）：

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

[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"
```

## 实例与状态过期（窗口 TTL）

每条规则的实例（per-key 匹配状态）按 `match` 窗口时长保留（`match<sip:5m>` → 距最后事件 5m）。
运行时**周期扫描**会按墙钟时间推进有效水位（`watermark + 距上次处理事件的墙钟时间`），因此
**输入静默时实例也会按窗口 TTL 自动过期释放**，无需新事件触发。可通过
`rule.instances` / `window.memory_bytes` 指标观察实例数与窗口内存。

## 模式

- `mode = "daemon"`：常驻运行，要求至少一个启用的 source（任意类型）
- `mode = "batch"`：批处理回放，要求至少一个启用的 `file` source，且不允许启用任何非 `file` 类型的 source

## Sources

输入统一通过 `[[sources]]` 配置。

### TCP Source

通过 connector factory 构建。使用 `connect = "tcp_src"` + `addr` + `port` + `framing` + `data_format` 参数：

```toml
[[sources]]
connect = "tcp_src"
name = "ingress_tcp"
addr = "127.0.0.1"
port = "9800"
framing = "len"             # len | line | auto
stream_tag = ""                 # 可选：固定 stream tag；留空则用帧 tag 或 stream_tag_field
```

`data_format` 取值：

| 值 | 含义 | stream tag 来源 |
|------|------|-------------|
| `ndjson` | JSON Lines 文本 | 固定 `stream_tag`，或用 `stream_tag_field` 从 payload 读取 |
| `arrow_ipc` | 原始 Arrow IPC Stream | 必须配 `stream_tag` |
| `arrow_framed` | wp_arrow 帧 `[4B tag_len][tag][IPC Stream]` | 用帧内 tag，或配 `stream_tag` 覆盖 |

说明：

- TCP 接入本身就是 source
- `framing` 控制字节级分帧（`len` = RFC6587 octet-counting，`line` = 按换行，`auto` = 自动检测）
- `data_format` 控制帧内 payload 格式（由 connector factory 校验）
- `daemon` 模式通常通过该入口接收实时数据

### File Source

当前支持四种格式：

| 格式 | 含义 | `stream_tag` |
|------|------|----------|
| `ndjson` | 逐行 JSON | 可用固定 `stream_tag`，也可用 `stream_tag_field` |
| `csv` | 逗号分隔文本 | 可用固定 `stream_tag`，也可用 `stream_tag_field` |
| `arrow_framed` | 当前 `wp_arrow` 分帧文件格式 | 可省略 |
| `arrow_ipc` | 标准 Arrow IPC file | 必填 |

#### `ndjson`

```toml
[[sources]]
type = "file"
path = "data/events.jsonl"
stream_tag = "syslog"
data_format = "ndjson"
```

如果一个 NDJSON source 混合多个逻辑流，可以不配置固定 `stream_tag`，改用
`stream_tag_field` 指定承载 tag 的字段：

```toml
[[sources]]
type = "file"
path = "data/events.jsonl"
data_format = "ndjson"
stream_tag_field = "wp_oml_name"
```

#### `arrow_framed`

```toml
[[sources]]
type = "file"
path = "data/events.arrowf"
data_format = "arrow_framed"
```

说明：

- 文件格式为当前 `wp_arrow` 分帧格式
- 读取方式为 `[4B len][encode_ipc payload]...`
- 默认按帧内 `tag` 路由
- 如有需要，也可显式写 `stream_tag` 覆盖

#### `arrow_ipc`

```toml
[[sources]]
type = "file"
path = "data/events.arrow"
stream_tag = "syslog"
data_format = "arrow_ipc"
```

说明：

- 标准 Arrow IPC file 不携带业务路由 tag
- 因此必须显式配置 `stream_tag`

### 为什么不用自动识别

`arrow_framed` 与 `arrow_ipc` 都属于 Arrow 相关格式，但语义不同：

- `arrow_framed` 自带逐帧边界和路由 tag
- `arrow_ipc` 是标准文件格式，不包含该路由信息

因此不做自动判别。不指定 `data_format` 时默认 `ndjson`；Arrow 相关格式必须显式写：

- `arrow_framed`
- `arrow_ipc`

## Runtime

```toml
[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"
```

说明：

- `schemas` / `rules` 支持 glob
- 可使用 `schemas/**/*.wfs` 递归扫描

## 输出格式

```toml
[output]
time_format = "%Y-%m-%d %H:%M:%S%.3f"
time_zone = "utc"
```

说明：

- `time_format` 是项目级默认时间输出格式，使用 chrono/strftime 格式语法
- `strftime(@emit_time)` 等单参数调用使用该默认格式
- `strftime(@emit_time, "%Y-%m-%d")` 显式传入格式时，以函数参数为准
- 未配置 `[output]` 时，默认 `time_format = "%Y-%m-%d %H:%M:%S%.3f"`
- 当前 `time_zone` 只支持 `utc`，保证多节点部署输出一致

## 窗口默认值与覆盖

窗口配置放在 `windows = "..."` 指向的外部文件（例如 `windows.toml`），不在 `wfusion.toml` 内联。`[window_defaults]` 所有字段都是必填，无隐式默认：

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

按 window 覆盖：

```toml
[window.high_volume_events]
mode = "local"
max_window_bytes = "1GB"
over_cap = "1h"
```

## 变量预处理

```toml
[vars]
FAIL_THRESHOLD = "5"
SCAN_THRESHOLD = "10"
```

在 `.wfl` 中引用：

```wfl
fail | count >= $FAIL_THRESHOLD;
```

支持：

- `$VAR`
- `${VAR:default}`

这些变量不仅可用于 `.wfl` 预处理，也会用于 `wfusion.toml` 中的字符串字段，例如：

- `runtime.schemas`
- `runtime.rules`
- `sinks`
- `work_root`
- `[[sources]].path`
- `logging.file`
- sink 配置文件中的字符串字段，例如 connector / route / defaults 里的 `params.*`

解析顺序：

- 先查 CLI `--var`
- 再查合并后的 `[vars]`
- 再查当前 loader 注入的 scoped vars
- 再回退到环境变量
- 最后才使用 `${VAR:default}` 的默认值

例如：

```toml
[runtime]
schemas = "${CASE_PATH}/models/schemas/*.wfs"
rules = "${CASE_PATH}/models/rules/*.wfl"
```

### Loader Scoped Vars

对 `wfusion.toml` 及相关 loader，目前会按作用域注入这些变量：

- `CONFIG_DIR`
  当前正在解析的配置文件所在目录
- `WORK_DIR`
  CLI `--work-dir` 指定的运行基准目录；如果未指定，则等于 base config 所在目录

补充说明：

- `wf-vars` 本身不定义这些名字
- 这些名字由 `wf-config` / `wfusion` 的 loader 在各自作用域内补入
- `WORK_ROOT` 不是通用 loader scoped var；它只会在 runtime 组装 sink / rule 上下文时按需显式提供

例如：

```toml
sinks = "${WORK_DIR}/topology/wf_sinks"
work_root = "${WORK_DIR}"

[logging]
file = "${CONFIG_DIR}/logs/wfusion.log"
```

### 路径语义

当前实现采用两段式规则：

1. 先做 overlay merge 和已知路径字段 rebasing
2. 再做变量展开

具体来说：

- base config 中的相对路径，默认相对 base config 所在目录
- overlay 中的已知路径字段，先相对 overlay 文件自身目录解释，再折算到最终运行基准
- 如果字符串里显式写了 `${WORK_DIR}` / `${CONFIG_DIR}`，则以当前 loader 注入的 scoped vars 值为准
- `${WORK_ROOT}` 只在 runtime 额外提供该变量的作用域里可用
- 绝对路径始终原样保留

## Overlay / 变更配置

`wfusion` 支持在 base config 之上叠加一个或多个 overlay 文件：

```bash
wfusion daemon \
  --config conf/wfusion.toml \
  --overlay conf/batch.toml \
  --overlay conf/local-dev.toml
```

当前规则：

- 按命令行顺序应用 overlay，后面的覆盖前面的
- TOML table 递归 merge
- 标量和数组整体替换
- `[vars]` 也参与 merge，因此 overlay 可以覆盖 base 中的同名变量
- 已知路径字段会先按 overlay 文件自己的目录解释，再折算到最终运行基准

当前会做路径折算的字段包括：

- `sinks`
- `work_root`
- `runtime.schemas`
- `runtime.rules`
- `logging.file`
- `[[sources]].path`

### `--work-dir`

`--work-dir` 当前同时承担两层职责：

- 作为运行时相对路径的最终基准目录
- 作为 loader scoped var `WORK_DIR` 的取值来源

因此以下两种写法都成立：

```bash
wfusion daemon --config conf/wfusion.toml --work-dir /path/to/project
```

```toml
sinks = "${WORK_DIR}/topology/wf_sinks"
```

以及：

```toml
[runtime]
schemas = "models/schemas/*.wfs"
rules = "models/rules/*.wfl"
```

上面第二种属于“显式 scoped var 表达”；第三种属于“隐式相对路径”。长期推荐更偏向显式表达，因为诊断和迁移更清晰。

例如 base:

```toml
mode = "daemon"

[runtime]
rules = "rules/base/*.wfl"

[vars]
CASE_PATH = "/srv/case-a"
```

overlay:

```toml
mode = "batch"

[runtime]
rules = "rules/replay/*.wfl"

[vars]
CASE_PATH = "/tmp/case-a"
```

最终结果等价于：

- `mode = "batch"`
- `runtime.rules = "rules/replay/*.wfl"`
- `CASE_PATH = "/tmp/case-a"`

## 路径解析

默认情况下，`wfusion.toml` 中的相对路径都相对于配置文件所在目录解析，例如：

- `runtime.schemas`
- `runtime.rules`
- `sinks`
- `[[sources]].path`
- `logging.file`
- `work_root`

如果需要临时改成相对于另一个目录运行，可以在 CLI 里传：

```bash
wfusion daemon --config conf/wfusion.toml --work-dir ..
```

此时上述相对路径会改为相对于 `--work-dir` 指定的目录解析。

## Sink 路由

告警输出通过 connector-based sink 路由系统配置：

```toml
sinks = "sinks"
```

目录结构（Connector 定义在 `sinks` 同级目录的 `connectors/sink.d/` 下）：

- `defaults.toml`
- `business.d/`
- `infra.d/`
- `../connectors/sink.d/`

输出格式取决于 sink：

- raw sink 通常输出扁平 JSON 行
- record sink 可直接输出 Arrow framed / Arrow IPC 等结构化格式

单个 sink 还可以声明输出字段投影与顺序：

```toml
[[sink_group.sinks]]
connect = "file_json"
fields = ["__wfu_rule_name", "__wfu_score", "sip", "fail_count"]

[sink_group.sinks.params]
file = "security_alerts.jsonl"
```

说明：

- `fields` 写在 `[[sink_group.sinks]]` 顶层，不在 `[sink_group.sinks.params]` 里
- 它控制该 sink 最终能看到哪些字段，以及字段顺序
- 未列出的字段不会发给该 sink
- 若配置了不存在的字段，运行时会报错
- 这是一层 reactor 侧输出投影；若某个 connector 自身也在 `params` 里定义 `fields`，两者语义不同

## 输出记录

告警链路在进入 sink 之前，会统一转换成结构化记录：

- 系统字段使用 `__wfu_` 前缀
- `yield (...)` 中的业务字段按原名展开
- 若业务字段与 `__wfu_` 前缀冲突，运行时直接报错

最终 alert 记录固定系统字段如下：

| 字段 | 说明 |
|------|------|
| `__wfu_id` | 确定性输出 ID |
| `__wfu_rule_name` | 规则名称 |
| `__wfu_score` | 风险评分 |
| `__wfu_entity_type` | 实体类型 |
| `__wfu_entity_id` | 实体标识 |
| `__wfu_origin` | 产出路径：`event` / `close:*` |
| `__wfu_close_reason` | close 原因；event 路径为空字符串 |
| `__wfu_fired_at` | 基于事件时间或 close 水位生成的业务时间 |
| `__wfu_emit_time` | 运行时实际发出该记录的时间 |
| `__wfu_summary` | 引擎生成的摘要 |

示例：

```json
{
  "__wfu_id": "1a2b3c4d",
  "__wfu_rule_name": "brute_force",
  "__wfu_score": 70.0,
  "__wfu_entity_type": "ip",
  "__wfu_entity_id": "10.0.0.1",
  "__wfu_origin": "close:timeout",
  "__wfu_close_reason": "timeout",
  "__wfu_fired_at": "2026-03-11T10:05:00.000Z",
  "__wfu_emit_time": "2026-03-11T10:05:00.123Z",
  "__wfu_summary": "rule=brute_force; scope=[sip=10.0.0.1]; step0=3.0; origin=close:timeout",
  "sip": "10.0.0.1",
  "fail_count": 5
}
```

### 中间 enriched 记录

如果某条规则的 `yield` 目标还会被下游规则继续消费，则应把它视为“中间 enriched 记录”，而不是最终告警。

这类记录推荐透传的系统字段只有：

- `__wfu_score`
- `__wfu_rule_name`
- `__wfu_entity_type`
- `__wfu_entity_id`

当某个 window 被下游规则消费时，这 4 个字段会被编译器自动视为该 window 的可用字段；下游规则可以直接写 `x.__wfu_score`，无需在 `.wfs` 中重复声明这些列。

默认不应暴露：

- `__wfu_fired_at`
- `__wfu_emit_time`
- `__wfu_origin`
- `__wfu_summary`

如果目标 window 定义了 `time` 列，runtime 会在用户未显式给该列赋值时，自动继承输入事件时间到该 time 列，供下游 `match<...>` 使用；这个时间不会额外生成新的 `__wfu_*` 字段。

另外，`__wfu_*` 是保留前缀，不能作为业务 `yield` 字段名手工写出。

中间 window 之间只能形成无环链路；禁止自回写或多规则循环回写。

### `yield_fields` 导出规则

- 标量字段按 schema 类型导出
- 若缺失 schema 类型信息，则按值推断：
  - number -> `float`
  - bool -> `bool`
  - string -> `chars`
- `array/*` 与未类型化 `array` 导出为数组（Arrow `Array` 类型），JSON sink 渲染为 JSON 数组，不降级为字符串

数组示例：

```json
{
  "ports": [22, 80, 443]
}
```

## 运行引擎

启动：

```bash
wfusion daemon --config wfusion.toml
```

启动流程：

1. 加载并校验 `wfusion.toml`
2. 解析 `.wfs`
3. 解析并编译 `.wfl`
4. 创建窗口缓冲区和规则执行器
5. 启动 sources
6. 启动事件调度循环

执行链：

```text
Source -> Router -> WindowRegistry -> RuleExecutor -> AlertDispatcher -> SinkDispatcher
```

热加载约定：

- 修改 `.wfl` 或 `[vars]` 后可热加载
- 修改 `.wfs` 需要重启
