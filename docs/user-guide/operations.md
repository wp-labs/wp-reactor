# 第三部分：运维与工具链

前两部分讲了核心概念和规则编写。这一部分讲怎么跑起来、怎么调试、怎么验证。

---

## 1. 启动引擎

### 1.1 常驻模式（daemon）

生产环境的标准模式，启动后持续监听 TCP 端口接收数据：

```bash
wfusion daemon --config wfusion.toml
```

要求配置中至少有一个启用的 source（任意类型）。带指标输出：

```bash
wfusion daemon --config wfusion.toml --metrics --metrics-interval 2s
```

### 1.2 批处理模式（batch）

离线回放文件，处理完毕后自动退出：

```bash
wfusion batch --config wfusion.toml --overlay conf/batch.toml
```

要求至少有一个 `type = "file"` 的 source，不允许启用任何非 `file` 类型的 source。

### 1.3 信号处理

| 信号 | 行为 |
|------|------|
| `SIGINT` (Ctrl+C) | 触发优雅关闭 |
| `SIGTERM` | 触发优雅关闭 |

优雅关闭流程：停止 Receiver → flush 所有规则状态机 → drain 告警管道 → 关闭 sink → 退出。保证零告警丢失。

---

## 2. 配置诊断

配置诊断能力已从 `wfusion config` 迁移到 `wfadm conf`（`wfadm` 是同级 `warp-fusion` 仓库产出的项目管理 CLI），当前提供 `conf diff` 与 `conf update`。

### 2.1 配置差异对比

比较两组加载参数下的配置差异：

```bash
wfadm conf diff \
    --config conf/wfusion.toml \
    --overlay conf/dev.toml \
    --to-overlay conf/batch.toml
```

左侧参数：`--config` / `--overlay` / `--var` / `--work-dir`；右侧参数：`--to-config` / `--to-overlay` / `--to-var` / `--to-work-dir`。加 `--expanded` 比较变量展开后的差异；`--path-prefix` 只看某个子树。

### 2.2 追踪配置来源

`--expanded` 模式下，差异项的 `old_origin` / `new_origin` 显示“最终展开值来自哪里”：

- 纯文件来源时仍显示文件路径
- 变量驱动时显示 `<cli:KEY>` / `<env:KEY>` / `<builtin:WORK_DIR>` / `<default:FOO>`
- 如果一个最终值同时由多个来源拼接而成，则显示 `<mixed:...>`

### 2.3 Overlay 叠加机制

`--overlay` 可以多次指定，后指定的覆盖先指定的：

```bash
wfadm conf diff \
    --config conf/wfusion.toml \
    --overlay conf/dev.toml \
    --overlay conf/dev-jack.toml
```

优先级：`dev-jack > dev > base`。

> 旧版 `wfusion config render / origins / vars` 子命令已移除。变量解析顺序、scoped var（`CONFIG_DIR` / `WORK_DIR`）与路径语义仍见 [运行时配置](./runtime-config.md)。

---

## 3. wfl — 规则开发工具

`wfl` 是规则开发的核心工具，不需要启动引擎即可使用。

### 3.1 explain — 理解编译结果

```bash
wfl explain rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --var FAIL_THRESHOLD=3
```

输出编译后的执行计划：事件绑定、匹配步骤、关闭模式、评分公式、yield 字段。用于理解"编译器把你的规则翻译成了什么"。

### 3.2 lint — 静态检查

```bash
wfl lint rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --var FAIL_THRESHOLD=3
```

检查项包括：

| 检查 | 类别 | 说明 |
|------|------|------|
| Window 引用是否存在 | Error | use 导入的 window 是否定义了 |
| 字段引用是否正确 | Error | alias.field 是否匹配 schema |
| 类型兼容性 | Error | 比较操作两边类型是否匹配 |
| 多 key 警告 | Warning | match key >= 4 个字段时警告高基数风险 |
| 缺少 limits | Warning | 建议每条规则声明资源预算 |

`contract_version` 不是 lint 警告：只有 `yield @vN` 存在且与 `meta { contract_version }` 不一致时，checker 会报 Error（T51）。

### 3.3 fmt — 代码格式化

```bash
wfl fmt rules/brute_force.wfl        # 预览格式化结果
wfl fmt -w rules/*.wfl               # 原地写入
wfl fmt --check rules/*.wfl          # CI 模式：只检查，不一致则报错
```

### 3.4 test — 运行契约测试

```bash
wfl test rules/brute_force.wfl \
    --schemas "schemas/*.wfs"
```

运行规则中的 `test { ... }` 块。每条测试可验证：

- `hits == N`：命中次数
- `hit[i].score == value`：告警评分
- `hit[i].origin == "event"` 或 `"close:timeout"`：触发来源
- `hit[i].entity_type` / `hit[i].entity_id`：实体信息

加 `--shuffle --runs N` 做随机乱序测试（验证事件顺序不敏感的规则）。

### 3.5 replay — 离线回放

```bash
wfl replay rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --input test_data/events.jsonl \
    --var FAIL_THRESHOLD=3
```

用 NDJSON 数据文件模拟事件注入，不需要手动指定 alias（按 schema 自动匹配）。注意离线模式没有 window store，join 和 window.has() 不可用。

### 3.6 verify — 对拍验证

一步完成 `replay + verify`，将实际告警与期望告警对拍：

```bash
wfl verify --case brute_force --data-dir data
```

对拍时自动比较每个告警的字段级差异，输出 markdown 格式的差异报告。这在规则重构时特别有用——确保新规则产出与原规则一致。

---

## 4. wfgen — 测试数据生成

`wfgen` 生成模拟事件流，用于压测和验证。输入是 `.wfg` 场景文件。

### 4.1 场景文件示例

```wfg
use "schemas/security.wfs"
use "rules/brute_force.wfl"

#[duration=30m]
scenario brute_force_detect<seed=42> {
    traffic {
        stream auth_events gen 200/s        # 每秒 200 条背景流量
    }

    injection {
        hit<30%> auth_events {              # 30% 概率注入命中流量
            sip seq {
                use(action="failed") with(3)     # 依次注入 3 次失败
            }
        }
    }

    expect {
        hit(brute_force_then_scan) >= 95%   # 期望命中率 >= 95%
    }
}
```

### 4.2 子命令

**生成数据：**

```bash
wfgen gen \
    --scenario examples/count/scenarios/brute_force.wfg \
    --format jsonl \
    --out out/
```

**生成并直接发送到引擎：**

```bash
wfgen gen \
    --scenario examples/count/scenarios/brute_force.wfg \
    --format jsonl \
    --out out/ \
    --send --addr 127.0.0.1:9800
```

**场景校验：**

```bash
wfgen lint examples/count/scenarios/brute_force.wfg
```

**告警对拍验证：**

```bash
wfgen verify \
    --actual out/actual_alerts.jsonl \
    --expected out/brute_force.except.jsonl \
    --meta out/brute_force.except.meta.jsonl
```

**持续压测：**

```bash
wfgen bench \
    --scenario examples/count/scenarios/brute_force.wfg \
    --duration 5m \
    --send --addr 127.0.0.1:9800
```

---

## 5. 指标与监控

### 5.1 终端指标快照

启动时加 `--metrics` 参数：

```bash
wfusion daemon --config wfusion.toml --metrics --metrics-interval 2s
```

定期输出到日志的关键指标（`summary_line`）：

| 指标 | 含义 |
|------|------|
| `rx_rows` | 已接收行数 |
| `routed` | 已路由到窗口的行数 |
| `dropped_late` | 迟到丢弃数 |
| `matches` | 规则命中数 |
| `alerts` | 告警输出数 |
| `window_bytes` | 窗口内存占用 |

另有按周期的速率表（`interval_table`），列包括 `row/s`、`late/s`、`rules/s`、`sm/s`、`memory`、`out/s`。这些终端快照由 `[metrics] console_output`（默认 `true`）控制；Prometheus 导出和 monitor-channel 快照不受该开关影响。

### 5.2 指标导出（monitor sink NDJSON）

```toml
[metrics]
enabled = true
prometheus_listen = "0.0.0.0:9091"
report_interval = "15s"
```

当前实现通过 monitor sink 按 `report_interval` 周期导出 NDJSON 指标记录（每条记录带 `stage` / `name` / label / value），不暴露 Prometheus HTTP 端点；`prometheus_listen` 目前只写入日志。

| stage | name | 含义 |
|-------|------|------|
| `receiver` | `connections_total` / `frames_total` / `rows_total` | 连接、帧、行计数 |
| `receiver` | `decode_errors_total` / `read_errors_total` / `window_miss_total` | 解码/读取/窗口 miss 计数 |
| `router` | `route_calls_total` / `delivered_total` / `dropped_late_total` / `skipped_non_local_total` | 路由与丢弃计数 |
| `rule` | `events_total` / `matches_total` / `instances`（按 rule） | 规则事件/命中/实例数 |
| `alert` | `dispatch_total` / `emitted_total` / `channel_send_failed_total` / `sink_dispatch_failed_total` / `channel_full_total` | 告警分发计数 |
| `window` | `memory_bytes` / `window_capacity_bytes` / `rows` / `batches` / `append_total` / `evict_total` / `late_total`（按 window） | 窗口状态 |
| `evictor` | `sweeps_total` / `time_evicted_total` / `memory_evicted_total` | 驱逐计数 |
| 时延 | `rule.scan_timeout_seconds` / `rule.flush_seconds` / `receiver.decode_seconds` / `alert.dispatch_seconds` / `event.e2e_latency_seconds` | 直方图，按 `_p50` / `_p99` 两条记录导出 |

### 5.3 关闭时的 RunSummary

引擎关闭时，metrics 任务会打印 `RunSummary` 汇总表（不是某个 `config` 子命令的输出）：

```
stat     row/s  late/s  rules/s  sm/s  memory  out/s
avg      ...    ...     ...      ...   ...     ...
max      ...    ...     ...      ...   ...     ...
total    rows / late / rules / sm_delta / out
```

列包括 `row/s`、`late/s`、`rules/s`、`sm/s`、`memory`、`out/s`（avg/max）以及累计总量，没有按规则的明细行。

---

## 6. 日志与调试

### 6.1 日志配置

```toml
[logging]
level = "info"              # trace | debug | info | warn | error
format = "plain"            # plain | json
file = "logs/wf-engine.log"

[logging.modules]
"wf_runtime::receiver" = "debug"
"wf_core::rule::match_engine" = "trace"
```

### 6.2 关键日志事件

| 日志事件 | 含义 | 关注点 |
|----------|------|--------|
| `WarpFusion reactor started` | 引擎启动成功 | listen 地址是否正确 |
| `cursor gap detected` | RuleTask 的 cursor 落后于 eviction | 窗口数据被提前淘汰，可能丢失事件 |
| `reason = "dropped_late"` | 迟到数据被丢弃 | 检查 watermark / allowed_lateness 配置 |
| `sink dispatch error` | sink 写入失败 | 检查磁盘空间、文件权限 |

### 6.3 常见问题排查

**规则不命中？**
```bash
# 1. 检查规则语法和语义
wfl lint rules/my_rule.wfl --schemas "schemas/*.wfs"

# 2. 理解编译结果
wfl explain rules/my_rule.wfl --schemas "schemas/*.wfs"

# 3. 用测试数据离线验证
wfl replay rules/my_rule.wfl --schemas "schemas/*.wfs" \
    --input test_events.jsonl
```

**配置不生效？**
```bash
# 1. 对比最终合并配置（base + overlay）
wfadm conf diff --config wfusion.toml --overlay dev.toml

# 2. 只看 window 子树（--path-prefix）
wfadm conf diff --config wfusion.toml --path-prefix window

# 3. 查看变量展开后的差异（--expanded，含来源标记）
wfadm conf diff --config wfusion.toml --var CASE_PATH=/tmp/a \
    --to-var CASE_PATH=/tmp/b --expanded
```

**内存增长过快？**
- 检查 `max_window_bytes` 和 `max_total_bytes` 是否合理
- 检查 `evict_interval` 是否太短导致驱逐来不及
- 检查 scope key 基数——`match<sip,username,dport:5m>` 三字段组合可能产生大量实例
- 查看 `rule.instances` 指标（monitor sink 按 rule 输出）判断是否有状态机膨胀

**告警丢失？**
- 检查 sink 路由：yield_target 是否被至少一个 business group 覆盖
- 检查 error_group 配置：sink 写入失败时是否转发到 error group
- 流畅关闭后再退出，不要 `kill -9`

---

## 7. E2E 验证

推荐的完整验证链路：

```bash
# 1. 启动引擎（批处理模式）
wfusion batch --config conf/wfusion.toml --overlay conf/batch.toml \
    --work-dir /path/to/project &

# 2. 生成并发送测试数据
wfgen gen --scenario scenarios/test_case.wfg \
    --format jsonl --out out/ --send --addr 127.0.0.1:9800

# 3. 对拍验证
wfgen verify \
    --actual out/actual_alerts.jsonl \
    --expected out/expected.jsonl \
    --meta out/expected.meta.jsonl
```

或直接运行内置 E2E 测试（在 `../warp-fusion` 仓库中）：

```bash
cargo test -p wfgen e2e_datagen_brute_force -- --nocapture
```

该测试自动完成：生成事件 → 启动 wfusion → TCP 发送 → 告警对拍。

---

## 8. 下一步

- [核心概念与处理过程](./core-concepts.md) — 回到第一部分，理解引擎原理
- [规则编写指南](./rule-writing.md) — 回到第二部分，学习规则语法
- [语言参考](./language-reference.md) — 完整语法规范
