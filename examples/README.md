# WFL Examples

按场景组织的 WFL 规则示例，每个目录自包含 schemas、rules、data 等资源，可独立运行和学习。

## 目录结构

```
examples/
├── wfusion.toml              # WFusion 运行时配置
├── sinks/                    # 输出 sink 配置
│
├── count/                    # 场景 1: count 阈值
├── distinct/                 # 场景 2: distinct 去重计数
├── sum/                      # 场景 3: sum 聚合 + on close + tick
├── multi_step/               # 场景 4: 多步骤序列
├── avg/                      # 场景 5: avg 聚合 + 算术 score
└── close_modes/              # 场景 6: close trigger 三种模式
```

每个场景目录包含：

| 子目录 | 说明 |
|---|---|
| `schemas/` | Window schema 定义 (`.wfs`) |
| `rules/` | 规则文件，含内联测试 (`.wfl`) |
| `data/` | 用于 replay 的样本数据 (`.ndjson`) |
| `out/` | replay 输出目录 |
| `scenarios/` | 数据生成场景 (`.wfg`，部分场景有) |

## 场景说明

### 1. count/ — 暴力破解检测

基础 count 阈值 + event filter。统计同一 IP 的登录失败次数，超过阈值触发告警。

- **规则**: `brute_force.wfl` — `fail | count >= $FAIL_THRESHOLD`
- **Schema**: `security.wfs` — auth_events (syslog)
- **Score**: 70.0

```bash
cd examples/count
wfl test rules/brute_force.wfl --schemas "schemas/*.wfs" --var FAIL_THRESHOLD=3
```

### 2. distinct/ — 端口扫描检测

distinct 变换 + event filter。统计同一 IP 连接的不同目标端口数，超过阈值判定为端口扫描。

- **规则**: `port_scan.wfl` — `c.dport | distinct | count >= 10`
- **Schema**: `network.wfs` — conn_events (netflow)
- **Score**: 60.0

```bash
cd examples/distinct
wfl test rules/port_scan.wfl --schemas "schemas/*.wfs"
```

### 3. sum/ — 数据外泄检测

sum 聚合 + on close + tick。累计同一 IP 的传输字节数，事件阶段和窗口关闭阶段分别设置不同阈值。

- **规则**: `data_exfil.wfl` — `on event { c.bytes | sum >= 100000000; }` + `on close { c.bytes | sum >= 50000000; }`
- **Schema**: `network.wfs` — conn_events (netflow)
- **Score**: 85.0

```bash
cd examples/sum
wfl test rules/data_exfil.wfl --schemas "schemas/*.wfs"
```

### 4. multi_step/ — 链式攻击检测

多步骤序列匹配。要求同一 IP 先完成端口扫描（5 次 SYN），再进行暴力登录（3 次失败），两个条件同时满足才触发。

- **规则**: `chain_attack.wfl` — `scan | count >= 5` + `login | count >= 3`
- **Schema**: `network.wfs` — conn_events (netflow)
- **Score**: 90.0

```bash
cd examples/multi_step
wfl test rules/chain_attack.wfl --schemas "schemas/*.wfs"
```

### 5. avg/ — DNS 隧道检测

avg 聚合 + 算术 score 表达式。计算 DNS 响应的平均大小，异常大的响应可能表明 DNS 隧道。

- **规则**: `dns_tunnel.wfl` — `d.resp_size | avg >= 500`
- **Schema**: `dns.wfs` — dns_events (dns_log)
- **Score**: `50.0 + 20.0` = 70.0

```bash
cd examples/avg
wfl test rules/dns_tunnel.wfl --schemas "schemas/*.wfs"
```

### 6. close_modes/ — 窗口关闭模式

演示 WFL 三种窗口关闭触发方式：eos（流结束）、timeout（超时）、flush（刷新）。

- **规则**: `close_demo.wfl` — `on event { c | count >= 1; }` + `on close { c | count >= 1; }`
- **Schema**: `network.wfs` — conn_events (netflow)
- **Score**: 70.0

```bash
cd examples/close_modes
wfl test rules/close_demo.wfl --schemas "schemas/*.wfs"
```

## 运行全部测试

```bash
cd examples/count      && wfl test rules/brute_force.wfl  --schemas "schemas/*.wfs" --var FAIL_THRESHOLD=3
cd examples/distinct   && wfl test rules/port_scan.wfl    --schemas "schemas/*.wfs"
cd examples/sum        && wfl test rules/data_exfil.wfl   --schemas "schemas/*.wfs"
cd examples/multi_step && wfl test rules/chain_attack.wfl --schemas "schemas/*.wfs"
cd examples/avg        && wfl test rules/dns_tunnel.wfl   --schemas "schemas/*.wfs"
cd examples/close_modes && wfl test rules/close_demo.wfl  --schemas "schemas/*.wfs"
```

## Replay 示例

使用 `data/` 目录中的样本数据进行 replay：

```bash
cd examples/distinct
wfl replay rules/port_scan.wfl --schemas "schemas/*.wfs" --input data/conn_events.ndjson --event c
```

## 编写测试的注意事项

- digit 类型字段在 `row()` 中使用数字字面量：`bytes = 20000000`（不要加引号）
- `tick` 使用时长语法：`tick(6m);`、`tick(31s);`
- `flush` 通过 options 块指定：`options { close_trigger = flush; }`
- 匹配引擎按顺序执行：event 步骤必须先满足，close 步骤才会被评估
