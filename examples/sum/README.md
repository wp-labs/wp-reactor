# sum/ — 数据外泄检测

sum 聚合 + `on close` OR 双路径触发。演示两条独立告警路径：突发流量立即告警 + 窗口关闭时总量告警。

## 目录结构

```
sum/
├── schemas/
│   └── network.wfs          # Window schema: conn_events + network_alerts
├── rules/
│   └── data_exfil.wfl       # 规则 + 内联测试
└── data/
    └── conn_events.ndjson   # replay 样本数据
```

## 规则说明

**规则**: `data_exfil`

```
events { c : conn_events }

match<sip:10m> {
    // OR 模式：两条路径独立触发
    on event  { burst: c.bytes | sum >= 100000000; }  // 100MB 立即告警
    on close  { total: c.bytes | sum >= 50000000; }   // 50MB 关闭时告警
} -> score(85.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `conn_events`（stream: netflow） |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 10 分钟 |
| **路径 1** | 突发流量 >= 100MB，**立即触发**（origin=`event`） |
| **路径 2** | 窗口关闭时总量 >= 50MB，**关闭时触发**（origin=`close:timeout`） |
| 命中条件 | OR 模式——任一条件满足即触发 |
| Score | 85.0（高风险） |
| Entity | `ip`, ID 为 `c.sip` |
| Yield | `network_alerts`（sip, alert_type, detail） |

### 关键语法点

- **`c.bytes | sum`**: 对 `bytes` 字段求和，统计总流量
- **`on close`**: OR 模式——事件路径与关闭路径**独立**触发，各自产生告警
- **`burst:` / `total:`**: 给聚合结果加标签，便于区分触发路径
- **双阈值设计**: 高阈值（100MB）用于即时告警，低阈值（50MB）用于窗口结束时兜底检查
- **`tick()`**: 测试中使用 `tick(11m)` 推进时间触发窗口关闭

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `exfil_close_timeout` | 5 条记录各 12MB，总计 60MB（满足 on close 50MB，不满足 on event 100MB），tick 超时 | 1 hit, origin=`close:timeout` |
| `exfil_burst_immediate` | 4 条记录各 25MB，总计 100MB（满足 on event 立即触发，窗口关闭时 on close 也触发） | 2 hits: origin=`event` + `close:eos` |
| `low_traffic` | 2 条记录共 3KB 流量 | 0 hits |

## 运行

```bash
# 运行内联测试
wfl test rules/data_exfil.wfl --schemas "schemas/*.wfs"

# replay 样本数据
wfl replay rules/data_exfil.wfl --schemas "schemas/*.wfs" \
    --input data/conn_events.ndjson --event c

# 查看规则编译结果
wfl explain rules/data_exfil.wfl --schemas "schemas/*.wfs"
```

## Schema

`network.wfs` 定义两个 window：

**conn_events**（输入）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| dip | ip | 目标 IP |
| dport | digit | 目标端口 |
| bytes | digit | 字节数 |
| protocol | chars | 协议 |
| action | chars | 动作 |
| event_time | time | 事件时间 |

**network_alerts**（输出）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| alert_type | chars | 告警类型 |
| detail | chars | 告警详情 |

## 典型应用场景

- **数据外泄检测**: 监控大流量出站连接
- **异常传输**: 识别短时间内大量数据传输
- **带宽滥用**: 检测异常带宽消耗行为

## 关闭模式对比

| 模式 | 语法 | 触发条件 | 适用场景 |
|------|------|----------|----------|
| 仅事件 | 省略 close | 条件满足立即触发 | 实时检测，立即告警 |
| **OR 模式** | `on close` | 事件/关闭**各自独立**触发 | 多路径兜底，本示例场景 |
| AND 模式 | `and close` | 两路径**同时满足**才触发 | 必须完整窗口周期确认 |

## 对比 count 与 distinct

| 聚合方式 | 适用场景 | 示例 |
|----------|----------|------|
| `count` | 统计事件条数 | 登录尝试次数 |
| `distinct \| count` | 统计不同值数量 | 访问的不同端口数 |
| `sum` | 累计数值字段 | 总传输字节数 |
| `avg` | 计算平均值 | 平均响应大小 |
