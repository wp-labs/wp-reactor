# sum/ — 数据外泄检测

sum 聚合 + `and close` 双阶段匹配。累计同一 IP 的传输字节数，事件阶段检测大流量，关闭阶段确认总量达标才触发告警。

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
    on event  { c.bytes | sum >= 100000000; }      // 100MB
    and close { total: c.bytes | sum >= 50000000; } // 50MB
} -> score(85.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `conn_events`（stream: netflow） |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 10 分钟 |
| 事件阶段 | 累计流量 >= 100MB（标记 event_ok） |
| 关闭阶段 | 窗口关闭时总量 >= 50MB（close_ok） |
| 命中条件 | `event_ok && close_ok`，AND 模式 |
| Score | 85.0（高风险） |
| Entity | `ip`, ID 为 `c.sip` |
| Yield | `network_alerts`（sip, alert_type, detail） |

### 关键语法点

- **`c.bytes | sum`**: 对 `bytes` 字段求和，统计总流量
- **`and close`**: AND 模式——事件路径满足仅标记状态，不立即告警；窗口关闭时合并判定
- **双阈值设计**: 事件阈值较高（100MB）用于早期标记，关闭阈值较低（50MB）确保最终判定
- **`tick()`**: 测试中使用 `tick(11m)` 推进时间触发窗口关闭

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `exfil_close_timeout` | 5 条记录各 20MB，总计 100MB，tick 超时 | 1 hit, origin=close:timeout |
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

## 对比 count 与 distinct

| 聚合方式 | 适用场景 | 示例 |
|----------|----------|------|
| `count` | 统计事件条数 | 登录尝试次数 |
| `distinct \| count` | 统计不同值数量 | 访问的不同端口数 |
| `sum` | 累计数值字段 | 总传输字节数 |
| `avg` | 计算平均值 | 平均响应大小 |
