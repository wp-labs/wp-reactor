# distinct/ — 端口扫描检测

distinct 去重计数 + event filter。统计同一 IP 连接的不同目标端口数，超过阈值判定为端口扫描。

## 目录结构

```
distinct/
├── schemas/
│   └── network.wfs          # Window schema: conn_events + network_alerts
├── rules/
│   └── port_scan.wfl        # 规则 + 内联测试
└── data/
    └── conn_events.ndjson   # replay 样本数据
```

## 规则说明

**规则**: `port_scan`

```
events { c : conn_events && action == "syn" }

match<sip:5m> {
    on event { c.dport | distinct | count >= 10; }
} -> score(60.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `conn_events`（stream: netflow），过滤 `action == "syn"` |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 5 分钟 |
| 聚合方式 | `distinct` 去重计数——统计不同的目标端口数 |
| 事件阶段 | 同一 IP 访问 >= 10 个不同端口 |
| Score | 60.0 |
| Entity | `ip`, ID 为 `c.sip` |
| Yield | `network_alerts`（sip, alert_type, detail） |

### 关键语法点

- **`c.dport | distinct | count`**: 管道式聚合写法，先对 `dport` 去重，再计数
- **与 `count(c)` 的区别**: `count(c)` 统计事件条数，`c.dport | distinct | count` 统计不同端口数
- **复合 key**: 可用 `match<sip,dport:5m>` 按 IP+端口组合分组

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `scan_detected` | 同一 IP 访问 10 个不同端口 | 1 hit, score=60.0, entity_id=10.0.0.1 |
| `repeat_ports_no_trigger` | 同一 IP 只访问 2 个端口（重复访问） | 0 hits |
| `different_sip_isolated` | 两个 IP 各访问 2-3 个端口（分散） | 0 hits |

## 运行

```bash
# 运行内联测试
wfl test rules/port_scan.wfl --schemas "schemas/*.wfs"

# replay 样本数据
wfl replay rules/port_scan.wfl --schemas "schemas/*.wfs" \
    --input data/conn_events.ndjson --event c

# 查看规则编译结果
wfl explain rules/port_scan.wfl --schemas "schemas/*.wfs"
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
| protocol | chars | 协议（tcp/udp/icmp） |
| action | chars | 动作（syn/ack/rst/login_fail 等） |
| event_time | time | 事件时间 |

**network_alerts**（输出）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| alert_type | chars | 告警类型 |
| detail | chars | 告警详情 |

## 典型应用场景

- **端口扫描检测**: 攻击者对单个 IP 进行端口扫描时会触发大量 SYN 连接
- **横向移动检测**: 内网主机访问异常数量的不同端口
- **服务发现**: 识别网络中的活跃服务（反向使用，降低阈值）
