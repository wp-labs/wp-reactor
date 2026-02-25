# avg/ — DNS 隧道检测

avg 聚合 + 算术 score 表达式。计算 DNS 响应的平均大小，异常大的响应可能表明 DNS 隧道。

## 目录结构

```
avg/
├── schemas/
│   └── dns.wfs              # Window schema: dns_events + dns_alerts
├── rules/
│   └── dns_tunnel.wfl       # 规则 + 内联测试
└── data/
    └── dns_events.ndjson    # replay 样本数据
```

## 规则说明

**规则**: `dns_tunnel`

```
events { d : dns_events }

match<sip:5m> {
    on event { d.resp_size | avg >= 500; }
} -> score(50.0 + 20.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `dns_events`（stream: dns_log） |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 5 分钟 |
| 聚合方式 | `avg` 平均值——计算响应大小的平均 |
| 事件阶段 | 平均响应大小 >= 500 字节 |
| Score | 算术表达式 `50.0 + 20.0` = 70.0 |
| Entity | `ip`, ID 为 `d.sip` |
| Yield | `dns_alerts`（sip, alert_type, detail） |

### 关键语法点

- **`d.resp_size | avg`**: 计算响应大小的平均值
- **算术 score**: `score(50.0 + 20.0)` 支持表达式计算
- **DNS 隧道原理**: 正常 DNS 响应通常 < 200 字节；隧道利用大响应（TXT 记录可达 4KB）传输数据

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `tunnel_detected` | 3 条记录平均响应大小 500+ | 1 hit, score=70.0 |
| `normal_dns` | 正常 DNS 响应（50-60 字节） | 0 hits |

## 运行

```bash
# 运行内联测试
wfl test rules/dns_tunnel.wfl --schemas "schemas/*.wfs"

# replay 样本数据
wfl replay rules/dns_tunnel.wfl --schemas "schemas/*.wfs" \
    --input data/dns_events.ndjson --event d

# 查看规则编译结果
wfl explain rules/dns_tunnel.wfl --schemas "schemas/*.wfs"
```

## Schema

`dns.wfs` 定义两个 window：

**dns_events**（输入）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| domain | chars | 查询域名 |
| query_type | chars | 查询类型（A/AAAA/TXT/MX 等） |
| resp_size | digit | 响应大小（字节） |
| event_time | time | 事件时间 |

**dns_alerts**（输出）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| alert_type | chars | 告警类型 |
| detail | chars | 告警详情 |

## 典型应用场景

- **DNS 隧道检测**: 通过异常大的响应识别 DNS 隧道
- **数据外泄**: DNS 隧道常用于绕过防火墙进行数据传输
- **C2 通信**: 检测通过 DNS 的命令与控制通信

## 扩展检测

```wfl
// 结合 TXT 记录和大响应
events { d : dns_events && query_type == "TXT" }

match<sip:5m> {
    on event {
        d | count >= 10;               // 频繁查询
        d.resp_size | avg >= 1000;     // 大响应
    }
}
```

## 可用聚合函数

| 函数 | 说明 | 返回值类型 |
|------|------|------------|
| `count` | 计数 | digit |
| `sum` | 求和（digit/float 字段） | digit/float |
| `avg` | 平均值 | float |
| `min` | 最小值 | 同输入 |
| `max` | 最大值 | 同输入 |
| `distinct \| count` | 去重计数 | digit |
