# conv/ — Top-N 结果集变换 (L3)

`conv` 后处理变换 + `fixed` 窗口。在固定 1 小时窗口内统计各 IP 的端口扫描数，窗口到期后通过 `conv { sort(-scan) | top(2) ; }` 对批量结果排序并只保留 Top-2 扫描者。

## 目录结构

```
conv/
├── schemas/
│   └── network.wfs          # Window schema: conn_events + network_alerts
├── rules/
│   └── top_scanners.wfl     # 规则 + 内联测试
└── data/
    └── conn_events.ndjson   # replay 样本数据
```

## 规则说明

**规则**: `top_port_scanners`

```wfl
events { c : conn_events && action == "syn" }

match<sip:1h:fixed> {
    on event {
        c | count >= 1;
    }
    and close {
        scan: c.dport | distinct | count >= 3;
    }
} -> score(80.0)

conv {
    sort(-scan) | top(2) ;
}
```

| 项目 | 说明 |
|------|------|
| 事件源 | `conn_events`（stream: netflow），过滤 `action == "syn"` |
| 分组键 | `sip`（源 IP） |
| 窗口 | **固定窗口** 1 小时（`fixed`） |
| 事件阶段 | 至少有 1 个事件（标记 event_ok） |
| 关闭阶段 | 不同端口数 >= 3 |
| 命中条件 | `event_ok && close_ok`，AND 模式 |
| **Conv** | `sort(-scan) \| top(2)`——按扫描数降序，只保留 Top-2 |
| Score | 80.0 |
| Entity | `ip`, ID 为 `c.sip` |
| Yield | `network_alerts`（sip, alert_type, detail） |

### 关键语法点

- **`fixed` 窗口**: 按时间对齐的固定窗口（如 0:00-1:00, 1:00-2:00）
- **`conv {}`**: 结果集后处理，仅作用于 `fixed` 窗口 + `on close` 的输出
- **管道操作**: `sort(-scan) | top(2)` 串联排序和截取
- **`scan:` 标签**: 给聚合结果命名，供 conv 引用

## conv 操作列表

| 操作 | 语法 | 说明 |
|------|------|------|
| sort | `sort(-field)` / `sort(+field)` | 按字段排序，`-` 降序，`+` 升序 |
| top | `top(N)` | 保留前 N 条 |
| dedup | `dedup(field)` | 按字段去重，保留首次出现的 |
| where | `where(expr)` | 布尔过滤 |

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `top2_scanners` | 3 个 IP 分别扫描 5/3/4 个端口 | 2 hits，保留 IP-A(5) 和 IP-C(4) |
| `below_threshold` | 仅 2 个端口 | 0 hits |
| `conv_mixed_qualifying` | 4 个 IP（5/4/3/2 端口） | 2 hits，非合格(2端口)被过滤 |

## 运行

```bash
# 运行内联测试
wfl test rules/top_scanners.wfl --schemas "schemas/*.wfs"

# replay 样本数据
wfl replay rules/top_scanners.wfl --schemas "schemas/*.wfs" \
    --input data/conn_events.ndjson --event c

# 查看规则编译结果（含 conv 展开）
wfl explain rules/top_scanners.wfl --schemas "schemas/*.wfs"
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

- **Top-N 告警**: 只关注最严重的 N 个实体，减少告警噪音
- **排行榜**: 每小时/每日的 Top 攻击者、Top 流量源
- **聚合报表**: 固定时间窗口的统计报表

## conv 组合示例

```wfl
conv {
    where(scan >= 10) |      // 过滤：只保留扫描 >=10 个端口的
    sort(-scan) |            // 按扫描数降序
    top(5) |                 // 保留 Top-5
    dedup(sip) ;             // 按 IP 去重
}
```

## 约束

- `conv` **仅**可与 `fixed` 窗口 + `on close` 配合使用
- `conv` + `sliding` 窗口会触发编译错误
- `conv` 操作在关闭阶段执行，不影响实时检测延迟
