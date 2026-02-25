# close_modes/ — 窗口关闭模式演示

演示 WFL 三种窗口关闭触发方式：`eos`（流结束）、`timeout`（超时）、`flush`（刷新）。

## 目录结构

```
close_modes/
├── schemas/
│   └── network.wfs          # Window schema: conn_events + network_alerts
├── rules/
│   └── close_demo.wfl       # 规则 + 内联测试
└── data/
    └── conn_events.ndjson   # replay 样本数据
```

## 规则说明

**规则**: `close_demo`

```
events { c : conn_events }

match<sip:5m> {
    on event { c | count >= 1; }
    and close { total: c | count >= 1; }
} -> score(70.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `conn_events`（stream: netflow） |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 5 分钟 |
| 事件阶段 | 至少有 1 个事件 |
| 关闭阶段 | 窗口关闭时至少有 1 个事件 |
| 命中条件 | `event_ok && close_ok`，AND 模式 |
| Score | 70.0 |
| Entity | `ip`, ID 为 `c.sip` |
| Yield | `network_alerts`（sip, alert_type, detail） |

### 关键语法点

- **`and close`**: AND 模式——关闭时合并判定
- **`close_reason`**: 关闭时可访问的上下文变量，取值为 `timeout`/`flush`/`eos`
- **三种触发方式**:
  - `eos`: 输入流正常结束（end-of-stream）
  - `timeout`: 窗口到期（5 分钟无新事件）
  - `flush`: 显式刷新（运维/热加载时触发）

## 内联测试

| 测试名 | 场景 | 触发方式 | 预期 |
|--------|------|----------|------|
| `close_eos` | 单条记录，测试结束 | eos | 1 hit, origin=close:eos |
| `close_timeout` | 单条记录 + tick(6m) | timeout | 1 hit, origin=close:timeout |
| `close_flush` | 单条记录 + options flush | flush | 1 hit, origin=close:flush |

## 运行

```bash
# 运行内联测试
wfl test rules/close_demo.wfl --schemas "schemas/*.wfs"

# 单独运行特定测试
wfl test rules/close_demo.wfl --schemas "schemas/*.wfs" --test close_timeout

# replay 样本数据
wfl replay rules/close_demo.wfl --schemas "schemas/*.wfs" \
    --input data/conn_events.ndjson --event c
```

## 关闭模式对比

| 模式 | 语法 | 事件路径 | 关闭路径 | 适用场景 |
|------|------|----------|----------|----------|
| 仅事件 | 省略 close | 满足即触发 | 无 | 实时检测，立即告警 |
| OR | `on close` | 独立触发 | 独立触发 | 事件/关闭各产生告警 |
| AND | `and close` | 标记状态 | 合并判定 | 必须完整窗口才告警 |

## 关闭原因与告警来源

| 触发方式 | `close_reason` | `origin` 字段值 |
|----------|----------------|-----------------|
| 流结束 | `eos` | `close:eos` |
| 超时 | `timeout` | `close:timeout` |
| 刷新 | `flush` | `close:flush` |

## 生产建议

```wfl
// 推荐：按原因分流，避免 flush/eos 误报
match<sip:5m> {
    on event { ... }
    and close {
        // 仅 timeout 触发，忽略 flush/eos
        close_reason == "timeout";
        ...
    }
}
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
