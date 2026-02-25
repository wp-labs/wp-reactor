# multi_step/ — 链式攻击检测

多步骤序列匹配。要求同一 IP 先完成端口扫描（5 次 SYN），再进行暴力登录（3 次失败），两个条件同时满足才触发。

## 目录结构

```
multi_step/
├── schemas/
│   └── network.wfs          # Window schema: conn_events + network_alerts
├── rules/
│   └── chain_attack.wfl     # 规则 + 内联测试
└── data/
    └── conn_events.ndjson   # replay 样本数据
```

## 规则说明

**规则**: `chain_attack`

```
events {
    scan  : conn_events && action == "syn"
    login : conn_events && action == "login_fail"
}

match<sip:30m> {
    on event {
        scan  | count >= 5;
        login | count >= 3;
    }
} -> score(90.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `conn_events`（stream: netflow），两个别名：`scan`（syn）和 `login`（login_fail） |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 30 分钟 |
| 事件阶段 | 同一 IP 满足：`scan >= 5` **且** `login >= 3` |
| 逻辑关系 | 多步骤 AND 关系——所有条件必须同时满足 |
| Score | 90.0（严重风险） |
| Entity | `ip`, ID 为 `scan.sip` |
| Yield | `network_alerts`（sip, alert_type, detail） |

### 关键语法点

- **多事件源**: `events {}` 中声明多个别名，每个可独立过滤
- **多步骤匹配**: `on event` 中多个 step 按顺序求值，全部满足才触发
- **同源关联**: 所有步骤共享同一个 `match<key>` 分组，确保是同一实体的行为序列
- **OR 分支**: 可用 `step_a || step_b` 实现任一分支完成即推进

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `full_chain` | 5 次扫描 + 3 次登录失败（同一 IP） | 1 hit, score=90.0 |
| `scan_only` | 仅 5 次扫描，无登录失败 | 0 hits |

## 运行

```bash
# 运行内联测试
wfl test rules/chain_attack.wfl --schemas "schemas/*.wfs"

# replay 样本数据
wfl replay rules/chain_attack.wfl --schemas "schemas/*.wfs" \
    --input data/conn_events.ndjson

# 查看规则编译结果
wfl explain rules/chain_attack.wfl --schemas "schemas/*.wfs"
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
| action | chars | 动作（syn/login_fail 等） |
| event_time | time | 事件时间 |

**network_alerts**（输出）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| alert_type | chars | 告警类型 |
| detail | chars | 告警详情 |

## 典型应用场景

- **APT 攻击链**: 扫描 → 渗透 → 横向移动 → 数据窃取
- **入侵检测**: 多阶段攻击行为的关联识别
- **攻击取证**: 还原攻击者的完整行为序列

## 扩展思路

```wfl
// 三阶段攻击链
match<sip:1h> {
    on event {
        scan   | count >= 5;      // 阶段 1: 端口扫描
        exploit | count >= 1;     // 阶段 2: 漏洞利用
        exfil  | sum >= 1000000;  // 阶段 3: 数据外泄
    }
}
```
