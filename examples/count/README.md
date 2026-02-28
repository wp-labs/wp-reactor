# count/ — 暴力破解检测

基础 count 阈值 + event filter + and close 双阶段匹配。统计同一 IP 的登录失败次数，超过阈值且窗口关闭时仍有失败记录才触发告警。

## 目录结构

```
count/
├── schemas/
│   └── security.wfs         # Window schema：auth_events + security_alerts
├── rules/
│   └── brute_force.wfl      # 规则 + 内联测试
├── data/
│   └── auth_events.ndjson   # replay 样本数据
└── scenarios/
    └── brute_force.wfg       # 新语法场景（stream-first）
```

## 规则说明

**规则**: `brute_force_then_scan`

```
events { fail : auth_events && action == "failed" }

match<sip:5m> {
    on event { fail | count >= ${FAIL_THRESHOLD:3}; }
    and close { fail | count >= 1; }
} -> score(70.0)
```

| 项目 | 说明 |
|------|------|
| 事件源 | `auth_events`（stream: syslog），过滤 `action == "failed"` |
| 分组键 | `sip`（源 IP） |
| 窗口 | 滑动窗口 5 分钟 |
| 事件阶段 | 同一 IP 登录失败 >= 3 次（可通过 `$FAIL_THRESHOLD` 变量配置） |
| 关闭阶段 | `and close`（AND 模式）——窗口关闭时失败记录 >= 1 |
| 命中条件 | `event_ok && close_ok`，仅在窗口关闭时发出单次告警 |
| Score | 70.0 |
| Entity | `ip`, ID 为 `fail.sip` |
| Yield | `security_alerts`（sip, fail_count, message） |

### 关键语法点

- **`${FAIL_THRESHOLD:3}`**: 变量预处理，支持环境变量覆盖，默认值为 3
- **`and close`**: AND 模式——事件路径满足后不立即发出告警，仅标记 `event_ok = true`；窗口关闭时判定 `close_ok`，两者都满足才触发。与 `on close`（OR 模式，两路径独立触发）不同
- **`fmt()`**: 格式化字符串函数，用于生成告警 message
- **`count(fail)`**: 在 yield 中引用聚合结果

## 内联测试

| 测试名 | 场景 | 预期 |
|--------|------|------|
| `close_hit` | 3 次失败（同一 IP），触发 eos 关闭 | 1 hit, score=70.0, origin=close:eos |
| `below_threshold` | 2 次失败（低于阈值 3） | 0 hits |

## 运行

```bash
# 运行内联测试
wfl test rules/brute_force.wfl --schemas "schemas/*.wfs" --var FAIL_THRESHOLD=3

# 自定义阈值
wfl test rules/brute_force.wfl --schemas "schemas/*.wfs" --var FAIL_THRESHOLD=5

# replay 样本数据
wfl replay rules/brute_force.wfl --schemas "schemas/*.wfs" \
    --input data/auth_events.ndjson --event fail

# 查看规则编译结果
wfl explain rules/brute_force.wfl --schemas "schemas/*.wfs" --var FAIL_THRESHOLD=3

# 语法检查
wfl check rules/brute_force.wfl --schemas "schemas/*.wfs" --var FAIL_THRESHOLD=3
```

## Schema

`security.wfs` 定义两个 window：

**auth_events**（输入）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| username | chars | 用户名 |
| action | chars | 操作类型（login/logout/failed） |
| event_time | time | 事件时间 |

**security_alerts**（输出）:

| 字段 | 类型 | 说明 |
|------|------|------|
| sip | ip | 源 IP |
| fail_count | digit | 失败次数 |
| message | chars | 告警描述 |

## 样本数据

`data/auth_events.ndjson` 包含 6 条 syslog 认证事件，涉及 3 个 IP：
- `10.0.0.1`: 4 次 failed（超过阈值）
- `10.0.0.2`: 1 次 success（不匹配 filter）
- `10.0.0.3`: 1 次 failed（低于阈值）

## 数据生成

`scenarios/brute_force.wfg` 使用 wfgen 生成大规模测试数据：

```bash
wfgen gen --scenario scenarios/brute_force.wfg --format jsonl --out data/
```

场景参数：10 分钟时间跨度，基础速率约 180/s（`100/s + wave(base=80/s, ...)`），并注入 30% hit / 10% near_miss / 60% miss 样本。

该场景采用新语法（stream-first + hit/near_miss/miss + seq/use/with）。
