# functions/ — 字符与集合函数组合示例

演示如何在一条规则中组合使用 `trim`、`replace`、`split`、`mvcount`、`mvdedup`、`mvjoin`。

## 目录结构

```
functions/
├── schemas/
│   └── security.wfs                # Window schema: auth_events + security_alerts
├── rules/
│   └── action_string_set_ops.wfl   # 函数组合规则
└── data/
    └── auth_events.ndjson          # replay 样本数据
```

## 规则说明

**规则**: `action_string_set_ops`

```wfl
match<sip:5m:fixed> {
  on event { e | count >= 1; }
  and close { e | count >= 2; }
} -> score(72.0)

yield security_alerts (
  token_count = mvcount(split(replace(trim(e.action), " ", ""), ",")),
  uniq_actions = mvjoin(mvdedup(split(replace(trim(e.action), " ", ""), ",")), "|")
)
```

| 字段 | 表达式 | 说明 |
|------|--------|------|
| `token_count` | `mvcount(split(replace(trim(e.action), " ", ""), ","))` | 先去首尾空白，再去空格，再按逗号切分并统计 token 数 |
| `uniq_actions` | `mvjoin(mvdedup(split(replace(trim(e.action), " ", ""), ",")), "|")` | 按逗号切分后去重并拼接为摘要字符串 |

## 运行

```bash
# 规则检查
wfl lint rules/action_string_set_ops.wfl --schemas "schemas/*.wfs"

# replay 样本数据
wfl replay rules/action_string_set_ops.wfl --schemas "schemas/*.wfs" \
  --input data/auth_events.ndjson --event e

# 查看编译结果
wfl explain rules/action_string_set_ops.wfl --schemas "schemas/*.wfs"
```
