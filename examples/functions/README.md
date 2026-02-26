# functions/ — 函数组合与 Top50 函数可验证示例

演示两类函数示例：

1. 字符与集合函数组合（原有）
2. Top50 函数补齐后的可验证示例（新增 20 个函数）

## 目录结构

```
functions/
├── schemas/
│   └── security.wfs                # Window schema: auth_events + security_alerts
├── rules/
│   └── action_string_set_ops.wfl   # 函数组合规则
│   └── top50_function_showcase.wfl # Top50 函数示例规则
└── data/
    └── auth_events.ndjson          # 原有 replay 样本数据
    └── top50_functions.ndjson      # Top50 示例数据
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

## Top50 函数示例（新增）

**规则**: `top50_function_showcase`

覆盖并可验证以下函数：

- 数值：`abs` `round` `ceil` `floor` `sqrt` `pow` `log` `exp` `clamp` `sign` `trunc` `is_finite`
- 字符串：`ltrim` `rtrim` `concat` `indexof` `replace_plain` `startswith_any` `endswith_any`
- 空值：`coalesce` `isnull` `isnotnull`
- 多值：`mvsort` `mvreverse`
- 时间：`strftime` `strptime`

### 运行

```bash
# 规则检查
wfl lint rules/top50_function_showcase.wfl --schemas "schemas/*.wfs"

# 运行内联 contract（可验证）
wfl test rules/top50_function_showcase.wfl --schemas "schemas/*.wfs"

# replay 示例数据
wfl replay rules/top50_function_showcase.wfl --schemas "schemas/*.wfs" \
  --input data/top50_functions.ndjson --event e
```

### 验证点（输出字段）

可在输出中核对（字段在 `function_showcase_alerts` window）：

- `round_v = 12.35`
- `pow_v = 256`
- `log_v = 2`
- `clamp_v = 100`
- `replace_plain_v = "a-b-c"`
- `coalesce_v = "fallback"`
- `isnull_v = true`
- `mvsort_v = ["a","b","c"]`
- `mvreverse_v = ["c","a","b"]`

`top50_function_showcase.wfl` 内置了 `test top50_function_showcase_contract`，
可直接通过 `wfl test` 验证上述关键字段。
