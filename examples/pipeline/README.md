# pipeline/ — 多级管道 `|>` (M28.5)

演示两级规则管道：第一阶段按 `(sip, username)` 在固定 5 分钟窗口产出“失败突发”摘要（失败 >= 3），第二阶段按 `sip` 在固定 30 分钟窗口统计 `_in` 中的突发用户名数（>= 2）并告警。

## 目录结构

```
pipeline/
├── schemas/
│   └── security.wfs              # Window schema: auth_events + security_alerts
├── rules/
│   └── repeated_fail_bursts.wfl  # 两级管道规则
└── data/
    └── auth_events.ndjson        # replay 样本数据
```

## 规则说明

**规则**: `repeated_fail_bursts`

```wfl
match<sip,username:5m:fixed> {
  on event { e | count >= 1; }
  and close { burst: e | count >= 3; }
}
|> match<sip:30m:fixed> {
  on event { _in | count >= 1; }
  and close { users: _in.username | distinct | count >= 2; }
} -> score(85.0)
```

| 项目 | 说明 |
|------|------|
| Stage-1 | 输入 `auth_events`，按 `match<sip,username:5m:fixed>` 聚合，窗口 close 时 `count(fail) >= 3` 产出该用户名的一次 burst |
| Stage-2 | 输入 `_in`（Stage-1 输出），按 `match<sip:30m:fixed>` 聚合，窗口 close 时 `distinct(_in.username) >= 2` 触发 |
| Entity | `entity(ip, _in.sip)` |
| Yield | 输出 `security_alerts(sip, fail_count, message)` |
| Score | 85.0 |

## 验证方式

当前示例推荐使用 `replay` 做端到端验证（原始 `auth_events` -> stage-1 -> stage-2）。

## 运行

```bash
# replay 样本数据
wfl replay rules/repeated_fail_bursts.wfl --schemas "schemas/*.wfs" \
    --input data/auth_events.ndjson --event e

# 查看管道展开后的编译计划
wfl explain rules/repeated_fail_bursts.wfl --schemas "schemas/*.wfs"
```

## 关键语法点

- `|>`：将前一 stage 命中结果作为下一 stage 的输入流
- `_in`：编译器注入的保留别名，代表上游 stage 输出
- 非最终 stage 不写 `score/entity/yield`，最终 stage 统一产出告警
