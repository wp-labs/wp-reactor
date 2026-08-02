# sinks/ — 告警输出目标配置

WarpFusion 的告警输出采用 connector-based sink 路由。`sinks/` 目录存放路由组（business/infra），Connector 定义放在同级 `connectors/sink.d/` 下。

## 目录结构

```
sinks/
├── defaults.toml            # 全局默认标签
├── business.d/              # 业务路由组（按 yield target window 通配符匹配）
│   ├── security.toml        #   windows = ["security_*"] → 输出 security_alerts.jsonl
│   └── catch_all.toml       #   windows = ["*"] → 兜底输出 all.jsonl
└── infra.d/                 # 基础设施组
    ├── default.toml         #   __default 兜底（未匹配任何业务组时）
    └── error.toml           #   __error 容错（写入失败时）

connectors/
└── sink.d/                  # Connector 定义（id + type + 默认参数）
    └── file_json.toml       #   id = "file_json"，type = "file"
```

## 路由逻辑

```
OutputRecord → to_data_record() → SinkDispatcher.dispatch(yield_target, DataRecord)
        ├─ 按 yield target window 通配符匹配 business group
        ├─ 无匹配 → __default group（兜底）
        └─ 写入失败 → __error group（容错）
```

`[[sink_group.sinks]]` 里的 `connect` 引用 `connectors/sink.d/` 中定义的 connector id。

## 业务路由组

`business.d/security.toml`：

```toml
[sink_group]
name = "security_output"
windows = ["security_*"]

[[sink_group.sinks]]
connect = "file_json"
name = "sec_file"

[sink_group.sinks.params]
file = "security_alerts.jsonl"
```

- `windows` 是 yield target window 名的 wildmatch 模式，`security_*` 匹配所有 `security_` 开头的输出 window
- 每个 sink 可写 `params` 覆盖 connector 默认参数（只有 connector `allow_override` 声明的 key 才能覆盖）

## Connector 定义

`connectors/sink.d/file_json.toml`：

```toml
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["base", "file", "sync"]

[connectors.params]
fmt = "json"
base = "alerts"
file = "default.jsonl"
sync = false
```

`id` 是规则/sink 引用名；`type` 决定输出编码（`file` 类型当前输出 JSON Lines）。

## 输出记录

进入 sink 的每条告警是结构化 `DataRecord`，系统字段带 `__wfu_` 前缀，`yield (...)` 里的业务字段按原名展开。JSON Lines 输出示例：

```json
{"__wfu_id":"308a0bfb8ebc3787","__wfu_rule_name":"port_scan","__wfu_score":60.0,"__wfu_entity_type":"ip","__wfu_entity_id":"10.0.0.1","__wfu_origin":"event","__wfu_fired_at":"2026-01-01T00:00:09.000Z","__wfu_emit_time":"...","__wfu_summary":"rule=port_scan; scope=[sip=10.0.0.1]; step0=10.0; origin=event"}
```

如需按字段投影/排序输出，可在 `[[sink_group.sinks]]` 顶层写 `fields = [...]`：

```toml
[[sink_group.sinks]]
connect = "file_json"
fields = ["__wfu_rule_name", "__wfu_score", "sip", "fail_count"]

[sink_group.sinks.params]
file = "security_alerts.jsonl"
```

未列出的字段不会发给该 sink；配置了不存在的字段运行时会报错。

## 故障排查

| 问题 | 排查方向 |
|------|----------|
| 告警未输出 | 检查 yield target 是否被至少一个 business group 的 `windows` 匹配 |
| 兜底/容错 | 确认 `infra.d/default.toml`（`__default`）与 `infra.d/error.toml`（`__error`）存在 |
| 字段缺失 | 检查 `fields` 投影是否遗漏，或 connector `allow_override` 是否允许该参数覆盖 |
