# sinks/ — 告警输出目标配置

WarpFusion 支持将告警输出到多种目标（Sinks），包括文件、HTTP 端点、Syslog 等。本目录包含 Sink 配置的示例。

## 目录结构

```
sinks/
├── defaults.toml            # 全局默认配置
├── business.d/              # 业务告警 sinks
├── infra.d/                 # 基础设施告警 sinks
└── sink.d/                  # 通用 sinks
```

## 配置结构

Sink 配置采用 TOML 格式，支持多目录组织：

| 目录 | 用途 |
|------|------|
| `business.d/` | 业务风险告警（安全事件、用户行为等） |
| `infra.d/` | 运维告警（系统状态、性能指标等） |
| `sink.d/` | 通用输出目标 |

## 默认配置

`defaults.toml`:

```toml
tags = ["env:dev"]
```

## Sink 类型

### File Sink（文件输出）

```toml
[sink.security_alerts_file]
type = "file"
path = "/var/log/warpfusion/alerts.jsonl"
format = "jsonl"           # jsonl | json | csv
```

### HTTP Sink（Webhook）

```toml
[sink.webhook]
type = "http"
url = "https://api.example.com/alerts"
method = "POST"
headers = { Authorization = "Bearer token" }
timeout = "30s"
retry = 3
```

### Syslog Sink

```toml
[sink.syslog]
type = "syslog"
host = "syslog.example.com"
port = 514
protocol = "udp"           # udp | tcp
tls = false
facility = "local0"
severity = "info"
```

### Kafka Sink

```toml
[sink.kafka]
type = "kafka"
brokers = ["kafka1:9092", "kafka2:9092"]
topic = "security-alerts"
compression = "snappy"
acks = "all"
```

## 路由配置

在 `wfusion.toml` 中指定 sinks 目录：

```toml
sinks = "sinks"
```

在规则中指定输出目标：

```wfl
rule brute_force {
    ...
    yield security_alerts (
        sip = fail.sip,
        alert_type = "brute_force"
    )
}
```

`sink` 名称与 `yield` 目标 window 名称匹配。

## 高级特性

### 条件路由

```toml
[sink.critical_webhook]
type = "http"
url = "https://pagerduty.com/integration"
condition = "severity >= 80"   # 仅高风险告警
```

### 多副本输出

```toml
[sink.multi_backup]
type = "multi"
sinks = ["file_primary", "file_backup", "webhook"]
```

### 批量与缓冲

```toml
[sink.buffered_file]
type = "file"
path = "/var/log/warpfusion/alerts.jsonl"
batch_size = 100
flush_interval = "5s"
max_buffer = "10MB"
```

### 告警去重

```toml
[sink.dedup_webhook]
type = "http"
url = "https://api.example.com/alerts"
dedup_window = "5m"        # 5 分钟内相同 alert_id 只发送一次
```

## 输出格式

### JSON Lines（默认）

```json
{"alert_id":"sha256-hash","rule_name":"brute_force","entity_type":"ip","entity_id":"10.0.0.1","score":70.0,"emit_time":"2026-01-01T00:05:00Z"}
```

### 扩展字段

```toml
[sink.enriched]
type = "file"
path = "/var/log/alerts.jsonl"
include_fields = ["rule_name", "entity_type", "entity_id", "score", "emit_time"]
add_tags = ["datacenter:dc1", "team:security"]
```

## 运维建议

1. **分级输出**: 高风险告警 → PagerDuty/钉钉，中低风险 → 日志文件
2. **本地缓冲**: 网络不稳定时使用文件缓冲，避免丢数据
3. **监控指标**: 关注 `sink_drops_total`、`sink_latency_seconds` 等指标
4. **权限控制**: Sink 目录设为只读，防止运行时修改配置

## 故障排查

| 问题 | 排查方向 |
|------|----------|
| 告警未输出 | 检查 sink 名称与 yield target 是否匹配 |
| HTTP 失败 | 查看 `wf-engine.log` 中的网络错误 |
| 磁盘满 | 检查文件 sink 的日志轮转配置 |
| 格式错误 | 验证 TOML 语法和字段类型 |
