# wf-runtime 指标与统计设计（评审草案）
<!-- 角色：架构师 | 状态：Draft for review | 创建：2026-02-26 | 更新：2026-02-26 -->

## 1. 背景与目标

当前 `wf-runtime` 已具备完整数据闭环（Receiver → Router → RuleTask → Alert Sink → Evictor），但缺少统一的、可量化的运行时指标体系。

本设计目标：

1. **低开销度量处理性能与内存利用率**（优先）
2. **支持 Prometheus 暴露与离线压测对齐**（M23）
3. **吸收 `wp-motor` 统计机制的有效部分**，避免把历史耦合带入 `wf-runtime`

---

## 2. 度量范围（第一阶段）

### 2.1 性能指标

- `wf_receiver_connections_total`：接入连接数
- `wf_receiver_frames_total`：接收帧数
- `wf_receiver_rows_total`：接收行数
- `wf_router_route_calls_total`：路由调用数
- `wf_router_delivered_total` / `wf_router_dropped_late_total`
- `wf_rule_events_total{rule}`：规则消费事件数
- `wf_rule_matches_total{rule}`：规则命中数
- `wf_alert_emitted_total{rule}`：告警输出数
- `wf_alert_channel_send_failed_total`：告警通道发送失败数

### 2.2 时延指标

- `wf_rule_scan_timeout_seconds{rule}`（Histogram）
- `wf_rule_flush_seconds{rule}`（Histogram）
- `wf_alert_dispatch_seconds`（Histogram）
- `wf_receiver_decode_seconds`（Histogram）

> 说明：第一阶段不做“每条事件端到端 latency”精确跟踪，避免高开销与高基数。

### 2.3 内存与容量指标

- `wf_window_memory_bytes{window}`（Gauge）
- `wf_window_rows{window}`（Gauge）
- `wf_window_batches{window}`（Gauge）
- `wf_evictor_time_evicted_total` / `wf_evictor_memory_evicted_total`
- `wf_rule_instances{rule}`（Gauge，活跃状态机实例数）
- `wf_rule_cursor_gap_total{rule,window}`（数据被 eviction 追越次数）

---

## 3. 架构方案

### 3.1 总体结构

```
Hot Path (Receiver / Router / RuleTask / Alert / Evictor)
    │
    ├─ Fast metrics API (counter/gauge/hist observe)
    │      - lock-free / low-lock
    │      - no allocation on critical path
    │
    └─ Optional TopN event bus (only when enabled)
           - bounded channel
           - drop-on-overflow + dropped counter

Metrics Aggregator Task
    │
    ├─ Prometheus exporter (/metrics)
    └─ Periodic summary log (human-readable)
```

### 3.2 设计原则

1. **热路径最小侵入**：优先 counter/gauge 原子更新，不在热路径构建 `DataRecord`。
2. **有界与可退化**：任何异步统计通道必须有界；满了就丢并计数，不能反压主链路。
3. **低基数优先**：label 仅允许 `rule/window/target/stream` 等有限集合，禁止 `entity_id`。
4. **单调时钟**：时延使用 `Instant`，避免 wall-clock 跳变影响。
5. **统计与告警分离**：运行态指标采集不依赖业务 sink 成败。

---

## 4. 对 `wp-motor stat` 的复用评估

| wp-motor 机制 | 结论 | 是否适合引入 `wf-runtime` | 原因 | 建议 |
|---|---|---|---|---|
| 分阶段统计（pick/parse/sink） | ✅ 借鉴 | 适合 | 有助于定位瓶颈链路 | 映射为 receiver/router/rule/alert/evictor 五阶段 |
| `slice + total` 双视图 | ✅ 借鉴 | 适合 | 便于看窗口趋势与累计趋势 | 保留为 periodic summary 视图 |
| 单独 Monitor 任务集中汇总 | ✅ 借鉴 | 部分适合 | 思路可复用，但实现需去耦 | 做独立 `metrics task`，不依赖 `TaskController` |
| `MonSend` 通道化上报 | ⚠️ 选择性借鉴 | 部分适合 | 通道模型有用，但 payload 过重 | 改为轻量事件结构 + bounded channel |
| `ReportVariant::Stat` 快照模型 | ❌ 不建议直接用 | 不适合 | 偏“表格报表”，不适合 Prometheus 原语 | 改为 Counter/Gauge/Histogram 原语 |
| `DataRecord/DataField` 作为统计载体 | ❌ 不建议直接用 | 不适合 | 热路径格式化开销高、类型层次过深 | 指标层使用原生数值结构 |
| 大通道容量（如 100000） | ❌ 不建议 | 不适合 | 弱化拥塞信号，放大内存不可控风险 | 小容量 + 丢弃计数 |
| `stat_print` 表格输出 | ⚠️ 保留调试形态 | 部分适合 | 调试有价值，生产可观测不足 | 仅作为 debug summary，主输出走 `/metrics` |
| TopN + LRU（按维度裁剪） | ✅ 借鉴 | 适合（可选） | 可控高基数统计 | 仅在 `metrics.topn.enabled=true` 打开 |
| Monitor 告警 DSL (`AlertRule`) | ❌ 暂不引入 | 不适合当前阶段 | 与引擎运行态指标主线不一致，且当前利用率低 | 后续单独做 ops alert 规则层 |

---

## 5. 为什么不直接搬 `ActorMonitor + MonSend`

不是“不能学”，而是“不能原样照搬”。核心差异：

1. **运行时耦合差异**：`wp-motor` 版本绑定了 `TaskController`、旧任务编排和监控 sink 生命周期；`wf-runtime` 生命周期模型不同（Reactor 两阶段关闭）。
2. **输出目标差异**：`wp-motor` 以表格/监控 sink 为主；`wf-runtime` 目标是 Prometheus + 基准对齐（M23）。
3. **性能约束差异**：`wf-runtime` 重点是 CEP/Window 热路径，统计层必须更“轻量、可丢弃、可控基数”。

---

## 6. 对应到 `wf-runtime` 的埋点位置

- `receiver`：连接、帧、解码耗时、解码失败、route report 聚合
- `router`：delivered/dropped_late/skipped_non_local
- `rule_task`：pull 批次数、事件数、match 数、close/flush 耗时、cursor gap
- `alert_task`：dispatch 数、dispatch 耗时、序列化失败数、channel backlog
- `evictor_task`：sweep 次数、time/memory eviction 数
- `window`：周期采样 memory/rows/batches（Gauge）

---

## 7. 配置草案

```toml
[metrics]
enabled = true
report_interval = "2s"           # 周期摘要日志（评审结论）
prometheus_listen = "127.0.0.1:9901"

[metrics.topn]
enabled = false                   # 暂不进入 Phase 1
max = 20
queue_capacity = 4096
```

策略：

- `metrics.enabled=false` 时，除极少数必要计数外全部短路。
- `topn.enabled=false` 时，不创建 TopN 通道与聚合状态。

---

## 8. 落地计划（建议）

### Phase 1（最小可用）

- 完成核心 Counter/Gauge
- 暴露 `/metrics`
- 补充 e2e/集成测试校验关键指标递增
- 不做端到端事件 latency 精确统计
- 不引入 TopN 聚合

### Phase 2（性能画像）

- 引入关键 Histogram（decode/dispatch/flush）
- 增加窗口/规则维度 Gauge 周期采样

> 进展（2026-02-26）：已完成第一版实现（`wf_receiver_decode_seconds`、`wf_alert_dispatch_seconds`、`wf_rule_scan_timeout_seconds{rule}`、`wf_rule_flush_seconds{rule}`，以及窗口 Gauge 周期采样缓存）。

### Phase 3（可选增强）

- 引入 TopN + LRU 维度聚合（按配置开关）
- 增加 ops summary 日志模板

---

## 9. 评审结论（2026-02-26）

1. 第一阶段不加入“端到端事件 latency”精确统计。
2. 先使用 `rule` 单维 label，不扩展 `rule + target` 双维。
3. TopN 暂不添加，延后到后续阶段（默认关闭）。
4. `report_interval` 默认值调整为 `2s`。
