# 后端维护与容错设计

> 统一处理 Redis / SQL DB / Sink 三类后端故障的检测、降级、探测与恢复。
>
> 参考 `wp-motor` 的 `ActMaintainer` + `KnowdbHandler` + `RescueFileSink` 模式。
>
> 状态：Draft for review | 创建：2026-06-19

## 1. 问题

wfusion 引擎依赖三类外部后端，任一不可用时行为各异，且缺乏统一的故障检测和恢复机制：

| 后端 | 调用位置 | 故障频率 | 当前故障行为 | 问题 |
|------|---------|---------|------------|------|
| Redis | `external()` 逐事件调用 | 网络/运维 | `Err` → `None` → 静默漏报 | 每事件 200ms 超时；无熔断 |
| SQL DB | `knowdb.toml` provider window | 网络/运维 | bootstrap `?` 传播 → 引擎拒绝启动 | 硬依赖；不可降级 |
| Sink | `alert_task` 逐 batch 发送 | 网络/磁盘满 | `send_batch` Err → WARN 日志 | 告警数据丢失；无急救 |

### 1.1 目标

1. **统一维护 task**：单一后台 task 负责所有后端的健康探测与恢复
2. **分策略降级**：查询型后端（Redis/SQL）接受数据丢失（fail-closed）；输出型后端（Sink）不丢数据（rescue swap）
3. **bootstrap 容错**：后端不可用时引擎照常启动，降级运行
4. **热路径零开销**：故障检测和恢复逻辑不在热路径中

### 1.2 非目标

- 不引入新的外部依赖
- 不改造 `wp_knowledge` facade API（外部 crate）
- P0 不做 per-service 可配参数（全局默认值）

---

## 2. 故障处理策略分类

三类后端按**数据是否允许丢失**分为两种策略：

### 策略 A：Circuit Breaker（查询型后端）

适用：Redis（`external()`）、SQL DB（knowledge provider）

```
故障检测（热路径）
  │  调用返回 Err
  ▼
breaker.record_failure()  ← 累加计数
  │  达到阈值
  ▼
breaker.open = true       ← 熔断打开
  │
  ├─ 后续调用直接返回 fallback（零延迟）
  │    Redis:  Bool(false)
  │    SQL:    空结果
  │
  ▼
恢复检测（维护 task）
  │  周期探测
  ▼
breaker.close()           ← 熔断关闭，恢复正常调用
```

**语义**：fail-closed —— "宁可漏报不可误报"。查询不可用时，规则条件视为不满足。

### 策略 B：Rescue Swap（输出型后端）

适用：Sink（告警输出）

```
故障检测（热路径）
  │  send_batch 返回 Err
  ▼
swap_to_rescue()          ← primary 替换为 RescueFileSink
  │  原 primary 句柄 → bad_sink_channel
  ▼
后续 alert 写入 RescueFileSink（不丢数据）
  │
  ▼
恢复检测（维护 task）
  │  从 bad_sink_channel 取故障 sink
  │  reconnect() → 成功?
  ▼
swap_back(sink)           ← 恢复原 primary，停止 RescueFileSink
```

**语义**：fail-safe —— "数据不能丢"。故障期间写入急救文件，恢复后继续正常输出，急救文件可离线回放。

---

## 3. Circuit Breaker（统一原语）

`CircuitBreaker` 是策略 A 的核心状态。设计为极简的原子状态机，热路径只需一次 `AtomicBool::load`。

```rust
/// 统一熔断器。用于查询型后端（Redis / SQL）。
///
/// 热路径（per-event）：`is_open()` 只读 + `record_failure/success` 原子写。
/// 维护 task：`close()` 探测成功后关闭熔断。
pub struct CircuitBreaker {
    open: AtomicBool,
    consecutive_failures: AtomicU32,
}

impl CircuitBreaker {
    pub fn is_open(&self) -> bool { ... }
    pub fn record_failure(&self) { ... }   // 累加，达到阈值时 open=true
    pub fn record_success(&self) { ... }   // 重置计数
    pub fn close(&self) { ... }            // 维护 task 专用
    pub fn trip(&self) { ... }             // 立即熔断（bootstrap 容错用）
}
```

### 配置（P0 全局默认）

```rust
const FAILURE_THRESHOLD: u32 = 5;      // 连续 5 次 Err 触发熔断
const PROBE_INTERVAL: Duration = 10s;  // 熔断后 10s 探测一次
const MAX_PROBE_INTERVAL: Duration = 60s; // 指数退避上限
```

P1 放入 `knowdb.toml`：

```toml
[health_check]
failure_threshold = 5
probe_interval = "10s"
max_probe_interval = "60s"
```

---

## 4. Rescue Swap（Sink 急救）

### 4.1 RescueFileSink

移植 `wp-motor` 的 `RescueFileSink`，数据格式兼容：

```
<rescue_dir>/<sink_name>-<timestamp>-<seq>.dat.lock  ← 写入中
<rescue_dir>/<sink_name>-<timestamp>-<seq>.dat       ← 完整可消费（rename 去 .lock）
```

每行一个 JSON `RescueEntry`：

```json
{"version":1,"kind":"record","record":{"fields":[...]}}
```

flush + close 后 rename `.dat.lock` → `.dat`，表示文件完整，可被 `wfusion rescue` 命令读取回放。

### 4.2 SinkRuntime 改造

每个业务 sink 持有 `rescue_dir` 路径。故障时：

```rust
impl SinkRuntime {
    /// 故障时调用——primary 替换为 RescueFileSink
    async fn swap_to_rescue(&mut self) -> Result<()> {
        let rescue_path = format!("{}/{}-{}-{}.dat.lock", ...);
        let rescue = RescueFileSink::new(&rescue_path).await?;
        let old_primary = std::mem::replace(&mut self.primary, rescue);
        // old_primary 通过 bad_sink_channel 交给维护 task
        self.bad_sink_tx.send(SinkHandle::new(self.name.clone(), old_primary)).await;
        Ok(())
    }

    /// 恢复时调用——恢复原 primary，停止 RescueFileSink
    async fn swap_back(&mut self, recovered: SinkBackend) -> Result<()> {
        let old = std::mem::replace(&mut self.primary, recovered);
        old.stop().await?;  // flush + rename .dat.lock → .dat
        Ok(())
    }
}
```

### 4.3 bad_sink_channel / fix_sink_channel

两个 mpsc channel 连接热路径和维护 task：

```
alert_task (热路径)                     maintenance_task (维护)
  │                                       │
  │  send_batch → Err                     │  loop {
  │  swap_to_rescue()                     │    select! {
  │    bad_sink_tx ──────────────────────►│      bad_sink_rx.recv() → reconnect()
  │                                       │        Ok → fix_sink_tx.send(sink)
  │  fix_sink_rx.recv() ◄────────────────│      Err → 重新入队 + sleep(backoff)
  │  swap_back(sink)                      │    }
  │                                       │  }
```

---

## 5. 统一维护 task

### 5.1 结构

```rust
/// 统一后端健康维护 task。
///
/// 在一个 tokio task 中轮询所有注册的后端：
/// - Circuit Breaker 后端：探测 → 成功则 close
/// - Rescue Swap 后端：从 bad channel 取 → reconnect → 送回 fix channel
pub async fn run_maintenance(
    breaker_backends: Vec<BreakerProbe>,    // Redis / SQL
    sink_bad_rx: mpsc::Receiver<SinkHandle>,
    sink_fix_tx: mpsc::Sender<SinkHandle>,
    cancel: CancellationToken,
) {
    let mut probe_interval = PROBE_INTERVAL;
    loop {
        tokio::select! {
            // Sink 维护：从 bad channel 取故障 sink
            Some(bad_sink) = sink_bad_rx.recv() => {
                match bad_sink.reconnect().await {
                    Ok(_) => {
                        let _ = sink_fix_tx.send(bad_sink).await;
                        wf_info!(res, "sink reconnected");
                    }
                    Err(e) => {
                        wf_debug!(res, error = %e, "sink reconnect failed, retrying");
                        let _ = sink_bad_tx.send(bad_sink).await;  // 重新入队
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
            // Circuit Breaker 维护：周期探测
            _ = tokio::time::sleep(probe_interval) => {
                let mut any_open = false;
                for backend in &breaker_backends {
                    if backend.breaker.is_open() {
                        any_open = true;
                        match backend.probe().await {
                            Ok(_) => {
                                backend.on_recover().await;
                                backend.breaker.close();
                                wf_info!(res, backend = backend.name(),
                                    "backend recovered");
                            }
                            Err(e) => {
                                wf_debug!(res, backend = backend.name(),
                                    error = %e, "probe failed");
                            }
                        }
                    }
                }
                probe_interval = if any_open {
                    (probe_interval * 2).min(MAX_PROBE_INTERVAL)
                } else {
                    PROBE_INTERVAL
                };
            }
            _ = cancel.cancelled() => break,
        }
    }
}
```

### 5.2 BackendHealth trait

```rust
/// Circuit Breaker 后端的健康探测 trait。
/// 每种查询型后端实现自己的 probe + on_recover。
#[async_trait]
pub trait BackendHealth: Send + Sync {
    /// 探测后端是否可用。
    async fn probe(&self) -> Result<(), String>;

    /// 探测成功后的恢复动作。
    /// Redis: noop（连接池自愈）
    /// SQL:   触发 window 数据 reload
    async fn on_recover(&self) {}

    fn name(&self) -> &str;
    fn breaker(&self) -> &CircuitBreaker;
}
```

### 5.3 生命周期

```
启动顺序（消费者先于生产者）：
  alert → maintenance → evictor → engines → receiver

关闭顺序（LIFO）：
  receiver → engines → maintenance → evictor → alert
```

maintenance 在 alert 之后启动（确保急救 sink 就绪）、在 alert 之前关闭（确保恢复的告警能输出）。

---

## 6. 各后端的具体实现

### 6.1 Redis（external function）

```rust
struct RedisHealth {
    breaker: CircuitBreaker,
}

impl BackendHealth for RedisHealth {
    async fn probe(&self) -> Result<(), String> {
        // 用 facade 做一次轻量探测
        wp_knowledge::facade::health_check()
            .map_err(|e| e.to_string())
    }

    async fn on_recover(&self) {
        // Redis 连接池自带重连，关闭熔断器即可
    }

    fn name(&self) -> &str { "redis" }
    fn breaker(&self) -> &CircuitBreaker { &self.breaker }
}
```

**热路径集成**（`ExternalRuntime::call`）：

```rust
pub fn call(&self, service: &str, args: &[Value]) -> Option<Value> {
    if self.breaker.is_open() {
        return Some(Value::Bool(false));  // 零延迟短路
    }
    match self.do_call(service, args) {
        Ok(v) => { self.breaker.record_success(); v }
        Err(_) => { self.breaker.record_failure(); Some(Value::Bool(false)) }
    }
}
```

**bootstrap 容错**：

```rust
// 不再 ? 传播——Redis 不可用时引擎照常启动
match init_knowledge_redis(&knowdb_path, base_dir) {
    Ok(()) => {}
    Err(e) => {
        wf_warn!(conf, error = %e, "Redis init failed; starting degraded");
        redis_health.breaker.trip();  // 初始即熔断
    }
}
```

### 6.2 SQL DB（knowledge provider）

```rust
struct SqlHealth {
    breaker: CircuitBreaker,
    knowdb_handler: KnowdbHandler,
    registry: Arc<WindowRegistry>,
    provider_tables: Vec<String>,
}

impl BackendHealth for SqlHealth {
    async fn probe(&self) -> Result<(), String> {
        wp_knowledge::facade::health_check()
            .map_err(|e| e.to_string())
    }

    async fn on_recover(&self) {
        // SQL 恢复后，重新加载 provider window 数据
        wf_info!(conf, "reloading knowledge tables after SQL recovery");
        self.knowdb_handler.ensure_thread_ready();
        load_knowledge_into_windows(&self.knowdb_handler, &self.registry);
    }

    fn name(&self) -> &str { "sqldb" }
    fn breaker(&self) -> &CircuitBreaker { &self.breaker }
}
```

**bootstrap 容错**（借鉴 `KnowdbHandler` 的 lazy-init 模式）：

```rust
let knowdb_handler = KnowdbHandler::new(base_dir, &knowdb_path, &authority_uri, &dict);
// 不在 bootstrap 强制初始化——延迟到首次使用或维护 task 探测时
```

### 6.3 Sink（alert output）

Sink 不用 circuit breaker，用 **rescue swap + ActMaintainer 重连** 模式。

**热路径集成**（`alert_task` / `SinkDispatcher`）：

```rust
// 每个业务 sink group 持有：
struct SinkRuntime {
    primary: SinkBackend,          // 实际 sink (TCP/File/Kafka)
    rescue_dir: PathBuf,           // 急救目录
    bad_sink_tx: mpsc::Sender<SinkHandle>,  // → maintenance task
    fix_sink_rx: mpsc::Receiver<SinkHandle>, // ← maintenance task
}

// send_batch 失败时：
async fn handle_sink_error(&mut self) {
    self.swap_to_rescue().await;  // primary → RescueFileSink
    // 数据继续写入急救文件，不阻塞
}
```

**维护 task**（`ActMaintainer` 模式）：

```rust
// 从 bad_sink_rx 取故障 sink
let bad_sink = sink_bad_rx.recv().await;
match bad_sink.reconnect().await {
    Ok(_) => sink_fix_tx.send(bad_sink).await,  // 修好了
    Err(_) => { sink_bad_tx.send(bad_sink).await; sleep(5s).await; }  // 没修好
}
```

---

## 7. 急救数据回放

急救目录中的 `.dat` 文件可被 `wfusion rescue` 命令离线回放：

```bash
# 修复 sink 后，回放急救数据
wfusion rescue --from <rescue_dir> --config <wfusion.toml>
```

回放流程：
1. 扫描 `<rescue_dir>/*.dat`（忽略 `.dat.lock`）
2. 逐行读取 `RescueEntry`
3. 按原始 sink 配置重新发送

数据格式兼容 `wp-motor` 的 `RescueFileSink` 输出。

---

## 8. KnowdbHandler（延迟初始化）

借鉴 `wp-motor` 的 `KnowdbHandler` 模式，替代当前 bootstrap 中的强制初始化：

```rust
pub struct KnowdbHandler {
    root: Arc<PathBuf>,
    conf: Arc<PathBuf>,
    authority_uri: Arc<String>,
    initialized: Arc<AtomicBool>,
    dict: Arc<EnvDict>,
}

impl KnowdbHandler {
    /// 延迟初始化——首次使用或维护 task 探测时调用。
    /// 失败时只 WARN 不 fatal。
    pub fn ensure_thread_ready(&self) {
        if self.initialized.load(Acquire) { return; }
        match wp_knowledge::facade::init_thread_cloned_from_knowdb(...) {
            Ok(_) => { self.initialized.store(true, Release); }
            Err(e) => { wf_warn!(conf, error = %e, "knowdb init failed"); }
        }
    }
}
```

每个 rule task 启动时调 `ensure_thread_ready()`。如果后端不可达，task 继续运行，只是查询会失败。

---

## 9. 统一行为矩阵

| 场景 | Redis 挂 | SQL 挂 | Sink 挂 | 全挂 |
|------|---------|--------|---------|------|
| **启动** | WARN + 熔断初始 open | WARN + lazy-init | 正常 | 引擎正常启动 |
| **热路径** | `external()` → `Bool(false)` | join → 空结果 | swap 到急救文件 | 规则不匹配 + 告警进急救文件 |
| **每事件延迟** | ~0ms（熔断短路） | ~0ms（空结果） | ~0ms（写本地文件） | ~0ms |
| **数据安全** | 接受丢失（fail-closed） | 接受丢失（fail-closed） | **不丢**（rescue file） | 告警在急救文件中 |
| **探测** | 维护 task 每 10s 探测 | 维护 task 每 10s 探测 | 维护 task 重连 sink | 指数退避探测 |
| **恢复** | 关闭熔断 → external 恢复 | 关闭熔断 + reload window | reconnect → swap back | 逐个恢复 |
| **日志** | 1×trip + 1×recovery | 同左 | 1×swap + 1×reconnect | 同左 |

---

## 10. 与 wp-motor 的对应关系

| wp-motor | wf-runtime | 说明 |
|----------|-----------|------|
| `ActMaintainer` | `maintenance_task` | 统一后端维护 task |
| `bad_sink_q` / `fix_sink_q` | `bad_sink_channel` / `fix_sink_channel` | Sink 故障/恢复 channel |
| `RescueFileSink` | `RescueFileSink`（移植） | 急救文件 sink |
| `swap_backsink()` / `use_back_sink()` | `swap_to_rescue()` / `swap_back()` | Sink 主备切换 |
| `recover_sink()` | `swap_back()` | 恢复原 primary |
| `KnowdbHandler` | `KnowdbHandler`（移植） | 延迟初始化 + 容错 |
| `ErrorHandlingPolicy` | P0 固定 fail-closed / fail-safe | P1 可配策略 |
| `KnowledgeStatsTelemetry` | P1 接入 wf-runtime metrics | 可观测性 |

---

## 11. 落地计划

### Phase 0（紧急：消除 Redis 故障对引擎的影响）

- [ ] `CircuitBreaker` 原语（`wf-runtime/src/maintenance/circuit_breaker.rs`）
- [ ] `maintenance_task` 框架 + Redis 探测（`wf-runtime/src/maintenance/mod.rs`）
- [ ] `ExternalRuntime` 热路径集成熔断（`runtime.rs`）
- [ ] bootstrap 容错：Redis 初始化失败时 WARN + trip，不阻塞启动

### Phase 1（完整框架：SQL + Sink）

- [ ] `KnowdbHandler` 延迟初始化（替代 bootstrap 强制初始化）
- [ ] SQL DB 探测 + `on_recover` reload window
- [ ] `RescueFileSink` 移植（`wf-runtime/src/sink/rescue.rs`）
- [ ] `SinkRuntime` 改造：`swap_to_rescue()` / `swap_back()`
- [ ] `bad_sink_channel` / `fix_sink_channel` 集成
- [ ] maintenance_task 同时处理 circuit breaker + sink reconnect

### Phase 2（可运维性）

- [ ] `wfusion rescue` 命令（读取急救文件回放）
- [ ] per-backend 可配参数（阈值、cooldown、退避策略）
- [ ] metrics 接入：`wf_backend_circuit_breaker_open`、`wf_sink_rescue_active`
- [ ] health check API（`/health` endpoint）

---

## 12. 文件规划

```
wf-runtime/src/
├── maintenance/               ← 新增
│   ├── mod.rs                 ← run_maintenance task + spawn helper
│   ├── circuit_breaker.rs     ← CircuitBreaker 原语
│   └── probe.rs               ← BackendHealth trait + Redis/SQL probes
├── sink/
│   ├── rescue.rs              ← 新增：RescueFileSink（移植自 wp-motor）
│   └── runtime.rs             ← 改造：SinkRuntime swap_to_rescue / swap_back
├── external/
│   ├── runtime.rs             ← 改造：集成 CircuitBreaker
│   └── health.rs              ← 新增：RedisHealth impl BackendHealth
├── knowledge.rs               ← 新增：KnowdbHandler + SqlHealth
└── lifecycle/
    ├── bootstrap.rs           ← 改造：容错初始化
    └── spawn.rs               ← 改造：spawn_maintenance_task
```

---

## 13. 相关文档

- External Function 设计 → [external-function-design.md](external-function-design.md)
- 并发架构（push 单写者 actor）→ [window-channel-actor-design.md](window-channel-actor-design.md)
- 错误处理 → [error-handling.md](error-handling.md)
- wp-motor 错误分析 → [wp-motor-error-analysis.md](wp-motor-error-analysis.md)
