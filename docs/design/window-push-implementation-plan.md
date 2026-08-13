# 方案 B 实施计划（Push + 解析 Worker 池）· 从后往前改造

> **状态：R1 / R2 / R3 已合入**（`c5932e7` R1、`2c38a9a` R2、R3 见下实施记录）；
> **R4 待办**
>
> 2026-08-13 · 关联 [window-push-model-design.md](window-push-model-design.md)（架构设计）、
> [window-push-consumer-model.md](window-push-consumer-model.md)（候选分析）、
> [architecture.md](architecture.md)（现行 pull 模型）
>
> **实施记录（2026-08-13 更新）**：
> - R1（`c5932e7`）：规则 worker 走 push channel，`run_push_loop` 消费 `Arc<Vec<Arc<Event>>>`。
> - R2（`2c38a9a`）：`Router::route` 拆 `route_parse`（并行解析）+ `route_commit`（单 worker 按 seq 有序
>   append + 广播）；`lifecycle/parse_pool.rs` 有界通道 + N 解析 worker + 1 commit worker。比文档原设计
>   更优：用 BTreeMap 重排序保证 watermark/广播顺序性。
> - 正确性（R2 提交内，N=200000 vs R1）：matches_total 一致（269383），dropped_late=0，delivered=200000；
>   1182 tests 通过。
> - **配置改名**（`c60a82f`）：`executor_parallelism` → `parse_parallelism`（解析池，默认 2）/ `rule_parallelism`
>   （P2a 规则分片，默认 6）。旧键 `executor_parallelism` 已移除，serde 静默忽略——**旧 conf 里的该键不再生效**。

---

## 0. 总原则

1. **从后往前改造**：push 链的**消费端先改**，逐环往前（源端最后）。每一步的**下游
   已是新的、验证过的**，改上游只喂它新数据——不存在"中游改了、下游还是旧的"
   的中间不确定态。**每一步都能独立确认成功**再前进。
2. **先并行后切换**：新路径与旧路径并存，验证后再切换，保留回退开关。
3. **先正确后性能**：每阶段先保证规则正确性（触发计数、alert 输出），再谈吞吐。
4. **安全网先行**：开工前建立基准 + 正确性验证工具。
5. **小步提交**：每个可编译、可测试的原子改动一个 commit，便于 bisect。

**push 链（从后往前对应改造顺序）**：

```
source 解码 RecordBatch  →  解析（→Event）  →  广播 Arc  →  规则 worker 消费
    [A 源端]                [B 解析位置]        [C 广播]     [D 消费端] ← 先改这里
```

改造顺序：**D → C → B → A**（每一步的上游先不改，用桥接喂已验证的下游）。

---

## 1. 安全网（开工前必做）

| 工具 | 用途 |
|------|------|
| 吞吐基准 | `nexmark_pk/run.sh stream 2000000` + `/tmp/wf_measure.sh`（events/matches 时间线） |
| 正确性基准 | 固定 seed，对比规则触发计数 + `default.ndjson` alert 输出（数量/内容） |
| 回归测试 | `cargo test -p wf-engine -p wf-runtime`（386 + 125） |
| 性能剖面 | `/tmp/wfprof_run.sh`（macOS sample，semaphore/CPU 分布） |

**验收基线**：matches 2.37M ≈ **10.6s**，semaphore ≈ **9000**。

---

## 2. R1：规则 worker 数据面 → channel（消费端 D，最先）

**目标**：规则 worker 从 channel 收 `Arc<Vec<Event>>` 推进状态机，不再 `window.read()`
拉取。**只改消费端**；生产端用最小桥接喂它（route 侧解析 + 广播，即方案 A 的
`append_parsed` + 广播）。

### 改动

1. **每规则一个 channel**：spawn_rule_tasks 时创建，注册到广播表。
2. **规则 worker 改造**（`run_rule_task`）：
   - `loop`：从 channel `recv` Arc → 推进状态机（替换 `pull_and_advance` 的窗口读）；
   - 保留 `scan_timeouts`（timeout 仍需窗口水位）；
   - cursor/gap 语义简化（push 天然不丢，除非背压丢弃）。
3. **过渡桥接**（生产端，R2 再替换）：
   - `Router::route` 用 `append_parsed`（解析在 route，方案 A）+ append 后**广播 Arc**
     到订阅规则的 channel。
   - 此阶段 source 仍直接 route（未改），规则从 channel 收。

### 成功判据（明确）

- [x] 全量测试通过（`c5932e7` 后 1182 tests；当前 wf-engine 392 + wf-runtime 127）
- [x] **正确性一致**：规则触发计数 + alert 输出与基线完全相同（`c5932e7` 对比基线一致）
- [x] 规则读路径确认不再 `window.read()`（push 模式规则走 channel，`run_push_loop`）
- [x] 吞吐允许略降（桥接用方案 A，有已知负效应），**只要正确性成立即成功**

### 回退

规则从 channel 切回窗口 pull（保留旧路径，开关切换）。

---

## 3. R2：解析 Worker 池（替换桥接的解析，环节 B）

**目标**：把 R1 桥接里的「route 内解析」移到独立解析 worker 池，source 与解析解耦，
解析并行。**下游（规则 channel 消费）已在 R1 验证**，此步只换生产端。

### 改动

1. **新增解析管线**：`parse_channel`（有界）+ N 解析 worker task。
2. 解析 worker 收 `RecordBatch` → `batch_to_events`（窗口 `materialize_fields`）→
   `Router::route`（`append_parsed`）→ 广播 Arc 到规则 channel（复用 R1 的广播）。
3. **source task 改造**：`receive_batch()` 后推 `parse_channel`（不再直接 route）。
4. **worker 数**：`runtime.executor_parallelism`（`=1` 时行为接近现状）。

### 成功判据

- [x] 全量测试通过；正确性与 R1 一致（`2c38a9a`：N=200000 matches_total 一致 269383，dropped_late=0）
- [x] **source task 不再解析**（代码可查：source 只 decode + `prepare_batch` projection → 推 parse channel），receiver 解耦
- [ ] 吞吐 ≥ R1（解析并行，A 的负效应消除）→ **≥ 基线 10.6s**（需对当前基线复测；注意
      实测 conf 曾用已移除的 `executor_parallelism`，实际 `parse_parallelism=2`，见实施记录）
- [x] 剖面：`batch_to_events` 从 source task 移到解析 worker（`route_parse` 内，且窗口锁外解析）

### 回退

停解析 worker，source 直连 route（回到 R1 桥接）。

---

## 4. R3：source 直连改造收尾（源端 A）

**目标**：确认 source 解码 → 解析 channel → 整链的正确性与吞吐。R2 已含 source 推
channel，此步为整链验证 + 清理。

**实施记录（2026-08-13）**：
- 4 个文件源 replay（ndjson/csv/arrow_framed/arrow_ipc）从 inline `route_batch` →
  `Router::route` 收进 parse 池：`parse_pool.rs` 新增 `push_decoded_batch` / `build_parse_item`
  共享 helper（流式源与文件源统一走同一条 parse → 有序 commit → 广播链）。
- `route_batch` 及其投影兜底重试删除（parse 池依赖 `prepare_batch` 在 push 前投影）；
  2 个投影测试改为直接单测 `prepare_batch`。
- 语义变化（与流式一致）：文件源 route 错误由 commit worker `wf_warn` 记录，不再
  propagate；window-miss 在 push 前处理，语义不变。

### 成功判据

- [x] 全链正确性 = 基线；吞吐 ≥ 基线
  - wf-engine 392 + wf-runtime 127 全过（含文件源 replay 经 parse 池的用例）
  - 文件源端到端：daemon 起 file source，`file source replay complete rows=200000`，
    告警 76569 条落盘（N=200k，与基线一致）
  - 吞吐无回归：eps_obj blackhole N=2.68M match 追平 **24.18s**（基线 24.1s）
- [x] semaphore 变化记录：macOS `sample` 显示规则读路径**无窗口读锁等待**——剩余
  semaphore 主要是 tokio worker park（空闲）+ blocking 线程池 dispatch（控制面/写侧），
  符合「规则数据面已脱离窗口读锁」预期

---

## 5. R4：窗口控制面化（清理）

**目标**：按管道设计（[rule-sharding-and-aggregation-window.md](rule-sharding-and-aggregation-window.md)）
把「窗口」收敛为两类，规则数据面彻底脱离窗口锁：

- **输入窗口**（stream → window）：只留 watermark / timeout / eviction / join——供
  match 时间窗与 join 查询（control-plane，规则数据面不读）。
- **管道（Pipe）**（yield 目标 / `|>` 中间 / conv）：`over=0` 纯透传（跳过 buffer 存储、
  只广播到订阅者），`over>0` 保留供下游 match；订阅者（规则 / sink / 变换算子）统一走
  `PipeFanout`（替代仅挂规则的 `RuleFanout`）。

> **注**：`over=0` 纯透传是 R4 的核心前置——当前 `append_inner` 对 `over=0` 仍照常
> `push_back` 存储、`evict_expired` 对 `over=0` 直接 return（永不时间驱逐），只靠
> `max_window_bytes` 内存 cap 兜底（见 `window/buffer/eviction.rs`）。R4 需先实现
> 「`over=0` 跳过 buffer 存储、只广播」，管道才会真正纯透传。

### 前置（over=0 纯透传 + PipeFanout 落地）

- [ ] over=0 窗口/管道跳过 buffer 存储，只广播到订阅者
- [ ] PipeFanout 扩展：规则 / sink / 变换算子同挂一个 fanout

### 成功判据

- [ ] semaphore 中规则读锁部分消失（对比基线 ~9000）
- [ ] 正确性 = 基线；吞吐 ≥ 基线

---

## 6. 后续（R2/R4 之后独立评估）

| 项 | 说明 |
|----|------|
| **广播背压** | R1 先方案甲（channel 满 → 阻塞，保正确）；验证后评估乙（丢弃 + gap 指标） |
| **规则内按 key 分片**（executor_parallelism） | 独立于 push 改造，可并行推进：单规则拆 N shard，key 哈希，处理 conv/限流/指标一致性 |
| **中间窗口贯通** | push 链贯穿中间窗口（append 后广播下游） |

## 7. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 消费端切换引入正确性回归 | R1 明确正确性判据（触发计数/alert 完全一致）+ 安全网 |
| 广播背压语义错误 → 丢数据/阻塞 | R1 先阻塞保正确，后评估丢弃；gap 指标 |
| watermark 乱序（并行 route） | R1/R2 保持 append 顺序（桥接与解析 worker 提交有序） |
| 收益不及预期（semaphore 非唯一瓶颈） | R2 后实测确认；若 semaphore 降但吞吐不升，停下重新定位 |
| 改动面大、回归 | 从后往前 + 安全网 + 小步提交 + 每步回退开关 |

---

## 8. 里程碑

| 里程碑 | 判定 |
|--------|------|
| **M1（R1 完成）** | 规则从 channel 收数据，**正确性 = 基线**（消费端切换成功） |
| **M2（R2 完成）** | 解析移 worker 池并行，吞吐 ≥ 基线 10.6s，正确性不变 |
| **M3（R4 完成）** | semaphore 规则读锁部分消失，正确性 = 基线，吞吐 ≥ 基线 |

每个里程碑产出：测试通过 + 正确性对比 + 基准数据 + 剖面对比 + 可回退。
