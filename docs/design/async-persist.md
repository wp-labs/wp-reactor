# 通用异步落盘机制（AsyncPersister）

> 状态：**M6-1 设计 + 实施中** · 2026-08-27 · 优先级：高
> 动机：q18 spill 驱逐的 redb 写事务占驱逐耗时 99%（profile 实测：
> 每批 157MB 写 25.4s @256MB 页缓存；1GB 页缓存 2.7s），同步阻塞 ingest 热路径
> → EPS 从无 spill 12.6M 塌方到 144K-1.6M。根治 = 驱逐写移出热路径。
> 场景：stats 状态 spill（第一个适配者）+ 未来窗口溢出/审计落盘/中间结果落盘。
> 实现：`crates/wf-engine/src/match_engine/async_persist.rs` + `spill.rs` 适配
> 复现：`wf-examples/performance/nexmark_pk` → `./bench.sh q18 replay 100m`

---

## 1. 问题（数据驱动）

q18 100M spill 驱逐路径 profile（2026-08-27 实测，每片 2 次驱逐 × 24.8 万键/批）：

| 段 | 每批耗时 | 占比 | 性质 |
|---|---|---|---|
| scan（clock 扫描） | ~110ms | 0.4% | 热路径必做（决定驱逐哪些键） |
| clone（状态拷贝） | ~113ms | 0.4% | 可消除（move 替代） |
| **write（redb 事务）** | **25.4s @256MB 缓存 / 2.7s @1GB 缓存** | **99%** | **可移出热路径** |

**结论**：写段占绝对主导且与 ingest 无依赖 → 异步化。热路径只留"决定驱逐 + 内存释放"
（同步必须，`account_new_bucket` 预算检查依赖内存立即释放），持久化写交给后台。

## 2. 通用机制（AsyncPersister）

```rust
/// 批量写后端（每批一次事务/一次 IO）。
pub trait BatchWriter<T> {
    fn write_batch(&mut self, items: Vec<T>) -> Result<(), String>;
}

/// 通用异步落盘队列：热路径 submit → 后台 worker 攒批 → 后端批量写。
/// 通用性：T=数据单元（任意）；B=BatchWriter<T>（redb/文件/...）；
/// 背压=字节预算满则 Backpressure（调用方退化为同步写）；失败=error_cb 回调。
pub struct AsyncPersister<T, B> { ... }

impl<T, B: BatchWriter<T>> AsyncPersister<T, B> {
    /// 提交一批待落盘数据（热路径，非阻塞）。
    /// 返回 Ok(()) 入队；Err(Backpressure) = 队列超预算 → 调用方同步写兜底。
    pub fn submit_batch(&self, items: Vec<T>, est_bytes: usize) -> Result<(), PersistError>;
    /// 等待队列排空（close/读回前调用；worker 写完所有已提交批次后返回）。
    pub fn flush(&self) -> Result<(), String>;
    /// 队列是否为空（读回前判空）。
    pub fn is_idle(&self) -> bool;
    /// 停止并排空（关闭通道 → worker 消化剩余 → join）。
    pub fn shutdown(self) -> Result<(), String>;
}
```

**组件与职责**：

| 组件 | 职责 | 通用性 |
|---|---|---|
| `submit_batch` | 入有界通道 + 字节记账；超预算返回 Backpressure | 任何"热路径产数据、后台落盘"场景 |
| worker 线程（std::thread） | `recv` 首批 → `try_recv` 攒批（≤ 预算或 1ms）→ `write_batch` | 单 worker FIFO = 保序（同 key 后写覆盖先写，幂等） |
| `BatchWriter<T>` | 一批一次后端写入（redb 单事务 / 文件 append / ...） | trait 化后端，可插拔 |
| `error_cb` | 写失败回调（spill = 置 spill_failed 拒收新键；其他 = 重试/告警） | 策略注入 |
| `flush()` | Condvar 等 `queued_bytes == 0` | close/读回前的 drain 语义 |
| 字节预算 | 队列内存有界（不因积压复涨） | 有界性承诺的承载点 |

**为什么不用 tokio async**：evict_to_spill 在同步执行路径上（无 runtime 依赖），
std::thread + std::sync::mpsc 让机制完全脱离 runtime，任何调用方可用；
redb 写本身是阻塞 IO，放专用线程反而更简单（无 executor 饥饿）。

**为什么单 worker（保序）**：驱逐→读回→再驱逐同一键时，redb 覆盖写要求
"后写赢"。多 worker 并发写同 hash 可能乱序（旧值覆盖新值）。单 worker FIFO
天然保序。吞吐：redb 单线程写 24.8 万键 2.7s，10 片 = 10 worker 并行，够。

**为什么攒批**：redb 事务成本高（页分配/提交），批越大摊销越好。worker
攒多批驱逐合一次事务；调用方单次驱逐本身也是批量（收集到预算水位才提交）。

## 3. spill 适配（第一个适配者）

```
evict_to_spill（热路径，同步）:
  ① scan clock 收集驱逐链（保留）
  ② 链从 buckets 移除（move，替代 clone）→ estimated_bytes 同步减 ✓
  ③ 记账 spill_evictions
  ④ submit_batch(链, est_bytes)
       ├─ Ok         → 返回（写由后台消化）
       └─ Backpressure → 同步 write_batch 兜底（当前行为）
后台 worker:
  ⑤ write_batch → redb 单事务 insert（复用现有序列化层）
  ⑥ 成功 → 记账 spilled_bytes；失败 → error_cb（spill_failed 拒收）
读回（take / drain_up_to）:
  先 flush()（等队列排空）→ 再读 redb —— 否则读不到刚驱逐未落盘的键
```

**SpillStore trait 的适配**：
- `put_batch` 语义变为"提交（或同步兜底）"，签名不变（返回 Result）
- `take`/`drain_up_to` 内部先 `flush()`
- `cleanup` = `shutdown()` + 删文件

## 4. 语义变化与取舍

| 项 | 原（同步） | 新（异步） | 取舍 |
|---|---|---|---|
| 热路径阻塞 | 2.7-25s/批 | ~110ms（仅 scan） | 吞吐 ↑ |
| 驱逐键内存 | clone 驻留至写完成 | 立即释放 | 内存 ↓ |
| 写失败 | 键保留内存（不丢） | 键已移除，失败即丢 | 磁盘满极端场景可重 ingest（§8 无持久化语义） |
| 队列内存 | — | 字节预算（背压阈值） | 有界性保持 |

## 5. 其他潜在适配场景（未来）

- 窗口溢出（bid_events 大窗数据落盘）
- 审计/明细落盘（alert 全量归档，异步文件 append）
- 中间结果（join 状态）落盘

## 6. 里程碑

- M6-1：`async_persist.rs` 通用模块（trait + 队列 + worker + 背压 + flush + shutdown）+ 单测
- M6-2：spill 适配（RedbSpillStore 内部 AsyncPersister；evict_to_spill 改 move + submit）
- M6-3：读回前 flush 语义 + 失败回调接线（spill_failed）
- M6-4：实测（30M/100M EPS/RSS 对比）+ 本设计文档定论

## 7. 写并发形态 A/B（2026-08-27 实测定论）

| 形态 | 30M EPS | 100M-1GB EPS | 结论 |
|---|---|---|---|
| **每分片独立 worker**（= 多 worker 多文件, 分片键 hash 定位） | **10.9M** | **11.6M** | **最优**——驱逐与写每片流水线自洽 |
| 共享队列 + 4 worker（文件 hash 路由） | 0.59M | 未测 | 4 worker 跟不上 10 片驱逐速率 → 背压 |
| 全局单写者（串行写所有文件） | 0.57M | 未测 | 小写量浪费并行度 |

**结论**："多 worker + 多文件 + 目标 hash 定位"的最优实现 = **每分片独立写队列**
（分片键 hash → 分片 executor → 分片 worker；每文件单写者 ✓ 无 redb 事务冲突；
驱逐与写流水线自洽——片驱逐一批片 worker 消化一批）。共享队列/全局单写者
都是降级（worker 数 < 文件数时跟不上驱逐速率）。

**追加优化（实测）**：
1. 驱逐批内按 hash 排序 → redb 随机写变近似顺序写（bench 1.4-2.4x）
2. 攒批上限（`WF_SPILL_BATCH_BYTES` 默认 64MB）——无上限会合并出超大批
   （几百 MB）拖死小页缓存后端（实测单批 197s）
3. `WF_SPILL_CACHE_MB`（页缓存）/ `WF_SPILL_FSYNC_EVERY` / `WF_SPILL_PROFILE`
   （worker 分段计时）实验开关
