# q13 100M RSS 26.4GB：mimalloc 段区峰值水位随数据量放大

> 状态：**待修复** · 2026-08-25 · 优先级：高（违反"流式内存与总量无关"目标）
> 关联：`notes/q13-side-input-join-progress.md`（M-13 分片根治段，30M 达标态）、
> `notes/q13-side-input-join-progress.md`（q13a 列式化段）
> 复现场景：`wf-examples/performance/nexmark_pk` → `./bench.sh q13 replay 100m`
> 前置修复（已完成，未 commit）：广播按订阅类型裁剪（RoundRobin-only → batch-only）

---

## 1. 症状

q13（q13a→bid_mod→q13b 双规则链，分片态）100M 跑批 RSS_peak **26.4GB**，
30M 仅 **8.4–9.9GB**。流式处理下内存应与数据总量无关——100M 比 30M 多 ~17GB
即为问题。

| 规模 | EPS | RSS_peak | CPU avg | bid_mod acked_lag 峰值 |
|---|---|---|---|---|
| 30M（修复后，两次） | 4.06M / 3.41M | 9.87GB / 8.43GB | 934–1018% | ~170 |
| 100M（修复后） | 3.19M | **26.4GB** | 1078% | **765** |

30M 达标态（本次广播裁剪修复后）EPS 翻倍 + 内存达标；100M 内存问题独立存在。

## 2. 复现证据（100M，footprint 采样于 ingest 完成后尾部消化期）

- RSS 曲线：**线性涨至 26,424MB 后平台期**（持续到结束，非无限增长）
- footprint：
  - **IOAccelerator：3407MB dirty + 23GB reclaimable**（= mimalloc 段区，
    128MB × ~180 段，内容已 purge 归还、段未还 OS）
  - **RSS 的 23GB 是段区（reclaimable），物理 dirty 仅 3.4GB**
- 窗口内存峰值（`metrics.ndjson` window.memory_bytes）：
  - bid_events 2.92GB（over=30min 有界）+ bid_mod 2.81GB + auction 0.56GB +
    person 0.16GB ≈ **6.4GB 有界，正常**
- mimalloc stats（2026-08-25 早前分片态）：committed 峰值 28.8GB current、
  purged 30.6GB（持续归还）、18 arena
- 正确性：`[clean]`（memory_evicted_total=0），EPS 3.19M > ingest 3M/s

## 3. 根因（机制判断，待验证）

**RSS 峰值 = mimalloc 峰值分配水位**：mimalloc arena 不还 OS，只 purge 内容；
RSS 按历史峰值借入的段区保留（footprint 的 reclaimable = 已归还内容的段）。

100M 下峰值分配被 **消费滞后积压** 顶高：

```
q13a 分片生产（~4M/s） > q13b 消费（~3.2M/s）
  → bid_mod 在途积压：acked_lag 峰值 765 批（30M 仅 ~170）
  → 每批在途 Arrow batch 列缓冲 × 积压量 = 分配峰值水位
  → mimalloc 借入 26GB 段区 → RSS 26.4GB 平台期
```

窗口存储本身有界（6.4GB），events 冗余广播已在本修复中裁剪——剩余的大头是
**在途 batch 积压的峰值分配**。30M 下 q13b 跟得上（EPS > 3M/s ingest）不积压 →
峰值水位低 → 9.9GB。积压是否应存在：RoundRobin 广播 channel bounded=8 已背压
q13a，但 **bid_mod 窗口 append 无界**（q13b 若同时从窗口 pull 或 ack 滞后），
积压发生在窗口存储 + 在途。

## 4. 已尝试 / 已排除

| 方案 | 结果 | 结论 |
|---|---|---|
| 广播无条件裁剪 batch-only | 3 测试 FAIL（`push.events` 契约） | 必须按订阅类型 |
| **广播按订阅类型裁剪**（本次） | 30M EPS 4.06M、RSS 9.87GB ✓ | **保留，勿回退** |
| 回退 q13a 单 worker | 1.52M EPS、5.9GB | 倒退，不做 |
| mimalloc arena 配置 / 换分配器 | 未验证 | 段区只是表象，root 是峰值水位 |

## 5. 下一步（先数据，后动手）

1. **跑 30M 对比 footprint**：确认 30M 段区小、dirty 小——把「峰值分配水位」
   与「窗口保留」两个成分分开（预期：30M reclaimable 段区 ≈ 6-8GB）。
2. **确认积压来源**：`metrics.ndjson` 的 bid_mod `acked_lag` 时间曲线——
   积压在 ingest 早期（引擎追赶期）还是全程持续？`window.memory_bytes` 曲线
   同看。
3. **确认 q13b 消费瓶颈**：`rule profiling` 计数（列式命中率 0 条 = 列式早退）；
   RoundRobin 10 worker 的占空比是否均衡（尾批失衡 → 单 worker 滞后拖尾）。
4. 候选修复（选数据支持的）：
   - q13b 消费追平生产（占空比/调度），从根上消除积压——**首选**；
   - batch 大小（36.5k 行/批）在 100M 下 flush 频率与在途量权衡；
   - 窗口 append 无界 → 若 q13b 走 push 广播而非窗口 pull，考虑 append 后
     快速驱逐（evict=153 已有时间驱逐，看是否滞后）。
5. 目标：100M RSS < 10GB，EPS 维持 3.2M+。

## 6. 关联

- `notes/q13-side-input-join-progress.md` M-13 段（分片根治背景 + 本次修复细节）
- `notes/q13-side-input-join-progress.md` q13a 列式化段（203ns/行，分配量级大降）
- 全局 100M 内存盘点（2026-08-25 全量跑批，RSS>10G 判定为问题）：
  q5 17.5GB / q13 27.1GB（旧态）/ q14 18GB / q16 22.9GB / q18 24.2GB /
  q19 32.9GB / q22 30GB——各 Q 独立跟进
