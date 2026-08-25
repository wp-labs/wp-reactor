# 内存问题定位方法论：分账 → 消融 → 差分

> 引擎 RSS 超预期 / 随数据量增长 / 不回落时，用「三层分账 + 逐段消融 + 存量差分」
> 定位持有者——而不是一上来就猜假设 / 上 profiler。本方法沉淀自 q13 内存问题
> （30M RSS 9.9GB→28.8GB→14.4GB 的完整排查：9 次假设被实测否决后收敛），
> 内容不依赖具体查询，可迁移到任何流式负载。
>
> 姊妹篇：[`PERF_BISECTION_METHOD.md`](PERF_BISECTION_METHOD.md) 管吞吐（EPS），
> 本文管内存（RSS/分配水位）。两者共用诊断机制（perf-diag 分段门控），
> 一张墙梯同时产出 EPS 与 RSS 两列——**时间和内存的墙常常是同一面**。

## 0. 三条核心原则

1. **先让问题可观测，再谈定位**——猜假设之前，先把"未知的 RSS"变成可对账的
   分账等式。猜在本问题上命中率 0/9，分账 2 次 2 次当场定案。
2. **先消融定位，再动手优化**——不要在最可能的方向上优化很久（q13 曾花大力
   气优化 pipe 写入足迹 4.2×，内存纹丝不动——问题在 alert 构建段）。消融给出
   **段的边界**，优化才落到正确的段上。
3. **改一步测一步，每步用测试钉死**——量化代理（探针/对拍）让每次改动能
   立即看到收益或回退，守护测试让错误的"优化"在合入前就被抓住。

## 1. 适用场景

- RSS 峰值超过预算（本团队口径：100M 跑批 > 10GB 即疑点）
- RSS 随数据总量增长（流式引擎应与总量无关；有界除外，见 §3）
- 峰值后不回落（平台期/持续爬升）
- 改动后内存异常（升或降），需要归因

## 2. 三层分账：把"未知 RSS"变成等式

内存问题最大的障碍是**不可观测**：窗口会计只数引擎知道的部分，进程 RSS 混入
分配器/OS 行为。按层建立测量，逐层排除：

| 层 | 工具 | 回答 | 反例（本 session 教训） |
|---|---|---|---|
| ① 引擎会计 | `window.memory_bytes` / `allocated_bytes` | 窗口保留是否有界 | `content_bytes` 漏 bitmap/offsets；`get_array_memory_size` 对 IPC 共享缓冲**重复计整块**（content 1.58GB 报成 17.97GB）——用按缓冲指针去重的 `allocated_bytes` |
| ② 分配器计数 | `memory_probe`（测试构建 `#[global_allocator]`） | 引擎**请求**了多少字节（`Layout::size`） | 只在测试二进制生效，生产 mimalloc 不可比——用于路径消融与回归保护 |
| ③ 分配器进程统计 | `alloc.peak_commit/peak_rss`（mimalloc `mi_process_info`） | 分配器**实际持有**多少 | macOS 上 commit 是估算值，与 RSS 对照看（commit≈rss → 无段区伪影） |
| ④ OS 级 | macOS `footprint` / `heap` / `malloc_history` | 段类别 / 尺寸直方图 / 调用栈 | **必须整程采样**，单点会撞低谷（§5 坑 1） |

**分账等式**：`peak_commit ≈ Σ窗口 + parse在途 + mailbox在途 + fanout通道 + 未归因`
——残差是唯一证明"账对上了"的量。每项都有现成指标（§6）。

## 3. "内存随总量增长"的三种形态——先判定形态再选对策

| 形态 | 特征 | 对策 | 本例证据 |
|---|---|---|---|
| **持有** | 存活量大、随量长 | 找持有者（消融） | 排除（heap 差分存活差仅 320MB） |
| **泄漏** | 无限增长、永不回落 | 找泄漏点（记账配对检查） | 排除（RSS 平台期、purged 在归还） |
| **churn** | 存活小、**分配器页水位高** | **降分配次数**（内联/复用/直写） | **真身**：MALLOC_SMALL reclaimable 2863MB 全链独有，同时存活仅 320MB |

churn 判据：`malloc 存活差分小` 但 `footprint reclaimable / 分配器页水位` 大。
内存 ∝ **分配次数**（不 ∝ 存活字节）。修复方向是减分配次数（SmolStr 内联、
复用 scratch、直连 commit），而不是找"持有者"或调缓冲预算。

## 4. 定位流程：墙梯 → 消融 → 差分

```
① 墙梯（切段）    diag.sh 五档（recv/decode/floor/rules/full）
                  → RSS 列定位增量在哪段（q13：12.5GB 全在输出链）
② 消融（切子段）  段内再切：WF_DIAG_CUT_ALERT（规则级 env 开关）
                  → 定位到 alert 构建段（EPS 21.7M / RSS 4.1GB 同时归零）
③ 差分（辨机制）  heap/footprint 差分 + malloc_history 调用栈
                  → 判定 churn（存活 320MB vs 页水位 12GB）
④ 修复（减分配）  从前向后逐段消减每行分配 → 每段用量化代理验证
```

关键：**① 和 ② 用的是引擎自己的门控（perf-diag 5 个 gate + 规则子集），
不改代码、不重启**；③ 才需要外部工具；④ 才是改代码。

## 5. 防坑清单（本 session 实踩，勿重蹈）

1. **单点采样会骗人**：一次 footprint 采到 dirty 3.4GB → 误判"RSS 虚高"，
   整程采样（峰值 14GB）推翻。内存测量必须看**整程曲线**（footprint --sample /
   rss 曲线），不能看单点。
2. **按比例外推是幻觉**："窗口按比例贡献 4GB"、"over 可省 1.7-2.8GB"、
   "1.75× 低估"——全是纸面推算，全被一条命令的实测打脸（事件时间跨度算术、
   over 对照跑、allocated_bytes 实测）。**算出来 → 立刻验证**。
3. **Arc 共享会双算**：`flush_pipes` 把同一批次 append 窗口 + 广播，通道条目与
   窗口是同一份 Arrow 缓冲——分账时直接相加得出过"通道预算 8.2GB"的错结论。
   先看 `Arc::ptr_eq`/排队批数，再决定能否相加。
4. **参数调优 ≠ 根因**：`over` 调小无效（ack floor 门控驱逐）、`parse_buffer_bytes`
   2GB→256MB 无效（实测在途仅 20MB）——配置兵役只是掩盖，先分账看预算是否
   真的是约束（`parse.inflight_bytes` 已用值）。
5. **会计口径有系统性偏差**：`content_bytes` 漏 bitmap/offsets（新建批次 ~1.75×）、
   `get_array_memory_size` 重复计共享缓冲（11.4× 虚高）——用按指针去重的
   `allocated_bytes`；改口径只影响观测，不动预算语义（预算仍用 content_bytes，
   避免连锁改变已调优的容量行为）。
6. **先消融再优化**：在错误段上优化（q13a pipe 足迹 4.2× 下降）不会让内存
   变好——消融（CUT_ALERT）一次就指出正确的段。

## 6. 已内建的测量设施（用它们，别再造）

| 设施 | 位置 | 用途 |
|---|---|---|
| 分配器分账 | `alloc.{current_rss,peak_rss,current_commit,peak_commit,page_faults}` | 层 ③：进程 vs 引擎缺口 |
| 阶段在途 | `parse.inflight_bytes/budget_bytes`、`window.mailbox_inflight_bytes`、`window.fanout_queued_batches/capacity_batches` | 分账等式各项 |
| 窗口真实占用 | `window.allocated_bytes`（按缓冲去重） | 层 ① 修正口径 |
| 输出链消融 | `WF_DIAG_CUT_ALERT=1`（临时 env，生产勿设） | 切 alert 构建段 |
| 分配计数探针 | `wf-runtime::memory_probe`（`MemoryProbe::exclusive` + `peak_growth`） | 层 ②：单测内量化 |
| 墙梯 | `diag.sh STAGES=recv,decode,floor,rules,full` | 段定位（EPS + RSS 两列） |
| 载荷形状守护 | `intermediate_broadcast_is_batch_only_for_round_robin_subscribers`（变异测试验证过） | 防内存修复回退静默复现 |

## 7. 工作准则（沉淀）

1. **测试优先于推测**：对"结论的依据"（修复的不变量、排除结论的数据来源、
   分账完整性）都要有守护测试——本 session 补的三类缺口（载荷形状、新 API
   读数、指标导出）都是"测试不存在则结论无依据"的地方。
2. **否定性结论同样要记录**：每次被实测否决的假设都让搜索空间减半，写进
   issue 文档（含否决它的数据），后人不再重走。
3. **测量设施本身也会错**：脚本键名、正则批量替换、语义细微改动（`v as f64`
   丢失）都出过错——靠既有守护测试抓住。改完先跑测试再下结论。
4. **内存/时间的墙常常同源**：q13 的 12.5GB 与 83% 时间增量是同一段（alert
   构建）——墙梯一次跑批同时给 EPS 与 RSS 两列，先看是否同一段。
5. **规模相关的"有界"不是 bug**：`over` 决定的窗口保留量随事件时间跨度增长
   是语义要求（nexmark 30M 跨度 50min < bid_mod over 1h → 全量保留）。判定前
   先算事件时间跨度 vs over（10M 跨度 16.7min < 两个 over → 窗口主导）。

## 8. 参考

- 本案完整排查记录：`docs/issues/q13-memory-peak-scales-with-volume.md`
  （9 次假设否决 + 每步数据）
- 吞吐定位方法论：`docs/PERF_BISECTION_METHOD.md`（墙梯机制）
- 诊断模式设计：`docs/design/perf-diag-mode-design.md`
- 反压设计先例：`docs/issues/window-overload-drop-vs-backpressure.md`
  （窗口满时丢 vs 反压，与 churn 属不同问题但共享"先分账再动手"纪律）
