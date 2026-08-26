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

## 4.5 第二维度：列布局审计（段定位后、动手优化前）——补方法盲区

**盲区**：§2-§4 的分账/消融/差分只测"内存的量"（在哪段、多少字节），
**不测"数据的质"（列内值分布/唯一性）**。q13 因此多绕 5 轮：二分已把
6.1G 定位到列装载段，却解释成"在途积压/分配总量"，直到常量列折叠才
意识到"8 列 × 30M 行存的是同一批重复值"——**段内是否冗余，测量看不出来，
要看结构**。

**审计方法（唯一值密度）**：对段内的输出批/列做一次密度扫描：

```
每列 unique(值) / 行数
→ ≈ 1/行数（全列同值） → 常量列折叠候选（ColumnData::Const）
→ 低密度（少量重复）   → RLE/字典候选
→ = 1（每行不同）       → 正常列，不优化
```

**触发时机**：消融定位到段（"这段贡献 X GB"）之后、动手优化之前——先问
"段内数据结构是否合理"，再决定优化方向。q13 案例：`CUT_COLUMNS` 6.1G =
列数组总量 → 审计 12 列发现 **8 列全列同值**（6 系统常量列 + 2 字面量
yield）→ 常量列折叠省 8-11GB 分配总量 → 30M/100M RSS 双达标（14.3G→
3.5G、26.4G→4.9G）。

**产出复用**：审计发现的列直接对照 `dev/REUSE_GUIDE.md` §7 机制清单选型
（常量列折叠 `ColumnData<T>` / 批式 commit / 快车道……），不必再造。

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

## 6. 可复用资产（机制 / 指标 / API / 模式 / 测试——用它们，别再造）

### 6.1 诊断机制（直接跑，不写代码）

| 机制 | 位置 | 用法 |
|---|---|---|
| perf-diag 墙梯 | `wf-examples/performance/nexmark_pk/diag.sh` + `conf/perf-diag-wall.toml` | `WARMUP=0 STAGES=floor,rules,full ./diag.sh q13 30m` → EPS + RSS 两列 |
| 输出链消融 | `WF_DIAG_CUT_ALERT=1` env（`perf_diag.rs`，生产勿设） | 只切 alert 构建段、保留 pipe/join |
| 排查流程 | `docs/MEMORY_BISECTION_METHOD.md` §2-§4 | 墙梯→消融→差分→修复四步 |

### 6.2 可观测性指标（引擎已内置，直接读 `metrics.ndjson`）

| 指标 | 回答什么 |
|---|---|
| `alloc.{current,peak}_{rss,commit}`、`page_faults` | 进程 vs 引擎缺口（分账第一刀） |
| `parse.inflight_bytes` / `budget_bytes` | parse 在途是否真占预算（q13：2GB 预算只用 20MB → 预算假说排除） |
| `window.mailbox_inflight_bytes` / `budget_bytes` | 窗口 mailbox 在途 |
| `window.fanout_queued_batches` / `capacity_batches` | 规则分片通道排队（q13：555/2560=22%，且与窗口 Arc 共享不双算） |
| `window.allocated_bytes`（按缓冲指针去重） | 窗口真实占用——`content_bytes` 漏 bitmap/offsets，`get_array_memory_size` 重复计共享缓冲 |

### 6.3 可调用 API（新代码直接用）

| API | 位置 | 用途 |
|---|---|---|
| `RuleFanout::round_robin_only` / `queued_items` | `wf-engine/src/window/fanout.rs` | 订阅类型判断 / 通道排队量（含分片求和） |
| `Router::mailbox_inflight` | `wf-engine/src/window/router.rs` | mailbox 已用/容量字节 |
| `Window::allocated_usage` / `allocated_bytes(batch)` | `wf-engine/src/window/buffer/mod.rs` | 真实占用会计（驱逐/mailbox 预算仍用 content_bytes，勿混） |
| `MemoryProbe::exclusive` / `peak_growth` | `wf-runtime/src/memory_probe.rs` | 测试内量化分配峰值（N vs 3N 断言）；⚠ 全局计数器，独占执行（测试名过滤） |

### 6.4 优化模式（模式不是库，照抄到热路径）

| 模式 | 落点 | 适用场景（实测收益） |
|---|---|---|
| `Vec<Option<T>>` → Arrow builder | `PipeCol`（`rule_task.rs`） | 列装载：16B/值 → 8B+nullbitmap，字符串列 per-row 分配归零 |
| eval→stage 流式融合 | `PipeRowSink` trait（`each_exec.rs`）+ `PipeStagerSink` | 先物化整批中间结构 → 逐行 sink + 复用 scratch（811→195 B/行） |
| `Vec<String>` → `Vec<SmolStr>` | `AlertColumnBuilder`（`column_batch.rs`） | 短字符串列（≤22B 内联零堆分配）；fired_at 24B 超限保持 String |
| `StrSink` trait | `match_engine/key.rs` | String + SmolStrBuilder 统一渲染（2^53 边界由守护测试钉死） |
| 中转 Vec → 直连 `commit_each_row` | `each_exec.rs` | 消灭“累积整批 + 二次拷贝”（每行 3 String clone + staged cell clone） |
| `EntityCol` 列直读 | `each_exec.rs` | 免 `Value`/`SmolStr` 中转直读 Int64/Utf8 列（Int64/Utf8/Generic 三态） |

### 6.5 守护测试族（防回归，改引擎时跑）

| 测试 | 守护什么 |
|---|---|
| `intermediate_broadcast_is_batch_only_for_round_robin_subscribers` | 广播按订阅类型裁剪（**变异测试验证过**：`round_robin_only` 写反立刻失败） |
| `allocated_usage_tracks_real_buffers_and_drops_on_evict` | 会计配对完整性（append 增、驱逐归零，防记账虚增造假泄漏） |
| `str_sink_smol_builder_matches_string_rendering` + `hex_encode_smol_matches_string_version` | SmolStr 直写与 String 版本字节一致（entity_id / wfx_id 静默变值防护） |
| `queued_items_reports_backlog_across_shards` | fanout 排队读数（空队/压入/消费回落四态） |
| `parse_inflight_gauges_are_exported` + `window_memory_accounting_gauges_are_exported` | 分账指标必须被导出（防指标名/采样静默失效导致“误判已排除”） |
| `pipe_write_alloc_footprint`（ignored bench） | 分配足迹量化代理 + 会计保真度（④ 项）——改一步测一步的标尺 |

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
