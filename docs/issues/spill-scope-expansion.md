# Issue：spill 限额作用域扩展——每分片 → 规则级共享（spill-scope-expansion）

> 状态：✅ 已落地（2026-08-27）
> 优先级：高（q18 100M 限额语义正确性 + 静默丢键兜底）
> 关联设计：`../design/stats-state-spill-redb.md` §19（`max_disk` 规则级共享）、§20（`max_memory` 规则级共享）
> 影响 crate：wf-lang（键解析 / checker 校验 / lint 迁移提示）、wf-engine（stats 记账 `StatsWindowState` / `StatsExecutor`）、wf-runtime（`spawn.rs` 规则级注入）
> 复现场景：`wf-examples/performance/nexmark_pk` → `./bench.sh q18 replay 100m`

---

## 1. 问题定义

分片数（`shard_count`）是引擎内部细节（用户不可见），但 spill / 内存限额的旧语义按**每分片**生效：

- `max_memory` 旧语义 = 每分片驻留上限 → 10 片 = 10× 实际驻留（2GB/片 × 10 = 20GB）
- `max_spill_bytes` 旧语义 = 每分片磁盘上限 → 8GB/片 × 10 = 80GB 磁盘峰值

两个后果：

1. **违背用户直觉**：用户配置 `max_memory = "2GB"` 期望整个规则最多驻留 2GB，实际驻留 20GB；磁盘同理（配 8GB 实际可写 80GB）。
2. **限额无法兜底 → 静默丢键**：共享预算（内存 + 落盘）低于状态总量时触发 `over_limit_new_buckets` 拒收，而 **bench `[clean]` 正确性计数不覆盖该指标**——100M 实测丢 451 万键仍报 clean，只能靠 EMIT 与无 spill 基线对拍发现。

## 2. 变更范围

| 配置键 | 旧语义（每分片） | 新语义（规则级共享） | 兼容 |
|---|---|---|---|
| `max_memory` | 每分片驻留上限（N×limit） | **规则总驻留上限**——同规则全部分片共享一个占用计数 | — |
| `max_spill_bytes` | 每分片落盘上限（N×limit） | → 改名 **`max_disk`**，**规则总磁盘上限** | 旧键保留为别名：解析接受 + checker Warning 提示迁移（见 `wf-lang/src/checker/rules/limits.rs`） |
| `spill` | 每分片落盘开关 | → 改名 **`disk_provider`**，状态落盘后端（`"redb"`；`max_disk` 未配置 = 落盘不限，仅靠内存预算 + redb 写失败兜底） | 旧键保留为别名：解析接受 + checker Warning 提示迁移 |

**注入方式**（`wf-runtime/src/lifecycle/spawn.rs`）：每条规则创建一个 `Arc<AtomicU64>`（内存、磁盘各一），全部分片 clone 注入共享计数；单实例 = 1 分片，共享语义自然退化为单片。未注入共享计数（测试 / 单片直连）→ 自建本片独立计数，语义退化为旧行为。

## 3. 三层预算阶梯（检查口径改为共享计数）

```
内存 mem_used ≤ max_memory（规则共享）            → 全内存，不 spill
内存超 max_memory 且 共享 disk_used ≤ max_disk   → spill 最老键（LRU）腾空间
共享 disk_used ≥ max_disk                        → 停止 spill，回退拒收（over_limit_new_buckets + 告警）
redb 写失败（磁盘满 / IO 错）                     → 同拒收（计数 + 致命告警，不静默丢）
```

- `over_limit_new_buckets` 保留为**兜底**（spill 满 / 写失败才触发）
- 预算检查全部走共享计数：某分片用满后，其余分片驱逐回退拒收（规则级兜底生效）

## 4. 实现要点

记账口径（`stats_exec.rs` `StatsWindowState`，内存与磁盘对称）：

```
新建桶（account_new_bucket）   → mem_add(allowance)
驱逐落盘（evict_to_spill）     → mem_sub(allowance × 链长)  +  disk fetch_add(allowance × 链长)
读回（readback_bucket_mut）    → mem_add(allowance)          +  disk fetch_sub(allowance)
批末重算（refresh_estimated）  → mem_add/sub(差值)
close（take_buckets / reset） → mem_sub(本片净占用)；流式 drain disk fetch_sub —— 预算跨窗口复用
```

- `StatsExecutor` 持有共享计数（`mem_used_shared` / `spill_redb`，与 `SharedLimits` P2b 同模式）——reset_window 恢复新窗口，跨窗口持续生效
- 窗口 close 后共享计数归零（预算跨窗口可复用）

## 5. 验证（q18 100M，2937 万键 × ~960B ≈ 28.2GB 状态总量）

| max_memory | max_disk | RSS_peak | EPS | EMIT | 结论 |
|---|---|---|---|---|---|
| 2GB/片（旧） | 8GB/片（旧） | 28.3GB | 5.19M | 29,370,378 | 语义错误：实际 20GB 驻留 + 80GB 磁盘 |
| 2GB 规则 | 8GB 规则 | 25.9GB | 9.59M | **24,865,600** | 内存+落盘（2+8=10）< 28.2GB → 拒收丢 451 万键 |
| 10GB 规则 | 12GB 规则 | 23.2GB | 2.08M | **24,865,599** | 10+12=22 < 28.2GB → 仍拒收丢 451 万键 |
| 10GB 规则 | 20GB 规则 | 22.4GB | 1.38M | 29,370,378 | 10+20=30 > 28.2GB ✓ 零丢失（RSS 峰值在 close 期） |
| **15GB 规则（拍板）** | **20GB 规则** | **24.0GB** | **3.25M** | 29,370,378 | EPS 优先：内存多驻留 5GB → spill 少 4.8GB → EPS ×2.4 |

基线（无 spill 全内存）：12.6M EPS / 35-40GB RSS / 29,370,378 EMIT

## 6. 关键认知（教训）

1. **状态总量才是分母**：内存 + 落盘预算之和必须 ≥ 状态总量（28.2GB），否则拒收丢键——每次降低预算都要用 EMIT 对拍验证（bench `[clean]` 不可信）。
2. **EPS 与 RSS 权衡**：spill 每写 1GB ≈ 背压。20GB 落盘 → EPS 1.38M（vs 无 spill 12.6M，-89%）；要 EPS 高就得加大内存驻留（RSS 涨）。用户需按「RSS ≤ 20GB 优先」或「EPS 优先」定配置取向。
3. **RSS 峰值出现在 close 期**：读回 18GB 分批 + 输出 2937 万 alert 同时驻留；稳态 RSS ≈ 状态驻留 + 固定开销 ~8.8GB。
4. **bench `[clean]` 盲区**：`over_limit_new_buckets`（内存/spill 双拒收）不在 bench 正确性计数内——须纳入硬性检查。

## 7. P0 实测：spill 扩展到 q19（stats top-N，2026-08-27）

**目标**：把 spill 从 q18（Last）推广到 Top 度量，验证全链路 + 权衡。
**现状**：q19 `stats<10m:fixed> group by (b.auction)` + `top(10, b.price)`——100M 状态仅
**~0.5GB/窗**（每窗 ~36 万 auction × 10 条目，每片 close 时估算 157MB / 实际 51MB），
RSS 7.3GB **已达标**，本质无内存问题。

**验证矩阵**（`./bench.sh q19 replay 100m`，EMIT 基线 = 无 spill 44,145,610）：

| 配置 | EMIT | RSS_peak | EPS | 结论 |
|---|---|---|---|---|
| 无 spill 16GB（基线） | 44,145,610 | 7.3GB | 11.5M | 达标 |
| spill 4GB + 8GB（零驱逐） | 44,145,610 | 12.5~13.7GB | 9.8~11.9M | 正确性 ✓ 但 RSS +6GB |
| spill 300MB + 8GB（强制驱逐） | **44,089,718** | 19.0GB | 5.6M | **丢 55,892 键**（拒收，[clean] 盲区不报） |
| 无 spill 4GB（对照） | 44,145,610 | 7.3GB | 10.6M | max_memory 本身无影响 |

**发现 1：store 空转 churn → RSS +6GB**。配 spill 但零驱逐（状态 << 预算）时，
17 窗口 × 10 片 = **170 次 `RedbSpillStore::create` + cleanup**（含 worker 线程/redb 库/
异步写队列），mimalloc 分配器段区水位不归还 → close 期 RSS 13.2GB vs 无 spill 7.3GB。
`WF_SPILL_CACHE_MB=16` + `WF_SPILL_QUEUE_BYTES=16MB` 无效（非缓存/队列本身）。
**修复方向**：store **惰性创建**（首次驱逐才 `create`，零驱逐窗口零开销）——
改动小（spawn 注入改为配置，`evict_to_spill` 前 `ensure_spill_store`）。

**发现 2：预算 < 状态总量时驱逐滞后 → 拒收丢键**（§6 教训 1 的机制细节）。
300MB 共享预算 < 状态 ~500MB/窗：clock 二次机会驱逐（`max_scan` 限制 + touch 保护）
跟不上新键到达速率 → `over_limit_new_buckets` 拒收（日志实锤 17,464/23,228/12,773 行），
EMIT 对拍少 55,892。**配置指导**：预算须 ≥ 状态总量 × ~1.5（留驱逐滞后余量），
且每次降预算必须 EMIT 对拍（[clean] 不可信）。

**P0 结论**：Top 度量 spill 正确性成立（驱逐 20 万键 / 读回率 0.3% / close 合并零重复），
但 q19 状态小、无需 spill；**当前 spill 仅对 q18 这类「状态 >> 预算」查询有价值**。

### P0 修复：spill store 惰性创建（已落地 2026-08-27）

**实现**（`stats_exec.rs`）：`StatsWindowState` 增加 `spill_create: Option<SpillCreateSpec>`
（纯数据 `path + layout`，Send+Sync）——executor 每窗口 process 时注册创建规格
（不再立即 create），`account_new_bucket` 超限落盘时 take spec 才
`RedbSpillStore::create`。零驱逐窗口不建 redb 库/不起写 worker → 零开销。

**修复后实测**（`./bench.sh q19 replay 100m`，spill 4GB+8GB 零驱逐）：

| 指标 | 修复前 | 修复后 |
|---|---|---|
| RSS_peak | 12.5~13.7GB | **7.36GB**（回到无 spill 基线） |
| EMIT | 44,145,610 | 44,145,610（不变） |
| EPS | 9.8~11.9M | 13.2M |

**回归**：wf-engine 1214 测试 + wf-runtime 581 测试全绿；q18 100M 拍板配置
（驱逐 ~13GB 落盘）EMIT 29,370,378 与基线一致、RSS 24.0GB（与拍板记录相同）——
惰性创建不影响真实驱逐场景。

## 8. 遗留 / 后续

- [x] **spill store 惰性创建**（P0 发现 1，已修）：首次驱逐才建 store，零驱逐窗口零开销——修复「配 spill 但用不上 → RSS +6GB」（修复后 q19 100M RSS 7.36GB 回到基线）
- [x] **文件生命周期核实 + 冲突修复**（2026-08-27，见 §10）：读取/清理时机文档对齐代码；补启动清理 ④、create 删旧建新、`is_empty` 读侧 flush 竞态
- [x] **并发过度驱逐修复**（2026-08-27，见 §11）：25GB 配置 EPS 反降根因——多片并发超限时每片各驱逐水位差（过度 10×）；修复为逐链预订共享计数
- [ ] **预算配置指导**（P0 发现 2）：预算须 ≥ 状态总量 × ~1.5 留驱逐滞后余量；写入配置模板/文档
- [ ] `over_limit_new_buckets` 纳入 bench 硬性正确性检查（防静默丢键误报 `[clean]`）
- [ ] `max_spill_bytes` 别名废弃计划（当前 checker Warning 提示迁移，`wf-lang/src/checker/rules/limits.rs`）
- [ ] **输入分片（空键 stats 按行号切分）+ spill 暂不兼容**：配置了 spill 则 warn 并忽略（`spawn.rs` `input_shardable` 分支）
- [ ] close 期 RSS 峰值优化（读回分批 vs 输出驻留共存）
- [ ] 配置取向模板（RSS ≤ 20GB 优先 / EPS 优先）供用户一键选择

## 10. 文件生命周期核实（读取/清理时机与冲突，2026-08-27）

**问：spill 文件什么时候读？什么时候清理？同规则不同窗口会不会冲突？**

**文件 = 每任务实例/每分片一个**（`spill_{rule}_{pid}{_shard}.rb`，不含
window_start）——同一实例的连续窗口**复用同一路径**。窗口在单任务内严格串行
（一个 window 状态，close 即 reset），配合 close 清理，正常路径无并发。

**读取 3 个时机**：

1. **窗口进行中，驱逐键再来** → `take(hash)` 读回单键（命中内存 `spill_index`
   → redb 读回 → 内存副本继续累积，`readback` 集过滤 close 时的旧条目）。
2. **窗口 close** → `drain_up_to(n)` 流式分批读回（默认 5 万/批），与内存桶
   归并排序输出；读前 flush 异步写队列（已提交 = 已可见）。
3. **诊断/测试** `contains(hash)`（热路径用内存索引，不碰持久层）。

**清理 3 个时机**（全部已落地）：

1. **窗口 close / reset** → `cleanup()` 删 `.rb` + `.rbr`（跑批结束即清，正常
   路径磁盘零残留）。
2. **进程正常关闭** → Drop 只停写 worker（不删文件）——文件已随窗口 close
   删除；窗口未 close 就退出则残留。
3. **启动清理**（本次补上）：`Reactor::start` 最早删 `WF_SPILL_DIR` 下全部
   `spill_*.rb/.rbr` 崩溃残留。

**冲突结论**：

- 同规则连续窗口**无冲突**（串行 + close 即清 + 每窗空库起步）。
- key 分片独立文件独立写 worker，互不干扰；多进程文件名含 pid 不撞。
- 本次修复 2 个边界：**①** cleanup rm 失败残留会被 create 打开 → 旧窗键污染
  新窗输出（`RedbSpillStore::create` 现在打开前删旧建新，删失败致命）；
  **②** `is_empty()` 读 redb 表长度不 flush 异步写队列 → close 紧跟驱逐时
  `merge_spill_into_buckets` 早退丢 spill 键（`len()` 现与 contains/take/drain
  一致先 flush——竞态实测复现于多窗测试）。
- 多实例共用同一 `WF_SPILL_DIR` 时启动清理会互删文件——各实例须独立目录。

新增测试：`spill_consecutive_windows_same_rule_fresh_each_window`（两窗复用
同路径，无旧窗污染）、`spill_create_over_stale_file_starts_fresh`（旧文件不
被打开）、`cleanup_leftover_spill_files_in_removes_only_spill_files`（启动清理
只删 spill 文件）；改写 `redb_reopen_starts_fresh_and_drops_stale_file`（旧
「重开保留数据」契约与设计相悖，废弃）。

## 11. 并发过度驱逐修复（2026-08-27：q18 25GB 配置 EPS 反降根因）

**现象**：q18 100M replay，`max_memory=25GB` + `max_disk=20GB`（内存+落盘
45GB >> 状态 28.2GB，本应接近全驻留）EPS 仅 **2.07M**，低于 15GB 拍板的
3.25M。wfusion.log 实锤：10 片**各驱逐 260 万键（~2.5GB）、calls=1**、驱逐耗时
scan≈1.9s + clone≈0.7s **每片 2.6s 热路径同步阻塞**。

**根因（`stats_exec.rs` `evict_to_spill`）**：驱逐循环条件 `mem_used - pending >
target`，`pending` 是**每片局部**变量（写盘成功后才一次性扣共享计数）。10 片
并发超限时**每片都看到共享计数超限、各驱逐水位差**（25GB 上限 → target
22.5GB → 水位差 2.5GB）→ 总驱逐 = 2.5GB × 10 = **25GB**，而实际只需 3.2GB
（28.2 - 25）→ 过度驱逐 10×：落盘写放大 + 驱逐期间热路径阻塞。15GB 拍板时
需求 13.2GB ≈ 水位差×10 = 15GB，碰巧接近未暴露。

**修复（逐链预订）**：驱逐循环每选一个链就原子扣减共享内存计数（`mem_sub`）
——共享计数成为**单一事实源**，循环条件用实时值。多片并发时逐链原子扣减，
共享计数停在 target，总驱逐 = 超限部分（3.2GB 而非 25GB）。写盘失败/满时按
`reserved` 归还（驱逐未生效，内存键未删，不丢键）。

**修复后预期**：每片驱逐 ~0.32GB（26 万键）而非 2.5GB，驱逐耗时 ~0.3s 而非
2.6s，落盘 3.2GB 而非 25GB——EPS 应显著回升（接近/超过 15GB 拍板）。

**测试**：`spill_evicts_to_target_exact_overage`（驱逐记账一致：驱逐+内存=注入
总量，无拒收；内存/落盘共享计数各自一致）、
`spill_shared_memory_counter_no_over_eviction_under_concurrency`（两线程并发
驱逐，共享计数 ≥ target - 竞态余量——不过度驱逐回归保护）。注意：并发竞态
对旧代码非确定性（顺序调度下旧代码同样精确），修复效果最终以 q18 bench
（驱逐量/耗时）为准。

## 9. 关联文档

- `../design/stats-state-spill-redb.md`（设计总览 + §8 文件生命周期 + §19/§20 本次变更与实测）
- `wf-examples/performance/nexmark_pk/models/queries/q18.wfl`（配置已迁移至拍板值，注释含新口径）
- 实现：`crates/wf-engine/src/match_engine/executor/stats_exec.rs`、`crates/wf-engine/src/match_engine/spill.rs`、`crates/wf-runtime/src/lifecycle/spawn.rs`
