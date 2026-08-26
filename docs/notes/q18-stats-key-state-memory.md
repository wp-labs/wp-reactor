# Q18 每键状态分账：键数 × 每键状态（2026-08-26）

## 结论

- **键数不可减**：q18 语义 = `(bidder, auction)` 分组取最后一条（权威 SQL
  `ROW_NUMBER() OVER (PARTITION BY bidder, auction)`）。**键数由数据特征决定**
  ——wfgen 实测聚集度恒 0.319（bid 的 auction 50% 引用最近 100 个、bidder
  75% 引用最近 100 人 → 组合高度聚集）：30M → **881 万键**、100M → 2935 万键
  （早期手记「30M 键数 2300 万」是错误估算——把键数当成了行数）。
  减键数 = 改分组键 / 改窗口 = 改语义，不可接受。
- **每键状态已压 -69%**（2026-08-26 已实施）：度量专用累加器 `StatsAccum`
  enum（208B 全功能 struct → 24B 变体），每键 1072B → 336B，30M 状态内存
  24.7GB → 7.7GB（< 10G 红线）。
- **guard 拒收阈值已修复**：预算高估 1.55×→1.29×，16GB 预算可容纳键数
  961 万 → 3703 万（> 30M 实测 881 万 / 100M 2935 万，不再静默丢键）。
- **列式 close 分块 flush**（§6）：q18 100M close 期全量收集峰值 30-35G + 60s
  超时 SIGKILL（EMIT=0）→ 分块后峰值 ~1G/块。

## 1. 数据（`cargo test --release -p wf-engine q18_stats_last_key_state -- --ignored --nocapture`）

**优化前 → 优化后**：

```
size_of::<StatsAccum>() = 208 B → 24 B（enum: Numeric(Box) 8B / Distinct(Box) 8B /
                            Last(Option<Arc<RowFields>>) 16B / Top(Vec<TopEntry>) 24B）
预算/键（allowance）   = 1664 B → 432 B（固定 256 + 4×16(Last) + 行字段共享 112）
真实/键（求和+槽估）   = 1072 B → 336 B
预算高估倍数          = 1.55× → 1.29×
16GB 预算可容纳键数    = 961 万 → 3703 万（30M 实测键数 881 万 ✓ 不再拒收）
30M 真实状态内存估算   = 24.7 GB → 7.7 GB（< 10G 红线 ✓）
```

运行形态：N=500K 列式，键域 bidder 1010 × auction 2M ≈ 每行唯一（对齐 30M 真实形态）。
多 last 度量 Arc 共享仍生效（499,939/499,939 桶共享 1 份 RowFields）。

> **键数实测**（wfgen gen-nexmark 采样，2026-08-26）：聚集度恒 0.319——
> 100 万/500 万/1000 万事件档位均 0.319（键数严格随 bid 数线性）。
> 30M → 881 万键；100M → 2935 万键。

## 2. 每键 336B 构成（优化后）

| 构成 | 大小 | 说明 |
|---|---|---|
| 4 × `StatsAccum::Last`（变体） | 4×16 = **64B** | 原 4×208B=832B → 变体后每度量仅 Arc 指针 |
| 共享 `RowFields` 堆 | ~104B | 4 last 度量 Arc 共享 1 份（不变） |
| ScopeKey 栈+堆 | 24+48 = 72B | Pair(Int,Int)；每桶首见构建一次 |
| StatsBucket 头 + HashMap 槽 | ~96B | 含 foldhash 控制字/entry 估算 |

> 关键：变体必须**全部指针级**（Numeric/Distinct 走 Box）——若 Distinct 内联
> 96B，enum 总大小会涨到 104B，4 个 Last 变体回到 416B，优化失效。

## 3. 关键发现：guard 预算高估 → q18 30M 静默丢键（正确性风险，**已修复**）

- 旧 `bucket_allowance`（last 按 160B/度量固定计）→ **1664B/键**，高估真实 1.55×。
- q18.wfl `limits { max_memory = "16GB" }` → 拒收阈值 = 16GB/1664B ≈ **961 万键**。
- 30M 数据真实键数 ≈ 2300 万 → **~1340 万新键被 `account_new_bucket` 拒收**，
  计数进 `over_limit_new_buckets`（仅 log warn，bench 的 `[clean]` 不检查该
  计数器 → **跑批报告 [clean] 但输出缺键**）。
- RSS 16G 的构成 = 961 万桶 × ~1072B ≈ 10.3GB + 固定开销（parse buffer 等）——
  即「16GB 顶格拒收」而非「真实状态 16GB」。
- **修复后**：allowance 432B/键（按变体实算 + 行字段每桶共享 1 份）→ 阈值
  3703 万 > 2300 万，30M 不再触发拒收。

> 验证：现有 guard 单测（`stats_memory_guard_rejects_new_buckets_over_limit`
> 等 8 个）全过；q18 形态量化的拒收阈值由 q18_state bench 输出。
> **待办**：q18 30M 实测确认 `over_limit_new_buckets = 0` + 对拍 q18-verify.wfl
> 基数核对（历史跑批是否已丢键需单独核）。

## 4. 优化方向：度量专用紧凑累加器（✅ 2026-08-26 已实施）

```rust
pub enum StatsAccum {
    Numeric(Box<NumericAccum>),        // 8B（count/sum/min/max 共享）
    Distinct(Box<DistinctSet>),        // 8B
    Last(Option<Arc<RowFields>>),      // 16B
    Top(Vec<TopEntry>),                // 24B
}
// enum 总大小 24B（tag + 最大变体 Top）
```

- 热路径访问器 `numeric_mut()`/`distinct_mut()`/`last_mut()`/`top_mut()`
  （调用点已按 `measure.agg` 分派，变体不符 panic 暴露内部错误）。
- **count 字段语义**：仅 Numeric 度量维护（avg 的 count/sum 同步 D6）；
  distinct/last/top 变体不再维护 count（原 count 为死状态，输出不读）。
- **预算口径同步**：`bucket_allowance` 按变体实算（Numeric 80 / Distinct 96 /
  Last 16 / Top 24+160N）+ 行字段**每桶共享 1 份** 112B（row_cache 每行 1 份
  Arc，同桶所有 last/top 度量共享）。

## 5. 验证与待办

- [x] q18 每键状态 bench（`q18_stats_last_key_state`，含 size_of + 预算 vs 真实分账）
- [x] `StatsAccum` 度量专用累加器 + 全路径适配（行式/列式/merge/close/估算）
- [x] `bucket_allowance` 预算口径按变体修正
- [x] **列式 close 分块 flush**（2026-08-26 q18 100M 修复，见 §6）
- [ ] q18 100M 重测（RSS 应从 24G 显著回落，close 不再 >60s SIGKILL，EMIT 恢复）
- [ ] 对拍 q18-verify.wfl 基数核对
- [ ] wf-runtime 全量 `columnar_*_matches_interpreted_path` flaky（既有，非本次
      引入——非阻塞 `try_recv` drain 竞态；基线 3/3 复现，独立修）

## 6. 列式 close 全量收集 → 分块 flush（2026-08-26 q18 100M）

**现象**：q18 30M 正常（EMIT 881 万，RSS 9.2G）；100M **EMIT=0 + daemon SIGTERM
后 60s 未退出被 SIGKILL**，close 期 RSS 峰值 24G（64G 机器上触发压缩/swap →
系统级显示 100G+）。

**根因**：`EMIT_CHUNK=100 万` 分块机制（q19 30M 峰值优化）**只实现了非列式
路径**（`emit_close_record`）；q18/q19 实际走的 **columnar close 路径全量收集
后一次性执行**：

```rust
let mut columnar_closes: Vec<CloseOutput> = Vec::new();  // 全量 2935 万条
for bucket in close_window_by_bucket_rows() { ... }       // 全量 StatsCloseBucket ~5.9G
    columnar_closes.push(close);                          // ~4-9G
execute_close_direct_batch_columnar(&columnar_closes, ..) // 一次性构建 ~10G
```

close 期峰值 ≈ 5.9 + 9 + 10 + 状态 9.8（allocator 未归还）≈ **30-35G 瞬间**。

**修复**（`stats_task.rs`）：rich（last/top）与标量快路径都改为满 `EMIT_CHUNK`
即 `flush_columnar_closes`（多次调用追加同一 builder，`finish()` 统一 commit +
投递——`register_yield_column` 幂等 + `reserve_rows` 追加语义保证等价）。
峰值 ≈ 100 万条×条均 ≈ 1G。`EMIT_CHUNK` 改 `AtomicUsize` 供测试调小触发分块。

**保护测试**：`stats_task_columnar_close_chunked_matches_full`（chunk=2 vs 全量
输出逐字节一致，不丢不重不乱序）。

**待验证**：100M 重测（预期 close 期 RSS 峰值 ~13G = 状态 9.8 未归还 + 分块
构建 ~1G + 窗口 3.6G，close 数十秒内完成，EMIT ≈ 2935 万）。

## 7. 坑链与教训（2026-08-26 q18 连环三坑）

1. **键数估算拍脑袋**：早期手记「30M 键数 2300 万」把键数当行数——NEXMark
   的 bid 引用「最近 100 auction/最近 100 人」热点，组合**高度聚集**（聚集度
   0.319 实测）。教训：**数据特征必须实测（wfgen 采样数键），不许外推**——
   键数错了，后面所有每键成本的推算全错。
2. **预算口径失真 → guard 静默丢键**：`bucket_allowance` 按「全功能累加器 +
   last 160B/度量」估 → 高估 1.55× → 16GB 预算只够 961 万键，30M 跑批静默
   拒收 62% 新键（`over_limit_new_buckets` 仅 log warn，bench `[clean]` 不查）。
   教训：**状态结构变更必须同步校准预算**（q16 distinct 同款教训）——且
   **guard 拒收必须进 bench 正确性检查**（当前不查，差点漏掉丢键）。
3. **同一机制只实现了一条路径**：`EMIT_CHUNK` 分块（q19 30M 峰值优化）只做
   了非列式 `emit_close_record`；q18/q19 实际走的 columnar close 全量收集+
   一次性执行 → 100M close 期 30-35G + 60s 超时 SIGKILL（EMIT=0），系统级
   显示 100G+（64G 机器压缩/swap）。教训：**批量优化机制要逐一核对所有执行
   路径**（rich/标量/列式/行式），并补跨路径等价测试
   （`stats_task_columnar_close_chunked_matches_full`）。

## 8. 5 轮 review 结论（2026-08-26 流式 close 后）

- **正确性/语义（轮 1）**: ✓ 无问题。merge 的 Last/Top 静默跳过由 spawn 门控
  保证（partial 不含行序敏感度量）; 行字段注入门控覆盖 mixed 度量（首个带 row
  的度量承担注入）; 流式 close 对拍只比每规则 EMIT 计数（批间顺序不影响）。
- **内存（轮 2）**: ✓ 分块独立 builder 后峰值受控（v1 的 builder 累积缺陷已
  修——见 §6 v2）; Arc<RowFields> 批间正确释放。
- **并发/异步（轮 3）**: ⚠ 记录——`EMIT_CHUNK` 全局 `AtomicUsize` 无锁（并行
  测试互扰风险低，测试用后恢复）; close 同步装载在 async 上阻塞 worker（既有
  架构，非本次引入）。
- **性能（轮 4）**: 修 `take_buckets_up_to` v1 的 O(n²)（mem::take 全表 + 剩余
  重插新 HashMap, 100M 30 批 ≈ 4.4 亿次重插 close +~9s）→ `retain` 原地移除
  （每批 O(n) 轻量回调, 无哈希重建, close ~3s）; `first_field_values.clone()` →
  `mem::take` move（省 2 键深拷贝 × 2935 万）。
- **边界/回归（轮 5）**: ✓ chunk=0（逐条 flush 退化但正确）; 空链不存在（insert
  才建链）; 双 finish（perf_cut 分支走一次性 close 不重复）; guard 拒收计数跨
  流式批不受影响。

## 9. 列式直写 close（2026-08-26 v3/v4）——q18 最终形态

**效果**：30M RSS 33.8G → **19.2G**（-43%）；100M ~60G（监视器）→ **33-36G**（-45%）。
语义/正确性全保（EMIT 881 万 / 2937 万，[clean]）。

**修复链**（v1-v4）：
1. v1 `EMIT_CHUNK` 分块 flush（builder 累积缺陷 → 每块独立投递）
2. v2 流式 close（`take_buckets_up_to` 分批 + retain 免 O(n²) 重插）
3. v3 `execute_stats_close_batch_columnar`——**不构建 CloseOutput**（直接消费
   `StatsCloseBucket`，省 per-record ~500B 分配；`close_batch_prepare_with`
   物化源参数化）
4. v4 **去 StepData**——`build_wfx_id_from_labels`/`build_summary_from_labels`
   泛型化（(label, measure) 迭代器，零 StepData 构造）

**分配量**：close 期 22G → ~4G（每条 ~30 次 → ~6 次分配）。
**RSS 停滞点**：v3/v4 后 RSS 不再随分配削减降（~20G）——剩余 = 状态语义
（3G/9.8G）+ 窗口（3.6G）+ parse（2GB 预算）+ allocator 保留 ~5-8G。

## 9.1 采样器修复后的真实峰值确认（2026-08-26 17:2x）

**背景**：`bench.sh` 采样器修复（232af6b，kill_daemon 后再 kill 采样器，覆盖 close flush 期）后，
q18 100M 真实峰值从 33-36G（漏采 close）修正为 **37.8-40.8G**：

| 跑批 | RSS_peak | 说明 |
|---|---|---|
| 批内 all 100M | 39,730MB | 前序 q17 27.2G 残留后仍持平 → 非批内污染 |
| 单独 100M #1 | 40,787MB | 采样覆盖 close flush 全期 |
| 单独 100M #2 | 37,755MB | 复现一致（±4% 相位内） |

**结论**：q18 100M RSS ~38-41G 为**真实稳态值**（状态 9.8G + 窗口 3.6G + parse 2GB +
allocator 保留 + close flush 峰值叠加），非采样器漏采、非批内污染。

**连带确认（批内污染 vs 真实值，同一跑批内排除法）**：

| 查询 | 批内 100M | 单独 100M | 判定 |
|---|---|---|---|
| q18 | 39.7G | 40.8G / 37.8G | ✅ 真实 ~38-41G（超标） |
| q19 | 23.5G | **7,066MB** | ❌ 批内污染（前序 q18 残留，单独 7.1G 达标） |
| q11 | EPS 2.5M | **16.2M / 4.0G** | ❌ 批内负载干扰（单独正常） |

**教训**：批内跑批的 RSS_peak 受前序查询 allocator 残留影响（尤其 close flush 期长、
峰值高的查询后），**任何超红线项必须单独跑确认**才下结论。

## 10. 分片开销待查（2026-08-26）

`RULE_PARALLELISM=10` vs 2 的 RSS 差 ~5.6G，**3 个实验全部证伪**：
- RULE_PARALLELISM=2：RSS 14.2G（-5.6G）但 EPS 5.2M（-62%）——性能不可接受
- RULE_CHANNEL_CAPACITY 256→64：RSS 仅 -0.6G（在途 < 容量）
- frame_mb 8→2：RSS 不变（在途整帧批不是主因）

**判断**：差异大概率是负载/allocator 波动（10 分片跑时 load 12-17；2 分片 load
6.3——消费慢窗口应堆积但 RSS 反低，不符合机制开销逻辑）。**定位需 malloc 追踪**
（macOS Instruments / malloc_history），记为待查。RULE_CHANNEL_CAPACITY=64 保留
（EPS 略升 14.66M、RSS 微降，无害）。

## 复用机制

- `RowFields` 紧凑行字段（本笔记前置优化，已入 REUSE_GUIDE）
- 多 last 度量 Arc 共享（`apply_last_top` 的 row_cache）
- `bucket_allowance` 预算模型 + `over_limit_new_buckets` 拒收计数（§3 暴露预算
  口径失真的教训：**新增/变更状态结构必须同步校准预算**，同 q16 distinct 教训）
- **列式 close 分块 flush**（§6：同路径多次执行追加 builder——批量语义保持，
  峰值按块限界；新机制候选入 REUSE_GUIDE）
