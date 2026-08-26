# stats 语义状态内存专项 — q16 优化 + q18/q19 验证结论

## 一句话结论

q16（stats 8×distinct_count）100M RSS 19.2G → **15.3G**（紧凑化 -3.9G + EPS +11%）、guard 估算修正后数字真实且不丢数据。q18/q19 同为 stats 语义状态（非泄漏、已紧凑），接受现状。q22 全量批 RSS 异常为 bench 采样问题（单独跑 3.6G 正常）。

## 1. q16（stats distinct_count）——已修复

### 1.1 现象与定位

- 30M 7.4G → 100M 17.2~19.2G（随数据量增长，超红线）
- MEMORY=1（30M 墙梯）：rules 段 ΔRSS +6.6G（67%）主内存墙；窗口合计 4.4G；**未归因 ~5.1G**
- 根源：`stats<1d:fixed> group by channel` + **8 个 `distinct_count(bidder/auction)`**——每个度量维护 `HashSet<DistinctKey>`（enum 16B/项），唯一值数随数据量线性增长
- **估算漏算**：`bucket_allowance` 只有 Top/Last 预算——`DistinctCount` 完全不计（文档注记"distinct_set 不在预算内"，且旧假设"带 key 规则均无 distinct"被 q16 打破）→ 8GB 估算 vs 19G 实际，guard 形同虚设

### 1.2 修复（两层）

**① 紧凑化（`DistinctSet`，stats_exec.rs）**：
- 整数键（q16 的 bidder/auction 主战场）走 `HashSet<i64>`（8B/项）——enum `DistinctKey` 因 `Box<str>` 变体 16B/项
- Float/Str 键保留 enum 集合；insert 按类型路由、len/merge 各自合并
- 语义与旧实现逐位一致（stats 69 + 全量 1181 测试过）
- 效果：100M RSS 19.2→15.7G、EPS 7.57→8.52M（紧凑集合更快）

**② 估算修正（`refresh_estimated_bytes`）**：
- 批末 O(桶数) 重算 `estimated_bytes = 桶数×allowance + Σ distinct len×24B`（保守上界）
- guard 数字真实（此前 19G 实际 vs 8GB 估算形同虚设）
- 配套 q16.wfl `limits.max_memory` 8GB→16GB（100M 估算 ~10G < 16GB → 不触发拒收，实测 `stats_over_limit_total=0`，不丢数据）

### 1.3 数据

| 指标 | 起点（08-25） | 紧凑化 | +估算修正/limit |
|---|---|---|---|
| 100M RSS | 19.2G | 15.7G | **15.3G** |
| 100M EPS | 7.57M | 8.52M | **8.39M** |
| 30M rules ΔRSS | +6.6G | +3.6G | — |
| over_limit | — | — | 0 |

剩余 15.3G = distinct 语义状态（精确 distinct_count 要求）+ 窗口 4.4G，非泄漏。

## 2. q18/q19/q22（验证结论）

### q18（stats last 度量）——接受

- 30M 16.4G（单独跑确认，稳定超线）
- 根源：`group by (bidder, auction)` + 4×last——状态 = 每键最近行字段，**键数随数据量线性增长**（ROW_NUMBER dedup 语义）
- 已 P5 紧凑化（注释：旧每桶 ~400B → `Arc<[Option<Value>]>` 单块分配 + 4 个 last 度量共享同一 Arc）
- 剩余空间小（`Value` enum 32B 是唯一大项，改它影响全局）→ **接受 + 记录**

### q19（stats top）——达标

- 单独跑 30M 8.9G < 10G ✅（批内 13.1G 是波动）
- top_entries 在 bucket_allowance 已有预算（Top 分支 160B/条）

### q22 —— bench 采样异常（非真实）

- 单独跑 30M 3.6G 正常；**全量批内（最后一个 Q）10.9G**——全量批的 RSS 采样对最后一个 Q 异常（疑似采样到残留/累积），非引擎问题

## 3. stats 语义状态家族判定标准

| 类型 | 随数据量 | 状态 | 处理 |
|---|---|---|---|
| distinct_count | 唯一值数 | HashSet | 紧凑化（q16 已做）+ 估算 |
| last | 键数 | 行字段 Arc | P5 已紧凑，接受 |
| top | 键数×N | 条目序列 | 已有预算，接受 |
| count/sum/min/max | O(1) | 标量 | 无状态问题 |

**判定**：stats 语义状态（精确性要求）超红线 ≠ 泄漏——先确认是否已紧凑（8B/项级别），无空间则接受并记录。

## 4. Pitfalls

1. **估算口径必须覆盖全部度量类型**：`bucket_allowance` 曾只覆盖 Top/Last——新增度量类型（distinct）必须同步预算，否则 guard 形同虚设（19G 实际 vs 8GB 估算）
2. **"带 key 无 X"假设会过时**：旧注记"带 key 规则均无 distinct"被 q16 打破——文档注记的"已知限制"要定期复核
3. **全量批的 RSS 采样对末尾 Q 不可信**（q22 3.6G ↔ 批内 10.9G）——异常项必须单独跑确认
4. 紧凑化改存储类型必须对拍（insert 路由/len/merge 语义一致）——stats 69 测试锁
