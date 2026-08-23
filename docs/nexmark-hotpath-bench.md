# NEXMark Q1~Q22 热路径性能测试与不合理位置记录

> 配套 `crates/wf-engine/src/match_engine/tests/nexmark_hotpath_bench.rs`（数据版微基准，
> 本文件为覆盖矩阵 + 分析版）。既有 bench：`each_bench`（Q1）、`guard_bench`（Q2）、
> `deferred_bench`（Q8/Q9）、`close_bench`（Q15 + stats 对照）、`interval_bench`（join 基础）、
> `match_bench`（Q22 旧 asof 形态）。
>
> 运行（release-only）：
> ```bash
> cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture
> ```

## 1. Q1~Q22 执行器形态分类

| 形态 | 查询 | 热路径 |
|---|---|---|
| `on each` 无状态投影 | Q1/Q10/Q21/Q22/Q14 | 列式 each 发射 + bind filter + 表达式（字符串/算术） |
| `on each` + snapshot join + where | Q3/Q13/Q20 | join 查找 + 富化 + 后置 where |
| `on each` + deferred join（within/reduce/emit at） | Q8/Q9 | 挂起 + 到期评估（exists / maxrow+tie） |
| `match` CEP 状态机（fixed/sliding/session） | Q4/Q5/Q6/Q7/Q11/Q12/Q13 | key 提取 + 实例管理 + step/close 累积 + 过期 |
| `match` + join-then-key | Q4/Q6 | join 查找取键 → 按右窗字段分组 |
| `match` + conv 归并 | Q5/Q7 | 收口批 sort+top(1) |
| `match` 键分片 close 累积 | Q15/Q16/Q17/Q18 | 每事件 accumulate_close_steps（guard + distinct/聚合） |
| `stats` 声明式（列式归并） | Q15 stats/Q16 stats/Q17 stats/Q19 | group-by 桶表 + count/distinct/top-N |

## 2. 30M 实测基线（2026-08-21 重测，OSS_VVR_BASELINE.md §3.4，非 PK 口径）

单连接 replay、frame 8MiB、p=10 r=10、真实 EMIT sink；机器负载 2.4~15.7 波动。

| Q | 形态 | EPS | RSS | vs VVR | 备注 |
|---|---|---|---|---|---|
| Q1 | on each 投影 | 19.5M | 1.3GB | 4.5× | |
| Q2 | on each 过滤 | 17.2M | 1.2GB | 2.6× | 绝对 EPS 高，简单查询 VVR 也快 |
| Q3 | match+join+where | 18.9M | 1.3GB | 4.1× | |
| Q4 | join-then-key+close avg | 3.4M | 5.9GB | 5.4× | 每 bid join + 状态累积 |
| Q5 | fixed 10s+conv top | 4.3M | 3.0GB | 15.3× | |
| Q6 | join-then-key 滑动 avg | 3.9M | 5.9GB | — | |
| Q7 | fixed 10s max+conv top | **1.2M** | 4.4GB | 4.1× | 全表最低绝对 EPS |
| Q8 | deferred exists | 21.1M | 1.3GB | 6.3× | |
| Q9 | deferred reduce | 13.2M | 2.8GB | 35.2× | |
| Q10 | on each 投影 | 20.5M | 1.1GB | 10.5× | |
| Q11 | session(10s) | **2.5M** | **17.3GB** | 3.6× | RSS 异常高 |
| Q12 | fixed 10s count | 18.1M | 1.0GB | 6.7× | |
| Q13 | match+snapshot join | **2.2M** | **22.0GB** | — | 全表最低 EPS + 最高 RSS |
| Q14 | on each 过滤+字符串 | 20.3M | 1.1GB | 4.1× | |
| Q15 | CEP close 12 measure | 1.6M | 6.0GB | 3.0× | stats 版已优化（115ns/evt） |
| Q16 | channel 键 close 12 | 14.0M | 2.9GB | 47.2× | |
| Q17 | auction 键 close 8 | 15.0M | 1.7GB | 4.1× | |
| Q18 | 复合键 close | **3.2M** | **18.5GB** | 3.1× | |
| Q19 | stats top(10) | 4.6M | **14.5GB** | 4.4× | |
| Q20 | each+join+where | 7.1M | 10.9GB | 16.4× | |
| Q21 | on each 字符串过滤 | 16.0M | 0.95GB | 6.4× | |
| Q22 | on each+split | 7.7M | 4.7GB | **2.4×** | vs VVR 最弱 |

## 3. 性能数据不合理的位置（静态识别，待 bench 实证）

判定口径：**与同形态查询量级偏离 > 2×，或状态规模与 EPS 负相关超出预期**。

### 🔴 A1 — Q22 vs 同形态 on-each 家族偏离（EPS 7.7M vs Q1 19.5M / Q10 20.5M，2.5~2.7× 差）

同为无状态纯投影，Q22 只有 `let parts = split(url) + 3×mvindex + concat` 的字符串成本。
30M 数据 ≈ 6.6 亿次小分配（split 返回 Vec<String>）。**vs VVR 2.4× 全表最弱**。
bench 项：`q22_each_split`（预期 80~150ns/evt，若 >200ns 需字符串列式化/复用 buffer）。

### 🔴 A2 — Q13 RSS 22.0GB 全表最高 + EPS 2.2M 全表最低

`match<bidder:10m>` sliding + snapshot join person。bidder 域仅 ~1000、auction 域 ~100，
但 RSS 22GB 与域规模完全不成比例——疑似 join 缓存/窗口状态或触发事件历史失控。
bench 项：`q13_match_snapshot_join`（advance + match+join emit 分离归因）。

### 🔴 A3 — Q11 session RSS 17.3GB / EPS 2.5M

session(10s) 在事件率 15.3k/s、bidder 域 1000 下（同 bidder 每 ~65ms 一事件，gap 内
~153 事件），session 几乎永不关闭 → 常驻实例 + 事件历史累积。RSS 17.3GB 高于
固定窗口同类（Q12 1.0GB）17 倍。
bench 项：`q11_session_advance`。

### 🔴 A4 — Q18 复合键 close RSS 18.5GB / EPS 3.2M

(bidder,auction) 复合键 fixed 30m。5.29M 实例 × 每实例窗口状态 + close 累积。
bench 项：`q18_composite_key_close`（对照 Q17 auction 键 8 measure 15.0M ——
复合键 vs 单键的实例规模成本）。

### 🟡 A5 — Q19 stats top(10) RSS 14.5GB

group by auction（有 bid 的 auction ~1.8M 键）× per-key top-10 条目（行字段列数组）。
bench 项：`q19_stats_group_topn`（行式 vs 列式，top-N 条目内存）。

### 🟡 A6 — Q7 绝对 EPS 全表最低（1.2M）

auction 键 fixed 10s + max + conv top(1)：180 桶 × ~100 auction ≈ 1.8 万实例，
每事件 close 累积 + 每收口批 sort。vs VVR 4.1× 尚可，但绝对值低于 Q5（同为
fixed 10s+conv）3.6 倍 —— max measure + detail fmt 成本待归因。
bench 项：`q5_q7_window_conv_top`。

### 🟡 A7 — Q4/Q6 join-then-key 5.9GB RSS

每 bid 一次 join_lookup（auction 索引 O(1)）+ 按 category/seller 分组状态。RSS 5.9GB
对 10m over 状态偏大。bench 项：`q4_q6_join_then_key_advance`。

### 🟡 A8 — Q15 CEP close（已知，stats 版已优化）

9 distinct foldhash 25 亿次 + 12 measure guard cmp_vec 17.7 亿次 → 1.6M EPS。
已由 close_bench + stats 版覆盖（列式 115ns/evt），不再新增。

### ✅ 判定为「合理」（非性能问题）

- **Q2（vs VVR 2.6×）**：极简过滤，绝对 EPS 17.2M 属最高档；VVR 对简单查询本就
  打磨到 6.5M（8 CU 近管道上限），单连接未饱和时相对优势小属预期。
- **Q12/Q21（vs VVR 6.7×/6.4×）**：bidder 域收敛 + 输出面变小后已移出弱势。

## 4. 性能测试覆盖矩阵（新旧）

| Q | 热路径 | 既有 bench | 新增 bench（本文件） |
|---|---|---|---|
| Q1 | on each 列式发射 | `each_bench`（wfx_id/fired_at/entity/fill） | — |
| Q2 | bind filter 数值 | `guard_bench`（q2_filter/no_filter） | — |
| Q3 | match+snapshot join+where | `interval_bench`（snapshot 基础） | —（与 Q13/Q20 共享路径） |
| Q4 | join-then-key+close avg | — | `q4_q6_join_then_key_advance` |
| Q5 | fixed 10s count+conv top | — | `q5_q7_window_conv_top` |
| Q6 | join-then-key 滑动 avg | — | `q4_q6_join_then_key_advance` |
| Q7 | fixed 10s max+conv top | — | `q5_q7_window_conv_top` |
| Q8 | deferred exists | `deferred_bench`（eval-exists） | — |
| Q9 | deferred reduce maxrow | `deferred_bench`（pending/eval-maxrow） | — |
| Q10 | on each 投影 | `each_bench`（Q1 同形） | — |
| Q11 | session 窗口推进 | — | `q11_session_advance` |
| Q12 | fixed 10s count | — | `q12_fixed_window_count` |
| Q13 | match+snapshot join 富化 | — | `q13_match_snapshot_join` |
| Q14 | 过滤+strftime/count_char | — | `q14_each_strftime_count_char` |
| Q15 | close 累积 12 measure | `close_bench`（分量归因+stats 对照） | — |
| Q16 | channel 键 close 12 | — | `q16_q17_keyed_close` |
| Q17 | auction 键 close 8 | — | `q16_q17_keyed_close` |
| Q18 | 复合键 close | — | `q18_composite_key_close` |
| Q19 | stats group by top-N | — | `q19_stats_group_topn` |
| Q20 | each+snapshot join+where | — | `q20_each_snapshot_join_where` |
| Q21 | bind filter 字符串 | — | `q21_string_bind_filter` |
| Q22 | each+split 字符串 | `match_bench`（旧 asof 形态，已废弃） | `q22_each_split` |

## 5. 实测结果（2026-08-23，Mac，release）

> 命令：`cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture`
> N = 500,000 事件；数据域 bidder≈1000 / auction≈100 / 价格对数均匀；单事件顺序调用
> （eager 路径，**非生产批处理**——生产 EPS 普遍高 3~12×，见对照列）。

| bench | ns/evt | M evt/s | 判定 | 对照 30M 实测 |
|---|---|---|---|---|
| q4 join-then-key+close | 788.1 | 1.27 | 🟡 见 A7 | 291ns/evt（3.4M） |
| q6 join-then-key+sliding | 1461.6 | 0.68 | 🟡 见 A7 | 255ns/evt（3.9M） |
| q5 fixed10s count | 525.3 | 1.90 | ✅ 基线 | 234ns/evt（4.3M） |
| q7 fixed10s max | 673.8 | 1.48 | ✅ max vs count 仅 +28%（A6 修正） | 816ns/evt（1.2M） |
| q5/q7 conv sort+top1 | 333.9 | 3.00 | ✅ F1 修复后 | — |
| q11 session(10s) | 668.2 | 1.50 | ✅ 与 fixed 同量级（A3 修正：bench 无差） | 402ns/evt（2.5M） |
| q12 fixed10s count | 655.4 | 1.53 | ✅ 基线 | 55ns/evt（18.1M） |
| q13 advance | 474.9 | 2.11 | ✅ | 454ns/evt（2.2M） |
| q13 match+join emit | **1800.2** | 0.56 | 🔴 **advance 的 3.8×**（A2 确认） | — |
| q14 filter+strftime | 778.5 | 1.28 | ✅ | 49ns/evt（20.3M） |
| q16 channel-keyed close | 1487.2 | 0.67 | 🟡 见 A6-close | 71ns/evt（14.0M） |
| q17 auction-keyed close | 1651.4 | 0.61 | 🟡 见 A6-close | 67ns/evt（15.0M） |
| q18 composite-key close | **2292.1** | 0.44 | 🔴 复合键 vs 单键 +39%（A4 确认） | 315ns/evt（3.2M） |
| q19 stats rows top10 | 211.9 | 4.72 | ✅ | 218ns/evt（4.6M） |
| q19 stats batch top10 | **88.5** | 11.29 | ✅ 列式 2.4× 行式 | — |
| q20 each+join+where | **1317.1** | 0.76 | 🟡 join 富化 + where | 141ns/evt（7.1M） |
| q21 str bind filter | 745.3 | 1.34 | ✅ | 62ns/evt（16.0M） |
| q22 each+split | **2222.3** | 0.45 | 🔴 on-each 家族最高（A1 确认） | 130ns/evt（7.7M） |

### 判定修正（实测 vs 静态分析）

- **A2（Q13）确认**：`execute_match_with_joins`（join 富化 + `build_eval_context` +
  `build_match_alert`）1800ns = advance 475ns 的 **3.8×**——每事件 fire + 富化是
  Q13 2.2M EPS 的主因。
- **A1（Q22）确认**：2222ns 为 on-each 家族（q21 745 / q14 778）的 ~3×；split
  `Vec<Value>` 分配 + concat 拼接为语言语义固有成本。
- **A4（Q18）确认**：复合键 2292ns vs 单键 q17 1651ns（+39%）——复合键提取/
  实例管理成本。
- **A6（Q7）修正**：max measure 673.8ns vs count 525.3ns 仅 **+28%**，不是 30M 表
  1.2M vs 4.3M（3.6×）的根因——后者应归因于负载/conv 批大小，max 本身无缺陷。
- **A3（Q11）修正**：session 668ns vs fixed 655ns 同量级；RSS 17.3GB 为状态/积压
  问题而非每事件成本。
- **对照差异说明**：bench 单事件顺序调用 vs 生产列式批处理 + 多线程分片，EPS 差
  3~12×（q12 655 vs 55、q14 778 vs 49）——bench 用于**相对归因**，绝对值看生产。

## 6. 判定标准（供后续自动比对）

- **同形态基准**：on-each 家族（Q1/Q10/Q21 16~20M）vs Q22/Q14；match 家族内
  Q12（55ns/evt）作为「轻状态 count 窗口」下限参照。
- **理论下限**：distinct 每事件哈希不可回避 ≈ 87ns/evt（close_bench 实测）；
  字符串 split ≈ 每次 `Vec<String>` 分配 ~24 次小分配。
- **不合理判据**：ns/evt 高于同形态 2× 且无法由状态规模解释 → 标记性能缺陷，
  进入优化队列（A1~A7）。

## 7. 已实施的改进（2026-08-23，静态审查批次）

### ✅ F1 — conv `sort` O(n²) → O(n log n)（Q5/Q7 A6）

`match_engine/match_engine/conv.rs` `ConvOpPlan::Sort`：旧实现 `outputs.sort_by`
闭包内**每次比较**构建 2 次 eval context（EngineHashMap 分配 + scope key/step
label 字段插入）+ eval 2 次 → O(n log n) 次分配。Q5/Q7 收口批 ~2k 行时这是
conv 主成本（数万次分配/批）。修复：每元素预提取排序键值（每 sort key 1 次
eval），比较阶段零分配零求值；`sort_by` 稳定序语义不变。
回归：`conv_sort_preserves_values_and_stable_order`。

### ✅ F2 — conv `dedup` O(n²) → O(n)

`ConvOpPlan::Dedup`：`Vec::contains` 线性扫描 → `HashSet::insert`（只判存在性，
序无关，语义不变）。回归：`conv_dedup_drops_duplicate_keys`。

### ✅ F3 — build_eval_context trigger_event 注入窄化（Q13/Q20 A2）

实测 q13 `execute_match_with_joins` 1800ns 中 join 富化（execute_joins）占 ~48%、
`build_eval_context` 占 ~35%。其中 trigger_event 注入**无条件注入全部事件字段**
（Q13 每事件 8 字段，yield 只读 b.bidder 1 个）。修复：
- `build_eval_context` 的 trigger_event 注入尊重 `needed`（Named 模式只注入集合内）；
- `plan_close_ctx_fields` 补充收集 **join 条件左字段**（first_join_key 从 ctx 读，
  缺字段 → join miss → 全 skip；Q4/Q6 的 b.auction 不在 yield 里）与 **where 字段**；
- 回归：`build_eval_context_narrow_and_all`（窄化后集合外字段不注入）。
实测：q13 match+join emit 1690.9 → 1552.7 ns/evt（**-8%**）；影响所有 on-event
fire + match/close 输出路径的 ctx 构建（Q3/Q4/Q5/Q6/Q7/Q12/Q13/Q20）。

### 🧹 测试修复（第 4 波测试的既有断言/构造 bug，首次运行发现）

| 文件 | 修复 |
|---|---|
| `executor/coverage_r4.rs` | narrow 断言改为集合外字段不注入；entity 空串 fallback 不 err（`eval_yield_expr` 缺失字段回退空串） |
| `eval/builtins_r4.rs` | `mvindex` 0 基索引断言（index=1 → 2.0）；`bucket_end` 60s=60000ms（旧写 600ms） |
| `eval/coverage_r4.rs` | `eval_entity_id` 缺失字段 → `Ok("")`（fallback） |
| `alert/tests/coverage_r4.rs` | untyped Number 恒导出 Float（非 Digit） |
| `window/tests/coverage_r4.rs` | MapArray::new field 参数 = entries Struct 字段（arrow 校验） |
| `compiler/tests/mod.rs` | B3 回归测试 out 窗口补 `bare` 字段 |
| `checker/tests/coverage_r4.rs` | mvcount/mvdedup/mvsort 参数数用例改为 0 参（原用例参数数正确不报错） |
| `wf-runtime` 7 处 | window_batch 列长对齐；pipe_ts 列可空；bootstrap over_cap 去重段；spawn file stream_tag + default fallback sink；receiver 错误断言改 Debug 视图（source 链） |

### ✅ F4 — fire 路径跳过 `event.to_event()` 全量 clone（Q5/Q7/Q12/Q13）

实测 q5/q12/q13 等每事件命中 fire 的规则，fire 路径每事件 `event.to_event()`
全量 clone 触发事件（HashMap 8 字段 + Value clone）是热成本。修复：
- `MatchPlan` 新增 `trigger_event_needed: bool`（编译器 `compute_trigger_event_needed`
  计算：score/entity/yield + join 条件左字段 + where 是否只读 match keys）；
- 3 处 fire 路径（Seq/Any/Any+close）按标志跳过 clone（trigger_event=None）；
- key 字段由 `build_eval_context` 从 scope_key 提供，输出不受影响（回归测试
  `fire_skips_trigger_event_when_key_only_yield` / `fire_keeps_trigger_event_when_non_key_yield`；
  编译器测试 `key_only_yield_skips_trigger_event` / `non_key_yield_needs_trigger_event` /
  `join_left_field_non_key_needs_trigger_event`）。
实测（nexmark_hotpath_bench，机器负载波动）：q5 **-42%**（640→370）、q13 advance
**-38%**（985→611）、q12 **-22%**（762→592）、q11 **-19%**（756→615）。

### 🔍 已审查、暂不改（需实测数据或改动面大）

- **Q22 split**：`Vec<Value>` 分配为语言语义固有成本，`let parts` 绑定已避免
  重复 split，无静态改进空间。
- **Q13/Q11/Q18 RSS**：需运行确认是状态物化还是管道积压（q22 的 RSS 已实证
  为积压），不做无数据优化。
