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
| `on each` + snapshot join + where | Q3/Q13/Q20 | 列式 join 富化（F6：批级 lookup + 列式右窗读）——死 join（q13）消除后走无 join 列式路径 |
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
| Q4 | join-then-key+close avg | 3.09M | 7.3GB | 5.4× | 批级 join-then-key 后 2.66M→3.09M（+16%，A/B 同负载） |
| Q5 | fixed 10s+conv top | 4.3M | 3.0GB | 15.3× | |
| Q6 | join-then-key 滑动 avg | **0.55M** | 7.6GB | —（无对标） | 26M EMIT 每事件 emit 路径（CPU ~106% 单核）；F7-F9 后 0.50→0.55M；官方未落地（无 Flink 测试数据），不再投入（§8.4） |
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
bench 项：`q22_each_split`。实测 **2069ns**（2026-08-23 F4 后）——超出 §3 阈值
（>200ns 需优化）11 倍，**待重新评估**（split buffer 复用 / mvindex 免 Value 包装
/ concat 预分配），见 §7 遗留。

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
| Q4 | join-then-key+close avg | — | `q4_q6_join_then_key_advance` + `q4_q6_join_then_key_batch_precompute`（批级预解析 vs 行式对拍+加速比） |
| Q5 | fixed 10s count+conv top | — | `q5_q7_window_conv_top` |
| Q6 | join-then-key 滑动 avg | — | `q4_q6_join_then_key_advance` + `q4_q6_join_then_key_batch_precompute` |
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
| Q20 | each+snapshot join+where | — | `q20_each_snapshot_join_where`（行式）+ `q20_each_snapshot_join_where_columnar`（列式 F6，含行/列对拍断言） |
| Q21 | bind filter 字符串 | — | `q21_string_bind_filter` |
| Q22 | each+split 字符串 | `match_bench`（旧 asof 形态，已废弃） | `q22_each_split` |

## 5. 实测结果（2026-08-23，Mac，release，**F4 后**）

> 命令：`cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture`
> N = 500,000 事件；数据域 bidder≈1000 / auction≈100 / 价格对数均匀；单事件顺序调用
> （eager 路径，**非生产批处理**——生产 EPS 普遍高 3~12×，见对照列）。
> 本表为 F4（fire 跳过 trigger_event clone）之后全量混跑；机器负载波动 ±30%——
> F4 受益查询的单跑稳定值见判定修正（q5 370 / q12 592 / q11 615 / q13 611）。

| bench | ns/evt | M evt/s | 判定 | 对照 30M 实测 |
|---|---|---|---|---|
| q4 join-then-key+close | 837.1 | 1.19 | 🟡 见 A7 | 324ns/evt（3.09M，批级预解析后） |
| q4 join-then-key 批级预解析 | **327.8** | **3.05** | ✅ 行式 68.7%（**1.46×**），同批对拍一致 | — |
| q4 join-then-key 行式（内部解析） | 477.0 | 2.10 | 基准（同批对拍） | — |
| q6 join-then-key+sliding | **1176.7** | 0.85 | 🟡 见 A7；F4 后 -19%（1462→1177） | **1960ns/evt（0.51M）**——旧 255ns（3.9M）过期：26M EMIT 每事件 emit 路径为瓶颈 |
| q6 join-then-key 批级预解析 | **743.9** | **1.34** | ✅ 行式 83.4%（**1.20×**），同批对拍一致 | — |
| q6 join-then-key 行式（内部解析） | 892.2 | 1.12 | 基准（同批对拍） | — |
| q5 fixed10s count | 571.8 | 1.75 | ✅ 基线；F4 单跑 370 | 234ns/evt（4.3M） |
| q7 fixed10s max | 681.7 | 1.47 | ✅ max vs count 仅 +19%（A6 修正） | 816ns/evt（1.2M） |
| q5/q7 conv sort+top1 | 315.1 | 3.17 | ✅ F1 修复后 | — |
| q11 session(10s) | 709.3 | 1.41 | ✅ 与 fixed 同量级（A3 修正）；F4 单跑 615 | 402ns/evt（2.5M） |
| q12 fixed10s count | 697.0 | 1.43 | ✅ 基线；F4 单跑 592 | 55ns/evt（18.1M） |
| q13 advance | 696.0 | 1.44 | ✅ F4 单跑 611（-38%） | 454ns/evt（2.2M） |
| q13 match+join emit | **1877.9** | 0.53 | 🔴 **advance 的 2.7×**（A2 确认） | — |
| q14 filter+strftime | 766.1 | 1.31 | ✅ | 49ns/evt（20.3M） |
| q16 channel-keyed close | 1566.6 | 0.64 | 🟡 见 A6-close | 71ns/evt（14.0M） |
| q17 auction-keyed close | 1589.2 | 0.63 | 🟡 见 A6-close | 67ns/evt（15.0M） |
| q18 composite-key close | **2554.9** | 0.39 | 🔴 复合键 vs 单键 +61%（A4 确认） | 315ns/evt（3.2M） |
| q19 stats rows top10 | 202.5 | 4.94 | ✅ | 218ns/evt（4.6M） |
| q19 stats batch top10 | **89.4** | 11.19 | ✅ 列式 2.3× 行式 | — |
| q20 each+join+where | **1320.3** | 0.76 | 🟡 join 富化 + where | 141ns/evt（7.1M） |
| q20 each+join+where 列式（F6） | **183.7** | 5.44 | ✅ 行式 17.3%（**5.8×**，混跑 264ns=7.5×） | 33ns/evt（30M 9.24M） |
| q21 str bind filter | 723.3 | 1.38 | ✅ | 62ns/evt（16.0M） |
| q22 each+split | **2069.5** | 0.48 | 🔴 on-each 家族最高（A1 确认） | 130ns/evt（7.7M） |

### 判定修正（实测 vs 静态分析）

- **A2（Q13）确认**：`execute_match_with_joins`（join 富化 + `build_eval_context` +
  `build_match_alert`）1878ns = advance 696ns 的 **2.7×**——每事件 fire + 富化是
  Q13 2.2M EPS 的主因。F4（跳过 fire clone）已把 advance 从 ~985 压到 611（单跑）；
  剩余大头是 execute_joins 富化（~48%）+ build_eval_context（~35%）。
- **A1（Q22）确认**：2069ns 为 on-each 家族（q21 723 / q14 766）的 ~2.8×；split
  `Vec<Value>` 分配 + concat 拼接。**超出 §3 阈值（200ns）11 倍——值得重新评估
  （split buffer 复用 / mvindex 免 Value 包装），见 §7 遗留**。
- **A4（Q18）确认**：复合键 2555ns vs 单键 q17 1589ns（+61%，混跑波动）——复合键
  提取/实例管理成本。
- **A6（Q7）修正**：max measure 681.7ns vs count 571.8ns 仅 **+19%**，不是 30M 表
  1.2M vs 4.3M（3.6×）的根因——后者应归因于负载/conv 批大小，max 本身无缺陷。
- **A3（Q11）修正**：session 709ns vs fixed 697ns 同量级；RSS 17.3GB 为状态/积压
  问题而非每事件成本。
- **F4 效果（单跑对比，避开混跑争抢）**：q5 640→**370**（-42%）、q13 advance
  985→**611**（-38%）、q12 762→**592**（-22%）、q11 756→**615**（-19%）、
  q6 1350→**1177**（-13%）。
- **对照差异说明**：bench 单事件顺序调用 vs 生产列式批处理 + 多线程分片，EPS 差
  3~12×（q12 697 vs 55、q14 766 vs 49）——bench 用于**相对归因**，绝对值看生产。

## 6. 判定标准（供后续自动比对）

- **同形态基准**：on-each 家族（Q1/Q10/Q21 16~20M）vs Q22/Q14；match 家族内
  Q12（55ns/evt）作为「轻状态 count 窗口」下限参照。
- **理论下限**：distinct 每事件哈希不可回避 ≈ 87ns/evt（close_bench 实测）；
  字符串 split ≈ 每次 `Vec<String>` 分配 ~24 次小分配。
- **不合理判据**：ns/evt 高于同形态 2× 且无法由状态规模解释 → 标记性能缺陷，
  进入优化队列（A1~A7）。

## 7. 已实施的改进（2026-08-23，静态审查批次）

### ✅ F1 — conv `sort` O(n²) → O(n log n)（Q5/Q7 A6）

`match_engine/cep/conv.rs` `ConvOpPlan::Sort`：旧实现 `outputs.sort_by`
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

### 🔍 已审查、暂不改 / 已解决（需实测数据或改动面大）

- **Q22 split（A1）**：实测 2069ns 超出 §3 阈值 11 倍——**重新评估**：split 复用
  输出 buffer（Vec 预分配）、mvindex 直接索引 str 段避免 Value 包装、concat 预分配
  容量。此前判"无静态改进空间"与阈值矛盾，待专项做。
- ~~**Q13 RSS（A2）**~~ ✅ 已解决（2026-08-23 F5 死 join 消除 + F6 列式 join，
  22.9GB→7.1GB、EPS 1.7M→15.9M，见 §8）。
- **Q11/Q18 RSS（A3/A4）**：需运行确认是状态物化还是管道积压（q22 的 RSS
  已实证为积压），待逐项归因（见 §8.4）。
- **Q19 top-N RSS（A5）** / **Q4/Q6 join-then-key RSS（A7）**：确认但未优化。

## 8. RSS 归因实证（2026-08-23，q13 22GB 根因 + 修复）

### 8.1 归因结论（逐项证实）

1. **q13 30M RSS 22.9GB = bid 窗口持积压**，非 join 缓存/状态物化：
   - 实测 evictor Phase-1 扫掠时 bid 窗口 `rows=24.1M / bytes=5.5GB / floor=94`——
     驱逐被 ack floor 门控（`tb.seq < floor`），而 floor 只推进到 94（消费 1.7M/s
     落后推入 3M/s → 每轮 pull 只 ack 已读位置）→ 窗口持 [floor, newest] 全部积压；
   - 跑批结束（推入停止、消费追平）后 floor→775，窗口瞬间清到 3 批次——证实纯积压，
     无泄漏/常驻状态。
2. **生产从未调用 `set_join_key` → join 全量扫描**（前一轮诊断）已修复：spawn.rs 按
   join 首条件右字段接索引（seq-cut 感知，`JOIN_DIAG=1` 实测 900 万次 lookup 全部
   `idx_hits`、`scan_fb=0`）。但 q13 EPS 不变（1.7M）——**join 索引不是瓶颈**。
3. **真瓶颈 = CEP 行式路径**：sample 采样热帧 = `CepStateMachine::advance_at_with_diagnostics`
   + `AlertColumnBuilder::append_record` + `core::fmt::write` + `mi_heap_collect_ex`；
   rule profile：exec 41.5% + emit 48.8% = 90%。q13 的 `match<bidder:10m>
   { on event { b | count >= 1 } }` 每事件必匹配 → 每事件 advance + fire + 富化。
4. **`each_plan_columnar_safe()` 对带 join 的 each 规则返回 false**（each_exec.rs:423）——
   任何 each+join 规则退出列式快路径走行式（q13-each 2.29M / q20 2.55M vs q1 7.6M）。
5. **bid over 调小（5s/2m）破坏 q9**：q9 的 deferred 评估由 auction watermark（慢）
   驱动，bid 驱逐由 bid watermark（快）驱动，事件时间差 = 摄入积压滞后（30m 数据
   ~1700s）——over < 滞后时评估前 bids 已被驱逐（over=2m 时 q9 丢 73% 输出）。
   设计文档 D4 悬置项：deferred join 规则需在 join 窗口注册保留 pin（按自身
   watermark 推进 ack）；落地前 bid over 保持 1h。

### 8.2 修复（F5：死 join 消除，2026-08-23）

`RuleExecutor` 新增 `live_joins`（`compute_live_joins`）：输出表达式（lets/where/
score/entity/yield）**全为限定引用或字面量**时，Snapshot/Asof join 的富化字段无人
读取 → 死 join 消除（输出字节不变——Snapshot/Asof miss 保留事件、无过滤语义）。
Inner/Anti/within/reduce/emit_at 有输出/过滤语义 → 永不消除；任何 plain（未限定）
字段引用 → 保守保留全部 join。

q13.wfl 同步从 `match<bidder:10m>` 改写为 `on each b`（输出等价：每 bid 一条 alert，
27.6M @30m 不变）——`on each` + 死 join → 列式快路径。

**实测（30M，单连接 replay）**：

| 配置 | EPS | RSS | CPU |
|---|---|---|---|
| 旧：match + join 全表扫描 | 1.7M | 22.9GB | 867% |
| join 索引接上（无 seq 门禁） | 1.7M | 17.9GB | 829% |
| match→each（join 仍在） | 2.29M | 16.5GB | 794% |
| **each + 死 join 消除（F5）** | **15.9M** | **7.1GB** | **208%** |

q9（唯一 join bid 的 deferred 规则）10m 输出 557,204 = 基线一致 ✅（over 回 1h 后）。

### 8.3 列式 join 富化（F6，2026-08-23）

**背景**：q20 等 each+活 join 查询（2.5~2.9M/s）——join 富化被输出引用 → 死 join
消除不适用 → 走行式 each+join（每事件 `Event::clone()` + `enrich_join_row` 全字段
注入 + `find_matching_row` 复核）。

**实现**：`RuleExecutor.each_join_plan`（`parse_each_join_columnar`）——v1 形状：单
Snapshot join（单条件 flat 限定引用）+ `where` 为「右窗字段 <cmp> 字面量」合取 +
yield/entity 为字面量 / 左窗限定 / 右窗限定。新执行路径
`execute_each_direct_batch_columnar_join`：**批级去重 join_lookup**（hot key 一次查）
+ 列式右窗字段读（`JoinRow.field_value`，免 Event clone）+ 列式 where 求值
（`join_cmp` 复刻 `compare_values` 语义）。浮点左 key 保留 values_equal 复核
（f64→Int 截断假匹配）；批快照 miss 的行在行循环时点实时复查（与行式逐事件
同时机）。

**实测（30M 单连接，rate=3m）**：

| 配置 | EPS | RSS | CPU |
|---|---|---|---|
| 旧：each+join 行式 | 2.86M | 7.6GB | 736% |
| **each + 列式 join 富化（F6）** | **9.24M** | 7.9GB | 454% |

**微基准（`nexmark_hotpath_bench::q20_each_snapshot_join_where_columnar`，release，
N=500k，同进程行式/列式对拍 + 输出逐位断言）**：

| 路径 | ns/evt | M evt/s | 加速比 |
|---|---|---|---|
| 行式批路径（`execute_each_direct_batch`） | 1061 | 0.94 | 1× |
| **列式 join 富化（`execute_each_direct_batch_columnar_join`）** | **184** | **5.44** | **5.8×** |

（全量混跑下行式 1981ns、列式 264ns——7.5×；负载高时绝对 ns 上浮，加速比稳定。）
每跑一次都执行行式/列式 stats + 输出行逐位对拍断言——列式路径任何语义回归
都会在基准中报红。

输出正确性：rate=3m 下列式 3 次 = 行式 = **5,503,985 逐位一致** ✓（30m 数据）；
集成对拍测试（真实窗口+索引+RegistryLookup）锁定命中/miss/where 拒绝一致。

**语义说明（固有非确定性）**：数据生成允许 bid ±10 lead 引用**未来** auction——
处理 bid 时该 auction 若尚未 append 则 join miss（Flink 流 join 同语义）。EMIT 因此
依赖处理速率 vs ingest 的竞速：行式自身 rate=1m 时从 1.86M 掉到 1.79M（30m 数据
从 5.50M 掉到 5.26M 量级），列式（处理快）在同一速率下 miss 更多——非列式 bug，
是速率依赖的固有语义。rate=3m 标准口径下列式/行式逐位一致。

### 8.4 遗留

- **D4 deferred join 窗口 pin**：落地后 bid over 可调小，q13 类查询 RSS 再降 ~3GB。
- **match 形态 join 富化 emit 路径**（q3/q6）：advance 的 join-then-key 已批级化（§8.5）；
  match/close 输出时的 `execute_match_with_joins` 富化仍行式，待专项。
- **q11/q18/q19 的 RSS**：均为同类积压/状态结构问题，待逐项归因。
- **q22 split（A1）**：on-each 家族最高单事件成本，见 §7 遗留。
- **match advance 列式化（q6 剩余瓶颈）**：~~CEP 状态机本体（InstanceKey 构建 +
  HashMap 实例管理 + 逐事件 step 推进 + Matched 构造 ~790ns/evt）批级化需专用
  执行路径~~ **决策（2026-08-23）：Q6 不再投入**——官方 q6.sql 未落地（OVER
  WINDOW 不支持 retractions）、无 Flink 测试数据、VVR 未发布，无对拍锚点；
  stats 语义也不适用（行数 last-N vs 时间窗口、流式 vs 收口、join 键缺失三重
  不匹配）。Q6 保持 CEP 形态作为 join-then-key 形态代表，性能表 vs VVR 列为
  「无对标」，不参与对比。stats sliding + join 键留作引擎能力规划（有真实
  last-N 需求再做）。

### 8.5 批级 join-then-key（F7，2026-08-23）

**背景**：q4/q6 的 `match<category|seller:...>` 键来自 snapshot join 右窗字段（
join-then-key）——每事件 advance 都做一次 join 索引 lookup + `values_equal` 复核 +
右窗键字段物化（A7：占 q4 advance 路径 ~88.8%）。NEXMark bid 的 auction 引用高度
集中（50% 热点集中在最近 100 个）→ 一批 ~230 bid 去重后常 < 20 个唯一 auction。

**实现**：
- `precompute_join_then_keys`（新模块 `match_engine/cep/join_then_key.rs`，
  pub 导出）：对一批事件按驱动 key 去重，每唯一 key 一次 lookup；int 左 key 取桶首行
  （索引截断精确、复核恒真）；float 左 key 逐行复核（`1.5` 截断假匹配必须拒绝，与
  `find_matching_row` 一致）；任一环节 miss → `None`（跳过）。
- `advance_at_with_diagnostics` 新增 `key_override` 三态参数（`Some(Some(keys))` 用预
  解析 / `Some(None)` 预解析 miss / `None` 内部解析）；`advance_at_with_masks_key`
  对外暴露；`advance_at_with_masks`/`advance_at_with_progress` 保持旧行为。
- rule_task `process_batch` 行循环前批级预解析（`key_overrides`），非 debug 路径走
  `advance_at_with_masks_key`；debug 路径走内部解析（结果一致）。

**微基准（`nexmark_hotpath_bench::q4_q6_join_then_key_batch_precompute`，release，
N=500k，同批行式/批级对拍 + 前 10k 行 StepResult 逐位断言）**：

| 路径 | ns/evt | M evt/s | 加速比 |
|---|---|---|---|
| q4 fixed10m 行式（内部解析） | 477.0 | 2.10 | 1× |
| q4 fixed10m 批级预解析 | **327.8** | **3.05** | **1.46×** |
| q6 sliding10m 行式（内部解析） | 892.2 | 1.12 | 1× |
| q6 sliding10m 批级预解析 | **743.9** | **1.34** | **1.20×** |

**30M 实测（单连接 replay，A/B 同负载）**：q4 2.66M → **3.09M**（+16%，CPU 94% 仍
单核）；q3（非 join-then-key 对照）15.8M 无回归。q6 实测 0.51M 与改动无关（A/B
stash 后同负载 0.50M）——q6 瓶颈是 26M EMIT 的每事件 emit 路径（score/entity/
yield + join 富化），非 advance。

**正确性**：`rule_task_key_join_tests`（3 用例：int 热点重复/miss、null+miss、float
截断复核）逐行对拍两条路径 StepResult 序列一致；全量回归 2565 tests 绿。

### 8.6 match 每事件 emit 路径分配削减（F8，2026-08-23）

**背景**：q6 26M EMIT 的单核瓶颈（CPU ~106%，join-then-key 无法分片）中，
`execute_match_with_joins` 与 advance 对半（4s sample：1089 vs 1076 叶子采样）。
sample「Total number in stack」显示**分配器占压倒性比例**（mi_free/mi_malloc/
arena ~40%）——q6 每事件 ~10 次分配，26M 事件 ≈ 2.6 亿次分配/释放。

**实现**（确定性分配削减，逐项微基准验证）：
- yield 字段 Vec 预分配（`Vec::with_capacity(fields.len())` 替换
  `from_iter` 渐进扩容——sample 热点 spec_from_iter；match_exec/close_exec 两处）；
- `build_eval_context` 预容量 HashMap（`with_capacity_and_hasher`——hashbrown
  fallible_with_capacity 采样热点）；
- `OutputRecord.machine_id` String → Arc<str>，`build_machine_id` 空 id 直接
  Arc 复用 rule 名（sample 热点 String::clone——q6 每事件 22 字符堆分配）；
- `build_scope_key` 单 String 一次写入（旧实现每 key format! + Vec + join
  → 每事件 2 次分配 + Vec 分配），返回 Arc<str>；
- `build_wfx_id` scope_key 直接 hash Value 规范字节（Number → f64 LE bits，
  免 value_to_string 渲染 + String 分配；wfx_id 无字节级锚定，测试仅断言
  16 hex 格式与同输入稳定性）。

**微基准（`nexmark_hotpath_bench::q6_match_emit`，N=500k，q6 形状：
live_joins 空 + score 常量 + yield 读左窗）**：

| 分量 | 优化前 | 优化后 | 变化 |
|---|---|---|---|
| q6 match emit 全路径 | 1294.7 | ~1055 | **-18%** |
| q6 build_match_alert | 693.9 | **502.8** | **-27.5%** |
| q6 build ctx（窄化） | 202.6 | ~200 | 持平 |

q13 match+join emit（同路径）顺带 -12%。

**30M 实测**：q6 0.50M → **0.55M**（EPS 492k→578k 区间，load 6-8 波动 ±10%）；
全量回归 2565 tests 绿。剩余瓶颈：ctx HashMap 构建（~200ns）+ advance sliding
（~800ns）+ 单核串行（join-then-key 无法分片，v1 CONNECTIONS=1 结论）。

### 8.7 match emit ctx-free 快路径（F8.5，2026-08-23）

q6 的 ctx 只含 seller（scope_key）+ auction（trigger_event 字段）——
`build_eval_context` 每事件构建 2 字段 HashMap（~200ns/事件）。输出字段直读
scope_key + trigger_event 即可，免构建：
- 编译期 gate `compute_match_ctx_free`（OutputStatic.match_ctx_free）：score
  常量 + entity/yield 全 Field/Lit（无 General）+ 无 where + live_joins 空 +
  输出字段不命中 step label/tracked/_step_/_bind_；
- 运行时 `ResolveCtx` 抽象（Full / Free 两模式），`build_match_alert` 重构为
  inner；`execute_match_at` / `execute_match_with_joins_at` gate 通过时直接走
  free（execute_joins/where_ok 空转整体跳过）；
- 对拍测试：Full vs free 逐字段字节一致 + gate 反向（where / General 禁用）。

微基准：q6 emit 全路径 **1055→609ns（-42%）**；q13 同形状顺带受益。

### 8.8 collected_values 收集 gate（F9，2026-08-23）

`update_measure` 无条件 push collected_values（L3 序列函数经 ctx `_step_{i}_values`
读取），q6 等单 bind avg/count 规则无人消费——每事件 VecDeque push + Matched 时
StepData/MatchedContext 的 collected clone 纯浪费。收集移到调用方并 gate =
`plan.needs_field_history`（编译器 L3/close 非键/join/多 bind 置 true）。
微基准：q6 advance 797→791；q16/q17/q18 close 路径顺带受益。

**q6 当前构成（30M 实测 1822ns/evt）**：advance ~790 + emit（ctx-free）~650 +
rule_task 框架/scan/emit_batch ~380。剩余瓶颈 = CEP 状态机本体（InstanceKey
构建 + HashMap 实例管理 + 逐事件 step 推进 + Matched 构造）——批级化需专用
执行路径（见 §9 遗留），或 q6 类规则改走 stats 执行器列式聚合（结构性决策）。
