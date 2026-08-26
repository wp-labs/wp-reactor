# Q4（双规则链）EPS 回归定位与优化 — 归档

## 一句话结论

Q4（q4a deferred → 中间窗 auction_finals → q4b stats）30M EPS 从 7.66M 掉到 4.25M（回归），经**组件级 bench 穷举**定位后四连优化回到 **5.76M**（+35%）。剩余 vs 单规则 Q9（9.9M）的 0.58× 差距为**双规则链架构成本**（流程翻倍），已达该架构下合理水平，不再追。

## 1. 问题与归因路径

### 1.1 现象（2026-08-26）

| 时间点 | q4 30M EPS | RSS | 备注 |
|---|---|---|---|
| 08-24 20:50 | 7.66M | 8.9G | 优化后基线 |
| 08-26 11:15 | 4.21M | 5.7G | **-45% 回归** |
| 08-26 11:23 复测 | 4.25M | 5.8G | 稳定回归，非噪声 |

CPU 147%avg（单核为主）→ 指向规则求值路径串行化/变慢。

### 1.2 归因方法：组件级 bench 穷举（先数据后动手）

回归窗口内的代码改动只有 q13 主线（常量列折叠）+ merge。按组件逐个量化排除：

| 组件 | 工具 | 结果 | 判定 |
|---|---|---|---|
| join-then-key 微基准 | `nexmark_hotpath_bench::q4_q6_*` | 行式 417 vs 历史 477 ns/evt（更快） | ❌ 非回归（且测的是旧语义） |
| deferred 评估 | `deferred_bench` | eval-cand8 1726ns，eval-exists 1338ns | ❌ q9 同款成本，非 q4 独有 |
| **中间窗 staging** | 新 `q4a_stage_bench` | row 569.6 vs col 130.7 ns/row | ✅ 4.4× 可优化 |
| **中间窗轻量 build** | `q4a_deferred_eval_candidate_scan` | 全量 1338 vs 轻量 1273 ns/op | ✅ 1.4×（收益小于预期） |
| **条件复核** | 分解 bench | 每候选 ~17-21ns | ✅ 可跳过（asof 契约） |
| **enrich 注入** | 新 `q4a_eval_cost_decomposition` | 全量 679 vs bare 222 ns/op | ✅ 3.1× 主墙 |
| q4b stats 更新 | 新 `q4b_stats_group_avg` | 行式 65.8 / 列式 34.5 ns/evt | ❌ 非瓶颈（1.67M 行仅 0.11s） |

**关键教训**：三个"想当然"都被数据推翻——join-then-key 无回归（测错语义）、q4b stats 不是瓶颈、轻量 build 收益远小于预期（build 已被 q13b 磨薄）。**只有分解 bench 才找到真主墙（enrich）**。

## 2. 四连优化（全部 bench 数据驱动）

| # | 优化 | 提交 | bench 数据 | 30M 贡献 |
|---|---|---|---|---|
| 1 | staging 列式化 `push_record_columnar` | `e9dd351` | 569.6 → 130.7 ns/row（4.4×） | 4.25 → 4.66M |
| 2 | 中间窗轻量 build `build_each_alert_pipe` | `e9dd351` | 1338 → 1273 ns/op（1.4×） | → 4.77M |
| 3 | 条件复核冗余跳过 `cond_recheck_redundant` | `e9dd351` | ~17ns/候选 | → 5.15M |
| 4 | enrich 裸名注入 `enrich_join_row_bare` | `5d26fc9` | 679 → 222 ns/op（3.1×） | → **5.76M** |

### 2.1 staging 列式化

`stage_pipe_record` 原走 `push_record`（row path）：`record_window_fields` 每行
clone yield_fields + HashSet + meta 名 Arc::from（4 次分配）。新 `push_record_columnar`
复用 `new_columnar` 的 col_sources 计划，meta 值 SmolStr 内联，免全部中间分配。
生产路径（deferred emit / 行式 on-each 中间窗）全切列式；`push_record` 退化为
测试专用（`#[cfg(test)]`，对拍用）。

### 2.2 中间窗轻量 build

`execute_deferred_join` 拆 `evaluate_deferred_join`（返回 out_ctx）+ build 两步。
`scan_deferred` 对 intermediate 目标且 yield 不引用 `__wfu_*` meta
（`pipe_light_build_ready`，复用 q13a 的 `each_yield_meta_light` 空槽不可观测性）
走 `build_each_alert_pipe`：跳过 wfx_id 哈希+hex / fired_at ISO8601 / machine_id。
实测只省 65ns——**q13b 已把 alert 构建磨薄，收益小于预期**，但机制通用且语义安全。

### 2.3 条件复核冗余跳过

`asof_candidates` 契约（`types.rs` 文档）保证候选行的 key 字段 == pending.key；
`pending.key` 来自 `first_join_key_local`（ctx[cond.left]），`pending.key_field` 即
cond.right 字段名。故**单条件且右字段 == key_field 时 `row_matches_conds` 恒真**，
跳过省每候选一次字段查找+比较。多条件（key 只来自第一个 cond）保留复核。
⚠ 测试 FakeLookup 曾违反契约（返回全候选）——已按契约修复（deferred_join /
coverage_extra），这是**契约驱动测试修复**的实例。

### 2.4 enrich 裸名注入（主墙，3.1×）

`q4a_eval_cost_decomposition` 分解（cand4，合计 ≈1216ns 与实测 1256ns 吻合）：

| 段 | ns/op | 占比 |
|---|---|---|
| asof_candidates | 21 | 1.7% |
| filter（in_interval×4） | 3 | 0.3% |
| recheck（条件复核×4） | 84 | 6.8%（已跳过） |
| reduce（select_reduce_row×4） | 116 | 9.4% |
| **enrich_join_row** | **679** | **55.7%** ← 主墙 |
| build（轻量） | 320 | 26.1% |

主墙成因：`enrich_join_row` 每实例 `format!("{右窗}.{字段}")`×N 字段（String
分配）+ qualified/bare 双键插入 + 值 clone×2。**实证** `eval_field_value` 对
`Qualified(_, name)` 读裸名键（`fields.get(field_ref_name)`）、Path 引用丢弃
alias——qualified 键对表达式求值**不可达**（纯死数据）。`enrich_join_row_bare`
只做裸名 or_insert（不覆盖已有同名字段，q4a 的 dateTime 保留 auction 侧），
省 457ns/实例。eager 路径（execute_joins）保留全量注入（行为契约测试锁定）。

## 3. 结论：双规则链下界

q4 是 q9 的两段工作（评估 + 中间窗 relay + stats 二次消费），0.58× 接近
"流程翻倍"的理论下界。剩余差距拆解全是结构性成本：

| 来源 | 说明 |
|---|---|
| 任务串行依赖 | q4b 进度被 q4a 产出节奏卡住（攒批→广播→接收→处理管线延迟） |
| 中间窗双写 | 产出 append 进 auction_finals 窗口（over=1h 保留+驱逐）+ 广播 |
| 事件二次物化 | q4b 把 1.67M 行 batch 再读一遍（窗口读 + 事件构建） |
| watermark 传导链 | q4a 等 bid_events 前沿 → 产出 → q4b 等 auction_finals 完整性 |

**判定**：双规则链的固有成本，非局部可修热点，q4 已达合理水平。

## 4. 可复用机制（已入 REUSE_GUIDE §7）

- `push_record_columnar`（列式 staging，★4）
- `build_each_alert_pipe`（中间窗轻量 build，★4）
- `cond_recheck_redundant`（asof 契约复核跳过，★4）
- `enrich_join_row_bare`（qualified 死数据消除，★4）
- `q4a_eval_cost_decomposition`（组件分解 bench 模式，★4）

## 5. 未决 / 下一站

- **q6：30M 614K EPS（单核）**——其他 Q 最低 4M+，非架构差异可解释，疑似
  "26M EMIT 每事件 emit 路径"（早期文档），值得独立专项
- 100M 全量批内存遗留：q16（22.9G）、q18（24.2G）、q14（18.0G）未专项

## 6. Pitfalls（勿重蹈）

1. **别用错语义的 bench**：q4 的 join-then-key 微基准测的是旧语义（双规则链
   落地前），新语义必须新 bench——先用代码确认 bench 测的是当前实现
2. **契约 vs 测试实现**：优化依赖 WindowLookup 契约时，测试 FakeLookup 违反
   契约会静默破坏语义——跳过复核类优化必须同步修测试 lookup 按契约过滤
3. **"想当然"的瓶颈大多是错的**：q4b stats 看起来像瓶颈（其实 66ns/evt）、
   轻量 build 看起来收益大（其实 build 已被磨薄）——分解 bench 是唯一可靠手段
