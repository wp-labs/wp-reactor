# 列式执行改造 · 进展日志

> 关联设计：`columnar-execution-design.md`
> 工作分支：`feat/columnar-execution`
> 原则：小步开发、快速验证、多写测试、逐层验收（数据驱动）。

## 进度总览

| 里程碑 | 状态 | 验收 |
|---|---|---|
| M1 guard 基准 | ✅ 已就位 | `guard_bench` 基线（commit `6e0a850`） |
| M2a 重测 Q2 基线 + 覆盖率统计 | ✅ 完成 | guard 覆盖率 **85.3%**；Q2 基线（2GB content）**~6.8M（双峰 6.2~7.2M）**，详见下 |
| M2b L1 guard 列式化 | ✅ 完成 | 静态门 + 预编译列式求值器（f64 + 原生 Int64）；bind filter + 事件/close/seq branch guard 全部列式化（close 三值 permissive、event/neg 两值 must-be-true）；列式 guard **14.3ns/事件**（interpreted 216.9ns），<20ns 达标；≤2^53 对拍 100% 一致；**端到端 Q2 EPS 无增益（两道墙限，见 Step 8）** |
| M3 L2 物化延迟 | ✅ 端到端懒物化已落地 | `route_parse` 广播原始批 → `RuleTask` 全行扫时间列（watermark/过期）+ 只物化 bind-filter 命中行 → 只推进命中行 |
| M4 L3 输出列式 | ⬜ 未开始 | — |
| M5 L4/L5 | ⬜ 未开始 | — |

## 本次提交内容

### Step 1 — 静态判定门 `expr_is_columnar`（wf-lang）

- 文件：`crates/wf-lang/src/columnar.rs`（新模块，`pub mod columnar`）
- 纯 AST 分析，编译期一次性判定，运行期零分支：
  - **列式**：字面量（Number/StringLit/Bool）、扁平字段（Simple/Qualified/Bracketed）、
    `Neg`、全部算术/比较/逻辑二元算子（And/Or/Eq/Ne/Lt/Gt/Le/Ge/Add/Sub/Mul/Div/Mod）
  - **回退**：`FieldRef::Path`（嵌套路径）、`FuncCall`、`Object`/`Array`、
    `InList`、`IfThenElse`、`SystemVar`/`WfuMeta`/`PresetParam`
- 测试：12 个单测（字面量/扁平字段/嵌套路径/比较算术/短路/取反/函数/结构体/
  in/if/系统变量/混合表达式/递归嵌套）
- 验证：`cargo test -p wf-lang --lib` → 524 全绿

### Step 2 — 列式 guard 求值器（wf-engine）

- 文件：`crates/wf-engine/src/match_engine/columnar.rs`（新模块，`pub mod columnar`）
- 组件：
  - `ColumnarBatch<'a>`：批级视图，`projection: Vec<usize>` + `field_map`（字段名归一化
    Simple/Qualified/Bracketed → 同一投影索引），零拷贝；
  - `eval_guard_columnar(expr, view) -> BooleanArray`：逐行产出命中掩码；
  - 内部 `CScalar`（Num/Str/Bool）+ 递归求值器，直接读原生列（无 HashMap/Value 转换）。
- 语义：**先正确**——严格镜像 interpreted 的 f64 数值语义：
  - null / 缺字段 → `None` → 不命中（等价于 `batch_to_events` 跳过 null）
  - `==`/`!=` 用 `f64::EPSILON`（镜像 `compare_cmp`）
  - `&&`/`||` 用 SQL 三值逻辑（镜像 `eval_logic_and/or`）
  - 算术/比较全 f64（镜像 `eval_arithmetic` / `compare_values`）
- **关键决策（记录在案）**：本步先做 **f64-exact**，使对拍在**任意输入**（含 `>2^53`
  整数、纳秒时间戳）上 100% 一致，比设计稿 §3.4 的「≤2^53 一致」更强。设计稿的
  **原生 Int64 取模/比较（"更准"）** 作为下一步在 f64 基线上叠加，届时才引入并记录
  `>2^53` 的已知语义差异。
- 测试：5 个对拍单测（Q2 guard、比较/算术/逻辑/取反混合、缺字段、浮点 epsilon 相等、
  非布尔顶层表达式），逐行断言 `eval_guard_columnar` == interpreted `eval_expr_ext`。
- 验证：`cargo test -p wf-engine --lib columnar` → 5 全绿；`wf-engine --lib` → 468 全绿（2 忽略为 guard_bench）

### Step 3 — 原生 Int64 取模/比较（§3.4 "更准" 语义）

- 在 Step 2 的 f64 基线上叠加原生 `i64` 分派：
  - `CScalar` 增加 `Int(i64)`：`Int64` 列、`Timestamp(Ns)` 列读为原生 `i64`；
    整数型 `Number` 字面量（`fract()==0 && |n|<2^53`）读为 `Int`；
  - `%` 与比较（`== != < > <= >=`）在**两操作数均为 `Int`** 时走原生 `i64` 运算；
  - `+ - * /` 以及任何 `i64`/`f64` 混算仍走 f64（与 interpreted 完全一致）；
  - `==`/`!=` 在浮点路径保留 epsilon；原生 `i64` 路径用精确相等。
- **语义差异（已锁定）**：原生 `i64` 仅在 `>2^53` 整数 / 纳秒时间戳上与 interpreted f64
  分叉（如 `2^53 == 2^53+1`：列式 false、interpreted true）。这是设计稿 §3.4 记录的
  "更准" 行为，不是回归。
- 测试：新增 2 个（`≤2^53` 边界 100% 一致 + `>2^53` 分叉锁定），共 7 个 columnar 单测。
- 验证：`cargo test -p wf-engine --lib columnar` → 7 全绿；`wf-engine --lib` → 470 全绿（2 忽略）。

### Step 4 — 预编译列计划 + 接线（§3.2/§3.3）

- 求值器从「逐行递归 + HashMap 定位列」升级为**预编译列计划**：
  - `ColRef`（`Int64/Float64/Utf8/Bool/TimestampNs/Null`）：字段在编译期一次性解析到
    类型化列引用（或 `Null`=缺字段/不支持类型，与 `extract_value` 的 None 等价）；
  - `ColumnExpr` 树：`Lit/Col/Neg/And/Or/Cmp/Arith`，字段已解析，逐行热循环**零 HashMap**
    查找、零逐行 downcast；
  - `eval_guard_columnar` = `compile_expr`（一次）→ 逐行 `eval_cx`。
- 接线：`RuleExecutor` 新增两个批级列式过滤入口（`executor/mod.rs`）：
  - `bind_filter_columnar_mask(alias, batch) -> Option<BooleanArray>`
  - `each_filter_columnar_mask(batch) -> Option<BooleanArray>`
  - 二者在 filter 为列式时返回掩码，否则返回 `None`（回退逐行 interpreted）。
- 测试：新增 `columnar_wiring.rs` 4 个接线对拍（bind/each 列式掩码 == 逐事件、
  非列式/无 filter 返回 None）；`guard_bench` 增加列式 vs interpreted 基准。
- **基准（release，1M 行）**：

  ```text
  columnar_batch(auction%123==0):   14.3 ns/event  (70.1M ev/s)
  interpreted_per_event:           216.9 ns/event  ( 4.6M ev/s)
  ```

  列式 guard **14.3ns/事件**，较 interpreted 降 ~15x，**<20ns 达标**（M2b 验收项）。

### Step 5 — 运行时循环接线（§3.3 push 路径）

- 把原始 `RecordBatch` 通过 push 路径透传到 `RuleTask`，用列式掩码替换逐行 bind-filter：
  - `RulePush` 新增 `batch: Option<Arc<RecordBatch>>`（`fanout.rs`）；
  - `RuleFanout::broadcast_with_batch` 转发原始批（Single/RoundRobin 订阅；Sharded 分区后行索引
    不再对齐全批，暂传 `None` 回退 interpreted）；
  - `commit_appended_batch` 克隆原始批并广播（`commit.rs`，`RecordBatch` clone 仅 Arc 自增）；
  - `RuleTask::process_batch` 新增 `batch: Option<&RecordBatch>`，逐 alias 预计算
    `bind_filter_columnar_mask`，`alias_accepts` 命中掩码时用 `mask.value(row)`，否则回退
    `event_matches_alias`（`rule_task.rs`）。
- 覆盖范围：**bind filter**（§3.3 伪代码的 `self.guard`，即 `events { ... where ... }`）在
  match 与 `on each` 两路径都接入；push 路径为主，pull 路径暂传 `None`。
- **重要边界（记录在案）**：qradar 规则集的 guard 是 **branch guard**（`on event { c && <guard> | count >= N }`），
  在状态机 `step.rs`/`close.rs`/`seq.rs` 内逐事件求值，**不在本轮接线范围内**（需把批级 guard mask 传进
  `advance_at_with`，更接近 L4/L5）。本轮 bind filter 列式化对 Q2 形态（`events { b && ... }`）直接生效，
  对 qradar 的 branch guard 还需单独接线。
- 测试：新增 `columnar_bind_filter_matches_interpreted_path`（wf-runtime）——同一批数据分别用
  `batch: Some`（列式）与 `batch: None`（interpreted）push，断言输出逐位一致。
- 验证：`cargo test -p wf-runtime --lib` → 161 全绿；`cargo check --workspace --all-targets` → 通过。

### Step 6 — branch guard 列式化（qradar 真身）

- 把批级 branch-guard 掩码传进状态机：
  - `columnar.rs` 新增 `GuardMasks`（按 `(step_index, branch_index)` 索引的 `BooleanArray`）；
  - `RuleExecutor::branch_guard_masks(batch)` 预计算事件步骤的列式 branch guard；
  - `CepStateMachine::advance_at_with_masks(..., row, masks)` 新增入口；`advance_at_with`/`with_progress`
    委托 `masks=None`（向后兼容）；`advance_at_with_diagnostics` 穿透 `row`+`masks`；
  - `step.rs`：`StepEvaluationInput` 增加 `step_index`/`row`/`masks`，guard 求值优先用
    `masks.value(step, branch, row)`，未命中回退 `eval_expr_ext`。
- 覆盖范围：**事件步骤 branch guard**（`on event { c && <guard> | count >= N }`，qradar 主形态）。
  close/seq 的 branch guard 仍逐事件 interpreted（未在 qradar 热路径）。
- 测试：新增 `columnar_branch_guard_matches_interpreted_path`（wf-runtime）——同一批数据 `batch: Some`
  与 `batch: None` 输出逐位一致，命中 `sip == "10.0.0.1"` 且 count>=3 只触发一次。
- 验证：`cargo test -p wf-runtime --lib` → 162 全绿；`cargo test -p wf-engine --lib` → 474 全绿。

### Step 7 — L2 物化延迟原语（mask → indices → 按需物化）

- 新增 L2 原语（`event_bridge.rs` / `columnar.rs`）：
  - `materialize_rows(batch, indices)` / `materialize_rows_filtered(...)`：只物化命中行；
  - `mask_to_indices(mask) -> Vec<u32>`：`Mask → Indices` 的有序收集（§3.1）。
- 测试：新增 `mask_to_indices_and_materialize_rows_match_batch_to_events`（wf-engine），
  验证命中行物化结果 == 整批 `batch_to_events` 对应行；新增 `deferred_materialization_throughput`
  （0.82% 命中时只物化 82/10000 行）。
- **边界（记录在案）**：这是 L2 的**原语**，尚未把上游 `route_parse` 改为懒物化——当前 push 路径仍
  在 `route_parse` 整批 `batch_to_events`（一次、共享），`RuleTask` 拿到的是已物化事件。真正的端到端
  懒物化需改 `route_parse`/`RulePush` 让事件按需物化（见待办 3）。
- 基准（release）：整批 `batch_to_events` ~11.9M ev/s（物化全部 10000 行/批）；
  `materialize_rows` 0.82% 命中 ~10M ev/s（只物化 82 行/批）——命中率越低省得越多。
- 验证：`cargo test -p wf-engine --lib` → 476 全绿。

### Step 8 — 端到端 Q2 A/B（列式 vs interpreted）

- 方法：重编 `wfusion`（含列式改动，`warp-fusion/target/release/wfusion`）vs PATH 里的旧
  interpreted 二进制，`./bench.sh q2 cont 100m`（2GB content）交错跑、按相位配对。
- 结果（EPS，均 `[clean]`、EMIT=747816）：

  | 相位 | interpreted（OLD） | 列式（NEW） | Δ |
  |---|---|---|---|
  | 低相 | 6,172,721 / 6,166,338 | 6,180,622 | ≈ +0.2% |
  | 暖相 | 6,644,820 | 6,699,820 / 6,682,188 | ≈ +0.6~0.9% |

- **结论（数据驱动，不夸大）**：端到端 Q2 EPS **无增益**（Δ ≈ 0，淹没在 ±8% 双峰噪声里）。
  这与设计稿 §5.2 的「两道墙」模型一致——Q2 的 EPS 门是**窗口 actor 单写者（P0-③）**，
  不是规则侧 guard；guard 列式化（217→14.3ns/事件）只降低每事件成本，不推倒第二道墙，
  故 EPS 不随 guard 提速翻倍。**guard 的收益在 per-event ns（micro 基准），不在 EPS**。
- **对 L1 的定性**：M2b 验收（guard <20ns + 对拍 100%）已达成；端到端 EPS 不承诺（§8 明说
  「不承诺固定 EPS 倍数」）。列式的真实价值是「把 CPU 饱和拐点右移，为 P0-③ 破墙预留 CPU 余量」。

### Step 9 — L2 端到端懒物化接线（分析 → 窗口 → route_parse → 规则任务）

- 按既有 `field_usage` 的「规则分析 → 窗口参数 → route_parse」先例，把「延迟物化」接到底：
  - `wf-lang`：`WindowFieldUsage` 新增 `defer_materialization`（窗口所有 bound 规则的 bind filter
    均为列式时该窗口可延迟）；
  - `wf-engine`：`WindowParams.defer_materialization` + `Window.defer_materialization()`；
    `route_parse` 命中时跳过整批物化（`events=None`）；`RulePush.events` 改为 `Option`，
    `RuleFanout::broadcast_batch_only`（只广播原始批，排除 sharded 窗口）；
    `commit_appended_batch` 在 `events=None` 且有订阅时走 `broadcast_batch_only`；
  - `wf-runtime`：`schema_bridge` 写入 `defer_materialization`；`RuleTask::process_batch` 在
    `events=None` 时从原始批重建。
- 测试：`wf-lang` 新增 `defer_materialization_only_when_every_bind_is_columnar`；
  `wf-runtime` 新增 `deferred_materialization_matches_eager_path`（`events=None`+`batch` vs
  `events=Some` 输出逐位一致）。
- **边界（记录在案）**：`RuleTask` 当前在 `events=None` 时**仍整批 `batch_to_events`**（正确、
  只是把物化点从 `route_parse` 移到规则任务，**尚未省 CPU/RSS**）。真正的省发生在「只物化命中行」
  ——需要把 `process_batch` 的扫描（读时间列，遍历全行）与状态机推进（只命中行）分离，且保持
  「扫描/推进交错」语义（短窗口会同一批内过期，不可简单分相）。这是下一步。
- 验证：`cargo test -p wf-lang --lib` → 525 全绿；`wf-engine` → 476 全绿；`wf-runtime` → 163 全绿。

### Step 10 — L2 逐行懒物化（真正的省）

- 把 `RuleTask::process_batch` 从「整批 `batch_to_events`」改为「全行扫时间列 + 只物化命中行」：
  - `wf-engine`：`CepStateMachine::time_field()` 公开访问器；
    `event_bridge::batch_time_col_index` + `batch_event_time_nanos_at`（列索引只解析一次，
    逐行直读）——从时间列读事件时间，**保持与 eager `extract_event_time` 完全一致的
    f64 往返**（Int64/Timestamp 先 `as f64` 再 `as i64`），null/缺字段/非数值 → 0；
  - `wf-runtime`：新增 `DeferredRows { times, hit_indices, hit_events }`；
    `process_batch` 在 `events=None && batch=Some && machine=Some && !debug` 时走懒物化：
    逐 alias 的列式 bind mask 取 OR 得到命中行 → `materialize_rows` 只物化命中行；
    循环改为按 **行号** 遍历，每行都 `scan_expired_at`（用 `times[row]`），只有命中行才
    `advance_at_with_masks`（用预物化 Event）。
- **语义保证（关键）**：`scan`（watermark/过期）仍对**每一行**执行（含被 bind filter
  拒绝的行），`advance`（状态机步进）只对命中行执行，且两者在**同一行内按先 scan 后
  advance 的顺序交错**——短窗口在同一批内过期（`T=0` 建实例 → 拒绝行 `T=400s` 的 scan
  先过期 → 后续命中行从 count=1 重来）不破坏。
- **边界（记录在案）**：懒物化只在 `!debug_enabled` 时启用——debug 详情日志需要被拒绝
  行的 Event 来渲染 `event_debug_ref`，所以 debug 开时回退整批物化（正确性优先，debug
  不是热路径）。`on each` 路径（`machine=None`）保持 eager（defer 分析本就不覆盖 each
  规则；each 已有 C2 批路径）。
- 测试：新增 `deferred_materialization_scans_every_row_for_intra_batch_expiry`
  （wf-runtime）——构造逐行时间戳（0/100s/400s/400s）+ 列式 bind filter，验证
  「拒绝行的 scan 仍触发 300s 窗口过期，最终不 fire count>=3」，deferred 与 eager 输出
  逐位一致。既有 `deferred_materialization_matches_eager_path` 现在真正走懒物化路径。
  `wf-engine` 新增 `test_batch_event_time_nanos_matches_extract_event_time_roundtrip`，
  锁定 Int64/Timestamp 的 f64 往返（含 `2^53+1`）与 null/缺字段 → 0 语义。
- 验证：`cargo test -p wf-engine --lib` → 477 全绿；`wf-runtime --lib` → 164 全绿；
  `cargo check --workspace --all-targets` → 通过。
- **review 修复（2026-08-18）**：懒物化路径原先把非命中行 `continue` 到 close-emission
  块之前，导致被 bind filter 拒绝的行的 `scan_expired_at_with_conv` 产生的 close 被
  **丢弃**（eager 路径会对每行都 emit close）。已改为把状态机 advance 包在
  `if let Some(event)` 内、close-emission 块对每行都执行；新增回归测试
  `deferred_materialization_preserves_close_emission_for_rejected_rows`。
  **已知限制（非正确性）**：懒物化用 `materialize_rows`（全 schema），未套用窗口的
  `materialize_fields` 投影——命中行会物化更多字段（仅 RSS/CPU，规则不读这些字段）。
- **端到端 Q2 复测（`bench.sh q2 cont 100m`，重编 `wfusion`，2026-08-18）**：

  ```text
  6,669,322  /  6,697,347   （均 clean，EMIT q2_mod_123 = 747816 精确一致）
  ```

  两次均落在**暖相**（基线暖相 ~6.64-6.70M），与 Step 8 的 NEW 暖相
  （6,682,188 / 6,699,820）一致——**EPS 无回归、也无增益**（符合 §5.2「两道墙」：
  L2 降的是 per-event 物化成本，Q2 EPS 门仍是窗口 actor 单写者）。RSS_peak ~7.4GB，
  与基线相当（懒物化只省 CPU/RSS 的物化侧，Q2 窗口本就按 `field_usage` 投影子集）。

### Step 11 — branch guard close/seq 收尾

- 把列式 branch-guard 从事件步骤扩展到 close 步骤与 seq 否定步骤：
  - `columnar.rs`：`GuardMasks` 拆成 `event`/`close`/`neg` 三张 mask 表（`insert_event`/
    `insert_close`/`insert_neg` + `event_value`/`close_value`/`neg_value`）；
    `eval_guard_columnar` 改为 **保留 null**（`None` 结果 → null slot，不再坍缩成 `false`）——
    两值消费者（`value()`）读 null 仍得 `false`，向后兼容；三值消费者可用 `is_null()` 区分
    「显式 false」与「字段缺失」。
  - `executor/mod.rs`：`branch_guard_masks` 额外计算 close-step 与 seq-negation 掩码
    （`neg_idx` 与 `SeqRuntime::build` 的 negation-only 顺序一致）。
  - `close.rs`：`accumulate_close_steps` 接收 `row`+`masks`，用 `close_value`（三值）实现
    **permissive** 语义——只有显式 `false` 阻断，null/缺失放行（镜像 interpreted 的
    `Some(Bool(false))` 才阻断）。
  - `seq.rs`：`scan_negations` 接收 `row`+`masks`，用 `neg_value`（两值）实现
    **must-be-true** 语义（null → false），与事件步骤一致。
  - `step.rs`：`evaluate_step_with_progress` 改用 `event_value`（等价改名）。
  - `match_engine/mod.rs`：`advance_at_with_diagnostics` 把 `row`+`masks` 穿透到
    `scan_negations` / `accumulate_close_steps`。
- **语义差异（记录在案）**：close-step guard 是 permissive（只显式 false 阻断），事件/neg
  guard 是 must-be-true（null 阻断）——因此 close 用三值 mask、事件/neg 用两值 mask。
  `close_reason` 类的 close guard（合成事件字段，不在批里）不可列式，仍走 interpreted
  （`expr_is_columnar` 对缺失字段照常判列式，但 close-time 合成事件本就不走 batch 路径）。
- 测试：新增 `columnar_close_seq.rs` 3 个（wf-engine）——mask 结构（event/close/neg 三表 +
  null 感知）、close-step permissive 对拍、negation must-be-true 对拍；均逐事件
  `advance_at_with_masks`（列式）== `advance_at_with`（interpreted）。
- 验证：`cargo test -p wf-engine --lib` → 480 全绿；`wf-runtime --lib` → 164 全绿；
  `cargo check --workspace --all-targets` → 通过。

### Step 12 — Q1 `on each` 懒物化（defer，广播原始批）

- **问题**：Q1（`on each`，无 bind filter）仍 eager 物化——`route_parse` 对每个窗口调用
  `batch_to_events_filtered`（每事件一个 `HashMap`，~300B/行）→ 窗口 mailbox 按
  `content_bytes + events_bytes` 记账，~37MB/批把 64MB 窗口 mailbox 塞成 ~2 槽，
  10 个 parse worker 争 2 槽（dispatch 等 mailbox 占 parse 忙时 65.5%）。
- **改动**：
  - `wf-lang/field_usage.rs`：`defer_materialization` 允许 `on each` 规则——
    `plan_defer_safe = each_plan.is_some() || 所有 bind filter 均列式`（match 语义不变）。
  - `wf-engine/window/fanout.rs` + `commit.rs`：`RulePush` 增加 `materialize_fields`，
    窗口 actor 广播原始批时把字段白名单一并带给规则任务，保证规则任务物化出的
    `Event` 字段集与 eager 路径一致（wfx_id 稳定）。
  - `wf-runtime/rule_task.rs`：`process_batch` 增加 `materialize_fields` 参数；
    `events=None`（deferred）时用 `batch_to_events_filtered(batch, materialize_fields)`
    物化，而不是 `batch_to_events`（全字段）。
- **测试**：`field_usage.rs` 新增 `each_rule_defers_materialization_without_needs_all`。
- **测量（q1 cont 100m，WARMUP=1）**：
  - `[parse-split]`：dispatch 占比从 **65.5% → 7.4%**（mailbox 争抢解除）；
    route_parse 从 ~17ms/批降到 ~48µs/批（不再物化）。
  - EPS：6.15M / 6.63M / 7.13M / 7.16M / 7.17M（仍双峰，高相 ~7.17M 与基线一致）。
  - RSS 峰值：~8.9GB（对比 1.0.2 同配置 14GB，窗口不再持有事件 HashMap）。
- **结论**：defer 解除了 mailbox 争抢、明显降内存，但 **EPS 中性**——Q1 吞吐天花板不是
  物化/mailbox，而是管线深度（Little's law，`preread` 2GB 甜点，见
  `concurrency-scaling.md` 两道墙模型）。真正要提 Q1 吞吐得**旁路窗口 actor**（无状态
  `on each` 窗口是纯开销），或进一步列式化 each 路径消除物化——但单就 defer 而言已把
  mailbox 这道墙拆掉，为后续铺路。

## 验证结果

```text
cargo test -p wf-lang --lib   → 526 passed
cargo test -p wf-engine --lib  → 480 passed, 3 ignored (guard_bench 基准)
cargo test -p wf-runtime --lib → 165 passed
```

## M2a — qradar 规则集 guard 纯列运算覆盖率

- 规则集：`wf-examples/performance/qradar_pk/models/rules/throughput.wfl`（453 条规则）
- 统计工具：`crates/wf-engine/examples/guard_coverage.rs`（编译后遍历每个 RulePlan 的
  bind filter / 事件/close/seq branch guard / on-each filter，用 `expr_is_columnar` 分类）
- 结果：

  ```text
  rules: 453
  guards: 184
  columnar: 157 (85.3%)
  non-columnar: 27 (14.7%)
  ```

- 27 个非列式 guard 全部归为两类：
  1. **嵌套路径**（`FieldRef::Path`）：`conn_info.geo.country`、`tags[0]`、`conn_info.vlan`、
     `conn_info.flow_id` 等 object/array 字段遍历 → 按 §3.2 回退；
  2. **函数调用**（`FuncCall`）：`indexof`/`startswith`/`endswith`/`abs`/`round`/`concat`/
     `length` → 按 §3.2 回退。

- 结论：85.3% 的 guard 是纯列运算，L1 收益在真实负载上不会打折（>50% 门槛通过）。

### Q2 稳定基线重测（2GB content，100M，2026-08-18）

- 命令：`./bench.sh q2 cont 100m`（`parse_buffer_bytes=2GB`，CONNECTIONS=4，
  SHARD_KEYS=bid_events:auction，instances=4，p=10 r=10，100k 帧）；5 次全 `[clean]`、
  `EMIT q2_mod_123 = 747816`（正确，= 0.8129% × 100M）。
- 结果（EPS）：

  ```text
  6,207,247  /  6,685,834  /  7,227,088  /  7,225,745  /  6,700,509
  ```

- **结论（双峰，非 ±5% 紧）**：机器呈**双峰相位**——高相 ~7.23M（与 `concurrency-scaling.md`
  P0-② 的 q2=7.23M 精确吻合）、低相 ~6.2~6.7M；均值 ~6.8M、跨度 ±8%。这与设计稿 §5
  测量纪律的「同配置差 ±8%」一致——**M2a 的「±5%」在双峰机器上不可达**，应以
  「高相 ~7.23M / 低相 ~6.2-6.7M」作为 Q2 基线口径，后续 L1-L3 端到端对比需按相位配对
  （同相 A/B 或取高相）。

## 待办 / 下一步

1. ~~**L2 逐行物化优化（真正的省）**~~ ✅ 已完成（Step 10）：`process_batch` 已改为全行扫时间列 +
   只物化命中行，扫描/推进交错语义保留，并有逐行时间戳 + 列式 bind filter 的对拍测试。
2. ~~**branch guard 收尾**~~ ✅ 已完成（Step 11）：close/seq 的 branch guard 已列式化
   （close 用三值 permissive mask，neg 用两值 must-be-true mask），并有对拍测试。
3. **guard_bench 随机对拍**：把 `columnar_wiring` 的对拍扩展到随机数据（负值/null/2^53±1）进一步加固。
4. ~~**L2 端到端 Q2 复测**~~ ✅ 已完成（Step 10 末尾）：两次 `bench.sh q2 cont 100m` 均 clean、
   暖相 6.67M/6.70M、EMIT 747816 精确一致，EPS 无回归（受「两道墙」限无增益，见 Step 8）。
5. ~~**Q1 `on each` 懒物化（defer）**~~ ✅ 已完成（Step 12）：解除 mailbox 争抢（dispatch 65.5%→7.4%）
   并降 RSS（~14GB→~8.9GB），但 **EPS 中性**——Q1 天花板是管线深度，不是物化。
6. ~~**Q1 旁路窗口 actor**~~ 🔬 已探明（未合入）：无状态 `on each` 窗口的 actor 是纯开销，
   把 raw batch 直接广播到规则任务（跳过 append/reorder/evict）确实把 **append/广播速率** 从
   ~7.17M 拉到 **~10.16M**；但用**滞后口径**（规则任务处理完才记 append，EMIT 精确=92M）
   端到端仅 **~3.83M**——这是**并发广播**（10 parse worker 直推规则通道）导致的退化，不是
   sink。切掉 sink 单独复测（`flush_alerts` 直接 drop）：EPS 7.52M/8.18M（vs 基线 7.17M），
   **sink 只占 ~5-10%**。**结论：Q1 端到端天花板仍是管线深度（preread 2GB / Little's law），
   sink 与窗口 actor 都不是主墙；旁路与 sink 切均已回退。**
