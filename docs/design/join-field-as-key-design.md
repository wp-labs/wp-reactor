# join 字段作窗口键（join-then-key）设计（Path A）

> 目标：让 `match<category:10m>` 等**引用 join 侧字段**的窗口键可用——事件在路由进
> 窗口前先执行 snapshot join 拿到键值，再按该键分组。解锁按 join 字段分组的查询面
> （NEXMark Q4 按 category、Q6 按 seller），一次解决两个已知语义约束。
> 状态：P0/P1/P2 已完成（2026-08-21，见 §10）；归属 wp-reactor（wf-lang + wf-engine
> + wf-runtime），及 wfgen oracle 同步（verify 对拍）。

## 1. 背景：为什么需要

NEXMark Q4（`nexmark/nexmark` `q4.sql`）：内层每 auction 的胜出价 `MAX(price)`，
外层按 `category` 分组求 `AVG`。category 是 **auction 的字段**，而驱动事件是 bid——
bid 事件本身没有 category，须 join auction 才能拿到。

现状卡点（`SEMANTIC_ALIGNMENT.md` §5.3）：

1. **match 键必须来自驱动事件自身字段**（q6 注释同款约束："窗口键必须来自原始事件"）
   ——`key.rs::extract_key` 从事件字段解析键，字段缺失即 `None` → 该事件被跳过。
2. **conv 后处理无聚合算子**（仅 sort/top/dedup/where）——外层 `AVG` 无法表达。

本文只解决卡点 1（join 字段作键）。卡点 2（二级聚合 avg-of-max）不在范围：
官方 Q4 的 avg-of-max 口径以文档标注（q4 当前为 avg×close 面，见 §6 备注）。

## 2. 现状盘点（关键代码路径）

| 环节 | 位置 | 现状 |
|---|---|---|
| 键声明 | `wf-lang/src/plan.rs` `MatchPlan { keys: Vec<FieldRef>, key_map }` | `match<category:10m>` 的 `category` 编译期解析到**驱动 bind 的字段**；非驱动字段编译报错 |
| 键提取 | `wf-engine/src/match_engine/cep/key.rs::extract_key(event, keys, key_map, alias)` | 从事件 `FieldSource` 取值；缺失 → `None` → `advance_at_with_diagnostics` 返回 `Accumulate`（跳过） |
| 窗口路由 | `match_engine/mod.rs` `advance_at_with_diagnostics`（L300+） | `extract_key` → `InstanceKey::sliding/fixed(scope_key, bucket)` |
| join 执行 | `executor/context.rs::execute_joins`（pub(super)）+ `match_exec.rs` with-joins 富化 | **懒执行**：match/close 命中时才算（`execute_match_with_joins`），事件进窗口前不做 join |
| 窗口查找 | `match_engine/types.rs` `WindowLookup` trait（snapshot / join_lookup / with_timestamps） | `advance_at_with_masks` 已接收 `windows: Option<&dyn WindowLookup>`——**运行时传真 lookup**，oracle 传 `None` |
| oracle（wfgen） | `crates/wfgen/src/oracle/mod.rs` `run_oracle_events_opts` | 逐规则 SM 独立推进；**无窗口状态、无 WindowLookup** → join 一律不评估（q21 已知差异即源于此） |

## 3. 语法与编译（wf-lang）

### 3.1 语法

保持现有 `match<KEY:duration>` 语法，编译器放宽键解析：

```wfl
// Q4 按 category（join auction 拿 category）
rule q4_avg_price_by_category {
    events { b : bid_events }
    match<category:10m:fixed> {
        on event { b | count >= 1; }
        and close { w: b.price | avg >= 10; }
    } -> score(20.0)
    join auction_events snapshot on b.auction == auction_events.id
    ...
}
```

`category` 解析顺序：**驱动 bind 字段 → 各 snapshot join 的 right window 字段**。
在驱动字段中找不到、但在某个 snapshot join 侧窗口 schema 中找到 → 标记为 join 键。

### 3.2 plan 变更

`MatchPlan` 增加：

```rust
/// 键来自 snapshot join 侧（join-then-key）的解析描述。None = 键全在驱动事件上。
pub key_join: Option<JoinKeyPlan>,

pub struct JoinKeyPlan {
    /// 规则 joins 列表中的第几个 join（右窗提供键值）
    pub join_idx: usize,
    /// 右窗名（join_lookup 目标窗口）
    pub right_window: String,
    /// 驱动侧 join 左键字段（如 b.auction）——从事件取值驱动 join_lookup
    pub left_field: FieldRef,
    /// join 条件右侧键字段（如 auction_events.id）——右窗查找索引键
    pub right_key_field: String,
    /// 右窗字段名（如 auction_events.category）
    pub right_field: String,
    /// 键的逻辑名（key_map / scope 键名用，默认 = right_field）
    pub key_name: String,
}
```

### 3.3 编译校验（新增）

- join 模式必须为 **snapshot**（anti 无行可取值；asof 有窗口时序语义，v1 不支持）；
- join 条件左侧（left）必须包含**驱动 bind 的字段**（如 `b.auction`），保证能从
  事件解析出 join 查找键；
- 键引用多个 join 字段（复合 join 键）→ 编译报错，v1 仅单 join 键；
- 键同时引用驱动字段和 join 字段（混合键）→ v1 报错，后续扩展；
- **key_mapping（key_block）与 join 键同用 → v1 编译报错**（交互语义后续定义）；
- **多驱动 bind → v1 报错**（join 键规则须单 bind；nexmark 全单 bind）；
- join 键字段须标量 base 类型（float/结构化排除，同 join 索引键规则）；
- join 键规则 join 须**恰好一个条件**（v1 单 join 键）；
- 限定引用 join 窗口的键（`auction_events.category`）→ 报错，须写非限定形式。

## 4. 运行时执行（wf-engine：join-then-key）

### 4.1 执行点

`advance_at_with_diagnostics`（`match_engine/mod.rs` L300+）在 `extract_key` **之前**：

```
if let Some(kjp) = &self.plan.key_join {
    // 1) 从事件取 join 左键（现有字段解析）：如 b.auction
    //    —— 查找键字段 = JoinKeyPlan.left_field（驱动侧）
    // 2) windows.join_lookup(right_window, right_key_field, key_value)
    //    —— right_key_field = joins[join_idx].conds.first().right 的字段名
    //       （右窗匹配字段，Q4 是 auction_events.id）；与待读的键值字段
    //       right_field（category）是两回事，勿混用。
    //    —— windows 来自 advance_at_with_masks 的入参（运行时已是真 lookup）
    // 3) 命中 → 取 joined row 的 right_field 作为键值；未命中 → 与"键字段缺失"
    //    同语义：返回 Accumulate，跳过（不建实例）
}
```

**与 match-time join 的一致性**：join-then-key 做的是裸 `join_lookup`（按
`right_key_field` 哈希取候选行）+ 取**首行**；match-time join（`execute_joins`）是
`first_join_key` 哈希查 + `find_matching_row` 全条件校验 + 取首匹配行。两者一致的
前提是 **K1c 的"恰好一个条件"约束**（候选行即全部匹配行，无需二次过滤）与**首行语义**
（NEXMark join 键唯一 → 首行=唯一行；oracle 侧 HashMap 键覆盖保持单行）。多条件
join 在 v1 被编译拒绝，因此不存在"join_lookup 取到 find_matching_row 会拒绝的行"
的路径。

实现要点：**键提取直接读 joined row**，不必克隆/富化整个事件——join-then-key
在 advance 入口完成（`join_lookup` → `row.field_value(right_field)`），
`extract_key` 路径仅在无 key_join 时走。

### 4.2 热路径成本

- join 查找 = `join_lookup`（哈希索引，O(1) 级）——仅当规则声明 key_join 时每事件
  多一次查表；无 join 键的规则零开销（现有路径不变）。
- 列式向量化（批量 join-then-key）作为后续优化（P3+），见 §7。

### 4.3 与现有 join 的关系

- match/close 输出时的 join 富化（现有 execute_match_with_joins /
  execute_close_with_joins）**保持不变**——join-then-key 只在键提取前多走一次
  join_lookup，两者独立。
- 同一规则可同时有 join 键 + match/close 时 join 富化（如 Q4 键=category、输出还想
  带 seller）：各自独立执行。
- **v1 范围声明（oracle 对拍面）**：oracle 的 match 与 close 输出均已接
  `execute_*_with_joins` 并传 lookup（2026-08-21 二轮 review 修复），因此
  yield/entity/score 读额外 join 字段的规则也能字段级对拍；Q4/Q6 读键与聚合字段，
  恰好满足最简形态。

## 5. oracle 同步（wfgen：verify 对拍的前提）

### 5.1 缺口

`run_oracle_events_opts` 目前逐规则推进 SM，**没有窗口状态**（`windows` 传 None），
join 一律不评估。join 键规则下 oracle 将永远拿不到键 → 全部跳过 → 与引擎 EMIT 对不上。

### 5.2 实现（P2，2026-08-21）

1. **窗口状态**：`OracleLookup` 按规则 joins 维护 join 目标窗口行
   `HashMap<key_repr, (ts, Event)>`，键字段 = join 条件右侧字段（`conds.first().right`）；
   `over` 滑动过期用**时间索引（BTreeMap）从头部批量弹出**（watermark 单调，
   摊销 O(1)/事件——全量 retain 是 O(n²)，即 18 分钟卡死的根因之一）。
   **watermark 按窗口自身事件流推进**（二轮 review 修复）：auction 行只由 auction
   事件驱逐，驱动事件（bid）的时间戳不推进 join 窗口水印——与引擎窗口驱逐语义
   一致（NEXMark 的 over≈auction 生命周期会掩盖这一差异，但语义不依赖它）。
2. **预加载**：事件循环前预加载 join 窗口全部行——镜像引擎 replay 的 **append
   超前**（pull/push 解耦下窗口 actor append 快于规则消费，join 可见"已 append
   行"，含 bid 随机引用的"未来"auction；同步流式仅 57% 命中 vs 引擎 100%）。
3. **fixed 桶边界扫描**：fixed 窗口规则只在事件跨桶边界时 `scan_expired_at_with_conv`
   （q4 10m 162s → 110s）。
4. **WindowLookup 实现**：`join_lookup`（哈希直查）/`snapshot`/`snapshot_with_timestamps`
   返回 `JoinRow::Event`。
5. `run_oracle_events_full` 增加 `schemas: &[WindowSchema]` 入参（cmd_verify_nexmark
   传 schemas_list；无 schemas = 无窗口状态 = 历史行为）。

### 5.3 对 q21 的连带收益

oracle 有窗口状态后，q21（anti join）真实评估：oracle=引擎=0（anti 全 drop），
**q21 移出 known-diff**（2026-08-21 实证）。

## 6. 测试与验证锚点

| 层 | 锚点 | 预期 | 状态 |
|---|---|---|---|
| wf-lang 单测 | 键解析到 join 字段 / 非 snapshot join 报错 / 复合键报错 / key_mapping 禁用 / 歧义 / 非标量 | 编译期正确性 | ✅ 13 例（checker）+ 2 例（compiler） |
| wf-engine 单测 | join 键命中 → 建对应 key 实例；未命中 → 跳过；键字段缺失 → 跳过；无 lookup → 跳过 | 状态机行为 | ✅ 8 例 |
| 端到端（新 Q4） | `match<category:10m:fixed>` + close avg | oracle == 引擎（fixed+close 收口非确定为独立已知问题，见 §7） | ⚠ known-diff（oracle 为语义参考值） |
| 端到端（新 Q6） | `events { b } + join auction + match<seller:10m>` + avg | 标准 Q6「按 seller 均价」面 | ⚠ known-diff（join 可见性非确定） |
| 回归 | 现有 26 规则 + oracle 测试全绿 | 无 join 键规则零行为变化 | ✅ |

> 备注：Q4 的**外层 avg-of-max**（卡点 2）不在本设计范围——需要二级聚合管道
> （stage-1 输出喂 stage-2 或 conv 聚合算子），另行设计；q4 以 avg×close 面
> 标注"部分对齐"。

## 7. 边界、风险与已知限制

1. **join miss 即跳过**：bid 引用的 auction 未到/已过期 → 无键 → 跳过（与现有
   "键字段缺失跳过"一致）。引擎与 oracle 同输入序 → 对拍一致。
2. **fixed+close 收口非确定性**（已有已知问题，`SEMANTIC_ALIGNMENT.md` §6.1）：
   join 键不影响该问题；Q4/Q9/Q16 仍报已知差异 ⚠ 直到引擎 EOF 扫尾修复。
   实证（2026-08-21）：q4 200k 引擎收 3 桶（78）vs 10M 只收 1 桶（26）——批级
   收口预算 1024 + 尾桶依赖墙钟 scan_timeouts，**同规则不同规模收口数不同**。
3. **join 可见性非确定（引擎 replay）**：窗口 actor append 超前于规则消费 +
   evictor sweep（默认 30s）时机 → 引擎 join 命中率运行时非确定（200k：bid 随机
   引用"未来"auction 时引擎 100% 命中 vs oracle 语义参考值 57%）。oracle 预加载
   镜像 append 超前，保留按事件时间 `over` 过期；差异归 verify known-diff。
4. **性能**：join 键规则每事件一次 join_lookup；92M bid 热路径上的成本待实测
   （P1 后 q4/q6 EPS 对比）。列式批量 join-then-key 为后续优化。
5. **分片（与 rule-sharding 互斥）**：join 键的路由键（category）在 join **前**不可得，
   而 join 又发生在 shard **内部**（路由之后）——要按 category 路由需先 join，要
   join 需先路由到持有 auction 窗口的 shard（该窗口自身也可能分片），鸡生蛋。**这是
   根本性不兼容，不是"键闭包假设冲突"可修**：join-then-key 与 rule-sharding 互斥，
   v1 仅 `CONNECTIONS=1`（单连接、无分片）验证；多分片需单独设计（auction 全量
   复制到各 shard 或按 join 左键路由等，P3+）。
6. **asof/anti join 作键**：v1 不支持（asof 时序语义、anti 无行）。
7. **内联 test 块**：test harness（`contract.rs`）走 `sm.advance_at`（无 windows），
   不维护 join 目标窗口状态 → join 键规则的内联 test 全部 miss（`hits==0` 假通过）；
   **join 键规则禁用内联 test**，验证锚点 = 端到端对拍（oracle vs 引擎）。
8. **多行命中语义**：`join_lookup` 返回 `Vec<JoinRow>`。引擎与 oracle 均取**首行**
   （oracle 的 `HashMap<key_repr, …>` 键覆盖保留**最新**行；NEXMark auction/person
   主键唯一 → 首行=唯一行，两者一致）。**v1 要求 join 键唯一**（重复键的多行语义
   未定义，首行 vs 最新行可能分歧）；超出 NEXMark 的重复键场景需显式约定。

## 8. 分阶段实施与验收

| 阶段 | 内容 | 验收 | 状态 |
|---|---|---|---|
| P0 | wf-lang：键解析放宽 + `key_join` plan + 编译校验 | 单测：解析/报错三例 | ✅ 2026-08-21（K1b/K1c + JoinKeyPlan + 编译镜像） |
| P1 | wf-engine：`advance` 前 join-then-key（extract_key join 旁路） | 单测：命中/未命中/缺失；26 规则回归 | ✅ 2026-08-21（`advance_at_with_diagnostics` join-then-key 旁路；8 单测） |
| P2 | wfgen oracle：窗口状态 + WindowLookup + 入参传递 | 新 Q4/Q6 对拍锚点；q21 known-diff 复评 | ✅ 2026-08-21（预加载 + 事件时间过期；q21 对拍打通 oracle=引擎=0；q4/q6 差异归 known） |
| P3 | 优化：列式 join-then-key、多分片键路由 | q4/q6 EPS 对比、分片对拍 | ⬜ 未开始 |

## 9. 评审结论（2026-08-21，对照实际代码核实）

### 9.1 方案正确性已验证的点

| 断言 | 代码证据 |
|---|---|
| 运行时 advance 传真 WindowLookup | `rule_task.rs:964` `advance_at_with_masks(alias, &row_event, event_nanos, Some(&lookup), ...)`——每次 advance 都传 |
| join 查找 O(1) 索引路径 | `window_lookup.rs:163-175` `RegistryLookup::join_lookup`：窗口维护的 join 索引直接返回 `Arc<Event>`，无 seq 水印时走索引而非扫描 |
| 键校验位置 = K1 | `checker/rules/keys.rs:28-39`：未限定键须存在于**所有非 join 事件源**，且**已显式跳过 join 窗口**（`scope.join_windows`）——这就是要改的精确位置 |
| join 窗口在 checker scope 中可见 | `checker/rules/scope_build.rs:61` `scope.join_windows.push(target)` |
| `advance_at_with_masks` 已带 `windows` 参数 | `match_engine/mod.rs` 签名 |
| oracle 无窗口状态（join 一律不评估） | `wfgen/oracle/mod.rs` `windows` 恒传 None |

### 9.2 遗漏与修正（已并入实施）

1. **key_mapping 交互未定义**：语言已有 `key_block`/`KeyMapPlan`（`wfl_parser/match_p.rs:47`、
   `checker/rules/keys.rs:145` `check_key_mapping_clause`，多 bind 规则按 alias 映射逻辑键）。
   → **v1 约束：join 键规则禁用 key_mapping**（编译报错），交互语义后续定义。✅
2. **extract_key 需透传 windows**：join 键旁路需要。→ 运行时在 advance 入口
   join-then-key（`JoinKeyPlan` 自带 `left_field`/`right_window`/`right_key_field`，
   状态机无需访问 `RulePlan.joins`）。✅
3. **wfl 内联 test 块的 join 窗口行为未验证（真实风险）**：确认 test harness 走
   `sm.advance_at`（无 windows）不维护 join 窗口状态 → **join 键规则禁用内联 test**，
   验证锚点 = 端到端对拍。✅
4. **多驱动 bind 未约束**：→ **v1 约束单驱动 bind**（编译报错），多 bind 后续。✅
5. **oracle `over` 入参来源已确认**：`cmd_gen` / `cmd_verify_nexmark` 调用 run_oracle 时
   均已持有 schemas。✅（`run_oracle_events_full` 增加 schemas 入参）
6. **性能估计补充**：join-then-key 每事件一次哈希 join_lookup（~O(1)）+ JoinRow 读字段，
   估算单事件附加 ~0.1-0.5µs；对 5-10M EPS 级规则影响可测但有限，P1 后以 q4/q6 EPS
   实测为准；列式批量 join-then-key（P3）可再回收。
7. **一致性天然成立**：join 键查找与 match-time join 用同一 `RegistryLookup`（含
   `with_source_watermark` 的 seq 可见性），可见性语义一致，无额外对齐成本。

### 9.3 结论

方案可行，P0/P1/P2 划分合理，已全部实施（2026-08-21）。新增约束（key_mapping 禁用、
单 bind、内联 test 禁用）已在 §3.3/§7 记录。

## 10. 实施记录（2026-08-21，P0/P1/P2 完成）

- **P0（wf-lang）**：`MatchPlan.key_join: Option<JoinKeyPlan>`（含 `left_field`/`right_key_field`/`right_field`）；checker K1b 放宽简单键到 snapshot join 右窗（非 snapshot/复合/混合/key_mapping/多 bind/歧义/非标量均编译报错）；compiler `resolve_join_key` 镜像。
- **P1（wf-engine）**：`advance_at_with_diagnostics` 在 extract_key 前 join-then-key——`windows.join_lookup(right_window, right_key_field, left_val)` 命中取 `right_field` 作键，未命中/无 lookup/缺左键 = 跳过。8 个单测覆盖命中/未命中/缺字段/无 lookup/键缺失。
- **P2（wfgen oracle）**：
  - `OracleLookup`：按规则 joins 维护 join 目标窗口行（key → (ts, Event)），`over` 滑动过期改为**时间索引（BTreeMap）批量弹出**——修复全量 retain 的 O(n²)（2m 事件 110s → 11s，即 18 分钟卡死根因之一）；
  - **预加载**：事件循环前预加载 join 窗口全部行（镜像引擎 replay 的 append 超前——bid 随机引用"未来"auction 时同步流式仅 57% 命中 vs 引擎 100%）；
  - **fixed 桶边界扫描**：fixed 窗口规则只在跨桶边界事件时 `scan_expired_at_with_conv`（q4 10m 162s → 110s）；
  - **q21 对拍打通**：oracle 有 anti join 窗口状态后 oracle=引擎=0（anti 全 drop），q21 移出 known-diff；
  - **已知差异（引擎 replay 非确定）**：q4（fixed+close 收口：200k 收 3 桶 vs 10m 收 1 桶）+ q6（join 可见性：append 超前 + evictor sweep 30s 时机）→ 归入 verify known-diff，oracle 为语义参考值。
- **实证数据（10M，seed=1）**：q4 oracle 52 ⚠ 引擎 26；q6 oracle 1,522,840 ⚠ 引擎 1,110,764（均为引擎 replay 非确定性差异，非 join-then-key 实现缺陷——200k 旧 q4 count 版 oracle=引擎=184,000 证明基础 join 路径完全一致）。
- **二轮 review（2026-08-21）**：oracle close 路径补 `execute_close_with_joins` 富化（此前只富化 match，close 规则读额外 join 字段会失配）；join 窗口过期改按**窗口自身 watermark**（bid 时间不再驱逐 auction 行——NEXMark 数值实测不变：q4 52、q6 1,522,840，印证 over≈拍卖时长掩盖）；预加载改 `I: Clone` 流式双遍（不收集 Vec，30M/100M 防 OOM，无 join 规则跳过预加载遍）；文档明示分片互斥与多行命中语义。
