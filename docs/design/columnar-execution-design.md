# 整体列式执行设计（Columnar Execution）

> **状态：设计稿（2026-08-17）**
> **动机**：输入已是列式（Arrow RecordBatch），但计算中段逐行物化为
> `Event(HashMap)` 并逐行求值——这是 Q1/Q2 吞吐上限的直接成本（实测
> guard 表达式 122ns/事件、字段提取 57ns/事件）。本文给出"全程列式"的
> **核心执行设计**与分层改造方案，改造按层推进、**每层用单元基准对比验收**。

---

## 1. 现状审计：数据流里哪些列式、哪些逐行

```
列式 ✓        逐行物化 ✗       逐行求值 ✗       逐行填充 ✗
IPC 解码       batch_to_events   guard/表达式       AlertColumnBuilder
RecordBatch ──→ Vec<Event> ─────→ 规则匹配/输出 ──→ 列缓冲(已批量预留)
   (列式)      每行一个 HashMap   每事件 interpreted  commit_each_row
               每字段 hash insert   评估(122ns/事件)   逐行写列
```

| 环节 | 当前形态 | 成本锚点（实测） |
|---|---|---|
| 输入 | **列式**（Arrow IPC，无压缩，wire≈content） | — |
| 窗口存储 | 批级（`TimedBatch` 存整批 `RecordBatch`） | 已列式（批级） |
| **窗口读→规则** | `batch_to_events` 逐行物化 `Event{HashMap}`（每行建 HashMap、每字段提取+转换+insert） | 物化是共享管道大头（Q1 250ns/事件中占比未单独测，字段提取单次已 57ns） |
| 规则过滤 | `event_matches_alias` → guard 逐行表达式 | **122ns/事件**；无 filter 基线 3.8ns（**guard 增量 118.7ns 占 filter 路径内部 96.9%**，非 Q2 每事件占比——Q2 每事件 310ns 中 guard ~120ns、共享管道/状态机其余） |
| 字段提取 | `Event.fields: HashMap<SmolStr,Value>` 哈希查找 | **57ns/事件** |
| 规则匹配 | 逐行 state machine（per-key 实例 `HashMap`） | Q5/Q7 规则密集路径 ~800ns/事件 |
| **输出** | `AlertColumnBuilder`：**列缓冲 + `reserve_rows` 批量预留**，但逐行 `commit_each_row` 填充 | 输出构建占规则计算 ~57%（二分法①） |

**结论**：输入输出两侧已是"列式骨架"，**中段（物化 + 逐行求值）是唯一的行式残留**，
也是可收割成本最集中的区域。

---

## 2. 目标架构：全程列式

```
IPC 解码 ──→ RecordBatch（列式）
      │
      ├─ 列式过滤/guard（按列原生类型 SIMD）→ 行选择（掩码/索引）
      │
      ├─ 事件物化延迟化：列过滤/投影先行，命中行才物化 Event
      │
      ├─ 输出列式填充：命中行 → 整列写（批量，非逐行）
      │
      └─ 有状态路径：状态机接口不变，列式输入→命中行物化→逐行 advance
```

**不变式**：列式只作用于**无状态、纯投影/过滤/输出**部分；**有状态规则语义
（per-key 实例、窗口保序、join）保持不变**，状态访问仍逐行——列式不改变
规则语义，只改变无状态部分的执行形态。

---

## 3. 核心执行设计（本设计的地基）

> 分层方案（§4）的全部改造围绕本节五个组件展开。**先定数据表示与执行模型，
> 再谈分层**——否则"列式"只是口号。

### 3.1 列式数据表示：ColumnarBatch（复用 Arrow，不新造容器）

**决策：中间表示直接复用 Arrow `RecordBatch` + 轻量不可变视图；行选择是独立的
执行状态，不进入视图结构（多规则/多阶段各自持有，互不污染）。**

```rust
/// 规则处理阶段的列式视图：投影（零拷贝，不可变）。
pub struct ColumnarBatch<'a> {
    batch: &'a RecordBatch,        // 原始列（输入或窗口批）
    projection: Vec<usize>,        // 本规则可见字段 → batch 列索引（静态预编译）
    field_map: HashMap<FieldRef, usize>,  // 字段名 → projection 内索引（Simple/Qualified 均已归一化）
}

/// 行选择：独立的执行状态（视图不变，选择在规则/阶段间传递）。
pub enum RowSelection {
    All,                           // 全行（无过滤）
    Mask(arrow::array::BooleanArray),  // 列式 guard 产出：每行命中位（与 arrow_select::filter 直接衔接）
    Indices(Vec<u32>),             // 物化/状态机阶段：命中行索引（有序）
}
```

- **字段名 → 列索引映射（正确性基础）**：`field_map` 由编译期从规则字段引用
  构建——`FieldRef::Simple("auction")` 与 `FieldRef::Qualified("b", "auction")`
  **归一化到同一列**（alias 前缀剥离，与现有 `Event.fields` 平铺列名的解析一致）；
  列式求值/物化全部通过 `field_map` 定位列，**不再按名字现场查找**；
  字段缺失（规则引用了列中不存在的字段）→ 该字段行判空（与 `batch_to_events`
  跳过缺失列一致）；

- **字段读取**：`view.field_value(row, field_idx)` 直接从 Arrow 列取（**原生类型**），
  仅在需要 `Value` 时转换（见 3.4 类型映射）；
- **零拷贝投影**：`projection` 只是列索引数组，不复制数据；每规则投影到**自己读的
  字段**（静态预编译）；上游全批广播零拷贝（§3.5-E），投影是规则侧职责；
- **行选择演进**：`All → Mask`（列式 guard 过滤）→ `Indices`（物化/状态机）；
  `Mask → Indices` 是一次 O(n) 收集（`BooleanArray` → 命中索引）；
- **生命周期**：`view` 借用输入/窗口批（不可变）；`RowSelection` 为栈值，无堆分配
  （`Mask`/`Indices` 按需）；列数据零复制，**列式中间结果只有掩码和索引**
  （物化出的 `Event` 是按需的例外，§3.3）。

### 3.2 表达式执行模型：AST 保留 + 列式求值器（双轨）

**决策：`wf_lang::ast::Expr` 不变（语义/编译共用），新增向量化求值器。**

```text
Expr(AST, 现有) ──┬─ expr_is_columnar()? ── yes ─→ ColumnExpr 计划（列运算树）
                  │                               │
                  └─ no（函数/嵌套/array/object）→ 现有 interpreted eval（逐行）
```

- **判定时机（编译期，运行期零开销）**：`expr_is_columnar` 是**静态判定**（规则
  不变），在规则编译期预计算——产出预编译的列计划（如 `ColumnarGuard`：列算子树
  + 类型分派表），运行期每批直接执行，**不做运行时判定**；
- **列式判定** `expr_is_columnar(expr) -> bool`：纯字段算术/比较/常量
  （`Field % const == const`、`Field > const`、`Field == Field`、`!`、`&&`/`||`
  短路）；含函数/深度嵌套路径 → false（回退逐行）；**列表索引路径 `root[i]`**
  （根字段 + 一个常量下标，如 `c.tags[0]`）→ true（列式 List-Index：JSON 数组
  列 / 原生 List 列的偏移读，逐行免整数组 `Value` 重建，2026-08-23 增补）；
  **含窗口查询（`window.has(...)` 等逐事件窗口读）的 guard → false**（需
  `windows` 参数，无法列式）；**多 bind 规则逐 bind 独立判定**（每个 bind 的
  filter 单独编译为列计划或回退）；
- **类型不匹配（订阅时静态判定，非运行期）**：`expr_is_columnar` 是静态的，看不到
  运行期列类型；类型匹配在**规则订阅源时一次性比对**——guard 涉及字段的声明类型
  vs source schema 列类型，存在不匹配（如声明 digit/Int64 但列是 Utf8 数字字符串）
  → **该规则（或该 bind）标记为逐行**（guard 禁用列式）；运行期不逐批判类型
  （schema 静态，订阅时已定），保证两轨结果一致；**若未来允许变 schema，再退化
  为逐批类型检查（当前运行期零分支）**；
- **字段不在 source schema（按 null 处理，非回退）**：guard 引用了 source schema
  **不存在的字段**（非 null，是列根本不在）→ 列式 `field_map` 定位不到该列时，
  **该字段按 null 处理**（`field_value` 返回 None，等价于全 null 列）——null 在
  guard 中短路不命中（§3.4 #3），与 `batch_to_events` 跳过缺失列、规则读到
  `None` 完全等价；**不因此整规则回退**（缺字段不阻塞其它字段的列式求值，
  回退反而无谓丢掉该规则/bind 的列式覆盖率）；
- **列计划**：`ColumnExpr` 递归求值，输入列 → 输出 `Mask`/值列；算子按
  **Arrow 列原生类型**分派（Int64 取模 ≠ Float64 取模 ≠ Utf8 比较）；
- **guard 求值** `eval_guard_columnar(expr, view) -> Mask`：对**全列**批量求值产出
  命中 `Mask`，再与当前 `RowSelection` 求交集（伪代码 `combine(sel, mask)`）——
  语义上等价于“仅 selection 内行求值”，实现上全列求值更利于 SIMD；**null 行
  短路为不命中**（与 `batch_to_events` 跳过 null 一致）；
- **`&&`/`||` 短路语义（列式定义）**：列式 `And` = `left_mask & right_mask`、
  `Or` = `left_mask | right_mask`（**全列逐位**，与 selection 的交集在 `combine`
  层统一处理）；布尔结果与逐行短路等价——表达式无副作用，字段缺失/null 统一
  按不命中处理；复合短路场景（含 null 字段）进 §3.4 对拍验证；
- **算子形态**：简单算子先按行循环（但已免 HashMap/Value 转换），热点算子
  （整数取模/比较）后续 SIMD 化——**先正确，再向量化**；
- **掩码流**：算子间只流动 `RowSelection`，不物化值列（除非投影需要）。

### 3.3 执行器插入点与接口边界

**插入点：`RuleTask::process_batch`（per-rule 单规则任务，真实架构）**——当前收到
**已物化的 `Arc<Vec<Arc<Event>>>`**，改为收到 `&RecordBatch` + 列式处理：

```rust
// 改造前（真实架构）：上游窗口 fanout 广播已物化 events（Arc<Vec<Arc<Event>>>），
//   每规则任务 process_batch(window, seq, events) → 逐行 event_matches_alias + advance_at
// 改造后：上游全批广播 RecordBatch（零拷贝，复用 TimedBatch.batch），
//   规则任务各自投影 view + 按需物化——共享从“物化 Event”变为“共享全批列引用”。
fn RuleTask::process_batch(batch: &RecordBatch) {
    let view = ColumnarBatch::new(batch, self.field_projection());    // 本规则字段投影（静态预编译）
    // 执行顺序：① 分片 key mask（r 分片：本 shard 的 key 命中位，列式哈希）
    //           ② guard mask（列式或逐行回退）→ ③ 物化 → ④ 状态机
    let mut sel: RowSelection = self.shard_key_mask(&view);           // 不属本 shard 的行连 guard 都不求值
    sel = match self.guard.as_ref() {
        Some(g) if expr_is_columnar(g) => combine(sel, eval_guard_columnar(g, &view)),  // 3.2 列式
        Some(g) => combine(sel, interpreted_filter_rows(g, &view)),   // 逐行回退，产出 Indices
        None => sel,
    };
    let events = materialize(&view, &sel, &self.field_projection());     // 3.5-F 仅选中行；物化字段 = 规则读字段并集
    for (row, ev) in events {
        let outcome = machine.advance_at(alias, &ev, ts, Some(&lookup));  // 接口不变（含窗口读 lookup）
        if matched { /* 列式输出填充，§4-L3 */ }
    }
}
```

- **入**：`&RecordBatch`（规则任务收到批，不物化 Event 数组）；
- **出**：matched 行（`(row_idx, step_data)`）→ 输出列式填充（从 view 取列值，
  不依赖已 drop 的 Event）；
- **上游共享层（改造点）**：窗口 fanout 现在广播已物化 `Arc<Vec<Arc<Event>>>`（一次
  物化、多订阅规则共享，#19）。列式后改**全批广播 `RecordBatch`（零拷贝，直接
  复用 `TimedBatch` 已存的 batch 引用，不做投影）**——`TimedBatch` 本就存着整批
  `RecordBatch`（`window/buffer/types.rs:37` `batch: RecordBatch`），fanout 广播
  这个已存批即零拷贝（不新造容器、不复制列数据），是「上游广播 RecordBatch」
  可行性的地基；各规则任务从同一全批按
  自身 `field_projection()` 投影 view + 按需物化；共享从“物化 Event”变为“共享
  全批列引用”，物化只发生在需要状态机的规则（§3.5-F）；`on each` 规则
  （each_exec 批路径，C2）完全不物化；
- **`materialize` 字段范围（正确性要点）**：物化的 Event 必须包含**规则读取字段
  并集** = guard ∪ step ∪ yield ∪ entity/key 字段（即 `self.field_projection()`）。
  漏一个字段 = 规则在 step/yield 读到空值，静默错——投影静态预编译，
  `materialize` 只从投影内取；
- **事件时间 `ts` 来源**：`advance_at(alias, &ev, ts)` 的 `ts` 从**时间列**逐行取
  （`view.field_value(row, ts_idx)`，ts_idx = 时间字段在投影中的位置）；列式改造
  必须包含时间列索引解析；
- **`on each` 路径**：伪代码为 match 状态机路径；`on each` 规则走 each_exec
  批路径（C2 已存在），guard 列式同样适用（同一 `eval_guard_columnar`），但
  命中行直接走批输出、不进 state machine（L1 范围两者都覆盖）；
- **与规则分片（r）**：分片在 key 哈希——**分片 key 过滤是列式的第一层 mask**
  （`shard_key_mask`：取 key 列 → 哈希 → 本 shard 命中位，先于 guard，不属本
  shard 的行连 guard 都不求值）；各分片独立列式处理；
- **多 bind 规则（seq/多 events）**：每个 bind 的 filter **独立列式求值**（各自
  `eval_guard_columnar` → 独立 mask），bind 间按规则语义取交集（`combine`）；
  bind 之间的 step 匹配（顺序/否定/计数）仍逐行状态机——列式只作用于
  bind filter，不改 step 语义；Q2 为单 bind，掩盖了此点；
- **兼容**：`expr_is_columnar == false`（含类型不匹配、含窗口查询回退）的规则
  走现有逐行路径，**逐条规则可混合**。

### 3.4 类型映射与语义等价（正确性核心）

| Value（现有） | Arrow 列（列式） | 列式运算 | 等价性 |
|---|---|---|---|
| `Number(f64)` | `Int64/Int32/UInt*` | 按原生整数（取模/比较） | **需验证**：现有 f64 `%` 对 \|v\|<2^53 精确 → Int64 取模等价；负值符号一致（Rust `%` 均为截断余数） |
| `Number(f64)` | `Float64` | f64 运算（同现有） | 天然一致 |
| `Number(f64)` | `Timestamp(Ns)` | 时间戳按 i64 比较 | **需验证（比整数 2^53 更危险）**：纳秒 ~1.7e18 ≫ 2^53，现有 `Timestamp → Value::Number(ns as f64)` 已丢精度——列式按 i64 更准但**可能改变比较结果**（相邻纳秒在 f64 判等、i64 判不等）；时间戳是规则高频比较字段，进 §3.4 对拍清单 |
| `Str(SmolStr)` | `Utf8` | 字节比较/前缀 | 一致（Utf8 字节序 == Value::Str 比较） |
| `Bool` | `Boolean` | 位运算 | 一致 |
| `Array/Object` | `List/Struct` | **不回退列式**（逐行 interpreted） | 语义不变 |

**语义等价验证**（落地 L1 的必过项）：
1. `guard_bench` 增加**对拍测试**：随机数据（含负值、null、边界 2^53±1）上，
   **≤2^53 范围内**列式 guard 与 interpreted guard 结果 **100% 一致**；>2^53
   的 diverge 按 #2 记录的语义差异验证（列式 i64 更准，不作为对拍失败）；
2. **>2^53 的整数（含时间戳纳秒 ~1.7e18）**：现有 f64 丢精度、列式 Int64 保
   精度——**列式更准确，但可能改变比较结果**（如比较 `2^53` 与 `2^53+1`：f64
   下两者均舍入到 `2^53` 判等为真，i64 下判等为假）；**不是「向上兼容/不受
   影响」**，与 Timestamp 行同属「更准但语义可能 diverge」——须进对拍清单、
   在文档记录为已知语义差异；
3. null 语义：列式短路不命中 == 现有跳过 null（一致）；
4. **复合短路**：`&&`/`||` + null/缺失字段的组合（如 `a>0 && b==1` 中 b 为
   null、`a||b` 中 a 非空 b 缺失等）逐位对拍——列式位运算与逐行短路+null
   传播结果一致（§3.2 定义）。示例字段均为简单字段；**深度嵌套路径（`s.a.b`）
   已由 §3.2 回退逐行，不在列式对拍范围**；**列表索引路径 `c.tags[i]` 属列式**
   （§3.2 增补）：对拍覆盖 JSON-数组列（null 元素先剔除再取下标、越界/null/
   非数组 → null、object/array 元素 → 确定 false 非 null）与原生
   List/LargeList/FixedSizeList 列（`extract_list_values` 同款 null 剔除）；
5. **类型不匹配（回退）与字段缺失（null，不回退）**：类型不匹配由订阅时 schema
   比对判定（§3.2），该 guard 回退逐行 interpreted；**字段缺失按 null 处理**
   （§3.1/§3.2），列式 null 不命中与 interpreted 读到 None 等价、不触发回退——
   两者结果均进对拍覆盖。

### 3.5 多规则共享与有状态路径适配

**E. 多规则共享列计划**（延续 #19 事件解析共享的列式形态）：
- **上游全批广播零拷贝**（与 §3.3 一致）：窗口 fanout 广播同一 `RecordBatch`
  引用（复用 `TimedBatch` 已存的 batch，不做 union 投影）——多订阅规则共享的是
  **全批列引用**；
- **规则侧各自投影**：各规则任务从全批按自身 `field_projection()` 投影 view，
  各自 `eval_guard_columnar` → 独立物化/状态机（投影是规则内零拷贝列索引数组，
  非上游共享层职责）；
- 现状 `route_commit` 已共享物化（events 一次），列式下共享从“物化 Event”细化
  为“共享全批列引用 + 各规则独立按需物化”。

**F. 有状态路径的列式适配**（状态机接口不变）：
- `RowSelection::Indices` → 按索引物化 Event（**只物化命中行**：Q2 命中 0.81%，
  99.19% 行零物化）；
- 物化行喂 `advance_at`（接口不变，逐事件）；
- **远期（L4）批状态访问**：`Indices` 内 key 去重 → 每 key 一次状态查找 →
  批量更新，仅作为独立优化，不阻塞 L1-L3。

---

## 4. 分层改造方案（基于 §3 组件落地）

### L0 已就位（不新做）

- 输入列式（IPC 解码）；
- 窗口批级存储（`TimedBatch`）；
- 输出列缓冲 + `reserve_rows`（C1 已实现，待 L3 收尾填充方式）。

### L1 guard/过滤列式化（Q2 专项，最先落地）

- **落地范围**：§3.1 `ColumnarBatch` + §3.2 `expr_is_columnar`/`eval_guard_columnar`
  的纯列运算子集 + §3.4 语义等价验证；
- **成本锚点**：guard 122ns/事件、字段提取 57ns/事件 → 列级求值（免 HashMap/
  Value 转换）预期 **<20ns/事件**（每事件省 ~110ns）；
- **类型映射**：按 §3.4 表，Int64 取模等算子按列原生类型实现；
- **覆盖率预期（需先静态统计）**：Q2 是理想子集。落地前先用 qradar 450 规则集
  统计「guard 为纯列运算」的比例——若 <50%，L1 收益在真实负载上打折；
- **验收**：`guard_bench` 改进前后对比（`cargo test --release -p wf-engine
  q2_guard -- --ignored --nocapture`）：guard 增量 **118.7 → <20ns/事件**，
  且 §3.4 对拍 100% 一致。

### L1-替代：Q2 语义简化（与列式 guard 并列评估）

- **思路**：Q2 语义 = `auction % 123 == 0` 过滤 + `count>=1` 且每次 fire 不
  reset——**等价于"过滤后每命中事件直接输出"**，不需要 match 状态机；
- **候选**：Q2 改走 `on each` 批处理路径（C2 each 批式向量化，已存在），
  guard 过滤 + 直接输出，省掉 state machine 与实例表全部成本；
- **对比**：比列式 guard 更简单（不碰表达式引擎，只换规则形态）；但依赖
  "count>=1 不 reset"语义的等价识别，需 checker 验证（若有 throttle/fail-rule
  等联动，语义不等价则排除）；
- **决策**：L1 与 L1-替代各做一次 Q2 端到端原型，**取收益/工程量更优者**。

### L2 事件物化延迟化

- **落地范围**：§3.1 `RowSelection` + §3.5-F 的按需物化——不再整批
  `batch_to_events`，列过滤/投影先行，`Indices` 命中行才物化 Event；
- **成本锚点**：`batch_to_events_with` 每行 new HashMap + 每字段提取转换
  insert（Q2 命中 0.81%，99% 行的物化是纯浪费）；
- **前置**：L1 掩码/索引直接复用；
- **验收**：`batch_to_events_ingest_throughput`（已有）对比；Q2 端到端
  310 → 目标 ~220ns/事件。

### L3 输出列式化收尾（P0-④）

- **落地范围**：`commit_each_row` 逐行填充 → 按 `Indices` 批量写列
  （一次 append 命中行列值，字符串走 batch-shared `Arc`）；
- **yield 表达式边界**：输出列式填充只对**常量 / 直接字段值**（`detail = b.auction`、
  `request_count = 1`）批量取列；**复杂 yield 表达式**（格式化/函数拼接，如
  `detail = concat(...)`）逐行 interpreted 后仍走列缓冲写入（`commit_each_row`
  保留为回退路径）；
- **成本锚点**：输出构建占规则计算 ~57%（二分法①）；
- **注意**：只省 CPU，非吞吐杠杆（二分法① 已证：预算放开前减输出无收益）；
  **必须在 L1/L2 之后做**——预算/物化先放开，输出减负才转化为吞吐；
- **验收**：`process_batch` 段计时对比。

### L4 有状态规则状态 SoA / 批处理（长期）

- **落地范围**：§3.5-F 的批状态访问——`Indices` 内 key 去重后单次状态查找、
  热 key 缓存；状态字段 SoA（同字段连续内存）；
- **成本锚点**：Q5/Q7 ~800ns/事件（规则密集路径）；
- **验收**：q5/q7 端到端提升 ≥20% 且 EMIT 逐位一致。

### L5 全列式执行引擎（终极，方向性）

- 表达式编译器按列计划执行（参考主流流引擎 vectorized execution）；
- 任意表达式、窗口读、join 全部列计划化（§3.2 双轨的"列式"一轨扩展到全部）；
- **工程量大，作为架构演进方向**，不排期——先以 L1-L3 验证收益。

---

## 5. 量化收益：每事件成本是真杠杆（per-event ns，非 EPS 上限）

> **关键 reframe**：列式执行改的是**每事件成本（per-event ns）**，不是 EPS 上限。
> EPS 由**车道数（并发扩展）与 P0-③ 窗口 actor 单写者（第二道墙）** Gate（见
> `concurrency-scaling.md` §2.3 两道墙模型）——§3 自己也承认 L3“只省 CPU，
> 非吞吐杠杆”。故本节以 **per-event ns delta** 为真杠杆，EPS 只作派生估计，
> 且固定车道配置、与 `concurrency-scaling.md` 同一口径。

### 5.1 真杠杆：per-event ns delta（L1-L3 实际改动的东西）

| 层 | per-event ns delta | 依据 |
|---|---|---|
| **L1 guard 列式** | **-118ns**（guard 增量 118.7 → <20ns/事件） | `guard_bench` 实测（122ns/事件，无 filter 基线 3.8ns） |
| **L2 物化延迟** | 物化成本 ×（1 − 命中率）（Q2 命中 0.81% → 省 ~99% 行物化） | `batch_to_events` 每行 new HashMap；只物化命中行 |
| **L3 输出列式** | 输出占规则计算 ~57% 的大半 | 二分法①（`commit_each_row` 逐行构建） |

> **这些 ns delta 才是 L1-L3 的直接产出**——不依赖任何 EPS 基线，也不受车道
> 配置影响；`guard_bench`/物化基准/`process_batch` 段计时可直接验收。

### 5.2 EPS 派生（固定车道配置，与 `concurrency-scaling.md` 同口径）

- **per-event ns 是真杠杆，EPS 是派生**：L1-L3 直接改的是每事件 CPU 成本（§5.1），
  EPS 由**车道数（并发扩展）与 P0-③ 窗口 actor 单写者（第二道墙）** Gate
  （`concurrency-scaling.md` §2.3 两道墙模型）——不随 L1-L3 直接翻倍；
- **正确因果链**：per-event ns ↓ → 同车道数下 CPU 预算更早释放 → **在 P0-③
  窗口 actor 墙之前，能塞下更多车道/更多规则前 CPU 先饱和** → 抬升可达 EPS
  天花板。列式**不推倒第二道墙**——它把“CPU 先饱和”的拐点右移，使窗口 actor
  单写者成为更靠后、更纯粹的限制，为后续 P0-③/W-WCP 破墙预留 CPU 余量；
- **基线口径（与 `concurrency-scaling.md` P0-② 同一份，不另起炉灶）**：当前
  最优配置（4 连接 / instances=4 / 2GB content 记账）下稳态 **q1 ≈ 7.58M、
  q2 ≈ 7.23M**（`concurrency-scaling.md` §6 决策日志 #7 的 P0-② 甜点）。L1-L3
  **不承诺在这些数字上翻倍**——上限由 P0-③ 窗口 actor 单写者决定。

| 查询 | L1-L3 可收割成本 | 主要收益层 |
|---|---|---|
| Q2（过滤） | guard 122ns + 字段提取 57ns + 整批物化（99% 行浪费） | L1 + L2 |
| Q1（无状态） | 物化 + 输出（占规则计算 ~57%） | L2 + L3 |
| Q5/Q7（状态） | 状态机 ~800ns 不可列式，仅输出可列式 | L3（有限） |

> **不承诺 EPS 具体倍数**：per-event ns delta（§5.1）是可直接验收的真杠杆；
> EPS 派生受 P0-③ 窗口 actor 墙与核数约束。收益以 **M2b 落地后 Q2 端到端实测**
> 为准——数据驱动，不承诺。

---

## 6. 正确性约束（列式不改变语义）

1. **有状态规则**：per-key 实例、fire/reset、窗口过期语义**完全不变**——
   列式只作用于无状态的过滤/投影/输出；
2. **保序**：`Indices` 保持批次内原始行序（有序收集）；窗口 per-source seq
   语义不受影响；
3. **null 语义**：列式过滤保留 Arrow null 语义（`is_null` 短路不命中，与
   `batch_to_events` 跳过 null 一致）——§3.4 对拍验证；
4. **数字语义**：列式按列原生类型运算，与现有 f64 的等价性按 §3.4 表验证
   （>2^53 整数/时间戳为已知语义差异——更准但可能改变比较结果，记录在案）；
5. **任意表达式**：`expr_is_columnar == false` 时回退逐行 interpreted
   （§3.2 双轨）——列式只快纯列运算，**不改变任何表达式结果**；
6. **join/窗口读**：维持现有逐行语义（L5 前不触碰）。

---

## 7. 风险与边界

| 风险 | 说明 | 缓解 |
|---|---|---|
| 列式改造侵入规则语义 | 状态/窗口/join 是语义核心 | 分层只碰无状态路径；每层 EMIT 逐位对照 + §3.4 对拍 |
| 表达式多样性 | 任意函数/嵌套 guard 无法列式 | §3.2 双轨：非纯列运算回退逐行；L1 覆盖率先静态统计 |
| 收益估计偏差 | 估算是外推；EPS 受窗口 actor 墙与核数约束 | M2a 在 2GB content 口径重测稳定基线；每层单元基准 + 端到端验收，数据驱动 |
| 状态路径不可列式 | Q5/Q7 是真实 SIEM 核心 | L4 SoA/批处理单独评估，不阻塞 L1-L3 |
| 双轨维护成本 | 同一 Expr 两套求值器 | `expr_is_columnar` 单一判定点 + 对拍测试锁等价 |
| 工程成本 | 全列式是重写 | L1 先行（一个 guard 子集），收益验证后再扩 |

---

## 8. 里程碑与验收（数据驱动）

| 里程碑 | 内容 | 验收 |
|---|---|---|
| **M1（当前）** | guard 单元基准落地 | `guard_bench` 基线记录：guard 118.7ns 增量、字段 56.7ns |
| **M2a** | **重测 Q2 稳定基线**（2GB content 口径，与 `concurrency-scaling.md` P0-② 同一份） | Q2 基线三测稳定（±5%）；同时统计 qradar 450 规则 guard 纯列运算覆盖率 |
| **M2b** | L1（§3.1+§3.2 guard 子集 + §3.4 对拍）或 L1-替代（语义简化）——原型对比取优 | guard 增量 **<20ns/事件**（`guard_bench` 为**单元口径**：直接调 `event_matches_alias`，不含 key 提取/lookup/窗口——增量度量正确、非 Q2 每事件全成本）；§3.4 对拍 100% 一致；Q2 端到端提升**以 per-event ns delta 折算**（消 ~118ns guard 增量，占比以 M2a 基线实测为准，不承诺固定 EPS 倍数）；EMIT 逐位一致 |
| **M3** | L2 物化延迟化（§3.5-F 按需物化） | `batch_to_events` 基准提升；Q1/Q2 共享管道成本降 |
| **M4** | L3 输出列式化（P0-④ 收尾） | 规则计算 CPU -50%+（输出占规则计算 ~57% 的大半）；端到端提升以 per-event ns 折算，不承诺固定 EPS |
| **M5（远期）** | L4 状态 SoA / L5 全列式 | 独立立项 |

---

## 9. 关联

- 单元基准：`wf-engine/src/match_engine/tests/guard_bench.rs`（Q2 guard 路径，
  release 跑）；`tests/perf.rs`（物化/吞吐 loose checks）；
- 现有列式基础：`alert/column_batch.rs`（C1 输出列缓冲）、
  `hot-path-vectorization-design.md`（C1/C2）、`event_bridge.rs`
  （`batch_to_events` 物化点）；
- 二分法结论：`concurrency-scaling.md` §2.3（输出占规则 57%、预算第一道墙）；
- Q2 关键路径分析：guard 评估是 Q1/Q2 差距根源（本会话 M1 实测）。
