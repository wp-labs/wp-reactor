# S1 边界确认：片 4-6 直接搬迁的实测边界（2026-09-04）

> 目的：验证 v0.3 立项 X（cep 同步执行核直接搬迁 wf-cep）的边界假设，产出 S1 三大清单
> （cep→engine 依赖对表 / engine→cep 访问面 / 测试归位），修订 2026-09-03 收口决定对
> 「不可约层 ≈ 12-15k 行」的判定。方法：三路并行只读扫描（grep -n 取证 + 定义处回读）。

## 1. 规模修正（wc 实测）

| 区 | 行数 | 备注 |
|---|---|---|
| cep 生产（16 文件：mod/types/advance/window/expiry/close/conv/join_then_key/key/limits/seq/state/step + eval/{mod,cmp,funcs}） | **7,608** | 旧口径 11.8k = 含测试 |
| cep/tests（6 文件） | 5,868 | — |
| event_bridge 族（event_bridge.rs/views/tests） | 1,554 | 载具段在其中 |

→ 直接搬迁主体 ≈ 生产 7.6k + views 载具段 + 随迁测试（§5），**小于旧判定**；收口决定
「12-15k」高估了纯生产面（其口径混入测试与 event_bridge 全族）。

## 2. 依赖闭包实测（对收口决定的修正）

**形态修正**：闭包真实存在，但不是「语义逻辑 → 引擎函数」的调用环，而是**双向契约环**：
cep 定义 trait（`FieldSource`/`WindowLookup`）→ engine 以引擎数据载具实现 trait
（event_bridge_views.rs:175/420 等）→ 载具类型（`JoinRow`/`TriggerEvent`/`ColumnarEvent`）
又被 cep 的 trait/enum/struct 签名**反向引用**（types.rs:7 import 的消费点：L195
`MatchedContext.trigger_event`、L306/312/321/341 `WindowLookup` 方法签名、L384
`AsofLookup::Hit(JoinRow)`）。

**环②过度声称修正**：`FieldSource::extract_scope_key` 默认路径 → `extract_scope_key_from_row`
（key.rs:391）是纯字段提取，**不触 eval**；进入 eval 的只有 `extract_scope_key_mixed`
（key.rs:408，key_exprs 混合键分支）。

**干净面（重要）**：cep 语义逻辑内部（eval/key/step/close/seq/advance/window）对引擎
**零调用**（spill / async_persist / contract / alert / executor 运行时类型 / engine 顶层
window 全部零引用）；15 处跨模块编译引用中 A 类 6 处（RowFields/EngineHashSet/time/
regex_cache/cidr_cache）真身在 wf-cep、只改路径。

## 3. 迁出障碍点（B1-B4）与推荐

| # | 引擎类型 | 定义处 | cep 使用形态 | 推荐 |
|---|---|---|---|---|
| B1 | `GuardMasks` | columnar.rs:189（字段 = 3× `EngineHashMap<(usize,usize), BooleanArray>`） | 5 文件参数下传 + 3 查表点（step.rs:62 event_value / close.rs:56 close_value / seq.rs:116 neg_value），**只读不构造** | **随迁 wf-cep**：纯 arrow 数据面（BooleanArray + cep::EngineHashMap），仿 wf_cep::rows 先例 |
| B2 | `TriggerEvent` | event_bridge_views.rs:376（enum：Arc<RecordBatch> 列式 / Event(Arc<Event>)） | types.rs:195 字段类型；advance/window API 参数；window.rs:459/684/755 **cep 直接构造** | **随迁 wf-cep**：本体 = cep::Event + Arc<RecordBatch> + 索引，arrow 数据面内 |
| B3 | `JoinRow` | event_bridge_views.rs:305 | WindowLookup 4 方法签名 + AsofLookup::Hit —— 最硬断点 | **随迁 wf-cep**（同 B2） |
| B4 | `ColumnarEvent` | event_bridge_views.rs:79（&RecordBatch 免物化视图） | join_then_key.rs:48 单点列读（batch 级 join-then-key 预解析） | **随迁 wf-cep**；`value_at` 的 null/结构化语义需与 batch_to_events 字节一致（随迁时对照） |

**推荐路线**：B1-B4 作为「数据面载具下沉」批次一起随迁 → 孤儿约束全消（Event/
FieldSource/ColumnarEvent/TriggerEvent/JoinRow 同 crate），engine 侧 event_bridge 留
`pub use wf_cep::…` shim 保公开路径。**不取 trait 泛型化路线**（WindowLookup 行抽象：
动签名面大，单一消费方 = YAGNI，呼应 v0.3 对 Y 的降级）。engine 内剩余 `impl FieldSource
for DeferredLeft`（deferred_exec.rs:41，本地类型 impl 外部 trait）合法保留。

## 4. 可见性改动清单（engine→cep 访问面）

**生产代码 13 项现 pub(crate) 必须提 pub**（wf-cep 普通构建被 engine 消费，cfg(test)
不可依赖）：`eval_expr`、`eval_expr_ext`、`eval_field_value`、`eval_field_value_src`、
`value_to_string`、`extract_scope_key_from_row`、`extract_event_str`（CepStateMachine 关联
fn）、`push_i64_exact_decimal`、`key::StrSink`（trait）、`eval::cmp::apply_fmt_template`、
`eval::cmp::timestamp_nanos_to_utc`。
→ **private_interfaces 连锁**：`eval_expr_ext` 签名含 `&mut EngineHashMap<String,
RollingStats>` → RollingStats 提 pub；`push_i64_exact_decimal` 形参 `&mut impl StrSink` →
同批提 pub。

**testkit 承接（仅剩测消费，可 `#[doc(hidden)] pub mod testkit`）**：`ValueKey`、
`StepState`、`RollingStats`（或直接提 pub doc-hidden）、`accumulate_close_steps`（签名含
GuardMasks——B1 随迁后解锁）、`extract_key_simple`、`scope_key_from_values`、
`scope_key_shard_index`。

**engine shim 保路径**：`match_engine::cep` 兼容 shim（照 regex_cache 先例：
`pub(crate) use wf_cep::…::*`）→ 剩测/剩码 pub 项引用零改动；cep/mod.rs:32-42 re-export
机械改写为 `pub use wf_cep::…`。反向垫片消除：cep 迁出后 regex_cache/cidr_cache/time
（engine 内均 pub(crate) shim）直连 wf_cep，不再经 engine 中转。

## 5. 测试归位三分类（35 候选 + helpers）

| 分类 | 数量 | 明细 |
|---|---|---|
| **整文件随迁** | 21 | cep/tests：mod、coverage_more、first_match_time；tests/*：accu、any_l2、close、seq_l2、seq_order、cep_core；l2：baseline、expr 族（expr+expr_control+expr_time+expr_funcs）、fixed、keymap、limits；l3 整组（mod+conv+hop+session） |
| **需拆分** | 7 | cep/tests coverage_extra/r4/derived_key（GuardMasks/JoinRow/ColumnarEvent 段）、eval_coverage（RuleExecutor 段 537-699）、join_key（TestLookup harness）、l2/mod.rs（RuleExecutor import）+ l2/guards.rs（随 harness 决策） |
| **留 engine** | 7+1 组 | tests/executor/ 17 文件（~8k）；core_coverage 整组 5 文件 2730；columnar_fieldview/close_seq/wiring；deferred_join；l2 execute/joins；regression/ 5 文件（语义纯但作引擎回归桶） |
| **公共设施** | 1 | tests/helpers.rs（纯 wf_lang + cep::Event/Value）——引擎侧 regression/executor/bench 全依赖，**两侧都要**：wf-cep 测试树落同源副本 or 提为 wf_cep 公共 util |

**P4 §4 草案修订点**：① core_coverage.rs 移出随迁候选（整组深依赖 executor/event_bridge/
contract，Agent 实测文件头自述四主题跨界）；② l2 非整组可迁（execute/joins 依赖
RuleExecutor）；③ helpers.rs 互斥依赖单列。

**与 B1-B4 的联动**：若载具随迁（§3 推荐），cep/tests coverage_extra（GuardMasks 段
987-1133 + JoinRow 段 1186-1235）、coverage_r4（KeyJoinLookup JoinRow 物化）、derived_key
（ColumnarEvent 段 303-367）、join_key（TestLookup）的引擎面**随载具一并解除** → 「需拆分」
7 可降至 ~3（eval_coverage、l2/mod.rs、guards 视 harness 决定）。

## 6. 批次重排与工作量重估（修订 S1-S4）

| 批 | 内容 | 门禁 | 估时 |
|---|---|---|---|
| **B0 载具下沉** | GuardMasks + JoinRow/TriggerEvent/ColumnarEvent + 其 FieldSource impl + value_at 语义对照 → wf-cep；event_bridge shim pub use | wf-cep 编译 + engine 全绿 + clippy×2 | 0.5 天 |
| **B1 cep 生产整迁** | cep 7.6k 整迁 + §4 可见性 13 项提 pub + testkit + engine cep shim | 同上 + 全量测试守恒 | 0.5-1 天 |
| **B2 测试归位** | §5 随迁组 + 拆分点落位 + helpers 共享方案 | 全量测试 + llvm-cov 对比 | 0.5-1 天 |
| **B3 收尾** | CI 墙校验确认 + bench q1/q5/q13 对照 + CHANGELOG 2.1.0 → tag → warp-fusion 验证 | 跨仓绿 | 0.5 天 |

**合计 ≈ 2-3 天**，与 v0.3「S1 1-2 天 + S2 0.5-1 天」判定同量级；最大不确定项是 B0 的
`value_at`/guard 语义对照与 B2 的 helpers 共享方案（建议 helpers 提为 wf-cep 常编公共
模块 + engine shim，一劳永逸，仿 error/external 先例）。

## 7. 一句话结论

片 4-6 的直接搬迁**比收口决定判定的更可行**：阻塞不在模块拓扑（引擎运行时面零引用），
而在 event_bridge_views 三载具 + columnar::GuardMasks 四个纯 arrow 数据面类型的归属
决策——推荐作为 B0 批次随迁（非抽象化），其后 cep 生产 7.6k 即可整体平移、孤儿约束消解、
engine 剩测经 shim + 13 项提 pub + testkit 全覆盖。
