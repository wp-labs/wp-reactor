# P4 规划：抽取纯语义 crate `wf-cep`

> 状态：方案评审稿（2026-09-03）· 决策 D1 定名 `wf-cep` · v0.1/v0.2 已交付（片 1-3）· 片 4-6 立项决定见文末（2026-09-04 v0.3）
> 背景：结构性待办 P4——把不依赖 tokio/arrow 的纯语义逻辑从 `wf-engine`(118k)
> 单 crate 中抽出，立编译器强制依赖墙，缩短语义测试迭代，为差分/性质测试铺路。

## 1. 边界取证（实测）

| 区域 | 规模 | tokio/arrow | 判定 |
|---|---|---|---|
| `match_engine/cep/` 生产 | 7.5k | 仅 `join_then_key.rs` 带 `arrow::RecordBatch` | 迁出主体 |
| `cep/` 自带测试 | 4.2k | 少量 tokio::test | 随迁 |
| `executor/`(43k) / `window/`(13.5k) | — | 10 文件 arrow / actor 58×tokio | 留引擎 |
| `wf-lang` | 48.7k | 纯 | 作唯一依赖 |

engine 内引用 cep 路径 181 行，绝大多数在 `match_engine/tests` 语义套件。

## 2. 决策记录

- **D1 命名**：`wf-cep`（纯语义：CEP 状态机 + eval + Value/Event + key/scope/limits）。
- **D2 泄漏处置**：`GuardMasks`（纯位集）下沉 wf-cep；`join_then_key` 批级函数上移
  engine 侧（`advance_at_with_masks_key` 已支持外部预解析 `key_override`）。
- **D3 测试归属**：语义套件随语义走。分类见 §4。
- **D4 墙的强制**：wf-cep 的 Cargo.toml 不引入 arrow/tokio；CI 校验依赖树防回潮。

## 3. 目标依赖方向

```text
wf-lang
  ↑
wf-cep (新：cep 状态机/eval/Value/Event/key/scope/limits + GuardMasks + testkit)
  ↑
wf-engine (executor/columnar/window/alert/sink + 门面 `match_engine::{…}` 改从 wf-cep 重导出)
  ↑
wf-runtime
```

兼容：engine 门面重导出不变 → wf-runtime / warp-fusion 现有 import 零改动。
wf-cep 提供 `#[doc(hidden)] pub mod testkit`：eval_expr 等内部求值入口 + 纯 plan
builder helpers（供 engine 剩测/bench 使用；避免重复实现）。

## 4. 测试归属分类（S2 迁移清单底稿，2026-09-03 引用面扫描）

- **随迁 wf-cep**（语义/内部访问）：
  - `cep/tests/*`（4.2k，coverage_*/derived_key/first_match_time）
  - `tests/l2/`（baseline/expr/guards 引用内部 `eval_expr*`；全目录语言一致性）
  - `tests/l3/`（conv 语义；`conv.rs` 现仅走公开面，目录级随迁保持一致）
  - `tests/accu.rs` / `any_l2.rs` / `close.rs` / `seq_l2.rs` / `seq_order.rs` /
    `core_coverage.rs`（无 executor 依赖确认后随迁）
- **留引擎**：`executor/*`、`columnar_*`、`event_bridge_*`、`deferred_*`、
  `spill_*`、`stats_*`、`*_bench.rs`（列式/编排集成；内部 eval 走 testkit）、
  `contract*`（已在 crate 级 tests/）
- **移前必查**：l2/l3/accu/seq/close/core_coverage 是否 import executor/columnar
  engine 内部（如有则拆分或留引擎），以最终清单为准（S2 第一步产出）。

## 5. 迁移步骤与门禁

| 步 | 内容 | 门禁 |
|---|---|---|
| S0 | 本方案评审定稿 | review |
| S1 | 建 wf-cep skeleton；cep 生产+cep/tests 整迁；`mod cep`→依赖重导出；GuardMasks/join_then_key 处置；testkit（eval + helpers） | workspace check + wf-cep 测试 + 引擎剩测全绿 |
| S2 | 语义套件按 §4 归位（先出最终清单） | 全量测试 + llvm-cov 覆盖对比 |
| S3 | CI 依赖墙校验（wf-cep 无 arrow/tokio）；unreachable_pub 门禁自动覆盖新 crate；bench q1/q5/q13 对照 | CI 绿 + 性能数字对照 |
| S4 | CHANGELOG/版本（2.1.0 候选）→ tag → warp-fusion 升依赖验证 | 跨仓库绿 |

## 6. 风险与收益

- 风险：引擎集成测试对 cep pub(crate) 访问点（已收敛到 eval 入口）→ testkit 承接；
  搬移过程测试丢失 → llvm-cov 对比把关。
- 收益：`cargo test -p wf-cep` 独立快速迭代；语义层与 IO/列式隔离（依赖墙强制）；
  摆脱 118k crate 编译与异步测试偶发干扰；为差分/proptest（P0-3）与二期拆
  executor 列式后端铺路。
- 工作量：2–4 个工作日，主体在 S2 测试归位。

---

## 变更记录

- 2026-09-03 · 边界勘误：cep 类型层与引擎列式表示（TriggerEvent/JoinRow/RowFields/
  ColumnarEvent）深度交织（双表示设计），「无 arrow 纯化」前提不成立；大搬家
  方案搁置。
- 2026-09-03 · **v0.1 落地（纯叶收敛）**：新建 `crates/wf-cep`，收纳
  `time` / `regex_cache` / `cidr_cache` / `error` 四个纯叶模块（≈300 行 +
  测试）；engine 以 shim 模块重导出，公开路径零变化；CI 增加 wf-cep 依赖墙
  （禁 tokio/arrow）。后续 P4 扩展（eval/Value 等）以本 crate 为落点，需先解决
  列式表示抽象或接受「同步内核（arrow 数据面允许）」重定界。

---

## 决策 A（2026-09-03）：墙修订 + 同步执行核大拆

- 边界勘误：CEP 语义类型按设计直接操作 Arrow 行（ColumnarEvent 借 &RecordBatch、
  TriggerEvent 持 Arc<RecordBatch>），且孤儿规则令 Event/FieldSource/Value 必须
  同 crate → 「无 arrow 纯化」不可能，纯叶（v0.1）是例外不是常态。
- **墙修订**：wf-cep 允许 arrow 纯数据面；禁止 tokio / async / 网络 / 持久化 IO
  （CI 墙步已改）。
- **目标**：wf-cep 收编 match_engine 同步执行核（cep + event_bridge + columnar +
  executor + alert + external + contract + 已迁纯叶）；engine 留 window /
  sink / async_persist / spill / 门面 shim（公开路径零变化，engine 内部引用经
  shim 模块保持）。
- **切片积压（自底向上，每片绿闸推进）**：
  1. ✅ 纯叶：time/regex_cache/cidr_cache/error（v0.1）
  2. ✅ Value 层：Value/EngineHashMap/EngineHashSet/MACHINE_ID -> wf_cep::value
     （Event/FieldSource 留引擎避孤儿；types.rs 别名重导出）；external -> wf_cep
     （engine pub mod shim）
  3. ✅ RowFields/RowFieldLayout/RowFieldSlot -> wf_cep::rows（arrow 数据面；
     stats_exec 删除区 + wf_cep::rows import；executor/mod.rs 重导出改源；
     spill 访问器升 pub(doc hidden)；RowFields::f64_at 所需 value_to_f64 随迁）
  4. ⏳ cep（types 引擎耦合段切留 engine）+ event_bridge（FieldSource 随 Event 迁）
  5. ⏳ executor/columnar（解 alert/spill/window::scope_key_columnar 后）
  6. ⏳ engine 收尾：window/sink/async_persist/spill + 门面收缩；测试归位
- 每片完成 = wf-cep 编译 + engine 编译 + 全量测试 + 两道 clippy 门禁。

---

## 收口决定（2026-09-03）：P4 停于片 3，wf-cep v0.2 交付

- **不可约层判定**：片 4 起（Event/FieldSource/ScopeKey/key/eval/WindowLookup/
  event_bridge）依赖闭包闭环——孤儿规则（Event 与 FieldSource 同 crate）、
  FieldSource 默认方法 → key.rs → eval_expr_ext → WindowLookup → JoinRow/
  TriggerEvent（Arc<RecordBatch>）互相咬合，最小可搬集 ≈ 12-15k 行 +
  types.rs 切分 = 独立批次（1-2 天），非「片」级改动。
- **已交付（wf-cep v0.2）**：
  - 纯叶：time / regex_cache / cidr_cache / error
  - value：Value / EngineHashMap / EngineHashSet / MACHINE_ID
  - external（全局 OnceLock 单例随迁）
  - rows：RowFieldSlot / RowFieldLayout / RowFields（arrow 数据面）
  - 依赖墙：禁 tokio/async/网络/持久化 IO，arrow 数据面允许（CI 校验）
  - engine 全部经 shim/别名重导出，公开路径零变化；测试总量对齐
- **片 4-6（搁置，需独立排期）**：cep types 切分 + event_bridge/key/eval 下沉
  同步执行核；若未来要「语义层独立秒测」，前提是**列式行表示抽象化设计**
  （泛型/擦除 ColumnarEvent 句柄），应单独立项评审后再动。

---

## 立项决定（2026-09-04，v0.3）：片 4-6 以「直接搬迁」立项（X），列式抽象化（Y）降级

- **立项 X**：片 4-6 同步执行核**直接搬迁** wf-cep——cep 11.8k（2026-09-04 实测
  11,829 含 eval/tests）+ event_bridge 族 1.5k（含 views/tests）+ types.rs 541 行切分；
  **接受 arrow 数据面**（决策 A 墙内），不引入列式行表示抽象。
- **降级 Y**：列式行表示抽象化（泛型/擦除 ColumnarEvent 句柄）**不做**；仅当出现
  第二行表示消费方，或 P0-3 差分/proptest 大规模铺开后 arrow 编译成实际瓶颈时
  重新立项（届时在 X 地基上接口面已清晰，评估成本大幅下降）。

### 立项论据（2026-09-04 评审：迭代/性能 + 可维护/可读两维度）

1. **边界锁死**：30+ 刀文件级拆分后 cep/event_bridge/columnar 边界已现成（cep 目录
   即新 crate 骨架），但全是 `pub(crate)` 纸面边界——同一 crate 内靠纪律不靠编译
   器，必回潮；crate 墙（孤儿规则 + 依赖方向 + 可见性）是唯一能兑现那 30 刀投入
   的方式。
2. **测试信号纯化**：语义测试（l2/l3/accu/seq/close/core_coverage，数百个）现与
   columnar/executor/spill 混居 wf-engine 116.7k 行单体，且 cep 自带测试被迫
   `tokio::test`、受 async 偶发 flaky 污染回归信号（async_persist 案例）；X 后
   `cargo test -p wf-cep` = 纯语义信号，定位面砍半。
3. **叶子化**：wf-cep 只依赖 wf-lang，内部改动不触碰 engine；语义迭代/proptest
   落点独立。实测增量 `-p wf-engine --lib` 1334 测试 2.35s 非瓶颈（不构成 Y 的
   「秒测」理由）；wf-runtime 30s 固定 sleep 集成测试为真瓶颈，两案均不影响。
4. **代价对比**：X 的代价 = 一次定型的 shim 胶水面（片 6 门面收缩可控）；Y 的代价 =
   概念税（「双表示」复杂度搬家而非消除）+ 跨 crate trait 永久接口负债 + 快路径
   类型体操，单一消费方下属 YAGNI。

### 立项条款（交付标准，防长期债）

- **条款 1（shim 最小化）**：片 6 收尾门面收缩，`match_engine::{…}` 改从 wf-cep
  重导出，engine shim/转发面压到最小、不留长期转发债（crate 级一次定型，对齐坑
  #12/#24 的 re-export 三约束经验）。
- **条款 2（类型归属自然度 = S1 评审门）**：cep/types.rs 541 行切分若出现
  Event/FieldSource 等类型被迫留 engine 的不自然归属（孤儿/双向 impl 咬合），说明
  墙的画法不对——回头重审边界（含局部 Y 抽象），不留「类型放错层」的长期别扭布局。

### 排期（沿用决策 A 切片节奏，每片绿闸推进）

| 步 | 内容 | 门禁 |
|---|---|---|
| S1 | cep/types.rs 切分方案 + cep 11.8k + event_bridge 1.5k 整迁 + testkit（engine 剩测对 cep pub(crate) 访问点收敛） | wf-cep 编译 + engine 剩测全绿 |
| S2 | 测试归位最终清单（l2/l3/accu/seq/close/core_coverage 对 executor/columnar import 扫描后定，P4 §4「移前必查」） | 全量测试 + llvm-cov 覆盖对比 |
| S3 | CI 墙校验（已有）+ bench q1/q5/q13 对照 | CI 绿 + 性能数字对照 |
| S4 | CHANGELOG 2.1.0 候选 → tag → warp-fusion 升依赖跨仓验证 | 跨仓库绿 |

- 工作量重估：S1 主体 1-2 天（沿用 2026-09-03 判定）；S2 0.5-1 天（以最终清单为准）。
- **S1 边界确认已完成（2026-09-04，见 `p4-wf-cep-s1-boundary.md`）**：阻塞点收敛为 B1-B4 四个
  arrow 数据面类型（GuardMasks/JoinRow/TriggerEvent/ColumnarEvent）的归属决策，推荐 B0 批次
  随迁；cep 生产实测 7.6k 行（旧 11.8k 口径含测试）；批次重排 B0-B3 ≈ 2-3 天。

---

## 执行记录（2026-09-04，v0.4）：B0 拆为数据面小刀推进，B0-1/B0-2 已绿闸

- `8e7094e` **B0-1**：GuardMasks 下沉 `wf_cep::masks`（columnar.rs:189 → wf-cep，engine
  columnar.rs `pub use` shim 保路径；零依赖自包含，走通跨 crate 搬迁机制）。
- `08d1eaa` **B0-2**：值提取核心下沉 `wf_cep::value_extract`（event_bridge.rs 的
  extract_field_value 族 + WFL_FIELD_TYPE_* 常量 + wfl_structured_field_kind + serde_json
  依赖入 wf-cep；event_bridge.rs shim re-export，31 个消费文件零改动）。
- 门禁：每刀 wf-engine 1334+73 / wf-runtime 606+15 / clippy×2 0 / fmt 0——测试数完全守恒。
- **批次修正（孤儿规则实证）**：S1 §6 的 B0「载具 + 其 FieldSource impl 下沉」在孤儿规则下
  不可独立完成——FieldSource 仍在 engine（cep/types.rs），impl 若留 engine 会因跨 crate
  私有字段访问失败（E0616），若随载具迁 wf-cep 则 extract_scope_key 回退依赖
  extract_scope_key_from_row（cep/key.rs，B1 才迁）。→ **B0-3 三载具下沉并入 B1**（cep
  同步核整迁时与 FieldSource/types.rs 同批搬，孤儿自然消解）；B0 剩余可选小刀 =
  scope_key_from_column 归位（依赖 ScopeKey，同样等 B1）。

---

## 执行记录（2026-09-04，v0.5）：B1（cep 整迁）+ B2（测试归位）已绿闸

- `940a405` **B1**：cep 同步执行核 16 生产文件 + cep/tests 140 测试整迁 wf_cep::cep；
  B0-3 载具并入（ColumnarEvent/JoinRow/TriggerEvent/FieldIndex + scope_key 族 →
  wf_cep::row_views）；engine 全 shim 保路径（99 引用文件零改动）；孤儿消解；
  cep→wf_config 反向耦合以常量上移 wf_lang 根解决（DEFAULT_OUTPUT_TIME_FORMAT）。
- `11a76bb` **B2**：语义套件归位 wf_cep::sem_tests——G2 顶层（accu/any_l2/close/seq_l2/
  seq_order/cep_core）+ join_key + l2 语义子集（baseline/expr 族/fixed/guards/keymap/limits）
  + l3 整组 + eval_coverage 纯 eval 段，共 **201 测试随迁**（wf-cep 测试 140→341）；
  helpers/l2-harness 双份（engine 侧保完整副本 + allow(dead_code)）；eval_coverage 拆分
  （RuleExecutor/L3 yield 段留 engine）；core_coverage 整组/regression/executor/bench/
  columnar/event_bridge 测试留 engine（S1 判定）。
- **测试分布（守恒）**：wf-cep 341（cep 140 + sem_tests 201）/ wf-engine 1002+73 /
  wf-runtime 606+15 / wf-lang 1051；clippy×2 = 0、fmt = 0。语义层独立测试
  `cargo test -p wf-cep` = 341 passed 0.01s——「语义改动不触发 engine 编译」兑现。
- **剩余**：B3 收尾（CHANGELOG 2.1.0 → tag → warp-fusion 升依赖跨仓验证 + CI 墙确认）。
