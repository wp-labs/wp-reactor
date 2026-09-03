# P4 规划：抽取纯语义 crate `wf-cep`

> 状态：方案评审稿（2026-09-03）· 决策 D1 定名 `wf-cep` · 未开工
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
