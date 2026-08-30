# MoJu Draft Review — wp-reactor

基于 `moju-code extract`（0.1.9）对 `crates/` 四个 crate 提取的事实，全新反向建模为 MoJu 2.0 草稿。
`moju verify moju/draft` 目标为全部通过。

---

## 高置信度事实（建议直接接受）

- **domain 划分**：按当前 crate 边界映射 —— `lang`(wf-lang)、`engine`(wf-engine)、`config`(wf-config)、`runtime`(wf-runtime)。与代码模块天然对齐。
- **类型字段**：全部取自 `facts.json`（来自真实源码），字段名/类型为代码事实。
- **state（枚举）**：变体名逐字取自 facts。
- **新概念覆盖**：columnar SoA（ColumnarBatch/StatsBucketAccs/SoAColGroup）、spill 落盘（SpillStore/RedbSpillStore/AsyncPersister）、window actor（WindowMailbox/WindowMsg）、seq/reduce/let/stats 语言子句、conv_stage/stats_task 运行时任务、perf_diag 均已建模。

## 推断/需人工审查项

| 项目 | 推断依据 | 风险 |
|------|---------|------|
| `lang: ExprPlan` 变体 | 不在 facts type_defs，变体取自旧模型（Const/Field/Binary/FuncCall） | 中 —— 需对照 `src/plan.rs` 确认 |
| `ResolvedSinkSpec`/`WireFormat`（外部依赖类型） | 来自 wp-connector-api / wp-core-connectors，非 workspace 源码；已折叠为 `String`（与 engine `SinkRuntime.spec: String` 一致） | 已解决 —— 外部契约，建议后续以 interface 建模 |
| `runtime: Admin actor / RunCommand 等 command` | 设计性触发消息，代码中经 CLI 隐含 | 低 —— 与旧模型一致 |
| runtime 层 service/subsystem/topology | 单进程 daemon 部署形态 | 低 —— 部署形态推断 |
| flow 步骤划分（ProcessEvents/LoadConfig/ReactorStart） | 从代码数据管道概括 | 低 —— 步骤为概念级 |

## 代码有但模型排除（实现细节，不建议建模）

- 测试/bench 辅助类型、解析中间类型、执行内部上下文、tracing/memory_probe/parse_pool 内部。
- `wf-data`：仅 `parse_json_timestamp_nanos` 等纯函数，无 struct/enum，未建域。

## 建议的下一步

1. 人工审查 `ExprPlan`、`ResolvedSinkSpec` 两处引用完整性。
2. `moju verify moju/draft` 通过后，逐域校对 module owns 与 dependency_rule。
3. 提升到 `moju/model/`，运行 `moju-code diff` 评估与代码注解的一致性（当前代码无 `#[moju]` 注解，diff 会全量报告"模型有、代码无注解"，属预期）。
