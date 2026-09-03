# 结构性重构：任务进展交接（2026-09-03 v3 快照 — H 拆分 + 行循环基准落地后）

> 本文件记录 wp-reactor/warp-fusion 结构性重构进行中状态，供新 session 直接续接。
> 配套方案文档：`docs/design/p4-wf-cep-plan.md`；发布条目：`CHANGELOG.md [2.0.17]`。

## 0. 仓库与分支状态（2026-09-03，H 拆分两笔已提交、未 push）

- **wp-reactor @ main = `0011d4f`**，**领先 origin/main 3 笔未 push**：`e9d020a` docs 交接 v2 快照 · `b201c19` refactor: process_batch 行循环 H-1..H-5 拆分 · `0011d4f` test: row_loop 行循环 release 基准。origin/main 仍停 `af58c2c`（v2.0.17）。工作树干净。
- **tags `v2.0.16` + `v2.0.17` 均已推 origin**。工作树干净（moju 产物已提交）。
- **warp-fusion @ alpha**：仍为**本地 path 依赖**（`@gxl:block(local_reactor)`，git tag 块注释停 v2.0.15）；工作树 `M Cargo.toml` + `M Cargo.lock` **未提交**（会破坏他人构建）。**下一步：切 `tag = "v2.0.17"` 并定本地块去留**（需用户决策）。
- wf-skills 已同步。

## 1. 验证基线 / 发布门禁（v2.0.17 发布前全跑过）

| 门禁 | 命令 | 结果 |
|---|---|---|
| workspace 全量 | `cargo test --all-features -- --test-threads=1` | wf-lang 1051 / wf-engine 1338 / wf-runtime 606 + 各 crate 全绿 |
| clippy 1 | `cargo clippy --all-targets --all-features -- -D warnings` | 0 |
| clippy 2 | `cargo clippy --lib --workspace --all-features -- -D warnings -W unreachable_pub` | 0（StatsBucket 修复后） |
| wf-cep 依赖墙 | `cargo tree -p wf-cep \| grep tokio` | 无（允许 arrow 数据面） |
| 跨仓 | warp-fusion（本地 path 依赖）`cargo test --workspace` | 364 全绿（含 oracle 对拍 e2e） |
| fmt | `cargo fmt --all -- --check` | **全仓红 = 存量 rustfmt 版本漂移**（30+ 文件，无一本会话改动文件）；勿整仓 fmt，只 fmt 改动文件 |

## 2. 已完成：#1 rule_task 拆件（S1 ✅ S2 ✅ S3 4 刀 ✅ H 主行循环拆分 ✅）

- S1 目录化 `2fca9ed`；S2：`b9a1338` debug.rs、`e7af387` stager.rs（PipeBatchStager 全家）。
- S3（process_batch ~1300 行内抽）：`56da04c` columnar_each 快路径 → `process_batch_columnar_each`；`b3e3811` DeferredRows 构造 → `build_deferred_rows`；`0a7ba41` `PendingAlertColumns::builder_for` 收口 emit 路径 8 处同构 get-or-create（−133 行）；`98dd085` 诊断块外抽 `log_batch_start`（&self）/`log_batch_summary`（&mut self，含 dump_profiling）。
- process_batch 主体 ~1300 → ~1050 行。
- H 主行循环拆分（`b201c19`，五刀逐刀 wf-runtime 606 + clippy 0）：process_batch ~1085 → **503 行**；每行 = event_nanos → scan → row-source → advance → close/match emit。
  - H-1 双路分发上提：machine / on-each 行拆两个独立循环（`self.machine` 批内恒定）；machine 行体逐行 let-else 重借。
  - H-2 on-each 行循环 → `process_batch_each_rows`（250 行；lookup/rule_name 方法内重建，each_direct_rows 向量化收口移入）。
  - H-3 machine 行循环 → `process_batch_machine_rows`（291 行）+ `MachineRowsCtx`（纯 Copy 批级上下文，方法内解构同名绑定复用行体）。
  - H-4/H-5 advance / scan-close 相位 → 同步自由函数 `advance_machine_row_aliases`（184 行）/ `scan_expired_and_route_closes`（39 行）——machine 行内最后的 &mut 使用点让渡；release 全内联（nm 无独立符号）。
  - 行内 emit 相位（含 &self async + lookup）刻意保留（拆法见 §4.1）。
- 行循环级回归基准（`0011d4f`）：`cargo test --release -p wf-runtime row_loop -- --ignored --nocapture`——machine-eager / machine-deferred 列式 / on-each 三路 × ~1M 行直驱 `process_batch`（无 sleep）；`rule_task_r4` harness（Spec/make_task/machine_rule/run_with_dispatch 等 24 项）提 `pub(super)` 复用。

## 3. 已完成：#3 stats_exec.rs 拆件（4162 行 → 6 文件）

- `a491018` 目录化；`f218947` 拆 accum.rs/state.rs（含混合 doc 归属重组、`super::eval::` 相对路径改绝对）；`f942040` 拆 exec.rs/eval.rs 收口；`17377f7` StatsBucket root re-export 降 pub(crate) 对齐 unreachable_pub。
- 终态：mod.rs 35 行（re-export 面），masks 137 / accum 388 / state 1182 / exec 1376 / eval 1136；executor/mod.rs 转发与测试深路径**零改动**。

## 4. 待办积压（按优先级）

1. **#1 H 尾 — machine 行内 emit 相位（~90 行）**：close/match emit 块仍内联在 `process_batch_machine_rows`（含 `&self` async `stage_or_emit_record` + `lookup`）。拆法 A = 每行 &mut self 方法 → 逐行 async future（热点不取）；拆法 B = 按 ALERT_BATCH_SIZE 批外收口 emit 节奏（需改 emit 时序 + 对拍）。当前评估：维持现状，勿为拆而拆。
2. **可选拆件候选**（沿用 stats_exec 流程，但先与 P4 立项对表避免拆了又搬）：`match_engine/columnar.rs` 3684L、`executor/each_exec.rs` 3200L、`lifecycle/spawn.rs` 2007L、`lifecycle/compile.rs` 1876L、`perf_diag.rs` 1938L、`checker/types/check_funcs.rs` 1965L。
3. **P4 片4-6**（cep/event_bridge/key/eval 同步执行核下沉 wf-cep）：搁置；前置 = 「列式行表示抽象化」设计立项（孤儿规则 + TriggerEvent 持 Arc<RecordBatch> 不可约层）。
4. **warp-fusion 收尾**：切 `tag = "v2.0.17"` + 本地 path 块去留 + 重锁后 `cargo test --workspace` 复验。
5. **fmt 存量漂移清理**（另会话）：固定 rustfmt 版本或一次性全仓格式化单独立提交，避免与 CI 打架。
6. **push wp-reactor**：main 领先 origin 3 笔（e9d020a/b201c19/0011d4f）。

## 5. 关键坑（再会话必读，含本会话新增）

1. 文件工具对 warp-fusion/wf-skills 子树不可达——用 terminal heredoc/python 写；wp-reactor 两者皆可。
2. 全字段字面量 + `..Default::default()` 触发 clippy `needless_update`（-D warnings 禁）。
3. 行段盲切带错 doc/依赖：拆件一律平衡括号块 + 先读交叉引用/读-first。
4. 子模块私有项父不可见（可见性只向下流）：迁入子模块的项要 `pub(crate)`。
5. file-module 子模块解析到 `<name>/` 目录；`#[path]` 相对当前文件目录。
6. 背景 `(cmd &)` 残留 fd/进程：用完 pkill；macOS 无 `timeout`。
7. 迁移脚本误匹配史：排除 struct/enum/impl/`->`/`::` 前缀上下文（MatchPlan::default 递归挂死案例）。
8. wp-reactor 的 Cargo.lock 被 gitignore（不提交）。
9. **本地 rustfmt 与 CI 版本漂移**：勿整仓 `cargo fmt --all`，只 fmt 改动文件（`rustfmt --edition 2024 <files>`）。
10. wf-engine 偶发 flaky（columnar_bind_*）复跑即绿，勿当回归。
11. **&mut self 方法与活借用冲突**：大方法内常有自字段借用贯穿（aliases 等），外抽 helper 须 `&self`，或「先取 idx（不可变借用即止）再按 idx 可变索引」形态（builder_for 三稿才过借用检查）。
12. **root re-export 三约束**：`pub(crate) use` 链需源项 ≥ pub(crate)；pub 声明在私有模块 + 链顶止 pub(crate) 触发 `-W unreachable_pub`；lib 无生产消费的链报 unused import → 组合解：`pub(crate) use` + `#[allow(unused_imports)]`（StatsBucket / accumulate_* 先例）。
13. `cargo fix --lib` 会删「lib 未用但测试消费」的 root re-export（accumulate_* 案例）——用后必复查各 mod.rs 声明区。
14. 正则批量提可见性会吞 fn 后空格（含泛型 `<` 前漏网）并误伤 struct 外 `name:` 局部行——只做 struct 块内/col4 锚定。
15. 子模块相对路径漂移：单文件时 `super::eval` 指 executor::eval，拆深一层静默指向新子模块——迁层后全量核 `super::` 引用。
16. **常用门禁（改动文件级）**：`CARGO_INCREMENTAL=0 cargo test -p wf-engine --lib`（1334，~2.4s）/ `-p wf-runtime --lib`（606，~30s）；clippy all-targets `-D warnings`；增量缓存损坏遇 ICE 时用 `CARGO_INCREMENTAL=0`。
17. **`&mut self` 方法 × 借自 `self.router` 的 `lookup` 不可共存**（H-2/H-3 各踩一次）：`RegistryLookup` 自 `&self.router` 构造后，跨 `&mut self` 方法调用即 E0502——解法：方法内重建 lookup（传 `lookup_max_seq`+`window_name`），并把 lookup 的最后一个使用点（each_direct_rows 向量化收口）一并移入方法内。
18. **行内相位抽「同步自由函数」**（H-4/H-5 模式）：machine 行内 `&mut machine` 只可一行内短借（行体含大量 `&self` 方法调用），抽 helper 须为同步且无 self 方法调用 → 模块级自由函数独立传参（machine/executor/上下文/收集器），避开 `&mut self.machine` 跨方法冲突；含 await 或 `&self` 方法（emit 相位）不适用，维持行内。抽完用 `nm target/release/deps/libwf_runtime-*.rlib` 验内联（无独立符号 = 零调用开销）。
19. **clippy 数据流派生 lint**：`if self.machine.is_some()` 门控后 `.as_mut().expect()` → `unnecessary_unwrap`（改 `let-else + unreachable!`）；参数从 owned 变引用后 `&x` / `Some(&x)` → `needless_borrow`；`Option::as_ref().map(as_slice)` → `option_as_ref_deref`（改 `as_deref()`）。
20. **性能对拍方法论**：顺序 A/B 有温升/调度伪差（本会话曾误报 3–12%「变快」，交替后归零）——必须**交替跑取中位**；`-p wf-runtime` 全套 debug 与 release 同为 ~30s = 固定 sleep 主导、无计算敏感性；行循环基准须「无 sleep + 直驱 process_batch」。
21. **大块搬移脚本切片坑**：锚点匹配用 `.strip()`（`rstrip` 漏前导空格）；`startswith("12 空格")` 会误中 16 空格行；for 块 `[start:end-1]` 切片会把 for-close 之后的行吞进新 fn（H-4 案例：profile 尾行进函数体、括号失衡）——搬完先对目标 fn 做括号平衡校验再编译；fn 尾是循环时要补返回值。
22. **r4 测试 harness 已 `pub(super)`**（`Spec`/`make_task`/`machine_rule`/`make_window`/`run_with_dispatch` 等）——行循环/机器路径新测试直接 `use super::rule_task_r4::{...}`，勿重复造 RuleTaskConfig/窗口基建。

## 6. 自 v2.0.16 起提交清单

v2.0.17 内容（15 笔 + release）：

`d66b2f2` 导航文档 · `515ea83` fanout 目录化 · `2fca9ed` rule_task 目录化 · `b9a1338` debug.rs · `e7af387` stager.rs · `56da04c` S3-1 columnar_each · `b3e3811` S3-2 build_deferred_rows · `a491018` stats_exec 目录化 · `f218947` accum/state · `f942040` exec/eval · `17377f7` StatsBucket 门禁 · `f135a67` handoff 快照 · `a189431` moju 产物 · `0a7ba41` S3-3 builder_for · `98dd085` S3-4 诊断块 · `af58c2c` chore: release v2.0.17（tag）

v2.0.17 后（3 笔，**未 push**）：`e9d020a` docs: handoff v2 快照 · `b201c19` refactor: process_batch 行循环 H-1..H-5 · `0011d4f` test: row_loop 行循环基准
