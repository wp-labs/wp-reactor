# 结构性重构：任务进展交接（2026-09-04 v8 快照 — >1500 行文件拆分 + fmt 存量漂移清理完成）

> 本文件记录 wp-reactor/warp-fusion 结构性重构进行中状态，供新 session 直接续接。
> 配套方案文档：`docs/design/p4-wf-cep-plan.md`；发布条目：`CHANGELOG.md [2.0.17]`。

## 0. 仓库与分支状态（2026-09-04，>1500L 拆分 13 刀 + fmt 清理 2 笔已提交、未 push）

- **wp-reactor @ main = `565f12c`**，**领先 origin/main 16 笔未 push**（`e4c6c3e` … `565f12c`，见 §3.7/§3.8/§6）。origin/main = `e3fba4c`（v6 快照，已推）。工作树干净（本文档提交后）。
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
| fmt | `cargo fmt --all -- --check` | **0**（`2411024` 全仓收敛 18 文件后；CI fmt job 固定 toolchain 1.98，`565f12c`，见 §3.8） |

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

## 3.5 已完成：§4.2 拆件候选推进（大测试外移 + 文件拆组 + 样板收口/切片）

- `perf_diag.rs` 1938 → 744（`4536c3f`，tests ~1190 外移 `perf_diag_tests.rs`，#[path] cfg(test) 子模块先例）。
- `lifecycle/spawn.rs` 2007 → 1554（`66e3fe4`，receiver/source 组 475 行 → `spawn_receiver.rs`：spawn_receiver_task 提 `pub(crate)` + spawn.rs `pub(super) use` 保接口；仅测试消费 helper 用 `#[allow(unused_imports)] use` 组合）。
- `lifecycle/compile.rs` 1876 → 795（`8b91c0e` tests 外移 `compile_tests.rs` + `d663f9c` 诊断/源码定位组 408 行 → `compile_diag.rs`，27 fn `pub(super)` + 顶层 allow-use）。
- `checker/types/check_funcs.rs` 1966 → 1124：`f69784e` 样板收口（145 处统一 `errors.push(CheckError{…})` → `rule_error(rule_name, message)`，−920）；`052c285` 分类切片（`check_func_call` 815 → 58 行 else-if 分派 + agg 130 / numeric 81 / time 86 / str 290 / mv 125 / misc 112，56 arm 原样搬移）。
- `checker/types/check_funcs.rs` 1124 → 1140（`17eae54`）：`check_str_func` 子切片——pattern 族（regex_match/cidr_match/contains/startswith/endswith/startswith_any/endswith_any）→ `check_pattern_func` 95 行、hash 族（md5/sha1/sha256/hex/sha1_n/stable_id）→ `check_hash_func` 58 行，str 文本核心 arm 留 `check_str_func`（290 → 168）；dispatch 双 `matches!`+return 置于 fn 中段（非尾表达式，needless_return 不触发）；三 fn 各带独立 catch-all 无重复。
- `lifecycle/spawn.rs` 1554 → 1489（`f15c0ef`）：metrics 组 75 行 → `spawn_metrics.rs`（`spawn_metrics_task` 提 `pub(crate)` + spawn.rs `pub(super) use` 保接口，`run_monitor_consumer`/`metrics_record_to_data_record` 随迁，`use super::*` glob 引父层绑定），与 `spawn_receiver.rs` 先例同构。
- 门禁：每笔 wf-lang 1051 / wf-runtime 606 + clippy all-targets + clippy2（-W unreachable_pub）0。

## 3.6 已完成：17 个超大型文件拆分（2026-09-04，测试 13 + 生产 4，全部 #in-file #[path] sibling 子模块）

> 门禁：每刀 wf-lang 1051 / wf-engine 1334（+73 ignored）/ wf-runtime 606（+15）+ clippy all-targets ×3 + clippy2 0；每波后 consolidated 复跑（并行刀中间态会短暂编译失败，以收尾后全树为准）。原 59,910 行 → 收口 hub 合计 6,112 行；~65 个新 sibling 文件。提交自 `572b725`…`1858c51`（17 笔）。

**生产拆件（4）**：

| 文件 | 收口 | 子模块 |
|---|---|---|
| `window/fanout/mod.rs` 2074 | 181 | dispatch.rs 591（impl RuleFanout 分发，struct 留父面保私有字段对 sibling tests 可见）+ scope_key.rs 83（pub(crate) 列式 scope-key）+ tests.rs 1262（内联测试外移） |
| `executor/each_exec.rs` 3200 | 194 | plan.rs 366（ScorePlan/join plan 门控）+ direct.rs 664（行式直发+安全门）+ col_exec.rs 998 + col_join.rs 1067（巨型 impl 按方法边界切两块；超大单方法 ~560 行不动） |
| `match_engine/columnar.rs` 3684（pub mod） | 488 | columnar_compile.rs 622 + columnar_eval.rs 990（CVec 求值核）+ columnar_tests.rs 1622（原 mod tests 外移；公开路径逐条核对 0 遗失，零项私→pub 提升——ColumnarBatch 核心 impl 留 root 使 resolve_field/column_at 保持私有即被子模块可见） |
| `engine_task/rule_task/mod.rs` 5169 | 564 | rule_task_run 1125 / rows 885（H-1..H-5 行循环族整簇）/ scan 832 / emit 1185（生产 4 面）；内联测试外移 debug_stats_tests 66 / pipe_stager_tests 410 / retention_pin_tests 90 / row_domain_tests 50 |

**测试/基准拆件（13，原样搬移、测试名/断言/属性零改动）**：

| 文件 | 收口 | 子模块 |
|---|---|---|
| `checker/tests/coverage_extra.rs` 2509 | 157 | coverage_funcs 691 / funcs2 365 / rules 701 / expr 637（76 测试） |
| `executor/coverage_extra.rs` 8261 | 400 | exec 857 / join 740 / each 1105 / each_columnar 1147 / columnar_expr 1120 / columnar_fallback 826 / close_deferred 1358 / yield_fire 829（93 测试） |
| `executor/stats_exec_test.rs` 3474 | 308 | basic 232 / columnar 841 / grouped 531 / last_top 854 / state 761 |
| `executor/coverage_r4.rs` 2350 | 481 | context 351 / close_each 801 / match_alert 759（28 测试） |
| `executor/eval/tests.rs` 4219 | 87 | tests_yield_agg 540 / yield_funcs 852 / builtin_str_mv 776 / builtin_fmt_hash_time 616 / builtin_agg_stat 480 / utils_l3_expr 923（76 测试） |
| `tests/core_coverage.rs` 2863 | 191 | types 762 / executor 670 / joins 694 / close_each 604（52 测试） |
| `tests/nexmark_hotpath_bench.rs` 2668 | 1171（共享基座） | state_advance 491 / each_emit 460 / stats_state 586（20 测试） |
| `tests/executor/yield_tests.rs` 3430 | 27 | each_match 825 / close 917 / stat_evidence 1005 / nested_path 699 |
| `window/buffer/tests.rs` 2908 | 93 | tests_eviction 714 / state 799 / cursor 427 / join 903 |
| `engine_task/stats_task_tests.rs` 2210 | 212 | stats_task_q15 517 / windows 735 / ranked 771（28 测试） |
| `engine_task/tests.rs` 5502 | 671 | engine_task_tests_core_paths 1048 / sharded_pull 655 / intermediate_relay 748 / downstream_close 925 / bind_each 474 / port_scan 223 / conv_stage 834（54 测试；tests.rs 是共享 harness 屋，被外部引用的 8 个 harness 项留原文件） |
| `engine_task/deferred_integration_tests.rs` 3023 | 655 | deferred_q9 563 / q8 673 / q13 462 / q13_sharded 565 / wfl 170（31 测试） |
| `engine_task/rule_task_bench.rs` 2366 | 233 | bench_pipe 583 / join 525 / alloc 923 / row_loop 135 |

## 3.7 已完成：全仓 >1500 行文件拆分（2026-09-04，测试 6 + 生产 7，13 刀；提交 `e4c6c3e`…`31b741f`）

> 门禁同 §3.6（每刀 + 每波 consolidated；origin/main 已在 e3fba4c，本批 13 笔未 push）。拆分后全仓最大文件 = `lifecycle/spawn.rs` 1489（此前已判定无再拆候选）。

**生产拆件（7，含 4 个 hub/dir-module）**：

| 文件 | 收口 | 子模块 |
|---|---|---|
| `event_bridge.rs` 1509（pub mod） | 368 | event_bridge_views.rs 539（ColumnarEvent/JoinRow/TriggerEvent 列式视图面）+ event_bridge_tests.rs 647（内联测试外移）；23 pub 项逐条核对 0 遗失 |
| `metrics/mod.rs` 1510（pub mod） | 553 | counters.rs 678（impl RuntimeMetrics 全量下沉）+ records.rs 311（impl MetricsSnapshot::to_records）；只搬 impl 不移动项声明 → 零 re-export 新增 |
| `alert/column_batch.rs` 1581 | 294 | column_batch_stage.rs 463 + column_batch_commit.rs 264（impl AlertColumnBuilder 按方法边界切两块）+ column_batch_tests.rs 604；struct 全留父面，零 pub 项移入 |
| `match_engine/spill.rs` 1588（pub mod） | 41 | spill_store.rs 173（trait+Mem）+ spill_redb.rs 383 + spill_serde.rs 580（手写字节编解码）+ spill_tests.rs 459；模块名 redb→redb_store 避 extern crate 同名遮蔽（E0659/E0425）；14 pub 项全保留 |
| `executor/mod.rs` 1613（hub） | 251 | plan_analysis.rs 508（构造期静态分析族）+ rule_exec.rs 901（impl RuleExecutor 整块下沉 + 尾部原语簇）；声明/re-export 区原位保留；YieldKind/OutputStatic/ReduceLabelReads 交叉密集留 hub |
| `cep/mod.rs` 1986（目录 hub） | 576 | advance.rs 290（advance 入口家族）+ window.rs 790（advance_window 单方法 ~740 行整块）+ expiry.rs 393（close/scan_expired）；6 私有方法提 pub(super)，零 pub 提级；match_engine/mod.rs 零改动 |
| `compiler/mod.rs` 1680（wf-lang） | 85 | rules.rs 610 + match_build.rs 234 + bind_tracking.rs 365 + clause_build.rs 444；测试消费项 `#[cfg(test)] pub(crate) use` 门控（lib 无生产消费） |

**测试拆件（6，原样搬移）**：`l2/expr.rs` 1612→18（expr_control/time/funcs 3 模块 41 测试）· `columnar_tests.rs` 1622→90（guard 950 / output 605，32 测试）· `receiver/tests.rs` 1697→252（file_replay 632 / arrow_coerce 425 / route_dispatch 422）· `stats_spill_test.rs` 1714→171（mem 682 / shared 475 / redb 368，27 测试）· `close_bench.rs` 1729→304（q15_cep 338 / stats_hotpath 797 / stats_merge 333）· `direct_tests.rs` 1831→99（join 826 / row 439 / columnar 504，19 测试）。

## 3.8 已完成：fmt 存量漂移清理（2026-09-04，`2411024` + `565f12c`）

> §4.2 任务 #5 结项。漂移根源 = CI fmt job 浮动 `stable`：每次 stable 升级都可能改 rustfmt 规则，长期「只 fmt 改动文件」政策下积出全仓存量漂移。利用重构暂停 + 树干净窗口整仓收敛，纯格式化零语义变化。

- `2411024` chore：`cargo fmt --all` 一次收敛 **18 文件 3765+/3078-**（wf-runtime 12：perf_diag 族/compile 族/lifecycle 多文件/external×2/alert_task；wf-lang 2：check_funcs.rs 951 行 diff = 重构前 144 hunk 历史漂移 + 本刀混入一次清掉、scope.rs；wf-engine fanout/tests.rs 2398 行、wf-config 2、wf-cep lib.rs 1）。
- `565f12c` ci：fmt job 改 `dtolnay/rust-toolchain@master` + `toolchain: 1.98`（= 本仓格式化产出所用 rustfmt 1.9.0，与本地 1.97.1 的 rustfmt 同为 1.9.0、同规则）+ 前置 `rustfmt --version` 打印便于排障；test/clippy job 仍随 matrix stable/beta。
- 门禁复验（与基线完全守恒）：`cargo fmt --all -- --check` = 0；wf-lang 1051 / wf-engine 1334+73（首跑 1 笔 async_persist flaky，单独复跑绿，类坑 #10）/ wf-runtime 606+15；clippy all-targets + clippy2 unreachable_pub 均 0。
- 说明：warp-fusion 跨仓未动；本地与 CI rustfmt 同 1.9.0 理论无残余差异，若 CI 仍红先看 `rustfmt --version` 输出核对。

## 4. 待办积压（按优先级）

1. **#1 H 尾 — machine 行内 emit 相位（~90 行）**：close/match emit 块仍内联在 `process_batch_machine_rows`（含 `&self` async `stage_or_emit_record` + `lookup`）。拆法 A = 每行 &mut self 方法 → 逐行 async future（热点不取）；拆法 B = 按 ALERT_BATCH_SIZE 批外收口 emit 节奏（需改 emit 时序 + 对拍）。当前评估：维持现状，勿为拆而拆。
2. **可选拆件候选**（剩余）：**全仓已无 >1500L 文件**（§3.6+§3.7 共 30 刀）。量级候选：`lifecycle/spawn.rs` 1489（receiver/metrics 两组已拆，剩余 alert/evictor/rule 任务组互相借用深，前判无再拆候选，如需可重评估）；`config_loader/fusion_tests.rs` 1476、`diagnostics.rs` 1445（wf-lang，样板集中）、`window/buffer/mod.rs` 1442、`window_lookup.rs` 1439、`cep/tests/coverage_r4.rs` 1411、`eval/builtins.rs` 1382（这些均在仓库既有先例上界 ~1400 附近，属「可拆可不拆」，未立项前不动）。columnar.rs/each_exec.rs/cep/mod.rs/event_bridge.rs 已**文件级**拆分——P4 片4-6 语义下沉仍待立项对表（现模块边界 = advance/window/expiry/views 等，是更好的对表基础）。
3. **P4 片4-6**（cep/event_bridge/key/eval 同步执行核下沉 wf-cep）：搁置；前置 = 「列式行表示抽象化」设计立项（孤儿规则 + TriggerEvent 持 Arc<RecordBatch> 不可约层）。
4. **warp-fusion 收尾**：切 `tag = "v2.0.17"` + 本地 path 块去留 + 重锁后 `cargo test --workspace` 复验。
5. **fmt 存量漂移清理**：✅ 已完成（`2411024` 全仓收敛 + `565f12c` CI 固定 1.98，见 §3.8）。
6. **push wp-reactor**：main 领先 origin 13 笔（e4c6c3e…31b741f，见 §3.7/§6）。

## 5. 关键坑（再会话必读，含本会话新增）

1. 文件工具对 warp-fusion/wf-skills 子树不可达——用 terminal heredoc/python 写；wp-reactor 两者皆可。
2. 全字段字面量 + `..Default::default()` 触发 clippy `needless_update`（-D warnings 禁）。
3. 行段盲切带错 doc/依赖：拆件一律平衡括号块 + 先读交叉引用/读-first。
4. 子模块私有项父不可见（可见性只向下流）：迁入子模块的项要 `pub(crate)`。
5. file-module 子模块解析到 `<name>/` 目录；`#[path]` 相对当前文件目录。
6. 背景 `(cmd &)` 残留 fd/进程：用完 pkill；macOS 无 `timeout`。
7. 迁移脚本误匹配史：排除 struct/enum/impl/`->`/`::` 前缀上下文（MatchPlan::default 递归挂死案例）。
8. wp-reactor 的 Cargo.lock 被 gitignore（不提交）。
9. **fmt 策略（2026-09-04 更新）**：漂移源 = CI fmt job 浮动 stable（升级即改 rustfmt 规则）。已根治：全仓 `cargo fmt --all` 一次性收敛（`2411024`，18 文件 3765+/3078-，纯格式化）+ CI fmt job 固定 toolchain 1.98（rustfmt 1.9.0，`565f12c`）。日常仍只 fmt 改动文件即可；确需整仓 fmt 须树干净 + 单独提交。
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
23. **re-export 到父层的可见级**：子模块 fn 以 `pub(super)`（=父）re-export 到父的 `pub(super)`（=祖）会 E0364——源需 `pub(crate)`（spawn_receiver_task 先例）。
24. **`#[path]` sibling 文件模块（不目录化）**：文件模块内 `#[path="x.rs"] mod x;` 指向同目录 sibling（相对当前文件目录）；可见性组合——生产消费的项 `pub(super) use`、仅测试消费的项 `#[allow(unused_imports)] use`（StatsBucket 先例）；**私有 use 绑定可被子模块 `use super::*` glob 导入**（spawn_coverage/compile_tests 靠此不写 use 列表）；lib 构建的 allow 不误伤。
25. **`private_interfaces`**：`pub(super)` fn 返回/使用 private struct → 报错；struct 与字段需同步提 `pub(super)`（compile_diag 的 YieldPresetDeclLocation 案例）。
26. **同文件多刀分笔提交 + detached HEAD 陷阱**：`git add -p` 按 hunk 分离同文件两刀为两笔（compile.rs tests 外移 vs diag 拆出案例）；**中间态提交后必须 checkout 验证可编译；但 `git checkout <hash>` 会 detached HEAD——验证完要 `git checkout main`（或 `git branch -f main` + 切回），否则后续提交落在游离头**（f69784e 事故：main 停 d663f9c，detached 上多一笔后 ff-only 修复）。
27. **check_funcs 样板收口/切片方法**：① 先统计 severity/rule/test 变体确认样板 100% 统一再抽 helper（145 push 全 Error/Some(rule_name)/None）；② match 按类别切片时 **arm 正则须支持 guard arm**（`"now" | … if cond =>`），否则 guard arm 被并入前一 arm 段、随错误类别搬走静默丢检查（now 无参检查丢失→2 测试挂）；③ 原 match 尾 `_ => {}` catch-all 会并入最后 arm 段——生成 catch-all 前先查重；④ 入口残留已搬变量的解构要删；⑤ dispatch 用 else-if 链避免 needless_return；⑥ 收口/切片的 message 内容零改动靠 checker 大量消息断言测试兜底。
28. wf-lang 单 crate 测试 ~0.02s（1051），样板/切片刀可全量快速验证；改动被下游消费时复验 `-p wf-runtime --lib`（606）。
29. **大测试文件主题分片模板**（本会话 13 刀验证）：刀文件内 `#[path = "<name>.rs"] mod <name>;` 声明子模块（不改 parent mod.rs → 同 crate 多刀可并行不冲突）；新文件 = `//! 中文主题` + `use super::*;`（父私有项/use 绑定 glob 继承，零 use 清单）+ 原样切片（section banner 连续切，测试名/断言/属性零改动）；独占 helper 随搬防 dead_code，共享 harness/import 留父；搬后清父文件失用 import；子模块内 `super::` 因层级下沉失效处改 `crate::` 绝对路径（#15，deferred/eval/tests 三刀各踩）。搬移内容用「HEAD 重建比对/逐字节 diff + fn 名集合守恒」自检；lib 测试数守恒（1051/1334/606）为最终断言。
30. **生产模块拆件可见性三形态**（fanout/each_exec/columnar/rule_task 四刀验证）：① 私有 mod（each_exec/fanout）：被消费项移子模块后父文件对等 re-export（源 ≥ re-export 级，E0364-safe）；struct 留父面 + impl 下沉（私有字段对 sibling 测试可见性只向下流，零提级最干净——fanout RuleFanout/columnar ColumnarBatch 先例）；② pub mod（columnar）：原 pub 项子模块内保持 pub + 父 `pub use` re-export，路径与可见级逐条对账（脚本 0 遗失）；③ impl 块按方法边界整块切（不破 impl），方法间只经 `&self`/模块级 helper 的组可拆；`&mut` 字段贯穿或超大单方法（~500 行）宁可不拆；私有方法移子模块提 `pub(super)`（有效域 = 父模块子树，含 sibling 测试直调面），入口方法提 `pub(crate)` + mod.rs 顶层 `#[allow(unused_imports)] use` re-bind（rule_task_run 先例）。
31. **并行 agent 拆件协作**：多刀并发同树（各刀 in-file 声明 → 父文件零冲突）可行，但 cargo 锁串行化构建、且并行中间态会让工作树**短暂编译失败**（agent 误判外部会话/互相等待）——收尾后必须 consolidated 复跑三 crate 测试 + clippy×2 为准；agent 勿并发 commit（共享 index 竞态会互相吞文件）——由主控统一 add/commit 每刀。
32. **hub/mod.rs 拆件法**（executor/cep/compiler/metrics 四 hub 验证）：root 保留「声明/re-export 区 + struct + 共享簿记」，impl 按方法组**整块下沉**子文件（`use super::*` 引类型，struct 留 root 则私有字段零提级——metrics counters/cep advance-window-expiry/executor rule_exec 先例）；超大单方法（advance_window ~740 行）与交叉密集类型组（YieldKind/OutputStatic/ReduceLabelReads 被 6+ sibling super:: 引用）宁留 hub；被测试消费的项用 `#[cfg(test)] pub(crate) use` 门控（lib 无生产消费点防 unused import，compiler bind_tracking 先例）。
33. **子模块命名避 extern crate 同名**：`mod redb;` 会在父模块遮蔽 extern prelude（E0659/E0425）——改名 redb_store；同理会撞 crate 名的子模块命名前先想 extern prelude。

## 6. 自 v2.0.16 起提交清单

v2.0.17 内容（15 笔 + release）：

`d66b2f2` 导航文档 · `515ea83` fanout 目录化 · `2fca9ed` rule_task 目录化 · `b9a1338` debug.rs · `e7af387` stager.rs · `56da04c` S3-1 columnar_each · `b3e3811` S3-2 build_deferred_rows · `a491018` stats_exec 目录化 · `f218947` accum/state · `f942040` exec/eval · `17377f7` StatsBucket 门禁 · `f135a67` handoff 快照 · `a189431` moju 产物 · `0a7ba41` S3-3 builder_for · `98dd085` S3-4 诊断块 · `af58c2c` chore: release v2.0.17（tag）

v2.0.17 后（9 笔，**未 push**）：
`e9d020a` docs: handoff v2 快照 · `b201c19` refactor: process_batch 行循环 H-1..H-5 · `0011d4f` test: row_loop 行循环基准 · `715ba85` docs: handoff v3 快照 · `4536c3f` test: perf_diag tests 外移 · `66e3fe4` refactor: spawn receiver 拆出 · `8b91c0e` test: compile tests 外移 · `d663f9c` refactor: compile diag 拆出 · `f69784e` refactor: check_funcs 样板收口

`052c285` refactor: check_func_call 按类别切片 6 helper

v2.0.17 后续（4 笔，未 push）：`f3c3482` docs: 交接文档 v4 快照 · `17eae54` refactor(wf-lang): check_str_func 子切片 pattern/hash 族 · `f15c0ef` refactor(wf-runtime): spawn metrics 任务组拆出 spawn_metrics.rs ·（v5 快照本文档提交后）

v6 前 17 刀（2026-09-04 超大型文件拆分，未 push，见 §3.6）：
`572b725` test(wf-lang): checker/tests/coverage_extra.rs 分片 4 · `1b84507` test(wf-engine): executor/coverage_r4.rs 分片 3 · `b26d899` test(wf-engine): yield_tests.rs 分片 4 · `826beae` test(wf-engine): window/buffer/tests.rs 分片 4 · `49dee17` test(wf-runtime): stats_task_tests.rs 分片 3 · `4e7a566` test(wf-engine): executor/coverage_extra.rs 分片 8 · `bbbfde5` test(wf-engine): stats_exec_test.rs 分片 5 · `daf203a` test(wf-engine): eval/tests.rs 分片 6 · `6c8e517` test(wf-engine): core_coverage.rs 分片 4 · `cceaeb6` test(wf-runtime): deferred_integration_tests.rs 分片 5 · `f55b665` test(wf-engine): nexmark_hotpath_bench.rs 分片 3 · `f9cec9a` test(wf-runtime): engine_task/tests.rs 分片 7 · `9c91059` test(wf-runtime): rule_task_bench.rs 分片 4 · `b276cae` refactor(wf-engine): window/fanout 拆分 · `f4e27b2` refactor(wf-engine): columnar.rs 拆分 · `bfef438` refactor(wf-engine): each_exec.rs 拆分 · `1858c51` refactor(wf-runtime): rule_task/mod.rs 拆分

（v6 快照本文档提交后）

v7 前 13 刀（2026-09-04 >1500L 文件拆分，未 push，见 §3.7）：
`e4c6c3e` test(wf-lang→wf-engine): l2/expr.rs 分片 3 · `a8b56a4` test(wf-engine): columnar_tests.rs 分片 2 · `4507dd2` test(wf-runtime): receiver/tests.rs 分片 3 · `3c41537` test(wf-engine): stats_spill_test.rs 分片 3 · `ca9dc88` test(wf-engine): close_bench.rs 分片 3 · `69d5e74` test(wf-engine): direct_tests.rs 分片 3 · `86f3abb` refactor(wf-engine): event_bridge.rs 拆分 · `da0b8c4` refactor(wf-engine): spill.rs 拆分 · `5a7d9ff` refactor(wf-engine): alert/column_batch.rs 拆分 · `c25a620` refactor(wf-runtime): metrics/mod.rs 拆分 · `a8aa49f` refactor(wf-engine): executor/mod.rs 拆分 · `5a87a63` refactor(wf-engine): cep/mod.rs 拆分 · `31b741f` refactor(wf-lang): compiler/mod.rs 拆分

（v7 快照本文档提交后）

v8（fmt 存量漂移清理 2 笔，未 push，见 §3.8）：
`2411024` chore: rustfmt 全仓格式化收敛（18 文件 3765+/3078-，纯格式化零语义）· `565f12c` ci: fmt job 固定 toolchain 1.98（rustfmt 1.9.0）+ 打印版本

（v8 快照本文档提交后）
