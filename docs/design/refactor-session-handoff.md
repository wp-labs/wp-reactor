# 结构性重构：任务进展交接（2026-09-04 v5 快照 — §4.2 str 子切片 + spawn metrics 两刀收尾后）

> 本文件记录 wp-reactor/warp-fusion 结构性重构进行中状态，供新 session 直接续接。
> 配套方案文档：`docs/design/p4-wf-cep-plan.md`；发布条目：`CHANGELOG.md [2.0.17]`。

## 0. 仓库与分支状态（2026-09-03，§4.2 拆件多笔已提交、未 push）

- **wp-reactor @ main = `f15c0ef`**，**领先 origin/main 13 笔未 push**（`e9d020a` … `f15c0ef`，见 §6）。origin/main 仍停 `af58c2c`（v2.0.17）。工作树干净（本文档提交后）。
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

## 3.5 已完成：§4.2 拆件候选推进（大测试外移 + 文件拆组 + 样板收口/切片）

- `perf_diag.rs` 1938 → 744（`4536c3f`，tests ~1190 外移 `perf_diag_tests.rs`，#[path] cfg(test) 子模块先例）。
- `lifecycle/spawn.rs` 2007 → 1554（`66e3fe4`，receiver/source 组 475 行 → `spawn_receiver.rs`：spawn_receiver_task 提 `pub(crate)` + spawn.rs `pub(super) use` 保接口；仅测试消费 helper 用 `#[allow(unused_imports)] use` 组合）。
- `lifecycle/compile.rs` 1876 → 795（`8b91c0e` tests 外移 `compile_tests.rs` + `d663f9c` 诊断/源码定位组 408 行 → `compile_diag.rs`，27 fn `pub(super)` + 顶层 allow-use）。
- `checker/types/check_funcs.rs` 1966 → 1124：`f69784e` 样板收口（145 处统一 `errors.push(CheckError{…})` → `rule_error(rule_name, message)`，−920）；`052c285` 分类切片（`check_func_call` 815 → 58 行 else-if 分派 + agg 130 / numeric 81 / time 86 / str 290 / mv 125 / misc 112，56 arm 原样搬移）。
- `checker/types/check_funcs.rs` 1124 → 1140（`17eae54`）：`check_str_func` 子切片——pattern 族（regex_match/cidr_match/contains/startswith/endswith/startswith_any/endswith_any）→ `check_pattern_func` 95 行、hash 族（md5/sha1/sha256/hex/sha1_n/stable_id）→ `check_hash_func` 58 行，str 文本核心 arm 留 `check_str_func`（290 → 168）；dispatch 双 `matches!`+return 置于 fn 中段（非尾表达式，needless_return 不触发）；三 fn 各带独立 catch-all 无重复。
- `lifecycle/spawn.rs` 1554 → 1489（`f15c0ef`）：metrics 组 75 行 → `spawn_metrics.rs`（`spawn_metrics_task` 提 `pub(crate)` + spawn.rs `pub(super) use` 保接口，`run_monitor_consumer`/`metrics_record_to_data_record` 随迁，`use super::*` glob 引父层绑定），与 `spawn_receiver.rs` 先例同构。
- 门禁：每笔 wf-lang 1051 / wf-runtime 606 + clippy all-targets + clippy2（-W unreachable_pub）0。

## 4. 待办积压（按优先级）

1. **#1 H 尾 — machine 行内 emit 相位（~90 行）**：close/match emit 块仍内联在 `process_batch_machine_rows`（含 `&self` async `stage_or_emit_record` + `lookup`）。拆法 A = 每行 &mut self 方法 → 逐行 async future（热点不取）；拆法 B = 按 ALERT_BATCH_SIZE 批外收口 emit 节奏（需改 emit 时序 + 对拍）。当前评估：维持现状，勿为拆而拆。
2. **可选拆件候选**（剩余）：`match_engine/columnar.rs` 3684L、`executor/each_exec.rs` 3200L（以上与 P4 片4-6 重叠，先立项对表）。`spawn.rs` receiver/metrics 两组已拆出（现 1489L，无再拆件候选）；`check_funcs.rs` 1140L（str 族已按 pattern/hash 子切片，各 fn 均 < 170 行；stat/label 组 143 行暂不动）。
3. **P4 片4-6**（cep/event_bridge/key/eval 同步执行核下沉 wf-cep）：搁置；前置 = 「列式行表示抽象化」设计立项（孤儿规则 + TriggerEvent 持 Arc<RecordBatch> 不可约层）。
4. **warp-fusion 收尾**：切 `tag = "v2.0.17"` + 本地 path 块去留 + 重锁后 `cargo test --workspace` 复验。
5. **fmt 存量漂移清理**（另会话）：固定 rustfmt 版本或一次性全仓格式化单独立提交，避免与 CI 打架。
6. **push wp-reactor**：main 领先 origin 13 笔（e9d020a…f15c0ef）。

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
23. **re-export 到父层的可见级**：子模块 fn 以 `pub(super)`（=父）re-export 到父的 `pub(super)`（=祖）会 E0364——源需 `pub(crate)`（spawn_receiver_task 先例）。
24. **`#[path]` sibling 文件模块（不目录化）**：文件模块内 `#[path="x.rs"] mod x;` 指向同目录 sibling（相对当前文件目录）；可见性组合——生产消费的项 `pub(super) use`、仅测试消费的项 `#[allow(unused_imports)] use`（StatsBucket 先例）；**私有 use 绑定可被子模块 `use super::*` glob 导入**（spawn_coverage/compile_tests 靠此不写 use 列表）；lib 构建的 allow 不误伤。
25. **`private_interfaces`**：`pub(super)` fn 返回/使用 private struct → 报错；struct 与字段需同步提 `pub(super)`（compile_diag 的 YieldPresetDeclLocation 案例）。
26. **同文件多刀分笔提交 + detached HEAD 陷阱**：`git add -p` 按 hunk 分离同文件两刀为两笔（compile.rs tests 外移 vs diag 拆出案例）；**中间态提交后必须 checkout 验证可编译；但 `git checkout <hash>` 会 detached HEAD——验证完要 `git checkout main`（或 `git branch -f main` + 切回），否则后续提交落在游离头**（f69784e 事故：main 停 d663f9c，detached 上多一笔后 ff-only 修复）。
27. **check_funcs 样板收口/切片方法**：① 先统计 severity/rule/test 变体确认样板 100% 统一再抽 helper（145 push 全 Error/Some(rule_name)/None）；② match 按类别切片时 **arm 正则须支持 guard arm**（`"now" | … if cond =>`），否则 guard arm 被并入前一 arm 段、随错误类别搬走静默丢检查（now 无参检查丢失→2 测试挂）；③ 原 match 尾 `_ => {}` catch-all 会并入最后 arm 段——生成 catch-all 前先查重；④ 入口残留已搬变量的解构要删；⑤ dispatch 用 else-if 链避免 needless_return；⑥ 收口/切片的 message 内容零改动靠 checker 大量消息断言测试兜底。
28. wf-lang 单 crate 测试 ~0.02s（1051），样板/切片刀可全量快速验证；改动被下游消费时复验 `-p wf-runtime --lib`（606）。

## 6. 自 v2.0.16 起提交清单

v2.0.17 内容（15 笔 + release）：

`d66b2f2` 导航文档 · `515ea83` fanout 目录化 · `2fca9ed` rule_task 目录化 · `b9a1338` debug.rs · `e7af387` stager.rs · `56da04c` S3-1 columnar_each · `b3e3811` S3-2 build_deferred_rows · `a491018` stats_exec 目录化 · `f218947` accum/state · `f942040` exec/eval · `17377f7` StatsBucket 门禁 · `f135a67` handoff 快照 · `a189431` moju 产物 · `0a7ba41` S3-3 builder_for · `98dd085` S3-4 诊断块 · `af58c2c` chore: release v2.0.17（tag）

v2.0.17 后（9 笔，**未 push**）：
`e9d020a` docs: handoff v2 快照 · `b201c19` refactor: process_batch 行循环 H-1..H-5 · `0011d4f` test: row_loop 行循环基准 · `715ba85` docs: handoff v3 快照 · `4536c3f` test: perf_diag tests 外移 · `66e3fe4` refactor: spawn receiver 拆出 · `8b91c0e` test: compile tests 外移 · `d663f9c` refactor: compile diag 拆出 · `f69784e` refactor: check_funcs 样板收口

`052c285` refactor: check_func_call 按类别切片 6 helper

v2.0.17 后续（4 笔，未 push）：`f3c3482` docs: 交接文档 v4 快照 · `17eae54` refactor(wf-lang): check_str_func 子切片 pattern/hash 族 · `f15c0ef` refactor(wf-runtime): spawn metrics 任务组拆出 spawn_metrics.rs ·（v5 快照本文档提交后）
