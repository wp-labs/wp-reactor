# 结构性重构：任务进展交接（2026-09-03 快照，session 重启用）

> 本文件记录 wp-reactor/warp-fusion 结构性重构的进行中状态，供新 session 直接续接。
> 配套方案文档：`docs/design/p4-wf-cep-plan.md`。

## 0. 仓库与分支状态

- **wp-reactor @ main**：本地领先 origin（origin 停在 `132cd5d update moju`），**11 笔本地提交未推送**（见 §1）。
- **本地 tag `v2.0.16`**（commit `0ae1202`）已建**未推送**（push 曾被用户中断）。
- **warp-fusion @ alpha**：`Cargo.toml` 已由用户切到本地 path 依赖（`@gxl:block(local_reactor)`，git tag 块注释掉）；`Cargo.lock` 已由 cargo 改写为 path 来源。工作树：`M Cargo.toml` + `M Cargo.lock`（**未提交**；进仓库会破坏他人构建，需用户决策）。
- wf-skills / warp-fusion 其它提交均已同步（此前完成）。

## 1. wp-reactor 本地已提交（自 origin 132cd5d 之后，按序）

| commit | 内容 |
|---|---|
| `c0f8191` | match_engine 内层归位 `cep/` + pub 面 27 孤儿收敛 + 黑盒契约测试外迁 + 三 crate lib.rs 导航文档 |
| `9e79cc4` | MatchPlan/WindowSpec/CloseMode/MatchMode 支持 Default + 测试共享构造器定点加固 |
| `9f7d0bc` | wf-lang/wf-runtime/wf-config 63 处孤儿 pub 收敛 + CI lib unreachable_pub 门禁 |
| `8b508ed` | wf-cep v0.1（time/regex_cache/cidr_cache/error 纯叶）+ CI 依赖墙（禁 tokio/arrow） |
| `360d9bb` | 决策 A：墙修订（禁 tokio、arrow 允许）+ Value 层/external 下沉 engine shim |
| `bb37558` | 片3：RowFields/RowFieldLayout/RowFieldSlot → wf-cep::rows |
| `3754d4e` | P4 收口文档（片4-6 不可约层，搁置需列式抽象设计立项） |
| `0ae1202` | chore: release v2.0.16（tag v2.0.16 本地，未推） |
| `d66b2f2` | 生产单体文件导航文档（rule_task/each_exec/fanout/check_funcs/spawn） |
| `515ea83` | fanout.rs → fanout/（mod.rs + partition.rs 分片行路由） |
| `2fca9ed` | rule_task.rs 目录化 → rule_task/mod.rs（覆盖模块 #[path] 改 ../） |

验证基线：wf-lang 1051 / wf-engine 1334 / wf-runtime 606 全绿；双 clippy 门禁 0。
跨仓库：warp-fusion 切本地依赖后 `cargo test --workspace` 364 项全绿 + hello_detection e2e 1 条告警（9+ 笔重构对下游零破坏）。

## 2. 当前工作树（#1 S2 半成品，未提交）

**目标**：#1 = rule_task 拆件（S1 目录化 ✅ 已提交 `2fca9ed`；S2 debug/stager 子模块；S3 巨型方法内抽）。

**已做（未提交）**：
- 新建 `crates/wf-runtime/src/engine_task/rule_task/debug.rs`：`RuleBatchDebugStats` struct+impl（38 行）迁入，字段/方法提 `pub(crate)`，`#[derive(Debug, Default)]`，`DEBUG_DETAIL_LIMIT` const 随迁。
- `rule_task/mod.rs`：删除原 struct/impl 区与孤儿 derive；顶部加 `mod debug;` + `use debug::{DEBUG_DETAIL_LIMIT, RuleBatchDebugStats};`。
- **`cargo test -p wf-runtime --lib --no-run` = 0 error**。

**未完成（重开后第一步）**：
1. 若终端报 `Too many open files`：先 `pkill -f cargo; pkill -f rustc; pkill -f 'target/debug/deps'`（此前排查挂死用过 `(cargo test &)` 后台任务，残留 fd）。
2. 验证：`cargo test -p wf-runtime --lib`（预期 606 全绿）→ `cargo clippy -p wf-runtime --all-targets -- -D warnings`（0）→ rustfmt 仅改动的 mod.rs/debug.rs。
3. 提交，建议信息：`refactor(wf-runtime): RuleBatchDebugStats 迁 rule_task/debug.rs 子模块`。

## 3. 结构性待办积压（决策依据见 p4-wf-cep-plan.md）

- **#1 S2 尾**：`PipeBatchStager` 全家（PipeState/PipeCol/PipeColSource enums + PipeStagerSink + impl/struct，~870 行）迁 `rule_task/stager.rs`——**先读其与主 impl 的交叉引用再动**。
- **#1 S3**：主 impl 内 ~1300 行巨型方法内抽（读批/掩码/advance/emit/背压/诊断）——读-first 专项。
- **#3 stats_exec.rs 拆件**：挂起。**行段盲切失败已还原**（段间把非移动函数的 doc 错挂、依赖网交织），重做需先产出「逐函数归属 + doc 配对 + 依赖导入」图谱再切。
- **P4 片4-6**（cep/event_bridge/key/eval 同步执行核下沉）：搁置；孤儿规则 + 列式句柄（TriggerEvent 持 Arc<RecordBatch>）构成不可约层，需列式行表示抽象化设计立项。
- **发布线**：本地 11 笔 + tag v2.0.16 未推；推送后 warp-fusion 可切回 `git tag = v2.0.16` 并决定 Cargo.toml 本地块去留。

## 4. 关键坑（再会话必读）

1. **文件工具对 warp-fusion/wf-skills 子树不可达**——用 terminal heredoc/python 写文件；wp-reactor 两者皆可。
2. **全字段字面量 + `..Default::default()` 触发 clippy `needless_update`**（-D warnings 下禁）；已在 MatchPlan 文档注明。
3. **行段盲切文件会带错 doc/依赖**（stats_exec 失败案例）；拆件一律：平衡括号块 + 先读交叉引用/读-first。
4. **子模块私有项父不可见**（可见性只向下流）：搬入子模块的项要 `pub(crate)`。
5. **file-module 子模块解析到 `<name>/` 目录**；`#[path]` 相对当前文件目录；rule_task 覆盖模块已是 `#[path = "../rule_task_*.rs"]`（目录化后）。
6. **背景 `(cmd &)` 会残留 fd/进程**：用完必须 pkill；macOS 无 `timeout` 命令。
7. **迁移脚本误匹配史**：会把 spread/搬移插进 `impl Default`、`-> Type {` 返回类型造成自递归或语法损坏——脚本要排除 struct/enum/impl/`->`/`::` 前缀上下文；曾因此出现 MatchPlan::default 无限递归挂死（sample 抓栈定位）。
8. **wp-reactor 的 Cargo.lock 被 gitignore**（不提交）。
9. **本地 rustfmt 与 CI 版本漂移**：勿整仓 `cargo fmt --all`（会大面积噪音），只 fmt 改动文件。
10. wf-engine 全量测试偶发单例失败（如 columnar_bind_*）为历史 flaky，复跑即绿，勿当回归。

## 5. 2026-09-03 续接会话进展（自 §2-§4 快照之后）

**#1 S2 收口**（rule_task 拆件 S1+S2 全绿）：
- `b9a1338` RuleBatchDebugStats → `rule_task/debug.rs`（clippy 修 DEBUG_DETAIL_LIMIT 测试显式 import）。
- `e7af387` PipeBatchStager 全家 → `rule_task/stager.rs`（含 value_to_json*/resolve_pipe_shape/PendingEventBatch；SourceRawErr+ToStructError 双 trait 供 source_raw_err/to_err）。

**#1 S3 部分**（process_batch ~1300 行内抽，读-first 已完成全段精读）：
- `56da04c` columnar_each 快路径早退 → `async fn process_batch_columnar_each`。
- `b3e3811` DeferredRows 构造（L2 延迟物化相位）→ `fn build_deferred_rows`。
- `0a7ba41` `PendingAlertColumns::builder_for` 收口 emit 路径 **8 处同构 get-or-create**（背压相位去重，−133 行；借 index 再索引形态绕开跨臂可变借用）。
- 剩余候选（未动，风险递增）：行循环主相位 H（~600 行，借网交织）、E/L 诊断块（批摘要日志/墙钟注入，可打包回传 struct 抽 begin_batch）。每刀后门禁：wf-runtime 606 全绿 + clippy 0。

**#3 stats_exec.rs 拆件完成**（图谱 → 目录化 → 逐簇切出，全部门禁绿）：
- `a491018` 目录化：stats_exec.rs → stats_exec/mod.rs；`f218947` 拆 `accum.rs` + `state.rs`（含混合 doc 归属重组、super::eval 相对路径改绝对）；`f942040` 拆 `exec.rs`（执行器核）与 `eval.rs`（求值簇）收口。
- 终态：mod.rs 35 行（仅头文档 + mod 声明 + 同名 re-export 面），masks 137 / accum 388 / state 1182 / exec 1376 / eval 1136。executor/mod.rs 转发与测试深路径零改动；wf-engine 1334 + wf-runtime 606 全绿 + clippy 0。
- 坑追加：拆深一层后 `super::X` 语义全变（原指 executor）；正文用全局正则提 pub(crate) 会吞 fn/struct 后空格（generic `<` 前也漏）并误伤 struct 外同名变量行——均当场定位修复；`cargo fix --lib` 会顺手删“lib 未用但测试消费”的 root re-export（accumulate_*），需 `#[allow(unused_imports)]` 重挂。
