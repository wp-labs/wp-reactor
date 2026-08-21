# 性能 profile 覆盖率（profile-cov）

系统化发现「低效代码」的方法：把 **LLVM 覆盖率（代码执行覆盖）** 与 **性能基准（热路径）**
结合成双维度统计。三类输出直接回答「哪里可能藏低效/未验证的实现」：

| 类别 | 含义 | 行动 |
|------|------|------|
| A. 死/冷代码 | 测试与性能基准都未覆盖的行 | 低效实现可能藏在未验证路径；要么补路径要么删除 |
| B. 热路径缺测试 | 性能基准执行但单测未覆盖的行 | 热路径无正确性保护——改坏时基准跑通、单测抓不到，须补测试 |
| C. 热点行 | 性能基准中**执行计数**最高的行 | 优化候选：计数 × 单次成本 = 优化收益（配合 macOS `sample` 的耗时归因） |

## 用法

```bash
# 默认：close_bench each_bench guard_bench match_bench 四个引擎基准
scripts/profile-cov.sh

# 自定义基准 / 限定包
BENCHES="close_bench" PKGS="wf-engine" scripts/profile-cov.sh
```

输出：`target/profile-cov/{tests.json, bench.json, report.txt}`（report.txt 为可读报告）。

## 原理

1. `cargo llvm-cov --tests` 跑全部单测 → `tests.json`（正确性覆盖）
2. `cargo llvm-cov --tests -- --ignored <benches>` 跑 `#[ignore]` 性能基准 → `bench.json`（热路径覆盖）
3. 逐文件对比行级覆盖差集 + 行级执行计数（`segments[].count`）

> 关键：两次运行之间必须清理 `target/llvm-cov-target/*.profraw`，否则 llvm-cov
> 合并全部 profraw，差集失真（实测：混叠时"仅测试"列全 0）。

## 局限与互补

- **覆盖率 = 执行过，≠ 耗时**：低效实现（O(n²)）与高效实现执行计数相同。C 类
  给出「调用频率」，还需 `sample`/Instruments 给「单次耗时」——两者相乘才是
  真实优化收益（q15 的 `push_capped` 就是 88% 采样耗时 + 每事件 8 次调用被定位）。
- **基准覆盖面**：close_bench 只覆盖 wf-engine 状态机层；`rule_task.rs`（生产外围）
  显示为"仅测试覆盖"——这类模块需要端到端/集成基准（bench.sh 跑批 + sample）。

## 真实运行时热路径（profile-runtime.sh）

微基准覆盖的是引擎内部路径；**真实运行**（TCP 接收 / parse / 列式转换 / routing /
窗口 / emit / ack 全链路）的热点分布完全不同。用 LLVM **instrument-coverage 插桩**
构建 wfusion，跑真实 bench，精确统计每行执行次数：

```bash
RUNTIME_BENCH="q1 q4 q12" TOTAL=10m scripts/profile-runtime.sh
# 或直接集成进 bench.sh（默认 10M 数据量）：
PROFILE=1 ./bench.sh all replay 10m
```

流程：插桩构建（隔离 CARGO_TARGET_DIR）→ `LLVM_PROFILE_FILE` 跑真实 bench
（bench.sh 透传）→ `llvm-profdata merge` → `llvm-cov export` → 行级计数报告。
产物：`data/profile/{runtime.profdata, runtime.json}`（bench.sh PROFILE 模式）或
`target/profile-runtime/`（profile-runtime.sh）。

## 闭环：真实覆盖 vs 微基准 vs 指标预期

三件套形成性能治理闭环：

```bash
# 1. 真实运行覆盖（插桩跑批，默认 10M）
PROFILE=1 ./bench.sh all replay 10m        # → data/profile/runtime.json

# 2. 覆盖差集：真实运行 vs 微基准（需先跑 profile-cov.sh 生成 bench.json）
scripts/compare-profile.sh                  # → 差集 = 需补 test bench 的路径

# 3. 指标偏离：bench 结果 vs VVR/OSS 预期
wf-examples/performance/nexmark_pk/scripts/compare-metrics.sh
# → < 0.5×VVR 标 ⚠（优化候选）；注意插桩跑批 EPS 含 2-5× 开销，
#   指标对比须用正常二进制重跑
```

首跑差集结论（q1/q4/q12 真实运行 vs close_bench 微基准）：
- A. 真实执行、微基准未覆盖 5370 行——大头是 wf-lang/wf-config 编译/校验路径
  （daemon 启动真实编译 wfl/wfs，微基准不经过）→ 补编译类 test bench
- B. 微基准执行、真实未触发 905 行——close_bench 覆盖但 q1/q4 未触发的
  match_engine 分支（each_exec 182 行、columnar 111 行等）；扩大 query 集后收敛

> 注意：插桩构建需 `-Cdebuginfo=1`（llvm-cov 需要行映射）；两次跑批间清理
> profraw；`profile-generate`（PGO）不生成行级映射，llvm-cov 读不了——
> 必须用 `-Cinstrument-coverage`。

## 2026-08-22 首跑结论（close_bench + each_bench，500k 事件）

热点行（每事件调用次数 = 计数 ÷ 500k）：
- `executor/alert.rs:54-60` 每事件 ~144 次（alert 构建路径）
- `eval/mod.rs:25-32`（`EvalTimeScope::enter`）每事件 ~133 次 thread_local 访问——
  诊断计时埋点在生产热路径，纯开销候选
- `types.rs:82-84` 每事件 ~58 次
- `key.rs:444-449`（`eval_field_value_src`）每事件 ~53 次
- `eval/cmp.rs:11-15`（`compare_values`）每事件 ~41 次

热路径缺测试：`event_bridge.rs` 12 行（ColumnarEvent 相关）等，值得补单测。
