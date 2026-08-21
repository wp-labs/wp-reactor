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

## 2026-08-22 首跑结论（close_bench + each_bench，500k 事件）

热点行（每事件调用次数 = 计数 ÷ 500k）：
- `executor/alert.rs:54-60` 每事件 ~144 次（alert 构建路径）
- `eval/mod.rs:25-32`（`EvalTimeScope::enter`）每事件 ~133 次 thread_local 访问——
  诊断计时埋点在生产热路径，纯开销候选
- `types.rs:82-84` 每事件 ~58 次
- `key.rs:444-449`（`eval_field_value_src`）每事件 ~53 次
- `eval/cmp.rs:11-15`（`compare_values`）每事件 ~41 次

热路径缺测试：`event_bridge.rs` 12 行（ColumnarEvent 相关）等，值得补单测。
