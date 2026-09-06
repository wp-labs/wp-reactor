# Moju AI Context

This file is context for `.moju/ai/ai-task.md`. Follow the selected AI task, not a generic fix task.

## Project
/Users/zuowenjian/devspace/rust/wfusion/wp-reactor/moju/model

## Active View
Code Quality

## Active Domain


## Selected Element
file `crates/wf-lang/src/preprocess/mod.rs`

## Model Summary
代码质量报告为采集快照，重构前须核对当前源码；下方度量为派发任务时的值，任务完成后须重算并比较。

## Quality Target

目标：file `crates/wf-lang/src/preprocess/mod.rs`

### 基线度量（派发任务时采集）
- 代码行 535 行（拆分敏感）
- 文件复杂度密度 338.3/KLOC（7/10 档）（拆分敏感）
- 最大圈复杂度 43
- 最长函数 176 行
- 超圈复杂度函数 3 个
- 超长函数 0 个
- 行覆盖率 —
- 目标告警数 4 条

### 主要问题函数
- `crates/wf-lang/src/preprocess/mod.rs:79` `preprocess_impl_with_preserved_bare_vars` 圈复杂度 43 · 长度 176 行
- `crates/wf-lang/src/preprocess/mod.rs:556` `try_skip_pattern_block` 圈复杂度 36 · 长度 101 行
- `crates/wf-lang/src/preprocess/mod.rs:295` `yield_preset_decl_range` 圈复杂度 21 · 长度 54 行
- `crates/wf-lang/src/preprocess/mod.rs:421` `find_matching_angle` 圈复杂度 15 · 长度 47 行
- `crates/wf-lang/src/preprocess/mod.rs:378` `skip_param_default_or_separator` 圈复杂度 14 · 长度 42 行

### 目标相关告警
- `crates/wf-lang/src/preprocess/mod.rs:295` cyclomatic 21 > 阈值 20
- `crates/wf-lang/src/preprocess/mod.rs:556` cyclomatic 36 > 阈值 20
- `crates/wf-lang/src/preprocess/mod.rs:79` cyclomatic 43 > 阈值 20
- `crates/wf-lang/src/preprocess/mod.rs:79` nesting_depth 9 > 阈值 4

### 报告与复算
- 报告：`/Users/zuowenjian/devspace/rust/wfusion/wp-reactor/moju/model/moju-layout/code-quality.json`
- 复算命令：`moju-code code-quality /Users/zuowenjian/devspace/rust/wfusion/wp-reactor --out /Users/zuowenjian/devspace/rust/wfusion/wp-reactor/moju/model/moju-layout/code-quality.json`

## Diagnostics
- none

## Related Files
- /Users/zuowenjian/devspace/rust/wfusion/wp-reactor/moju/model/moju-layout/code-quality.json

## Source Snippets
No source snippets were available.

## Working Rules
- Use `.moju/ai/ai-task.md` as the task source.
- Keep changes focused on relevant `.mju`, `layout.json`, or necessary documentation files.
- Do not introduce duplicate definitions.
- Run the relevant `moju verify .` / `moju readiness .`, or the project's existing validation command.
