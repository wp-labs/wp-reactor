# Jumo AI Context

This file is context for `.jumo/ai/ai-task.md`. Follow the selected AI task, not a generic fix task.

## Project
/Users/zuowenjian/devspace/rust/wfusion/wp-reactor/jumo/model

## Active View
Code Quality

## Active Domain


## Selected Element
file `crates/wf-cep/src/cep/eval/cmp.rs`

## Model Summary
代码质量报告为采集快照，重构前须核对当前源码；下方度量为派发任务时的值，任务完成后须重算并比较。

## Quality Target

目标：file `crates/wf-cep/src/cep/eval/cmp.rs`

### 基线度量（派发任务时采集）
- 代码行 389 行（拆分敏感）
- 文件复杂度密度 228.8/KLOC（5/10 档）（拆分敏感）
- 最大圈复杂度 13
- 最长函数 31 行
- 超圈复杂度函数 0 个
- 超长函数 0 个
- 行覆盖率 —
- 目标告警数 0 条

### 主要问题函数
- `crates/wf-cep/src/cep/eval/cmp.rs:86` `try_eval_expr_to_f64` 圈复杂度 13 · 长度 31 行
- `crates/wf-cep/src/cep/eval/cmp.rs:24` `compare_strs` 圈复杂度 7 · 长度 12 行
- `crates/wf-cep/src/cep/eval/cmp.rs:46` `compare_cmp` 圈复杂度 7 · 长度 11 行
- `crates/wf-cep/src/cep/eval/cmp.rs:64` `from_binop` 圈复杂度 7 · 长度 11 行
- `crates/wf-cep/src/cep/eval/cmp.rs:170` `round_with_precision` 圈复杂度 6 · 长度 15 行

### 目标相关告警
- 无

### 报告与复算
- 报告：`/Users/zuowenjian/devspace/rust/wfusion/wp-reactor/jumo/model/jumo-layout/code-quality.json`
- 复算命令：`jumo-code code-quality /Users/zuowenjian/devspace/rust/wfusion/wp-reactor --out /Users/zuowenjian/devspace/rust/wfusion/wp-reactor/jumo/model/jumo-layout/code-quality.json`

## Diagnostics
- none

## Related Files
- /Users/zuowenjian/devspace/rust/wfusion/wp-reactor/jumo/model/jumo-layout/code-quality.json

## Source Snippets
No source snippets were available.

## Working Rules
- Use `.jumo/ai/ai-task.md` as the task source.
- Keep changes focused on relevant `.mju`, `layout.json`, or necessary documentation files.
- Do not introduce duplicate definitions.
- Run the relevant `jumo verify .` / `jumo readiness .`, or the project's existing validation command.
