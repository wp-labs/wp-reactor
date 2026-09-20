# conv 表达式完全未做语义检查（独立 issue 草稿）

> **状态：未完成**。本轮只补齐了 conv 的**位置依赖函数闸门**（下节「修复进展」），
> 通用类型检查（字段解析 / 类型兼容 / `let` 引用 / `where` 非 bool / `dedup`·`sort` 键的
> 结构化字段）**仍然缺失**——字段拼错、类型不符在 conv 里依旧静默。
>
> 由 warp-fusion#101 同类位置排查中发现；**不属于** #101 的修复范围，单独立项。

## 问题摘要

`conv { ... }` 链里的 `sort` / `dedup` / `where` 表达式**完全不经过语义检查**：

- `crates/wf-lang/src/checker/rules/conv_check.rs` 的 `check_conv` 只校验
  「conv 与派生/嵌套 key 互斥」「window mode 必须 fixed/hop」「`top_ties` 需前导 `sort`」，
  **没有对任何 conv 表达式调用 `check_expr_type`**。
- 因此字段拼写错误、类型错误、未声明的 `let`、位置依赖函数等**全部编译期静默通过**。

## 运行期事实

conv 表达式由 wf-cep 在收口批上**按 output 逐行**求值：

- `crates/wf-cep/src/cep/conv.rs:78/176/203/215` → `eval_expr(&expr, &ctx)`
- ctx 由 `build_eval_context(o, keys)`（`conv.rs:227`）构造，**只注入 scope key 与 step label**
  （`event_step_data` / `close_step_data` 的 measure value），没有 `_step_*` 序列、没有窗口表、
  没有滚动基线状态。

后果：

- 字段名拼错 / 类型不符 → 求值为 `None`：`where` 恒不通过（**整批被静默过滤**）、
  `dedup` 全部落到 `"__none__"`（**只剩一条**）、`sort` 键恒相等（**排序静默失效**）。
- L3 集合函数 / `window.has(...)` / `baseline(...)` → 同 #101 类静默为空
  （**这一项已在本轮修复中通过位置闸门拒绝**，见下）。

## 修复进展

本轮（#101 同类位置修复）已补齐 conv 的**位置依赖函数闸门**：
`conv_check.rs` 现在对 `sort` / `dedup` / `where` 调用
`check_expr_position(.., ExprPosition::ConvExpr, ..)`，拒绝 L3 / `window.has` / `baseline`。

仍然缺失的是**通用类型检查**（字段解析、类型兼容、`let` 引用等），本 issue 承接该部分。

## 建议修复

1. 在 `check_conv` 中为每个 conv 表达式接上 `check_expr_type`
   （用与 close 输出同源的 scope：key 字段 + step label + 规则级 `let`）。
2. `where` 追加 bool 类型校验（对齐 `rule.r#where` / stats `where` 的做法）。
3. `dedup` / `sort` 键做「可排序/可哈希标量」校验（object/array 拒绝），
   对齐 join key 的 `is_structured_key_type` 口径。
4. 补 checker 回归：字段拼错、类型不符、未声明 `let`、`where` 非 bool、`dedup` 结构化字段
   → 编译期报错。

## 验收标准

- conv 表达式里的字段拼写错误在编译期报错（含可定位信息）；
- `where` 非 bool、`dedup`/`sort` 用 object/array 字段编译期拒绝；
- 既有合法 conv 规则（`examples/conv`、checker/parser conv 测试）保持通过；
- 不引入与 close 输出语义不一致的重复实现（复用同一 scope 构造路径）。

## 证据索引

| 项 | 位置 |
|---|---|
| checker 缺类型检查 | `crates/wf-lang/src/checker/rules/conv_check.rs` |
| AST | `crates/wf-lang/src/ast/conv.rs`（`ConvStep::{Sort,Dedup,Where}`） |
| 运行期求值（无动态上下文） | `crates/wf-cep/src/cep/conv.rs:78,176,203,215` |
| conv ctx 构造 | `crates/wf-cep/src/cep/conv.rs:227` |
