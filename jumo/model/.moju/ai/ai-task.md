请读取 `.moju/ai/context.md`。

任务：
重构，降低复杂度，补充单元测试

任务 ID：
code.refactor_reduce_complexity

当前视图：
Code Quality

当前选中：file `crates/wf-lang/src/preprocess/mod.rs`

目标质量信息（任务开始前采集，来自 `/Users/zuowenjian/devspace/rust/wfusion/wp-reactor/moju/model/moju-layout/code-quality.json`）：
目标：file `crates/wf-lang/src/preprocess/mod.rs`
- 代码行 535 行（拆分敏感）
- 文件复杂度密度 338.3/KLOC（7/10 档）（拆分敏感）
- 最大圈复杂度 43
- 最长函数 176 行
- 超圈复杂度函数 3 个
- 超长函数 0 个
- 行覆盖率 —
- 目标告警数 4 条

主要问题函数（优先处理）：
- `crates/wf-lang/src/preprocess/mod.rs:79` `preprocess_impl_with_preserved_bare_vars` 圈复杂度 43 · 长度 176 行
- `crates/wf-lang/src/preprocess/mod.rs:556` `try_skip_pattern_block` 圈复杂度 36 · 长度 101 行
- `crates/wf-lang/src/preprocess/mod.rs:295` `yield_preset_decl_range` 圈复杂度 21 · 长度 54 行
- `crates/wf-lang/src/preprocess/mod.rs:421` `find_matching_angle` 圈复杂度 15 · 长度 47 行
- `crates/wf-lang/src/preprocess/mod.rs:378` `skip_param_default_or_separator` 圈复杂度 14 · 长度 42 行

目标相关告警：
- `crates/wf-lang/src/preprocess/mod.rs:295` cyclomatic 21 > 阈值 20
- `crates/wf-lang/src/preprocess/mod.rs:556` cyclomatic 36 > 阈值 20
- `crates/wf-lang/src/preprocess/mod.rs:79` cyclomatic 43 > 阈值 20
- `crates/wf-lang/src/preprocess/mod.rs:79` nesting_depth 9 > 阈值 4

具体要求：
请读取 `.moju/ai/context.md`（其中包含目标的质量基线与最坏函数定位），必要时再读 code-quality.json。针对当前选中的代码模块或文件进行内部重构。
以报告中的模块归属和文件路径确定范围，先核对当前源码，再处理过长文件、过长函数和高圈复杂度。
保持公开接口和业务行为兼容，优先拆分职责、提取内聚函数、降低嵌套和重复逻辑；不要通过删除功能或测试降低指标。
仅修改目标及必要的调用点，补充有意义的单元测试，执行相关测试和构建。
完成后**必须**按任务末尾的「验证与对比」用给定命令重算质量报告，并与基线逐项比较后再下结论；拆分或搬移代码导致的行数/密度变化不能单独算作改善。覆盖率未采集不代表 0%。

验证与对比（必须执行，结论只认这份对照）
1. 完成改动后重算报告，命令必须与下面完全一致（写入 Studio 正在读取的同一份报告，否则界面不会更新）：
   moju-code code-quality /Users/zuowenjian/devspace/rust/wfusion/wp-reactor --out /Users/zuowenjian/devspace/rust/wfusion/wp-reactor/moju/model/moju-layout/code-quality.json
2. 按**同一目标、同一口径**与上面的「基线度量」逐项比较（也可读报告的同名字段）：
   - 稳健指标（可直接判定好坏）：最大圈复杂度、最长函数、超圈复杂度函数数、超长函数数、告警数。只接受持平或改善；退化必须说明原因或回退。
   - 结构敏感指标（**不可单独作为改善证据**）：代码行、每千行复杂度密度、文件数。拆分或搬移代码必然改变它们，必须同时给出模块 subtree 口径的数字才能下结论。
   - 模块目标以 `subtree_*` 为判定口径（代码在目录内移动不影响它），`own_*` 只作参考。
3. 如果拆分产生了新文件，或把代码移到了其他模块/目录，必须显式列出，并说明比较口径；不要只报目标文件的下降。
4. 覆盖率：只有用同一条 `--coverage` 命令重新导入后才可比较；未重新采集不得声称覆盖率改善，也不得把未采集当成 0%。
5. 输出 before → after 对照表，并给出结论（改善 / 持平 / 退化 + 依据）；不得凭主观判断宣称改善。

通用约束：
1. 不要改无关文件。
2. 保持现有模型语义，不要做无关重构。
3. 如果必须修改项目实现代码，先说明原因，并保持改动最小。
4. 最后总结修改内容、验证命令和结果。
