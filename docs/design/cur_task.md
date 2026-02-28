# 当前任务

## WFG 新语法（方案 3）语法讨论结论

- 目标：优先可读性，且规则验证逻辑必须显式。
- 兼容性：不兼容旧 `.wfg`，按新系统设计。
- 元信息：使用 `#[]` 注解；`//` 为注释，`#` 不是注释。
- 命名：`oracle` 改为 `expect`。
- 正确性断言：`hit(rule) / near_miss(rule) / miss(rule)` 百分比阈值。
- 生成模型：`stream` 为中心，window 约束由 `.wfs/.wfl` 推导。
- 注入模型：
  - `hit<30%> / near_miss<10%> / miss<60%> <stream> { ... }`
  - 序列写法：`user seq { use(... ) with(count,window) ... }`
  - 标签（hit/near_miss/miss）仅表示样本类别，不隐式代表规则逻辑。
- 速率模型：支持稳定、`wave`、`burst`、`timeline`。
