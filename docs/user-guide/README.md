# WFL 用户指南

本目录是 `wp-reactor` 面向规则编写者、运行时配置者和排障人员的使用级文档入口。设计细节放在 `docs/design/`，这里优先回答“怎么写规则、怎么配运行时、怎么验证和排查”。

## 快速入口

| 文档 | 适合场景 |
|------|----------|
| [quick-start.md](./quick-start.md) | 第一次接触项目，快速理解 `.wfs` / `.wfl` / `fusion.toml` 三文件模型 |
| [core-concepts.md](./core-concepts.md) | 理解 window、match、OutputRecord、sink 路由和执行链路 |
| [rule-writing.md](./rule-writing.md) | 编写检测规则，学习常见场景、yield 时间变量、稳定统计上下文和结构化输出 |
| [language-reference.md](./language-reference.md) | 查语法和函数，包括 WFS 字段类型、WFL 表达式、时间变量、统计上下文和结构化字面量 |
| [on-each.md](./on-each.md) | 做逐条评分、语义事件 enrichment、上游评分下游聚合 |
| [runtime-config.md](./runtime-config.md) | 配置 source、sink、window 默认值、运行时参数和输出字段 |
| [operations.md](./operations.md) | 运行、热加载、指标、日志和排障 |
| [tooling.md](./tooling.md) | 使用 `wfl` / `wfgen` / replay / explain 等开发验证工具 |

## 推荐阅读顺序

1. 先读 [快速开始](./quick-start.md)，跑通一个最小规则。
2. 读 [核心概念与处理过程](./core-concepts.md)，理解固定执行链 `EVENTS -> SCOPE(match) -> JOIN -> ENTITY -> YIELD`。
3. 按 [规则编写指南](./rule-writing.md) 学习常见检测规则写法。
4. 写规则时查 [语言参考](./language-reference.md)，尤其是 `yield`、时间变量、统计上下文和函数行为。
5. 接入真实链路时读 [运行时配置](./runtime-config.md) 和 [运维指南](./operations.md)。
6. 需要本地生成数据、回放、解释执行计划时读 [开发与测试工具](./tooling.md)。

## 当前重点能力

- **yield 时间变量**：在输出窗口中声明普通 `time` 字段，并用 `@event_first_time`、`@event_last_time`、`@evidence_start_time`、`@evidence_end_time`、`@window_start_time`、`@window_end_time`、`@emit_time` 显式赋值。
- **稳定统计上下文**：用 `stat.count(window_event(alias))`、`stat.count(match_event(label))`、`stat.count(match_distinct(label))`、`stat.value(trigger(label))`、`stat.value(final(label))` 输出可解释统计证据。
- **结构化输出**：输出 window 可声明 `object`、`array`、`array/T`，规则中用 WFL 字面量构造，避免手写 JSON 字符串。
- **中间 window / pipeline**：`yield` 目标可以作为下游规则输入，用于 enrichment、逐条评分和多阶段聚合。
- **sink 输出控制**：运行时支持 `wf_meta_disable` 禁用 `__wfu_*` 元字段输出，业务字段应在 `yield` 中显式声明和赋值。

## 文档维护约定

- 使用级说明写在本目录；实现原理和设计取舍写到 `docs/design/`。
- 新增 WFL 语法或函数时，同步更新 [language-reference.md](./language-reference.md)，并在 [rule-writing.md](./rule-writing.md) 给出可运行风格的示例。
- 新增运行时配置项时，同步更新 [runtime-config.md](./runtime-config.md) 和必要的排障说明。
- 示例字段名优先使用业务可读名称，例如 `first_seen`、`last_seen`、`rule_window_start`、`latest_analysis_time`，避免暴露内部 `__wfu_*` 名称作为业务接口。
