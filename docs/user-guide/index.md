# WFL 用户指南

> WarpFusion Language (WFL) v2.1

本目录用于存放面向使用者的文档，按主题拆分，避免所有内容堆在单个文件中。

## 文档导航

- [快速开始](./quick-start.md)
- [核心概念与处理过程](./core-concepts.md)
- [规则编写指南](./rule-writing.md)
- [On Each 与逐条打分](./on-each.md)
- [语言参考](./language-reference.md)
- [运行时配置](./runtime-config.md)
- [开发与测试工具](./tooling.md)

## 推荐阅读顺序

1. 先读 [快速开始](./quick-start.md)，理解 `.wfs` / `.wfl` / `fusion.toml` 三文件模型。
2. 读 [核心概念与处理过程](./core-concepts.md)，理解 window、match、OutputRecord 和 sink 路由。
3. 按 [规则编写指南](./rule-writing.md) 学习常见检测规则写法，包括稳定统计上下文和结构化 `object` / `array` 输出。
4. 如果要做逐条评分、语义事件 enrichment、上游评分下游聚合，读 [On Each 与逐条打分](./on-each.md)。
5. 再读 [运行时配置](./runtime-config.md)，完成 `wfusion` 的 source / sink / runtime 配置。
6. 编写规则时查阅 [语言参考](./language-reference.md)，其中包含 WFS 字段类型、WFL 统计上下文、结构化字面量和函数行为。
7. 做本地验证、回放和数据生成时查阅 [开发与测试工具](./tooling.md)。

## 核心概念

WFL 是 WarpFusion 的检测 DSL，用于编写安全关联检测、告警归并与实体行为分析规则。

固定执行链为：

```text
BIND -> SCOPE(match) -> JOIN -> ENTITY -> YIELD
```

三文件模型如下：

| 文件 | 作用 |
|------|------|
| `.wfs` | 逻辑数据定义（window、field、time、over） |
| `.wfl` | 检测逻辑（bind / match / join / yield） |
| `fusion.toml` | 物理参数（mode、sources、runtime、sinks） |

## 当前运行时约定

- `wfusion` 支持两种模式：`daemon`、`batch`
- 基于 TCP 接收数据的入口是 `[[sources]] type = "tcp"`，不是 `[server]`
- file source 当前支持三种格式：
  - `ndjson`
  - `arrow_framed`
  - `arrow_ipc`

其中：

- `arrow_framed` 表示当前 `wp_arrow` 分帧文件格式
- `arrow_ipc` 表示标准 Arrow IPC file 格式
- 不做自动识别，必须显式写 `format`
