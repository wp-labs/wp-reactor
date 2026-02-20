# WP-Reactor 代码学习路径

按**自底向上、先静态后动态**的顺序组织，沿依赖图从叶子 crate 向入口 crate 推进。

---

## 阶段 0：建立全局认知

先读示例文件，建立对"三文件架构"的感性认识：

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 0-1 | `examples/security.wfs` | 窗口是什么？stream/time/over/fields 各代表什么 |
| 0-2 | `examples/brute_force.wfl` | 规则长什么样？events/match/on event/on close/entity/yield |
| 0-3 | `examples/fusion.toml` | 运行时配置如何把 .wfs + .wfl 组装起来 |

**目标**：能用自然语言描述"3 次登录失败 → 产出告警"的完整语义。

---

## 阶段 1：DSL 层 — `wf-lang`

零外部依赖的纯解析/编译层，最适合独立学习。

### 1A：数据结构

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1-1 | `crates/wf-lang/src/schema.rs` | 核心数据类型：`BaseType`、`FieldDef`、`WindowSchema` |
| 1-2 | `crates/wf-lang/src/ast.rs` | WFL AST 节点：`RuleDecl`、`MatchClause`、`StepExpr`、`YieldClause` |
| 1-3 | `crates/wf-lang/src/plan.rs` | 编译产物：`RulePlan`、`MatchPlan`、`BranchPlan`、`EntityPlan` |

### 1B：解析器

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1-4 | `crates/wf-lang/src/wfs_parser/mod.rs` | winnow 组合子解析 .wfs → `WindowSchema`，学 parser combinator 模式 |
| 1-5 | `crates/wf-lang/src/wfl_parser/mod.rs` | WFL 主解析入口 |
| 1-6 | `crates/wf-lang/src/wfl_parser/expr.rs` | 表达式解析：算术/比较/逻辑/函数调用 |
| 1-7 | `crates/wf-lang/src/wfl_parser/match_p.rs` | match 子句解析：on event / on close / score |
| 1-8 | `crates/wf-lang/src/preprocess.rs` | `$VAR` 变量替换预处理器 |

### 1C：编译与检查

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1-9 | `crates/wf-lang/src/compiler/mod.rs` | AST → Plan 的编译过程 |
| 1-10 | `crates/wf-lang/src/checker/types.rs` | 类型系统与推导 |
| 1-11 | `crates/wf-lang/src/checker/scope.rs` | 变量作用域追踪 |
| 1-12 | `crates/wf-lang/src/checker/rules.rs` | 规则级语义检查 |

### 验证练习

```bash
cargo test -p wf-lang -- --nocapture   # 160 个测试，对照 test 用例理解 parser 行为
```

---

## 阶段 2：配置层 — `wf-config`

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 2-1 | `crates/wf-config/src/types.rs` | `HumanDuration`("5m")、`ByteSize`("256MB") 的自定义 serde 反序列化 |
| 2-2 | `crates/wf-config/src/window.rs` | `WindowDefaults` + `WindowOverride` 的 merge 逻辑 |
| 2-3 | `crates/wf-config/src/alert.rs` | `SinkUri` 枚举、`file://` URI 解析 |
| 2-4 | `crates/wf-config/src/fusion.rs` | `FusionConfig` 总结构，`impl FromStr` 解析 TOML |
| 2-5 | `crates/wf-config/src/validate.rs` | over vs over_cap 交叉校验 |

### 验证练习

```bash
cargo test -p wf-config -- --nocapture
```

---

## 阶段 3：核心引擎 — `wf-core`

系统的心脏，分**窗口子系统**和**规则子系统**两条线。建议投入最多精力。

### 3A：窗口子系统

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 3A-1 | `crates/wf-core/src/window/buffer.rs` | `Window` 结构：时间有序 RecordBatch 队列、watermark、内存驱逐、snapshot |
| 3A-2 | `crates/wf-core/src/window/registry.rs` | `WindowRegistry`：stream\_tag → Window 订阅表的构建 |
| 3A-3 | `crates/wf-core/src/window/router.rs` | `Router`：(stream\_tag, batch) → 多窗口路由，watermark 过滤 |
| 3A-4 | `crates/wf-core/src/window/evictor.rs` | `Evictor`：两阶段驱逐（TTL → 全局内存贪心） |

### 3B：规则子系统（核心中的核心）

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 3B-1 | `crates/wf-core/src/rule/event_bridge.rs` | Arrow RecordBatch → `Vec<Event>` 转换桥接 |
| 3B-2 | `crates/wf-core/src/rule/match_engine.rs` | **CEP 状态机**：`Instance`、`BranchState`、聚合函数、`StepResult` 三态（Accumulate / Advance / Matched）、scope key 隔离 |
| 3B-3 | `crates/wf-core/src/rule/tests/cep_core.rs` | 对照测试学状态机：单步阈值、多步顺序、OR 分支、distinct、composite key |
| 3B-4 | `crates/wf-core/src/rule/tests/close.rs` | on close 路径：flush/timeout → evaluate\_close → execute\_close |
| 3B-5 | `crates/wf-core/src/rule/executor.rs` | `RuleExecutor`：`execute_match()` / `execute_close()` → `AlertRecord` |
| 3B-6 | `crates/wf-core/src/alert/types.rs` | `AlertRecord` 字段：alert\_id 幂等键、score、close\_reason |
| 3B-7 | `crates/wf-core/src/alert/sink.rs` | `AlertSink` trait + `FileAlertSink` + `FanOutSink` |

### 验证练习

```bash
cargo test -p wf-core -- --nocapture   # 90 个测试，重点关注 cep_core 和 close 模块
```

---

## 阶段 4：异步运行时 — `wf-runtime`

将阶段 3 的同步引擎用 tokio 编排起来。

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 4-1 | `crates/wf-runtime/src/receiver.rs` | TCP 监听 → 长度前缀帧 → Arrow IPC 解码 → event channel |
| 4-2 | `crates/wf-runtime/src/scheduler.rs` | **调度器核心循环**：event dispatch、Semaphore 背压、1s timeout scan、shutdown drain + flush |
| 4-3 | `crates/wf-runtime/src/alert_task.rs` | 告警消费者任务，channel close 即退出 |
| 4-4 | `crates/wf-runtime/src/evictor_task.rs` | 定时驱逐任务 |
| 4-5 | `crates/wf-runtime/src/schema_bridge.rs` | WindowSchema × WindowConfig → WindowDef 桥接 |
| 4-6 | `crates/wf-runtime/src/lifecycle.rs` | **FusionEngine**：启动 12 步编排、TaskGroup LIFO 关闭顺序 |

### 重点理解

TaskGroup 的启动/关闭顺序为什么是 LIFO：

```
启动顺序:  alert → infra → scheduler → receiver
关闭顺序:  receiver → scheduler → alert → infra
```

上游生产者（receiver）先退出，下游消费者（alert sink）最后退出，保证零告警丢失。

### 验证练习

```bash
cargo test -p wf-runtime -- --nocapture   # 15 单元测试 + 1 e2e 测试
```

---

## 阶段 5：端到端 — 串联全流程

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 5-1 | `crates/wf-runtime/tests/e2e_mvp.rs` | 完整数据流：构造 config → 启动引擎 → 发送 TCP Arrow 帧 → shutdown flush → 验证告警文件 |
| 5-2 | `crates/wf-cli/src/main.rs` | 生产入口：clap CLI → 加载 fusion.toml → 信号处理 → 优雅关闭 |

### 动手练习

在 `examples/` 下用 CLI 启动引擎，手动理解完整生命周期：

```bash
cargo run -p wf-cli -- run --config examples/fusion.toml
```

---

## 阶段 6（可选）：测试数据生成 — `wf-datagen`

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 6-1 | `crates/wf-datagen/src/wfg_parser/` | .wfg 场景 DSL 解析 |
| 6-2 | `crates/wf-datagen/src/datagen/` | 事件生成 + 注入 + 故障注入（duplicate / drop / reorder） |
| 6-3 | `crates/wf-datagen/src/oracle.rs` | 预言机：给定事件流 → 预计告警 |
| 6-4 | `crates/wf-datagen/src/verify.rs` | 实际告警 vs 预期告警的 diff |

---

## 附录 A：依赖关系图

```
wf-cli ─────────┐
                 ├─► wf-runtime
                 │     ├─► wf-core
                 │     │     ├─► wf-lang     (解析 + 编译)
                 │     │     ├─► wf-config   (配置)
                 │     │     └─► arrow       (列式内存)
                 │     ├─► wf-config
                 │     ├─► wf-lang
                 │     ├─► wp-arrow          (IPC 编解码)
                 │     └─► tokio             (异步运行时)
                 └─► wf-config

wf-datagen (独立二进制)
  ├─► wf-core
  ├─► wf-lang
  └─► arrow
```

---

## 附录 B：核心数据流

```
.wfs + .wfl + fusion.toml
         │
    ┌────▼────┐
    │ wf-lang │  解析 + 编译 → RulePlan
    └────┬────┘
         │
    ┌────▼──────┐
    │ wf-config │  加载 TOML → FusionConfig
    └────┬──────┘
         │
    ┌────▼────┐
    │ wf-core │  WindowRegistry + CepStateMachine + RuleExecutor
    └────┬────┘
         │
    ┌────▼───────┐
    │ wf-runtime │  Receiver → Router → Scheduler → AlertSink
    └────┬───────┘
         │
    ┌────▼────┐
    │ wf-cli  │  main() 入口
    └─────────┘
```

运行时数据流：

```
TCP 客户端发送: [4B 长度][stream_name][Arrow IPC batch]
        │
        ▼
   ┌──────────┐
   │ Receiver │  接受 TCP → 解码 Arrow IPC → 发送到 event channel
   └────┬─────┘
        │
        ▼
   ┌────────┐
   │ Router │  查询 Window 订阅表 → 路由 batch 到匹配窗口
   └────┬───┘
        │
        ▼
   ┌───────────┐
   │ Scheduler │  为每个相关 RuleEngine 分发 → Semaphore 控制并发
   └─────┬─────┘
         │
         ▼
   ┌───────────────┐
   │ Rule 执行任务  │  batch → Events → CEP 状态机推进 → AlertRecord
   └───────┬───────┘
           │
           ▼
   ┌───────────┐
   │ AlertSink │  接收 AlertRecord → 序列化 JSON → 写入文件
   └───────────┘
```

---

## 附录 C：关键文件速查表

| 组件 | 文件 |
|------|------|
| WFS 解析器 | `crates/wf-lang/src/wfs_parser/mod.rs` |
| WFL 解析器 | `crates/wf-lang/src/wfl_parser/mod.rs` |
| WFL 编译器 | `crates/wf-lang/src/compiler/mod.rs` |
| 类型检查器 | `crates/wf-lang/src/checker/rules.rs` |
| 窗口缓冲 | `crates/wf-core/src/window/buffer.rs` |
| CEP 状态机 | `crates/wf-core/src/rule/match_engine.rs` |
| 规则执行器 | `crates/wf-core/src/rule/executor.rs` |
| 路由器 | `crates/wf-core/src/window/router.rs` |
| 驱逐器 | `crates/wf-core/src/window/evictor.rs` |
| 调度器 | `crates/wf-runtime/src/scheduler.rs` |
| TCP 接收器 | `crates/wf-runtime/src/receiver.rs` |
| 生命周期管理 | `crates/wf-runtime/src/lifecycle.rs` |
| 配置加载 | `crates/wf-config/src/fusion.rs` |
| CLI 入口 | `crates/wf-cli/src/main.rs` |
| E2E 测试 | `crates/wf-runtime/tests/e2e_mvp.rs` |

---

## 建议节奏

- **阶段 0-1**：建立语感，理解 DSL 设计。快的话一天，慢的话两天。
- **阶段 2**：快速过，半天足够。
- **阶段 3**：投入最多精力。`match_engine.rs` 是全系统最核心的文件，建议结合测试用例逐函数阅读。
- **阶段 4**：理解并发编排模式，重点是 scheduler 事件循环和 lifecycle 启停顺序。
- **阶段 5**：串联验证，确认端到端理解无盲区。
- **阶段 6**：按需深入。
