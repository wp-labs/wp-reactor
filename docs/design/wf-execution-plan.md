# WarpFusion 执行计划
<!-- 角色：架构师 / 项目管理 | 创建：2026-02-15 | 更新：2026-02-19 -->

> 本文档将 WarpFusion 引擎基建（[10-warp-fusion.md](10-warp-fusion.md) P0–P7）、WFL 语言实现（[wfl-desion.md](wfl-desion.md) Phase A–D）与测试数据生成（[wfl-desion.md](wfl-desion.md) §18）统一拆分为 33 个里程碑（M01–M33），分属十一个阶段。

## 总览

```
阶段 I ─ 数据基建          阶段 II ─ 配置与窗口       阶段 III ─ WFL 编译器
M01 Arrow 类型映射    ✅   M06 TOML 配置解析    ✅   M11 WFL 词法语法+AST  ✅
M02 Arrow 行列转换    ✅   M07 .wfs Schema 解析  ✅   M12 语义检查+变量预处理✅
M03 Arrow IPC 编解码  ✅   M08 Window 类型与缓冲 ✅   M13 编译器 → RulePlan ✅
M04 Arrow IPC Sink    ✅   M09 WindowRegistry 路由✅
M05 Sink 断连重连     ✅   M10 Router + Evictor  ✅

阶段 IV ─ 执行引擎        阶段 V ─ 运行时与闭环       阶段 VI ─ 生产化
M14 MatchEngine CEP   ✅   M17 Receiver          ✅   M21 热加载
M15 缺失检测+超时     ✅   M18 Scheduler+Lifecycle     M22 多通道告警+去重
M16 RuleExecutor+join       M19 告警系统                M23 监控+性能
                            M20 ★ E2E MVP 验收          M24 开发者工具链

阶段 VII ─ WFL L2         阶段 VIII ─ WFL L3          阶段 IX ─ 行为分析     阶段 X ─ 分布式
M25 join+baseline+has      M27 tumble+conv             M29 session+collect     M30 分布式 V2
    +key映射+limits            +composable pattern          +statistics+baseline
M26 条件/字符串/时间       M28 |> 管道+隐式yield            +score
    +replay+yield契约

阶段 XI ─ 测试数据生成 (wf-datagen)
M31 .wfg Parser+随机生成   ✅    M32 Rule-aware+Oracle+Verify ✅   M33 时序扰动+压测 ✅
```

---

## 阶段 I：数据传输基建（M01–M05）

### M01：wf-arrow 类型映射 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-arrow` |
| 范围 | DataType → Arrow DataType 映射（8 种基础类型 + Array 递归）；`to_arrow_schema(Vec<FieldDef>) → Schema` |
| 依赖 | 无 |
| 验收 | 单元测试：Chars/Digit/Float/Bool/Time/IP/Hex/Array 全覆盖 |
| 状态 | **已完成** — `wf-arrow/src/schema.rs` 实现 WpDataType 映射 |

### M02：wf-arrow 行列转换 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-arrow` |
| 范围 | `records_to_batch(Vec<DataRecord>, Schema) → RecordBatch`；`batch_to_records(RecordBatch) → Vec<DataRecord>` |
| 依赖 | M01 |
| 验收 | 往返一致性测试：record → batch → record == 原始数据；空记录 / 大批量边界测试 |
| 状态 | **已完成** — `wf-arrow/src/convert.rs` 实现 |

### M03：wf-arrow IPC 编解码 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-arrow` |
| 范围 | `encode_ipc` / `decode_ipc`（帧格式：4B BE len + stream_tag + Arrow IPC RecordBatch）；帧读取器（从 TCP 流中按长度前缀切分帧） |
| 依赖 | M02 |
| 验收 | 编解码往返测试；帧头字段完整性校验；畸形帧拒绝测试 |
| 状态 | **已完成** — `wf-arrow/src/ipc.rs` 实现 |

### M04：wp-motor Arrow IPC Sink ✅

| 项目 | 内容 |
|------|------|
| crate | `wp-motor` |
| 范围 | ArrowIpcSink 基础实现（TCP 单向推送）；连接状态机（Connected / Disconnected / Stopped） |
| 依赖 | M03 |
| 验收 | Sink 通过 TCP 发送 Arrow IPC 消息，对端 `decode_ipc` 解码正确 |
| 状态 | **已完成** — `wp-motor/src/sinks/backends/arrow_ipc.rs` 实现 |

### M05：Sink 断连重连 ✅

| 项目 | 内容 |
|------|------|
| crate | `wp-motor` |
| 范围 | 断连后指数退避重连（1s→2s→4s→…→30s 封顶）；重连成功后继续推送新数据（无 WAL，断连期间数据丢弃） |
| 依赖 | M04 |
| 验收 | 断连→退避→重连 全流程测试；重连后正常推送测试 |
| 状态 | **已完成** — 指数退避逻辑集成在 ArrowIpcSink 中 |

---

## 阶段 II：配置与窗口运行时（M06–M10）

### M06：wf-config TOML 解析 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-config` |
| 范围 | fusion.toml 完整解析：`[server]`/`[runtime]`/`[window_defaults]`/`[window.*]`/`[alert]`；over vs over_cap 校验（不满足报错拒绝启动）；配置默认值继承（window 级覆盖 defaults） |
| 依赖 | 无（可与 M01-M05 并行） |
| 验收 | fusion.toml 示例加载成功；非法配置报错测试（over > over_cap、缺失必填项） |
| 状态 | **已完成** — `wf-config/src/fusion.rs` 实现 |

### M07：Window Schema (.wfs) 解析器 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` |
| 范围 | .wfs EBNF → winnow 解析器；产出 WindowSchema 数据结构；语义约束：window 名称全局唯一、over > 0 时 time 必选且为 time 类型、over = 0 表示静态集合 |
| 依赖 | 无（可与 M06 并行） |
| 验收 | security.wfs 示例解析成功；违反约束报错测试 |
| 状态 | **已完成** — `wf-lang/src/wfs_parser/mod.rs` 实现 |

### M08：Window 类型与缓冲核心 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-core/window` |
| 范围 | WindowSchema + WindowRtConfig + DistMode + LatePolicy 类型定义；Window 缓冲实现：`append(RecordBatch)` / `snapshot() → Vec<RecordBatch>` / `evict_expired(now)` / `memory_usage() → usize`；TimedBatch（event_time_range + byte_size） |
| 依赖 | M01（Arrow Schema 映射） |
| 验收 | 追加数据 + 时间淘汰测试；snapshot 只读不阻塞写入测试 |
| 状态 | **已完成** — `wf-core/src/window/buffer.rs` 实现 |

### M09：WindowRegistry 订阅路由 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-core/window` |
| 范围 | WindowRegistry：`windows: HashMap<String, Arc<RwLock<Window>>>`；订阅注册：扫描 WindowSchema 构建 `subscriptions: HashMap<String, Vec<Subscription>>`；多流联合支持（stream = ["syslog", "winlog"]） |
| 依赖 | M08 |
| 验收 | 单流路由 + 多流联合路由测试；Window 名称查询测试 |
| 状态 | **已完成** — `wf-core/src/window/registry.rs` 实现 |

### M10：Router 数据分发 + Evictor ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-core/window` |
| 范围 | Router 单机直发（Local 模式，batch → window.append）；Watermark 推进逻辑（`max(event_time) - watermark_delay`）；LatePolicy 处理（Drop / 预留 Revise + SideOutput 接口）；Evictor 定时扫描 + max_window_bytes / max_total_bytes 淘汰 |
| 依赖 | M09 |
| 验收 | Arrow IPC → Router → Window 数据入库端到端测试；超 max_window_bytes 自动淘汰测试；迟到数据 Drop 测试 |
| 状态 | **已完成** — `wf-core/src/window/router.rs` + `evictor.rs` 实现 |

---

## 阶段 III：WFL L1 编译器（M11–M13）

### M11：WFL L1 词法语法解析 + AST ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` |
| 范围 | WFL L1 词法分析器（关键字、标识符、字面量、运算符）；L1 语法解析器（winnow）覆盖：`use` / `rule` / `meta` / `events` / `match<key:dur>` / `yield` / `-> severity` / `-> score(expr)` / `entity()` / `fmt()` / OR 分支 / 复合 key / `on event` + `on close`；AST 定义：RuleNode / MetaNode / EventsNode / MatchNode / StepNode / YieldNode / ExprNode |
| 依赖 | M07（.wfs 解析器复用基础 winnow 设施） |
| 验收 | brute_scan.wfl 等 L1 规则解析为 AST；语法错误定位测试（行号+列号） |
| 状态 | **已完成** — `wf-lang/src/ast.rs` + `wf-lang/src/wfl_parser/` 实现 |

### M12：语义检查 + 变量预处理 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` |
| 范围 | `$VAR` / `${VAR:default}` 变量替换预处理（编译前阶段）；语义检查器：① .wfs ↔ .wfl 交叉校验（`use` 引用的 window 必须存在、字段类型一致）② 类型约束 T1–T10 检查（聚合输入类型、比较类型对齐等）③ 语义约束 R1–R6（match 必须有 events、yield 字段子集映射等） |
| 依赖 | M11, M07 |
| 验收 | 变量替换测试；非法规则全面报错：未定义 window、字段类型不匹配、聚合在非 digit/float 字段上、yield 字段不在目标 window 中 |
| 状态 | **已完成** — `wf-lang/src/checker/` 实现 |

### M13：WFL 编译器 → RulePlan ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` |
| 范围 | AST → CepStateMachine（步骤序列 + 转换条件 + 超时 + OR 分支）；AST → join SQL 生成（DataFusion SQL 方言）；AST → severity 映射（`-> high` 静态 / `-> { expr => level }` 动态 / `-> score(expr)` 评分）；聚合 desugar（`alias.field | distinct | count` → 聚合 IR） |
| 依赖 | M12 |
| 验收 | brute_scan.wfl 编译为 RulePlan（状态机步骤 + join SQL + severity 全部正确）；entity() 编译为系统字段注入 |
| 状态 | **已完成** — `wf-lang/src/compiler/` 实现，产出 `plan.rs` 中定义的 RulePlan |

---

## 阶段 IV：规则执行引擎（M14–M16）

### M14：MatchEngine CEP 状态机 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-core/rule` |
| 范围 | CepStateMachine 运行时：事件到达 → `advance(event)` → `StepResult { Accumulate / Advance / Matched }`；多步序列匹配（step1 → step2 → ... → matched）；OR 分支（`branch_a || branch_b ;`）：任一分支完成即推进；复合 key 分组（`match<sip,dport:5m>`）：按 key 组合隔离状态机实例 |
| 依赖 | M13（RulePlan 定义） |
| 验收 | 单步阈值匹配测试；多步序列匹配测试；OR 分支测试；复合 key 隔离测试 |
| 状态 | **已完成** — `wf-core/src/rule/match_engine.rs` 实现，11 项测试通过 |

### M15：缺失检测 + 超时管理 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-core/rule` |
| 范围 | `on close` 步骤求值：窗口关闭时对 `close_steps` 求值（如 `count == 0` 检测缺失响应）；`close_reason` 上下文注入（`timeout` / `flush` / `eos`），close guard 可按原因分流；maxspan 过期：超过 `WindowSpec::Sliding(dur)` 后自动过期并触发 `on close` 求值；定时扫描接口 `scan_expired(now)` 供 Scheduler 调用 |
| 依赖 | M14 |
| 验收 | A → NOT B 缺失检测场景（请求无响应）；maxspan 过期自动重置测试；on close 触发求值测试；close_reason guard 过滤测试 |
| 状态 | **已完成** — `CloseReason`/`CloseOutput` 类型 + `close()`/`scan_expired()` API，10 项新测试通过（共 21 项） |

### M16：RuleExecutor + DataFusion join

| 项目 | 内容 |
|------|------|
| crate | `wf-core/rule` |
| 范围 | RuleExecutor 集成：状态机完成 → 创建 SessionContext → 注册 Window 快照为临时表 → 执行 join SQL → 产出 AlertRecord；severity 求值（静态 / 动态 / score）；空窗口安全（`RecordBatch::new_empty(schema)`） |
| 依赖 | M14, M08（Window snapshot） |
| 验收 | 事件序列 → 状态机完成 → join SQL 执行 → AlertRecord 字段正确；空窗口不报错测试 |

---

## 阶段 V：运行时与单机闭环（M17–M20）

### M17：Receiver ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-runtime` |
| 范围 | Receiver task：TCP 监听（accept 多连接：wp-motor + 分布式节点）；按长度前缀分帧；Arrow IPC 解码；投递到 Router |
| 依赖 | M10（Router）, M03（IPC 解码） |
| 验收 | 多连接并发接收测试；持续接收 wp-motor 数据测试；连接断开不影响其他连接测试 |
| 状态 | **已完成** — `wf-runtime/src/receiver.rs` 实现，3 项测试通过 |

### M18：Scheduler + Lifecycle

| 项目 | 内容 |
|------|------|
| crate | `runtime` |
| 范围 | Scheduler task：`tokio::select!` 事件驱动分发 + 超时扫描循环 + 控制命令接收；全局并发上限（`Semaphore(executor_parallelism)`）；执行超时（`tokio::time::timeout` 包裹 join SQL）；Lifecycle：启动顺序（AlertSink → Executor → Scheduler → Evictor → Router → Receiver）；信号处理（SIGTERM/SIGINT）；优雅关闭（先停 Receiver，等执行完毕） |
| 依赖 | M16（RuleExecutor）, M17（Receiver） |
| 验收 | 多规则并行分发测试；并发上限背压测试；Ctrl-C 优雅关闭测试 |

### M19：告警系统

| 项目 | 内容 |
|------|------|
| crate | `wf-core/alert` |
| 范围 | AlertRecord 定义（rule_name / severity / fired_at / matched_rows / summary）；`alert_id = sha256(rule_name + scope_key + window_range)` 幂等键生成；FileAlertSink 实现（JSON Lines 写入文件，含 alert_id 字段） |
| 依赖 | M16（AlertRecord 产出） |
| 验收 | 告警写入 JSON Lines 文件；alert_id 相同输入一致性测试；文件可被 jq 解析 |

### M20：端到端 MVP 验收 ★

| 项目 | 内容 |
|------|------|
| 范围 | **集成验收**：wp-motor 发送模拟日志 → TCP + Arrow IPC 传输 → WarpFusion 接收 → brute_force_then_scan 规则触发 → 告警写入文件；CLI 启动命令（`wf run -c fusion.toml`） |
| 依赖 | M05, M06, M07, M13, M18, M19（全链路） |
| 验收 | **单机 MVP 达成**：一条完整 L1 规则从数据接收到告警输出全流程跑通；可作为独立进程启动运行 |

---

## 阶段 VI：生产化（M21–M24）

### M21：热加载

| 项目 | 内容 |
|------|------|
| crate | `runtime` |
| 范围 | `.wfl` 热加载触发方式：CLI（`wf reload`）+ HTTP API（`POST /api/reload`）；加载流程：加载 .wfl → 变量替换 → 解析 + 语义检查 → 编译新 RulePlan[] → 校验成功后 Drop 旧状态机 → 初始化新 Scheduler；校验失败保持旧规则不变 |
| 依赖 | M20 |
| 验收 | 不停机更新规则测试；校验失败回退测试；reload 后新事件走新规则测试 |

### M22：多通道告警 + 去重缓存

| 项目 | 内容 |
|------|------|
| crate | `wf-core/alert` |
| 范围 | HttpAlertSink（POST 到 HTTP 端点，alert_id 作为幂等键 header）；SyslogAlertSink（alert_id 写入 structured data）；AlertSink 本地去重缓存（最近 `2 × max(rule.maxspan)` 的 alert_id 集合，重复 ID 跳过） |
| 依赖 | M19 |
| 验收 | 告警输出到 HTTP / Syslog 测试；重复 alert_id 去重测试 |

### M23：监控指标 + 性能基准

| 项目 | 内容 |
|------|------|
| crate | `runtime` |
| 范围 | Prometheus metrics 暴露：窗口内存使用 / 各窗口行数 / 规则触发次数 / 告警产出数 / 事件处理延迟 / 连接断开次数 / 接收吞吐量；性能基准测试框架：固定规则 + 固定数据集 → 测量延迟 / 吞吐 / 内存 |
| 依赖 | M20 |
| 验收 | Prometheus metrics 端点可用（`/metrics`）；基准：1K EPS 下延迟 <1s，内存 <256MB |

### M24：开发者工具链

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` |
| 范围 | `wf explain`：输出 RulePlan 的人类可读描述（状态机步骤、join SQL、severity 映射）；`wf lint`：静态检查（常见错误、性能陷阱、废弃用法）；`wf fmt`：规则格式化（统一缩进、空格、换行） |
| 依赖 | M13 |
| 验收 | explain 输出可读；lint 检出未使用变量 / 无效过滤等；fmt 格式化后重新解析一致 |

---

## 阶段 VII：WFL L2 语言增强（M25–M26）

### M25：L2 关联、基线、集合判定与语义增强

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` + `wf-core` |
| 范围 | `join dim_window on field == field`（LEFT JOIN enrich）：编译为 DataFusion SQL + 运行时 Window 快照注册；`baseline(expr, dur)`：基线偏离检测（均值 ± N 倍标准差），运行时维护滑动窗口统计量；`window.has(field)`：集合判定（编译为 `EXISTS` 子查询或 `IN` 列表）；**§17 P0-1 显式 key 映射**：`key { logical = alias.field }` 消除多源 key 歧义；**§17 P0-2 Join 时间语义一等化**：`join ... snapshot on ...` / `join ... asof on ... within dur` 显式声明 join 模式；**§17 P0-3 规则资源预算**：`limits { max_state; max_cardinality; max_emit_rate; on_exceed }` 防止高基数状态膨胀 |
| 依赖 | M20（MVP 稳定后） |
| 验收 | 情报关联场景（IP 命中威胁情报）；基线偏离场景（登录频率异常）；集合判定场景（IP 在黑名单中）；显式 key 映射多源 join 测试；snapshot / asof 语义正确性测试；limits 触发 throttle / drop_oldest 行为测试 |

### M26：L2 条件表达式、函数扩展、回放与输出契约

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` + `wf-core` |
| 范围 | `if expr then expr else expr` 条件表达式；字符串函数：`contains(field, pattern)` / `regex_match(field, pattern)` / `len(field)` / `lower(field)` / `upper(field)`；时间函数：`time_diff(t1, t2) → float` / `time_bucket(field, interval) → time`；`wf replay`：离线数据回放调试（读取文件 → 模拟事件流 → 执行规则 → 输出匹配结果）；**§17 P0-4 输出契约版本化**：`yield output@v2 (...)` 为输出声明契约版本，支持灰度与审计回放，同名 output 的契约版本可并存 |
| 依赖 | M25 |
| 验收 | 条件表达式求值测试；5 个字符串函数 + 2 个时间函数单元测试；replay 回放离线数据并输出匹配结果；yield 契约版本跨版本字段变更编译期校验测试 |

---

## 阶段 VIII：WFL L3 高级语法（M27–M28）

### M27：L3 固定间隔窗口、结果集变换与可组合规则片段

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` + `wf-core` |
| 范围 | `match<key:dur:tumble>` 固定间隔窗口：按 dur 对齐切分不重叠窗口，每个窗口独立聚合；`conv { sort(-field) \| top(10) \| dedup(field) \| where(expr) ; }` 结果集变换：排序 / Top-N / 去重 / 后聚合过滤；**§17 P1-1 可组合规则片段**：`pattern name(params) { ... }` 参数化片段，编译期展开到标准 RulePlan，不可引入隐式副作用，`wf explain` 可完整还原 |
| 依赖 | M26（L2 稳定后）；需 feature gate `l3` |
| 验收 | tumble 时间分桶聚合场景；Top-N 统计场景（每小时端口扫描最多的 10 个 IP）；pattern 片段展开正确性测试；`wf explain` 还原展开前后一致 |

### M28：L3 多级管道与隐式 yield

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` + `wf-core` |
| 范围 | `\|>` 多级管道：规则内串联，后续 stage 自动绑定 `_in` 别名；隐式 yield：`yield (field=...)` 无目标名，编译器推导中间 window；隐式 window：编译器自动生成中间 Window Schema，无需手写 .wfs |
| 依赖 | M27 |
| 验收 | 两级管道场景（端口扫描检测 `\|>` 扫描源聚合）；三级管道链测试；`wf explain` 可展示 desugar 后的 Core IR |

---

## 阶段 IX：行为分析（M29）

### M29：行为分析能力

| 项目 | 内容 |
|------|------|
| crate | `wf-lang` + `wf-core` |
| 范围 | **会话窗口**：`match<key:session(gap)>` 按活动间隔自动分割会话，gap 为静默超时；**集合函数**：`collect_set(field)` / `collect_list(field)` / `first(field)` / `last(field)`；**统计函数**：`stddev(field)` / `percentile(field, p)`；**增强基线**：`baseline(expr, dur, method)` 支持 ewma / median 方法 + 基线状态持久化快照（重启恢复）；**数值风险评分**：`-> score(expr)` 产出单一风险分 + `entity(type, id_expr)` 实体声明 + 跨规则评分累加；**§17 P1-2 顺序/乱序不变性契约测试**：`contract ... { options { permutation = "shuffle"; runs = N; } expect { stable_hits; stable_score; } }` 保证时间语义稳定（配合 M33 wf-datagen 扰动能力） |
| 依赖 | M28（L3 稳定后） |
| 验收 | 用户会话行为建模场景（异常会话检测）；实体风险评分场景（多规则评分聚合）；基线持久化重启恢复测试 |

---

## 阶段 X：分布式（M30）

### M30：分布式 V2

| 项目 | 内容 |
|------|------|
| crate | `wp-motor` + `wf-core` + `runtime` |
| 范围 | **Sink 侧路由**：wp-motor Arrow IPC Sink 按 key hash 行级分桶路由到不同 WarpFusion 实例；**Router 分布式**：支持 `partitioned(key)` 按 key 分区 + `replicated` 全局复制；**多实例验证**：多个 WarpFusion 实例各管一部分 key，等值 JOIN 本地完成零跨节点通信；**两阶段聚合**（预留）：各节点局部聚合 → 汇总节点合并（全局 GROUP BY） |
| 依赖 | M20（单机 MVP）；与 M25-M29 可并行 |
| 验收 | 2 节点部署：`partitioned(sip)` 数据分布正确；`replicated` 全量复制正确；分布式等值 JOIN 端到端测试 |

---

## 阶段 XI：测试数据生成（M31–M33）

> 详细设计见 [wfl-desion.md §18](wfl-desion.md)。各阶段穿插在其依赖就绪的最早时机，可与其他阶段并行推进。

### M31：wf-datagen P0 — .wfg Parser + Schema 驱动随机生成 ✅

> **可立即启动**：唯一依赖 M07 已完成。建议与 M16→M18→M20 并行推进。

| 项目 | 内容 |
|------|------|
| crate | `wf-datagen` |
| 范围 | `.wfg` 场景 DSL 解析器（EBNF → AST，语法见 §18.2）；schema 驱动随机数据生成（从 `.wfs` 读取字段类型 → 按 gen 函数分布产出样本）；seed 可复现（固定 seed + 确定性 RNG）；输出格式 JSONL + Arrow IPC；CLI `wf-datagen gen --scenario ... --format ... --out ...`；`wf-datagen lint` 一致性校验（.wfg 引用与 .wfs/.wfl 的一致性） |
| 依赖 | M07 ✅ |
| 验收 | .wfg 文件解析为 AST 测试；同 seed 两次生成结果一致；JSONL / Arrow 输出可被 `wf run --replay` 消费；lint 检出引用缺失 |
| 状态 | **已完成** — `wfg_parser/` 解析器 + `datagen/` 生成器 + `output/` JSONL/Arrow IPC 输出，54 项测试通过 |

### M32：wf-datagen P1 — Rule-aware 生成 + Oracle + Verify ✅

> **M31 完成后可立即启动**：依赖 M13 已完成。生成 + oracle 部分独立于引擎；verify 对拍需 M20（MVP）就绪。

| 项目 | 内容 |
|------|------|
| crate | `wf-datagen` |
| 范围 | Rule-aware 数据生成：按 `.wfl` 编译产物驱动 hit / near_miss / non_hit 三类数据分布；Reference Evaluator 自动计算 oracle（期望告警）；oracle 输出为标准 JSONL（match key = `rule_name, entity_type, entity_id, close_reason`）；`wf-datagen verify` 对拍命令（actual vs oracle 差异报告）；CI 阻断条件（`missing == 0 && unexpected == 0 && field_mismatch == 0`） |
| 依赖 | M13 ✅, M31；verify 端到端需 M20 |
| 验收 | 生成的 hit 数据确实触发规则；near_miss 数据不触发规则；oracle 与 `wf run --replay` 实际告警对拍通过；verify 差异报告格式正确 |
| 状态 | **已完成** — `inject_gen.rs` rule-aware 生成 + `oracle/` Reference Evaluator + `verify/` 贪心配对报告 + `verify` CLI 子命令，60 项测试通过 |

### M33：wf-datagen P2 — 时序扰动 + 压测 ✅

| 项目 | 内容 |
|------|------|
| crate | `wf-datagen` |
| 范围 | 时序扰动矩阵（乱序 / 迟到 / 重复 / 丢弃，可组合，由 `.wfg` faults 块声明）；压测模式（高 EPS 连续生成，持续指定时长）；PR 友好差异报告（Markdown 格式，可直接贴入 PR）；配合 §17 P1-2 顺序/乱序不变性契约测试（M29）做回归防线 |
| 依赖 | M32 |
| 验收 | 扰动后 oracle 仍正确校验（考虑 allowed_lateness 边界）；压测模式下引擎无崩溃无内存泄漏；差异报告 Markdown 可在 GitHub PR 渲染 |
| 状态 | **已完成** — `FaultType` 枚举 + `fault_gen.rs` 两阶段扰动（assign + transform）+ `verify` Markdown 报告 + `bench` 吞吐量子命令，74 项测试通过 |


## 里程碑依赖图

```
阶段 I                  阶段 II                 阶段 III
M01→M02→M03→M04→M05     M06 ─────┐              M11→M12→M13
                         M07 ─┐   │                ↑
                              │   │              M07┘
                         M08→M09→M10
                           ↑
                         M01┘

                    阶段 IV              阶段 V
                    M14→M15              M17→┐
                      ↑  ↓               M18→┤ M20 ★ MVP
                    M13┘ M16             M19→┘
                         ↑                 ↑
                    M08,M14┘          全链路┘

        阶段 VI                  阶段 VII         阶段 VIII        IX      X
        M21─┐                    M25→M26           M27→M28 ──→ M29
        M22 ├─ 生产化                ↑                ↑
        M23 │                      M20┘             M26┘        M30 ←─ M20
        M24─┘                                                   (可与VII-IX并行)

        阶段 XI（wf-datagen，与 IV–V 并行推进，优先轨道）
        M31 ✅ ─→ M32 ✅ ─→ M33 ✅
         ↑          ↑         ↑
        M07✅     M13✅    M20(verify 端到端)

关键路径: M01→M02→M03→M04→M05 → M10 → M17 → M18 → M20(MVP)
并行路径: M06∥M07 可与阶段I并行; M11-M13 可与M08-M10并行; M30 可与M25-M29并行
wf-datagen: M31 ✅; M32 ✅; M33 ✅; verify 端到端需 M20 汇合
```

### 当前推荐执行顺序

两条并行轨道同步推进，在 M20 汇合后即可跑通 `gen → run → verify` 全流程：

```
轨道 A（引擎）    M16 ──→ M18 ──→ M19 ──→ M20 ★ MVP
                                                │
轨道 B（测试）    M31 ✅ → M32 ✅ (gen+oracle) ─┤──→ M32(verify端到端) ──→ M33 ✅
                                                │
                                           汇合点：端到端 verify
```


## 里程碑分阶段汇总

| 阶段 | 里程碑 | 阶段目标 | 状态 |
|------|--------|---------|------|
| **I 数据基建** | M01–M05 | wp-motor 能通过 Arrow IPC 可靠传输数据 | ✅ 已完成 |
| **II 配置与窗口** | M06–M10 | 配置可加载、Window 能接收路由并缓存数据 | ✅ 已完成 |
| **III WFL 编译器** | M11–M13 | .wfs + .wfl 编译为 RulePlan | ✅ 已完成 |
| **IV 执行引擎** | M14–M16 | CEP 状态机 + DataFusion join 可执行 | M14–M15 ✅ / M16 待开始 |
| **V 运行时闭环** | M17–M20 | **单机 MVP：数据接收→规则执行→告警输出** | M17 ✅ / M18–M20 待开始 |
| **VI 生产化** | M21–M24 | 热加载、多通道告警、监控、工具链 | 待开始 |
| **VII L2 增强** | M25–M26 | join / baseline / key 映射 / limits / 条件表达式 / yield 契约 | 待开始 |
| **VIII L3 高级** | M27–M28 | tumble / conv / composable pattern / 多级管道 | 待开始 |
| **IX 行为分析** | M29 | session / collect / statistics / score / 乱序不变性 | 待开始 |
| **X 分布式** | M30 | 多节点分布式部署 | 待开始 |
| **XI 测试数据生成** | M31–M33 | .wfg DSL / rule-aware oracle / 时序扰动压测 | ✅ 已完成 |


## 验收检查点

| 检查点 | 里程碑 | 判定标准 | 状态 |
|--------|--------|---------|------|
| **CP1 传输就绪** | M05 | wp-motor → TCP → 对端解码正确；断连重连恢复正常 | ✅ 已通过 |
| **CP2 编译就绪** | M13 | brute_scan.wfl 编译为正确的 RulePlan | ✅ 已通过 |
| **CP3 单机 MVP** | M20 | 一条 L1 规则从数据接收到告警输出全流程跑通 | 待验收 |
| **CP4 生产就绪** | M24 | 热加载 + 监控 + 1K EPS 性能达标 + 工具链可用 | 待验收 |
| **CP5 语言完整** | M28 | L1/L2/L3 全语法可用，8 种示例场景全部通过 | 待验收 |
| **CP6 完整版本** | M30 | 行为分析 + 分布式部署通过端到端验证 | 待验收 |
| **CP7 测试闭环** | M33 | gen → run → verify 全流水线 CI 集成；扰动回归通过 | 待验收 |


## 风险检查点

| 时机 | 检查项 | 不通过时动作 |
|------|--------|------------|
| M05 末 | Arrow IPC 传输吞吐是否足够（目标 ≥10K EPS） | 评估 batch 尺寸调优或协议优化 |
| M13 末 | WFL 编译器复杂度是否可控、AST → RulePlan 映射是否清晰 | 简化 L1 语法、推迟边缘 case |
| M16 末 | DataFusion SessionContext 开销是否可接受 | 评估 Context 复用策略或替换为手写 SQL 子集执行器 |
| M20 末 | 端到端延迟是否达标（目标 <1s @ 1K EPS） | 优化 Window 快照路径、Scheduler 分发策略 |
| M26 末 | L2 函数扩展是否引入表达式求值瓶颈 | 延迟优化或推迟非核心函数 |
| M28 末 | L3 feature gate 是否引入意外复杂度 | 推迟 L3 非核心特性（如隐式 window） |
| M32 末 | Reference Evaluator 与引擎语义是否一致 | 先收窄 oracle 覆盖范围至 L1，L2/L3 逐步扩展 |


## 相关文档

- WarpFusion 设计方案 → [10-warp-fusion.md](10-warp-fusion.md)
- WFL v2 设计方案 → [wfl-desion.md](wfl-desion.md)
- WFL 与主流 DSL 对比分析 → [11-wfl-dsl-comparison.md](11-wfl-dsl-comparison.md)
- wf-datagen 测试数据生成方案 → [wfl-desion.md §18](wfl-desion.md)
- 下一阶段设计提案（P0/P1） → [wfl-desion.md §17](wfl-desion.md)
