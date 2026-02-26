# WFL 与主流关联引擎 DSL 对比分析
<!-- 角色：架构师 | 状态：v2.1 对齐完成（Top50） | 创建：2026-02-13 | 更新：2026-02-26 -->

> 本文档对比 WFL（Warp Fusion Language）与 YARA-L 2.0、Elastic EQL、Sigma、Splunk SPL、KQL（Microsoft Sentinel）五种主流关联/检测 DSL 的能力差异，分析 WFL 的设计优势与已知短板。
>
> 本文基于 [wfl-desion.md](wfl-desion.md)（2026-02-20，v2.1）的最新设计。


## 1. 能力矩阵

| 能力维度 | **WFL** | **YARA-L 2.0** | **Elastic EQL** | **Sigma** | **Splunk SPL** | **KQL (Sentinel)** |
|---------|---------|---------------|----------------|-----------|---------------|-------------------|
| 时序链 | `match` 多步 + OR 分支 | `$e1 before $e2` | `sequence by` | ✗ | `transaction` | ✗ |
| 双阶段匹配 | `on event` + `on close` | 仅 match section | ✗ | ✗ | ✗ | ✗ |
| 多维分组 | `match<f1,f2:dur>` 复合键 | `match` 单字段 | `by f1,f2` | ✗ | `by f1,f2` | `by f1,f2` |
| 固定间隔窗口 | `match<key:1h:fixed>` (L3) | ✗ | ✗ | ✗ | `timechart span=` | `bin(time, 1h)` |
| 会话窗口 | `match<key:session(gap)>` (L3) | ✗ | ✗ | ✗ | `transaction maxpause=` | ✗（需手写） |
| 聚合 | `count/sum/avg/min/max/distinct` | `#e > 3` | ✗ | `count`（基础） | `stats` 全功能 | `summarize` 全功能 |
| 统计函数 | `stddev`/`percentile` (L3) | ✗ | ✗ | ✗ | `stdev`/`perc95` 等 | `stdev`/`percentile` |
| 集合收集 | `collect_set`/`collect_list`/`first`/`last` + `mvcount`/`mvjoin`/`mvdedup` (L3) | ✗ | ✗ | ✗ | `values`/`list`/`first`/`last` + `mvcount`/`mvjoin` | `make_set`/`make_list` |
| 条件表达式 | `if/then/else` (L2) | ✗ | ✗ | ✗ | `eval if(c,a,b)` | `iff(c,a,b)` |
| 字符串函数 | `contains`/`regex_match`/`replace`/`replace_plain`/`trim`/`ltrim`/`rtrim`/`split`/`len`/`lower`/`upper`/`concat`/`indexof`/`startswith_any`/`endswith_any` (L2) | `re.regex` | `match`/`length` | `contains`（YAML） | 200+ 函数 | `contains`/`strlen` 等 |
| 时间函数 | `time_diff`/`time_bucket`/`strftime`/`strptime` (L2) | ✗ | ✗ | ✗ | `eval relative_time` | `datetime_diff`/`bin` |
| 格式化函数 | `fmt(STRING, expr, ...)` (L1) | ✗ | ✗ | ✗ | `printf` | `strcat`/`format_*` |
| 结果集变换 | `conv { sort \| top \| dedup ; }` (L3) | ✗ | ✗ | ✗ | `sort/head/dedup` | `sort/take` |
| 多级管道 | `\|>` 串联 + 隐式 window (L3) | ✗ | ✗ | ✗ | `\|` 无限管道 | `\|` 管道 |
| 缺失检测 | `on close { resp \| count == 0; }` | `!$e2` | `!sequence` | ✗ | `NOT` 子搜索 | ✗ |
| OR 分支 | `branch_a \|\| branch_b ;` | ✗ | ✗ | ✗ | ✗ | ✗ |
| 外部关联 | `join window snapshot/asof on ...` (L2) | 无（平台侧） | 无 | 无 | `lookup` | `externaldata` |
| Join 时点语义 | `snapshot` / `asof within` 一等语法 | ✗ | ✗ | ✗ | ✗ | ✗ |
| 集合判定 | `window.has(field[, target])` (L2) | `$e.ip in %list` | ✗ | ✗ | `inputlookup` | `in (externaldata)` |
| 数值风险评分 | `-> score(expr)` (L1) | ✗ | ✗ | ✗ | ✗（需 eval 手算） | ✗ |
| 分项可解释评分 | `-> score { item = expr @ weight; }` (L2) | ✗ | ✗ | ✗ | ✗ | ✗ |
| 条件命中映射 | `hit(cond)` → 1.0/0.0 (L2) | ✗ | ✗ | ✗ | ✗ | ✗ |
| 实体声明 | `entity(type, id_expr)` 必选 (L1) | ✗ | ✗ | ✗ | ✗ | ✗ |
| 风险等级派生 | `score` → runtime `level_map` 映射 | 固定 | 固定 | `level:` 固定 | 无 | 固定 |
| 基线偏离 | `baseline(expr, dur[, method])` (L2/L3) | ✗ | ✗ | ✗ | ✗（需外部 ML） | ✗（Fusion ML） |
| 统一输出 | `yield` 统一告警与归并 | ✗ | ✗ | ✗ | ✗ | ✗ |
| 子查询合并 | 隐式 yield 规则链 + join | ✗ | ✗ | ✗ | `join [subsearch]` | `join (subquery)` |
| 变量参数化 | `$VAR` / `${VAR:default}` (L1) | ✗ | ✗ | ✗ | `$token$` | ✗ |
| `in`/`not in` | `expr in (...)`/`expr not in (...)` | `in %list` | `in (...)` | ✗ | `IN (...)` | `in (...)` |
| 规则资源预算 | `limits { max_memory ... }` (v2.1) | ✗ | ✗ | ✗ | 平台配置 | 平台配置 |
| 输出契约版本 | `yield target@vN` + `meta.contract_version` | ✗ | ✗ | ✗ | ✗ | ✗ |
| 可证明正确性门禁 | `test + shuffle + scenario verify` | 平台回放 | 平台测试 | ✗ | 平台测试 | 平台测试 |
| 数据/逻辑分离 | .wfs / .wfl / .toml 三文件 + `pack.yaml` | 单文件 | 单文件 | 单文件 | 单文件 | 单文件 |
| 能力分层 | L1/L2/L3 feature gate | ✗ | ✗ | ✗ | ✗ | ✗ |
| 热加载 | `wf reload` (Drop) | 平台管理 | Kibana UI | 无运行时 | 平台管理 | 平台管理 |
| 编辑器与语言服务 | Zed 已支持语法高亮 + LSP（诊断/跳转/补全） | 平台 UI | Kibana/IDE 插件 | 社区插件丰富 | 平台 UI + 插件 | Azure/Kusto 工具链 |


## 2. 逐引擎对比

### 2.1 vs YARA-L 2.0（Google Chronicle）

| 维度 | WFL 优势 | YARA-L 优势 |
|------|---------|------------|
| 时序链 | `match` 多步 + OR 分支 + `on close` 缺失检测 | `$e1 before $e2` 语法更简洁 |
| 双阶段匹配 | `on event`（实时求值）+ `on close`（窗口关闭求值）显式分离 | 仅 match section，无双阶段概念 |
| 聚合 | 6 个基础聚合 + `stddev`/`percentile` + `conv` + `fixed` | 基础（仅 `#e > N`） |
| 行为分析 | session window + `collect_set`/`first`/`last` + `baseline` + `score` | 无 |
| 表达式 | `if/then/else` + `hit()` + 字符串/时间函数 + `in`/`not in` | 仅基础条件 |
| 字符串函数 | `contains`/`regex_match`/`replace`/`replace_plain`/`trim`/`ltrim`/`rtrim`/`split`/`len`/`lower`/`upper`/`concat`/`indexof`/`startswith_any`/`endswith_any` | `re.regex`（能力有限） |
| 时间格式化/解析 | `strftime`/`strptime` | 无 |
| 风险评分 | `-> score(expr)` 单通道 + `-> score { item @ weight }` 分项可解释 + 跨规则累加 | ✗ |
| 实体建模 | `entity(type, id_expr)` 一等语法，必选声明 | 无实体概念 |
| 输出 | `yield` 统一告警与归并，窗口可配 sinks | 仅告警，无数据归并 |
| 风险等级 | `score` + runtime `level_map` 配置化映射（可版本化） | 规则级固定 |
| 分离度 | 三文件分离 + `pack.yaml` 入口 | 单文件，简洁但职责混合 |
| 上手成本 | 三文件 + 六阶段 + L1/L2/L3 分层 | 单文件 + 四段式，门槛更低 |
| 生态 | 独立引擎，需自建 | Chronicle 平台内置，开箱即用 |

**总结**：WFL 在检测表达力上全面超越 YARA-L。WFL 的核心差异化在于：① OR 分支时序 + 双阶段匹配（`on event`/`on close`）；② 分项可解释评分（`score { item @ weight }`）+ 跨规则累加；③ 一等实体声明（`entity()`），YARA-L 完全没有实体行为建模能力；④ 行为分析扩展（session window、collect 函数、baseline、score）进一步拉开差距；⑤ v2.1 的 `join snapshot/asof`、`limits`、`yield@vN` 与 conformance 门禁提高了可治理性。代价是学习曲线更陡——但 L1/L2/L3 分层降低了初始上手门槛。

### 2.2 vs Elastic EQL

| 维度 | WFL 优势 | EQL 优势 |
|------|---------|---------|
| 时序链 | 多步 + OR 分支 + `on close` 缺失检测 | `sequence by ... with maxspan` 一行式更紧凑 |
| 双阶段匹配 | `on event` + `on close` | 无 |
| 聚合 | 完整聚合 + 统计函数 + `conv` + `fixed` | 无聚合能力 |
| 行为分析 | session window + collect 函数 + baseline + score | 无 |
| 风险评分 | `-> score(expr)` + 分项可解释评分 + 实体声明 | 无 |
| 字符串 | `contains`/`regex_match`/`replace`/`replace_plain`/`trim`/`ltrim`/`rtrim`/`split`/`len`/`lower`/`upper`/`concat`/`indexof`/`startswith_any`/`endswith_any` | `match`/`wildcard`/`length`/`stringContains` |
| 时间格式化/解析 | `strftime`/`strptime` | 非核心能力（通常依赖平台侧处理） |
| 外部关联 | `join` + `.has()` | 无 |
| 查询能力 | 检测+归并+行为分析统一 | 纯事件查询，和 Kibana 深度集成 |
| 部署 | 独立进程 | 依赖 Elasticsearch 集群 |

**总结**：EQL 定位是事件查询语言（和 ES 绑定），WFL 定位是独立检测+行为分析引擎。EQL 的 `sequence` 语法更简洁，字符串函数更丰富，但缺乏聚合、行为分析、风险评分和输出能力。v2.1 进一步把 `join` 时点语义和输出契约版本做成语言级能力，这是 EQL 不覆盖的工程治理域。两者不是同层竞争。

### 2.3 vs Sigma

| 维度 | WFL 优势 | Sigma 优势 |
|------|---------|-----------|
| 表达力 | 时序链、双阶段匹配、聚合、join、conv、fixed、session、baseline、score | 仅单事件匹配 |
| 行为分析 | session window + collect + 统计函数 + baseline + 可解释风险评分 + 实体建模 | 无 |
| 字符串/时间函数 | 语言内置函数可组合（含 `replace_plain`/`strftime`/`strptime`） | 无运行时函数体系（依赖目标后端） |
| 可移植性 | 绑定 WarpFusion 引擎 | 平台无关，可编译到任何 SIEM |
| 社区 | 无 | 5000+ 开源规则，社区庞大 |
| 上手 | 多关键字 + 六阶段（L1 子集可快速上手） | YAML 格式，几乎零学习成本 |

**总结**：Sigma 是"规则分发格式"，WFL 是"执行语言"。Sigma 赢在可移植性和社区，WFL 赢在表达力与执行语义。v2.1 增加的 `limits`、`yield@vN`、conformance 门禁进一步强化了执行侧治理。二者互补——可以考虑支持 Sigma 规则导入编译为 WFL。

### 2.4 vs Splunk SPL

| 维度 | WFL 优势 | SPL 优势 |
|------|---------|---------|
| 时序链 | `match` 多步 + OR 分支 | `transaction` 功能等价但语法不同 |
| 双阶段匹配 | `on event` + `on close` 显式分离 | 无对应概念 |
| OR 分支 | `branch_a \|\| branch_b ;` 一等语法 | 无直接支持 |
| 风险评分 | `-> score(expr)` 单通道 + `-> score { item @ weight }` 分项可解释 + 跨规则累加 | 无内置概念（需 eval 手算） |
| 实体建模 | `entity(type, id_expr)` 一等语法 + 跨规则评分累加键 | 无内置（需外部模型） |
| 会话窗口 | `match<key:session(gap)>` 一等语法 | `transaction maxpause=` 功能等价 |
| 统一输出 | `yield` 告警+归并+行为分析统一 | 告警是平台层，非语言层 |
| 基线偏离 | `baseline(expr, dur, method)` 内置 + 持久化 | 无内置（需 MLTK 外部模块） |
| 风险等级 | `score` → runtime `level_map` 可版本化映射 | 无内置概念 |
| 聚合深度 | 基础聚合 + `stddev`/`percentile` + `conv` + `fixed` | **仍超** — `eventstats/streamstats` + 无限管道 + 200+ 函数 |
| 集合收集 | `collect_set`/`collect_list`/`first`/`last` + `mvcount`/`mvjoin`/`mvdedup` | `values`/`list`/`first`/`last` + `mvcount`/`mvjoin` 功能等价 |
| 条件表达式 | `if/then/else` + `hit()` 覆盖核心场景 | `eval if()`/`case()` + 完整表达式引擎 |
| 字符串函数 | `contains`/`regex_match`/`replace`/`replace_plain`/`trim`/`ltrim`/`rtrim`/`split`/`len`/`lower`/`upper`/`concat`/`indexof`/`startswith_any`/`endswith_any` | **仍远超** — `substr`/`mvindex` 等 200+ |
| 时间格式化/解析 | `strftime`/`strptime` | `strftime`/`strptime`（功能等价） |
| 子查询 | 隐式 yield 规则链（需两条规则） | `join [subsearch]` 单条查询内完成 |
| 行保留 | 无 | `eventstats` 保留原始行 |
| 部署 | 单机轻量 | 重量级平台 |

**总结**：行为分析扩展后，WFL 与 SPL 的差距进一步缩小。集合收集、统计函数、条件表达式都已对齐 SPL 核心能力；字符串侧在 `replace_plain`/`ltrim`/`rtrim`/`concat`/`indexof`/`startswith_any`/`endswith_any` 落地后继续收敛，时间侧新增 `strftime`/`strptime`。**WFL 的新增差异化**：分项可解释评分（`score { ... }`）、一等实体建模（`entity()`）、跨规则评分累加——这三项是 SPL 完全不具备的检测/分析原语。SPL 仍在通用计算（200+ 函数、eventstats、无限管道）上保持优势，两者的差距从"聚合能力远弱"收窄到"通用函数库丰富度"——这是检测 DSL vs 通用查询语言的本质差异。

### 2.5 vs KQL（Microsoft Sentinel）

| 维度 | WFL 优势 | KQL 优势 |
|------|---------|---------|
| 时序链 | `match` 多步 + OR 分支 | 无原生时序链（需手写 join + 时间条件） |
| 双阶段匹配 | `on event` + `on close` | 无 |
| 会话窗口 | `match<key:session(gap)>` 一等语法 | 无原生（需 `row_window_session` 手写） |
| 风险评分 | `-> score(expr)` + 分项可解释评分 + 跨规则累加 | 无 |
| 实体建模 | `entity(type, id_expr)` 一等语法 | 无（UEBA 是平台层 ML，非语言层） |
| 基线偏离 | `baseline(expr, dur, method)` 内置 + 持久化 | 无（Sentinel Fusion 依赖 ML 模型） |
| 风险等级 | `score` → runtime `level_map` 配置化映射 | 无 |
| 统一输出 | `yield` | 无 |
| 字符串函数 | `contains`/`regex_match`/`replace`/`replace_plain`/`trim`/`ltrim`/`rtrim`/`split`/`len`/`lower`/`upper`/`concat`/`indexof`/`startswith_any`/`endswith_any` | `contains`/`startswith`/`endswith`/`replace_string`/`trim`/`split`/`strlen`/`tolower`/`toupper`/`strcat`/`indexof` 等 |
| 时间格式化/解析 | `strftime`/`strptime` | `format_datetime`/`parse_datetime` |
| 聚合 | `conv` + `fixed` + `stddev`/`percentile` 覆盖主场景 | `summarize` 全功能 + `make-series` 时序分析 |
| 集合收集 | `collect_set`/`collect_list`/`first`/`last` + `mvcount`/`mvjoin`/`mvdedup` | `make_set`/`make_list`/`arg_min`/`arg_max` 功能等价 |
| 条件表达式 | `if/then/else` + `hit()` | `iff()`/`case()` 功能等价 |
| 可视化集成 | 无（交给下游） | Sentinel 工作簿深度集成 |
| 子查询 | 规则链 | `join (subquery)` 内联 |
| 部署 | 独立轻量 | Azure 云平台绑定 |

**总结**：KQL 没有原生时序链和会话窗口检测（Sentinel 的 Fusion 引擎是 ML 驱动而非规则驱动），WFL 在多步序列检测和行为分析上有结构性优势。随着 `replace_plain`/`ltrim`/`rtrim`/`concat`/`indexof`/`startswith_any`/`endswith_any` 及 `strftime`/`strptime` 落地，WFL 在字符串与时间处理上的可用性进一步提升。**WFL 的新增差异化**：分项可解释评分和一等实体建模是 KQL/Sentinel 在语言层完全缺失的（Sentinel UEBA 依赖平台 ML，非规则可控）。WFL 的 baseline 内置能力也优于 KQL。KQL 在聚合丰富度和可视化上更强，但绑定 Azure 生态。


## 3. WFL 独有能力

以下能力在对比的五种 DSL 中均无直接对应：

| 能力 | 语法 | 层级 | 价值 |
|------|------|:----:|------|
| OR 分支时序 | `a \|\| b ;` | L1 | 攻击后行为分叉检测（C2 或数据外泄） |
| 双阶段匹配 | `on event { ... }` + `on close { ... }` | L1 | 实时求值与窗口关闭求值显式分离，缺失检测（A→NOT B）语义清晰 |
| 实体声明 | `entity(type, id_expr)` | L1 | 规则必选的一等实体键声明，驱动跨规则评分累加 |
| 数值风险评分 | `-> score(expr)` | L1 | 规则只产出数值分，与等级解耦，支持跨规则累加 |
| 分项可解释评分 | `-> score { item = expr @ weight; ... }` | L2 | 多维指标加权评分，产出 `score_contrib` JSON 明细 |
| 条件命中映射 | `hit(cond)` → 1.0/0.0 | L2 | 布尔条件映射为评分权重，简化 score 表达式 |
| 内置基线偏离 | `baseline(expr, dur[, method])` | L2/L3 | 无需外部 ML 模块即可做行为偏离检测，支持持久化 |
| Join 时点语义一等化 | `join ... snapshot/asof ... within` | L2 | 消除在线/回放在维表取值时点上的语义漂移 |
| 规则资源预算 | `limits { max_memory; ... }` | v2.1 | 规则级资源防护，阻断高基数状态膨胀 |
| 输出契约版本化 | `yield target@vN` + `meta.contract_version` | v2.1 | 下游字段演进可灰度、可回滚、可审计 |
| Conformance 门禁 | `test + shuffle + scenario verify` | v2.1 | 把”正确性验证”前置为发布门槛 |
| `yield` 统一输出 | 告警和归并共用一个关键字 + window 抽象 | L1 | 消除 alert/output 概念分裂 |
| 风险等级派生 | `score` → runtime `level_map`（可版本化） | — | 等级映射与规则解耦，审计友好 |
| 三文件分离 | .wfs / .wfl / .toml + `pack.yaml` | L1 | 数据工程师、安全分析师、SRE 各改各的 |
| 隐式 window | `yield (...)` 无目标名，编译器推导 | L3 | 多级规则链零配置 |
| 能力分层 | L1(MVP) / L2(增强) / L3(高级+feature gate) | — | 渐进学习，避免首版认知过载 |
| 格式化函数 | `fmt(STRING, expr, ...)` | L1 | 统一告警 message 格式化 |


## 4. 与 SPL/KQL 聚合差距分析

WFL 设计初期与 SPL/KQL 在聚合能力上存在多项差距，经过 `fixed`、`conv`、`|>` 等机制的引入，以及行为分析扩展（`if/then/else`、`hit()`、字符串/时间/集合/统计函数），差距已大幅缩小：

| 维度 | 差距状态 | WFL 实现 | SPL 对应 |
|------|---------|---------|---------|
| 多维分组 | **已消除** | `match<sip,dport:5m>` 复合键 | `stats ... by f1, f2` |
| 时间分桶 | **已消除** | `match<sip:1h:fixed>` 固定间隔窗口 (L3) | `timechart span=1h` |
| Top-N / 排序 / 去重 | **已消除** | `conv { sort(-f) \| top(10) ; }` (L3) | `sort / head / dedup` |
| 后聚合过滤 | **已消除** | `conv { where(count > 5) ; }` (L3) | `\| where count > 5` |
| 条件表达式 | **已消除** | `if c then a else b` + `hit(c)` (L2) | `eval if(c,a,b)` |
| 集合收集 | **已消除** | `collect_set`/`collect_list`/`first`/`last` + `mvcount`/`mvjoin`/`mvdedup` (L3) | `values`/`list`/`first`/`last` + `mvcount`/`mvjoin` |
| 统计函数 | **已消除** | `stddev`/`percentile` (L3) | `stdev`/`perc95` |
| 字符串函数 | **进一步缩小** | `contains`/`regex_match`/`replace`/`replace_plain`/`trim`/`ltrim`/`rtrim`/`split`/`len`/`lower`/`upper`/`concat`/`indexof`/`startswith_any`/`endswith_any` (L2, 15 个) | 200+ 函数 |
| 多级管道 | **大幅缩小** | `\|>` 规则内串联 + 隐式 window (L3) | `\|` 无限管道 |
| 子查询合并 | **大幅缩小** | 隐式 yield 规则链 + join 引用规则名 | `\| join [subsearch]` |
| 行保留聚合 | 仍有差距 | 无（流式模型不保留原始行） | `eventstats` |

11 项中 7 项已消除、3 项大幅缩小、1 项属分析查询能力（有意不做）。

**WFL 反超 SPL/KQL 的维度（新增）：**

| 维度 | WFL | SPL | KQL |
|------|-----|-----|-----|
| 分项可解释评分 | `score { item @ weight }` + `score_contrib` JSON | ✗ | ✗ |
| 一等实体声明 | `entity(type, id_expr)` 必选 | ✗ | ✗（平台 ML） |
| 跨规则评分累加 | `(entity_type, entity_id, time_bucket)` 累加键 | ✗ | ✗ |
| 内置基线 + 持久化 | `baseline(expr, dur, method)` 语言原语 | 需 MLTK | 需 Fusion ML |
| 双阶段匹配 | `on event` + `on close` | ✗ | ✗ |


## 5. 已知短板

| 短板 | 影响 | 是否需要解决 |
|------|------|-------------|
| 行保留聚合（eventstats） | 无法"给每行附加聚合值后保留原始行" | 否——分析查询能力，交给下游 SIEM |
| 字符串/多值函数库深度 | 已补齐 `substr`/`startswith`/`endswith`/`mvindex`/`mvappend`，但整体函数总量仍低于 SPL | 可后续按需扩展 |
| 通用数学函数 | 已补齐 `abs`/`round`/`ceil`/`floor`/`sqrt`/`pow`/`log`/`exp`/`sign`/`trunc`/`clamp`，仍缺 `sin`/`cos` 等 | 可后续按需扩展 |
| 社区规则库 | 无现成规则 | 可考虑支持 Sigma 规则导入 |
| 三文件 + pack.yaml 认知成本 | 新用户需理解文件协作关系 | L1 子集 + 模板 + 文档覆盖 + Zed 语法高亮/LSP 降低上手成本 |
| L3 feature gate 复杂度 | 高级特性需显式启用，增加配置步骤 | 文档明确分层边界 |


## 6. 定位总结

```
        查询能力强 <─────────────────────────────────> 检测+行为分析能力强

SPL ████████████████████░░░░░░░░░░░         通用查询，检测是附属
KQL ███████████████████░░░░░░░░░░░░         通用查询，检测靠 ML
WFL ░░░░░░████████████████████████░         检测+行为分析为核心，聚合覆盖主场景
EQL ░░░░░░░░░░░░░████████████░░░░░         事件查询+序列检测，无聚合
YARA-L ░░░░░░░░░░░░░░░█████████░░░░         纯检测，弱聚合
Sigma ░░░░░░░░░░░░░░░░░░██████░░░░         规则分发格式，无执行
```

WFL 在检测语言中表达力最强（OR 分支、双阶段匹配、缺失检测、数据归并），同时通过行为分析扩展（session window、collect 函数、统计函数、baseline、score）将能力边界从"安全检测"推进到"实体行为分析"。与 SPL/KQL 的差距从设计初期的"远弱"持续收窄——11 项差距中 7 项已消除，剩余差距集中在通用函数库丰富度和行保留聚合，这是 DSL vs 通用查询语言的设计边界，不是能力缺陷。v2.1 进一步补上工程治理维度（`join` 时点语义、规则预算、契约版本、conformance 门禁），让“能表达”走向“可证明、可演进”。

**WFL v2.1 的核心设计演进**：

| 演进 | 说明 |
|------|------|
| 评分模型统一 | 规则只产出 `score`（`[0,100]`），不再声明等级；等级由 runtime `level_map` 配置化派生 |
| 实体一等化 | `entity(type, id_expr)` 为必选语法，驱动跨规则评分累加键 `(entity_type, entity_id, time_bucket)` |
| 评分可解释 | `score { item = expr @ weight; ... }` 分项评分产出 `score_contrib` JSON 明细 |
| Core IR 收敛 | 四原语（Bind/Match/Join/Yield）为唯一语义内核，所有语法糖编译期 desugar |
| 六阶段管道 | BIND→SCOPE→JOIN→ENTITY→YIELD→CONV（ENTITY 为声明位，不新增计算算子） |
| Join 语义固定 | `join` 必须显式声明 `snapshot` 或 `asof within` |
| 资源预算内建 | `limits` 成为规则必填，编译阶段产出成本/风险评估 |
| 输出契约治理 | `yield target@vN` + `meta.contract_version`，支持版本化演进 |
| 正确性门禁 | `test + shuffle + scenario verify` 三层校验作为发布门槛 |

WFL 的独特定位：**唯一同时提供时序检测、实体建模、可解释数值评分、内置基线的独立 DSL**。SPL/KQL 通过平台能力（ML 模块、外部插件）可实现类似效果，但不是语言层原语——WFL 将这些能力内化为编译期可检查、运行期可解释的语言一等公民。随着 Zed 语法高亮与 LSP 落地，WFL 在开发体验上的短板也开始收敛。

## 7. SPL Top50 对齐清单（v2.1）

为避免“只追函数总数”的失真，后续目标改为：

- **目标 A（覆盖率）**：SPL 高频 Top50 函数覆盖率 ≥ 90%
- **目标 B（数量兜底）**：SPL 函数量级的 1/4（约 50）作为阶段里程碑

### 7.1 Top50 清单与状态

| SPL 常用函数 | WFL 对应 | 状态 | 优先级 |
|---|---|---|---|
| `count` | `count` | ✅ 已支持 | — |
| `dc` / `distinct_count` | `distinct` | ✅ 已支持 | — |
| `sum` | `sum` | ✅ 已支持 | — |
| `avg` | `avg` | ✅ 已支持 | — |
| `min` | `min` | ✅ 已支持 | — |
| `max` | `max` | ✅ 已支持 | — |
| `if` | `if ... then ... else ...` | ✅ 已支持 | — |
| `stdev` | `stddev` | ✅ 已支持 | — |
| `perc95` | `percentile` | ✅ 已支持 | — |
| `lower` | `lower` | ✅ 已支持 | — |
| `upper` | `upper` | ✅ 已支持 | — |
| `len` / `strlen` | `len` | ✅ 已支持 | — |
| `match` / `regex` | `regex_match` | ✅ 已支持 | — |
| `replace` | `replace` | ✅ 已支持 | — |
| `trim` | `trim` | ✅ 已支持 | — |
| `split` | `split` | ✅ 已支持 | — |
| `mvcount` | `mvcount` | ✅ 已支持 | — |
| `mvjoin` | `mvjoin` | ✅ 已支持 | — |
| `mvdedup` | `mvdedup` | ✅ 已支持 | — |
| `first` | `first` | ✅ 已支持 | — |
| `last` | `last` | ✅ 已支持 | — |
| `substr` | `substr` | ✅ 已支持 | — |
| `startswith` | `startswith` | ✅ 已支持 | — |
| `endswith` | `endswith` | ✅ 已支持 | — |
| `mvindex` | `mvindex` | ✅ 已支持 | — |
| `mvappend` | `mvappend` | ✅ 已支持 | — |
| `abs` | `abs` | ✅ 已支持 | — |
| `ceil` | `ceil` | ✅ 已支持 | — |
| `floor` | `floor` | ✅ 已支持 | — |
| `round` | `round` | ✅ 已支持 | — |
| `sqrt` | `sqrt` | ✅ 已支持 | — |
| `pow` | `pow` | ✅ 已支持 | — |
| `log` | `log` | ✅ 已支持 | — |
| `exp` | `exp` | ✅ 已支持 | — |
| `clamp` | `clamp` | ✅ 已支持 | — |
| `sign` | `sign` | ✅ 已支持 | — |
| `trunc` | `trunc` | ✅ 已支持 | — |
| `is_finite` | `is_finite` | ✅ 已支持 | — |
| `ltrim` | `ltrim` | ✅ 已支持 | — |
| `rtrim` | `rtrim` | ✅ 已支持 | — |
| `concat` | `concat` | ✅ 已支持 | — |
| `indexof` | `indexof` | ✅ 已支持 | — |
| `replace`（字面量替换） | `replace_plain` | ✅ 已支持 | — |
| `startswith`（多候选） | `startswith_any` | ✅ 已支持 | — |
| `endswith`（多候选） | `endswith_any` | ✅ 已支持 | — |
| `coalesce` | `coalesce` | ✅ 已支持 | — |
| `isnull` | `isnull` | ✅ 已支持 | — |
| `isnotnull` | `isnotnull` | ✅ 已支持 | — |
| `mvsort` | `mvsort` | ✅ 已支持 | — |
| `reverse`（多值） | `mvreverse` | ✅ 已支持 | — |

> 当前清单口径下，Top50 已支持 50/50（100.0%），覆盖率目标已超额完成。

### 7.2 实现批次回顾

| 批次 | 函数 | 目标 |
|---|---|---|
| Batch-1 | `substr`, `startswith`, `endswith`, `mvindex`, `mvappend` | ✅ 已完成，补齐字符串/多值高频短板 |
| Batch-2 | `abs` | ✅ 已完成，达到 Top30 覆盖率 90%（27/30） |
| Batch-3 | `round`, `ceil`, `floor`, `strftime`, `strptime` | ✅ 已完成，补齐数值取整与时间格式化/解析能力（5 个函数） |
| Batch-4 | `sqrt`, `pow`, `log`, `exp`, `clamp`, `sign`, `trunc`, `is_finite`, `ltrim`, `rtrim`, `concat`, `indexof`, `replace_plain`, `startswith_any`, `endswith_any`, `coalesce`, `isnull`, `isnotnull`, `mvsort`, `mvreverse` | ✅ 已完成，扩展 20 个函数并达成 Top50（50/50） |


## 相关文档

- WarpFusion 设计方案 → [warp-fusion.md](warp-fusion.md)
- WFL v2.1 设计方案 → [wfl-desion.md](wfl-desion.md)
- WarpFusion 执行计划 → [wf-execution-plan.md](wf-execution-plan.md)
