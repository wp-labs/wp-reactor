# WFL v2 设计方案（整合版）
<!-- 角色：架构师 / 语言设计 | 状态：Draft | 更新：2026-02-15 -->

## 1. 设计目标与范围

### 1.1 目标
- 在保持轻量前提下，实现三件事：
  - 更简洁：默认可读语法，首屏可上手。
  - 更强表达：覆盖安全检测与实体行为分析主场景（阈值、时序、缺失、关联、基线、会话、评分）。
  - 更易懂：语法、语义、执行模型一一对应，可解释可调试。

### 1.2 范围
- WFL 是 WarpFusion 的检测 DSL，不是通用流计算平台 SQL。
- 优先支持：安全关联检测、告警归并与实体行为分析。
- 不追求：任意 DAG、任意子查询、全功能分析查询语言。

---

## 2. 核心思想：一个语义内核

> WFL 是前端语言；真正执行的是 Core IR。语法可以演进，语义内核不漂移。

### 2.1 Core IR 四原语（唯一真相源）
1. `Bind`：绑定事件源（window + filter）。
2. `Match`：按 key+duration 维护状态机并求值步骤。
3. `Join`：对匹配上下文做 LEFT JOIN enrich。
4. `Yield`：写入目标 window（含系统字段）。

### 2.2 语法糖策略
- `|>`、`conv`、隐式 window 都是语法糖。
- 编译阶段先 desugar 成 Core IR，再交运行时。
- 运行时不理解语法糖，只执行 Core IR。

---

## 3. 三文件 + RulePack 模型

### 3.1 文件职责
- `windows.ws`：逻辑数据定义（window、field、time、over）。
- `rules.wfl`：检测逻辑（bind/match/join/yield）。
- `runtime.toml`：物理参数（mode、max_bytes、watermark、sinks）。

### 3.2 RulePack 入口
- `pack.yaml` 作为统一入口，声明版本、特性和文件列表。

```yaml
version: "2.0"
features: ["l1", "l2"]
windows:
  - windows/security.ws
rules:
  - rules/brute_scan.wfl
runtime: runtime/fusion.toml
```

### 3.3 设计约束
- `.ws` 是上游依赖（先有数据定义，后有规则）。
- `.wfl` 仅能引用 `use` 导入的 window。
- `.toml` 只管物理参数，不写业务规则。

---

## 4. 能力分层（L1/L2/L3）

### L1（默认，MVP）
- `events + match + yield + fixed severity + fmt()`
- 含：OR 分支（`||`）、复合 key（`match<f1,f2:dur>`）、`on close` 缺失检测
- 含：`count`/`sum`/`avg`/`min`/`max`/`distinct` 聚合
- 含：`$VAR` / `${VAR:default}` 变量预处理
- 场景：阈值、单步/多步时序、缺失检测、基础聚合。

### L2（增强）
- `join + severity_map + baseline + window.has() + entity()`
- 场景：情报关联、动态分级、异常偏离、集合判定、实体建模。

### L3（高级，feature gate）
- `|>`、`conv`、`tumble`、隐式中间窗口。
- 场景：多级聚合、Top-N、固定间隔报表、后处理。

> 默认启用 L1/L2；L3 需显式开启 `features: ["l3"]`。

**分层对照表：**

| 特性 | L1 | L2 | L3 |
|------|:--:|:--:|:--:|
| `use` / `rule` / `meta` / `events` | ✓ | | |
| `match<key:dur>` 单/复合 key、滑动窗口 | ✓ | | |
| 多步序列、`on close`、OR 分支（`\|\|`） | ✓ | | |
| `count`/`sum`/`avg`/`min`/`max`/`distinct` | ✓ | | |
| `yield target (...)` 显式目标 | ✓ | | |
| `-> level` 固定 severity | ✓ | | |
| `fmt()` 格式化 | ✓ | | |
| `$VAR` 变量预处理 | ✓ | | |
| `join` 外部关联 | | ✓ | |
| `-> { expr => level }` 动态 severity | | ✓ | |
| `baseline()` 基线偏离 | | ✓ | |
| `window.has(field)` 集合判定 | | ✓ | |
| `entity(type, id_expr)` 实体声明 | | ✓ | ✓ |
| `tumble` 固定间隔窗口 | | | ✓ |
| `conv { ... }` 结果集变换 | | | ✓ |
| `\|>` 多级管道 | | | ✓ |
| 隐式 yield / 隐式 window | | | ✓ |
| **── 行为分析扩展 ──** | | | |
| `if/then/else` 条件表达式 | | ✓ | |
| 字符串函数（`contains`/`regex_match`/`len`/`lower`/`upper`） | | ✓ | |
| 时间函数（`time_diff`/`time_bucket`） | | ✓ | |
| 集合函数（`collect_set`/`collect_list`/`first`/`last`） | | | ✓ |
| `match<key:session(gap)>` 会话窗口 | | | ✓ |
| 统计函数（`stddev`/`percentile`） | | | ✓ |
| 增强 `baseline(expr, dur, method)` + 持久化 | | | ✓ |
| `-> score(expr)` 数值风险评分 | | | ✓ |

### 4.1 行为分析能力扩展（规划）

> 以下能力用于支持实体行为分析场景（用户会话建模、行为基线、风险评分），**不影响 Core IR 四原语和五阶段管道结构**。所有新能力均为函数/表达式/窗口模式/实体声明扩展，编译器将新语法 desugar 到现有 Bind/Match/Join/Yield 框架内执行。

#### 4.1.1 L2 行为分析基础

**条件表达式**：`if expr then expr else expr`
- 分支计算，替代多规则拆分。
- 典型场景：按条件赋值（`if duration > 300 then "long" else "short"`）。

**时间函数**：
- `time_diff(t1, t2)` → float：两时间戳间隔（秒），用于响应时延分析、会话间隔计算。
- `time_bucket(field, interval)` → time：时间分桶，配合 `tumble` 做时间粒度归并。

**字符串函数**：
- `contains(field, pattern)` → bool：子串包含判定。
- `regex_match(field, pattern)` → bool：正则匹配判定。
- `len(field)` → digit：字符串长度。
- `lower(field)` / `upper(field)` → chars：大小写转换。

#### 4.1.2 L3 行为分析高级

**集合函数**（窗口内值收集）：
- `collect_set(alias.field)` → array/T：去重值收集（用于行为模式提取：一个会话访问了哪些资源）。
- `collect_list(alias.field)` → array/T：有序值收集（用于操作序列还原：按时间排列的操作链）。
- `first(alias.field)` → T：窗口内首个值。
- `last(alias.field)` → T：窗口内末个值。

**会话窗口**：`match<key:session(gap)>`
- 按活动间隔自动分割会话：相邻事件时间差超过 `gap` 即切分新窗口。
- 与滑动窗口（固定时长）和 tumble（固定间隔）互补；适用于用户登录会话、操作序列等不规则时间跨度场景。
- `gap` 为 DURATION 类型，语义为"静默超时"。

**统计函数**：
- `stddev(alias.field)` → float：标准差（异常偏离检测）。
- `percentile(alias.field, p)` → float：分位数（P50/P95/P99 分析），`p` 为 0~100 的 digit。

**增强基线**：
- `baseline(expr, dur, method)` 扩展 `method` 参数：`mean`（默认）/ `ewma`（指数加权） / `median`。
- 基线持久化：基线状态定期快照落盘，重启后恢复（不从零冷启动）。

**数值风险评分**：`-> score(expr)`
- 替代分类 severity（low/medium/high/critical），输出数值型风险分数。
- 支持跨规则累加：多条规则对同一实体产出 score，下游聚合总分。
- `score(expr)` 中 `expr` 须为 digit/float 类型。

#### 4.1.3 结构影响评估

| 组件 | 是否变更 | 说明 |
|------|:--------:|------|
| Core IR 四原语 | 否 | Bind/Match/Join/Yield 不变 |
| 主执行链 | 否 | BIND→SCOPE→JOIN→YIELD→CONV 不变；`entity(...)` 作为 YIELD 前置声明，不新增独立执行阶段 |
| 表达式求值器 | 扩展 | 新增 `if/then/else` 节点、新内置函数 |
| WindowStore | 扩展 | 新增 session window 模式 |
| MatchEngine | 扩展 | 支持 session gap 触发窗口切分 |
| YieldWriter | 扩展 | 支持 `score()` 数值输出与 `entity_type/entity_id` 系统字段注入 |
| 运行时状态 | 扩展 | baseline 持久化需增加 snapshot 组件 |

---

## 5. WFL 语义模型（整合原方案）

WFL 采用固定主执行链，阶段顺序不可变（`entity(...)` 为 YIELD 前置声明，不新增独立执行阶段）：

`BIND -> SCOPE -> JOIN -> [ENTITY] -> YIELD -> CONV`

- BIND：`events { alias : window && filter }`
- SCOPE：`match<keys:window_spec> { steps } [-> severity_map | -> score(expr)]`
- JOIN：`join dim_window on sip == dim_window.ip`
- ENTITY：`entity(host, e.host_id)`（可选，声明规则输出实体键）
- YIELD：`yield target_window (field = expr, ...)`（L3 允许 `yield (field=...)` 隐式目标）
- CONV（L3）：`conv { where/sort/top/dedup ... }`

### 5.1 关键统一（解决旧版歧义）
- **Step 数据源必须显式**：`source_ref | ...`，不允许空 source。
- `|>` 展开后，后续 stage 自动绑定编译器注入别名 `_in`（显式可见，可 `wf explain` 查看）。`_in` 是保留标识符，用户不可作为普通别名使用。
- `yield` 采用 **子集映射**：yield 命名参数 + 系统字段必须是目标 window fields 的子集（名称、类型一致）。
- yield 中不得出现未定义字段；未覆盖的非系统字段写入 `null`。同一输出 window 可被多条规则复用。
- `match` 采用显式双阶段：`on event { ... }`（必选）+ `on close { ... }`（可选，窗口关闭求值）。
- `entity(type, id_expr)` 为实体建模一等语法，禁止再依赖 `yield` 手工拼 `entity_type/entity_id`。
- **聚合写法统一**：`alias.field | distinct | count` 与 `distinct(alias.field)` 在语义上等价，编译阶段统一 desugar 为同一聚合 IR。

---

## 6. Window Schema（.ws）

### 6.1 EBNF（简化版）

```ebnf
schema_file   = { window_decl } ;
window_decl   = "window" , IDENT , "{" , { window_attr } , fields_block , "}" ;
window_attr   = stream_attr | time_attr | over_attr ;
stream_attr   = "stream" , "=" , ( STRING | string_array ) ;
string_array  = "[" , STRING , { "," , STRING } , "]" ;
time_attr     = "time" , "=" , IDENT ;
over_attr     = "over" , "=" , ( DURATION | "0" ) ;
fields_block  = "fields" , "{" , { field_decl } , "}" ;
field_decl    = field_name , ":" , field_type ;
field_name    = IDENT | dotted_ident | quoted_ident ;
dotted_ident  = IDENT , "." , IDENT , { "." , IDENT } ;   (* 兼容 WPL 产出的 detail.sha256 类字段 *)
quoted_ident  = "`" , { ANY - "`" } , "`" ;               (* 包含特殊字符时使用反引号 *)
field_type    = base_type | ("array" , "/" , base_type) ;
base_type     = "chars" | "digit" | "float" | "bool" | "time" | "ip" | "hex" ;
```

### 6.2 语义约束（保留原设计）
- window 名称全局唯一。
- `over > 0` 时 `time` 必选且类型为 `time`。
- `over = 0` 表示静态集合，可省略 stream/time。
- 多 stream window 要求 schema 兼容。
- 无 `stream` 属性的 window 仅作为 yield 目标（不订阅任何数据流）。

### 6.3 类型映射
- `chars -> Utf8`
- `digit -> Int64`
- `float -> Float64`
- `bool -> Boolean`
- `time -> Timestamp(Nanosecond, None)`
- `ip/hex -> Utf8`
- `array/T -> List(T)`

---

## 7. WFL 文法

> L3 特性（`|>`、`conv`、`tumble`）以 `(* L3 *)` 标注。L1/L2 实现可忽略带 L3 标注的产生式。

```ebnf
wfl_file      = { use_decl } , { rule_decl } ;
use_decl      = "use" , STRING ;
rule_decl     = "rule" , IDENT , "{" , [ meta_block ] , events_block , stage_chain , "}" ;

stage_chain   = stage , { "|>" , stage } , [ entity_clause ] , yield_clause , [ conv_clause ] ;  (* |> 和 conv 为 L3 *)
stage         = match_clause , { join_clause } ;

meta_block    = "meta" , "{" , { IDENT , "=" , STRING } , "}" ;

events_block  = "events" , "{" , event_decl , { event_decl } , "}" ;
event_decl    = IDENT , ":" , IDENT , [ "&&" , expr ] ;

match_clause  = "match" , "<" , match_params , ">" , "{" , on_event_block , [ on_close_block ] , "}" , [ "->" , severity_out ] ;
match_params  = [ field_ref , { "," , field_ref } ] , ":" , window_spec ;
window_spec   = DURATION                              (* 滑动窗口 *)
              | DURATION , ":" , "tumble"              (* 固定间隔窗口，L3 *)
              | "session" , "(" , DURATION , ")"  ;    (* 会话窗口，L3 行为分析 *)
on_event_block= "on" , "event" , "{" , match_step , { match_step } , "}" ;
on_close_block= "on" , "close" , "{" , match_step , { match_step } , "}" ;
match_step    = step_branch , { "||" , step_branch } , ";" ;
step_branch   = [ IDENT , ":" ] , source_ref , [ "." , IDENT | "[" , STRING , "]" ] , [ "&&" , expr ] , pipe_chain ;
source_ref    = IDENT ;                (* events 别名 或 |> 后续 stage 的 _in *)
pipe_chain    = { "|" , transform } , "|" , measure , cmp_op , primary ;
transform     = "distinct" ;
measure       = "count" | "sum" | "avg" | "min" | "max" ;

join_clause   = "join" , IDENT , "on" , join_cond , { "&&" , join_cond } ;     (* L2 *)
join_cond     = field_ref , "==" , field_ref ;

severity_out  = severity_map | score_expr ;                                     (* score 为 L3 行为分析 *)
severity_map  = severity | "{" , sev_branch , { "," , sev_branch } , "}" ;     (* 动态 map 为 L2 *)
sev_branch    = ( expr | "_" ) , "=>" , severity ;
severity      = "low" | "medium" | "high" | "critical" ;
score_expr    = "score" , "(" , expr , ")" ;                                   (* L3 行为分析：数值风险评分 *)

entity_clause = "entity" , "(" , entity_type , "," , expr , ")" ;              (* L2+：实体声明 *)
entity_type   = IDENT | STRING ;

yield_clause  = "yield" , [ IDENT ] , "(" , named_arg , { "," , named_arg } , ")" ;  (* 省略 IDENT 的隐式 yield 为 L3 *)
named_arg     = IDENT , "=" , expr ;

conv_clause   = "conv" , "{" , conv_chain , { conv_chain } , "}" ;             (* L3 *)
conv_chain    = conv_step , { "|" , conv_step } , ";" ;
conv_step     = ("sort" | "top" | "dedup" | "where") , "(" , [ conv_args ] , ")" ;
conv_args     = expr , { "," , expr } ;

(* 表达式（简化） *)
expr          = or_expr ;
or_expr       = and_expr , { "||" , and_expr } ;
and_expr      = cmp_expr , { "&&" , cmp_expr } ;
cmp_expr      = add_expr , [ cmp_op , add_expr ]
              | add_expr , "in" , "(" , expr , { "," , expr } , ")"
              | add_expr , "not" , "in" , "(" , expr , { "," , expr } , ")" ;
cmp_op        = "==" | "!=" | "<" | ">" | "<=" | ">=" ;
add_expr      = mul_expr , { ("+" | "-") , mul_expr } ;
mul_expr      = unary_expr , { ("*" | "/" | "%") , unary_expr } ;
unary_expr    = [ "-" ] , primary ;
primary       = NUMBER | STRING | "true" | "false"
              | field_ref
              | func_call
              | agg_pipe_expr
              | if_expr
              | "(" , expr , ")" ;
if_expr       = "if" , expr , "then" , expr , "else" , expr ;                  (* L2 行为分析：条件表达式 *)
func_call     = [ IDENT , "." ] , IDENT , "(" , [ expr , { "," , expr } ] , ")" ;
agg_pipe_expr = source_ref , [ "." , IDENT | "[" , STRING , "]" ] , { "|" , transform } , "|" , measure ;
field_ref     = IDENT
              | IDENT , "." , IDENT
              | IDENT , "[" , STRING , "]" ;              (* 访问带点字段：alias[\"detail.sha256\"] *)

(* 词法（简化） *)
IDENT         = ALPHA , { ALPHA | DIGIT | "_" } ;
NUMBER        = DIGIT , { DIGIT } , [ "." , DIGIT , { DIGIT } ] ;
STRING        = '"' , { ANY - '"' } , '"' ;
DURATION      = DIGIT , { DIGIT } , ( "s" | "m" | "h" | "d" ) ;
ALPHA         = "a".."z" | "A".."Z" | "_" ;
DIGIT         = "0".."9" ;
ANY           = ? any unicode char ? ;
```

### 7.1 保留标识符
- `_in`：`|>` 后续 stage 的隐式输入别名，编译器注入，用户必须以此名引用前级输出。

### 7.1.1 带点字段名访问
- `.ws` 允许字段名包含 `.`（如 `detail.sha256`）。
- `.wfl` 引用这类字段时，使用下标形式：`alias["detail.sha256"]`，避免与 `alias.field` 命名空间歧义。

### 7.2 表达式与函数
- 表达式支持：`== != < > <= >= && || in not in + - * / %`。
- 内置函数：

| 函数 | 签名 | 层级 | 说明 |
|------|------|:----:|------|
| `count` | `count(alias)` → digit | L1 | 事件计数 |
| `sum` | `sum(alias.field)` → digit/float | L1 | 求和（field 须为 digit/float） |
| `avg` | `avg(alias.field)` → float | L1 | 平均值（field 须为 digit/float） |
| `min` | `min(alias.field)` → T | L1 | 最小值（field 须为可排序类型） |
| `max` | `max(alias.field)` → T | L1 | 最大值（field 须为可排序类型） |
| `distinct` | `distinct(alias.field)` → digit | L1 | 去重计数（须为 Column 投影） |
| `fmt` | `fmt(STRING, expr, ...)` → chars | L1 | 位置参数格式化，`{}` 占位符 |
| `baseline` | `baseline(expr, duration)` → float | L2 | 滚动基线均值（expr 须为 digit/float） |
| `baseline` | `baseline(expr, duration, method)` → float | L3 | 扩展方法：`mean`(默认)/`ewma`/`median`；支持持久化 |
| `window.has` | `window.has(field)` / `window.has(field, target_field)` → bool | L2 | 成员判定：判断当前上下文字段值是否存在于目标 window 字段值集合 |
| **── 行为分析扩展 ──** | | | |
| `if/then/else` | `if expr then expr else expr` → T | L2 | 条件表达式，两分支类型须一致 |
| `time_diff` | `time_diff(t1, t2)` → float | L2 | 两时间戳间隔（秒） |
| `time_bucket` | `time_bucket(field, interval)` → time | L2 | 时间分桶（interval 为 DURATION 字面量） |
| `contains` | `contains(field, pattern)` → bool | L2 | 子串包含判定 |
| `regex_match` | `regex_match(field, pattern)` → bool | L2 | 正则匹配判定（pattern 须为 STRING 字面量） |
| `len` | `len(field)` → digit | L2 | 字符串长度 |
| `lower` | `lower(field)` → chars | L2 | 转小写 |
| `upper` | `upper(field)` → chars | L2 | 转大写 |
| `collect_set` | `collect_set(alias.field)` → array/T | L3 | 窗口内去重值收集 |
| `collect_list` | `collect_list(alias.field)` → array/T | L3 | 窗口内有序值收集 |
| `first` | `first(alias.field)` → T | L3 | 窗口内首个值 |
| `last` | `last(alias.field)` → T | L3 | 窗口内末个值 |
| `stddev` | `stddev(alias.field)` → float | L3 | 标准差 |
| `percentile` | `percentile(alias.field, p)` → float | L3 | 分位数（p 为 0~100） |

- 集合判定（L2）：`window.has(field)` 判断“当前上下文字段值”是否存在于目标 window 的同名字段值集中；目标 window 须为静态集合（`over = 0`）或维度表。返回 `bool`。

**`window.has(field)` 语义（L2）：**

| 问题 | 规则 |
|------|------|
| `field` 参数含义 | 参数是**当前上下文的字段值表达式**（如 `domain`、`req.domain`），不是字段名字面量。 |
| 匹配目标字段 | 默认按“同名字段”匹配：`window.has(req.domain)` 会在目标 window 中查找字段 `domain`。 |
| 异名字段匹配 | 使用两参数形式：`window.has(ctx_expr, target_field)`，如 `bad_ips.has(req.sip, ip)`。 |
| 匹配范围 | 仅在目标字段上做等值成员判定，不做全字段扫描。 |
| 可用窗口类型 | 目标 window 必须是静态集合（`over = 0`）或维度表（低频更新、用于 enrich/查表；在 `runtime.toml` 通过 `role = "dimension"` 声明）。 |
| 判定时点 | 在规则求值时基于目标 window 的当前快照判定。 |

**聚合双写法（统一语义）：**

| 写法 | 示例 | 说明 |
|------|------|------|
| 管道式 | `scan.dport | distinct | count` | 常用于 `match` 步骤 |
| 函数式 | `distinct(scan.dport)` | 常用于 `yield` / `severity_map` 表达式 |

- 两种写法编译后等价，统一到同一聚合 IR。
- 推荐：`match` 用管道式，`yield` 用函数式，保持可读性一致。

### 7.3 变量预处理（L1）

`.toml` 中 `[vars]` 定义的变量可在 `.wfl` 中引用，编译前文本替换：

```toml
# runtime.toml
[vars]
FAIL_THRESHOLD = "5"
SCAN_THRESHOLD = "10"
```

```wfl
match<sip:5m> {
  fail | count >= $FAIL_THRESHOLD;          // → count >= 5
  scan.dport | distinct | count > ${SCAN_THRESHOLD:10};  // 带默认值
}
```

- `$VAR`：直接替换，变量未定义则编译错误。
- `${VAR:default}`：变量未定义时使用默认值。
- 预处理发生在解析之前（纯文本替换）。

### 7.4 关键语义
- OR 分支：任一命中即转移；未命中分支字段为 `null`。
- `on event`：事件到达时求值；`on close`：窗口关闭时求值（固定窗口到期、session gap 超时或 flush）。
- 若省略 `on close`，关闭阶段视为恒为 true，不额外阻断命中。
- `join`：固定 LEFT JOIN 语义。
- `conv`：仅 `tumble` 可用。

---

## 8. 编译模型（整合原方案）

### 8.1 编译流水线
1. 变量预处理：`$VAR` / `${VAR:default}`。
2. 解析：`.ws` + `.wfl` -> AST。
3. 语义检查：字段、类型、window 引用、over 约束。
4. desugar：展开 `|>`、隐式 stage、`conv` 钩子。
5. 生成 Core IR（Bind/Match/Join/Yield）。
6. 输出 RulePlan（供 MatchEngine 执行）。

### 8.2 RulePlan 结构
```rust
pub struct RulePlan {
    pub name: String,
    pub binds: Vec<BindPlan>,
    pub match_plan: MatchPlan,
    pub joins: Vec<JoinPlan>,
    pub entity_plan: Option<EntityPlan>,
    pub yield_plan: YieldPlan,
    pub conv_plan: Option<ConvPlan>,
}
```

### 8.3 Parser/AST 草图（Match 双阶段）

```rust
pub struct MatchClauseAst {
    pub params: MatchParamsAst,
    pub on_event: Vec<MatchStepAst>,           // 必选，且非空
    pub on_close: Option<Vec<MatchStepAst>>,   // 可选
}

pub struct MatchStepAst {
    pub branches: Vec<StepBranchAst>,          // 对应 `||`
}

pub struct StepBranchAst {
    pub label: Option<String>,
    pub source: SourceRefAst,
    pub field: Option<FieldSelectorAst>,
    pub guard: Option<ExprAst>,
    pub pipe: PipeChainAst,
}

pub struct EntityClauseAst {
    pub entity_type: EntityTypeAst,
    pub entity_id_expr: ExprAst,
}
```

```rust
pub struct EntityPlan {
    pub entity_type: EntityType,
    pub entity_id_expr: ExprPlan,   // 运行期求值后统一归一化到 chars
}
```

```rust
pub enum MatchPhase {
    Event,
    Close,
}

pub struct MatchPlan {
    pub keys: Vec<FieldRef>,
    pub window_spec: WindowSpec,
    pub event_steps: Vec<StepPlan>,
    pub close_steps: Vec<StepPlan>,            // 为空时等价 close_ok = true
}
```

- 运行期判定：`event_ok = eval(event_steps)`，`close_ok = close_steps.is_empty() || eval(close_steps)`。
- 最终命中条件：`event_ok && close_ok`。
- `on_close` 仅做条件判定，不新增独立 `join/yield` 执行动作。

### 8.4 explain 输出（必须）
- 展开后的规则（去语法糖）。
- 状态机图（状态/转移/超时）。
- 字段血缘（字段从哪来、在哪步变换）。

---

## 9. 运行时执行模型（整合原方案）

### 9.1 固定执行链
`Receiver -> Router -> WindowStore -> MatchEngine -> JoinExecutor -> YieldWriter -> Sink`

### 9.2 事件驱动
- 新事件只分发到“引用该 window”的规则。
- 匹配成功后触发 join/yield。
- timeout wheel 定期触发窗口关闭与 maxspan 过期，并执行 `on close` 求值。

### 9.3 并发控制
- 全局 `Semaphore(executor_parallelism)`。
- join/yield 包裹超时。
- 规则执行与 IO 解耦（避免阻塞主分发循环）。

---

## 10. 容错与一致性语义

### 10.1 传输层
- 语义：At-Least-Once（尽力而为）。
- 帧头字段：`source_id + stream_tag + batch_seq`。
- Sink：发送前先写本地 WAL，收到 ACK 后清理已确认条目。
- Receiver：写入接收侧 Ingress WAL（可重放）+ 位图滑动窗口去重（支持乱序）。

**Ingress WAL 规格（Receiver 侧）：**

| 项 | 规范 |
|----|------|
| 保留策略 | **独立配置**，不与 Sink WAL 共享；参数名 `ingress_wal_retention`（默认 `30m`）。 |
| 组织方式 | **per-source 分目录**：`.../ingress-wal/<source_id>/segment-*`，回放与清理按 source 隔离。 |
| 回放粒度 | 按 `(source_id, batch_seq)` 单调顺序回放；不同 source 互不影响。 |
| fsync 策略 | 可配置：`strict`（每条 fsync）/ `interval`（默认，`100ms` 或 `256` 条触发）/ `off`（仅测试）。 |
| ACK 推进 | 仅能推进到“已落 Ingress WAL 且连续无缺口”的 `ack_seq`。 |

建议配置（`runtime.toml`）：

```toml
[receiver.ingress_wal]
dir = "/var/lib/warpfusion/ingress-wal"
retention = "30m"               # ingress_wal_retention
fsync_mode = "interval"         # strict | interval | off
fsync_interval = "100ms"        # interval 模式生效
fsync_batch = 256               # interval 模式生效
```

**ACK 协议（累积确认）：**

```
Sink                                  Receiver
 │ ── Frame(source_id, seq=1, tag, batch) ──→ │
 │ ── Frame(source_id, seq=2, tag, batch) ──→ │
 │ ←── Ack(source_id, ack_seq=2) ──────────── │  // 连续前缀累积确认
 │                                             │
 │  Sink 收到 ack_seq=2 → 清理 WAL 中 seq ≤ 2
```

- **ACK 帧**：`source_id(u64) + ack_seq(u64)`；`ack_seq` 必须是“已持久化且连续”的最大序号（即 `<= ack_seq` 无缺口）。
- **落盘时机**：Receiver 将 batch 追加到 Ingress WAL 并 `fsync` 成功后，才允许推进 `ack_seq`；仅写入 Window 内存不允许 ACK。
- **乱序处理**：若先收到 `seq=5` 再收到 `seq=4`，在 `4` 到达前 `ack_seq` 不能越过缺口。
- **断连重放**：重连后 Sink 从 WAL 中最小未确认 seq 开始重放，Receiver 位图去重保证幂等。
- **重启恢复**：WarpFusion 启动时先回放 Ingress WAL，再恢复实时接收；已 ACK 的 batch 必须可从 Ingress WAL 重建到 WindowStore。
- **WAL 保留**：可配置（默认 30m，对应 `ingress_wal_retention`），超期清理不可恢复，`wal_dropped_batches` 监控。
- **恢复边界**：当前仅保证 `ingress_wal_retention` 时间窗内可重放恢复；超过该窗口需依赖外部补数或后续 checkpoint 能力（V3+）。

**Ingress WAL 与 WindowStore 关系：**
1. 运行时写入顺序：`Ingress WAL append(+fsync) -> WindowStore apply -> ACK`。  
2. 重启回放顺序：按 `source_id` 分组、`batch_seq` 升序执行 `Ingress WAL -> 去重器 -> WindowStore`。  
3. 重复 batch 在回放阶段被去重器丢弃，不会重复写入 WindowStore。  

### 10.2 基线持久化（行为分析）
- 目标：避免重启后 baseline 从零冷启动，减少误报尖峰。
- 快照对象：`baseline(expr, dur[, method])` 的聚合状态（mean/ewma/median），并记录对应 `ack_seq` 锚点。
- 恢复顺序：先加载快照，再从锚点回放后续 `Ingress WAL` 增量，避免重复计入。

建议配置（`runtime.toml`）：

```toml
[behavior.baseline_store]
dir = "/var/lib/warpfusion/baseline"
snapshot_interval = "5m"
max_snapshots = 24
restore = "latest"             # latest | clean
```

### 10.3 告警幂等
- `alert_id = sha256(rule_name + scope_key + window_range)`。
- AlertSink 本地去重缓存 + 下游透传。

### 10.4 事件时间
- per-window：`watermark`、`allowed_lateness`、`late_policy`。
- 迟到策略：`drop | revise | side_output`。

---

## 11. 热加载策略

- 仅 `.wfl` 与 `[vars]` 支持热加载。
- `.ws` 与运行时物理参数变更需重启。
- reload 采用 Drop 策略：丢弃在途状态机，立即切新规则。

```text
wf reload
  -> 读取新 .wfl + [vars]
  -> 语法/语义检查
  -> 编译 RulePlan
  -> 原子替换规则集
```

---

## 12. 语义约束（整合版）

### 12.1 Events
- 别名唯一；window 必须存在；过滤字段必须存在。

### 12.2 Match
- key 可为空。
- 固定窗口：`duration > 0`；会话窗口：`gap > 0`。
- `maxspan` 仅用于固定窗口，且 `maxspan <= 涉及 window 的最小 over`。
- `sum/avg` 仅数值；`distinct` 仅列投影。
- `on event` 块必选且至少包含一条 step。
- `on close` 块可选；若省略，等价于关闭阶段恒为 true。
- step 必须显式 source（不允许空 source）。

**双阶段求值语义：**

| ID | 规则 |
|----|------|
| M1 | `on event` 在每次事件到达时求值，更新事件阶段命中状态 `event_ok`。 |
| M2 | `on close` 在窗口关闭时求值一次，得到关闭阶段命中状态 `close_ok`；若缺省则 `close_ok = true`。 |
| M3 | 规则在该窗口上的最终命中条件为 `event_ok && close_ok`。 |
| M4 | `on close` 只做条件判定，不引入新的 `join/yield` 动作；命中后仍按同一 stage 的 `join -> yield` 流程输出。 |

**match key 解析规则：**

| ID | 规则 |
|----|------|
| K1 | `match<k1,k2:dur>` 中未限定名 key（如 `sip`）要求在本 match 涉及的所有事件源中都存在同名字段。 |
| K2 | key 可用限定名（如 `fail.sip`）消歧；仅影响解析，不改变“各事件源都需可提取该 key”的约束。 |
| K3 | 多事件源字段名不同（如 `fail.sip` vs `scan.src_ip`）时，**不能**直接在同一 match key 中做自动映射；需先在上游 `.ws` 对齐字段名，或用前级规则 `yield` 归一化后再匹配。 |
| K4 | key 字段跨事件源类型必须一致；不允许 `ip` 与 `chars`、`digit` 与 `chars` 混用。 |
| K5 | 复合 key 按位置形成键元组（`<k1,k2>`），各位置按 K1~K4 独立校验。 |

**session 模式约束：**

| ID | 规则 |
|----|------|
| S1 | `session(gap)` 中 `gap` 必须 > 0。 |
| S2 | session 关闭条件：同 key 在 `gap` 内无新事件（静默超时），或收到流结束/显式 flush。 |
| S3 | `on close` 在 session 关闭时求值；关闭时点为 S2 触发时刻。 |
| S4 | session 无固定 duration；状态保留仍受 window `over` 与运行时上限约束。超限时强制切段并触发一次 `on close` 求值。 |

### 12.3 Join
- `on` 两侧字段必须可解析且类型**一致**（跨类型编译错误）。
- join 右侧字段（来自 join window）**必须**以 `window_name.field` 限定名引用；左侧可使用上下文字段（如 `sip` 或 `fail.sip`）。
- 多 join 按声明顺序执行。

### 12.4 Yield
- 目标 window 必须存在，且满足：`stream` 为空（纯输出 window）并且 `over > 0`。
- yield 命名参数 + 系统字段必须是目标 window fields 的**子集**（名称和类型匹配）。
- yield 中不得出现 window 未定义的字段名；未覆盖的非系统字段值为 null。
- 自动注入系统字段：`rule_name`(chars)、`emit_time`(time)。
- 若声明 `entity(type, id_expr)`，自动注入 `entity_type`(chars)、`entity_id`(chars)。
- 使用 `-> score(expr)` 时必须声明 `entity(type, id_expr)`。
- 输出等级二选一：`-> severity_map` 注入 `severity`(chars)，`-> score(expr)` 注入 `score`(float)。
- `severity` 与 `score` 在同一规则中互斥，并存时报编译错误。
- `entity_type/entity_id` 为系统字段，禁止在 `yield` 命名参数中手工赋值。
- `yield target(...)` 为默认写法（L1/L2）；`yield (...)` 隐式目标仅在 L3 允许。

### 12.5 Pipeline/Conv
- `|>` 后续 stage 禁止 `events`。
- `yield`/`conv` 仅末 stage 可出现。
- `conv` 仅 tumble 模式可用。

**`|>` 展开语义：**

| ID | 规则 |
|----|------|
| P1 | 编译器为每个 `\|>` 前级生成隐式中间规则 + 隐式 window，语义等价于手写多规则 |
| P2 | 后续 stage 的 match key 必须是前级输出字段的子集 |
| P3 | **隐式集合绑定**：后续 stage 编译器注入 `events { _in : <前级隐式 window> }`，用户以 `_in` 引用 |
| P4 | **字段作用域**：`_in` 可见字段**仅限**前级 key 字段 + 聚合结果；原始事件字段**不穿透** |
| P5 | **聚合字段命名**：隐式推导时取函数名（`count`/`sum`/...）；同名冲突编译错误，须改用显式 yield 消歧 |
| P6 | **空值传播**：前级 OR 分支未命中侧字段为 null；后续 stage 聚合跳过 null（count 不计、sum 跳过） |
| P7 | **类型继承**：key 字段保持原类型；聚合字段统一为 `digit`（count/sum/min/max）或 `float`（avg） |
| P8 | 隐式 window 的 `over` 取下一级 match 的 duration |

### 12.6 类型系统（编译期强制）

所有类型和引用检查在**编译期**完成，不允许延迟到运行时报错。

**类型规则：**

| ID | 规则 |
|----|------|
| T1 | `sum(a.f)` / `avg(a.f)` — f 必须为 `digit` 或 `float`；否则编译错误 |
| T2 | `min(a.f)` / `max(a.f)` — f 必须为可排序类型（`digit`/`float`/`time`/`chars`）；`ip`/`bool` 不可排序 |
| T3 | `distinct(a.f)` 或 `a.f | distinct | count` — 参数必须为 Column 投影（`alias.field` 或 `alias["detail.sha256"]`），不接受 Set 级别（`alias`）；返回 `digit` |
| T4 | `count(a)` 或 `a | count` — 参数为 Set 级别，返回 `digit`；`count(a.f)` 编译错误（应使用 `distinct`） |
| T5 | `fmt(STRING, expr, ...)` — `{}` 占位符数量必须等于后续参数数量；每个参数可为任意类型；返回 `chars` |
| T6 | `baseline(expr, duration)` — expr 必须为 `digit`/`float` 类型表达式；返回 `float` |
| T7 | `==` / `!=` 两侧操作数类型必须一致；跨类型比较编译错误 |
| T8 | `>` / `>=` / `<` / `<=` 两侧操作数必须为 `digit` 或 `float`；不同数值类型自动提升为 `float` |
| T9 | `&&` / `\|\|` 两侧必须为 `bool` 类型表达式 |
| T10 | yield 命名参数的值表达式类型必须与目标 window 对应字段类型一致 |
| T11 | `window.has(x)` 中 `x` 必须可解析为当前上下文字段值；其类型必须与目标 window 同名字段类型一致 |
| T12 | `window.has(x, f)` 中 `f` 必须是目标 window 已定义字段名；`x` 类型必须与 `f` 类型一致 |
| T13 | `window.has(...)` 的目标 window 必须满足：`over = 0` 或被标记为维度表（dimension） |
| **── 行为分析扩展类型规则 ──** | |
| T14 | `if c then a else b` — `c` 必须为 `bool`；`a` 与 `b` 类型必须一致；返回类型 = `a` 的类型 |
| T15 | `time_diff(t1, t2)` — `t1`、`t2` 必须为 `time` 类型；返回 `float`（秒） |
| T16 | `time_bucket(f, interval)` — `f` 必须为 `time` 类型；`interval` 为 DURATION 字面量；返回 `time` |
| T17 | `contains(f, pat)` — `f` 必须为 `chars`/`ip`/`hex`；`pat` 须为 STRING 字面量；返回 `bool` |
| T18 | `regex_match(f, pat)` — `f` 必须为 `chars`/`ip`/`hex`；`pat` 须为 STRING 字面量（编译期校验正则合法性）；返回 `bool` |
| T19 | `len(f)` — `f` 必须为 `chars`/`ip`/`hex`；返回 `digit` |
| T20 | `lower(f)` / `upper(f)` — `f` 必须为 `chars`；返回 `chars` |
| T21 | `collect_set(a.f)` / `collect_list(a.f)` — 参数必须为 Column 投影；返回 `array/T`（T 为 f 的类型） |
| T22 | `first(a.f)` / `last(a.f)` — 参数必须为 Column 投影；返回类型 = f 的类型 |
| T23 | `stddev(a.f)` — f 必须为 `digit` 或 `float`；返回 `float` |
| T24 | `percentile(a.f, p)` — f 必须为 `digit` 或 `float`；`p` 须为 digit 字面量且 0 ≤ p ≤ 100；返回 `float` |
| T25 | `baseline(expr, dur, method)` — expr 须为 `digit`/`float`；method 须为 STRING 字面量（`"mean"`/`"ewma"`/`"median"`）；返回 `float` |
| T26 | `-> score(expr)` — expr 须为 `digit` 或 `float`；yield 自动注入 `score: float` 系统字段 |
| T27 | `severity_map` 与 `score(expr)` 在同一规则中互斥；并存时报编译错误 |
| T28 | `entity(type, id_expr)` 中 `type` 必须为编译期常量（IDENT 或 STRING） |
| T29 | `entity(type, id_expr)` 中 `id_expr` 必须为可标识标量（`chars`/`ip`/`hex`/`digit`）；执行期统一归一化为 `chars` |
| T30 | 使用 `-> score(expr)` 的规则必须声明 `entity(type, id_expr)`；缺失时报编译错误 |
| T31 | `yield` 命名参数中禁止出现 `entity_type` / `entity_id`（由系统注入） |

**静态引用解析：**

| ID | 规则 |
|----|------|
| R1 | **跨步引用**：`label.field` / `label["field.with.dot"]` 中 `label` 必须是当前步骤的**前序步骤**标签；引用后续步骤编译错误 |
| R2 | **跨步字段类型**：`label.field` 或下标形式的字段类型由 label 所在步骤的事件集 window schema 静态确定；不存在的字段编译错误 |
| R3 | **events 字段引用**：`alias.field` / `alias["field.with.dot"]` 中 `alias` 必须在 `events {}` 中声明，字段必须在对应 window 的 `fields {}` 中定义 |
| R4 | **join 字段引用**：join window 字段**必须**以 `window_name.field` 或 `window_name["field.with.dot"]` 限定名引用；裸字段名不搜索 join window |
| R5 | **解析优先级**：events 别名 → match 步骤标签 → 聚合函数结果；未找到即编译错误，不做回退猜测 |
| R6 | **OR 分支 nullable**：`\|\|` 各分支标签字段在后续可引用，但标注为 nullable；对 nullable 字段做 `sum`/`avg` 时编译器警告 |

### 12.7 实体主键与评分聚合

| ID | 规则 |
|----|------|
| E1 | 实体键通过 `entity(type, id_expr)` 显式声明；编译器生成系统字段 `entity_type/entity_id`。 |
| E2 | `type` 建议使用稳定字面量（如 `user`/`host`/`ip`/`process`）；同一规则内不可变化。 |
| E3 | `id_expr` 允许引用当前上下文字段或表达式结果；执行期若为空则该次输出丢弃并计入 `entity_id_null_dropped`。 |
| E4 | 使用 `-> score(expr)` 的规则必须声明 `entity(...)`；不再允许自动推导实体键。 |
| E5 | 跨规则评分累加键固定为 `(entity_type, entity_id, time_bucket)`；缺少任一维度不得参与累加。 |
| E6 | `yield` 中禁止手工写 `entity_type/entity_id`，避免与系统注入冲突。 |

---

## 13. 示例（从原设计迁移后的标准写法）

### 13.1 阈值检测
```wfl
use "security.ws"

rule brute_force {
  events {
    fail: auth_events && action == "failed"
  }
  match<sip:5m> {
    on event {
      fail | count >= 3;
    }
  } -> high
  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} failed {} times", fail.sip, count(fail))
  )
}
```

### 13.2 时序关联 + enrich
```wfl
use "security.ws"

rule brute_then_scan {
  events {
    fail: auth_events && action == "failed"
    scan: fw_events
  }
  match<sip:5m> {
    on event {
      fail | count >= 3;
      scan.dport | distinct | count > 10;
    }
  } -> { count(fail) > 10 => critical, _ => high }
  join ip_blocklist on sip == ip_blocklist.ip
  yield security_alerts (
    sip = fail.sip,
    threat = ip_blocklist.threat_level,
    message = fmt("{} brute+scan", fail.sip)
  )
}
```

### 13.3 缺失检测（A -> NOT B）
```wfl
use "dns.ws"

rule dns_no_response {
  events {
    req: dns_query && bad_domains.has(req.domain)
    resp: dns_response
  }
  match<query_id:30s> {
    on event {
      req | count >= 1;
    }
    on close {
      resp | count == 0;
    }
  } -> medium
  yield security_alerts (
    sip = req.sip,
    domain = req.domain,
    message = fmt("{} query {} no response", req.sip, req.domain)
  )
}
```

### 13.4 多级管道（L3）
```wfl
use "security.ws"

rule port_scan_detect {
  events {
    d: fw_events && action == "deny"
  }
  match<sip,dport:5m> {
    on close {
      d | count >= 3;
    }
  }
  |> match<sip:10m> {
    on close {
      _in | count >= 10;
    }
  } -> high
  yield security_alerts (
    sip = _in.sip,
    port_count = count(_in),
    message = fmt("{} scanned {} ports", _in.sip, count(_in))
  )
}
```

### 13.5 用户会话行为分析（L3 行为分析）
```wfl
use "access.ws"

rule abnormal_session {
  meta {
    description = "检测单次会话内异常操作模式：访问资源过多或操作间隔异常"
  }
  events {
    op: user_operations
  }
  match<uid:session(30m)> {
    on close {
      op | count >= 1;
    }
  } -> {
    distinct(op.resource) > 50 => critical,
    time_diff(last(op.event_time), first(op.event_time)) > 1800 => high,
    _ => medium
  }
  yield behavior_alerts (
    uid = op.uid,
    resource_count = distinct(op.resource),
    resources = collect_set(op.resource),
    op_sequence = collect_list(op.action),
    first_seen = first(op.event_time),
    last_seen = last(op.event_time),
    session_duration = time_diff(last(op.event_time), first(op.event_time)),
    message = fmt("{} accessed {} resources in session", op.uid, distinct(op.resource))
  )
}
```

### 13.6 实体风险评分（L3 行为分析）
```wfl
use "security.ws"

rule entity_risk_score {
  meta {
    description = "基于多维行为指标计算实体风险分数"
  }
  events {
    e: endpoint_events
    ps: endpoint_events && contains(lower(process), "powershell")
  }
  match<host_id:1h:tumble> {
    on close {
      e | count >= 1;
    }
  } -> score(
    if count(e) > baseline(count(e), 7d) * 2.0 then 30.0 else 0.0
    + if distinct(e.dest_ip) > 100 then 25.0 else 0.0
    + if count(ps) > 0 then 20.0 else 0.0
    + if percentile(e.bytes_out, 95) > baseline(avg(e.bytes_out), 7d) * 3.0 then 25.0 else 0.0
  )
  entity(host, e.host_id)
  yield risk_scores (
    host_id = e.host_id,
    event_count = count(e),
    unique_dests = distinct(e.dest_ip),
    p95_bytes = percentile(e.bytes_out, 95),
    processes = collect_set(e.process),
    message = fmt("{} risk score computed over 1h window", e.host_id)
  )
}
```

### 13.7 登录行为基线偏离（L2/L3 行为分析）
```wfl
use "auth.ws"

rule login_anomaly {
  events {
    login: auth_events && action == "success"
  }
  match<uid:1h:tumble> {
    on close {
      login | count >= 1;
    }
  } -> {
    count(login) > baseline(count(login), 30d, "ewma") * 3.0 => high,
    _ => low
  }
  yield behavior_alerts (
    uid = login.uid,
    login_count = count(login),
    baseline_count = baseline(count(login), 30d, "ewma"),
    locations = collect_set(login.geo_city),
    login_category = if count(login) > 20 then "heavy" else if count(login) > 5 then "normal" else "light",
    message = fmt("{} login count {} vs baseline {}", login.uid, count(login), baseline(count(login), 30d, "ewma"))
  )
}
```

---

## 14. 与原方案差异与兼容

### 14.1 保留内容
- 三文件模型（.ws/.wfl/.toml）。
- 事件时间语义（watermark/allowed_lateness）。
- OR 分支、`on close`、join enrich、baseline、conv。

### 14.2 收敛改进
- 统一到 Core IR 四原语。
- 消除空 source step 歧义。
- 语法糖全部 desugar，不进运行时。
- L1/L2/L3 分层上线，避免首版过载。

### 14.3 迁移策略
- 提供 `wf migrate`：旧规则自动转换到 v2 规范。
- 提供 `wf lint --strict`：识别不兼容写法并给修复建议。

---

## 15. 实施路线

### Phase A（先稳）
- Core IR + L1 + 可读语法 + lint/fmt。

### Phase B（增强）
- L2（join/severity/baseline/entity）+ explain/replay。
- L2 行为分析基础：`if/then/else`、字符串函数、时间函数。

### Phase C（高级）
- L3（`|>`/`conv`）+ 性能优化 + 分布式 V2 完善。

### Phase D（行为分析）
- L3 行为分析：session window、集合函数（`collect_set`/`collect_list`/`first`/`last`）、统计函数（`stddev`/`percentile`）。
- 增强 baseline（`ewma`/`median` + 持久化快照）。
- 数值风险评分（`-> score(expr)`）+ `entity(type,id_expr)` + 跨规则评分累加。

---

## 16. 设计原则（团队共识）

1. 显式优先于隐式。
2. 一种能力只保留一种主写法。
3. 编译器可解释性优先于语法炫技。
4. 先稳定 L1/L2 再扩 L3。
5. 文法、示例、执行器必须同步演进。
