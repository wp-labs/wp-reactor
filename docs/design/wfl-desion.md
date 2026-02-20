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
- 优先支持：安全关联检测、风险告警归并与实体行为分析。
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
- `windows.wfs`：逻辑数据定义（window、field、time、over）。
- `rules.wfl`：检测逻辑（bind/match/join/yield）。
- `runtime.toml`：物理参数（mode、max_bytes、watermark、sinks）。

### 3.2 RulePack 入口
- `pack.yaml` 作为统一入口，声明版本、特性和文件列表。

```yaml
version: "2.0"
features: ["l1", "l2"]
windows:
  - windows/security.wfs
rules:
  - rules/brute_scan.wfl
runtime: runtime/fusion.toml
```

### 3.3 设计约束
- `.wfs` 是上游依赖（先有数据定义，后有规则）。
- `.wfl` 仅能引用 `use` 导入的 window。
- `.toml` 只管物理参数，不写业务规则。

---

## 4. 能力分层（L1/L2/L3）

### L1（默认，MVP）
- `events + match + score + entity + yield + fmt()`
- 含：OR 分支（`||`）、复合 key（`match<f1,f2:dur>`）、`on close` 缺失检测
- 含：`count`/`sum`/`avg`/`min`/`max`/`distinct` 聚合
- 含：`$VAR` / `${VAR:default}` 变量预处理
- 场景：阈值、单步/多步时序、缺失检测、基础聚合。

### L2（增强）
- `join + baseline + window.has() + derive + score 分项块`
- 场景：情报关联、异常偏离、集合判定、实体建模、可解释评分。

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
| `-> score(expr)` 单通道风险分 | ✓ | | |
| `score { item = expr @ weight; ... }` 分项评分 | | ✓ | |
| `derive { x = expr; ... }` 特征派生块 | | ✓ | |
| `fmt()` 格式化 | ✓ | | |
| `$VAR` 变量预处理 | ✓ | | |
| `join` 外部关联 | | ✓ | |
| `baseline()` 基线偏离 | | ✓ | |
| `window.has(field)` 集合判定 | | ✓ | |
| `entity(type, id_expr)` 实体声明 | ✓ | | |
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

### 4.1 行为分析能力扩展（规划）

> 以下能力用于支持实体行为分析场景（用户会话建模、行为基线、风险评分），**不影响 Core IR 四原语和五阶段管道结构**。所有新能力均为函数/表达式/窗口模式/实体声明/特征派生扩展，编译器将新语法 desugar 到现有 Bind/Match/Join/Yield 框架内执行。

#### 4.1.1 L2 行为分析基础

**条件表达式**：`if expr then expr else expr`
- 分支计算，替代多规则拆分。
- 典型场景：按条件赋值（`if duration > 300 then "long" else "short"`）。
- 评分场景推荐辅助函数 `hit(cond)`，将 bool 条件映射为 1.0/0.0。

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

**单通道风险评分**：`-> score(expr)` / `-> score { ... }`
- 规则层仅产出单一风险分，不再在规则中声明等级。
- 支持跨规则累加：多条规则对同一实体产出 score，下游聚合总分。
- `score(expr)` 或 `score { item = expr @ weight; ... }` 中 `expr` 均须为 digit/float 类型。

#### 4.1.3 结构影响评估

| 组件 | 是否变更 | 说明 |
|------|:--------:|------|
| Core IR 四原语 | 否 | Bind/Match/Join/Yield 不变 |
| 主执行链 | 调整 | 执行链统一为 BIND→SCOPE→JOIN→ENTITY→YIELD→CONV；其中 ENTITY 为声明位，不引入独立计算算子 |
| 表达式求值器 | 扩展 | 新增 `if/then/else` 节点、新内置函数 |
| WindowStore | 扩展 | 新增 session window 模式 |
| MatchEngine | 扩展 | 支持 session gap 触发窗口切分 |
| YieldWriter | 扩展 | 支持 `score` 输出与 `entity_type/entity_id` 系统字段注入 |
| 运行时状态 | 扩展 | baseline 持久化需增加 snapshot 组件 |

---

## 5. WFL 语义模型（整合原方案）

WFL 采用固定主执行链，阶段顺序不可变（`entity(...)` 为 YIELD 前置声明，不新增独立执行阶段）：

`BIND -> SCOPE -> JOIN -> ENTITY -> YIELD -> CONV`

- BIND：`events { alias : window && filter }`
- SCOPE：`match<keys:window_spec> { steps [derive] } -> score(expr)` 或 `-> score { ... }`
- JOIN：`join dim_window on sip == dim_window.ip`
- ENTITY：`entity(host, e.host_id)`（必选，声明规则输出实体键）
- YIELD：`yield target_window (field = expr, ...)`（L3 允许 `yield (field=...)` 隐式目标）
- CONV（L3）：`conv { where/sort/top/dedup ... }`

### 5.1 关键统一（解决旧版歧义）
- **Step 数据源必须显式**：`source_ref | ...`，不允许空 source。
- `|>` 展开后，后续 stage 自动绑定编译器注入别名 `_in`（显式可见，可 `wf explain` 查看）。`_in` 是保留标识符，用户不可作为普通别名使用。
- `yield` 采用 **子集映射**：yield 命名参数 + 系统字段必须是目标 window fields 的子集（名称、类型一致）。
- yield 中不得出现未定义字段；未覆盖的非系统字段写入 `null`。同一输出 window 可被多条规则复用。
- `match` 采用显式双阶段：`on event { ... }`（必选）+ `on close { ... }`（可选，窗口关闭求值）。
- `derive { ... }` 为特征派生块：先计算可复用特征，再供 `score`/`yield` 引用。
- `entity(type, id_expr)` 为实体建模一等语法，禁止再依赖 `yield` 手工拼 `entity_type/entity_id`。
- **聚合写法统一**：`alias.field | distinct | count` 与 `distinct(alias.field)` 在语义上等价，编译阶段统一 desugar 为同一聚合 IR。
- 新增 `contract { given/expect }` 规则契约测试块：仅用于 `wf test`/CI 前置校验，不进入生产执行链。

---

## 6. Window Schema（.wfs）

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
wfl_file      = { use_decl } , { rule_decl } , { contract_block } ;
use_decl      = "use" , STRING ;
rule_decl     = "rule" , IDENT , "{" , [ meta_block ] , events_block , stage_chain , "}" ;

stage_chain   = stage , { "|>" , stage } , entity_clause , yield_clause , [ conv_clause ] ;  (* |> 和 conv 为 L3 *)
stage         = match_clause , { join_clause } ;

meta_block    = "meta" , "{" , { IDENT , "=" , STRING } , "}" ;

events_block  = "events" , "{" , event_decl , { event_decl } , "}" ;
event_decl    = IDENT , ":" , IDENT , [ "&&" , expr ] ;

match_clause  = "match" , "<" , match_params , ">" , "{" , on_event_block , [ on_close_block ] , [ derive_block ] , "}" , "->" , score_out ;
match_params  = [ field_ref , { "," , field_ref } ] , ":" , window_spec ;
window_spec   = DURATION                              (* 滑动窗口 *)
              | DURATION , ":" , "tumble"              (* 固定间隔窗口，L3 *)
              | "session" , "(" , DURATION , ")"  ;    (* 会话窗口，L3 行为分析 *)
on_event_block= "on" , "event" , "{" , match_step , { match_step } , "}" ;
on_close_block= "on" , "close" , "{" , match_step , { match_step } , "}" ;
derive_block  = "derive" , "{" , derive_item , { derive_item } , "}" ;
derive_item   = IDENT , "=" , expr , ";" ;
match_step    = step_branch , { "||" , step_branch } , ";" ;
step_branch   = [ IDENT , ":" ] , source_ref , [ "." , IDENT | "[" , STRING , "]" ] , [ "&&" , expr ] , pipe_chain ;
source_ref    = IDENT ;                (* events 别名 或 |> 后续 stage 的 _in *)
pipe_chain    = { "|" , transform } , "|" , measure , cmp_op , primary ;
transform     = "distinct" ;
measure       = "count" | "sum" | "avg" | "min" | "max" ;

join_clause   = "join" , IDENT , "on" , join_cond , { "&&" , join_cond } ;     (* L2 *)
join_cond     = field_ref , "==" , field_ref ;

score_out     = score_expr | score_block ;
score_expr    = "score" , "(" , expr , ")" ;                                   (* 简洁写法 *)
score_block   = "score" , "{" , score_item , { score_item } , "}" ;            (* 可解释分项写法 *)
score_item    = IDENT , "=" , expr , "@" , NUMBER , ";" ;

entity_clause = "entity" , "(" , entity_type , "," , expr , ")" ;              (* L1：实体声明，规则必选 *)
entity_type   = IDENT | STRING ;

yield_clause  = "yield" , [ IDENT ] , "(" , named_arg , { "," , named_arg } , ")" ;  (* 省略 IDENT 的隐式 yield 为 L3 *)
named_arg     = yield_field , "=" , expr ;
yield_field   = IDENT | IDENT , "." , IDENT , { "." , IDENT } | quoted_ident ;    (* 与 .wfs field_name 对齐 *)
quoted_ident  = "`" , { ANY - "`" } , "`" ;                                     (* 同 §6.1 .wfs 定义 *)

conv_clause   = "conv" , "{" , conv_chain , { conv_chain } , "}" ;             (* L3 *)
conv_chain    = conv_step , { "|" , conv_step } , ";" ;
conv_step     = ("sort" | "top" | "dedup" | "where") , "(" , [ conv_args ] , ")" ;
conv_args     = expr , { "," , expr } ;

(* 规则契约测试（given/expect，供 wf test 使用） *)
contract_block = "contract" , IDENT , "for" , IDENT , "{" , given_block , expect_block , [ options_block ] , "}" ;
given_block    = "given" , "{" , { given_stmt } , "}" ;
given_stmt     = "row" , "(" , IDENT , "," , field_assign , { "," , field_assign } , ")" , ";"
               | "tick" , "(" , DURATION , ")" , ";" ;
field_assign   = ( IDENT | STRING ) , "=" , expr ;
expect_block   = "expect" , "{" , { expect_stmt } , "}" ;
expect_stmt    = "hits" , cmp_op , INTEGER , ";"
               | "hit" , "[" , INTEGER , "]" , "." , hit_assert , ";" ;
hit_assert     = "score" , cmp_op , NUMBER
               | "close_reason" , "==" , STRING
               | "entity_type" , "==" , STRING
               | "entity_id" , "==" , STRING
               | "field" , "(" , STRING , ")" , cmp_op , expr ;
options_block  = "options" , "{" , [ "close_trigger" , "=" , close_trigger_val , ";" ] , [ "eval_mode" , "=" , eval_mode_val , ";" ] , "}" ;
close_trigger_val = "timeout" | "flush" | "eos" ;
eval_mode_val  = "strict" | "lenient" ;

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
              | derive_ref
              | close_reason_ref
              | func_call
              | agg_pipe_expr
              | if_expr
              | "(" , expr , ")" ;
if_expr       = "if" , expr , "then" , expr , "else" , expr ;                  (* L2 行为分析：条件表达式 *)
derive_ref    = "@" , IDENT ;
close_reason_ref = "close_reason" ;
func_call     = [ IDENT , "." ] , IDENT , "(" , [ expr , { "," , expr } ] , ")" ;
                (* window.has 的第二参数为 STRING 字面量（目标字段名），语法上走 expr→primary→STRING，
                   语义上由编译器按 T12 规则校验为目标 window 的字段名。 *)
agg_pipe_expr = source_ref , [ "." , IDENT | "[" , STRING , "]" ] , { "|" , transform } , "|" , measure ;
field_ref     = IDENT
              | IDENT , "." , IDENT
              | IDENT , "[" , STRING , "]" ;              (* 访问带点字段：alias[\"detail.sha256\"] *)

(* 词法（简化） *)
IDENT         = ALPHA , { ALPHA | DIGIT | "_" } ;
NUMBER        = DIGIT , { DIGIT } , [ "." , DIGIT , { DIGIT } ] ;
INTEGER       = DIGIT , { DIGIT } ;                           (* 非负整数，用于 contract hits/hit[idx] *)
STRING        = '"' , { ANY - '"' } , '"' ;
DURATION      = DIGIT , { DIGIT } , ( "s" | "m" | "h" | "d" ) ;
ALPHA         = "a".."z" | "A".."Z" | "_" ;
DIGIT         = "0".."9" ;
ANY           = ? any unicode char ? ;
```

### 7.1 保留标识符
- `_in`：`|>` 后续 stage 的隐式输入别名，编译器注入，用户必须以此名引用前级输出。
- `@name`：`derive` 派生项引用前缀，不可作为普通字段名使用。
- `close_reason`：窗口关闭原因只读上下文字段（`timeout` / `flush` / `eos`）。

### 7.1.1 带点字段名访问
- `.wfs` 允许字段名包含 `.`（如 `detail.sha256`）。
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
| `hit` | `hit(cond)` → float | L2 | 条件命中映射：`true -> 1.0`，`false -> 0.0` |
| `derive` | `derive { x = expr; ... }` | L2 | 特征派生块：复用表达式结果，供 `score` 与 `yield` 引用 |
| `score`（分项） | `score { item = expr @ weight; ... }` | L2 | 分项评分聚合；用于解释每项贡献 |
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

- `score` 输出支持两种写法：`score(expr)`（简洁）与 `score { item = expr @ weight; ... }`（可解释）。
- `score` 分项块中，单项贡献 = `expr * weight`；总分为所有分项贡献之和。
- `derive` 先于 `score`/`yield` 求值；同一窗口内每个派生项只计算一次。
- `derive` 引用使用 `@name`；可用于 `score` 与 `yield`，不可用于 `events` 过滤。
- 集合判定（L2）：`window.has(field)` 判断“当前上下文字段值”是否存在于目标 window 的同名字段值集中；目标 window 须为静态集合（`over = 0`）或维度表。返回 `bool`。

**`window.has(field)` 语义（L2）：**

| 问题 | 规则 |
|------|------|
| `field` 参数含义 | 参数是**当前上下文的字段值表达式**（如 `domain`、`req.domain`），不是字段名字面量。 |
| 匹配目标字段 | 默认按“同名字段”匹配：`window.has(req.domain)` 会在目标 window 中查找字段 `domain`。 |
| 异名字段匹配 | 使用两参数形式：`window.has(ctx_expr, "target_field")`，如 `bad_ips.has(req.sip, "ip")`。第二参数为 STRING 字面量（目标 window 字段名），避免与别名/标签解析冲突。 |
| 匹配范围 | 仅在目标字段上做等值成员判定，不做全字段扫描。 |
| 可用窗口类型 | 目标 window 必须是静态集合（`over = 0`）或维度表（低频更新、用于 enrich/查表；在 `runtime.toml` 通过 `role = "dimension"` 声明）。 |
| 判定时点 | 在规则求值时基于目标 window 的当前快照判定。 |

**聚合双写法（统一语义）：**

| 写法 | 示例 | 说明 |
|------|------|------|
| 管道式 | `scan.dport | distinct | count` | 常用于 `match` 步骤 |
| 函数式 | `distinct(scan.dport)` | 常用于 `yield` / `score(expr)` 表达式 |

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
- `on event`：事件到达时求值；`on close`：窗口关闭时求值（固定窗口到期、session gap 超时、flush 或 eos）。
- 若省略 `on close`，关闭阶段视为恒为 true，不额外阻断命中。
- `null` 与运行时异常按 `runtime.eval.mode` 执行（`strict` 或 `lenient`），避免规则结果漂移。
- `join`：固定 LEFT JOIN 语义。
- `conv`：仅 `tumble` 可用。

---

## 8. 编译模型（整合原方案）

### 8.1 编译流水线
1. 变量预处理：`$VAR` / `${VAR:default}`。
2. 解析：`.wfs` + `.wfl` -> AST。
3. 语义检查：字段、类型、window 引用、over 约束。
4. desugar：展开 `|>`、隐式 stage、`conv` 钩子。
5. 生成 Core IR（Bind/Match/Join/Yield）。
6. 输出 RulePlan（供 MatchEngine 执行）。
7. 若存在 `contract_block`，输出 ContractPlan（供 `wf test` 执行；不进入生产运行时）。

### 8.2 RulePlan 结构
```rust
pub struct RulePlan {
    pub name: String,
    pub binds: Vec<BindPlan>,
    pub match_plan: MatchPlan,
    pub joins: Vec<JoinPlan>,
    pub entity_plan: EntityPlan,
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
    pub derives: Vec<DeriveItemAst>,           // 可选
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

pub struct DeriveItemAst {
    pub name: String,
    pub expr: ExprAst,
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

pub enum CloseReason {
    Timeout,
    Flush,
    Eos,
}

pub struct MatchPlan {
    pub keys: Vec<FieldRef>,
    pub window_spec: WindowSpec,
    pub event_steps: Vec<StepPlan>,
    pub close_steps: Vec<StepPlan>,            // 为空时等价 close_ok = true
    pub derive_plans: Vec<DerivePlan>,
}
```

- 运行期判定：`event_ok = eval(event_steps)`，`close_ok = close_steps.is_empty() || eval(close_steps)`。
- 关闭上下文：执行 `on close` 时注入 `close_reason ∈ {timeout, flush, eos}`。
- 派生求值：`derive_ctx = eval_once(derive_plans)`，供 `score`/`yield` 共享。
- 最终命中条件：`event_ok && close_ok`。
- `on_close` 仅做条件判定，不新增独立 `join/yield` 执行动作。

### 8.4 explain 输出（必须）
- 展开后的规则（去语法糖）。
- 状态机图（状态/转移/超时）。
- 字段血缘（字段从哪来、在哪步变换）。
- 评分展开（`score { ... }` 展开后的分项贡献与总分公式）。
- 派生图（`derive` 项依赖 DAG 与求值顺序）。

---

## 9. 运行时执行模型（整合原方案）

### 9.1 固定执行链
`Receiver -> Router -> WindowStore -> MatchEngine -> JoinExecutor -> YieldWriter -> Sink`

### 9.2 事件驱动
- 新事件只分发到“引用该 window”的规则。
- 匹配成功后触发 join/yield。
- timeout wheel 定期触发窗口关闭与 maxspan 过期，并以 `close_reason = timeout` 执行 `on close` 求值。
- 显式 flush 触发时，`close_reason = flush`。
- 输入流结束（end-of-stream）触发时，`close_reason = eos`。

### 9.3 并发控制
- 全局 `Semaphore(executor_parallelism)`。
- join/yield 包裹超时。
- 规则执行与 IO 解耦（避免阻塞主分发循环）。

---

## 10. 容错与一致性语义

### 10.1 传输层（单向 TCP，Best-Effort）
- 语义：Best-Effort / At-Most-Once。
- 协议：`wp-motor -> wp-reactor` 单向推送，帧格式为 `[4B len][payload]`。
- 不使用应用层 ACK，不引入发送端/接收端 WAL 与重放协议。
- 背压：以 TCP 流控为主，叠加本地有界队列保护。

**可靠性边界（明确承诺）：**
- TCP 仅保证“连接存活期间的字节传输”，不保证“接收端已处理/已持久化”。
- 发生进程崩溃、断连或队列溢出时，允许数据丢失；丢失数据不可自动重放恢复。
- 本模式适用于检测场景（允许少量漏检），不适用于审计级“逐条必达”场景。

**过载策略（必须配置）：**

| 项 | 规范 |
|----|------|
| 队列容量 | 接收侧/发送侧均使用有界队列，避免无界内存增长。 |
| 溢出策略 | `drop_oldest`（默认）\| `drop_newest` \| `sample`。 |
| 退化策略 | 连续溢出时触发限流或采样，并上报质量降级指标。 |
| 可观测性 | 至少暴露 `dropped_events_total`、`send_queue_full_total`、`reconnect_total`、`backpressure_seconds_total`。 |

建议配置（`runtime.toml`）：

```toml
[transport]
mode = "best_effort"            # 固定：best_effort
frame = "len32_payload"         # [4B len][payload]
max_frame_bytes = 1048576
read_timeout = "30s"
write_timeout = "30s"

[transport.backpressure]
queue_capacity = 65536
on_overflow = "drop_oldest"     # drop_oldest | drop_newest | sample
sample_ratio = 0.2               # on_overflow=sample 时生效
max_block = "200ms"

[monitoring.thresholds]
dropped_events_ratio_warn = 0.001
backpressure_seconds_warn = 30
```

### 10.2 基线持久化（行为分析）
- 目标：避免重启后 baseline 从零冷启动，减少误报尖峰。
- 快照对象：`baseline(expr, dur[, method])` 的聚合状态（mean/ewma/median）。
- 恢复顺序：启动时加载最新快照后，从实时输入继续更新；历史缺口需依赖外部补数。

建议配置（`runtime.toml`）：

```toml
[behavior.baseline_store]
dir = "/var/lib/warpfusion/baseline"
snapshot_interval = "5m"
max_snapshots = 24
restore = "latest"             # latest | clean
```

### 10.3 风险告警幂等
- `alert_id = sha256(rule_name + scope_key + window_range)`。
- AlertSink 本地去重缓存 + 下游透传（风险告警输出）。

### 10.4 事件时间
- per-window：`watermark`、`allowed_lateness`、`late_policy`。
- 迟到策略：`drop | revise | side_output`。

---

## 11. 热加载策略

- 仅 `.wfl` 与 `[vars]` 支持热加载。
- `.wfs` 与运行时物理参数变更需重启。
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
- `derive` 块可选；若存在，位于 `on event/on close` 之后。
- step 必须显式 source（不允许空 source）。

**双阶段求值语义：**

| ID | 规则 |
|----|------|
| M1 | `on event` 在每次事件到达时求值，更新事件阶段命中状态 `event_ok`。 |
| M2 | `on close` 在窗口关闭时求值一次，得到关闭阶段命中状态 `close_ok`；若缺省则 `close_ok = true`。 |
| M3 | 规则在该窗口上的最终命中条件为 `event_ok && close_ok`。 |
| M4 | `on close` 只做条件判定，不引入新的 `join/yield` 动作；命中后仍按同一 stage 的 `join -> yield` 流程输出。 |

**derive 求值规则：**

| ID | 规则 |
|----|------|
| D1 | `derive` 在 `on event/on close` 判定后求值，先于 `score` 与 `yield`。 |
| D2 | `derive` 在同一窗口内按声明顺序求值；每项仅计算一次并缓存。 |
| D3 | `@name` 仅可引用同一 `derive` 块内已声明项；前向引用编译错误。 |
| D4 | `derive` 禁止循环依赖；出现环时报编译错误。 |

**match key 解析规则：**

| ID | 规则 |
|----|------|
| K1 | `match<k1,k2:dur>` 中未限定名 key（如 `sip`）要求在本 match 涉及的所有事件源中都存在同名字段。 |
| K2 | key 可用限定名（如 `fail.sip`）消歧；仅影响解析，不改变“各事件源都需可提取该 key”的约束。 |
| K3 | 多事件源字段名不同（如 `fail.sip` vs `scan.src_ip`）时，**不能**直接在同一 match key 中做自动映射；需先在上游 `.wfs` 对齐字段名，或用前级规则 `yield` 归一化后再匹配。 |
| K4 | key 字段跨事件源类型必须一致；不允许 `ip` 与 `chars`、`digit` 与 `chars` 混用。 |
| K5 | 复合 key 按位置形成键元组（`<k1,k2>`），各位置按 K1~K4 独立校验。 |

**session 模式约束：**

| ID | 规则 |
|----|------|
| S1 | `session(gap)` 中 `gap` 必须 > 0。 |
| S2 | session 关闭条件：同 key 在 `gap` 内无新事件（静默超时），或收到流结束/显式 flush。 |
| S3 | `on close` 在 session 关闭时求值；关闭时点为 S2 触发时刻。 |
| S4 | session 无固定 duration；状态保留仍受 window `over` 与运行时上限约束。超限时强制切段并触发一次 `on close` 求值。 |

**`close_reason` 语义：**

| ID | 规则 |
|----|------|
| C1 | `close_reason` 为只读上下文字段，取值集合固定为：`timeout` / `flush` / `eos`。 |
| C2 | `close_reason` 仅允许在 `on close` 与其后的 `derive`/`yield` 中引用；在 `on event` 中引用编译错误。 |
| C3 | 关闭触发映射：定时器/窗口到期 -> `timeout`，显式 flush -> `flush`，输入流结束 -> `eos`。 |
| C4 | `on close` 规则可显式按原因分流（如 `close_reason == "timeout"`），用于抑制 flush/eos 场景误报。 |

**窗口关闭触发状态图（timeout / flush / eos）：**

```text
              +----------------------------+
              | Window Active (event_ok ?) |
              +-------------+--------------+
                            |
         close(timeout/flush/eos) arrives
                            v
              +----------------------------+
              | Inject close_reason        |
              | eval on close -> close_ok  |
              +-------------+--------------+
                            |
              +-------------+--------------+
              | event_ok && close_ok ?     |
              +------+---------------------+
                     |yes                  |no
                     v                     v
      +----------------------------+   +------------------+
      | emit join -> yield         |   | no emit          |
      | close_reason=timeout/..    |   | (window closes)  |
      +----------------------------+   +------------------+
```

**关闭原因与输出判定（统一规则）：**

| 关闭触发 | `close_reason` | 是否输出 |
|----------|----------------|----------|
| 窗口到期 / session gap 超时 | `timeout` | 仅当 `event_ok && close_ok` |
| 显式 flush | `flush` | 仅当 `event_ok && close_ok` |
| 输入流结束（EOS） | `eos` | 仅当 `event_ok && close_ok` |

> 说明：`close_reason` 仅标记“为什么关闭”，不改变命中判定公式；命中公式始终是 `event_ok && close_ok`。

**生产触发策略建议（flush / eos）：**

| 场景 | 推荐触发 | 目的 | 规则侧建议 |
|------|----------|------|------------|
| 日常在线运行 | 仅 `timeout` | 维持稳定窗口生命周期 | 缺失检测优先按 `close_reason == "timeout"` 判定，避免维护期误报 |
| 规则热加载 / 滚动发布 | 主动 `flush` | 在切换前收敛在途窗口并生成可审计关闭输出 | 对“必须完整窗口”的规则，可在 `on close` 中显式排除 `flush` |
| 有界批次输入完成 | `eos` | 明确批次边界并触发最终关闭 | 批处理规则可允许 `eos` 触发输出；实时规则可对 `eos` 单独分流 |
| 异常中断（崩溃/kill -9） | 无显式触发（恢复后进入新窗口周期） | 保证流程可继续，不承诺中断期间数据补回 | 不应依赖 `flush/eos` 才能产出关键检测结果 |

**运维流程建议：**
1. **滚动发布/重启：** `stop intake -> wait in-flight drain -> trigger flush -> wait flush outputs -> switch binary`。
2. **热加载规则：** 新规则先编译通过并原子替换，再对旧代状态执行一次 `flush`，确保审计链路连续。
3. **EOS 使用边界：** `eos` 仅用于“输入确实结束”的场景；对常驻流若频繁出现 `eos`，应视为上游稳定性异常并触发运维告警。

**监控指标建议：**
- `window_close_total{reason=timeout|flush|eos}`
- `window_emit_total{reason=timeout|flush|eos}`
- `window_emit_suppressed_total{reason=...}`（`event_ok && close_ok` 为 false）
- `unexpected_eos_total`（常驻流应接近 0）

**推荐配置（runtime.toml）：**

```toml
[runtime.close]
# 审计字段映射（与语义规则 C3 保持一致）
flush_emit_reason = "flush"
eos_emit_reason = "eos"

# 运维触发策略
enable_flush_on_reload = true      # 热加载时自动触发一次 flush
enable_flush_on_shutdown = true    # 优雅停机前触发 flush
drain_timeout = "30s"              # 等待在途窗口收敛
flush_wait_timeout = "10s"         # flush 后等待输出收敛
eos_for_bounded_input_only = true  # 仅有界输入允许主动 eos

[monitoring.thresholds]
unexpected_eos_total_per_5m = 1            # >0 即建议触发运维告警
flush_close_ratio_warn = 0.30              # 5m 内 flush close 占比运维告警阈值
window_emit_suppressed_ratio_warn = 0.20   # 抑制率预警
window_emit_suppressed_ratio_crit = 0.40   # 抑制率严重运维告警
```

- `flush_close_ratio` 计算口径：`close_total{reason=flush} / close_total{reason=timeout|flush|eos}`。
- `window_emit_suppressed_ratio` 计算口径：`emit_suppressed_total / (emit_total + emit_suppressed_total)`。

### 12.3 Join
- `on` 两侧字段必须可解析且类型**一致**（跨类型编译错误）。
- join 右侧字段（来自 join window）**必须**以 `window_name.field` 限定名引用；左侧可使用上下文字段（如 `sip` 或 `fail.sip`）。
- 多 join 按声明顺序执行。

### 12.4 Yield
- 目标 window 必须存在，且满足：`stream` 为空（纯输出 window）并且 `over > 0`。
- yield 命名参数 + 系统字段必须是目标 window fields 的**子集**（名称和类型匹配）。
- yield 中不得出现 window 未定义的字段名；未覆盖的非系统字段值为 null。
- 自动注入系统字段：`rule_name`(chars)、`emit_time`(time)、`score`(float)、`entity_type`(chars)、`entity_id`(chars)、`close_reason`(chars, nullable)。
- 使用 `score { ... }` 时，额外注入 `score_contrib`(chars, JSON) 记录分项贡献明细。
- `score_contrib` 传递契约：内部表示为 `map<string,float>`；写出时按 sink 协议序列化并透传下游（JSON sink 输出对象，行式 sink 输出 JSON 字符串）。
- `score` 仅由 `match ... -> score_out` 产生（`score(expr)` 或 `score { ... }`）；`entity_type/entity_id` 仅由 `entity(type, id_expr)` 产生。
- `close_reason` 仅在关闭触发输出时取值（`timeout`/`flush`/`eos`）；非关闭触发输出为 `null`。
- `entity(type, id_expr)` 为必选声明。
- `score` 范围固定为 `[0,100]`；超出范围按运行时策略处理（默认 `clamp`）。
- `score`、`entity_type`、`entity_id`、`score_contrib` 为系统字段，禁止在 `yield` 命名参数中手工赋值。
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
| T12 | `window.has(x, "f")` 中 `"f"` 必须是 STRING 字面量且为目标 window 已定义字段名；`x` 类型必须与 `f` 类型一致 |
| T13 | `window.has(...)` 的目标 window 必须满足：`over = 0` 或被标记为维度表（dimension） |
| **── 行为分析扩展类型规则 ──** | |
| T14 | `if c then a else b` — `c` 必须为 `bool`；`a` 与 `b` 类型必须一致；返回类型 = `a` 的类型 |
| T15 | `hit(c)` — `c` 必须为 `bool`；返回 `float`（`true -> 1.0`，`false -> 0.0`） |
| T16 | `time_diff(t1, t2)` — `t1`、`t2` 必须为 `time` 类型；返回 `float`（秒） |
| T17 | `time_bucket(f, interval)` — `f` 必须为 `time` 类型；`interval` 为 DURATION 字面量；返回 `time` |
| T18 | `contains(f, pat)` — `f` 必须为 `chars`/`ip`/`hex`；`pat` 须为 STRING 字面量；返回 `bool` |
| T19 | `regex_match(f, pat)` — `f` 必须为 `chars`/`ip`/`hex`；`pat` 须为 STRING 字面量（编译期校验正则合法性）；返回 `bool` |
| T20 | `len(f)` — `f` 必须为 `chars`/`ip`/`hex`；返回 `digit` |
| T21 | `lower(f)` / `upper(f)` — `f` 必须为 `chars`；返回 `chars` |
| T22 | `collect_set(a.f)` / `collect_list(a.f)` — 参数必须为 Column 投影；返回 `array/T`（T 为 f 的类型） |
| T23 | `first(a.f)` / `last(a.f)` — 参数必须为 Column 投影；返回类型 = f 的类型 |
| T24 | `stddev(a.f)` — f 必须为 `digit` 或 `float`；返回 `float` |
| T25 | `percentile(a.f, p)` — f 必须为 `digit` 或 `float`；`p` 须为 digit 字面量且 0 ≤ p ≤ 100；返回 `float` |
| T26 | `baseline(expr, dur, method)` — expr 须为 `digit`/`float`；method 须为 STRING 字面量（`"mean"`/`"ewma"`/`"median"`）；返回 `float` |
| T27 | `match ... -> score(expr)` 中 `expr` 须为 `digit` 或 `float`；规则产出单一 `score: float` |
| T28 | `match ... -> score { item = expr @ weight; ... }` 中每个 `expr` 必须为 `digit` 或 `float` |
| T29 | `score` 分项 `weight` 必须是 NUMBER 字面量，且 `0 <= weight <= 100` |
| T30 | `score` 分项名在同一 block 内必须唯一 |
| T31 | 规则总分范围为 `[0,100]`；超出范围按 `on_overflow` 策略处理（默认 `clamp`） |
| T32 | `entity(type, id_expr)` 中 `type` 必须为编译期常量（IDENT 或 STRING） |
| T33 | `entity(type, id_expr)` 中 `id_expr` 必须为可标识标量（`chars`/`ip`/`hex`/`digit`）；执行期统一归一化为 `chars` |
| T34 | 每条规则必须且仅能声明一个 `entity(type, id_expr)` |
| T35 | 每个 `match` 必须且仅能产出一个 `score_out`（`score(expr)` 或 `score { ... }`） |
| T36 | `yield` 命名参数中禁止出现 `score` / `entity_type` / `entity_id` / `score_contrib`（系统注入） |
| T37 | `derive` 项名在同一 `match` 内必须唯一；与系统保留名冲突时报编译错误 |
| T38 | `derive` 引用 `@name` 必须可解析到同一块内已声明项 |
| T39 | `derive` 图必须无环；存在环路时报编译错误 |
| T40 | `@name` 的类型等于其 `derive` 表达式类型；在 `score` 中使用时必须满足对应数值约束 |
| T41 | `runtime.eval.mode` 仅允许 `strict` 或 `lenient`；默认 `strict` |
| T42 | `strict` 模式下，`null` 参与比较/算术/逻辑或运行时异常会中止当前 `(rule,entity,window)` 求值，且不输出 `yield` |
| T43 | `lenient` 模式下，`null`/异常按配置缺省值折算后继续求值，并记录运行时运维告警计数 |
| T44 | `close_reason` 类型为 `chars`，仅允许与字符串字面量 `"timeout"`/`"flush"`/`"eos"` 比较 |
| T45 | 在 `on event` 中引用 `close_reason` 编译错误 |
| T46 | `yield` 中引用 `close_reason` 时类型为 `chars?`（nullable），需与目标字段类型一致 |

**静态引用解析：**

| ID | 规则 |
|----|------|
| R1 | **跨步引用**：`label.field` / `label["field.with.dot"]` 中 `label` 必须是当前步骤的**前序步骤**标签；引用后续步骤编译错误 |
| R2 | **跨步字段类型**：`label.field` 或下标形式的字段类型由 label 所在步骤的事件集 window schema 静态确定；不存在的字段编译错误 |
| R3 | **events 字段引用**（规则体内）：`alias.field` / `alias["field.with.dot"]` 中 `alias` 必须在 `events {}` 中声明，字段必须在对应 window 的 `fields {}` 中定义 |
| R3a | **events 过滤表达式**（`event_decl` 的 `&& expr` 部分）：过滤表达式的上下文是**单一 window**，裸 IDENT 直接解析为该 window 的字段名（如 `action == "failed"`）。若同时需要引用其他 window（如 `window.has`），则自身别名也可前置引用（如 `req: dns_query && bad_domains.has(req.domain)`），编译器将 `event_decl` 的 alias 在过滤表达式作用域内提前注册。 |
| R4 | **join 字段引用**：join window 字段**必须**以 `window_name.field` 或 `window_name["field.with.dot"]` 限定名引用；裸字段名不搜索 join window |
| R5 | **解析优先级**（规则体内）：events 别名 → match 步骤标签 → 聚合函数结果；未找到即编译错误，不做回退猜测。**例外**：`event_decl` 过滤表达式内裸 IDENT 按 R3a 规则优先解析为当前 window 字段 |
| R6 | **OR 分支 nullable**：`\|\|` 各分支标签字段在后续可引用，但标注为 nullable；对 nullable 字段做 `sum`/`avg` 时编译器警告 |
| R7 | **derive 引用解析**：`@name` 仅在当前 `match` 的 `derive` 作用域内解析，不回退到 events/join/步骤标签 |
| R8 | **close_reason 解析**：`close_reason` 解析为关闭上下文字段，不参与 events/join/步骤标签同名搜索 |

### 12.7 实体主键与评分聚合

| ID | 规则 |
|----|------|
| E1 | 实体键通过 `entity(type, id_expr)` 显式声明；编译器生成系统字段 `entity_type/entity_id`。 |
| E2 | `type` 建议使用稳定字面量（如 `user`/`host`/`ip`/`process`）；同一规则内不可变化。 |
| E3 | `id_expr` 允许引用当前上下文字段或表达式结果；执行期若为空则该次输出丢弃并计入 `entity_id_null_dropped`。 |
| E4 | 跨规则评分累加键固定为 `(entity_type, entity_id, time_bucket)`；缺少任一维度不得参与累加。 |
| E5 | `yield` 中禁止手工写 `score/entity_type/entity_id/score_contrib`，避免与系统注入冲突。 |

### 12.8 风险等级映射层（可选）

- WFL 规则层只产出 `score`，不声明等级字段。
- 若需要等级展示，由运行时按统一映射策略从 `score` 派生 `risk_level`。
- 映射策略需版本化（如 `risk_level_map_version`），便于审计与回放一致。
- 等级映射默认基于 `[0,100]` 评分刻度。

建议配置（`runtime.toml`）：

```toml
[risk]
risk_level_map = "default_v1"

[risk.score]
min = 0
max = 100
on_overflow = "clamp"      # clamp | error
emit_contrib = true         # 开启时输出 score_contrib(JSON)
contrib_format = "auto"      # auto | object | json_string

[risk.level_maps.default_v1]
low_max = 30
medium_max = 60
high_max = 85
# > high_max => critical
```

### 12.9 `score_contrib` 下游传递契约

- 生成时机：`match -> score_out` 求值完成后生成 `score_contrib`，再进入 `entity -> yield`。
- 透传字段：`rule_name`、`entity_type`、`entity_id`、`score`、`score_contrib` 一并写入输出 row。
- Sink 编码：
  - `contrib_format = "object"`：JSON 类 sink 以嵌套对象输出。
  - `contrib_format = "json_string"`：行式 sink 以 JSON 字符串输出。
  - `contrib_format = "auto"`：由 sink 类型自动选择（默认）。
- 下游约束：下游不得重算总分，应以 `score` 为准，仅将 `score_contrib` 用于解释与审计。

下游消费示例：

```json
{
  "rule_name": "entity_risk_score",
  "entity_type": "host",
  "entity_id": "host:node-01",
  "score": 75.0,
  "score_contrib": {
    "burst": 30.0,
    "uniq_dests": 25.0,
    "ps_hit": 20.0,
    "exfil": 0.0,
    "total": 75.0
  }
}
```

```json
{
  "rule_name": "entity_risk_score",
  "entity_type": "host",
  "entity_id": "host:node-01",
  "score": 75.0,
  "score_contrib": "{\"burst\":30.0,\"uniq_dests\":25.0,\"ps_hit\":20.0,\"exfil\":0.0,\"total\":75.0}"
}
```

- 上例第一条对应 `contrib_format = "object"`；第二条对应 `contrib_format = "json_string"`。

### 12.10 Null/异常语义矩阵（strict / lenient）

- 目标：统一 `null` 与运行时异常行为，避免不同执行节点产生结果漂移。

| 场景 | `strict` | `lenient` | 说明 |
|------|----------|-----------|------|
| 比较（`== != > >= < <=`）任一侧为 `null` | 运行时错误 `E_NULL_CMP`，中止当前求值 | 结果为 `false` | 避免隐式三值逻辑渗透到检测语义 |
| 算术（`+ - * / %`）任一侧为 `null` | 运行时错误 `E_NULL_ARITH` | 使用 `lenient_numeric_default`（默认 `0.0`）替代 | 仅在 `lenient` 折算 |
| 逻辑（`&& ||`）任一侧为 `null` | 运行时错误 `E_NULL_BOOL` | 使用 `lenient_bool_default`（默认 `false`）替代 | 保持 bool 决策稳定 |
| `hit(cond)` 中 `cond = null` | 运行时错误 `E_NULL_HIT` | 返回 `0.0` | 命中不确定时默认不加分 |
| 聚合 `sum/avg/min/max` 输入含 `null` | 跳过 `null`；若有效样本数为 0 则报 `E_EMPTY_AGG` | 跳过 `null`；若有效样本数为 0 返回 `lenient_numeric_default` | 与窗口稀疏数据兼容 |
| 聚合 `count(set)` | 与 `null` 无关，按事件条数计数 | 同 `strict` | `count(set)` 不看列值 |
| 聚合 `distinct(col)` | 跳过 `null` 值 | 同 `strict` | 统一高基数统计口径 |
| 除零（`x / 0`） | 运行时错误 `E_DIV_ZERO` | 返回 `lenient_numeric_default` | 建议配合运维告警计数监控 |
| `derive` 项求值异常 | 当前窗口求值失败，不输出 | 当前项按默认值折算，继续后续项 | 仍记录错误计数与规则名 |

对照样例（同一输入在 strict / lenient 下的结果）：

```wfl
rule null_semantics_demo {
  events {
    e: endpoint_events
  }
  match<host_id:1h:tumble> {
    on close {
      e | count >= 1;
    }
    derive {
      p95_out = percentile(e.bytes_out, 95);   // bytes_out 全为 null 时会触发空聚合
      first_proc = first(e.process);           // 可能为 null
    }
  } -> score {
    exfil = hit(@p95_out > 1000000) @ 70.0;
    script = hit(@first_proc == "powershell") @ 30.0;
  }
  entity(host, e.host_id)
  yield risk_scores (
    host_id = e.host_id,
    p95_bytes = @p95_out
  )
}
```

输入（同一窗口）：`host_id="node-01"`，`bytes_out=null`，`process=null`。

| 模式 | 结果 |
|------|------|
| `strict` | `derive.p95_out` 触发 `E_EMPTY_AGG`，当前 `(rule,entity,window)` 求值中止，不输出 `yield`。 |
| `lenient` + `coerce` | `p95_out = lenient_numeric_default(0.0)`，`first_proc` 比较结果为 `false`，总分 `score = 0.0`，可正常输出并附带 `score_contrib`。 |
| `lenient` + `drop_rule` | 发生异常时行为与 strict 接近：当前窗口不输出，但仍记入 lenient 统计。 |

回放样例（输入 -> 输出）：

输入事件（同一窗口）：

```json
[
  {
    "host_id": "node-01",
    "event_time": "2026-02-16T10:00:00Z",
    "bytes_out": null,
    "process": null
  }
]
```

`strict`（无输出，当前窗口被中止）：

```json
[]
```

`lenient + coerce`（继续输出）：

```json
[
  {
    "rule_name": "null_semantics_demo",
    "entity_type": "host",
    "entity_id": "node-01",
    "score": 0.0,
    "score_contrib": {
      "exfil": 0.0,
      "script": 0.0,
      "total": 0.0
    },
    "host_id": "node-01",
    "p95_bytes": 0.0
  }
]
```

`lenient + drop_rule`（无输出，但计数增加）：

```json
[]
```

建议配置（`runtime.toml`）：

```toml
[runtime.eval]
mode = "strict"                 # strict | lenient
lenient_numeric_default = 0.0    # lenient 生效
lenient_bool_default = false     # lenient 生效
lenient_string_default = ""      # lenient 生效
lenient_on_error = "coerce"      # coerce | drop_rule

[runtime.close]
flush_emit_reason = "flush"      # 固定为 flush，供审计
eos_emit_reason = "eos"          # 固定为 eos，供审计
```

- 监控建议：暴露 `eval_null_coerced_total`、`eval_runtime_error_total`、`eval_drop_rule_total` 三类指标。

### 12.11 规则契约测试（given / expect）

- 目标：把规则正确性前置到 CI，在发布前通过可回放、可断言的小样本契约测试拦截回归。
- 运行入口：`wf test --contracts rules/security.wfl`；可用 `--contract <name>` 只跑单个契约。
- 契约块只用于测试，不参与生产执行链。

**契约语义规则：**

| ID | 规则 |
|----|------|
| CT1 | `contract <name> for <rule_name>` 的目标规则必须存在且唯一。 |
| CT2 | `given` 中 `row(alias, ...)` 的 `alias` 必须在目标规则 `events {}` 中声明。 |
| CT3 | `row` 字段名允许 `IDENT` 或 `STRING`（用于 `detail.sha256` 等带点字段）。 |
| CT4 | `row` 按声明顺序注入；缺失字段按 `null` 处理，类型转换与运行时一致。 |
| CT5 | `tick(dur)` 推进测试时钟并触发窗口关闭；若未显式 `tick`，测试结束自动按 `options.close_trigger`（默认 `timeout`）收尾。 |
| CT6 | `expect { hits ... }` 断言该契约产生的输出条数；`hit[i]` 要求 `0 <= i < hits`。 |
| CT7 | `hit[i].field("x")` 读取第 `i` 条输出字段；字段不存在或类型不匹配即断言失败。 |
| CT8 | `options.eval_mode` 仅允许 `strict|lenient`；`options.close_trigger` 仅允许 `timeout|flush|eos`。 |

**CI 建议：**
- Pull Request 必跑：`wf test --contracts <all-wfl-files>`。
- 失败即阻断合并，并输出失败契约名、失败断言、输入重放片段。

**`wf test` 失败输出契约（建议实现）：**

- 退出码：全部通过返回 `0`；任一契约失败返回 `2`；解析/编译错误返回 `3`。
- 输出层级：先给 `summary`，再列 `failures[]`；每个失败项都可独立重放。

推荐 JSON（`--format json`）结构：

```json
{
  "summary": {
    "total": 12,
    "passed": 11,
    "failed": 1,
    "duration_ms": 842
  },
  "failures": [
    {
      "contract": "dns_no_response_timeout",
      "rule": "dns_no_response",
      "code": "E_ASSERT_EQ",
      "message": "hit[0].close_reason expected timeout but got flush",
      "assertion": "hit[0].close_reason == \"timeout\"",
      "actual": "flush",
      "replay": {
        "rows": 1,
        "ticks": ["31s"],
        "close_trigger": "flush"
      },
      "loc": {
        "file": "rules/dns.wfl",
        "line": 1333
      }
    }
  ]
}
```

建议错误码：
- `E_PARSE_CONTRACT`：契约语法错误。
- `E_RULE_NOT_FOUND`：`for <rule>` 目标不存在。
- `E_GIVEN_ALIAS`：`row(alias, ...)` 的 alias 未在规则 `events` 中声明。
- `E_ASSERT_EQ`：等值断言失败。
- `E_ASSERT_BOUNDS`：`hit[i]` 越界。
- `E_FIELD_MISSING`：`hit[i].field("x")` 字段不存在。

终端摘要（默认文本）建议：
- `FAILED contracts=1/12 file=rules/dns.wfl`
- `- dns_no_response_timeout: E_ASSERT_EQ at rules/dns.wfl:1333`
- `  assertion: hit[0].close_reason == "timeout"`
- `  actual: flush`
- `  replay: wf test --contracts rules/dns.wfl --contract dns_no_response_timeout --dump-replay`

---

## 13. 示例（从原设计迁移后的标准写法）

### 13.1 阈值检测
```wfl
use "security.wfs"

rule brute_force {
  events {
    fail: auth_events && action == "failed"
  }
  match<sip:5m> {
    on event {
      fail | count >= 3;
    }
  } -> score(70.0)
  entity(ip, fail.sip)
  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} failed {} times", fail.sip, count(fail))
  )
}
```

### 13.2 时序关联 + enrich
```wfl
use "security.wfs"

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
  } -> score(if count(fail) > 10 then 90.0 else 70.0)
  join ip_blocklist on sip == ip_blocklist.ip
  entity(ip, fail.sip)
  yield security_alerts (
    sip = fail.sip,
    threat = ip_blocklist.threat_level,
    message = fmt("{} brute+scan", fail.sip)
  )
}
```

### 13.3 缺失检测（A -> NOT B）
```wfl
use "dns.wfs"

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
      resp && close_reason == "timeout" | count == 0;
    }
  } -> score(50.0)
  entity(ip, req.sip)
  yield security_alerts (
    sip = req.sip,
    domain = req.domain,
    message = fmt("{} query {} no response", req.sip, req.domain)
  )
}
```

### 13.4 多级管道（L3）
```wfl
use "security.wfs"

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
  } -> score(80.0)
  entity(ip, _in.sip)
  yield security_alerts (
    sip = _in.sip,
    port_count = count(_in),
    message = fmt("{} scanned {} ports", _in.sip, count(_in))
  )
}
```

### 13.5 用户会话行为分析（L3 行为分析）
```wfl
use "access.wfs"

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
  } -> score(
    if distinct(op.resource) > 50 then 95.0
    else if time_diff(last(op.event_time), first(op.event_time)) > 1800 then 75.0
    else 55.0
  )
  entity(user, op.uid)
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
use "security.wfs"

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
    derive {
      burst = count(e) / baseline(count(e), 7d);
      uniq_dests = distinct(e.dest_ip);
      p95_out = percentile(e.bytes_out, 95);
      ps_hit = hit(count(ps) > 0);
      exfil = hit(@p95_out > baseline(avg(e.bytes_out), 7d) * 3.0);
    }
  } -> score {
    burst = hit(@burst > 2.0) @ 30.0;
    uniq_dests = hit(@uniq_dests > 100) @ 25.0;
    ps_hit = @ps_hit @ 20.0;
    exfil = @exfil @ 25.0;
  }
  entity(host, e.host_id)
  yield risk_scores (
    host_id = e.host_id,
    event_count = count(e),
    unique_dests = @uniq_dests,
    p95_bytes = @p95_out,
    processes = collect_set(e.process),
    message = fmt("{} risk score computed over 1h window", e.host_id)
  )
}
```

### 13.7 登录行为基线偏离（L2/L3 行为分析）
```wfl
use "auth.wfs"

rule login_anomaly {
  events {
    login: auth_events && action == "success"
  }
  match<uid:1h:tumble> {
    on close {
      login | count >= 1;
    }
  } -> score {
    surge = hit(count(login) > baseline(count(login), 30d, "ewma") * 3.0) @ 50.0;
    volume = hit(count(login) > 20) @ 30.0;
    geo_spread = hit(distinct(login.geo_city) > 3) @ 20.0;
  }
  entity(user, login.uid)
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

### 13.8 规则契约测试（CI 前置）
```wfl
contract dns_no_response_timeout for dns_no_response {
  given {
    row(req,
      query_id = "q-1",
      sip = "10.0.0.8",
      domain = "evil.test",
      event_time = "2026-02-17T10:00:00Z"
    );
    tick(31s);
  }
  expect {
    hits == 1;
    hit[0].score == 50.0;
    hit[0].close_reason == "timeout";
    hit[0].entity_type == "ip";
    hit[0].entity_id == "10.0.0.8";
    hit[0].field("domain") == "evil.test";
  }
  options {
    close_trigger = timeout;
    eval_mode = strict;
  }
}
```

---

## 14. 与原方案差异与兼容

### 14.1 保留内容
- 三文件模型（.wfs/.wfl/.toml）。
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

> 各 Phase 内条目按建议优先级排列；wf-datagen 各阶段穿插在其依赖就绪的最早 Phase。

### Phase A（先稳）
- Core IR + L1 + 可读语法 + lint/fmt。
- `wf test` 契约测试（given/expect）+ CI 阻断策略。
- **wf-datagen P0**：`.wfg` parser + schema 驱动随机生成 + seed 可复现 + JSONL/Arrow 输出（依赖 `.wfs` parser）。

### Phase B（增强）
- L2（join/baseline/window.has）+ explain/replay。
- L2 行为分析基础：`if/then/else`、字符串函数、时间函数。
- §17 P0-2 Join 时间语义一等化（snapshot/asof）—— 随 L2 join 一并落地。
- §17 P0-3 规则资源预算内建（limits）—— 先把资源安全兜住。
- §17 P0-1 显式 key 映射语法 —— 消除多源 key 歧义。
- **wf-datagen P1**：rule-aware（hit/near_miss/non_hit）+ Reference Evaluator + oracle 生成 + verify（依赖 `.wfl` compiler）。

### Phase C（高级）
- L3（`|>`/`conv`）+ 性能优化 + 分布式 V2 完善。
- §17 P0-4 输出契约版本化（yield contract）—— 下游契约治理。
- §17 P1-1 可组合规则片段（pattern/template）—— 提升规则复用。
- **wf-datagen P2**：时序扰动矩阵 + 压测模式 + PR 友好差异报告（依赖 P1）。

### Phase D（行为分析）
- L3 行为分析：session window、集合函数（`collect_set`/`collect_list`/`first`/`last`）、统计函数（`stddev`/`percentile`）。
- 增强 baseline（`ewma`/`median` + 持久化快照）。
- 单通道风险评分（`-> score(expr)` / `-> score { ... }`）+ `entity(type,id_expr)` + 跨规则评分累加。
- §17 P1-2 顺序/乱序不变性契约测试 —— 配合 wf-datagen 扰动能力做回归防线。

---

## 16. 设计原则（团队共识）

1. 显式优先于隐式。
2. 一种能力只保留一种主写法。
3. 编译器可解释性优先于语法炫技。
4. 先稳定 L1/L2 再扩 L3。
5. 文法、示例、执行器必须同步演进。


---

## 17. 下一阶段设计提案（P0/P1）

> 目标：在不破坏“简洁主链（BIND -> SCOPE -> JOIN -> ENTITY -> YIELD）”的前提下，提升表达力、可控性与可验证性。

### 17.1 P0-1 显式 key 映射语法（消除多源 key 歧义）

- 问题：`match<k1,k2:dur>` 在多源异名字段下需要上游对齐，规则层表达不够直接。
- 建议：引入 `key { logical = alias.field }`，把“逻辑键名”与“源字段”分离。

```wfl
match<:5m> {
  key {
    src = fail.sip;
    dst = scan.src_ip;
  }
  on event {
    fail | count >= 3;
    scan | count >= 10;
  }
} -> score(80)
```

- 约束：`key` 中逻辑键名唯一；映射字段类型必须一致；缺失键按编译错误处理。

### 17.2 P0-2 Join 时间语义一等化（snapshot / asof）

- 问题：仅有 `join ... on ...` 时，回放与在线对“维表取值时点”的理解可能不一致。
- 建议：显式声明 join 模式，禁止隐式推断。

```wfl
join ip_blocklist snapshot on sip == ip_blocklist.ip
join asset_profile asof on uid == asset_profile.uid within 24h
```

- 语义：
  - `snapshot`：使用当前可见最新维表。
  - `asof`：按事件时间回看最近版本（不晚于事件时间，且在 `within` 约束内）。

### 17.3 P0-3 规则资源预算内建（limits）

- 问题：规则可写性强，但缺少规则级资源上限，容易出现高基数状态膨胀。
- 建议：新增 `limits` 块，提供统一防护。

```wfl
limits {
  max_state = "512MB";
  max_cardinality = 200000;
  max_emit_rate = "1000/m";
  on_exceed = "throttle";   // throttle | drop_oldest | fail_rule
}
```

- 验收：触发上限时必须有稳定行为和指标（如 `rule_limit_exceeded_total`）。

### 17.4 P0-4 输出契约版本化（yield contract）

- 问题：下游消费字段演进时，缺少强约束版本标签。
- 建议：为输出声明契约版本，支持灰度与审计回放。

```wfl
yield risk_scores@v2 (
  host_id = e.host_id,
  score_detail = score_contrib
)
```

- 约束：同名 output window 的契约版本需可并存；跨版本字段变更需在编译期校验。

### 17.5 P1-1 可组合规则片段（pattern/template）

- 目标：复用常见安全模式，减少复制粘贴造成的语义漂移。
- 建议：支持参数化片段，在编译期展开到标准 RulePlan。

```wfl
pattern burst(alias, key, win, threshold) {
  match<${key}:${win}> {
    on event { ${alias} | count >= ${threshold}; }
  }
}
```

- 约束：片段不可引入隐式副作用；展开后必须可 `wf explain` 完整还原。

### 17.6 P1-2 顺序/乱序不变性契约测试

- 目标：保证时间语义稳定，避免“同数据不同到达顺序”导致结果漂移。
- 建议：扩展 `contract` 的 `options/expect`，增加重排测试模式。

```wfl
contract dns_order_invariance for dns_no_response {
  given {
    row(req, query_id="q-1", sip="10.0.0.8", domain="evil.test", event_time="2026-02-17T10:00:00Z");
    row(resp, query_id="q-1", sip="10.0.0.8", event_time="2026-02-17T10:00:01Z");
    tick(31s);
  }
  options {
    permutation = "shuffle";  // future: none | shuffle
    runs = 20;
  }
  expect {
    stable_hits == true;
    stable_score == true;
  }
}
```

- 验收：`wf test` 输出必须包含“重排轮次/失败样本/最小重放输入”。

### 17.7 建议落地顺序

1. **先做 P0-2 + P0-3**：先把语义一致性与资源安全兜住。
2. **再做 P0-1 + P0-4**：增强可表达性与下游契约治理。
3. **最后做 P1-1 + P1-2**：提升复用能力与回归防线。

---

## 18. 测试数据生成工具方案（wf-datagen）

> 目标：为 WFL 规则提供"可复现、可注入时序故障、可自动对拍"的测试数据与期望结果（oracle），将正确性校验前置到 CI。

### 18.1 设计目标

- **可复现**：同一 `seed + ws + wfl + scenario` 产出一致数据集。
- **可验证**：生成 `events` 与 `oracle`，支持自动差异比对。
- **可扰动**：支持乱序、迟到、重复、丢弃等时序扰动。
- **可接入**：支持 `gen -> run -> verify` 标准流水线接入 CI。

### 18.2 Scenario DSL（`.wfg`）

#### 18.2.1 设计原则

- **语义即语法**：每个语法块（`stream`、`inject`、`faults`、`oracle`）直接对应一个生成概念，不经中间数据格式间接表达。
- **与 WFL 同族**：复用 WFL 词法基础（IDENT、STRING、NUMBER、DURATION）和注释风格（`//`），降低学习成本。
- **声明头承载核心信息**：`stream alias: window rate`、`inject for rule on [streams]` 等关键语义在声明行即可读取，块体仅承载覆盖项与参数。
- **引用不重复声明**：字段类型以 `.wfs` 为准，规则语义以 `.wfl` 为准，`.wfg` 仅覆盖生成策略。

文件扩展名：`.wfg`（WarpFusion SCenario）。

#### 18.2.2 完整示例

```wfg
use "windows/security.wfs"
use "rules/brute_force.wfl"

scenario brute_force_load seed 42 {

  time "2026-02-18T00:00:00Z" duration 30m
  total 200000

  // ── 基础流量 ──

  stream fail: auth_events 200/s {
    sip    = ipv4(pool: 500)
    uid    = pattern("user-{seq:0000}")
    action = "failed"
  }

  stream success: auth_events 400/s {
    sip    = ipv4(pool: 500)
    uid    = pattern("user-{seq:0000}")
    action = "success"
  }

  // ── 模式注入 ──

  inject for brute_force on [fail] {
    hit       5%  count_per_entity=5 within=2m;
    near_miss 3%  count_per_entity=2 within=2m;
  }

  // ── 时序扰动 ──

  faults {
    out_of_order 2%;    // 交换相邻事件的发送顺序（事件时间不变，到达顺序乱）
    late         1%;    // 事件发送时间推迟到 watermark 之后（可能被引擎 drop）
    duplicate    0.5%;  // 重复发送同一事件
    drop         0.2%;  // 生成但不发送（oracle 中仍标记为已生成）
  }

  // ── 期望结果 ──

  oracle {
    time_tolerance  = 1s;
    score_tolerance = 0.01;
  }
}
```

#### 18.2.3 文法（EBNF）

> 词法基础（IDENT、NUMBER、STRING、DURATION、ALPHA、DIGIT）与 WFL §7 一致，此处不重复。新增 `PERCENT` 和 `RATE` 词法。

```ebnf
scenario_file   = { use_decl } , scenario_decl ;

use_decl        = "use" , STRING ;

scenario_decl   = "scenario" , IDENT , "seed" , NUMBER , "{" ,
                    time_clause ,
                    total_clause ,
                    { stream_block } ,
                    { inject_block } ,
                    [ faults_block ] ,
                    [ oracle_block ] ,
                  "}" ;

time_clause     = "time" , STRING , "duration" , DURATION ;
total_clause    = "total" , NUMBER ;

(* ── 流定义 ── *)
stream_block    = "stream" , IDENT , ":" , IDENT , RATE ,
                  [ "{" , { field_override } , "}" ] ;
field_override  = field_name , "=" , gen_expr ;
field_name      = IDENT | quoted_ident ;
quoted_ident    = "`" , { ANY - "`" } , "`" ;
gen_expr        = literal | gen_func ;
literal         = STRING | NUMBER | "true" | "false" ;
gen_func        = IDENT , "(" , [ named_arg , { "," , named_arg } ] , ")" ;
named_arg       = IDENT , ":" , ( STRING | NUMBER ) ;

(* ── 模式注入 ── *)
inject_block    = "inject" , "for" , IDENT , "on" , stream_list , "{" ,
                    inject_line , { inject_line } ,
                  "}" ;
stream_list     = "[" , IDENT , { "," , IDENT } , "]" ;
inject_line     = mode_kw , PERCENT , { param_assign } , ";" ;
mode_kw         = "hit" | "near_miss" | "non_hit" ;
param_assign    = IDENT , "=" , ( NUMBER | DURATION | STRING ) ;

(* ── 时序扰动 ── *)
faults_block    = "faults" , "{" , { fault_line } , "}" ;
fault_line      = IDENT , PERCENT , ";" ;

(* ── 期望结果配置 ── *)
oracle_block    = "oracle" , "{" , { param_assign , ";" } , "}" ;

(* ── 新增词法 ── *)
RATE            = NUMBER , "/" , ( "s" | "m" | "h" ) ;
PERCENT         = NUMBER , "%" ;
```

#### 18.2.4 语义约束

**引用校验：**

| ID | 规则 |
|----|------|
| SC1 | `use` 引用的 `.wfs` / `.wfl` 文件必须存在且可解析；文件类型由扩展名确定。 |
| SC2 | `stream` 的 alias 必须在所引用 `.wfl` 的某条规则 `events {}` 中声明。 |
| SC2a | `stream` 的 window 名（`:`后）必须与目标规则中该 alias 绑定的 window 一致。即 `.wfg` 中 `stream fail: auth_events` 要求 `.wfl` 中有 `fail: auth_events ...`；window 名不匹配则编译错误。 |
| SC3 | `stream` 的 window 名必须在 `.wfs` 中定义。 |
| SC4 | `field_override` 的字段名必须存在于该 stream 对应 window 的 `fields {}` 中。 |
| SC5 | `inject for <rule>` 的 `<rule>` 必须存在于所引用的 `.wfl` 中。 |
| SC6 | `inject on [streams]` 中的每个 alias 必须是该 `<rule>` 的 events alias 子集。 |
| SC7 | 未被 `inject` 覆盖的规则仅接收 `stream` 基础流量；oracle 中不为这些规则生成期望命中记录。 |

**值域校验：**

| ID | 规则 |
|----|------|
| SV1 | `seed` 必须为非负整数。 |
| SV2 | `total` 必须为正整数。 |
| SV3 | `RATE` 的数值部分必须 > 0。 |
| SV4 | `PERCENT` 的数值部分必须在 `(0, 100]` 之间。 |
| SV5 | 同一 `inject` 块内各 `mode_kw` 行的 `PERCENT` 之和不得超过 100%。 |
| SV6 | `faults` 中各项 `PERCENT` 之和不得超过 100%（一条事件最多命中一种 fault）。 |
| SV7 | `gen_expr` 的类型必须与 `.wfs` 中对应字段类型兼容（`const "failed"` 赋给 `chars` 字段合法，赋给 `digit` 字段编译错误）。 |
| SV8 | `oracle.time_tolerance` 必须为 DURATION 类型；`oracle.score_tolerance` 必须为 NUMBER 类型且 >= 0。 |
| SV9 | 目标规则 `events` 中的 filter 条件（`&& expr`）隐含字段值约束。生成器从 filter 中提取常量等值条件（如 `action == "failed"`），作为该 stream 对应字段的**隐式 const override**。若 `.wfg` 中已显式声明同字段的 `field_override`，则以显式声明为准；未声明时自动应用 filter 中的常量值。非常量条件（如 `x > 10`）不自动提取，需用户显式覆盖。 |

#### 18.2.5 `stream` 块详解

声明行 `stream alias: window rate` 承载三个核心属性：
- **alias**：对应 `.wfl` 规则中 `events {}` 的别名。
- **window**：对应 `.wfs` 中的 window 名，决定字段 schema。
- **rate**：基础事件生成速率（如 `200/s`、`12000/m`、`720000/h`）。

块体 `{ field_override ... }` 为可选；省略时所有字段按 `.wfs` 类型自动随机生成。此外，生成器会自动从目标规则的 `events` filter 中提取常量等值条件作为隐式覆盖（SV9），确保生成的数据能通过 filter 进入规则求值链路。

**gen 函数清单：**

| gen 函数 | 参数 | 适用字段类型 | 说明 |
|----------|------|:------------:|------|
| *(常量)* | 直接写字面值 | 全部 | `action = "failed"` |
| `ipv4` | `pool: N` | `ip` / `chars` | 从随机池取 IPv4，`pool` 控制 key 基数 |
| `ipv6` | `pool: N` | `ip` / `chars` | 同上，IPv6 |
| `pattern` | `format: STRING` | `chars` | 模板生成：`{seq:0000}` 递增，`{rand:N}` 随机 N 位 |
| `enum` | `values: STRING` | `chars` | 从逗号分隔列表均匀抽取，如 `enum(values: "a,b,c")` |
| `range` | `min: N, max: N` | `digit` / `float` | 均匀随机 |
| `timestamp` | *(无参)* | `time` | 默认：按 stream rate 在 `time` 范围内递增 |

未声明 override 的字段按类型默认策略生成：

| 字段类型 | 默认策略 |
|----------|----------|
| `chars` | 随机 8~16 字符字母数字串 |
| `digit` | `range(min: 0, max: 100000)` |
| `float` | `range(min: 0.0, max: 1000.0)` |
| `bool` | 50/50 随机 |
| `time` | 按 rate 递增 |
| `ip` | `ipv4(pool: 1000)` |
| `hex` | 随机 32 字符十六进制串 |

#### 18.2.6 `inject` 块详解

声明行 `inject for <rule> on [<streams>]` 指定目标规则和注入流。块体内每行声明一种模式：

```
mode_kw  percent  param=value ... ;
```

**模式语义：**

| 模式 | 语义 | oracle 中的期望 |
|------|------|----------------|
| `hit` | 按目标规则构造"应命中"事件序列 | 必须出现对应命中记录 |
| `near_miss` | 构造"接近命中但不命中"事件序列 | 不应出现命中记录 |
| `non_hit` | 构造"与规则无关或同实体但显式不满足条件"的事件 | 不应出现命中记录 |

**`near_miss` 细化（按规则结构）：**

| 规则结构 | near_miss 默认行为 | 可覆盖参数 |
|----------|--------------------|-----------|
| 单步阈值 `count >= N` | 生成 `N-1` 条（差一条不命中） | `count_per_entity` |
| 多步序列（A then B） | 只完成第一步，不产生第二步事件 | `steps_completed` |
| 时间窗口 `match<:dur>` | 事件分散在窗口内但数量不足 | `within` |

**`params` 约定：**
- `params` 的可用字段取决于 `target_rule` 的规则结构。
- 生成器从规则的 `match` 子句中提取阈值和窗口参数作为基准。
- `inject` 行中的 `param=value` 允许覆盖这些基准值；未覆盖的参数使用规则中的原始值。

#### 18.2.7 `faults` 块详解

`faults` 控制事件的时序扰动，作用于生成后的全局事件流。

| 扰动 | 语义 | 互斥性 |
|------|------|--------|
| `out_of_order` | 交换相邻事件的发送顺序（事件时间不变，到达顺序乱） | 一条事件最多命中一种 fault |
| `late` | 将事件的发送时间推迟到 watermark 之后（可能被引擎 drop） | 同上 |
| `duplicate` | 复制一份事件再发一次 | 同上 |
| `drop` | 生成但不发送（oracle 中仍标记为已生成） | 同上 |

处理顺序：生成器先生成完整事件流，再按各项比例随机标记 fault 类型（互斥），最后按 fault 类型变换发送行为。

#### 18.2.8 `oracle` 块详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `time_tolerance` | DURATION | `1s` | verify 阶段 emit_time 比对容差 |
| `score_tolerance` | NUMBER | `0.01` | verify 阶段 score 比对容差 |

省略 `oracle` 块时等价于不生成 oracle（P0 行为）。存在 `oracle` 块即表示启用 oracle 生成。

开关策略统一为“语法优先”：
- 默认行为由 `.wfg` 决定：有 `oracle` 块则生成 oracle，无 `oracle` 块则不生成。
- `gen` 子命令不再提供 `--oracle` 正向开关，仅保留 `--no-oracle` 作为一次性覆盖（即使存在 `oracle` 块也强制不生成）。

### 18.3 输出契约

- `<out>/<scenario>.jsonl|arrow`：生成事件流（`--format` 指定，默认 `jsonl`）。
- `<out>/<scenario>.oracle.jsonl`：期望告警（**仅当 `.wfg` 中存在 `oracle` 块时生成**；省略 `oracle` 块则不产出此文件）。
- `<out>/<scenario>.oracle.meta.json`：oracle 容差参数（与 oracle.jsonl 同时生成）。
- `<out>/<scenario>.faulted-oracle.jsonl`：扰动后期望告警（仅当存在 `faults` 块且 oracle 启用时生成）。

`manifest.json` 尚未实现（reserved for future）；当前通过 stdout 输出统计信息：

```
Generated 200000 events -> out/brute_force_load.jsonl
Oracle: 1234 alerts -> out/brute_force_load.oracle.jsonl
```

### 18.4 Oracle 生成策略

| 阶段 | Oracle 来源 | 说明 |
|------|-------------|------|
| P0 | 不生成（`.wfg` 中无 `oracle` 块） | 仅做数据可复现与吞吐链路验证 |
| P1 | rule-aware + 独立求值器（Reference Evaluator） | 用规则语义计算期望命中，避免"生成逻辑=验证逻辑"同源偏差 |
| P2 | 在 P1 基础上叠加时序扰动求值 | 生成延迟/乱序下的期望结果 |

**Reference Evaluator 定位：**
- 复用 `.wfl` compiler 的 RulePlan 输出，但运行在单线程、无时序扰动的理想环境中（事件严格按事件时间排序、无迟到/乱序）。
- 与生产 MatchEngine 共享 RulePlan 结构，但执行路径独立（无 watermark/背压），因此可检出生产引擎在时序处理中引入的偏差。

**模式与 Oracle 的对应关系：**

| 模式 | Oracle 期望 |
|------|-------------|
| `hit` | 必须出现对应命中记录 |
| `near_miss` | 不应命中 |
| `non_hit` | 不应命中 |

### 18.5 Verify 匹配规则

- `wf-datagen verify` 对比 `actual_alerts` 与 `oracle`，输出 `verify_report.json` 与 `verify_report.md`。
- 差异分类：`missing` / `unexpected` / `field_mismatch`。

匹配规则：

| 项 | 规则 |
|----|------|
| 匹配键 | `(rule_name, entity_type, entity_id, close_reason)`（均为 yield 系统字段） |
| 时间配对 | 同一匹配键下按 `abs(actual.emit_time - oracle.emit_time)` 最小贪心配对 |
| 时间容差 | 配对后 `abs(actual.emit_time - oracle.emit_time) <= time_tolerance`，超出则计入 `field_mismatch`（取 `.wfg` 中 `oracle.time_tolerance`，默认 `1s`） |
| 分数容差 | `abs(actual.score - oracle.score) <= score_tolerance`（取 `.wfg` 中 `oracle.score_tolerance`，默认 `0.01`） |
| 排序要求 | 无序比较（order-insensitive） |
| 多对多 | 同一匹配键下按"时间差最小、分差最小"贪心配对；未配对项分别计入 `missing/unexpected` |

最小差异报告结构：

```json
{
  "status": "fail",
  "summary": {
    "oracle_total": 1234,
    "actual_total": 1231,
    "missing": 14,
    "unexpected": 11,
    "field_mismatch": 9
  }
}
```

### 18.6 端到端数据流（gen -> run -> verify）

```text
*.wfg + *.wfs + *.wfl
         │
    wf-datagen gen
         │
   ┌─────┴─────────────┐
   │                    │
out/events/*    out/oracle/alerts.jsonl
   │                    │
wf run --replay         │
   │                    │
actual_alerts.jsonl     │
   │                    │
   └──────┬─────────────┘
          │
   wf-datagen verify
          │
verify_report.json/.md
```

说明：
- 回放默认走 `wf run --replay` 文件输入路径（绕过 TCP）。
- 若需覆盖 Receiver/TCP 链路，可启用 `wf-replay-sender --tcp` 模式发送 length-prefixed 帧。

### 18.7 CLI 约定

`gen` 子命令遵循 `.wfg` 的 `oracle` 块；如需临时关闭 oracle 生成，使用 `--no-oracle`。

```bash
# 生成
wf-datagen gen \
  --scenario tests/brute_force_load.wfg \
  --format jsonl \
  --out out/

# 一致性校验（检查 .wfg 引用与 .wfs/.wfl 的一致性）
wf-datagen lint tests/brute_force_load.wfg

# 对拍验证
wf-datagen verify \
  --actual out/actual_alerts.jsonl \
  --expected out/brute_force_load.oracle.jsonl \
  --meta out/brute_force_load.oracle.meta.json

# 覆盖 ws/wfl 引用（调试用途）
wf-datagen gen \
  --scenario tests/brute_force_load.wfg \
  --ws windows/security.wfs \
  --wfl rules/brute_force.wfl \
  --out out/

# 临时关闭 oracle（即使 .wfg 中存在 oracle 块）
wf-datagen gen \
  --scenario tests/brute_force_load.wfg \
  --no-oracle \
  --out out/
```

### 18.8 CI 接入标准流程

1. `wf-datagen gen` 生成 events；当 `.wfg` 存在 `oracle` 块且未指定 `--no-oracle` 时，同时生成 oracle。
2. `wf run --replay out/events/*` 产出 `actual_alerts.jsonl`（可切换 TCP 回放模式）。
3. `wf-datagen verify` 输出差异报告。
4. CI 阻断条件（默认）：`missing == 0 && unexpected == 0 && field_mismatch == 0`。

### 18.9 与 `contract` 的关系

| 维度 | `contract`（§12.11） | `scenario`（`.wfg`） |
|------|---------------------|---------------------|
| 用途 | 单规则小样本精确断言 | 多规则大规模统计验证 |
| 数据来源 | 手写 `row()`，逐条可控 | 生成器按分布自动产出 |
| 期望结果 | 手写 `expect { hits; hit[i].score; ... }` | Reference Evaluator 自动生成 oracle |
| 扰动 | 无（理想序列） | `faults` 支持乱序/迟到/重复/丢弃 |
| 定位 | **单元测试**：验证单条规则的逻辑正确性 | **集成测试**：验证引擎在接近生产负载下的端到端正确性 |
| CI 阶段 | PR 必跑（秒级） | 定时/Release 跑（分钟级） |

两者互补：`contract` 先保证规则逻辑正确，`scenario` 再保证引擎处理正确。

### 18.10 分阶段落地

| 阶段 | 功能 | 依赖 |
|------|------|------|
| P0 | `.wfg` parser + schema 驱动随机生成 + seed 可复现 + JSONL/Arrow 输出 | `.wfs` parser |
| P1 | rule-aware（hit/near_miss/non_hit）+ Reference Evaluator + oracle 生成 + verify | `.wfl` compiler |
| P2 | 时序扰动矩阵 + 压测模式 + PR 友好差异报告（md） | P1 |
