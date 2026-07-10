# 语言参考

本页以当前代码实现为准，主要对应 `wp-reactor/crates/wf-lang` 的 parser / checker / compiler 行为。

如果某项能力仍在设计、部分实现或存在语义偏差，不在这里展开说明；请转到 `docs/design/wfl-design.md` 查看状态标签和后续规划。

## Window Schema (`.wfs`)

Window 是 WFL 的数据抽象层，定义事件流的逻辑结构。

基本语法：

```wfs
window <名称> {
    stream = <数据流名>
    time = <时间字段>
    over = <保留时长>

    fields {
        <字段名>: <类型>
    }
}
```

字段类型：

| WFL 类型 | 底层映射 |
|----------|----------|
| `chars` | Utf8 |
| `digit` | Int64 |
| `float` | Float64 |
| `bool` | Boolean |
| `time` | Timestamp(Nanosecond) |
| `ip` | Utf8 |
| `hex` | Utf8 |
| `array` | Utf8（结构化数组，sink 决定最终编码） |
| `array/T` | List(T) |
| `object` | Utf8（结构化对象，sink 决定最终编码） |

属性说明：

- `stream`：数据流绑定；可省略，省略时该 window 只作为输出目标
- `time`：事件时间字段；`over > 0` 时必填
- `over`：保留时长；`0` 表示静态集合

带点字段名示例：

```wfs
window endpoint_events {
    stream = "endpoint"
    time = event_time
    over = 10m

    fields {
        host_id: chars
        event_time: time
        `detail.sha256`: hex
        `detail.process`: chars
    }
}
```

在 `.wfl` 中引用时使用下标形式：`alias["detail.sha256"]`。

## 检测规则 (`.wfl`)

一个 `.wfl` 文件当前可包含这几类顶层块：

- `use "schema.wfs"`
- `pattern ... { ... }`
- `rule ... { ... }`
- `test ... for ... { ... }`

最常见的规则结构：

```wfl
use "schema.wfs"

rule <规则名> {
    meta { ... }
    events { ... }

    match<key:duration> {
        on event { ... }
        on close { ... }
    } -> score(expr)

    entity(type, id)
    yield target@v1 (...)

    conv { ... }     // 可选
    limits { ... }   // 可选
}
```

也可以使用逐条无状态触发：

```wfl
rule <规则名> {
    events { ... }
    on each <alias> [where <expr>] -> score(expr)
    entity(type, id)
    yield target (...)
}
```

还支持多级管道：

```wfl
rule <规则名> {
    events { ... }

    match<...> { ... }
    |> match<...> { ... } -> score(expr)

    entity(type, id)
    yield target (...)
}
```

说明：

- `meta` 目前是字符串键值对
- `entity` 与 `yield` 仍然是规则必需部分
- `|>` 管道中，只有最终 stage 可以带 `-> score(...)`
- 当前 checker 还不支持 `on each` 与 pipeline stages 组合

### `pattern`

当前代码支持顶层 `pattern` 声明，以及在规则中调用它。

```wfl
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule brute_force {
    events { e : auth_events }
    burst(e, sip, 5m, 5)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
```

说明：

- `pattern` 位于顶层，与 `rule`、`test` 同级
- pattern body 当前承载的是一段 `match ... -> score(...)`
- 在规则中调用时会做参数替换，再按普通 `match` + `score` 解析
- 参数个数不匹配会直接报错

### `events`

```wfl
events {
    fail : auth_events && action == "failed"
    scan : fw_events
}
```

- 别名必须唯一
- window 必须在已导入 `.wfs` 中定义
- 过滤表达式支持比较、逻辑运算、`in` / `not in`

状态枚举这类条件，优先写成：

```wfl
events {
    bad : app_events && lower(status) in ("error", "failed", "failure", "timeout", "fatal", "panic", "abort")
}
```

不推荐展开成很长的 `a == x || a == y || ...`。

### `match`

```wfl
match<sip:5m> {
    on event {
        fail | count >= 3;
        scan.dport | distinct | count > 10;
    }
}
```

说明：

- key 可为空、单 key、复合 key
- 支持滑动窗口、固定窗口、会话窗口
- key 支持点字段和下标字段，例如 `match<e["detail.sha256"]:5m>`
- 多步是顺序关系，前一步命中后才进入后一步

固定窗口示例：

```wfl
match<sip:5m:fixed> {
    on event {
        fail | count >= 3;
    }
}
```

会话窗口示例：

```wfl
match<uid:session(30m)> {
    on event {
        e | count >= 1;
    }
    on close {
        e | count >= 1;
    }
}
```

显式 key 映射：

```wfl
match<:5m> {
    key {
        user_id = a.uid;
        user_id = b.user_name;
    }
    on event {
        a | count >= 1;
    }
}
```

### `on each`

```wfl
on each e where e.action == "failed" -> score(70.0)
```

说明：

- `on each` 与 `match` 互斥
- `e` 必须来自 `events`
- `where` 在单条记录上下文中求值
- 不创建 key / window instance
- 不支持 `on close`
- 不支持 `close_reason`
- 适合上游 enrichment 和逐条风险打分
- 当前 checker 不支持 `on each` 与 pipeline stages 组合
- 如果上游已有 OML/投影层，纯逐条语义映射优先放 OML，WFL 保留窗口聚合与告警逻辑

典型写法：

```wfl
rule enrich_each_event {
    events {
        e : auth_events
    }

    on each e -> score(if e.action == "failed" then 70.0 else 10.0)

    entity(ip, e.sip)

    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip
    )
}
```

### `on close`

用于缺失检测或 close 阶段判断：

```wfl
match<query_id:30s> {
    on event {
        req | count >= 1;
    }
    on close {
        resp && close_reason == "timeout" | count == 0;
    }
}
```

`close_reason` 可取：

- `"timeout"`
- `"flush"`
- `"eos"`

除了 `on close`，当前还支持：

```wfl
and close {
    resp && close_reason == "timeout" | count == 0;
}
```

说明：

- `on close` 表示 close 路径独立触发
- `and close` 表示 close 条件与 event 路径共同参与命中
- 两者在 AST 中都会进入 close block，只是 mode 不同

### `score`

```wfl
} -> score(70.0)
```

也可使用表达式：

```wfl
} -> score(if count(fail) > 10 then 90.0 else 70.0)
```

说明：

- 当前 parser / AST 对齐的是 `score(expr)`
- `expr` 支持数值、函数调用、`if ... then ... else ...`
- 多因子 `score { item = expr @ weight; ... }` 目前不在 `wf-lang` AST 里

### `entity`

```wfl
entity(ip, fail.sip)
entity(user, login.uid)
entity(host, e.host_id)
```

### `join`

支持 `snapshot` / `asof` / `asof within`：

```wfl
join geo_lookup snapshot on sip == geo_lookup.ip
join conn_risk asof within 24h on sip == conn_risk.ip
```

- `snapshot`：取右表当前快照
- `asof`：按事件时间回看最近一条 `ts <= event_time`
- `asof within`：在指定时间范围内回看
- 支持多条件：`join t snapshot on sip == t.ip && dport == t.port`

### `|>` pipeline

当前代码支持多级管道：

```wfl
rule r_pipe {
    events { d : fw_events }

    match<sip,dport:5m> {
        on event { d | count >= 1; }
        on close { d | count >= 3; }
    }
    |> match<sip:10m> {
        on event { _in | count >= 1; }
        on close { _in | count >= 10; }
    } -> score(80.0)

    entity(ip, _in.sip)
    yield out (x = _in.sip)
}
```

说明：

- 中间 stage 不允许带 `-> score(...)`
- 最终 stage 必须带 `-> score(...)`
- 下游 stage 通过 `_in` 读取上一 stage 输出

### `yield`

```wfl
yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected, risk={}", fail.sip, @score),
    risk_score = round(@score, 1)
)
```

也支持版本标签：

```wfl
yield security_alerts@v2 (
    sip = fail.sip
)
```

`@score` 表示“当前规则已经计算出的最终 score 值”。

- 只允许出现在 `yield (...)` 表达式里
- 在 `yield` 中可像普通数值一样参与任意表达式，例如 `round(@score, 1)`、`concat("risk=", @score)`
- 适合把规则 score 映射成业务字段，例如 `risk_score = @score`
- 它引用的是当前规则的 score，不是上游中间记录里的 `__wfu_score`
- 如果写了 `@vN`，checker 会校验它与 `meta { contract_version = "N" }` 一致

最终 alert 记录会自动注入：

- `rule_name`
- `emit_time`
- `score`
- `entity_type`
- `entity_id`
- `close_reason`

如果 `yield` 目标是给下游继续消费的中间 window，则按中间 enriched 记录约定透传以 `__wfu_` 为前缀的系统字段。推荐依赖：

- `__wfu_score`
- `__wfu_rule_name`
- `__wfu_entity_type`
- `__wfu_entity_id`

这几个字段对下游规则可直接引用；当某个 window 被识别为中间消费目标时，编译器会自动把它们视为该 window 的可用字段，不需要在 `.wfs` 里重复声明。

中间记录默认不暴露时间类 `__wfu_*` 字段；若目标 window 定义了 `time` 列，runtime 会在 `yield (...)` 未显式赋值时自动继承输入事件时间到该列。若你需要把时间作为普通字段继续使用，应显式写进 `yield (...)`。

`yield` 里也不能手工写 `__wfu_*` 字段名；这个前缀保留给运行时中间系统字段。

若某个 `yield` 目标会被下游规则继续消费，则所有这类中间 window 必须构成无环依赖图；禁止自回写和 `A -> B -> A` 形式的循环。

#### 结构化对象 / 数组输出

当规则需要输出 `risk_context`、`extensions` 这类结构化字段时，可以在 WFS 中使用结构化类型：

```wfs
window security_alerts {
    time = emit_time
    over = 48h

    fields {
        sip: ip
        risk_context: object
        extensions: object
        tags: array
        scores: array/float
        emit_time: time
    }
}
```

WFL 中采用与 OML 一致的对象块风格：`object { ... }` 使用字段赋值语句，字段之间用 `;` 分隔；数组字面量使用 `array [ ... ]`，与对象块保持结构化字面量的一致性。

```wfl
yield security_alerts (
    sip = e.sip,
    risk_context = object {
        score = @score;
        source = e.source;
        tags = array ["bruteforce", "ssh", e.action];
        geo = object {
            country = e.country;
            city = e.city;
        };
    }
)
```

语法：

```ebnf
object_expr     = "object", "{", object_item, { object_item }, "}" ;
object_item     = object_targets, "=", expr, [ ";" ] ;
object_targets  = ident, { ",", ident }, [ ":", field_type ] ;

array_expr      = "array", "[", [ expr, { ",", expr }, [ "," ] ], "]" ;
```

对象项右侧在 WFL 中应允许完整表达式，而不是只允许常量或字段读取；这样 `@score`、字段引用、函数调用、`if ... then ... else ...` 都可用于构造对象。

```wfl
risk_context = object {
    score : float = round(@score, 1);
    severity : chars = if @score >= 80.0 then "high" else "medium";
}
```

语义边界：

- `object` / `array` 是 WFL 的结构化值类型，不等同于 JSON。
- JSON、XML、文本、CSV 等最终编码由 sink 决定。
- JSON sink 应把 `object` 输出为 JSON object，把 `array` 输出为 JSON array。
- XML sink 可把 `object` 输出为元素树；文本/CSV sink 可选择序列化或 flatten。
- `json_object(...)` 不作为主语法；如后续提供，也应只是返回 `object` 的便捷函数。

### `conv`

当前代码支持 `conv` 作为 post-close 结果集变换：

```wfl
conv { sort(-score) | top(10) ; }
conv { sort(-score) ; where(count > 5) ; }
```

支持的操作：

- `sort(...)`
- `top(n)`
- `dedup(expr)`
- `where(expr)`

说明：

- `conv` 位于 `yield` 之后、`limits` 之前
- checker 当前要求 `conv` 只能用于 fixed window：`match<...:fixed>`

### `limits`

```wfl
limits {
    max_memory = "50MB";
    max_instances = 10000;
    max_throttle = "100/min";
    on_exceed = throttle;
}
```

`on_exceed` 可选：

- `throttle`
- `drop_oldest`
- `fail_rule`

## 表达式与函数

运算符优先级，从高到低：

1. 一元 `-`
2. `*` `/` `%`
3. `+` `-`
4. `==` `!=` `<` `>` `<=` `>=` `in` `not in`
5. `&&`
6. `||`

表达式能力：

- 比较与布尔运算：`== != < > <= >= && ||`
- 集合判断：`in` / `not in`
- 条件表达式：`if cond then a else b`
- 普通函数调用：`fmt(...)`、`contains(...)`
- 方法调用：`window.has(...)`
- 字段访问：`e.sip`、`e["detail.sha256"]`

### 常用聚合 /集合函数

| 函数 | 说明 |
|------|------|
| `count(alias)` | 事件计数 |
| `sum(alias.field)` | 求和 |
| `avg(alias.field)` | 平均值 |
| `min(alias.field)` | 最小值 |
| `max(alias.field)` | 最大值 |
| `distinct(alias.field)` | 去重计数 |

示例：

```wfl
fail | count >= 3;
scan.dport | distinct | count > 10;
e.bytes | sum >= 10000;
```

这些聚合表达式可以直接引用 `events { ... }` 里声明的 alias。
包括带过滤条件、但没有出现在 `on event` / `and close` step source 里的 alias，例如 `count(hi)`、`avg(elevated.risk_score)`。

### 常用普通函数

```wfl
fmt("{} failed {} times from {}", fail.username, count(fail), fail.sip)
```

当前代码里已接入并有 checker 支持的常见函数包括：

- 字符串：`contains`、`regex_match`、`startswith`、`endswith`、`lower`、`upper`、`len`、`concat`
- 数值：`round`、`abs`、`ceil`、`floor`、`sqrt`、`pow`、`log`、`exp`、`clamp`
- 空值 / 空白处理：`coalesce`、`isnull`、`isnotnull`、`is_blank`、`null_if_blank`、`default_if_blank`
- 时间：`time_diff`、`time_bucket`
- 当前引擎时间：`now`、`now_s`、`now_ms`、`now_us`、`now_ns`
- 哈希 / 编码：`md5`、`sha1`、`sha256`、`hex`、`stable_id`
- 画像 / 回看：`baseline`
- 方法调用：`window.has(...)`

示例：

```wfl
events {
    ps : endpoint_events && contains(lower(cmd), "powershell")
}
```

结合 `in` 可简化多值匹配：

```wfl
events {
    bad : endpoint_events && lower(status) in ("error", "failed", "failure")
}
```

#### 空值与空白字符串

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `coalesce(v1, v2, ...)` | 首个非空参数的类型 | 返回第一个非 null / 可求值的参数 |
| `isnull(expr)` | `bool` | 参数不可求值时返回 `true` |
| `isnotnull(expr)` | `bool` | 参数可求值时返回 `true` |
| `is_blank(text)` | `bool` | `text` 缺失、空字符串或全空白字符时返回 `true` |
| `null_if_blank(text)` | `chars` 或 null | `text` 为空白时返回 null，否则返回原字符串 |
| `default_if_blank(text, default)` | `chars` | `text` 缺失或为空白时返回 `default` |

示例：

```wfl
yield out (
    user = default_if_blank(e.user, "unknown"),
    src = coalesce(null_if_blank(e.src), e.sip)
)
```

`is_blank`、`null_if_blank`、`default_if_blank` 的参数必须是 `chars`。其中 `null_if_blank(...)` 常和 `coalesce(...)` 组合使用。

#### 当前引擎时间

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `now()` | `time` | 当前 UTC epoch nanoseconds；可直接传给 `strftime` |
| `now_s()` | `digit` | 当前 UTC epoch seconds |
| `now_ms()` | `digit` | 当前 UTC epoch milliseconds |
| `now_us()` | `digit` | 当前 UTC epoch microseconds |
| `now_ns()` | `digit` | 当前 UTC epoch nanoseconds |

示例：

```wfl
yield security_alerts (
    created_time = now(),
    created_ms = now_ms(),
    created_day = strftime(now(), "%Y-%m-%d")
)
```

说明：

- `now()` 表示规则输出时刻，不是事件发生时间。
- 同一条输出记录的多个 `yield` 字段里调用 `now_*`，会复用同一个内部时间戳。
- 当前运行时数值统一使用 `f64` 表示；`now_ns()` 可用于时间函数和比较，但不要把它当作可精确表达每一纳秒的整数序列号。

#### 时间格式化与解析

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `strftime(timestamp_nanos, format)` | `chars` | 按 chrono 格式字符串格式化 UTC 时间 |
| `strptime(text, format)` | `time` | 按格式解析时间，返回 epoch nanoseconds |
| `time_diff(t1, t2)` | `float` | 返回两个时间的秒级差值绝对值 |
| `time_bucket(t, interval_seconds)` | `time` | 将时间向下取整到指定秒级窗口 |

示例：

```wfl
yield out (
    day = strftime(e.event_time, "%Y-%m-%d"),
    age_seconds = time_diff(now(), e.event_time),
    bucket = time_bucket(e.event_time, 300)
)
```

#### 哈希、编码与稳定 ID

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `md5(text)` | `chars` | 返回小写十六进制 MD5 字符串 |
| `sha1(text)` | `chars` | 返回小写十六进制 SHA-1 字符串 |
| `sha256(text)` | `chars` | 返回小写十六进制 SHA-256 字符串 |
| `hex(text)` | `hex` | 返回字符串字节的小写十六进制编码 |
| `stable_id(prefix, value, ...)` | `chars` | 返回 `prefix` + SHA-256 前 16 位 |

示例：

```wfl
yield security_alerts (
    cmd_sha256 = sha256(e.cmdline),
    raw_hex = hex(e.raw),
    alert_id = stable_id("alert_", e.sip, e.user, e.event_time)
)
```

说明：

- `md5`、`sha1`、`sha256`、`hex` 的参数必须是 `chars`。
- `stable_id` 的第一个参数必须是 `chars` 前缀；后续参数必须是标量值，支持 `chars`、`digit`、`float`、`bool`、`time`、`ip`、`hex`。
- `stable_id` 对后续值使用带类型和长度的稳定编码后再计算 SHA-256，避免简单拼接导致的歧义。

#### 多值函数

| 函数 | 说明 |
|------|------|
| `mvcount(arr)` | 返回数组长度 |
| `mvjoin(arr, sep)` | 用分隔符拼接数组 |
| `mvindex(arr, index)` / `mvindex(arr, start, end)` | 取单个元素或范围 |
| `mvappend(v1, v2, ...)` | 合并标量或数组 |
| `split(text, sep)` | 将字符串拆成数组 |
| `mvdedup(arr)` | 数组去重，保留首次出现顺序 |
| `mvsort(arr)` | 数组排序 |
| `mvreverse(arr)` | 数组反转 |

## 规则测试

契约测试语法：

```wfl
test <测试名> for <规则名> {
    input {
        row(<别名>, <字段> = <值>, ...);
        tick(<时长>);
    }
    expect {
        hits == <数量>;
        hit[<索引>].score == <分数>;
        hit[<索引>].origin == <值>;
        hit[<索引>].entity_type == <值>;
        hit[<索引>].entity_id == <值>;
        hit[<索引>].field("<字段名>") == <值>;
    }
    options {
        close_trigger = timeout;
        eval_mode = strict;
        permutation = shuffle;
        runs = 8;
    }
}
```

示例：

```wfl
test brute_test for brute_force {
    input {
        row(fail, action = "failed", sip = "1.2.3.4");
        row(fail, action = "failed", sip = "1.2.3.4");
        row(fail, action = "failed", sip = "1.2.3.4");
        tick(6m);
    }
    expect {
        hits == 1;
        hit[0].score == 70.0;
        hit[0].entity_id == "1.2.3.4";
    }
    options {
        close_trigger = timeout;
    }
}
```

## 语义约束速查

Events 约束：

- 别名唯一
- window 必须存在
- 过滤字段必须存在于对应 window 中

Match 约束：

- `duration > 0`
- step 必须显式声明 source
- `match` 至少需要有效的事件/关闭路径才能通过后续语义检查
- `close_reason` 仅可在 `on close` 中引用
- `match` 与 `on each` 互斥
- `conv` 仅允许与 fixed window 搭配

On Each 约束：

- `alias` 必须来自 `events`
- `where` 必须返回 `bool`
- 不支持 `close_reason`
- 不支持集合函数和窗口状态函数
- 当前不支持与 pipeline stages 混用

Yield 约束：

- 目标 window 必须存在且 `stream` 为空
- 字段须为目标 window 的子集
- 禁止手工赋值系统字段
- 中间目标图必须无环
