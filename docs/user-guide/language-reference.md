# 语言参考

本页以当前代码实现为准，主要对应 `wp-reactor/crates/wf-lang` 的 parser / checker / compiler 行为。

如果某项能力仍在设计、部分实现或存在语义偏差，不在这里展开说明；请转到 `docs/design/wfl-design.md` 查看状态标签和后续规划。

## Window Schema (`.wfs`)

Window 是 WFL 的数据抽象层，定义事件流的逻辑结构。

基本语法：

```wfs
window <名称> {
    stream_tag = <逻辑流 tag>
    time = <时间字段>
    over = <保留时长>

    fields {
        <字段名>: <类型>
    }
}
```

字段类型：

| WFL 类型 | 使用场景 | 运行时存储 / 输出 |
|----------|----------|-------------------|
| `chars` | 输入、输出 | Utf8 |
| `digit` | 输入、输出 | Int64 |
| `float` | 输入、输出 | Float64 |
| `bool` | 输入、输出 | Boolean |
| `time` | 输入、输出 | Timestamp(Nanosecond) |
| `ip` | 输入、输出 | Utf8 |
| `hex` | 输入、输出 | Utf8 |
| `object` | 输入 stream、输出 / 中间 window | 结构化对象；中间 window 内按 UTF-8 JSON 桥接，最终 sink 决定编码 |
| `array` | 输入 stream、输出 / 中间 window | 结构化数组；中间 window 内按 UTF-8 JSON 桥接，最终 sink 决定编码 |
| `array/T` | 输入 stream、输出 / 中间 window | 结构化 typed array；元素按 `T` 做类型检查，最终 sink 决定编码 |

属性说明：

- `stream_tag`：数据流绑定；可省略，省略时该 window 只作为输出目标
- `time`：事件时间字段；`over > 0` 时必填
- `over`：保留时长；`0` 表示静态集合

输入 stream window（包含 `stream_tag = ...`）允许声明 `object` / `array` / `array/T` 字段，用于接收上游 Arrow Struct/List 等结构化列。provider window 暂不支持结构化字段。需要输出结构化对象或数组时，可直接透传输入结构化字段，也可在 `yield` 中用 WFL 的 `object { ... }` / `array [ ... ]` 构造。

带点字段名示例：

```wfs
window endpoint_events {
    stream_tag = "endpoint"
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
- 支持滑动窗口、固定窗口、会话窗口、HOP 滑动窗口
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

HOP 滑动窗口示例：

```wfl
match<auction:hop(10s, 2s)> {
    on event {
        b | count >= 1;
    }
    and close {
        n: b | count >= 1;
    }
}
```

- `hop(size, slide)`：滑动窗口，`size` 为窗口时长、`slide` 为推进步长，要求 `size % slide == 0`。
- 每个事件扇入 `size / slide` 个覆盖窗口（窗口按 epoch `slide` 边界对齐），各窗口独立累积。
- 窗口在 `w_start + size` 收口（天然落在 slide 边界）；`hop(size, size)` 等价 `fixed(size)`。
- 典型用途：滚动热点统计（NEXMark Q5 形态）——每 slide 更新一次最近 size 时长的 Top-N。

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

#### `on event seq` / `on event any` — 序列与共现

`on event` 的 `seq` / `any` 修饰符声明步骤的**排序模式**。裸 `on event { ... }` 等价 `seq`，向后兼容。

**`on event seq { ... }` — 有序序列（攻击链）**

步骤按书写顺序完成（step i+1 只在 step i 完成后评估），并支持步间约束：

```wfl
match<sip,dip:30m> {
    on event seq {
        has scan;                  // 存在性步骤：scan 事件至少一次（隐式 count >= 1）
        has login within 10m;      // login 必须在 scan 完成后的 10m 内
        not has failed within 5m;  // 否定步：scan 后 5m 内不得出现失败登录
        has xfer;                  // 总跨度由 match 窗口时长约束
    }
}
```

- `has <alias>` 存在性步骤，等价 `count >= 1`。
- 聚合步骤复用 pipe：`spray.user | distinct | count >= 5`。
- `within <dur>`：本步完成时刻 − 上一步完成时刻 ≤ dur。
- `not has <alias> within <dur>`：否定步，自上一完成步骤起 dur 内不得出现匹配事件。
- `consec`：严格相邻修饰符（默认允许步骤间夹带无关事件）。`skip = past_last | to_next`（to_next 延后 L3）。

**`on event any { ... }` — 无序共现**

所有 step 并行评估，全部满足即触发，顺序无关：

```wfl
match<sip,dip:10m> {
    on event any {
        scan | count >= 1;
        login | count >= 1;
        xfer | count >= 1;
    }
}
```

`login → scan → xfer` 的乱序序列也触发（弱相关性）。`any` 不支持 `within` / `not` / `consec` / `skip`
（依赖顺序，编译期拒绝）。

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

支持 `snapshot` / `asof` / `asof within` / `anti`：

```wfl
join geo_lookup snapshot on sip == geo_lookup.ip
join conn_risk asof within 24h on sip == conn_risk.ip
join blocked_list anti on sip == blocked_list.ip
```

- `snapshot`：取右表当前快照
- `asof`：按事件时间回看最近一条 `ts <= event_time`
- `asof within`：在指定时间范围内回看
- `anti`：排除式关联（白名单排除），仅保留右表无匹配的左记录
- 支持多条件：`join t snapshot on sip == t.ip && dport == t.port`

**join 后 `where` 过滤**：

```wfl
join person_events snapshot on a.seller == person_events.id
where person_events.state in ("OR", "ID", "CA")
```

- 在全部 join 富化完成后、alert 构建前求值；`false` / join miss 导致字段缺失（`None`）均抑制输出——对齐 SQL `INNER JOIN ... WHERE` 的丢行语义（Q3/Q20 形态）。
- 仅 `bool` 表达式合法；无 `join` 的规则不可用 `where`（编译期拒绝）。

**deferred join（`emit at`）**：

```wfl
rule q9_winning_bid {
    events { a : auction_events }
    on each a -> score(30.0)
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield nexmark_alerts (
        id = a.id,
        detail = fmt("winner {}", winner.bidder)
    )
}
```

- 驱动事件（`on each` 驱动）挂起，`emit at <expr>` 为到期时刻（事件时间 watermark 到达后评估）。
- `within [lo, hi]` 约束右表行的匹配时间区间；界可为驱动字段或函数（如 `bucket_end(p.dateTime, 10s)`，上开界写 `<bucket_end(...)`）。
- `reduce maxrow(field) tie(...)` 在匹配行集中选一（`minrow`/`top` 同族）；`as winner` 把胜者整行以裸键注入，`winner.field` 读取（Q9 形态）。
- 无匹配行不输出；`emit at` join 仅支持 `on each` 驱动形态（match 形态编译期拒绝）。
- 用途：auction 生命周期内胜出价（Q4 内层 / Q9）、窗口存在性 join（Q8 用 `emit at bucket_end(...)`）。

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

假设 `on event` 中定义了 `failed_hits: fail | count >= 3;`：

```wfl
yield security_alerts (
    sip = fail.sip,
    window_events = stat.count(window_event(fail)),
    fail_count = stat.count(match_event(failed_hits)),
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

#### `yield preset`

`yield preset` 用于复用公共输出字段集合，降低每条规则重复填写通用告警字段的成本。preset 可声明参数；参数按位置传入，带默认值的参数可省略。

```wfl
yield preset base_alerts <severity, source = "wfusion"> (
    rule_name = @__wfu_rule_name,
    score = @score,
    severity = $severity,
    source = $source
)

rule scan {
    ...
    yield scan_alerts : base_alerts<"high"> (
        alert_type = "scan",
        ioc_value = e.dip
    )
}
```

多个 preset 可按顺序组合：

```wfl
yield scan_alerts : base_alerts, ioc_fields (
    alert_type = "scan"
)
```

展开语义：

- 从左到右展开 preset
- 后面的 preset 覆盖前面的同名字段
- 当前 `yield (...)` 覆盖所有 preset 同名字段
- 展开后的字段仍按 `yield` 目标 window 做字段存在性、保留前缀和类型校验
- preset 不单独输出，也不绑定某个目标 window
- `$param` 只在 `yield preset` 声明内部表示 preset 参数；普通规则表达式中的 `$VAR` 仍按预处理变量处理
- 必填参数不能排在带默认值参数之后；缺少必填参数、实参数量过多、未知 `$param` 都是编译错误
- preset 中引用的事件 alias 在使用点解析；推荐 preset 优先放常量、`@score`、`@__wfu_*` 和时间系统变量

项目级公共 preset 可集中放入规则根目录下的 `_global.wfl`。规则根目录由 `runtime.rules` glob 的非通配前缀推导，例如 `rules/**/*.wfl` 对应 `rules/_global.wfl`，`rules/current/*.wfl` 对应 `rules/current/_global.wfl`。运行时会自动把它作为 project prelude 加载，并从普通规则文件列表中排除；`_global.wfl` 只允许 `yield preset` 声明，不会自动启用普通 `rule`（列表请走下面的 `use` 导入）。

#### 顶层列表 + `use` 导入

跨规则复用公共允许列表（issue #73）：一组 `in (...)` 右值只定义一次，多条规则以 `expr in <name>`（或 `expr not in <name>`）引用。列表声明是**顶层裸绑定**（无关键字、无可见性控制——WFL 规模小不做导出控制，use 导入的文件其全部顶层列表都可见）：

```wfl
// security_lists.wfl —— 列表定义文件（不含规则）
security_log_types = (
    "360_active_defense_log",
    "edr_alert_log",
    "fw_ips_protect_log"
)
high_risk_types = ("attack", "malware")
```

```wfl
// rules.wfl —— 规则文件 use 导入
use "security_lists.wfl"

rule alert_rule {
    events { s : sdm_event && s.log_type in security_log_types }
    ...
}

rule alert_entity_rule {
    events { s : sdm_event && s.log_type in security_log_types }
    ...
}
```

语义：

- 列表声明必须在规则之前（与 `yield preset` / `pattern` 同文法位置）；元素与手写 `in (...)` 列表同文法
- `use "file.wfl"` 是 **include 语义**：目标文件的全部顶层列表并入当前作用域（flatten、无限定名）；递归传播（A use B、C use A → C 也可见 B 的列表）；相对路径以被导入文件所在目录为基准
- `.wfs` 目标的 `use` 维持原样（schema 引用，由加载层另行加载，不在此导入）
- 编译期把引用展开为字面列表——元素类型检查、运行时求值与手写列表**逐字节等价**，不引入新语义
- **类型检查**：列表元素推断同类型（`in (...)` 字面列表与命名列表统一）；元素混合类型 → 报错；`expr in <list>` 左值类型与元素类型不兼容 → 报错（推断不出的元素如函数调用跳过，不误报）
- 错误面：引用未声明列表 / use 目标缺失 / 循环引用（A↔B）/ 重名（文件内与导入、导入与导入）→ 编译错误，可定位
- 支持 `not in <name>`；列表元素不支持嵌套引用其他列表
- `use` 只导入顶层列表——`pattern` 在解析阶段展开（技术上不可导入）、`yield preset` 走 `_global.wfl` prelude

#### 时间系统变量

这些变量用于把规则命中时间、证据时间和窗口时间输出为业务字段，避免依赖运行时内部的 `__wfu_*` 元数据字段。它们与 `@score` 一样，只允许在 `yield` 表达式中使用。

| 变量 | 类型 | 语义 |
|------|------|------|
| `@event_first_time` | `time` | 窗口实例**候选事件**跨度起点：进入该实例的第一条被接受事件的时间（fixed 窗口 ≠ 桶起点） |
| `@event_last_time` | `time` | 候选事件跨度终点：进入该实例的最后一条被接受事件的时间 |
| `@evidence_start_time` | `time` | 本次命中所用**证据**（被接受为命中依据的事件）跨度起点 |
| `@evidence_end_time` | `time` | 证据跨度终点（例如阈值规则下触发命中那条事件的时间） |
| `@first_match_time` | `time` | 实例首次完整命中（产生 match/close 结果）的引擎处理墙钟；accu 重复输出保持首次值 |
| `@window_start_time` | `time` | 规则窗口开始时间 |
| `@window_end_time` | `time` | 规则窗口结束时间 |
| `@emit_time` | `time` | 本次输出记录的稳定产出时间 |

时间语义区分（issue #82）：

- **候选事件跨度**（`@event_first_time` / `@event_last_time`）：窗口内进入该实例的全部被接受事件的首尾——适合 `first_seen` / `last_seen` 一类“该实体在窗口内何时开始/最后出现”的字段。
- **证据跨度**（`@evidence_start_time` / `@evidence_end_time`）：实际构成这次命中的事件跨度。对阈值规则，若窗口里还有更多事件尚未达到触发即到达（或 guard 拒绝），证据终点可能早于候选终点；两类规则一致时两组相等。
- `on event<accu>` 规则：分支证据状态跨 rearm 累积（`collect_set` 等证据逐条递增）→ 证据起点通常就是窗口首条证据事件，候选与证据随窗口共同推进。
- 乱序到达（事件时间回退）：候选 `first` 取到达序首条事件、`last` 取事件时间最大；证据 `start/end` 取分支记录的事件时间 min/max。
- **事件时间**来自输入事件字段，**处理墙钟**（`@first_match_time`）来自引擎处理时刻，不应混用。

推荐在输出 window 中显式声明业务字段：

```wfs
window security_alerts {
    over = 0

    fields {
        first_seen: time
        last_seen: time
        evidence_start_time: time
        evidence_end_time: time
        rule_window_start: time
        rule_window_end: time
        latest_analysis_time: time
    }
}
```

然后在 `yield` 中赋值：

```wfl
yield security_alerts (
    first_seen = @event_first_time,
    last_seen = @event_last_time,
    evidence_start_time = @evidence_start_time,
    evidence_end_time = @evidence_end_time,
    rule_window_start = @window_start_time,
    rule_window_end = @window_end_time,
    latest_analysis_time = @emit_time
)
```

约束：

- 这些变量只允许在 `yield` 表达式里使用。
- 这些变量在表达式里的数值表示为 epoch milliseconds；写入 `time` 字段时会按时间类型输出。
- `@emit_time` 在同一条输出记录内必须保持稳定，多次引用取同一个值。
- `@event_first_time` / `@event_last_time` 表达窗口内候选事件的首尾；`@evidence_start_time` / `@evidence_end_time` 表达本次命中的证据跨度；`@window_start_time` / `@window_end_time` 表达规则窗口边界，不应混用。

#### 稳定统计上下文

告警输出有时需要稳定的统计语义，例如进入窗口的候选事件数、命中事件数、distinct 后数量、阈值触发时的聚合值，以及 close/flush 输出时的最终聚合值。当前 API 采用 `stat.count(...)` / `stat.value(...)` 加统计选择器的形式：

```wfl
yield security_alerts (
    window_events = stat.count(window_event(auth)),
    matched_events = stat.count(match_event(port_scan)),
    distinct_ports = stat.count(match_distinct(port_scan)),
    trigger_count = stat.value(trigger(port_scan)),
    final_count = stat.value(final(final_ports))
)
```

统计选择器不是普通运行时函数，只能作为 `stat.count(...)` 或 `stat.value(...)` 的参数使用：

| 写法 | 返回类型 | 语义 |
|------|----------|------|
| `stat.count(window_event(alias))` | `digit` | 当前 rule instance/window 内，source alias `alias` 被纳入该窗口的候选事件总数 |
| `stat.count(match_event(label))` | `digit` | `on event` step label `label` 接受为证据的命中事件数；要求该 branch 使用 `count` measure |
| `stat.count(match_distinct(label))` | `digit` | `on event` step label `label` 的精确 distinct 数量；要求该 branch 使用 `distinct | count` |
| `stat.value(trigger(label))` | `float` | `on event` step label `label` 第一次满足阈值时的 measure value |
| `stat.value(final(label))` | `float` | `and close` step label `label` 在本次输出时的最终 measure value |

`window_event(alias)` 的计数点是：

- source alias 匹配。
- bind/filter 通过。
- scope key 提取成功。
- 事件已进入当前 rule instance/window。

它不要求 step guard 通过，不要求 distinct 后保留，不要求 threshold 触发，也不要求最终成为证据事件。

静态校验规则：

- 选择器参数使用静态符号，不加引号，例如 `window_event(auth)`、`match_event(port_scan)`。
- `window_event(alias)` 中的 `alias` 必须是当前规则声明的 source alias。
- `match_event(label)` / `match_distinct(label)` / `trigger(label)` / `final(label)` 中的 `label` 必须是当前规则中已命名的 match/close step label。
- `stat.count(match_event(label))` 要求对应 branch 使用 `count` measure。
- `match_distinct(label)` 要求对应 branch 使用 `distinct | count`，否则编译失败。
- `match_event(label)` / `match_distinct(label)` / `trigger(label)` 只能引用 `on event` label；`final(label)` 只能引用 `and close` label。
- 裸用 `window_event(...)`、`match_event(...)`、`trigger(...)` 等 selector 会编译失败。
- `stat.count(...)` / `stat.value(...)` 只允许在 `yield` 表达式里使用。

成本边界：

- 第一版只读取规则执行过程中已经维护的窗口、branch、distinct 和 measure 快照。
- 不支持 `stat.count(distinct(e.user_agent))` 这类任意字段统计。
- 不从 capped field buffer 推导精确统计。
- 所有统计仅在当前 rule instance/window 生命周期内有效。

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

输入 stream 中已经是结构化对象的字段可以直接透传，也可以用 `merge(...)` 做浅合并富化：

```wfl
yield security_alerts (
    extensions = merge(
        e.extension,
        object {
            source = "wfl";
            ioc_value = e.target_domain;
        }
    )
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
- `merge(obj1, obj2, ...)` 对 object 做从左到右的浅合并；后面的同名 key 覆盖前面的 key。缺失的 object 字段引用按空对象处理；object 字面量内部表达式失败、函数失败或非 object 参数会使 `merge()` 求值失败。
- JSON、XML、文本、CSV 等最终编码由 sink 决定。
- JSON sink 应把 `object` 输出为 JSON object，把 `array` 输出为 JSON array。
- XML sink 可把 `object` 输出为元素树；文本/CSV sink 可选择序列化或 flatten。
- 如果结构化字段写入下游中间 window，runtime 会把该值序列化为 UTF-8 JSON 字符串桥接；下游规则读取到的是 `chars` 表示。
- `array/T` 会检查数组元素类型。`array/float` 允许 digit 元素提升为 float；空数组可写入任意 array 字段。
- `mvcount`、`mvjoin`、`mvdedup`、`mvsort`、`mvreverse`、`mvindex`、`mvappend` 可处理 `array [...]` 字面量。`mvindex(array ["a", 1], 0)` 这种从异构数组取单个元素的写法会被拒绝，因为无法静态推断标量类型；使用三参数切片形式返回 array。
- `json_object(...)` 不作为主语法；如后续提供，也应只是返回 `object` 的便捷函数。

### `conv`

当前代码支持 `conv` 作为 post-close 结果集变换：

```wfl
conv { sort(-score) | top(10) ; }
conv { sort(-score) | top_ties(1) ; }   // RANK 语义：前 N 名 + 并列全输出
conv { sort(-score) ; where(count > 5) ; }
```

支持的操作：

- `sort(...)` — 稳定排序，`-` 前缀表降序
- `top(n)` — 截断前 n 条
- `top_ties(n)` — **RANK 语义并列全输出**：取前 n 条，并保留所有与第 n 条排序键**等值**的条目（并列者全出，不截断）。要求同一 chain 内前导 `sort(...)` 提供并列判定键（编译期检查）；`top_ties(0)` / 空输入安全退化为空/截断
- `dedup(expr)`
- `where(expr)`

说明：

- `conv` 位于 `yield` 之后、`limits` 之前
- checker 当前要求 `conv` 用于 `fixed` 或 `hop` 窗口：`match<...:fixed>` / `match<...:hop(size, slide)>`（窗口收口批边界确定；sliding/session 拒绝）
- 典型用途：每窗口收口批取 Top-N（NEXMark Q5/Q7 形态，`sort(-count) | top_ties(1)` 输出并列最高）

### `limits`

```wfl
limits {
    max_memory = "15GB";    // 规则级状态内存总上限（同规则分片共享, 2026-08-27 起）
    disk_provider = "redb"; // 状态落盘后端（2026-08-27 改名自 spill = "redb"; 旧键仍生效）
    max_disk = "20GB";      // 规则级磁盘总上限（2026-08-27 改名自 max_spill_bytes）
    max_instances = 10000;
    max_throttle = "100/min";
    on_exceed = throttle;
}
```

- `max_memory` / `max_disk` 都是**规则总量**——同规则全部分片共享一个占用计数,
  分片数是引擎内部细节（旧语义 2GB/片 × 10 = 20GB 的陷阱已废弃）。
- **三层预算阶梯**：状态估算 ≤ `max_memory` 全内存; 超限驱逐最老键落盘
  （`disk_provider = "redb"`）; 落盘超 `max_disk` 回退拒收新键（不丢已建桶）。
  ⚠ 内存+磁盘预算之和必须 ≥ 状态总量, 否则拒收丢键（bench `[clean]` 不报）。
- 旧键 `spill`（→ `disk_provider`）与 `max_spill_bytes`（→ `max_disk`）保留为
  兼容别名（lint 会给迁移 Warning）。
- **场景静态检查**（checker 编译期判定, 不静默忽略）:
  - `disk_provider` / `max_disk` 仅支持 **stats 规则**（match/on-each 规则无状态落盘
    路径, 配置 → Error）
  - `disk_provider` 要求 **非空键**（无 `group by` 的空键 stats 单桶无驱逐对象, 从不
    落盘 → Error）
  - `max_disk` 配了但没配 `disk_provider` → Warning（落盘未启用, 上限不生效）
- spill 仅适用 stats 规则（单实例 / key 分片; 输入分片暂不支持）。
- 详见 `docs/design/stats-state-spill-redb.md` §19/§20。

`on_exceed` 可选：

- `throttle`
- `drop_oldest`
- `fail_rule`

### `stats` — 声明式窗口统计

`stats` 规则形态（无 `match` 状态机，声明式窗口聚合）：

```wfl
rule q12_bidder_10s_window_count {
    events { b : bid_events }
    stats<10s:fixed> group by (b.bidder) {
        b | count as bid_count;
        b | avg(b.price) as avg_price;
        b | distinct(b.bidder) as uniq_bidders;
        b | last(b.url) as last_url;          // 最近合格行的整行字段（Q18）
        b | top(10, b.price) as top_prices;   // per-key top-N（Q19）
    }
    entity(digit, b.bidder)
    yield nexmark_alerts (
        id = b.bidder,
        detail = fmt("{} {}", b.bidder, stat.value(final(bid_count)))
    )
}
```

说明：

- 窗口规格 `stats<dur:mode>`：`fixed(dur)`（epoch 对齐固定桶）或 `session(gap)`；时长支持 `d` 后缀（如 `1d:fixed` = UTC 日历天桶）。
- `group by (keys)`：复合键分组；空键 = 全局单实例。
- 度量聚合：`count` / `sum(f)` / `avg(f)`（(sum,count) 归并） / `min(f)` / `max(f)` / `distinct(f)` / `last(f)`（保留最近行字段） / `top(N, f)`（per-key top-N）。
- `as <label>` 命名度量；yield 用 `stat.value(final(label))` 读取数值、`b.*` 读分组键/字段（last/top 行字段经 field_values 注入）。
- 每 (键 × 桶) 收口一行；无 `on each`/`match`/`score`，不参与 conv。
- 典型用途：NEXMark Q12/Q15-19 形态（按用户/分类/卖家/日历天的计数、去重、Top-N、末条）。

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
| `distinct`（`alias.field \| distinct \| count`） | 去重计数；仅作为 match 步骤的 pipe 变换使用，不是独立函数 |

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

- 字符串：`contains`、`regex_match`、`startswith`、`endswith`、`lower`、`upper`、`len`、`concat`、`join`、`join_by`
- 数值：`round`、`abs`、`ceil`、`floor`、`sqrt`、`pow`、`log`、`exp`、`clamp`
- 空值 / 空白处理：`coalesce`、`isnull`、`isnotnull`、`is_blank`、`null_if_blank`、`default_if_blank`
- 结构化对象：`merge`
- 时间：`time_diff`、`time_bucket`
- 网络：`cidr_match(ip, subnet)`（IP 是否落在子网内，subnet 为 `"addr/prefix"`，兼容 IPv4/IPv6，Sigma `|cidr` 等效）
- 当前引擎时间：`now`、`now_s`、`now_ms`、`now_us`、`now_ns`；时间值转换：`time_to_s`、`time_to_ms`
- 哈希 / 编码：`md5`、`sha1`、`sha1_n`、`sha256`、`hex`、`stable_id`
- 窗口集合：`collect_set`、`collect_list`、`first`、`last`
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
| `coalesce(v1, v2, ...)` | 首个非空参数的类型 | 返回第一个非 null / 可求值且非 blank 字符串的参数 |
| `merge(obj1, obj2, ...)` | `object` | 左到右浅合并 object，后面的同名 key 覆盖前面的 key；缺失的 object 字段引用按空对象处理，其他求值失败仍失败 |
| `isnull(expr)` | `bool` | 参数不可求值时返回 `true` |
| `isnotnull(expr)` | `bool` | 参数可求值时返回 `true` |
| `is_blank(text)` | `bool` | `text` 缺失、空字符串或全空白字符时返回 `true` |
| `null_if_blank(text)` | `chars` 或 null | `text` 为空白时返回 null，否则返回原字符串 |
| `default_if_blank(text, default)` | `chars` | `text` 缺失或为空白时返回 `default` |

示例：

```wfl
yield out (
    user = default_if_blank(e.user, "unknown"),
    src = coalesce(e.source_host, e.target_host, e.sip)
)
```

`coalesce(...)` 会跳过缺失字段、空字符串和全空白字符串；其他类型的可求值参数视为非空，直接作为 `yield` 字段赋值表达式时可继续按目标字段类型转换。`is_blank`、`null_if_blank`、`default_if_blank` 的参数必须是 `chars`。

#### 当前引擎时间

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `now()` | `time` | 当前 UTC epoch milliseconds；可直接传给 `strftime` |
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
- 默认 `time` 数值使用 epoch milliseconds；显式单位函数 `now_s()` / `now_us()` / `now_ns()` 按函数名返回对应单位。
- 当前运行时数值统一使用 `f64` 表示；需要可精确持久化的业务时间，优先写入 `time` 字段。

#### 时间值转换（epoch 单位）

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `time_to_ms(ts)` | `digit` | time/数值 → epoch 毫秒（13 位）；参数可为系统变量、时间字段或聚合结果，自动识别秒/毫秒/微秒/纳秒输入 |
| `time_to_s(ts)` | `digit` | time/数值 → epoch 秒（10 位） |

示例（告警表统一毫秒时间戳）：

```wfl
yield security_alerts (
    first_alert_time = time_to_ms(@event_first_time),
    first_insert_time = time_to_ms(s.parse_time),
    update_time = time_to_ms(@emit_time)
)
```

说明：

- 引擎内部时间表示对业务不可见——系统变量（`@event_*_time` 等）为毫秒，输入时间字段为纳秒；`time_to_*` 按数量级归一化后统一输出目标单位，两种来源结果一致。
- `time_to_ms(@event_first_time)` 等价于把该时间直接写入 `digit` 字段并输出毫秒；`strftime` 仍用于格式化为字符串。
- 注意：`yield` 表达式里的 `min(f)` / `max(f)` 是**匹配步骤序列聚合**（如 `min(match_event(e).x)`），对普通字段返回空；窗口内聚合请在 `match` 内用聚合语法，再在 `yield` 引用其结果。

#### 时间格式化与解析

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `strftime(timestamp, format)` | `chars` | 按 chrono 格式字符串格式化 UTC 时间；timestamp 可为秒/毫秒/微秒/纳秒 epoch 数值 |
| `strptime(text, format)` | `time` | 按格式解析时间，返回 epoch milliseconds |
| `time_diff(t1, t2)` | `float` | 返回两个时间的秒级差值绝对值 |
| `time_bucket(t, interval_seconds)` | `time` | 将时间向下取整到指定秒级窗口，返回 epoch milliseconds |

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
| `sha1_n(text, length)` | `chars` | 返回 SHA-1 小写十六进制字符串前 `length` 位，`length` 必须是 1 到 40 的整数 |
| `sha256(text)` | `chars` | 返回小写十六进制 SHA-256 字符串 |
| `hex(text)` | `hex` | 返回字符串字节的小写十六进制编码 |
| `stable_id(prefix, value, ...)` | `chars` | 返回 `prefix` + SHA-256 前 16 位 |

示例：

```wfl
yield security_alerts (
    cmd_sha256 = sha256(e.cmdline),
    raw_hex = hex(e.raw),
    alert_id = stable_id("alert_", e.sip, e.user, e.event_time),
    compact_id = sha1_n(join_by("|", e.sip, e.user, e.event_time), 16)
)
```

说明：

- `md5`、`sha1`、`sha256`、`hex` 的参数必须是 `chars`。
- `sha1_n(text, length)` 只截取 SHA-1 结果前 `length` 位，不改变输入内容。
- `stable_id` 的第一个参数必须是 `chars` 前缀；后续参数必须是标量值，支持 `chars`、`digit`、`float`、`bool`、`time`、`ip`、`hex`。
- `stable_id` 对后续值使用带类型和长度的稳定编码后再计算 SHA-256，避免简单拼接导致的歧义。
- 如果需要完全由规则作者控制拼接内容，可以使用 `sha1_n(join(...), n)` 或 `sha1_n(join_by(...), n)`。

#### 字符串拼接

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `join(value, ...)` | `chars` | 按参数顺序直接拼接，不加分隔符 |
| `join_by(separator, value, ...)` | `chars` | 按参数顺序拼接，并在字段之间插入显式分隔符 |

示例：

```wfl
yield security_alerts (
    raw_key = join(e.sip, e.user, e.action),
    readable_key = join_by("|", e.sip, e.user, e.action),
    alert_hash = sha1_n(join_by("|", e.sip, e.user, e.action), 16)
)
```

说明：

- `join` / `join_by` 不 trim、不改大小写、不转义 `%`、不转义 `|`。
- 空字符串按原样参与拼接，取不到的参数按空字符串片段处理。
- `join_by` 的 separator 必须是 `chars`；separator 取不到或求值失败时，函数整体失败。
- 参数支持标量值：`chars`、`digit`、`float`、`bool`、`time`、`ip`、`hex`；不接受 `array` / `object`。

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

#### 窗口集合函数

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `collect_set(alias.field)` | `array/T` | 收集当前 rule instance 内 alias 事件集合最近最多 1024 个字段值，按首次出现顺序去重 |
| `collect_list(alias.field)` | `array/T` | 收集当前 rule instance 内 alias 事件集合最近最多 1024 个字段值，保留出现顺序 |
| `first(alias.field)` | `T` | 返回当前 rule instance 内 alias 最近字段样本中的首个字段值 |
| `last(alias.field)` | `T` | 返回当前 rule instance 内 alias 最近字段样本中的末个字段值 |

`collect_set(alias.field)` 和 `stat.count(window_event(alias))` 基于同一个 alias 事件集合。常见 evidence 输出写法：

```wfs
window security_alerts {
    over = 0
    fields {
        event_count: digit
        evidences: array/chars
    }
}
```

```wfl
yield security_alerts (
    event_count = stat.count(window_event(s)),
    evidences = collect_set(s.event_id)
)
```

如果某条事件缺少 `event_id`，它仍计入 `event_count`，但不会进入 `evidences`。alias 字段集合保留最近最多 1024 个字段值；`collect_set` / `collect_list` / `first` / `last` 均基于这组最近样本。大窗口或重复 `event_id` 场景下，`evidences` 数组长度可能小于 `event_count`。

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

- step 必须显式声明 source
- `match` 至少需要有效的事件/关闭路径才能通过后续语义检查
- `close_reason` 仅可在 `on close` 中引用
- `match` 与 `on each` 互斥
- `conv` 仅允许与 fixed / hop 窗口搭配（sliding/session 拒绝）；`top_ties` 要求同 chain 前导 `sort`
- `emit at`（deferred join）仅支持 `on each` 驱动形态
- session 窗口的 gap 与 `asof within` 的时长必须 `> 0`（当前检查器不强制 `match` 滑动/固定窗口的 `duration > 0`，`match<sip:0>` 可通过解析）

Seq / Any 约束：

- 裸 `on event { ... }` 等价 `on event seq { ... }`，向后兼容
- `on event seq` 步骤按序完成，支持 `has` / `within` / `not` / `consec` / `skip`
- `on event any` 并行评估，顺序无关，**不支持** `within` / `not` / `consec` / `skip`（编译期拒绝）
- `not` 步骤不得引用字段聚合（编译期拒绝）；否定约束请用 `not has <alias> && <谓词>`
- `skip = to_next` 延后到 L3，使用会收到警告
- `on event seq` / `any` 不支持用于 pipeline stages（编译期拒绝）
- `has <alias>` 存在性步骤，等价 `count >= 1`

On Each 约束：

- `alias` 必须来自 `events`
- `where` 必须返回 `bool`
- 不支持 `close_reason`
- 不支持集合函数和窗口状态函数
- 当前不支持与 pipeline stages 混用

Yield 约束：

- 目标 window 必须存在且 `stream_tag` 为空
- 字段须为目标 window 的子集
- 禁止手工赋值系统字段
- 中间目标图必须无环
