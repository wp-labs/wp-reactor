# 第二部分：规则编写指南

第一部分介绍了核心概念和处理流程。这一部分从实际场景出发，逐步学会用 WFL 编写检测规则。

---

## 1. 场景一：简单阈值 — 暴力破解检测

> 同一 IP 在 5 分钟内登录失败 3 次，产出告警。

### 1.1 定义 Window

先从数据定义开始：

```wfs
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window security_alerts {
    over = 0

    fields {
        sip: ip
        window_events: digit
        fail_count: digit
        first_seen: time
        last_seen: time
        rule_window_start: time
        rule_window_end: time
        latest_analysis_time: time
        message: chars
    }
}
```

两个 window：`auth_events` 是输入窗口（订阅 `syslog` 流，保留 5 分钟），`security_alerts` 是输出窗口（`over = 0` 表示不保留历史数据，仅作为输出目标）。

### 1.2 编写规则

```wfl
use "security.wfs"

rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }

    match<sip:5m> {
        on event {
            failed_hits: fail | count >= 3;
        }
    } -> score(70.0)

    entity(ip, fail.sip)

    yield security_alerts (
        sip = fail.sip,
        window_events = stat.count(window_event(fail)),
        fail_count = stat.count(match_event(failed_hits)),
        first_seen = @event_first_time,
        last_seen = @event_last_time,
        rule_window_start = @window_start_time,
        rule_window_end = @window_end_time,
        latest_analysis_time = @emit_time,
        message = fmt("{} brute force detected", fail.sip)
    )
}
```

逐行解读：

**`events { fail : auth_events && action == "failed" }`**

绑定事件源。`fail` 是别名（后续引用用），`auth_events` 是 window 名，`&&` 后是过滤条件——只关心 `action == "failed"` 的事件。

**`match<sip:5m>`**

定义匹配窗口：按 `sip` 分组（不同 IP 独立计数），滑动窗口 5 分钟。`<sip:5m>` 读作"按 sip 分组的 5 分钟窗口"。

分组 key 除顶层/单层字段外，还支持**多层嵌套路径**与 **`let` 派生字段**（issue #83）：嵌套路径按叶值分组（root 需为结构化 object/array 字段），`let` 派生字段在事件进入窗口前求值后参与分组，二者结果一致。半结构化日志（如安全发现对象 `s.source_finding_obj.attacker.endpoint.ip`）建议优先在接入/解码层把关键字段上提为顶层字段（性能最优）；源不可控时用规则内嵌套/派生 key：

```wfl
let attacker_ip = s.source_finding_obj.attacker.endpoint.ip
match<attacker_ip:1d:fixed> {
    on event<accu> { s | count >= 1; }
}
```

派生/嵌套 key 的缺失、为空或路径中途漏写段 → 该事件不进入任何实例（与普通 key 缺失行为一致）。v1 限制：仅单事件源规则；与 `rule_shards > 1`、`conv`、pipeline stage 组合暂不支持（详见语言参考）。

**`on event { failed_hits: fail | count >= 3; }`**

事件触发条件。`failed_hits` 是 step label，用于在 `yield` 中稳定引用这一步的统计值；`fail` 引用 events 中绑定的别名，`|` 后是聚合条件：`fail` 事件的 `count`（累积计数）达到 3。

**`-> score(70.0)`**

命中后产出风险评分 70.0（范围 0-100）。

**`entity(ip, fail.sip)`**

告警归属的实体：类型为 `ip`，ID 取自 `fail.sip`。同一实体多次命中会被关联。

**`yield security_alerts (...)`**

输出到 `security_alerts` 窗口。`stat.count(window_event(fail))` 输出当前 rule instance/window 内进入窗口的候选失败事件数，`stat.count(match_event(failed_hits))` 输出 `failed_hits` 这一步接受为证据的命中事件数；`@event_first_time` / `@event_last_time` / `@window_start_time` / `@window_end_time` / `@emit_time` 输出稳定时间语义；格式化函数（`fmt(...)`）用于构造可读消息。

### 1.3 编写测试

```wfl
test brute_force_hit for brute_force {
    input {
        row(fail, sip = "10.0.0.1", username = "admin",
            action = "failed", event_time = "2026-01-01T00:00:00Z");
        row(fail, sip = "10.0.0.1", username = "admin",
            action = "failed", event_time = "2026-01-01T00:00:01Z");
        row(fail, sip = "10.0.0.1", username = "admin",
            action = "failed", event_time = "2026-01-01T00:00:02Z");
    }
    expect {
        hits == 1;
        hit[0].score == 70.0;
        hit[0].entity_type == "ip";
        hit[0].entity_id == "10.0.0.1";
    }
}
```

`row(alias, field = value, ...)` 注入测试事件，`expect { ... }` 断言命中次数和告警字段。用 `wfl test` 运行：

```bash
wfl test rules/brute_force.wfl
```

### 1.4 输出结构化上下文

如果 sink 需要收到结构化的风险上下文，不要把 JSON 当字符串拼出来。先在输出 window 中声明结构化字段：

```wfs
window security_alerts {
    over = 0

    fields {
        sip: ip
        fail_count: digit
        risk_context: object
        tags: array
        scores: array/float
    }
}
```

然后在 `yield` 里构造 `object` / `array`，或者透传输入 stream 中的结构化对象并增量合并字段：

```wfl
yield security_alerts (
    sip = fail.sip,
    fail_count = stat.count(match_event(failed_hits)),
    risk_context = object {
        score: float = @score;
        source = fail.sip;
        username = fail.username;
        tags: array = array ["bruteforce", "auth", fail.action];
    },
    tags = array ["bruteforce", "auth"],
    scores = array [@score, 1]
)
```

```wfl
yield security_alerts (
    risk_context = merge(
        fail.extension,
        object {
            source = "wfl";
            ioc_value = fail.sip;
        }
    )
)
```

使用要点：

- `object` / `array` 可用于输入 stream、输出 window 或中间 window；provider window 暂不支持结构化字段。
- 输入 stream 中的结构化字段可直接 `yield` 透传，也可用 `merge(obj1, obj2, ...)` 做浅合并富化；后面的同名 key 覆盖前面的 key。
- `merge()` 中缺失的 object 字段引用会按空对象跳过；如果 object 字面量内部字段表达式不可求值，或参数不是 object，`merge()` 会失败。
- `array/float` 允许整数元素自动提升为 float；`array/chars` 只接受字符串元素。
- 如果结构化字段写入中间 window，下游规则读取到的是 UTF-8 JSON 字符串桥接值；最终 sink 输出格式仍由 sink 决定。

### 1.5 输出证据时间和窗口时间

安全告警通常需要同时输出四类时间：

- 事件时间：窗口内该实体的候选事件（进入实例的被接受事件）首尾——`first_seen` / `last_seen` 用 `@event_first_time` / `@event_last_time`。
- 证据时间：本次命中实际依据的事件跨度——用 `@evidence_start_time` / `@evidence_end_time`。
- 窗口时间：规则窗口的开始和结束时间。
- 分析时间：本次告警输出的时间（`@emit_time`）与首次命中处理时刻（`@first_match_time`）。

建议把这些字段作为业务字段写入输出 window：

```wfs
window security_alerts {
    over = 0

    fields {
        sip: ip
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

规则中在 `yield` 里使用时间系统变量：

```wfl
yield security_alerts (
    sip = fail.sip,
    first_seen = @event_first_time,
    last_seen = @event_last_time,
    evidence_start_time = @evidence_start_time,
    evidence_end_time = @evidence_end_time,
    rule_window_start = @window_start_time,
    rule_window_end = @window_end_time,
    latest_analysis_time = @emit_time
)
```

命名建议：

- 对外字段使用 `first_seen` / `last_seen` 这类业务名时，右侧仍映射到明确语义的系统变量。
- 时间系统变量在表达式里的数值表示为 epoch milliseconds；写入 `time` 字段时按时间类型输出。
- 不使用 `event_fst_time` / `event_lst_time` 这类缩写，避免用户误解。
- 不依赖 `__wfu_emit_time` 等内部元数据作为业务输出；需要业务字段时在 `yield` 中显式赋值。

### 1.6 输出稳定统计上下文

告警通常还需要输出“为什么触发”的统计证据，例如窗口里总共看到了多少候选事件、命中了多少事件、distinct 后有多少端口、阈值触发时的计数以及 close 输出时的最终计数。当前写法使用 `stat.count(...)` / `stat.value(...)` 加统计选择器：

```wfl
rule port_scan {
    events {
        net : conn_events && action == "syn"
    }

    match<sip:5m> {
        on event {
            port_scan: net.dport | distinct | count >= 10;
        }
        and close {
            final_ports: net.dport | distinct | count >= 1;
        }
    } -> score(85.0)

    entity(ip, net.sip)
    yield security_alerts (
        sip = net.sip,
        window_events = stat.count(window_event(net)),
        matched_events = stat.count(match_event(port_scan)),
        distinct_ports = stat.count(match_distinct(port_scan)),
        trigger_count = stat.value(trigger(port_scan)),
        final_count = stat.value(final(final_ports))
    )
}
```

语义说明：

- `window_event(net)` 表示 alias `net` 进入当前 rule instance/window 的候选事件集合。
- `match_event(port_scan)` 表示 `on event` label `port_scan` 接受为证据的事件集合。
- `match_distinct(port_scan)` 表示 `on event` label `port_scan` 的精确 distinct 集合，要求该 branch 使用 `distinct | count`。
- `trigger(port_scan)` 表示 `on event` label `port_scan` 第一次满足阈值时的 measure 快照。
- `final(final_ports)` 表示 `and close` label `final_ports` 在本次输出时的最终 measure 快照。

使用要点：

- `net`、`port_scan`、`final_ports` 是静态符号，不加引号；checker 会在编译期校验 alias/label 是否存在。
- selector 只能出现在 `stat.count(...)` 或 `stat.value(...)` 里，不能单独作为普通函数使用。
- `stat.count(...)` / `stat.value(...)` 只能在 `yield` 表达式里使用。
- `stat.count(match_event(label))` 要求对应 branch 使用 `count` measure。
- `stat.count(match_distinct(label))` 要求对应 branch 使用 `distinct | count`。
- `stat.count(match_event(label))` / `stat.count(match_distinct(label))` / `stat.value(trigger(label))` 只能引用 `on event` label；`stat.value(final(label))` 只能引用 `and close` label。
- 第一版只读取规则本来已经维护的状态，不支持任意字段统计，避免无界内存成本。
- 如果需要 close/flush 输出时的最终计数，需要在 `and close { ... }` 中用单独 label 显式建模。

---

## 2. 场景二：多步骤序列 — 扫描后爆破

> 同一 IP 先做端口扫描（5 次 SYN），然后尝试登录失败（3 次）。两个步骤必须顺序发生。

```wfl
rule chain_attack {
    events {
        scan    : conn_events && action == "syn"
        login   : conn_events && action == "login_fail"
        success : conn_events && action == "login_success"
    }

    match<sip:30m> {
        on event seq {
            scan | count >= 5;               # 步骤 1：累积 5 次扫描
            login | count >= 3 within 10m;   # 步骤 2：10m 内累积 3 次登录失败
            not has success within 5m;       # 否定：步骤 2 后 5m 内无成功登录
        }
    } -> score(90.0)

    entity(ip, scan.sip)
    yield network_alerts (
        sip = scan.sip,
        alert_type = "chain_attack",
        detail = "scan then brute force, no success"
    )
}
```

**关键点**：

`on event seq` 中的步骤（分号分隔）必须**顺序满足**，并支持步间约束：
1. 先累积 5 次 `scan` 事件 → 推进到步骤 2
2. `login | count >= 3 within 10m` —— 10m 内累积 3 次登录失败 → 推进
3. `not has success within 5m` —— 否定步：上一步后 5m 内不得出现成功登录（爆破未得手）

如果先来 3 次 `login` 再来 5 次 `scan`，不会命中——步骤 1 的 `scan` 还没满足，步骤 2 的 `login` 事件会被忽略。

`within` 约束步间时间 gap；`not` 排除窗口内的否定事件。若不需要顺序/约束，可用 `on event any`
做无序共现（全部满足即触发，顺序无关）。

### OR 分支

可以在同一步骤中提供多条分支（用 `||` 分隔同一 `;` 终止步骤内的分支）：

```wfl
on event {
    scan | count >= 5;
    login | count >= 3 || exploit | count >= 1;
}
```

`login`、`exploit` 是 `events` 中声明的别名（例如 `login : conn_events && action == "login_fail"`）。步骤 1（`scan`）满足后，步骤 2 有两条分支——`login` 或 `exploit` 任一满足即命中。

---

## 3. 场景三：关闭模式 — 窗口结束时的检测

有些检测不适合事件驱动触发，更适合"窗口结束时统一判断"。

### 3.1 on close（OR 模式）

`on event` 和 `on close` 独立触发，各自产出告警：

```wfl
rule data_exfil {
    events { c : conn_events }

    match<sip:10m> {
        on event {
            burst: c.bytes | sum >= 100000000;  # 突发 100MB，立即告警
        }
        on close {
            total: c.bytes | sum >= 50000000;   # 窗口关闭时总量 50MB，也告警
        }
    } -> score(85.0)

    entity(ip, c.sip)
    yield network_alerts (sip = c.sip, alert_type = "data_exfil")
}
```

OR 模式下，两条路径独立：
- 流量突发 100MB → `on event` 立即触发，告警 origin = `event`
- 窗口关闭时累计达 50MB → `on close` 触发，告警 origin = `close:timeout`

同一个 IP 在窗口内可能产出两次告警。

### 3.2 and close（AND 模式）

```wfl
match<sip:5m> {
    on event {
        fail | count >= 3;
    }
    and close {
        fail | count >= 1;
    }
} -> score(70.0)
```

AND 模式下，**两个条件必须同时满足**才产出告警：
1. `on event` 条件满足（3 次失败）→ 设置 `event_ok = true`
2. 窗口关闭时 `and close` 条件也满足 → 如果 `event_ok && close_ok`，产出告警

这在需要"事件发生 + 窗口结束确认"的场景中很有用。

### 3.3 关闭触发方式

| 触发方式 | 含义 | 何时发生 |
|----------|------|---------|
| `timeout` | 窗口时间到期 | `over` 时长后自动触发 |
| `flush` | 引擎关闭 | `wfusion` 收到 SIGINT/SIGTERM 时 |
| `eos` | 数据流结束 | 测试中数据输入完毕自动触发 |

---

## 4. 场景四：管道 — 多阶段聚合

对于需要"先聚合再聚合"的复杂场景，用 `|>` 管道串联多个 match 阶段。

```wfl
rule repeated_fail_bursts {
    events {
        e : auth_events && action == "failed"
    }

    match<sip,username:5m:fixed> {
        on event { e | count >= 1; }
        and close { burst: e | count >= 3; }
    }
    |> match<sip:30m:fixed> {
        on event { _in | count >= 1; }
        and close { users: _in.username | distinct | count >= 2; }
    } -> score(85.0)

    entity(ip, _in.sip)
    yield security_alerts (
        sip = _in.sip,
        message = fmt("{} multi-user fail bursts", _in.sip)
    )
}
```

管道数据流：

```
阶段 1: match<sip,username:5m:fixed>
  按 (sip, username) 分组，fixed 5 分钟窗口
  关闭时：同一 (sip, username) 失败 >= 3 次 → 输出一条记录

  输出记录自动包含: username, sip, burst(count) 等字段

         ↓  _in 引用阶段 1 的输出

阶段 2: match<sip:30m:fixed>
  按 sip 分组，fixed 30 分钟窗口
  关闭时：不同 username >= 2 个 → 命中
```

`fixed` 窗口与默认的 `sliding` 窗口不同：fixed 窗口到期后整批处理，sliding 窗口事件到达即处理。管道中间阶段通常用 `fixed`。

`_in` 是管道中引用上一阶段输出的隐式别名。

---

## 5. 场景五：Conv — 结果后处理

命中告警后，可以用 `conv` 对结果集做排序、截断、并列全出、去重、过滤。

```wfl
rule top_port_scanners {
    events { c : conn_events && action == "syn" }

    match<sip:1h:fixed> {
        on event { c | count >= 1; }
        and close { scan: c.dport | distinct | count >= 3; }
    } -> score(80.0)

    entity(ip, c.sip)
    yield network_alerts (
        sip = c.sip,
        alert_type = "port_scan"
    )

    conv {
        sort(-scan) | top(2);
    }
}
```

`conv` 在同一个 `match` 窗口的**所有命中告警**上执行：

| 操作 | 含义 | 示例 |
|------|------|------|
| `sort(field)` | 按字段排序，`-field` 降序 | `sort(-scan)` |
| `top(n)` | 保留前 n 条 | `top(2)` |
| `top_ties(n)` | RANK 语义：前 n 条 + 与第 n 条并列的全部条目 | `sort(-scan) \| top_ties(1)` |
| `dedup(field)` | 按字段去重 | `dedup(sip)` |
| `where condition` | 条件过滤 | `where scan >= 5` |

`conv` 只能与 `fixed` / `hop` 窗口配合使用——因为 sliding 窗口的事件驱动模式下，告警是逐个产出的，没有"结果集"可以做后处理；hop 窗口在 slide 边界成批收口，与 fixed 同有确定收口批边界。

---

## 6. 逐条评分 — on each

如果只需要对每条事件打分、不需要窗口聚合，用 `on each`：

```wfl
rule enrich_each {
    events { e : auth_events }

    on each e -> score(if e.action == "failed" then 70.0 else 10.0)

    entity(ip, e.sip)
    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip,
        username = e.username
    )
}
```

`on each` 每条事件触发一次，无窗口状态，无分组。适合"先对每条事件做语义打分，再在下游窗口聚合"的两阶段架构。

---

## 7. 复用公共 Yield 字段

`yield preset` 用于把多条规则都会输出的公共字段集中定义，再在具体 `yield` 中组合使用。preset 可以声明参数，使用点通过 `preset<args...>` 按位置传入；声明中带默认值的参数可省略。

```wfl
yield preset base_alerts <severity, source = "wfusion"> (
    rule_name = @__wfu_rule_name,
    score = @score,
    severity = $severity,
    source = $source
)

rule scan {
    ...
    yield security_alerts : base_alerts<"high"> (
        alert_type = "scan",
        ioc_value = e.dip
    )
}
```

组合规则为：从左到右展开 preset，后者覆盖前者，当前 `yield (...)` 覆盖所有 preset；最终字段集合仍按目标 output window 做强校验。

`$severity`、`$source` 这类 `$param` 只在 `yield preset` 声明内部表示 preset 参数；普通规则表达式中的 `$VAR` 仍属于 WFL 预处理变量。

项目级公共 preset 可放入规则根目录下的 `_global.wfl`。规则根目录由 `runtime.rules` glob 的非通配前缀推导，例如 `rules/**/*.wfl` 对应 `rules/_global.wfl`，`rules/current/*.wfl` 对应 `rules/current/_global.wfl`。运行时会自动把它作为 project prelude 加载，并从普通规则文件列表中排除；`_global.wfl` 只允许 `yield preset` 声明，不会自动启用普通检测规则。

---

## 8. 内置函数速查

WFL 在 `match` 条件和 `yield` 赋值中均可使用内置函数：

| 类别 | 函数 | 说明 |
|------|------|------|
| **数学** | `abs`, `round`, `ceil`, `floor`, `sqrt`, `pow`, `log`, `exp` | 数值计算 |
| | `clamp(v, lo, hi)`, `sign`, `trunc` | 值限制与截断 |
| | `is_finite` | 浮点数校验 |
| **字符串** | `ltrim`, `rtrim`, `trim` | 空白裁剪 |
| | `concat(a, b, ...)`, `join(a, b, ...)`, `join_by(sep, a, b, ...)`, `fmt("{} {}", a, b)` | 拼接与格式化 |
| | `lower`, `upper`, `len` | 大小写与长度 |
| | `contains`, `startswith_any`, `endswith_any` | 包含判断 |
| | `indexof`, `replace_plain` | 搜索与替换 |
| | `split(s, sep)` | 拆分为多值数组 |
| **多值** | `mvindex(arr, i)`, `mvsort(arr)`, `mvreverse(arr)` | 数组操作 |
| | `mvjoin(arr, sep)` | 数组拼接为字符串 |
| | `collect_set(alias.field)`, `collect_list(alias.field)` | 窗口内最近字段样本收集 |
| **空值/空白** | `coalesce(a, b, ...)`, `isnull`, `isnotnull` | 按顺序取第一个非 null 且非 blank 字符串的值 |
| **Hash/ID** | `md5`, `sha1`, `sha1_n`, `sha256`, `hex`, `stable_id` | Hash、编码与稳定 ID |
| **时间** | `strptime(s, fmt)`, `strftime(t, fmt)` | 时间解析与格式化 |
| **条件** | `if cond then a else b` | 三目条件表达式 |

需要规则作者完全控制拼接内容时，可以用 `join` / `join_by` 组合 `sha1_n` 生成短 ID：

```wfl
raw_key = join(e.sip, e.user, e.action),
readable_key = join_by("|", e.sip, e.user, e.action),
short_hash = sha1_n(join_by("|", e.sip, e.user, e.action), 16)
```

`join` / `join_by` 不做 trim、大小写转换、转义或长度前缀编码；取不到的字段参数按空字符串片段处理。

输出 evidence 集合时，推荐让计数和 ID 集合引用同一个 alias：

```wfl
event_count = stat.count(window_event(s)),
evidences = collect_set(s.event_id)
```

`collect_set(s.event_id)` 与 `stat.count(window_event(s))` 基于同一个 rule instance 内的 `s` 事件集合；`collect_set` 对字段值去重并保留首次出现顺序。

---

## 9. 规则编写 Checklist

完成一条规则时，检查以下各项：

- [ ] `use` 导入了所需的 `.wfs` 文件
- [ ] `events` 中每个 alias 绑定了正确的 window，过滤条件正确
- [ ] `match<key:duration>` 的 key 选择合理（避免高基数 key 导致状态膨胀）
- [ ] 聚合步骤语义正确：`count`/`sum`/`avg`/`min`/`max`/`distinct` 选择恰当
- [ ] 关闭模式选择正确：`on close`（OR）vs `and close`（AND）
- [ ] `entity(type, id)` 声明了正确的实体类型和 ID 字段
- [ ] `yield target (...)` 的目标 window 存在，字段赋值正确
- [ ] `-> score(expr)` 评分在 [0, 100] 范围内
- [ ] `test` 块覆盖了命中路径和未命中路径
- [ ] 变量引用使用 `${VAR:default}` 语法提供默认值

---

## 9. 下一步

- [WFL 语言参考](./language-reference.md) — 完整语法与语义规范
- [运行时配置](./runtime-config.md) — TOML 配置详解
- [工具链](./tooling.md) — `wfl lint` / `wfl explain` / `wfgen` 使用
