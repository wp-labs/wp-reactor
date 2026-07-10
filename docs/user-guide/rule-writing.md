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
        fail_count: digit
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
            fail | count >= 3;
        }
    } -> score(70.0)

    entity(ip, fail.sip)

    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
```

逐行解读：

**`events { fail : auth_events && action == "failed" }`**

绑定事件源。`fail` 是别名（后续引用用），`auth_events` 是 window 名，`&&` 后是过滤条件——只关心 `action == "failed"` 的事件。

**`match<sip:5m>`**

定义匹配窗口：按 `sip` 分组（不同 IP 独立计数），滑动窗口 5 分钟。`<sip:5m>` 读作"按 sip 分组的 5 分钟窗口"。

**`on event { fail | count >= 3; }`**

事件触发条件。`fail` 引用 events 中绑定的别名，`|` 后是聚合条件：`fail` 事件的 `count`（累积计数）达到 3。

**`-> score(70.0)`**

命中后产出风险评分 70.0（范围 0-100）。

**`entity(ip, fail.sip)`**

告警归属的实体：类型为 `ip`，ID 取自 `fail.sip`。同一实体多次命中会被关联。

**`yield security_alerts (...)`**

输出到 `security_alerts` 窗口。字段赋值中可直接使用聚合函数（`count(fail)`）和格式化函数（`fmt(...)`）。

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

然后在 `yield` 里构造 `object` / `array`：

```wfl
yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
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

使用要点：

- `object` / `array` 只用于输出 window 或中间 window；输入 stream/provider window 不允许声明结构化字段。
- 源数据里的 JSON object/array 先按 `chars` 接入，需要输出时再用 `yield` 构造结构化值。
- `array/float` 允许整数元素自动提升为 float；`array/chars` 只接受字符串元素。
- 如果结构化字段写入中间 window，下游规则读取到的是 UTF-8 JSON 字符串桥接值；最终 sink 输出格式仍由 sink 决定。

---

## 2. 场景二：多步骤序列 — 扫描后爆破

> 同一 IP 先做端口扫描（5 次 SYN），然后尝试登录失败（3 次）。两个步骤必须顺序发生。

```wfl
rule chain_attack {
    events {
        scan : conn_events && action == "syn"
        login : conn_events && action == "login_fail"
    }

    match<sip:30m> {
        on event {
            scan | count >= 5;
            login | count >= 3;
        }
    } -> score(90.0)

    entity(ip, scan.sip)
    yield network_alerts (
        sip = scan.sip,
        alert_type = "chain_attack",
        detail = "scan then brute force"
    )
}
```

**关键点**：

`match` 中有两个步骤（分号分隔），必须**顺序满足**：
1. 先累积 5 次 `scan` 事件 → 推进到步骤 2
2. 再累积 3 次 `login` 事件 → 命中

如果先来 3 次 `login` 再来 5 次 `scan`，不会命中——步骤 1 的 `scan` 还没满足，步骤 2 的 `login` 事件会被忽略。

### OR 分支

可以在同一步骤中提供多条路径：

```wfl
on event {
    scan | count >= 5;
    login : conn_events && action == "login_fail" | count >= 3;
    exploit : conn_events && action == "exploit" | count >= 1;
}
```

步骤 1（`scan`）满足后，步骤 2 有两条路径——`login` 或 `exploit` 任一满足即命中。

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

命中告警后，可以用 `conv` 对结果集做排序、截断、去重、过滤。

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
| `dedup(field)` | 按字段去重 | `dedup(sip)` |
| `where condition` | 条件过滤 | `where scan >= 5` |

`conv` 只能与 `fixed` 窗口配合使用——因为 sliding 窗口的事件驱动模式下，告警是逐个产出的，没有"结果集"可以做后处理。

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

## 7. 内置函数速查

WFL 在 `match` 条件和 `yield` 赋值中均可使用内置函数：

| 类别 | 函数 | 说明 |
|------|------|------|
| **数学** | `abs`, `round`, `ceil`, `floor`, `sqrt`, `pow`, `log`, `exp` | 数值计算 |
| | `clamp(v, lo, hi)`, `sign`, `trunc` | 值限制与截断 |
| | `is_finite` | 浮点数校验 |
| **字符串** | `ltrim`, `rtrim`, `trim` | 空白裁剪 |
| | `concat(a, b, ...)`, `fmt("{} {}", a, b)` | 拼接与格式化 |
| | `lower`, `upper`, `len` | 大小写与长度 |
| | `contains`, `startswith_any`, `endswith_any` | 包含判断 |
| | `indexof`, `replace_plain` | 搜索与替换 |
| | `split(s, sep)` | 拆分为多值数组 |
| **多值** | `mvindex(arr, i)`, `mvsort(arr)`, `mvreverse(arr)` | 数组操作 |
| | `mvjoin(arr, sep)` | 数组拼接为字符串 |
| **空值** | `coalesce(a, fallback)`, `isnull`, `isnotnull` | NULL 处理 |
| **时间** | `strptime(s, fmt)`, `strftime(t, fmt)` | 时间解析与格式化 |
| **条件** | `if cond then a else b` | 三目条件表达式 |

---

## 8. 规则编写 Checklist

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
