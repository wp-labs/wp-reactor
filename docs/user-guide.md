# WFL 用户指南

> WarpFusion Language (WFL) v2.1 — 安全检测与实体行为分析 DSL

## 目录

- [1. 概述](#1-概述)
- [2. 快速开始](#2-快速开始)
- [3. 三文件模型](#3-三文件模型)
- [4. Window Schema (.wfs)](#4-window-schema-wfs)
- [5. 检测规则 (.wfl)](#5-检测规则-wfl)
- [6. 运行时配置 (fusion.toml)](#6-运行时配置-fusiontoml)
- [7. 表达式与函数](#7-表达式与函数)
- [8. 规则契约测试](#8-规则契约测试)
- [9. 测试数据生成 (wf-datagen)](#9-测试数据生成-wf-datagen)
- [10. 告警输出](#10-告警输出)
- [11. 运行引擎](#11-运行引擎)
- [12. 能力分层参考](#12-能力分层参考)
- [附录 A. 类型系统](#附录-a-类型系统)
- [附录 B. 语义约束速查](#附录-b-语义约束速查)

---

## 1. 概述

WFL 是 WarpFusion 的检测领域专用语言（DSL），用于编写安全关联检测规则、风险告警归并与实体行为分析逻辑。

**核心设计理念：**

- **简洁可读**：默认语法首屏可上手，安全分析师无需编程背景。
- **显式优先**：一种能力只保留一种主写法，减少歧义。
- **可解释可调试**：语法、语义、执行模型一一对应。

**WFL 不是什么：**

- 不是通用流计算 SQL。
- 不是任意 DAG 引擎。
- 不追求全功能分析查询语言。

### 1.1 核心执行模型

WFL 语法是前端语言，编译后转为 Core IR 四原语执行：

| 原语 | 职责 |
|------|------|
| `Bind` | 绑定事件源（window + filter） |
| `Match` | 按 key + duration 维护状态机并求值 |
| `Join` | 对匹配上下文做 LEFT JOIN 关联（L2） |
| `Yield` | 写入目标 window，输出告警 |

规则的固定执行链为：

```
BIND → SCOPE(match) → JOIN → ENTITY → YIELD
```

---

## 2. 快速开始

### 2.1 目录结构

一个典型的 WarpFusion 项目包含三类文件：

```
my-project/
├── fusion.toml           # 运行时配置（入口）
├── schemas/
│   └── security.wfs      # 数据定义（Window Schema）
├── rules/
│   └── brute_force.wfl   # 检测规则
└── alerts/               # 告警输出目录
```

### 2.2 第一个规则：暴力破解检测

**第 1 步 — 定义数据窗口** (`schemas/security.wfs`)

```wfs
window auth_events {
    stream = "syslog"
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

**第 2 步 — 编写检测规则** (`rules/brute_force.wfl`)

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

**第 3 步 — 配置运行时** (`fusion.toml`)

```toml
sinks = "sinks"

[server]
listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

[window_defaults]
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[vars]
FAIL_THRESHOLD = "3"
```

**第 4 步 — 启动引擎**

```bash
wf run --config fusion.toml
```

引擎启动后监听 `tcp://127.0.0.1:9800`，接收 Arrow IPC 格式的事件流，执行规则检测，输出告警到 `sinks/` 配置的目标文件。

---

## 3. 三文件模型

WFL 采用职责分离的三文件模型：

| 文件 | 扩展名 | 职责 | 热加载 |
|------|--------|------|:------:|
| Window Schema | `.wfs` | 逻辑数据定义（window、field、time、over） | 否 |
| 检测规则 | `.wfl` | 检测逻辑（bind/match/join/yield） | 是 |
| 运行时配置 | `.toml` | 物理参数（mode、内存、watermark、sinks） | 仅 `[vars]` |

**依赖关系：**

```
.wfs（先有数据定义）
  ↑
.wfl（规则引用 window）
  ↑
.toml（物理参数 + 变量）
```

- `.wfl` 仅能引用 `use` 导入的 window。
- `.toml` 只管物理参数，不写业务规则。
- `.wfs` 变更需要重启引擎。

---

## 4. Window Schema (.wfs)

Window 是 WFL 的数据抽象层，定义事件流的逻辑结构。

### 4.1 基本语法

```wfs
window <名称> {
    stream = <数据流名>
    time = <时间字段>
    over = <保留时长>

    fields {
        <字段名>: <类型>
        ...
    }
}
```

### 4.2 字段类型

| WFL 类型 | 含义 | 底层映射 |
|----------|------|----------|
| `chars` | 字符串 | Utf8 |
| `digit` | 整数 | Int64 |
| `float` | 浮点数 | Float64 |
| `bool` | 布尔值 | Boolean |
| `time` | 时间戳 | Timestamp(Nanosecond) |
| `ip` | IP 地址 | Utf8 |
| `hex` | 十六进制串 | Utf8 |
| `array/T` | T 类型数组 | List(T) |

### 4.3 Window 属性

#### stream — 数据流绑定

```wfs
// 单数据流
window auth_events {
    stream = "syslog"
    ...
}

// 多数据流（要求 schema 兼容）
window fw_events {
    stream = ["firewall", "netflow"]
    ...
}
```

- 无 `stream` 属性的 window 仅作为 `yield` 目标（不订阅任何数据流）。

#### time — 时间字段

```wfs
window auth_events {
    time = event_time    // 必须指向 fields 中 time 类型的字段
    ...
}
```

- `over > 0` 时 `time` 必选。

#### over — 数据保留时长

```wfs
// 时序窗口：保留 5 分钟内的事件
window auth_events {
    over = 5m
    ...
}

// 静态集合：无过期，用于维度表/黑名单
window ip_blocklist {
    over = 0
    fields {
        ip: ip
        category: chars
    }
}
```

### 4.4 带点字段名

WFS 支持包含 `.` 的字段名（兼容 WPL 产出的嵌套字段），使用反引号包裹：

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

在 `.wfl` 中引用这类字段时使用下标形式：`alias["detail.sha256"]`。

### 4.5 完整示例

```wfs
window auth_events {
    stream = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window dns_query {
    stream = "dns"
    time = event_time
    over = 5m

    fields {
        sip: ip
        domain: chars
        query_id: chars
        event_time: time
    }
}

window dns_response {
    stream = "dns"
    time = event_time
    over = 5m

    fields {
        query_id: chars
        rcode: digit
        event_time: time
    }
}

window ip_blocklist {
    over = 0
    fields {
        ip: ip
        threat_level: chars
        category: chars
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        domain: chars
        fail_count: digit
        port_count: digit
        threat: chars
        message: chars
    }
}
```

---

## 5. 检测规则 (.wfl)

### 5.1 规则结构

每条规则由以下部分按顺序组成：

```wfl
use "schema.wfs"              // 导入 window 定义

rule <规则名> {
    meta { ... }              // 可选：元数据
    events { ... }            // 必选：事件绑定
    match<key:duration> {     // 必选：匹配逻辑
        on event { ... }      // 必选：事件到达时求值
        on close { ... }      // 可选：窗口关闭时求值
    } -> score(expr)          // 必选：风险评分
    entity(type, id)          // 必选：实体声明
    yield target (...)        // 必选：输出字段
}
```

### 5.2 use — 导入 Window Schema

```wfl
use "security.wfs"
use "dns.wfs"
```

- 路径相对于规则文件所在目录解析。
- 规则中引用的所有 window 必须在导入的 `.wfs` 中定义。

### 5.3 meta — 规则元数据

```wfl
rule brute_force {
    meta {
        description = "Login failures followed by port scan from same IP"
        mitre       = "T1110, T1046"
    }
    ...
}
```

`meta` 块可选，用于标注规则的描述、MITRE ATT&CK 映射等信息。

### 5.4 events — 事件绑定

`events` 块声明规则关注的事件源，每个事件源包含一个别名和对应的 window，以及可选的过滤条件。

```wfl
events {
    <别名> : <window名> [&& <过滤表达式>]
    ...
}
```

**示例：**

```wfl
events {
    fail : auth_events && action == "failed"
    scan : fw_events
}
```

**语义：**

- 别名在规则内必须唯一。
- window 必须在导入的 `.wfs` 中定义。
- 过滤表达式中裸字段名直接解析为该 window 的字段（如 `action` 解析为 `auth_events.action`）。
- 过滤表达式支持比较、逻辑运算和 `in`/`not in`。

**过滤条件示例：**

```wfl
// 等值过滤
events { fail : auth_events && action == "failed" }

// 布尔字段
events { e : endpoint && active == true }

// in 列表
events { e : fw_events && action in ("deny", "drop", "reject") }

// not in
events { e : web_logs && method not in ("GET", "HEAD") }

// 字符串函数（L2 已实现）
events { ps : endpoint_events && contains(cmd, "powershell") }
events { ps : endpoint_events && contains(lower(process), "powershell") }
```

### 5.5 match — 匹配逻辑

`match` 是规则的核心，定义按什么 key 分组、在什么时间窗口内、用什么条件判定命中。

#### 语法

```
match< [key1, key2, ...] : duration > {
    on event { ... }
    [on close { ... }]
}
```

#### match key — 分组键

```wfl
match<:5m> { ... }               // 无 key（全局状态）
match<sip:5m> { ... }            // 单 key
match<sip,dport:5m> { ... }      // 复合 key
match<fail.sip:5m> { ... }       // 限定名 key
```

- key 为空时，所有事件共享同一状态机实例。
- key 可使用限定名消歧（如 `fail.sip`）。
- 多事件源字段名不同时，需使用 `key { ... }` 显式映射（L2）。

#### duration — 时间窗口

```wfl
match<sip:5m> { ... }    // 5 分钟滑动窗口
match<sip:1h> { ... }    // 1 小时滑动窗口
match<sip:30s> { ... }   // 30 秒滑动窗口
```

支持的时间单位：`s`（秒）、`m`（分钟）、`h`（小时）、`d`（天）。

#### on event — 事件到达求值

每个 step 是一条管道（pipe chain），描述"对哪个事件源的什么字段做什么聚合，然后和什么值比较"。

```
<source>[.field] [&& guard] | [transform |] measure cmp_op threshold ;
```

**单步阈值：**

```wfl
on event {
    fail | count >= 3;
}
```

含义：当 `fail` 事件的计数达到 3 时，此步骤命中。

**多步时序关联：**

```wfl
on event {
    fail | count >= 3;                       // 步骤 1：至少 3 次失败
    scan.dport | distinct | count > 10;      // 步骤 2：扫描超过 10 个端口
}
```

多步之间是**顺序关系**：步骤 1 命中后才开始评估步骤 2。

**带过滤条件的步骤：**

```wfl
on event {
    resp && rcode != 0 | count >= 5;
}
```

`&&` 后的条件（guard）在聚合前过滤事件。Guard 支持比较、逻辑运算、`in`/`not in`，以及字符串函数调用（`contains`/`lower`/`upper`/`len`）。

```wfl
// 函数调用作为 guard
on event {
    proc && contains(cmd, "powershell") | count >= 1;
}

// 嵌套函数 + in 列表
on event {
    conn && lower(proto) in ("tcp", "udp") | count >= 10;
}
```

**OR 分支：**

```wfl
on event {
    a | count >= 3 || b | count >= 5;
}
```

用 `||` 分隔多个分支，**任一命中**即满足该步骤。未命中分支的字段值为 `null`。

#### 聚合操作

**转换（Transform）：**

| 转换 | 含义 |
|------|------|
| `distinct` | 对字段值去重 |

**度量（Measure）：**

| 度量 | 含义 | 要求 |
|------|------|------|
| `count` | 计数 | source 级别（不带字段） |
| `sum` | 求和 | 字段须为 `digit` 或 `float` |
| `avg` | 平均值 | 字段须为 `digit` 或 `float` |
| `min` | 最小值 | 字段须为可排序类型 |
| `max` | 最大值 | 字段须为可排序类型 |

**管道式写法示例：**

```wfl
fail | count >= 3;                        // 事件计数
scan.dport | distinct | count > 10;       // 去重后计数
e.bytes | sum >= 10000;                   // 字段求和
e.latency | avg > 500;                   // 平均值
```

#### on close — 窗口关闭求值

`on close` 在窗口关闭时（timeout/flush/eos）求值一次，用于**缺失检测**等场景。

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

含义：在 30 秒窗口内，有 DNS 请求但无对应响应（超时关闭时响应计数为 0）。

**关闭原因 (`close_reason`)：**

| 值 | 含义 |
|----|------|
| `"timeout"` | 窗口到期 |
| `"flush"` | 显式 flush（如热加载） |
| `"eos"` | 输入流结束 |

- `close_reason` 仅在 `on close` 中可用，在 `on event` 中引用会编译错误。
- 若省略 `on close`，窗口关闭时视为恒满足。

**命中判定公式：**

```
最终命中 = event_ok && close_ok
```

- `event_ok`：`on event` 所有步骤都命中。
- `close_ok`：若有 `on close` 则为其判定结果，否则为 `true`。

### 5.6 score — 风险评分

每条规则产出单一风险分（范围 `[0, 100]`）。

**简洁写法：**

```wfl
} -> score(70.0)
```

**表达式写法：**

```wfl
} -> score(if count(fail) > 10 then 90.0 else 70.0)
```

**算术写法：**

```wfl
} -> score(50.0 + 20.0)
```

- `score` 超出 `[0, 100]` 按运行时策略处理（默认 clamp）。

### 5.7 entity — 实体声明

每条规则**必须**声明一个实体键，标识规则检测的对象。

```wfl
entity(<实体类型>, <标识表达式>)
```

**示例：**

```wfl
entity(ip, fail.sip)               // IP 实体
entity(user, login.uid)            // 用户实体
entity(host, e.host_id)            // 主机实体
```

- `entity_type` 建议使用稳定字面量（`ip`/`user`/`host`/`process`）。
- `entity_id` 允许引用当前上下文字段。
- 系统自动注入 `entity_type` 和 `entity_id` 到输出。

### 5.8 join — 外部关联（L2）

`join` 用于在 match 命中后、输出前，将外部维表数据关联到当前告警上下文。固定为 LEFT JOIN 语义：无匹配行时告警仍正常输出（缺少 join 字段可能导致引用失败）。

```wfl
join <右表window> <模式> on <左侧字段> == <右表window>.<右侧字段>
```

#### 模式选择

| 模式 | 语法 | 语义 |
|------|------|------|
| `snapshot` | `join w snapshot on ...` | 使用右表当前最新版本，找第一行匹配 |
| `asof` | `join w asof on ...` | 按事件时间回看，找**最近一行** `ts <= event_time` |
| `asof within` | `join w asof within 1h on ...` | 同 asof，但只在 `within` 时间窗口内回看 |

#### snapshot 示例

```wfl
rule brute_force_enrich {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
    } -> score(70.0)
    join geo_lookup snapshot on sip == geo_lookup.ip
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        country = geo_lookup.country,
        message = fmt("{} brute force from {}", fail.sip, geo_lookup.country)
    )
}
```

`geo_lookup` 是一个维表 window。命中后从中查找 `ip == sip` 的行，将 `country` 等字段注入上下文。

#### asof 示例

```wfl
rule threat_intel_match {
    events {
        conn : fw_events
    }
    match<sip:5m> {
        on event {
            conn | count >= 10;
        }
    } -> score(conn_risk.risk)
    join conn_risk asof within 24h on sip == conn_risk.ip
    entity(ip, sip)
    yield security_alerts (
        sip = sip,
        risk = conn_risk.risk,
        message = fmt("{} matched threat intel (risk={})", sip, conn_risk.risk)
    )
}
```

`asof` 模式根据事件时间在 `conn_risk` 表中找到最近一条 `ts <= event_time` 且 `ip` 匹配的行。`within 24h` 限制只回看 24 小时内的数据，超出范围视为未命中。

#### 使用说明

- **右表要求**：`asof` 模式要求右表 window 声明了 `time` 字段；`snapshot` 模式无此要求。
- **字段引用**：join 引入的字段在 yield/score/entity 中以 `window_name.field` 限定名引用（如 `geo_lookup.country`）。裸字段名（如 `country`）仅在与已有字段不冲突时可用。
- **多 join**：多个 join 按声明顺序执行，后续 join 可引用前序 join 新增字段。
- **on close 路径**：close 触发的 asof join 使用该匹配实例最后处理事件的时间（非全局水位），确保不会"前看"到实例生命周期之外的数据。

### 5.9 yield — 输出

`yield` 声明规则命中后输出哪些字段到目标 window。

```wfl
yield <目标window> (
    <字段> = <表达式>,
    ...
)
```

**示例：**

```wfl
yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    port_count = distinct(scan.dport),
    message = fmt("{}: brute force then port scan detected", fail.sip)
)
```

**规则：**

- 目标 window 必须存在且 `stream` 为空（纯输出 window）。
- yield 字段必须是目标 window `fields` 的子集（名称和类型匹配）。
- 未覆盖的非系统字段值为 `null`。
- 系统自动注入：`rule_name`、`emit_time`、`score`、`entity_type`、`entity_id`、`close_reason`。
- **禁止**在 yield 中手工赋值系统字段。

**字段引用方式：**

```wfl
yield out (
    a = sip,                      // 裸字段名
    b = fail.sip,                 // 限定名
    c = e["detail.sha256"]        // 带点字段名（下标形式）
)
```

### 5.10 limits — 资源预算（L2）

`limits { ... }` 为规则声明运行时资源上界，防止单条规则耗尽系统内存或产生过量告警。

> 省略 `limits` 块时编译器发出 Warning（未来版本可能升级为编译错误）。

#### 语法

```wfl
limits {
    max_memory = "50MB";          // 可选，规则总状态内存上限
    max_instances = 10000;     // 可选，活跃实例（key 数）上限
    max_throttle = "100/60s";   // 可选，告警产出速率上限
    on_exceed = "throttle";      // 可选，超限时动作（默认 throttle）
}
```

#### 字段说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_memory` | STRING | 无限制 | 所有活跃实例的估算总内存上限（如 `"50MB"`） |
| `max_instances` | INTEGER | 无限制 | 活跃实例数上限；超限时新 key 无法创建实例 |
| `max_throttle` | STRING | 无限制 | 滑动窗口内最大告警数（格式 `"count/duration"`，如 `"100/60s"`） |
| `on_exceed` | STRING | `"throttle"` | 超限动作：`throttle` / `drop_oldest` / `fail_rule` |

#### on_exceed 动作

| 动作 | 行为 |
|------|------|
| `throttle` | 丢弃当前触发（事件路径：重置实例状态；关闭路径：抑制告警输出），不影响后续事件 |
| `drop_oldest` | 淘汰 `created_at` 最早的实例后继续（仅对 `max_instances` / `max_memory` 有效；对 `max_throttle` 等效 `throttle`） |
| `fail_rule` | 标记规则永久失败，后续所有事件直接忽略，不可恢复 |

#### 运行时行为

- **`max_instances`**：新实例创建前检查活跃实例数。
- **`max_memory`**：每次事件到达时检查（包括已有实例增长和即将创建的新实例基础开销）。
- **`max_throttle`**：事件路径（match 命中）和关闭路径（timeout / flush / eos）均检查，共享同一滑动窗口计数器。

#### 示例

```wfl
rule brute_force {
    meta { lang = "2.1" }
    events { fail: auth_fail }

    match<sip:5m> {
        on event {
            fail where count >= 10 -> "brute"
        }
    }
    -> score(count)
    entity(ip, sip)
    yield alert_out (
        src_ip = sip
    )

    limits {
        max_instances = 50000;
        max_throttle = "200/60s";
        on_exceed = "throttle";
    }
}
```

### 5.11 完整规则示例

#### 阈值检测

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

#### 多步时序关联

```wfl
use "security.wfs"

rule brute_force_then_scan {
    meta {
        description = "Login failures followed by port scan from same IP"
        mitre       = "T1110, T1046"
    }
    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        port_count = distinct(scan.dport),
        message = fmt("{}: brute force then port scan detected", fail.sip)
    )
}
```

#### 缺失检测（A → NOT B）

```wfl
use "dns.wfs"

rule dns_no_response {
    events {
        req  : dns_query
        resp : dns_response
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

#### OR 分支（任一命中）

```wfl
use "security.wfs"

rule suspicious_activity {
    events {
        a : auth_events && action == "failed"
        b : fw_events && action == "deny"
    }
    match<sip:5m> {
        on event {
            a | count >= 3 || b | count >= 5;
        }
    } -> score(60.0)
    entity(ip, a.sip)
    yield security_alerts (
        sip = a.sip,
        message = fmt("{} suspicious activity", a.sip)
    )
}
```

#### 带 on close 的阈值检测

```wfl
use "security.wfs"

rule brute_force_confirmed {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= $FAIL_THRESHOLD;
        }
        on close {
            fail | count >= 1;
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

#### 字符串函数在 guard 中过滤（L2 已实现）

```wfl
use "security.wfs"

rule powershell_activity {
    events {
        proc : endpoint_events && contains(lower(cmd), "powershell")
    }
    match<host_id:10m> {
        on event {
            proc | count >= 3;
        }
    } -> score(75.0)
    entity(host, proc.host_id)
    yield security_alerts (
        message = fmt("{} powershell activity detected", proc.host_id)
    )
}
```

```wfl
use "security.wfs"

rule protocol_anomaly {
    events {
        conn : fw_events
    }
    match<sip:5m> {
        on event {
            conn && lower(proto) in ("tcp", "udp") | count >= 100;
        }
    } -> score(60.0)
    entity(ip, conn.sip)
    yield security_alerts (
        sip = conn.sip,
        message = fmt("{} high volume on standard protocols", conn.sip)
    )
}
```

---

## 6. 运行时配置 (fusion.toml)

### 6.1 完整配置参考

```toml
# ── 告警输出（Connector-based sink 路由） ──
sinks = "sinks"                              # 指向 sinks/ 配置目录

# ── 服务器 ──
[server]
listen = "tcp://127.0.0.1:9800"     # TCP 监听地址

# ── 运行时 ──
[runtime]
executor_parallelism = 2             # 执行线程并发度
rule_exec_timeout = "30s"            # 单条规则执行超时
schemas = "schemas/*.wfs"            # Schema 文件（支持 glob）
rules   = "rules/*.wfl"             # 规则文件（支持 glob）

# ── 窗口全局默认值 ──
[window_defaults]
evict_interval = "30s"               # 淘汰检查周期
max_window_bytes = "256MB"           # 单窗口内存上限
max_total_bytes = "2GB"              # 全局内存上限
evict_policy = "time_first"          # 淘汰策略：time_first | memory_first
watermark = "5s"                     # 水印延迟
allowed_lateness = "0s"              # 迟到容忍
late_policy = "drop"                 # 迟到策略：drop | accumulate

# ── 单窗口覆盖（按 window 名） ──
[window.auth_events]
mode = "local"                       # 分布模式
max_window_bytes = "256MB"           # 覆盖全局默认值
over_cap = "30m"                     # 保留上限

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"

# ── 变量（可在 .wfl 中引用） ──
[vars]
FAIL_THRESHOLD = "3"
SCAN_THRESHOLD = "10"
```

### 6.2 配置要点

#### Glob 模式

`schemas` 和 `rules` 支持 glob 模式，自动扫描匹配文件：

```toml
schemas = "schemas/*.wfs"             # 当前目录下所有 .wfs
schemas = "schemas/**/*.wfs"          # 递归子目录
rules   = "rules/*.wfl"
```

#### 窗口覆盖

`[window.<name>]` 可以为特定 window 覆盖全局默认值：

```toml
[window_defaults]
max_window_bytes = "256MB"            # 全局默认

[window.high_volume_events]
max_window_bytes = "1GB"              # 高流量窗口单独设置
over_cap = "1h"
```

#### 告警 Sink

告警输出通过 Connector-based sink 路由系统配置，使用 `sinks/` 目录：

```toml
sinks = "sinks"                              # 指向 sinks/ 配置目录
```

`sinks/` 目录结构：
- `defaults.toml` — 全局默认值
- `sink.d/` — Connector 定义（sink 类型 + 参数）
- `business.d/` — 业务路由组（按 yield-target 匹配）
- `infra.d/` — 基础设施组（default / error）

输出格式为 JSONL（每行一条 JSON 告警记录）。

### 6.3 变量预处理

`[vars]` 中定义的变量可在 `.wfl` 中引用，在编译前进行文本替换：

```toml
[vars]
FAIL_THRESHOLD = "5"
SCAN_THRESHOLD = "10"
```

```wfl
match<sip:5m> {
    on event {
        fail | count >= $FAIL_THRESHOLD;            // → count >= 5
    }
}
```

**语法：**

| 写法 | 含义 |
|------|------|
| `$VAR` | 直接替换；变量未定义则编译错误 |
| `${VAR:default}` | 变量未定义时使用默认值 |

- 预处理发生在解析之前（纯文本替换）。

---

## 7. 表达式与函数

### 7.1 运算符

**算术运算符：**

| 运算符 | 含义 | 操作数类型 |
|--------|------|-----------|
| `+` | 加 | digit/float |
| `-` | 减 | digit/float |
| `*` | 乘 | digit/float |
| `/` | 除 | digit/float |
| `%` | 取模 | digit/float |

**比较运算符：**

| 运算符 | 含义 | 要求 |
|--------|------|------|
| `==` | 等于 | 两侧类型一致 |
| `!=` | 不等于 | 两侧类型一致 |
| `<` | 小于 | digit/float |
| `>` | 大于 | digit/float |
| `<=` | 小于等于 | digit/float |
| `>=` | 大于等于 | digit/float |

**逻辑运算符：**

| 运算符 | 含义 | 要求 |
|--------|------|------|
| `&&` | 逻辑与 | 两侧须为 bool |
| `\|\|` | 逻辑或 | 两侧须为 bool |

**成员判定：**

```wfl
action in ("failed", "locked", "expired")
action not in ("success", "mfa_pass")
```

**运算符优先级（从高到低）：**

1. 一元 `-`
2. `*` `/` `%`
3. `+` `-`
4. `==` `!=` `<` `>` `<=` `>=` `in` `not in`
5. `&&`
6. `||`

### 7.2 字面量

| 类型 | 示例 |
|------|------|
| 整数 | `3`、`100`、`0` |
| 浮点数 | `70.0`、`3.14` |
| 字符串 | `"failed"`、`"hello world"` |
| 布尔值 | `true`、`false` |
| 时长 | `5m`、`30s`、`1h`、`7d` |

### 7.3 字段引用

| 形式 | 示例 | 说明 |
|------|------|------|
| 裸字段名 | `sip` | events 过滤中可直接使用 |
| 限定名 | `fail.sip` | 别名 + 字段名 |
| 下标形式 | `e["detail.sha256"]` | 带点字段名 |

### 7.4 内置函数

#### 聚合函数（L1）

| 函数 | 签名 | 说明 |
|------|------|------|
| `count` | `count(alias)` → digit | 事件计数 |
| `sum` | `sum(alias.field)` → digit/float | 求和 |
| `avg` | `avg(alias.field)` → float | 平均值 |
| `min` | `min(alias.field)` → T | 最小值 |
| `max` | `max(alias.field)` → T | 最大值 |
| `distinct` | `distinct(alias.field)` → digit | 去重计数 |

**聚合双写法（语义等价）：**

```wfl
// 管道式 — 推荐在 match 中使用
scan.dport | distinct | count > 10;

// 函数式 — 推荐在 yield/score 中使用
port_count = distinct(scan.dport)
```

#### 格式化函数（L1）

```wfl
fmt("{} failed {} times from {}", fail.username, count(fail), fail.sip)
```

- `{}` 为位置占位符，数量必须与参数数量一致。
- 参数可为任意类型。
- 返回 `chars`。

#### 字符串函数（已实现）

以下字符串函数可在 events 过滤、match guard、score 表达式和 entity 表达式中使用。

| 函数 | 签名 | 说明 |
|------|------|------|
| `contains` | `contains(haystack, needle)` → bool | 子串包含判定 |
| `lower` | `lower(field)` → chars | 转小写 |
| `upper` | `upper(field)` → chars | 转大写 |
| `len` | `len(field)` → digit | 字符串长度 |

**使用示例：**

```wfl
// events 过滤中使用 contains
events {
    ps : endpoint_events && contains(cmd, "powershell")
}

// guard 中使用 lower + in 组合
on event {
    conn && lower(proto) in ("tcp", "udp") | count >= 10;
}

// guard 中嵌套函数
on event {
    ps && contains(lower(process), "powershell") | count >= 1;
}

// score 表达式中使用
} -> score(if len(fail.username) > 20 then 80.0 else 60.0)
```

函数支持嵌套调用，例如 `contains(lower(field), "pattern")` 先将字段值转小写再做子串判定。

### 7.5 条件表达式（L2，设计中）

```wfl
if count(fail) > 10 then 90.0 else 70.0
```

- 条件须为 `bool`。
- 两个分支的类型须一致。

---

## 8. 规则契约测试

契约测试（Contract Test）用于在 CI 中前置验证规则的逻辑正确性，通过小样本输入和预期断言确保规则行为不退化。

### 8.1 契约语法

```wfl
contract <测试名> for <规则名> {
    given {
        // 注入测试事件
        row(<别名>, <字段> = <值>, ...);
        tick(<时长>);     // 推进测试时钟
        ...
    }
    expect {
        // 断言输出
        hits == <数量>;
        hit[<索引>].score == <分数>;
        hit[<索引>].entity_id == <值>;
        hit[<索引>].field("<字段名>") == <值>;
        ...
    }
    options {
        close_trigger = timeout;    // timeout | flush | eos
        eval_mode = strict;         // strict | lenient
    }
}
```

### 8.2 given — 输入事件

```wfl
given {
    row(fail, action = "failed", sip = "1.2.3.4");
    row(fail, action = "failed", sip = "1.2.3.4");
    row(fail, action = "failed", sip = "1.2.3.4");
    tick(6m);    // 推进 6 分钟，触发窗口关闭
}
```

- `row(alias, ...)` 的 `alias` 必须在目标规则 `events` 中声明。
- 字段名允许 `IDENT` 或 `STRING`（带点字段名用 `"detail.sha256"`）。
- `row` 按声明顺序注入；缺失字段按 `null` 处理。
- `tick(dur)` 推进测试时钟并触发窗口关闭。

### 8.3 expect — 输出断言

```wfl
expect {
    hits == 1;                              // 告警条数
    hit[0].score == 70.0;                   // 第 1 条告警的分数
    hit[0].close_reason == "timeout";       // 关闭原因
    hit[0].entity_type == "ip";             // 实体类型
    hit[0].entity_id == "1.2.3.4";          // 实体 ID
    hit[0].field("domain") == "evil.test";  // 自定义字段值
}
```

- `hits` 断言输出条数。
- `hit[i]` 访问第 i 条输出（0-based）。
- 比较运算符支持 `==`、`!=`、`<`、`>`、`<=`、`>=`。

### 8.4 完整示例

```wfl
// 暴力破解检测 — 基本命中
contract brute_test for brute_force {
    given {
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

// DNS 无响应 — 超时检测
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

## 9. 测试数据生成 (wf-datagen)

`wf-datagen` 是独立的测试数据生成工具，使用 `.wfg` 场景文件描述数据生成策略。

### 9.1 Scenario 文件 (.wfg)

```wfg
use "windows/security.wfs"
use "rules/brute_force.wfl"

scenario brute_force_load seed 42 {

    time "2026-02-18T00:00:00Z" duration 30m
    total 200000

    // 基础流量
    stream fail : auth_events 200/s {
        sip    = ipv4(500)
        action = "failed"
    }

    stream success : auth_events 400/s {
        sip    = ipv4(500)
        action = "success"
    }

    // 模式注入
    inject for brute_force on [fail] {
        hit       5%  count_per_entity=5 within=2m;
        near_miss 3%  count_per_entity=2 within=2m;
    }

    // 时序扰动
    faults {
        out_of_order 2%;
        late         1%;
        duplicate    0.5%;
        drop         0.2%;
    }

    // 期望结果
    oracle {
        time_tolerance  = 1s;
        score_tolerance = 0.01;
    }
}
```

### 9.2 stream — 流定义

```wfg
stream <别名> : <window名> <速率> {
    <字段> = <生成表达式>
    ...
}
```

- `别名` 必须对应 `.wfl` 规则中 `events` 的别名。
- `window名` 必须与规则中该别名绑定的 window 一致。
- `速率` 格式：`N/s`、`N/m`、`N/h`。

**生成函数：**

| 函数 | 参数 | 适用类型 | 说明 |
|------|------|----------|------|
| *(字面量)* | 直接写值 | 全部 | `action = "failed"` |
| `ipv4` | `pool` | ip/chars | `ipv4(500)` — 从 500 个 IP 池随机取 |
| `ipv6` | `pool` | ip/chars | `ipv6(100)` |
| `pattern` | 模板 | chars | `pattern("user_{}")` — `{}` 递增 |
| `enum` | 值列表 | chars | `enum("login", "logout")` |
| `range` | `min, max` | digit/float | `range(0, 100)` |

未声明的字段按类型默认策略随机生成。

### 9.3 inject — 模式注入

```wfg
inject for <规则名> on [<流别名>, ...] {
    hit       <百分比>  <参数>=<值> ...;
    near_miss <百分比>  <参数>=<值> ...;
    non_hit   <百分比>  <参数>=<值> ...;
}
```

| 模式 | 语义 | 期望输出 |
|------|------|----------|
| `hit` | 构造"应命中"事件序列 | 必须出现告警 |
| `near_miss` | 构造"接近但不命中"事件序列 | 不应出现告警 |
| `non_hit` | 无关事件 | 不应出现告警 |

### 9.4 faults — 时序扰动

```wfg
faults {
    out_of_order 2%;    // 交换相邻事件到达顺序
    late         1%;    // 推迟到 watermark 之后
    duplicate    0.5%;  // 重复发送
    drop         0.2%;  // 生成但不发送
}
```

各项百分比之和不得超过 100%。

### 9.5 CLI 命令

```bash
# 生成测试数据
wf-datagen gen \
    --scenario tests/brute_force_load.wfg \
    --format jsonl \
    --out out/

# 一致性校验
wf-datagen lint tests/brute_force_load.wfg

# 对拍验证
wf-datagen verify \
    --actual out/actual_alerts.jsonl \
    --expected out/brute_force_load.oracle.jsonl \
    --meta out/brute_force_load.oracle.meta.json
```

### 9.6 端到端流程

```
.wfg + .wfs + .wfl
       │
  wf-datagen gen           → events.jsonl + oracle.jsonl
       │
  wf run --replay          → actual_alerts.jsonl
       │
  wf-datagen verify        → verify_report.json
```

---

## 10. 告警输出

### 10.1 输出格式

告警以 JSONL 格式写入文件，每行一条 JSON 记录。

### 10.2 系统字段

每条告警自动包含以下系统字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `rule_name` | chars | 规则名称 |
| `score` | float | 风险评分 [0, 100] |
| `entity_type` | chars | 实体类型 |
| `entity_id` | chars | 实体标识 |
| `close_reason` | chars? | 窗口关闭原因（nullable） |
| `emit_time` | time | 告警产出时间 |
| `alert_id` | chars | 确定性告警 ID |

### 10.3 alert_id 生成

```
alert_id = sha256(rule_name + scope_key + window_range)
```

确定性 ID 可用于下游去重。

### 10.4 示例告警

```json
{
  "rule_name": "brute_force",
  "score": 70.0,
  "entity_type": "ip",
  "entity_id": "10.0.0.1",
  "close_reason": "timeout",
  "alert_id": "a1b2c3...",
  "fired_at": "2026-02-18T10:05:00Z",
  "sip": "10.0.0.1",
  "fail_count": 5,
  "message": "10.0.0.1 brute force detected"
}
```

---

## 11. 运行引擎

### 11.1 启动

```bash
wf run --config fusion.toml
```

引擎启动流程：

1. 加载并验证 `fusion.toml`
2. 解析所有 `.wfs` Schema 文件
3. 解析并编译所有 `.wfl` 规则文件（含变量替换）
4. 创建窗口缓冲区和规则执行器
5. 启动 TCP 监听
6. 启动事件调度循环
7. 等待 `Ctrl+C` 信号

### 11.2 数据接入

引擎通过 TCP 接收事件，帧格式为：

```
[4 字节长度][stream_name][Arrow IPC payload]
```

事件以 Apache Arrow RecordBatch 格式传输。

### 11.3 事件驱动执行

```
TCP → Receiver → Router → WindowStore → MatchEngine → YieldWriter → AlertSink
```

- **Receiver**：解码 Arrow IPC 帧。
- **Router**：按 stream 分发到订阅该 stream 的 window。
- **WindowStore**：按 watermark 过滤，维护时间有序缓冲。
- **MatchEngine**：CEP 状态机，逐事件驱动步骤推进。
- **YieldWriter**：生成告警记录。
- **AlertSink**：写入 JSONL 文件。

### 11.4 窗口管理

- **Watermark**：事件时间水印，延迟 watermark 之外的事件按 `late_policy` 处理。
- **淘汰**：按 `evict_interval` 周期检查，淘汰超过 `over` 时长的事件。
- **内存保护**：两阶段淘汰（TTL → 全局内存预算），防止内存溢出。

### 11.5 热加载

```bash
# 修改 .wfl 或 [vars] 后，引擎自动重新加载（Drop 策略）
# .wfs 变更需要重启
```

热加载流程：

1. 读取新 `.wfl` + `[vars]`
2. 语法/语义检查
3. 编译 RulePlan
4. 原子替换规则集（丢弃在途状态机）

### 11.6 优雅停机

收到 `Ctrl+C` 后，引擎按 LIFO 顺序停机：

1. 停止接收新事件
2. 等待在途事件处理完成
3. 关闭调度器
4. 刷写告警
5. 关闭资源

---

## 12. 能力分层参考

WFL 功能按 L1/L2/L3 分层，渐进式开放。

### L1（默认，MVP — 已实现）

| 特性 | 说明 |
|------|------|
| `use` / `rule` / `meta` / `events` | 基础规则结构 |
| `match<key:dur>` 单/复合 key | 滑动窗口匹配 |
| 多步序列 | 顺序关联 |
| `on close` | 窗口关闭求值 |
| OR 分支（`\|\|`） | 任一命中 |
| `count`/`sum`/`avg`/`min`/`max`/`distinct` | 聚合函数 |
| `yield target (...)` | 显式输出 |
| `-> score(expr)` | 风险评分 |
| `entity(type, id)` | 实体声明 |
| `fmt()` | 格式化 |
| `$VAR` / `${VAR:default}` | 变量预处理 |
| `contains`/`lower`/`upper`/`len` | 字符串函数（guard/score/entity 表达式） |

### L2（增强 — 部分实现）

**已实现：**

| 特性 | 状态 | 说明 |
|------|:----:|------|
| `contains(haystack, needle)` | 已实现 | 子串包含判定，可用于 guard/score/entity 表达式 |
| `lower(field)` / `upper(field)` | 已实现 | 大小写转换，支持嵌套调用 |
| `len(field)` | 已实现 | 字符串长度 |
| `join` + `snapshot`/`asof` | 已实现 | 外部关联（snapshot 及 asof 时点模式，含 within 窗口） |
| `limits { ... }` | 已实现 | 资源预算（max_memory / max_instances / max_throttle） |

**设计中（尚未实现）：**

| 特性 | 说明 |
|------|------|
| `baseline(expr, dur)` | 基线偏离 |
| `window.has(field)` | 集合判定 |
| `derive { x = expr; ... }` | 特征派生 |
| `score { item = expr @ weight; ... }` | 分项评分 |
| `if/then/else` | 条件表达式 |
| `regex_match(field, pattern)` | 正则匹配判定 |
| `time_diff`/`time_bucket` | 时间函数 |
| `hit(cond)` | 条件命中映射 |
| `coalesce`/`try` | 空值兜底函数 |
| `key { logical = alias.field }` | 显式 key 映射 |
| `yield target@vN` | 输出契约版本 |

### L3（高级 — 设计中）

| 特性 | 说明 |
|------|------|
| `\|>` 多级管道 | 级联规则 |
| `conv { ... }` | 结果集变换 |
| `tumble` | 固定间隔窗口 |
| `session(gap)` | 会话窗口 |
| `collect_set`/`collect_list`/`first`/`last` | 集合函数 |
| `stddev`/`percentile` | 统计函数 |
| 增强 `baseline(expr, dur, method)` | 多方法基线 |

---

## 附录 A. 类型系统

### 类型规则

所有类型检查在编译期完成。

| 规则 | 说明 |
|------|------|
| `sum`/`avg` 参数须为 `digit` 或 `float` | 数值聚合约束 |
| `min`/`max` 参数须为可排序类型 | `digit`/`float`/`time`/`chars` |
| `distinct` 参数须为列投影 | `alias.field`，不接受 `alias` |
| `count` 参数须为集合级别 | `alias`，不接受 `alias.field` |
| `==`/`!=` 两侧类型须一致 | 跨类型编译错误 |
| `>`/`>=`/`<`/`<=` 须为 `digit` 或 `float` | 不同数值类型自动提升为 `float` |
| `&&`/`\|\|` 须为 `bool` | 非布尔编译错误 |
| `fmt()` 占位符数量须与参数数量一致 | 编译期检查 |
| `contains(s, pat)` 参数须为 `chars`/`ip`/`hex` | 返回 `bool` |
| `lower(s)`/`upper(s)` 参数须为 `chars` | 返回 `chars` |
| `len(s)` 参数须为 `chars`/`ip`/`hex` | 返回 `digit` |

### 类型映射

| WFL | Arrow |
|-----|-------|
| `chars` | `Utf8` |
| `digit` | `Int64` |
| `float` | `Float64` |
| `bool` | `Boolean` |
| `time` | `Timestamp(Nanosecond, None)` |
| `ip` | `Utf8` |
| `hex` | `Utf8` |
| `array/T` | `List(T 的 Arrow 映射)` |

---

## 附录 B. 语义约束速查

### Events 约束

- 别名唯一。
- window 必须在 `.wfs` 中定义。
- 过滤字段必须存在于对应 window 中。

### Match 约束

- `duration > 0`。
- step 必须显式声明 source（不允许空 source）。
- `on event` 必选且至少一条 step。
- `on close` 可选；省略时关闭阶段恒为 true。
- `close_reason` 仅 `on close` 中可用。

### Yield 约束

- 目标 window 必须存在且 `stream` 为空。
- 字段须为目标 window 的子集。
- 禁止手工赋值系统字段（`score`/`entity_type`/`entity_id`）。
- 未覆盖字段值为 `null`。

### 字段引用解析优先级

events 别名 → match 步骤标签 → 聚合函数结果。未找到即编译错误。

### 保留标识符

| 标识符 | 含义 |
|--------|------|
| `_in` | `\|>` 后续 stage 的隐式输入别名（L3） |
| `@name` | `derive` 派生项引用前缀（L2） |
| `close_reason` | 窗口关闭原因上下文字段 |
