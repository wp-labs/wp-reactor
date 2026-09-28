# 外部维表点查（`external()`）

## 适用场景

`external()` 用于**按事件逐条点查外部维表**：事件来一条，就用该事件的字段去外部存储查一次。

适合的场景：

- 维表太大、不适合加载进引擎内存（亿级弱口令库、威胁情报 IP/域名库、企业泄露凭据等）
- 维表需要在外部实时更新，不希望重建窗口
- 「命中即告警」或「命中即富化」的逐条判定

不适合的场景：

- 需要窗口累计后再判断（用 `match`）
- 维表规模不大（万级以内），可以整体加载进内存（用 `join`，见下文对比）

> 维表存储、连接、缓存由 `knowdb.toml` 统一声明；`wfusion.toml` 不需要额外的 `external` 配置段。

## 基本语法与约束

```wfl
on each e where external("password_check", e.password_hash) -> score(75.0)
```

| 项 | 规则 |
|----|------|
| 写法 | `external("服务名", 参数)` |
| 服务名 | **必须是字符串字面量**（不能是字段或表达式） |
| 参数 | 至少一个；参数求值结果需是字符串或数字 |
| 位置 | 只能用于 `on each <别名> where ...`；**不能**放进 `match<key>` 的聚合步骤里 |
| 取反 | 支持 `not` / `!`，可写 `!external("服务名", 参数)` |
| 当前限制 | 多参数调用时**只使用第一个参数** |

`where` 在单条记录上下文中求值，`external()` 的返回值参与该判断，为真则命中当前事件。

正确的放置：

```wfl
on each e where external("ip_blacklist", e.sip) -> score(80.0)
```

错误的放置（`external()` 不能出现在 `match` 的聚合里）：

```wfl
match<sip:5m> {
    on event { e && external("check", e.hash) | count >= 1; }   // ❌
}
```

## 返回值语义

`external()` 的返回值取决于该服务在 `knowdb.toml` 里配置的查询类型，分两类：

| 查询类型 | 命中 | 未命中 |
|----------|------|--------|
| 布尔查询（存在性 / 成员判定） | `true` | `false` |
| 值查询（字段 / KV 查值） | 字符串 | 空 |

使用约定：

- **在 `where` 里做判定时，要用布尔查询**（存在性 / 成员判定），结果才是 `true` / `false`。
- **值查询用于 `yield` 富化**（把外部值写进输出字段）；直接用值查询做 `where` 判定不会命中。
- `not` / `!` 只对布尔结果生效。

## 配置 `knowdb.toml`

`knowdb.toml` 放在工程根目录（与 `wfusion.toml` 同级），wfusion 启动时自动加载，用来配置外部存储连接、结果缓存和 `external()` 的命名查询。

### 最小配置

```toml
version = 2
base_dir = "."

[provider.redis]
connection_uri = "redis://127.0.0.1:6379"

[cache]
enabled = true
capacity = 10000

[fun.password_check]
call = "sismember"
key  = "weak_passwords"
```

### `[provider.redis]` — 存储连接

| 字段 | 类型 | 默认值 | 说明 |
|------|------|:-----:|------|
| `connection_uri` | string | **必填** | 例如 `redis://127.0.0.1:6379` |
| `pool_size` | int | `8` | 连接池大小 |
| `connect_timeout_ms` | int | `3000` | 建连超时（ms） |
| `command_timeout_ms` | int | `100` | 单次命令超时（ms） |

### `[cache]` — 结果缓存

| 字段 | 类型 | 默认值 | 说明 |
|------|------|:-----:|------|
| `enabled` | bool | `true` | 是否启用缓存 |
| `capacity` | int | `1024` | LRU 容量 |
| `ttl_ms` | int | `30000` | 缓存 TTL（ms） |

### `[fun.<name>]` — 命名查询

每个 `[fun.<name>]` 定义一个 `external("<name>", 参数)` 可调用的查询。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|:-----:|------|
| `call` | string | **必填** | `bf_exists` / `sismember` / `hget` / `get` |
| `key` | string | **必填** | 目标键名（`get` 除外，见下） |
| `cache` | bool | `true` | 是否对该查询启用缓存 |
| `ttl_ms` | int | 无 | 覆盖全局 `[cache].ttl_ms` |

`call` 与返回类型：

| `call` | 返回 | 用途 |
|--------|------|------|
| `bf_exists` | 布尔 | Bloom filter 存在性判定 |
| `sismember` | 布尔 | 集合成员判定 |
| `hget` | 字符串 / 空 | 哈希字段查值（富化） |
| `get` | 字符串 / 空 | 简单 KV 查值（富化）；忽略 `key`，把参数直接当键名 |

## 示例：黑 IP / 白 IP 碰撞

### 场景

同一个 IP 既在威胁黑名单里，又在可信白名单里（例如：某漏扫器 IP 被误报进了情报库，或某合作伙伴 IP 上了黑名单）。这类情况需要两种处理：

- **消解**：白名单优先，避免对可信 IP 误报（点查级优先级）。
- **审计**：把碰撞本身报出来，由人去决定该 IP 最终留黑还是留白。

### 准备数据

```bash
redis-cli SADD ip_blacklist 203.0.113.5 198.51.100.9 192.0.2.77
redis-cli SADD ip_whitelist 10.0.2.1 203.0.113.5     # 203.0.113.5 同时在两表 → 碰撞
```

### 配置

```toml
version = 2
base_dir = "."

[provider.redis]
connection_uri = "redis://127.0.0.1:6379"

[cache]
enabled = true
capacity = 10000

[fun.ip_blacklist]
call = "sismember"
key  = "ip_blacklist"

[fun.ip_whitelist]
call = "sismember"
key  = "ip_whitelist"
```

### 方案 A：白名单优先，消解碰撞

黑名单源 IP 告警，但命中白名单则压掉：

```wfl
use "network.wfs"

rule malicious_src_ip {
    events { c : conn_events && c.action == "established" }
    on each c where
        external("ip_blacklist", c.sip)
            && !external("ip_whitelist", c.sip)
        -> score(80.0)
    entity(ip, c.sip)
    yield network_alerts (
        sip = c.sip,
        dip = c.dip,
        alert_type = "malicious_src_ip",
        detail = "source IP in blacklist and not whitelisted"
    )

    limits {
        max_memory = "64MB";
        max_instances = 10000;
        on_exceed = throttle;
    }
}
```

结果：

- `203.0.113.5`（碰撞）：黑名单命中 `true`、白名单命中 `true` → `!true = false` → 整条为假 → **不告警**（白名单赢）。
- `198.51.100.9`（纯黑名单）：`true && !false` → **告警**。

### 方案 B：把碰撞做成治理审计事件

想显式发现碰撞、而不是静默放行，就反过来查：

```wfl
use "network.wfs"

rule ip_list_collision_audit {
    events { c : conn_events }
    on each c where
        external("ip_blacklist", c.sip)
            && external("ip_whitelist", c.sip)      // 两表都命中 = 碰撞
        -> score(30.0)
    entity(ip, c.sip)
    yield security_alerts (
        sip = c.sip,
        alert_type = "ip_list_collision",
        detail = "IP present in both blacklist and whitelist; needs governance"
    )

    limits {
        max_memory = "64MB";
        max_instances = 10000;
        on_exceed = throttle;
    }
}
```

运维闭环：收到碰撞告警后确认——可信则从 `ip_blacklist` 移除，恶意则从 `ip_whitelist` 移除。碰撞清掉后方案 A 自然按预期放行或告警。

### 富化：把「为什么上榜」也带出来（可选）

存在性查询只给布尔值。要输出类别 / 负责人等外部值，用值查询在 `yield` 里取值：

```toml
[fun.ip_blacklist_reason]
call = "hget"
key  = "ip_blacklist_reason"     # HASH: ip -> category

[fun.ip_whitelist_owner]
call = "hget"
key  = "ip_whitelist_owner"      # HASH: ip -> owner
```

```wfl
    yield security_alerts (
        sip = c.sip,
        alert_type = "ip_list_collision",
        blacklist_category = external("ip_blacklist_reason", c.sip),
        whitelist_owner    = external("ip_whitelist_owner", c.sip)
    )
```

要点：

- `yield` 里的值查询未命中时会兜底成空字符串，因此可安全用于输出字段。
- 值查询**不要**用来做 `where` 判定，否则不会命中。

## 故障兜底与注意事项

外部存储不可用（连接失败、命令超时）时的行为：

| 查询类型 | 外部存储不可用时的结果 |
|----------|------------------------|
| 布尔查询 | `false`（判定式宁可漏报，不阻塞规则执行） |
| 值查询 | 空 |

需要注意：

- **白名单场景要额外小心**。布尔查询失败返回 `false`，意味着 `!external("ip_whitelist", ...)` 会变成 `true` —— 当外部存储挂掉时，白名单保护会失效、可能误报。生产上建议对白名单单独监控和降级，或把关键白名单同时放一份到内存托底（见下文 `join ... anti`）。
- **缓存**：布尔 / 值查询共用 `[cache]` 配置。维表在外部更新后，结果会在缓存 TTL（默认 30s）后生效；数据变更敏感的场景可调小 `ttl_ms`。

## 与 `join` 的选择

`external()` 走外部存储，`join`（`snapshot` / `asof` / `anti`）把右表加载进引擎内存。选择依据：

| 维度 | `external()` | `join ... anti / snapshot` |
|------|--------------|-----------------------------|
| 承载规模 | 亿级（外部存储，点查 O(1)） | 万级以内（加载进内存） |
| 更新方式 | 外部存储侧随时改，天然实时 | 按窗口 / 提供方刷新周期 |
| 规则位置 | `on each ... where`（逐条） | `match` / `on each` 之后、`entity` 之前 |
| 外部依赖 | 需要部署外部存储 | 只依赖内存表 |
| 典型用途 | 大情报库点查、命中即告警 | 小维表富化、白名单排除 |

白名单排除用 `join ... anti` 的写法：

```wfl
    join scanner_whitelist anti on c.sip == scanner_whitelist.sip
```

## 验证

```bash
# 规则语法 / 语义检查（`external()` 放进 match 会被拒绝）
wfl lint rules/*.wfl -s "schemas/*.wfs"

# 完整引擎（batch 模式；`external()` 依赖真实外部存储，轻量 test runner 不预加载外部维表）
wfusion batch -c ./wfusion.toml
```

## 参考

- 逐条求值建模：[On Each 与逐条打分](./on-each.md)
- 关联语法：[语言参考 · `join`](./language-reference.md#join)
- 设计背景：[External Function 设计](../design/external-function-design.md)（早期设计稿，具体配置以本文与 `knowdb.toml` 实际用法为准）
