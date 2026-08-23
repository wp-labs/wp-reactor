# Provider Window 设计

## 目标

统一外部数据源（CSV、SQLite、Postgres）接入，规则层通过 `join` 透明访问。Provider 窗口不需要 event buffer 参数（over/mode/max_window_bytes）。

## 配置分工

```
knowdb.toml            wfusion.toml [window.X]     作用
───────────            ──────────────────────       ────
[[tables]]             table = "threat_intel"      指定用哪个 knowdb 表
name = "threat_intel"  query = "SELECT ..."        窗口级别的数据裁剪
columns.by_header      refresh = "5m"              缓存刷新间隔
provider = "postgres"  (无其他参数)                不需要 over/mode/max_window_bytes
```

**knowdb.toml 管数据源定义**：表结构、列映射、provider 类型。

**wfusion.toml 管窗口行为**：取哪些数据、多久刷新。同一张 knowdb 表可以被多个窗口以不同 query 引用。

## 配置示例

### 最简（静态白名单）

```toml
[window.scanner_whitelist]
table = "scanner_whitelist"      # 指向 knowdb 中的表
# 没有 query → 默认 SELECT *
# 没有 refresh → 不刷新（静态）
```

### 同表不同窗口

```toml
# knowdb.toml
[[tables]]
name = "threat_intel"
provider = "postgres"

# wfusion.toml — 两个窗口，同一张表，不同裁剪
[window.high_risk]
table = "threat_intel"
query = "SELECT * FROM threat_intel WHERE score > 80"
refresh = "5m"

[window.recent_scanners]
table = "threat_intel"
query = "SELECT * FROM threat_intel WHERE updated_at > datetime('now', '-7 days') LIMIT 1000"
```

## 架构

```
┌──────────────────────────────────────────────────────┐
│ WFL Rule                                              │
│   join scanner_whitelist anti on e.sip == wl.sip     │
└──────────────────────┬───────────────────────────────┘
                       │ WindowLookup::snapshot("scanner_whitelist")
┌──────────────────────▼───────────────────────────────┐
│ WindowRegistry                                        │
│                                                       │
│  ┌──────────────────┐  ┌───────────────────────────┐ │
│  │ BufferWindow     │  │ ProviderWindow             │ │
│  │ (事件流窗口)      │  │ (外部数据窗口)              │ │
│  │                  │  │                            │ │
│  │ in-memory buffer │  │ table: "threat_intel"      │ │
│  │ stream → events  │  │ query: "SELECT ..."        │ │
│  │ over = 30m       │  │ refresh: "5m"              │ │
│  └──────────────────┘  │ cache: HashMap             │ │
│                        └──────────┬────────────────┘ │
│                                   │ facade::query()   │
└───────────────────────────────────┼───────────────────┘
                                    │
┌───────────────────────────────────▼───────────────────┐
│ wp-knowledge facade                                    │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────┐  │
│  │ CSV      │  │ SQLite   │  │ Postgres            │  │
│  └──────────┘  └──────────┘  └────────────────────┘  │
└───────────────────────────────────────────────────────┘
```

## 设计约束

**ProviderWindow 的 join 不走 SQL。** 数据在 bootstrap 或 refresh 时全量加载到本地 HashMap，join 期间纯内存操作。每事件一 SQL 是反模式，被禁止。

```
bootstrap/refresh: facade::query() → HashMap   （定时，低频）
join 期间:        HashMap::get()               （每事件，高频，纯内存）
```

## 数据加载策略

| 策略 | 适用 | 数据量 | 机制 |
|------|------|--------|------|
| 全量加载 | 白名单、配置表 | < 100K | bootstrap 时 `facade::query()` → HashMap |
| 查询裁剪 | 大表部分数据 | 不限 | window 级 `query` 过滤，结果仍全量入 HashMap |
| 定时刷新 | 需要更新的数据 | 不限 | `refresh` 到期 → 重新 `facade::query()` → 替换 HashMap |

> 没有 Join 下推。join 期间永远不走数据库。

### 全量加载

```toml
# 没有 query → 默认 SELECT * FROM scanner_whitelist
[window.scanner_whitelist]
table = "scanner_whitelist"
```

### 查询裁剪

```toml
[window.recent_threats]
table = "threat_intel"
query = "SELECT * FROM threat_intel WHERE updated_at > datetime('now', '-7 days') LIMIT 1000"
```

### Join 下推（计划）

```rust
// 当前：拉全部数据到内存匹配
let rows = windows.snapshot("scanner_whitelist");
let matched = find_matching_row(&rows, &conds, ctx);

// 目标：条件推给 SQL，只取匹配行
fn query_join(&self, conds: &[JoinCondition], event: &Event) -> Option<Row> {
    // c.sip == wl.sip → WHERE sip = '10.0.2.1'
    facade::query_row(sql, &[event.sip])
}
```

## 缓存层级

```
join 查询
  ↓
ProviderWindow 本地缓存 (HashMap<K, V>, refresh 控制)
  ↓ 未命中
facade 查询缓存 (LRU, ttl_ms 控制)        ← knowdb.toml [cache]
  ↓ 未命中
SQLite / Postgres
```

| 缓存层 | 配置 | 作用 |
|--------|------|------|
| 窗口本地缓存 | `refresh = "5m"` | 定时清空，下次 join 重新触发查询 |
| facade LRU | `ttl_ms = 300000` | 相同 SQL 参数 5 分钟内不查库 |

## 实现步骤

1. `ProviderWindow` 类型：实现 `WindowLookup`，内部持有 table + query + refresh + 本地缓存
2. bootstrap 时：`[window.X]` 中 `table` 字段指向 knowdb.toml 的表，创建 ProviderWindow
3. 默认行为：无 `query` → `SELECT *`，无 `refresh` → 静态不刷新
4. `join 下推`：`find_matching_row` → `provider.query_filtered(conds, event)`
5. 移除 BufferWindow 的 CSV 加载逻辑

## 与当前实现的关系

```
当前: knowdb.toml → bootstrap 全量读入 BufferWindow（一次性快照）
目标: knowdb.toml → ProviderWindow（按需查询 + 缓存 + 自动刷新）
```

当前实现验证了 knowdb → window 的可行性。后续换 `ProviderWindow` 即可。
