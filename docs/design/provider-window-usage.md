# Provider 窗口（side input）使用说明

> 状态：v1（2026-08-22，join 算子族 P4 交付）
> 配套设计：`docs/design/join-family-design.md` §7/§8/§14
> 场景：把**静态/准静态外部表**（knowdb CSV/PG）当作 join 的**有界侧输入**
> （side input）——NEXMark Q13（bid ⋈ person 表）。

## 1. 声明（三段接线）

side input 需要三处声明，缺一不可：

### 1.1 wfs 声明 schema（无 stream / time / over）

```wfs
// nexmark.wfs
window<provider> person_table {
    fields {
        id: digit
        state: chars
        city: chars
    }
}
```

- `window<provider>` 是静态窗口：**无** `stream_tag`、`time`、`over`（数据来自外部源，不是事件流）。
- 字段**必须是标量**（digit/chars/bool/time/ip/hex）；object/array 字段校验拒绝
  （`wfs_parser/validate.rs` `validate_provider_fields`）。
- `parse_wfs` 会把 provider 按 `to_flow_schema()` 合并进 flow schema 列表——checker 直接可见，
  无需额外注册。

### 1.2 windows.toml 绑定 knowdb 表

```toml
[window.person_table]
mode = "local"
max_window_bytes = "1MB"
table = "person_table"   # ← 关键：标记为 provider（table 绑定）
```

带 `table` 的窗口在 bootstrap 分区到 provider 侧（`lifecycle/bootstrap.rs:74-84`），
不构建 buffer 窗口。

### 1.3 knowdb.toml 数据源（CSV 或 PG）

```toml
[[tables]]
name = "person_table"
dir = "person_table"                 # CSV 目录
enabled = true
columns.by_header = ["id", "state", "city"]
```

bootstrap 检测到 `knowdb.toml` → `load_knowledge_into_windows` 加载并
`register_provider`（同名 buffer 窗口会被 provider 替换）。

## 2. 规则里 join provider

```wfl
rule q13_bid_person_join {
    events { b : bid_events }
    on each b -> score(10.0)
    join person_table snapshot on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield nexmark_alerts (id = b.bidder, alert_type = "q13_sidejoin",
        detail = "bid joined person", request_count = 1)
}
```

- 运行时 `RegistryLookup::join_lookup` 对 provider 窗口按**精确键等值**
  （`values_equal`）扫描静态行——静态小表 O(rows) 扫描是预期（无索引）。
- 命中 → 富化（`person_table.state` / 裸名 `state` 可用）；未命中 → snapshot
  语义（保留事件不富化）；缺省 inner → miss 丢事件。

## 3. v1 限制（checker 强制）

| 语法 | 静态窗口（provider） | 流式窗口 |
|---|---|---|
| `join x snapshot / on ...` | ✅ | ✅ |
| 缺省 inner | ✅ | ✅ |
| `join x anti` | ❌ 报错 | ✅ |
| `join x asof ...` | ❌ 报错（无 time） | ✅ |
| `within [...]` interval | ❌ 报错（无 time） | ✅ |
| `reduce ...` | ❌ 报错（v1） | ✅（deferred） |
| `emit at ...` | ❌ 报错 | ✅（deferred） |

错误信息统一以「provider/静态窗口 `x`（side input）v1 仅支持 snapshot（及缺省
inner）join」开头，并给出具体原因。

## 4. 边界与后续

- provider join 无索引 → 大静态表建议后续给 `ProviderWindow` 加内存键索引。
- `window.has()`（`snapshot_field_values`）对 provider 暂不支持（返回 None）。
- provider 声明与 flow 窗口同名 → 解析报「duplicate window name」。
- wfs 声明了 provider 但未绑 table/未建 knowdb 表 → join 静默 miss（配置错误，
  v1 未做强校验）。
