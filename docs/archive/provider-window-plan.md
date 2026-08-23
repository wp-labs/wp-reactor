# Provider Window 工作计划

## Phase 1：ProviderWindow 基础类型

**目标**：ProviderWindow 实现 WindowLookup trait。

**任务**：
- [ ] 定义 `ProviderWindow` 结构体（table, query, refresh, cache: HashMap）
- [ ] 实现 `WindowLookup` trait（snapshot, snapshot_with_timestamps, snapshot_field_values）
- [ ] 注册到 `WindowRegistry`，与 `BufferWindow` 共存
- [ ] 单元测试：Mock Provider，验证 lookup 行为

**产出**：`wf-engine/src/window/provider.rs`

---

## Phase 2：knowdb facade + 全量加载

**目标**：bootstrap 时 facade::query() 全量加载到 HashMap。join 纯内存。

**任务**：
- [ ] `KnowdbProvider::load_all()`：`facade::query(query)` → HashMap
- [ ] `snapshot()` → 遍历 HashMap 返回所有行
- [ ] `refresh` 定时器：到期清空 HashMap，重新 load_all()
- [ ] 集成测试：knowdb.toml + CSV → ProviderWindow → join 正确

**产出**：`wf-engine/src/window/provider/knowdb.rs`

---

## Phase 3：wfusion.toml 配置

**目标**：`[window.X]` 声明 Provider 窗口。

**任务**：
- [ ] 增加 `table`、`query`、`refresh` 字段
- [ ] bootstrap 时为每个 `[window.X]` 创建 ProviderWindow
- [ ] 默认：无 query → `SELECT *`，无 refresh → 静态
- [ ] 验证：table 指向 knowdb.toml 存在的表

**产出**：`wf-config` + `wf-runtime` bootstrap

---

## Phase 4：迁移当前实现

**任务**：
- [ ] `port_scan_whitelist` 改用 ProviderWindow
- [ ] 删除 `load_knowledge_into_windows`
- [ ] 清理 `build_record_batch_from_json` pub(crate)
- [ ] 回归测试

---

## 设计约束

**join 期间不走 SQL。** 数据在 bootstrap/refresh 时全量加载到 HashMap，join 纯内存操作。每事件一 SQL 被禁止。

## 依赖

```
Phase 1 → Phase 2 → Phase 3 → Phase 4
```
