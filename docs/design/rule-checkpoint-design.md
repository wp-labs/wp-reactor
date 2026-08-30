# 规则级 Checkpoint 设计（与 `limits` 并列）

<!-- 角色：架构师 | 状态：设计方案 v0.1（待评审） | 创建：2026-08-30 | 关联：baseline-online-design.md v2.2 -->

## 0. 阅读约定

| 标记 | 含义 |
|---|---|
| ✅ | 已落地，可直接使用 |
| 🟡 | 规划中 / 存在已知约束，下方注明真实状态 |
| ✗ | 不支持，或有意不做 |

本文所有源码引用均经 2026-08-30 回验，**引用前请复核行号**。

---

## 1. 问题：状态丢失发生在哪里

### 1.1 停止处理分两路

| | 可预知停止 | 不可预知崩溃 |
|---|---|---|
| 场景 | 版本升级、配置变更、扩缩容、计划维护 | 进程崩溃、OOM、节点掉线、断电 |
| 机制 | **优雅停机**：停之前主动落一次完整快照 | **周期性 checkpoint**：不知何时崩，只能定期拍 |
| 恢复精度 | 精确回到停止点 | 回滚到最近一次快照，之后进度靠重放 |
| 开销 | 仅停机时，**无常态开销** | 常态开销（快照写盘） |
| 复杂度 | 低——序列化落盘即可 | 中——需周期调度 + 版本管理 |

**两路都要做，但一期优先做周期性 checkpoint**——优雅停机无法覆盖崩溃场景，而崩溃恰恰是长周期统计最怕的。

### 1.2 两类规则状态，丢法不同

| 状态 | 归属 | 丢失后果 | 能否自动补回 |
|---|---|---|---|
| **`stats` 累加器** | 规则级，按 group key | 计数/求和归零 → 阈值永远触发不了 → **漏报** | ✅ 可——上游可重放时重新累加 |
| **`match` 部分匹配** | 规则级，按 entity key | 多步链已匹配前几步的状态丢失 → **漏掉进行中的攻击链** | ⚠️ 受限——引用窗口内具体事件，事件被驱逐后无法恢复 |

**关键区别**：`stats` 累加器是**从历史事件的聚合结果**，恢复后继续累加即可，不依赖原始数据；`match` 部分匹配**引用窗口内的具体事件**，恢复时那些事件必须仍在窗口内。

---

## 2. 设计决策

| # | 决策 | 说明 |
|---|---|---|
| **D1** | **快照对象 = 规则状态，不是窗口** | 窗口是上游事件副本、可重放重建，且是内存大头（q18 RSS 27.9GB 即窗口状态），快照它不可行；窗口还被多规则共享，快照会冗余。规则状态是从**已驱逐事件**推导的结果，不可重建——这才是该快照的东西。 |
| **D2** | **语法落点 = 与 `limits` 并列的 `checkpoint` 子句** | 而非内联进 `stats<...>`（`stats<...>` 按 `wfl_parser/stats_p.rs:79-95` 只接受 `duration[:fixed｜session]`，混入持久化会耦合"计算什么"与"如何持久化"），也非嵌套进 `limits`（与 spill 混淆）。并列最符合 WFL"子句各司其职"的惯例。 |
| **D3** | **按需开启，默认关闭** | 绝大多数检测规则不需要；只有长周期统计/基线类需要。**不引入全局屏障、无常态开销**，与 high-throughput 定位相容。 |
| **D4** | **周期可配（interval）** | 由规则按自身 RPO 需求设定，编译期定型（符合"规则即规划"）。 |
| **D5** | **恢复精度 = 近似恢复，不是 exactly-once** | 最多丢 `interval` 内的进度。对统计/基线类足够，对精确计数不够。配置语义里必须写明。 |

---

## 3. 语法设计

### 3.1 草案

```wfl
rule baseline_producer {
    events { e : metrics_stream && e.value != null }

    stats<1h:fixed> group by (e.entity, e.metric) {
        e | count          as n;
        e | sum(e.value)   as s;
        e | sumsq(e.value) as ss;      # 新增聚合，见 baseline 设计 §4.2
    }

    entity(str, e.entity)

    yield baseline_out (
        entity   = e.entity,
        metric   = e.metric,
        win_start= @window_start_time,
        n        = stat.value(final(n)),
        sum      = stat.value(final(s)),
        sum_sq   = stat.value(final(ss))
    )

    limits {
        max_memory       = "1GB";
        spill_provider   = "redb";     # 内存换出 · 无持久化语义
        max_spill_bytes  = "8GB";
    }

    checkpoint {
        interval         = "10m";      # 墙钟周期
        store            = "state";    # 独立目录，绝不用 spill_ 前缀
        max_store_bytes  = "2GB";
        retain           = 3;          # 保留最近 3 份
    }
}
```

### 3.2 字段定义

| 字段 | 必选 | 说明 |
|---|:--:|---|
| `interval` | ✅ | 快照周期，**墙钟**（防崩溃的持久性需求）。快照内容本身按事件时间定位（窗口边界、watermark）。 |
| `store` | ✅ | 存储目录/库名。**绝不可使用 `spill_` 前缀**——启动时会被 spill 清理逻辑删除（见 §5.1）。 |
| `max_store_bytes` | 建议 | 快照体积 ≈ 键数 × 累加器大小；高基数 group by 下必需，非可选。 |
| `retain` | 可选 | 保留份数，默认 3。 |
| `on_restore_fail` | 可选 | 恢复失败时的行为：`cold_start`（默认，仅失历史不影响在线检测）/ `fail_rule`。 |

### 3.3 与 `limits` 的分工（关键）

| | `limits` | `checkpoint` |
|---|---|---|
| 关注点 | **别用超**——资源预算 | **别丢了**——跨重启持久 |
| 磁盘语义 | spill：内存压力下的**临时换出**，无持久化语义 | 状态快照：**持久**，跨重启存活 |
| 生命周期 | 窗口 close 即删、启动清理 | 按 `retain` 保留，启动加载 |

---

## 4. 快照内容与恢复语义

### 4.1 一期：`stats` 累加器

**快照内容**（每个 group key 一行）：

```
(rule_id, group_key, window_boundary, n, sum, sum_sq, [min/max/distinct_sketch/last/topN])
```

对应 `StatsAggPlan`（`wf-lang/src/plan.rs:204`）的聚合槽位，按当前支持的聚合类型逐项序列化。

**恢复**：启动时按 `(rule_id, group_key)` 载入最近一份快照，恢复累加器与窗口边界，继续累加新事件。

**优势**：体积小（每键几十字节）、不依赖窗口历史、恢复可靠。

### 4.2 二期：`match` 实例（🟡 存在硬约束）

**约束**：部分匹配引用窗口内具体事件，恢复时那些事件必须仍在窗口内。而当前窗口驱逐是**消费感知驱逐**（`WindowProgress` ack floor：事件时间过期 **且** 所有消费者已 ack 才驱逐）——一旦消费完即驱逐，恢复时拿不到历史事件。

**可选路径**：

| 方案 | 做法 | 代价 |
|---|---|---|
| A. 摘要恢复 | 只恢复"匹配到第几步 + 关键字段值"，不恢复原始事件引用 | 语义可能不等价（`on close` 里引用事件字段的 measure 会失真） |
| B. 窗口驱逐联动 | 窗口驱逐前检查"相关规则状态是否已快照" | 复杂度高，且拖住内存释放 |
| C. 短窗口不恢复 | 接受短窗口（秒~分钟级）崩溃后重新累积 | 一期推荐，影响本就可控 |

**建议**：一期只做 `stats`，`match` 采用方案 C（不恢复）；二期再评估 A。

### 4.3 恢复流程

1. 启动 → 扫描 `store` 目录，定位各规则最近一份有效快照；
2. 校验快照版本与规则指纹（规则变更时快照失效 → 冷启动）；
3. 载入累加器/实例状态；
4. 从快照记录的 source 位点继续消费。

> ⚠ **恢复后进度是近似的**：快照之后、崩溃之前已处理但未快照的事件会重新消费一次 → 对 `stats` 累加器意味着**可能重复计数**。这是 D5"非 exactly-once"的具体体现。若需精确，须待输出侧幂等能力（见 §7 待确认项）。

---

## 5. 与现有机制的关系

### 5.1 与 spill（`disk_provider`）的语义隔离 🔴 最高优先级

**必须明确区分，否则会重演 baseline 的坑。**

> **spill 无持久化语义**（`wf-runtime/src/lifecycle/spawn.rs:238-302`）：spill 文件 `spill_{rule}_{pid}.rb` 在"窗口 close 后 `cleanup` 删除"；启动时 `cleanup_leftover_spill_files()` 删除目录下全部 `spill_*.rb/.rbr`，注释明写"**spill 无持久化语义，重启 = 重新 ingest**"。`SpillMode::Redb`（`wf-lang/src/compiler/mod.rs:1357`）是 stats 窗口在内存压力下的**中间态换出**通道，不是存储。

**历史教训**：`disk_provider` / `max_disk` 是 **2026-08-27 才改的名**，原名为 `spill` / `max_spill_bytes`（`compiler/tests/coverage_extra.rs:291-307`，旧名仍作兼容别名解析）。**这次改名加重了混淆**——`spill` 语义原本明确（换出），改成 `disk_provider` 后听起来像"磁盘存储"，直接导致了"基线支持持久化"的误读。

**建议**：借本次引入 checkpoint 之机，把 `limits` 里的字段**主名改回 `spill_provider` / `max_spill_bytes`**，`disk_provider` / `max_disk` 降为兼容别名。让"临时换出"与"持久快照"在命名上就分得开。

### 5.2 与窗口消费感知驱逐的冲突

当前 `WindowProgress` 以 ack floor 驱动驱逐，与"状态是否已快照"无关。若二期要做 `match` 恢复，二者必须联动（见 §4.2 方案 B）。**一期做 `stats` 不受此影响**——`stats` 累加器恢复不依赖窗口历史。

### 5.3 与 baseline 持久化方案的关系

`baseline-online-design.md` v2.2 §4.5 设计的"独立持久基线库"，本质就是**给 `stats` 累加器做应用层持久化**。本方案是把它**提升为引擎级一等能力**：

| | baseline 方案（现状） | 本方案 |
|---|---|---|
| 落点 | 应用/规则作者手工设计存储 | 引擎级 `checkpoint {}` 子句 |
| 适用 | 仅基线这一场景 | 任意 `stats` 规则 |
| 与 spill 隔离 | ✅ 已明确（独立目录/前缀） | ✅ 继承该约束，强制 `store` 不得用 `spill_` 前缀 |

二者**不冲突，是演进关系**：baseline 是第一个使用者。

### 5.4 与 `limits` 的校验惯例

现有 `limits` 校验（`checker/rules/limits.rs`）：

- 合法键白名单 `VALID_LIMIT_KEYS`（`:5-14`）：`max_memory` / `max_instances` / `max_throttle` / `on_exceed` / `disk_provider` / `spill`（别名）/ `max_disk` / `max_spill_bytes`（别名）
- **不适用的规则类型 → 编译期 Error**，而非静默忽略（`:259-269`：`disk_provider` 用于非 stats 规则报 `` `{}` 仅支持 stats 规则 ``）
- 空键 stats（无 group by）→ 编译期 Error（`:270-280`）

**本方案应沿用这套惯例**：`checkpoint` 用于不适用的规则类型时**编译期报错**，不要静默忽略。

---

## 6. 落地计划（分期）

| 期 | 范围 | 估时 |
|---|---|---|
| **一期** | `checkpoint {}` 子句（parser + checker + plan）+ `stats` 累加器快照/恢复 + 独立存储目录 + `limits` 字段改名（含兼容别名） | 3–5 天 |
| 二期 | `match` 实例摘要恢复（方案 A），或窗口驱逐联动（方案 B） | +3–5 天 |
| 三期 | 输出侧幂等 / 两阶段提交（迈向 exactly-once） | 待评估 |

---

## 7. 待确认项

1. **子句顺序**：`checkpoint` 放在 `limits` 之后；parser 是否强制顺序？
2. **适用规则类型**：一期仅 `stats`。用于 `match` / on-each 规则时——编译期报错（沿用 §5.4 惯例）？还是允许但降级？
3. **高基数保护**：`(entity, metric)` 百万级键时快照体积与耗时；是否需要采样/上限策略，或当超出 `max_store_bytes` 时的降级行为。
4. **interval 时钟语义**：确认为墙钟；快照内容按事件时间定位。二者混用会导致重放错位，需在实现中显式区分。
5. **规则变更时的快照失效**：规则指纹如何计算（是否含 `stats` 聚合列表、group by 键、窗口规格）？
6. **与 `limits { max_memory }` 的关系**：快照期间的内存峰值是否计入规则内存预算？

---

## 8. 源码引用索引

| 事实 | 位置 |
|---|---|
| `VALID_LIMIT_KEYS` 合法键白名单 | `wf-lang/src/checker/rules/limits.rs:5-14` |
| 非 stats 规则用 `disk_provider` → 编译期 Error | `wf-lang/src/checker/rules/limits.rs:259-269` |
| 空键 stats 用 `disk_provider` → Error | `wf-lang/src/checker/rules/limits.rs:270-280` |
| spill 无持久化语义 | `wf-runtime/src/lifecycle/spawn.rs:238-302` |
| `SpillMode::Redb` | `wf-lang/src/compiler/mod.rs:1357` |
| `spill`/`max_spill_bytes` 旧别名（2026-08-27 改名） | `wf-lang/src/compiler/tests/coverage_extra.rs:291-307` |
| `stats<...>` 只接受 `duration[:fixed｜session]` | `wfl_parser/stats_p.rs:79-95` |
| measure 语法 `alias｜agg(field) as label;` | `wfl_parser/stats_p.rs:160` |
| `StatsAggPlan` 聚合槽位 | `wf-lang/src/plan.rs:204` |
| `stat.value(final(label))` 校验与 Close 阶段 | `wf-lang/src/checker/rules/mod.rs:291` |
| 基线持久化设计（与 spill 解耦） | `wp-reactor/docs/design/baseline-online-design.md` §4.5 |
