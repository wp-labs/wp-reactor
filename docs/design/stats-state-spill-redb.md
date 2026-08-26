# stats 状态 spill 到 redb（大键状态内存有界化）

> 状态：**设计中** · 2026-08-26 · 优先级：高（q18 100M 状态 18.6G 是语义必然，
> 唯一的根治路径是让状态落盘，内存只保留活跃子集）
> 关联：`notes/q18-stats-key-state-memory.md`（q18 归因 + 键数线性增长根源 §10.4）、
> `MEMORY_ISSUES_100M.md` M-18
> 复现场景：`wf-examples/performance/nexmark_pk` → `./bench.sh q18 replay 100m`
> （状态 18.6G，100M RSS 35G+）

---

## 1. 问题背景（数据驱动）

q18（stats 1d group by (bidder, auction)）100M 状态内存：

| 项 | 量 | 性质 |
|---|---|---|
| 键数 | 2935 万 | 语义必然（权威 `PARTITION BY (bidder, auction)` + 数据生成器滑动窗口） |
| 每键状态 | 633B | 已优化（777B→633B，链 Vec 修复） |
| **状态合计** | **~18.6G** | **随键数线性，无法靠常数优化消除** |

**结论**：状态内存与「流式有界」目标冲突的唯一根源 = **所有键必须驻留内存**。
key insight：q18 的数据特征是**滑动窗口引用**——大多数键（冷键）只出现一次，
之后永不再被引用。它们占着内存但不再被访问。

## 2. 设计目标

1. **内存有界**：状态内存 ≈ 活跃键子集（近 watermark 窗口内的键）+ 存在性索引，
   不随总键数线性增长
2. **hot path 零磁盘读**：每事件查键 O(1) 内存操作，不碰 redb
3. **正确性等价**：spill 与否输出逐字节一致（对拍契约）
4. **通用**：q17/q19 及未来大键 stats 查询同样受益；无 spill 配置时零开销

## 3. 总体架构（两级存储）

```
┌─────────────────────────────────────────────────────┐
│ StatsWindowState                                    │
│  ┌─────────────────┐   ┌──────────────────────────┐ │
│  │ buckets          │   │ spill_index             │ │
│  │ HashMap<u64,     │   │ HashSet<u64>            │ │  ← 存在性（8B/键）
│  │   Vec<StatsBucket│   │ （hash → 已 spill）      │ │
│  │ >                │   └──────────────────────────┘ │
│  │ 活跃键（近窗）    │            │ put/get           │
│  └─────────────────┘            ▼                   │
│                    ┌──────────────────────────┐     │
│                    │ SpillStore (trait)       │     │
│                    │  ├─ Noop（默认，零开销） │     │
│                    │  └─ Redb（持久化）       │     │
│                    └──────────────────────────┘     │
└─────────────────────────────────────────────────────┘
```

**数据流**：

```
事件 → hash(键)
  ├─ buckets 命中 → 更新（活跃键，99% 走这）
  ├─ 未命中 & spill_index 含 hash → 读回（redb get）→ 更新 → 放回 buckets
  │     （读回后可能再驱逐一个冷键腾空间）
  ├─ 未命中 & 无 → 新键 → 建桶（若超预算 → 先 spill 最老键）
  └─ close → flush buckets → 读回 redb 全部 → 合并 → 排序输出
```

## 4. 触发策略：超预算驱逐最老未更新键（LRU-时间混合）

**语义**：当 `estimated_bytes` 超 `max_memory` 预算时，**驱逐「最后更新事件时间
最老」的键**到 redb，直到预算回落。

**实现**：StatsWindowState 维护一个**最近更新序**（LRU 链表或
`(last_update_nanos, hash)` 最小堆）。列式路径每事件命中桶时更新该桶的
时间戳（摊销：批内按行更新，或用 watermark 近似）。

**为什么 LRU 而非纯时间驱逐**：
- q18 滑动窗口特征：冷键 = 早期创建且不再更新的键 → LRU 尾部恰好是它们
- 纯时间驱逐（`last_update < watermark - X`）需要额外扫描，且 X 难定
- LRU 在超限时精准驱逐「最久未碰」的键，与 q18 死键特征完美契合

**读回再驱逐**：被 spill 的键若又来一条（罕见）→ 从 redb 读回 → 若 buckets
已满 → 驱逐当前 LRU 尾部 → 放回。读回是低频路径，redb 点查 + 一次驱逐可接受。

## 5. 限额语义：spill 替代拒收（带落盘上限）

**启用 spill 后**，`over_limit_new_buckets` 拒收**不再触发**（或大幅减少）：

```
旧：超内存预算 → 拒收新键（丢数据，正确性降级）
新：超内存预算 → spill 最老键腾空间 → 新键正常建桶（不丢数据）
```

**但 spill 本身也有上限**（2026-08-26 用户定调）：磁盘不能无限写。

```
wfl：
    limits {
        max_memory = "2GB"
        spill = "redb"          // 启用状态外溢（默认 off）
        max_spill_bytes = "8GB" // 落盘上限（默认 off 时无意义；启用后默认磁盘可用量的一半）
    }
```

**三层预算阶梯**（内存 → 磁盘 → 兜底拒收）：

```
内存 estimated_bytes ≤ max_memory          → 全内存，不 spill
内存超 max_memory 且 spill_bytes ≤ max_spill → spill 最老键（LRU）腾空间
spill_bytes ≥ max_spill_bytes              → 停止 spill，回退拒收
                                             （over_limit_new_buckets 计数 + 告警，与现状一致）
redb 写失败（磁盘满/IO 错）               → 同拒收（计数 + 致命告警，不静默丢）
```

- `over_limit_new_buckets` 保留为**兜底**（spill 满/写失败才触发）
- 行为变更需文档化：配置 `spill` 后，内存换磁盘、不丢键；spill 满才丢

## 6. 接口设计（SpillStore trait）

```rust
/// 状态外溢存储。`Noop` = 默认（无 spill 配置），`Redb` = redb 持久化。
pub trait SpillStore {
    /// 键是否已 spill（hot path 存在性检查，O(1) 内存操作）。
    fn contains(&self, hash: u64) -> bool;

    /// spill 一个键（put 到持久层；buckets 中已移除）。
    fn put(&mut self, hash: u64, key: &ScopeKey, accs: Vec<StatsAccum>) -> Result<(), SpillError>;

    /// 读回一个键（低频：spill 后键又来一条）。
    fn get(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)>;

    /// close：读回全部 spill 键。
    fn drain(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)>;

    /// 当前已 spill 键数（诊断/指标）。
    fn len(&self) -> usize;
}

pub enum SpillError {
    Io(std::io::Error),
    Redb(redb::Error),
    Corrupt(String),   // 读回数据损坏（致命，绝不静默丢键）
}
```

**Noop 实现**：`contains` 恒 false，`put`/`get`/`drain` 空操作——hot path
一个分支预测，零开销。

## 7. redb 表结构

```
db: Database（单文件，按任务实例隔离：spill_{rule}_{shard}.rb）

table "state": key = u64 hash（唯一），value = 序列化链
  └ 链：Vec<(serialized_scope_key, serialized_accs)>  // 同 hash 碰撞键合并
```

**序列化**（自包含，无需外部 schema）：
- `ScopeKey` → 手写字节编码（tag + payload，与 `scope_key_hash` 同构）：
  Empty=0 / Int=1+i64 / Float=2+u64 / Str=3+len+bytes / Pair=4+嵌套
- `StatsAccum` → 手写字节编码（tag + 变体）：
  Numeric(count,sum,min,max) / Distinct(元素集) / Last(row_fields) /
  Top(entries)
- `RowFields` → 按 layout 槽序写 numeric/strings/others/null_mask（**layout
  不序列化**——读回时按当前 executor 的 layout 解释，同一 executor 生命周期
  内 layout 不变，成立）

> 用 `serde` 而非手写？serde 对 `RowFields`（Box<[SmolStr]>）等需 derive，
> 且 redb 的 value 是 `&[u8]`——手写字节编码更可控（对拍契约 + 无中间 alloc）。
> 若后续复杂度上升，可换 `bincode` + serde derive。

## 8. 窗口生命周期与文件清除

**spill 文件 = 窗口级**：文件名带窗口 ID（`spill_{rule}_{window_start}.rb` + redb
配套的 `.rbr` WAL 文件）。q18 单窗（1d）每任务实例一个文件；多窗规则（q12
fixed）每窗独立文件。

**清除时机（4 个）**：

| 时机 | 动作 | 说明 |
|---|---|---|
| ① 窗口正常 close 完成 | drain 全部键后删文件（`.rb` + `.rbr`） | q18 跑批结束即清，磁盘零残留 |
| ② 窗口 reset（多窗规则） | 旧窗 close 完删旧文件，新窗开新文件 | 文件名带 window_start 天然隔离 |
| ③ 进程正常关闭 | 所有窗口 close 完 → 文件已删；最后清空 spill 目录 | 正常路径无残留 |
| ④ 进程异常退出（崩溃/kill） | **下次启动时清理 spill 目录残留** | redb 异常退出会留 `.rbr`；启动时 glob 删 `spill_*.rb/.rbr`（残留 = 旧窗口未 close 完，无保留价值） |

**删除失败处理**：rm 失败（权限/占用）→ 告警 + 记入日志，不阻塞（下次启动 ④
重试清理）。残留文件对正确性无影响（键已输出完，只是磁盘占用）。

> 设计取舍：不做「跨重启恢复 spill 状态」——spill 只是**内存换磁盘的临时
> 缓冲**，不是持久化语义。进程重启 = 重新 ingest，spill 文件无保留价值，
> 启动清理即可。若未来需要「断点续跑」，那是 checkpoint 范畴，另立设计。

## 9. close 流程（读回合并）

```
close:
  1. flush buckets 全部 → redb（内存键也落盘，单一来源防重复）
  2. drain redb 全部 → Vec<(ScopeKey, Vec<StatsAccum>)>
  3. 与原有逻辑合并：take_buckets_up_to 分批 → close_buckets_to_rows → 输出
  4. 删 spill 文件
```

**防重复**：close 时内存键先 flush 进 redb，再统一 drain——每个键只从 redb
读一次，无「内存一份 + redb 一份」的重复输出。

**排序契约**：drain 出的键按 ScopeKey 排序（现有 `take_buckets_up_to` 批内
排序逻辑复用），批间无序 OK（与现状一致，文档 §8 已确认）。

## 10. 与现有机制的关系

| 现有机制 | 关系 |
|---|---|
| `bucket_allowance` / `estimated_bytes` | spill 后**扣减已 spill 键**（不重复计预算） |
| `over_limit_new_buckets` 拒收 | 被 spill 替代（§5），保留为兜底 |
| `take_buckets_up_to` / close 链 | close 读回后复用（§9） |
| `comps_match` / `scope_key_from_comps` | spill 键序列化/读回复用（§7） |
| `take_partial` / `merge_partial`（分片） | **暂不支持 spill+分片组合**（q18 单实例）；可交换度量分片（q15/q16）spill 需先读回再传——初始版本 spill 仅单实例/空键规则可用，分片规则禁用 |
| 键值类型 | `ScopeKey` 全形态支持（Int/Float/Str/Pair），不限于 int 键 |

## 11. wfl 声明

```wfl
rule q18_last_bid_stats {
    ...
    limits {
        max_memory = "2GB"
        spill = "redb"          // 启用状态外溢（默认 off）
        // spill_path = "/tmp/wfusion-spill"  // 可选，默认工作目录
    }
}
```

解析：`LimitsPlan` 加 `spill: Option<SpillMode>`（None / Redb）；spawn 层按
配置构造 SpillStore 注入 StatsExecutor。

## 12. 测试计划

**单元**：
- `SpillStore` round-trip：put/get/drain 各键形态（Int/Float/Str/Pair +
  Numeric/Distinct/Last/Top 度量）字节一致
- `Noop` 零开销：contains 恒 false
- 序列化对拍：手写编码 ↔ 原值（含 null/空集/边界）

**集成（对拍契约）**：
- q18 形态 + 强制 spill（小 max_memory）→ 输出与不 spill **逐字节一致**
  （EMIT 行数 + 每条字段）
- spill 后键再来（读回路径）→ 正确更新
- close 读回合并 → 无重复无丢失

**性能（memory_probe）**：
- spill 前后状态内存曲线（预期 18.6G → 活跃子集 + 索引）
- hot path 退化（Noop vs Redb 的 contains 开销）

**回归**：现有 stats 测试（`close_buckets_to_rows` / `take_buckets_up_to` 等）
在 Noop 下全过——spill 是旁路，默认不改变任何行为。

## 13. 里程碑

1. **M1**：SpillStore trait + Noop + 序列化（ScopeKey/StatsAccum/RowFields）
2. **M2**：Redb 实现（put/get/drain）+ 单元测试
3. **M3**：接入 StatsWindowState（超预算驱逐 + spill_index + 读回 + close 合并）
4. **M4**：wfl 声明（spill）解析 + spawn 注入
5. **M5**：q18 100M 验证（内存曲线 + 对拍 + EPS 影响）

## 14. 风险与缓解

| 风险 | 缓解 |
|---|---|
| redb 点查慢（读回路径） | 读回是低频（死键不回来）；redb B+树点查 µs 级，close 前几乎不触发 |
| close 读回 18.6G I/O 耗时 | 顺序扫描（B+树叶子链），分钟级跑批可接受；对拍验证耗时 |
| 序列化 bug 丢数据 | 读回失败 → `SpillError::Corrupt` → panic（致命，不静默丢键） |
| 与分片组合 | 初始禁用（§10），后续按需扩展 |
| redb 文件膨胀 | 窗口级文件 + 4 个清除时机（§8）；`max_spill_bytes` 上限（§5） |
| 磁盘满 | `max_spill_bytes` 预算 + 写失败回退拒收（§5 三层阶梯） |
| 崩溃残留 `.rbr` | 启动时清理 spill 目录（§8 时机④）；残留无正确性影响 |
