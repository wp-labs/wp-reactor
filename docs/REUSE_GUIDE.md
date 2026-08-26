# 复用机制指南（Reuse Guide）——开发 AI 优先学习使用

> 沉淀自 q13 性能/内存专项（EPS 1.52M→10.2M、30M RSS 14.3G→3.5G、100M 26.4G→4.9G，
> 详见 `issues/q13-memory-peak-scales-with-volume.md`）。**遇到性能/内存/列式化问题，
> 先读本文档找到现成机制，别再造轮子。**
>
> 姊妹文档（方法论，深入读）：
> [`MEMORY_BISECTION_METHOD.md`](MEMORY_BISECTION_METHOD.md)（内存：分账→消融→差分）、
> [`PERF_BISECTION_METHOD.md`](PERF_BISECTION_METHOD.md)（吞吐：墙梯）。

---

## 1. 何时读本文档

- 查询 EPS 低 / RSS 超预期 / 内存随数据量增长 → **§2 定位工具链**（先定位段，再动手）
- 定位到段后要优化 → **§3 优化模式**（找现成可复用代码）
- 要采内存/性能数据 → **§4 测量设施与口径**（用对口径，别被假象骗）
- 新写列式/输出代码 → **§3.1 常量列折叠**（本团队最常用的列式化机制）

**铁律**：先定位段边界（消融/墙梯），再优化；改一步测一步（bench + 守护测试）。

---

## 2. 定位工具链（先定位再动手）

| 工具 | 命令 | 产出 | 何时用 |
|---|---|---|---|
| 性能墙 | `./diag.sh qN 30m` | 五档（recv/decode/floor/rules/full）EPS + 每档增量 + **RSS/DIRTY 列** | 吞吐墙在哪一段 |
| 内存墙 | `MEMORY=1 ./diag.sh qN 30m` | 每档 RSS_peak/ΔRSS/**DIRTY_peak** + 成分分账（窗口 Σ/每窗明细/parse/fanout） | 内存增长在哪段、由什么构成 |
| bench | `./bench.sh qN replay 30m\|100m` | 哨兵 EPS + RSS_peak + 正确性 | 优化前后对比（每步必跑） |
| 二分消融 | 临时 env gate（见下） | 段内部再切（alert 构建 vs 列装载…） | 墙梯定位到段后，段内细分 |
| fp 探针 | `footprint <pid>` 采样 | **dirty（真持有）vs RSS（含页表保留）** | 判断"真持有"还是"水位/伪影" |

### 二分消融（临时 env gate 模式）

墙梯定位到段后，段内逐段短路定位**子段**边界。模式（`perf_diag.rs` + `column_batch.rs`）：
- 现有：`WF_DIAG_CUT_ALERT`（切 alert 构建整段，保留 pipe/join 消费）
- 临时加 gate 的套路：`OnceLock<bool>` 读 env → 热路径短路点 `return Ok(())` / `continue`
- 实测定位 q13：列装载 6.1G（主因）/ 行值 eval ≈0 / yield stage ≈0 / join ≈0
- ⚠ 用完即撤（生产勿设）；逐段**各跑一轮 bench 对比 ΔRSS/EPS**

### fp 探针（区分真持有 vs 水位）

```
跑批中 footprint <pid> → dirty 13G（真持有，在途积压）
停止注入 1s 后        → dirty 5.6G（稳态有界）
RSS 保持 13.7G（页表保留未归还）→ RSS 是峰值水位代理，dirty 才是判据
```

---

## 3. 优化模式（可复用代码）

### 3.1 常量列折叠 `ColumnData<T>` ⭐（本团队最重要列式化机制）

**位置**：`wf-engine/src/alert/column_batch.rs`

**机制**：列内全部行同值（run of identical values）→ 折叠为 `Const(T)` 单值 + 行数，
读取按行展开 O(1)。`Rows(Vec<T>)` 保持每行独立值。

```rust
enum ColumnData<T> { Const(T), Rows(Vec<T>) }  // at(row): Const→唯一值, Rows→values[row]
```

**覆盖**：
- 系统字段列：on-each 的 rule_name/entity_type/origin/close_reason/emit_time（计划/批常量）
  → `Const`；summary / close 路径的 origin 等 per-row → `Rows`
- yield 字面量列：`register_yield_column(name, Some((meta, value)))` → values/metas 免每行
  cell（`YieldCol::is_const_column()`），读时展开

**实测收益**（q13）：30M RSS 11-14G → **3.5G**、EPS 7.4-8.7M → **10M**；100M RSS 20.4G → **4.9G**。
每行省：6 系统列 Arc clone + fill 2 次 const clone + 8 列 per-row 数组（~8-11GB 分配总量）。

**何时用**：任何全列同值的列（元数据、字面量字段、固定标签）。⚠ summary 不能折叠
（match 规则 per-row）；record 路径（append_record）字段可能 per-row，保持 Rows。

### 3.2 快车道：跳过 Value 中转

| 模式 | 位置 | 适用 | 实测 |
|---|---|---|---|
| 数字快车道 `stage_yield_cell_f64` | `column_batch.rs` + `each_exec.rs` | yield 字段与 entity 同一列且数字类型（q13 id=m.bidder） | EPS 5.4→7.6M、dirty 13→7.4G |
| `EntityCol` 列直读 | `each_exec.rs` | Int64/Utf8 列免 `Value`/`SmolStr` 中转（三态：I64/TsNanos/Utf8/Generic） | q13a 列式化 6.1× |
| `SmolStr` 内联 | `column_batch.rs` | 短字符串 ≤22B 零堆分配（wfx_id/entity_id） | per-row 堆分配归零 |
| `SmolStrBuilder` 直写 | `each_exec.rs` | 数字→字符串直写（`write_int64_value` + `StrSink` trait） | 免 String 中转 |

### 3.3 预物化：静态数据构建一次

**位置**：`wf-engine/src/window/provider/mod.rs`（`ProviderWindow::join_rows_lookup`）

**机制**：provider 静态表（side_input）在 `set_join_key` 时预物化 `Arc<Event>` 行
（`join_rows`），命中仅 Arc clone——对齐 `q13b_join_bench` 的 `IndexedLookup` 测试模式。
**收益有限**（q13 仅 -1G dirty）：q13b 批级预查已把 lookup 频率压到唯一 key 数。
**何时用**：provider 静态表 join 且无批级预查时收益大。

### 3.4 批级预查：按 key 去重查找

**位置**：`each_exec.rs` `execute_each_direct_batch_columnar_join`（`key_rows` 分组）

**机制**：批内按 join key 分组，每唯一 key 一次 `join_lookup`，hot key 共享结果——把
"每行一次 lookup"变成"每 key 一次"。**q13b 避免卡死的关键**（10k 行表 × 920k 事件）。

### 3.5 零拷贝 move 语义（教训）

`commit_each_rows_batch_moved`（owned Vec move-extend）**实测无效**（CPU/内存均不变）：
分配**总量**不变 → 内存水位不变。教训：**列装载的内存 ∝ 值进列的总量（∝行数×列值），
与 push 方式无关**——要降内存只能降总量（常量列折叠/列式化）或降每行 CPU（在途积压 ↓）。

---

## 4. 测量设施与口径

### 4.1 内存采样开关 `mem_sample`（默认开）

`wf-examples/performance/nexmark_pk/conf/perf-diag-wall.toml`：
```toml
mem_sample = true   # diag.sh 起 footprint 采样器（0.5s）→ 墙表 DIRTY_peak 列
                    # 关：MEM_SAMPLE=0 或 mem_sample = false（省 ~50% 核 spawn）
```
**设施**：`bench_lib.py` `footprint-sampler` 子命令（epoch dirty_mb）；`diag_mem_analyze.py` /
`diag_analyze.py` 墙表 DIRTY 列。非 macOS 自动 n/a。

### 4.2 采样位置与口径（在哪里采样最正确）

| 口径 | 正确位置 | 为什么 |
|---|---|---|
| RSS 峰值 | **引擎内部** `alloc.peak_rss_bytes`（metrics） | mi_process_info 内部跟踪历史峰值，**不漏峰**（外部 ps 采样会漏，实测 5-14G 波动） |
| dirty（真持有） | 外部 `footprint`（唯一来源） | mi_process_info 无 dirty；1s 周期足够（dirty 变化慢） |
| CPU | 外部 ps（cputime 差分） | 引擎无 CPU 自读数 |
| 成分分账 | 引擎内部 metrics（window/parse/fanout） | 每 100ms 全拍，精确到持有者 |

**判据**：100M 跑批 RSS > 10G 即疑点；**dirty 是权威**（RSS = dirty + 页表保留，波动大）。
bench 的 `RSS_peak` 单次不可信（运行间波动），对比用 `MEMORY=1 diag` + fp。

### 4.3 关键指标（metrics.ndjson）

- `alloc.peak_rss_bytes` / `current_rss_bytes`：进程 RSS（内部，权威峰值）
- `window.memory_bytes` / `allocated_bytes`：窗口保留（allocated 按指针去重，防双算）
- `parse.inflight_bytes` / `budget_bytes`：parse 在途（q13：2GB 预算只用 20MB）
- `window.fanout_queued_batches` / `acked_lag`：通道排队 / 消费滞后
- `alert.channel_depth` / `channel_full_total`：sink 通道积压

---

## 5. 快速上手流程（新性能/内存问题）

```
① 起数据    ./bench.sh qN replay 30m         → 基线 EPS/RSS（单次 RSS 仅参考）
② 定位段    ./diag.sh qN 30m（墙梯）           → EPS 墙 + RSS/DIRTY 在哪段
            或 MEMORY=1 ./diag.sh qN 30m（内存墙）
③ 细分段    二分消融（临时 env gate）           → 段内子段边界
④ 判性质    fp 探针（dirty vs 水位 / 停止注入后回落）→ 真持有 vs churn vs 泄漏
⑤ 选模式    §3 优化模式（常量列折叠/快车道/预物化…）
⑥ 改+测     每步 bench 对比 + 守护测试（改一步测一步）
```

---

## 6. 关键教训（AI 优先避免）

1. **先定位段再优化**：错段优化白干（q13 曾优化 pipe 足迹 4.2×，内存纹丝不动）
2. **over 调小不是内存修复**：驱逐由消费滞后（ack floor）门控，与 over 无关（4 个 over 值验证）
3. **分配总量不变 → 内存不变**：moved 批式教训（§3.5）
4. **每行 CPU → 在途 → 内存闭环**：提速即降内存（f64 快车道实证）
5. **测量设施自身会错**：键名（parse_budget_bytes 读成 0）、单位（MB vs 字节）、
   采样周期（漏峰）——用守护测试钉住指标导出
6. **RSS 是水位代理**：判内存用 dirty（footprint），判峰值用内部 alloc 读数
