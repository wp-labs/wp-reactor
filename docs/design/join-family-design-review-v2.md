# Review v2：`join-family-design.md`（设计评审稿 v2, 2026-08-22）

> 评审方法：逐条复核 v1 review 的 R1–R6 在 v2 中的落实，并**重新 grep/读当前源码**坐实代码级承诺
> （未沿用旧行号或旧评审结论，凡引用都重新定位）。权威语义对照
> `wf-examples/performance/nexmark_pk/NEXMARK_AUTHORITATIVE_SEMANTICS.md`（Q8/Q9 原文 SQL）。
> 文件:行号均 2026-08-22 实测。

---

## 0. 总体判断

**这份 v2 是近期几份设计稿里最扎实的一份。** 模型骨架（时态键查找 × mode × within × 触发）站得住；
v1 的 R1–R6 全部被吸收且**经源码复核成立**；最担心的两处**语义映射（Q8/Q9）经权威 SQL 核对准确**。

剩下的问题：**1 处实打实的错误引用（N1，会让读者找错文件）**、**1 处内部措辞矛盾（N2，each+emit at 与"互斥"打架）**，
以及若干小项。没有发现会导致"实现跑偏/算错"的结构性错误。

---

## 1. v1 R1–R6 落实 + 代码核对（全部成立）

| R | v1 问题 | v2 状态 | 代码坐实（2026-08-22 实测） |
|---|---|---|---|
| R1 | on-each 无延迟承载点；Q8/Q9 语法不成立 | ✅ 已吸收入 §2.2/§5.2，明确 deferred 走新 rule_task 分支 + `emit at` 显式标记 | `EachPlan{alias,filter}` 无 window/deadline/watermark（plan.rs:77-80）；`scan_timeouts` `let Some(machine)=…else{return}`（rule_task.rs:1585）；`flush` 同理（rule_task.rs:1747）。路径在 `wf-runtime/src/engine_task/rule_task.rs`（非 doc 旧暗示的 `wf-engine/src/window/`） |
| R2 | `as winner` + `yield winner.bidder` 裸名冲突 | ✅ 改为 object 注入 + `FieldRef::Path` | `eval_field_value`（match_engine/cep/key.rs:371-397）**逐段遍历 `Value::Object`**（`map.get(name)`）—— `winner.bidder`→`Path(["winner","bidder"])` 真实可行；`field_ref_name` 丢限定词已确认（key.rs:338-349） |
| R3 | `bucket_end` 内建不存在 | ✅ §6.1 已标注为新增内建（R3 接受） | `time_bucket(t,秒)` 确实存在、返回桶起始（builtins.rs:960 / funcs.rs:933 / infer.rs:172 / check_funcs.rs:386） |
| R4 | oracle 在独立仓库 | ✅ §8 已标注跨仓库 | `wfgen` 在 `warp-fusion/crates/wfgen`（独立于 wp-reactor） |
| R5 | interval 读路径精确化 | ✅ §5.1 改为复用 `asof_candidates`/`snapshot_with_timestamps` + retain `[lo,hi]` | `asof_candidates` 返回 `Option<Vec<(i64,JoinRow)>>`（types.rs:305-320）；`lookup_timestamped` 返回 `Vec<(i64,JoinRow)>`（buffer/mod.rs:124）；`WindowLookup` trait（types.rs:264），方法 snapshot_with_timestamps:276 / join_lookup:285 / asof_candidates:305 均存在 |
| R6 | deferred 输出 origin/emit 未定义 | ✅ §5.2 标注 `origin=AlertOrigin::Close{reason}`、`fired_at=到期 watermark`，P3 钉死 | close 输出路径 `close_exec.rs`（v1 已定位 :202-214） |

**额外核对 "90% 已存在" 的代码承诺：**
- `JoinIndex` 存每键带时间戳列式行定位（buffer/mod.rs:41-68，`IndexedRow{ts_nanos,batch,row,index}` + `KeyedRows{rows,max_ts}`）✅
- `set_join_key` 自动配置 join 索引（bootstrap.rs:239，在 `configure_join_indexes` 内）✅
- `provider_snapshot`（side input 源）✅ 存在，但**路径写错**（见 N1）

---

## 2. 语义映射准确性（对照权威 SQL）

### Q9 — Winning Bids ✅ 准确
- 权威 SQL（NEXMARK_AUTHORITATIVE_SEMANTICS.md:414-418）：
  `ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC)` … `WHERE A.id=B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires` … `rownum<=1`
- 文档 §6.2：`within [a.dateTime, a.expires]`（注释"闭"= `BETWEEN` 两端含）✅；`reduce maxrow(price) tie(dateTime asc)`（=`ORDER BY price DESC, dateTime ASC`）✅；`on a.id == bid_events.auction as winner`（=`A.id=B.auction`）✅。**逐段对应，无偏差。**

### Q8 — Monitor New Users ✅ 准确（且比预期更对）
- 权威 SQL（:353-371）：**TUMBLE join**——person/auction 各自滚 10s 桶，`ON P.id=A.seller AND P.starttime=A.starttime AND P.endtime=A.endtime`（同桶关联）。
- 文档 §6.1：`within [p.dateTime, <bucket_end(p.dateTime,10s)]` + 注释"上开对齐 TUMBLE 桶 [B,B+10s)"。文档用 `[p.dateTime, bucket_end)` 而非 `[bucket_start, bucket_end)`，**依赖生成器"auction 必晚于其 seller 的 person"保证**——§9 风险#6 已显式标注。由于该保证成立，`[p.dateTime, bucket_end)` 是 TUMBLE 同桶语义的正确子集（不会漏匹配）。**映射正确**，风险已声明。

> 结论：两份最难的语义映射都经权威 SQL 核对无误，是这份稿子的强项。

---

## 3. 问题清单

### 🔴 N1（错误引用，会让读者找错文件）— §1 表 `provider_snapshot` 路径写错
§1 表格写："`provider 窗口（表背）| crates/wf-runtime/src/engine_task/window_lookup.rs:136 provider_snapshot → side input 现成`"。
**实测**：`provider_snapshot` 仅存在于 `crates/wf-engine/src/window/registry.rs:237`（`pub fn provider_snapshot`），crate 与文件名都错；不存在 `wf-runtime/.../window_lookup.rs:136` 这个符号。
**修复**：改为 `crates/wf-engine/src/window/registry.rs:237`（能力本身存在，"side input 现成"结论不变）。

### 🟡 N2（内部措辞矛盾）— `each <alias>` + `emit at` 与"互斥"打架
§5.2：「deferred 规则在 rule_task 增加挂起队列…**与 on-each 即时路径互斥（一条规则二选一）**」。
但 §6.1/§6.2 的 Q8/Q9 都写成 `each p` / `each a` **加** `emit at`（deferred）。若 deferred 与 on-each "互斥（二选一）"，那 `each a ... emit at` 同时是 on-each 又是 deferred，自相矛盾。
**修复（二选一）**：
- 重述 §5.2："`each <alias>` 只声明驱动事件 + entity/yield；**当 join 带 `emit at` 时整条规则转为 deferred 输出路径**，绕过 on-each 即时输出"——即"互斥"指"输出路径二选一（eager vs deferred per emit at），而非禁止 each 驱动 deferred 规则"；
- 或在 §6 加一行"rule shape 说明"：此处的 `each` 是 deferred 规则的驱动事件声明，不是 on-each 规则。
（这是措辞/可读性，不是结构缺陷；deferred 机制本身成立。）

### 🟢 N3（交叉引用版本）— stats 文档版本对不齐
§前置/§7 引用 `docs/stats-executor-design.md（v6 统一桶键模型）`。本次我评审的 stats 稿是 v2；若 stats 已演进到 v6，join 稿里"stats 与 join 正交""stats P5 桶内回查"等依赖项需与之保持同步。建议 join 稿落地时回填实际 stats 版本号，避免读者拿 v6 假设对 v2 实现。
（非阻塞；仅提示跨文档一致性。）

### 🟢 N4（小）— Q8 可用算术避免新内建
§6.1 用 `bucket_end(p.dateTime, 10s)`，文档已自承是新内建（R3）。但 `time_bucket(t,10)` 返回桶起始，故 `bucket_end ≡ time_bucket(p.dateTime,10) + 10s` 可用算术替代，避免新增内建。建议在示例里二选一并标注：要么明确 `bucket_end` 为 P1 依赖，要么给出算术写法。

### 🟢 N5（小）— "7 个涉及 join" 计数略松
§1 "NEXMark 22 个查询里 7 个涉及 join"。按权威语义实际落入 join 家族的是 Q3/Q8/Q9/Q13/Q20/Q22 = 6（Q5/Q7 已移出归 stats P5；Q4 归 stats 两级聚合）。"7" 可能是把 Q5/Q7 暂计入。建议改为 6 或注明口径。

---

## 4. 给作者的修订清单（按优先级）

| 优先级 | 项 | 位置 | 改法 |
|---|---|---|---|
| P0 | N1 错误引用 | §1 表 | `provider_snapshot` 路径 → `wf-engine/src/window/registry.rs:237` |
| P1 | N2 措辞矛盾 | §5.2 / §6.1-6.2 | 澄清 `each`+`emit at` 的 rule shape（互斥=输出路径二选一，非禁 each 驱动） |
| P2 | N3 跨文档版本 | 前置/§7 | 回填 stats 文档实际版本号 |
| P3 | N4 算术替代 | §6.1 | 给 `bucket_end` 的算术写法或标 P1 依赖 |
| P3 | N5 计数 | §1 | "7"→"6"或注口径 |

---

## 5. 结论

- **可以定稿方向**：模型、R1–R6 落实、Q8/Q9 语义映射、interval 读路径"90% 已存在"的代码承诺——全部经源码复核成立。
- **定稿前必改**：N1（provider_snapshot 错误路径，P0）、N2（each+emit at 与"互斥"措辞矛盾，P1）。
- **可并行小修**：N3–N5。
- 相比前几份设计稿，这份的引用准确率明显更高；本轮只揪出 1 处真错引 + 1 处自相矛盾，无结构性跑偏。
