# Join 算子族设计 — 评审稿

> 状态：评审稿 v1（2026-08-22，对照实际代码核实）
> 评审对象：`docs/design/join-family-design.md` v1
> 方法：逐断言对照 wp-reactor 源码；关键架构点（on-each 执行模型）委托探索 agent
> 结论：模型骨架成立，但 **§6 示例语法有两处硬伤**（on-each 无延迟承载点、`label.field` 引用不成立），必须修订后定稿。

---

## 1. 已核实成立的断言（代码证据）

| 设计断言 | 代码证据 | 结论 |
|---|---|---|
| interval 读路径现成：按 key 拿 `(ts, row)` | `WindowLookup::asof_candidates` 返回 `Vec<(i64, JoinRow)>` — `match_engine/types.rs:305`；`snapshot_with_timestamps` — `types.rs:276`；已被 `execute_joins` Asof 分支复用 — `context.rs:191-237` | ✅ |
| `JoinIndex` 存带时间戳列式行定位 | `buffer/mod.rs:41-68`（`IndexedRow { ts_nanos, batch, row }`）+ `lookup_timestamped` — `buffer/mod.rs:371` | ✅ |
| join 键索引自动配置 | `lifecycle/bootstrap.rs:239` `configure_join_indexes` → `set_join_key` | ✅ |
| provider 窗口（side input 现成） | `crates/wf-engine/src/window/registry.rs:237` `provider_snapshot`（定义）；`window_lookup.rs:136` 为调用点；`WindowConfig.table` | ✅ |
| join 后过滤已有（INNER 语义） | Q3/Q20 `where window.field`；`execute_each_with_joins` 中 `where_ok` 严格抑制 — `each_exec.rs:87-118` | ✅ |
| oracle 已有窗口状态 + WindowLookup（对拍前提） | join-field-as-key-design §5.2 已实施 | ✅（但见 R4：在独立仓库） |
| 归约 measure 复用 stats 度量语义 | stats-design §4.3/§5（`last`/`top(N)` 扩展度量） | ✅ |

## 2. 必须修正的点（按严重度）

### R1（严重）—— `on each` 无「延迟输出」承载点，Q8/Q9 示例语法不成立

**核实**：
- `EachPlan { alias, filter }` 无任何 window/duration/deadline/watermark 字段 — `wf-lang/plan.rs:75-80`。
- on-each 运行时**不参与到期扫描、无 flush 收口**：`scan_timeouts` 首行 `let Some(machine) = ... else { return; }` — `rule_task.rs:1585-1587`；`flush` 同理 — `rule_task.rs:1746-1748`。
- 输出即事件到即时发：`build_each_alert` `fired_at = event_time_nanos`、`origin = AlertOrigin::Event` — `each_exec.rs:1005-1022`；`window_start == window_end == event_time` — `each_exec.rs:1049-1056`。

**问题**：设计 §6 的 Q8/Q9 写成 `on each a -> score(...)` + deferred join。但现有 on-each 是**无状态、无窗、无 watermark 的即时输出**——"挂起到 watermark ≥ 上界再输出"在 `EachPlan`/`RuleTask` 里**没有承载点**。

**修正**（并入设计 §5.2/§6）：
- 明确 deferred join 规则是一条**新的 rule task 分支**（挂起队列 + watermark 到期扫描，复用 `scan_timeouts` 的到期机制），**不属于现有 on-each 即时路径**。
- 语法上建议**显式声明延迟**而非隐式推断，让作者一眼看出这是 deferred 规则。两个选项：
  - (a) `emit at <字段表达式>`（重新引入，作为 deferred 标记 + 触发点）；
  - (b) 由 `within` 上界为行内字段自动推断 + 文档强制标注。
  - **建议 (b)**（上界字段引用 = deferred，语法最简），但 P3 必须实现新执行路径，评审不把它当 on-each 扩展。
- 列式热路径（`execute_each_direct*`，`each_exec.rs:135-856`）deferred 规则不走，标注实现差异。

### R2（严重）—— `as winner` + `yield winner.bidder` 的 `label.field` 在现有 eval 不成立

**核实**：
- `field_ref_name` 对 `Qualified`/`Bracketed` **丢弃限定词，只返回叶子字段名** — `match_engine/key.rs:338-349`。
- 表达式求值 `eval_field_value` 从裸顶层字段名起跳 — `key.rs:371-397`；`execute_joins` 注入的 `window.field` **限定键不会被表达式读取** — `context.rs:263-275`（裸键 `or_insert_with` 先到先得）。

**问题**：`yield winner.bidder` 实际解析到**裸 `bidder`**。Q9 具体会出错：驱动 auction 行与胜者 bid 行**重名字段 `dateTime`、`extra`**——驱动行先注入，`or_insert_with` 先到先得 → `yield winner.dateTime` 拿到的是 **auction 的 dateTime**。现有 join 后过滤（Q3/Q20）能工作只因 `where` 只读裸键且无重名冲突；`label.field` 不是可靠机制。

**修正**（并入设计 §3/§4.2）——reduce 结果以**裸键 object value** 注入 + `FieldRef::Path` 解析（现有 Path 已支持从裸顶层字段遍历 object — `expr.rs:31-34`）：
- `ctx.fields["winner"] = Value::Object{...}`（整行）；`winner.bidder` 编译为 `FieldRef::Path(["winner", "bidder"])` → 真正 label 限定，无裸名冲突。
- P1 语法必答此点，否则 maxrow 的 `as winner` 悬空。

### R3（中）—— `bucket_end(...)` 内建不存在

**核实**：现有 `time_bucket(time, interval_seconds)` — `checker/types/infer.rs:172`、`check_funcs.rs:386`（返回**桶起始**、秒单位）。

**修正**：Q8 写法 `[p.dateTime, <bucket_end(p.dateTime, 10s)]` 需新增 `bucket_end` 内建，或用 `[p.dateTime, <time_bucket(p.dateTime, 10) + 10s]`（算术）。建议新增 `bucket_end`（对齐 tier 的桶键函数族）。

### R4（中）—— oracle 在独立仓库，P3 对拍是跨仓库工作

**核实**：`wfgen` 在 `warp-fusion/crates/wfgen`，不在 wp-reactor；join-field-as-key 引用的 `wfgen/src/oracle/mod.rs` 即该仓库。

**修正**：设计 §8 标注 P3 oracle interval join 落在 warp-fusion，跨仓库依赖 + 验收对拍脚本跨仓库。

### R5（低）—— interval 读路径精确化

**修正**：§5.1"加时间谓词分支"精确到「复用 `asof_candidates` / `snapshot_with_timestamps`，retain `[lo, hi]`」——无需新读路径。

### R6（低）—— deferred join 输出的 origin/emit 语义未定义

**修正**：现有 close 输出 `origin = AlertOrigin::Close{reason}`（`close_exec.rs:202-214`）。deferred join 输出需定义 origin（复用 Close 或新增）与 `fired_at`（= 到期 watermark）。P3 实现时钉死，影响对拍断言。

---

## 3. 结论

- **模型骨架（时态键查找 × mode × within × 触发）成立**：interval 读路径现成，Q8/Q9 归并为 deferred 同一机制的核心判断站得住。
- **§3/§4/§6 需按 R1/R2 修订后定稿**：Q8/Q9 从 `on each` 改为显式 deferred 规则形态；`as winner` 改为真 label（object 注入 + Path）。
- **R3/R4 为文档事实修正**，不影响设计结构。

修订后的示例形态（预演 R1/R2 修正）：

```wfl
// Q9：deferred 规则（非 on-each 即时路径）+ 真 label
rule q9_winning_bid {
    events { a : auction_events }
    each a                                  // 驱动；输出延迟到 a.expires
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
    entity(digit, a.id)
    yield nexmark_alerts (id = a.id, detail = fmt("winner {}", winner.bidder), ...)
    // 运行时：挂起至 watermark ≥ a.expires → 归约 → winner 整行以 object 注入
}
```
