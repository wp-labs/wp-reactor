# Q13（Bounded Side Input Join）对齐进度 — 接力手记

> 2026-08-23 更新。本文件是跨 session 接力点：重开 session 先从「## 当前状态」读起。

## 目标

Q13 从「🟡 形状对齐」真正对齐权威语义：

```sql
SELECT B.auction, B.bidder, B.price, B.dateTime, S.`value`
FROM bid B JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
ON mod(B.auction, 10000) = S.key
```

现行 q13.wfl 已拆双规则链 q13a（on each bid → bid_mod 中间窗口）→ q13b（on each m + join side_input snapshot + yield detail=side_input.value）。每 bid 一行，按 mod(auction,10000) 查有界侧输入静态表（key 0..9999）富化输出。

## 当前状态（截至 2026-08-23 23:25）—— Q13 已对齐，全部验证通过

### ✅ 已完成并验证

0. **【本 session】端到端卡点根因定位并修复（knowdb CSV 类型推断）**
   - **根因**：`bootstrap.rs` knowdb CSV 加载把**所有列都存成 `Value::Str`**（无类型推断）→ provider join 索引键 `JoinKey::Str("2345")` 与 lookup 键 `JoinKey::Number(bits(2345.0))`（`b.auction % 10000` 是 f64 mod）**类型不匹配 → 每次索引 lookup miss → 回退 O(rows)=10000 行全表扫描** → 索引形同虚设，q13b 卡死
   - 诊断证据链：Q13DIAG 日志确认 set_join_key 已调（索引在路径上）→ rule_task.rs L923 确认列式 join 路径用 `RegistryLookup`（有索引分支）→ profiling 确认 `serialize_nanos` 230s/4min（列式 join 执行被计入 serialize 采样）→ bootstrap.rs L436 实锤全列 Str
   - **修复**（`crates/wf-runtime/src/lifecycle/bootstrap.rs`）：新增 `infer_knowledge_value(cell)`（数字串→Number、true/false→Bool、其余→Str，`is_finite` 防 NaN/Inf），CSV 与 PG 加载共用
   - 新增测试：`infer_knowledge_value_types_numeric_bool_and_string`（bootstrap.rs）+ `load_knowledge_numeric_csv_join_index_hits_number_key`（bootstrap_r4.rs，q13 回归：CSV 数字 key 进 Number、索引命中）
   - **实测（`TOTAL=1m ./bench.sh q13 replay 1m`，23:19:55）**：
     - `q13a_bid_mod` EMIT = **920,000** ✅；`q13b_side_input_join` EMIT = **920,000** ✅（此前 55,917）
     - RSS_peak 1,615MB → **494MB**；serialize_nanos 230s → **0.49s（467×）**；SUMMARY clean

1. **ProviderWindow join 索引**（`wf-engine/src/window/provider/mod.rs`，未提交）
   - `set_join_key()` 建 O(rows) 哈希索引（键类型 `JoinKey`，与 buffer 窗口同截断语义）
   - `join_lookup(&Value)` O(1)；`load()` 替换 rows 后自动重建索引
   - 单测 `set_join_key_builds_index_and_lookup_is_o1` 通过

2. **window_lookup.rs provider 分支接线**（`wf-runtime/src/engine_task/window_lookup.rs`）
   - `join_lookup` provider 分支：有索引走索引（O(1)），无索引回退全表扫描
   - 16 个 window_lookup 单测全过

3. **spawn.rs set_join_key 覆盖 provider 窗口**（`wf-runtime/src/lifecycle/spawn.rs`）
   - 与 buffer 窗口同路径设置（首 join 条件右字段）
   - ~~临时诊断日志 Q13DIAG~~ **已移除**（23:22 重建后确认日志零残留）

4. **flush_pipes 真实 seq + Notify**（`wf-runtime/src/engine_task/rule_task.rs`）
   - 广播从固定 u64::MAX 改为 append 返回的真实 seq（下游 ack 反映真实消费进度，否则 bench 完成判定提前 SIGTERM）
   - append 后显式 `notify_waiters()`（pull 下游消费不停滞）
   - 对应更新测试 `pure_relay_broadcasts_to_sharded_downstream`（断言 seq==0 而非 u64::MAX）

5. **性能基准 `q13b_join_bench`**（`wf-engine/src/match_engine/tests/q13b_join_bench.rs`，**已注册到 tests/mod.rs**）
   - 运行：`cargo test --release -p wf-engine q13b_join_bench -- --ignored --nocapture`
   - **实测数据（N=1M × 10000 行 side_input，release）**：

     | 形态 | ns/行 | 相对基线 |
     |------|-------|----------|
     | no-join 列式 each（基线） | 233.2 | 100% |
     | join + 全表扫描 | 141,227.1 | 60,559%（606× 退化） |
     | join + 哈希索引 | 446.2 | 191% |

   - **索引相对全表扫描加速 316.5×**，防御断言（>50×）通过

6. **端到端 10m verify（23:20:46，`TOTAL=10m ./bench.sh q13 replay 10m --verify`）**：
   - 引擎 EMIT vs oracle（真实规则引擎 ground truth）**identical ✅**：q13a/q13b 各 9,200,000
   - EPS=449,346；RSS_peak=9,362MB（见 ⚠️ 观察 1）；SUMMARY clean
   - 注：oracle 侧 q13a=q13b=9,200,000（join miss 的 bid 也计数，snapshot 外联语义）

7. **回归全绿**（最终二进制，Q13DIAG 移除后）：
   - `cargo test -p wf-engine --lib`: **1054 passed** / 0 failed / 39 ignored
   - `cargo test -p wf-runtime --lib`: **485 passed**（含本次新增 2）/ 0 failed / 1 ignored
   - `cargo test -p wfgen --lib`（warp-fusion workspace）: **149 passed**
   - `./bench.sh q13 replay 1m`（最终二进制）: 920,000/920,000 clean
   - `./bench.sh q20 replay 1m --verify`（join 代表查询回归）: **identical ✅**（196,517 = 196,517）

8. **文档**：`docs/design/provider-window.md` 已加「Join 索引」一节（含实测表）；`wf-examples/.../docs/CAPABILITY_GAP_MATRIX.md` Q13 🟡→✅（汇总 20 已有 / 特殊口径 2→1）

### ⚠️ 观察（非阻塞，待后续确认）

1. **10m 时 RSS_peak=9,362MB**：bid_mod 中间窗 over=2d 且事件时间跨度仅 ~30min（10m 数据）→ 窗口永不驱逐，9.2M 行全保留。这是双规则链物化中间窗的代价（权威 SQL 是 CTE 不物化）。1m 数据 494MB。若需压内存：缩中间窗 over 或评估中间窗是否需要逐出策略（勿擅自改 windows.toml 语义）。
2. **全量 22 查询 10m --verify 未跑**（`./bench.sh all replay 10m --verify` ~40min 串行）：本次改动只影响 knowdb 加载（仅 q13 用 provider），q1-q22 其余查询不引用 side_input，风险面为零；q20（活 join）已抽查无回归。

## 模型/配置层（wf-examples，已提交 `6a43401` 后新增、未提交）

- `models/schemas/nexmark.wfs`：+`bid_mod` 中间窗口、+`window<provider> side_input`
- `models/queries/q13.wfl`：双规则链 q13a/q13b（含 3 个 inline test：mod_key / hit / miss）
- `models/schemas/windows.toml`：+`bid_mod`（2GB/2d）、+`side_input`（table / over_cap=0s）
- `knowdb.toml` + `side_input/side_input.csv`（key 0..9999 → value-<key>；CSV 数字列现在被推断为 Number）

## oracle 中间管道（warp-fusion，已提交 `cac755a` 后新增、未提交）

- `oracle/mod.rs`：中间输出（yield target 被 bind）既 push_alert 又转 Event feed 下游（多级队列）
- `cmd_verify_nexmark.rs`：并行分组从 chunks 改为并查集按 yield-bind 依赖合并同组
- 验证：oracle q13a=9.2M、q13b=9.2M（10m，与引擎 identical）✅

## Pitfalls（已踩坑，勿重蹈）

- **`mod` 是关键字**：`use crate::...::tests::mod` 编译错；且 `match_engine::tests` 模块私有。q13b_join_bench.rs 里这行死代码已删
- **不要用 `Value` 做 HashMap key**：无 `Hash` trait，编译 E0599。用 `JoinKey::from_value(v)`
- **oracle 中间 feed 必须同组**：verify 的 chunk 并行分组成破坏跨组依赖 → 已改并查集
- **broadcast seq=u64::MAX 让下游 ack 失真**：bench 完成判定提前 SIGTERM → 必须真实 append seq
- **flush_pipes 直接 append 不走 actor 不触发 Notify**：pull 下游消费停滞 → append 后显式 notify_waiters
- **ProviderWindow 全表扫描是设计意图注释的**：「small static snapshots, O(rows) per lookup is expected」——但 10000 行 × 92 万事件卡死，需索引
- **knowdb CSV 全列 Str 是隐形炸弹**：数字列 join 数字表达式时索引键类型不匹配（Str vs Number）→ 每次 miss → 回退全表扫描。类型推断已在 bootstrap.rs 修复；改加载逻辑务必同步看 `JoinKey::from_value` 两侧类型
- **`wfusion` binary 在 warp-fusion workspace**（`warp-fusion/target/release/wfgen|wfusion`），不是 wp-reactor；bench.sh 用的就是这两个
- **macOS 无 `timeout` 命令**：用 terminal 工具的 timeout_ms 控制跑批

## 文件清单（未提交修改）

**wp-reactor**（`git status` 可见）：
- `M crates/wf-runtime/src/lifecycle/bootstrap.rs`（本 session：CSV/PG 类型推断 `infer_knowledge_value` + 单测）
- `M crates/wf-runtime/src/lifecycle/bootstrap_r4.rs`（本 session：q13 回归测试）
- `M crates/wf-engine/src/window/provider/mod.rs`（索引）
- `M crates/wf-runtime/src/engine_task/mod.rs`
- `M crates/wf-runtime/src/engine_task/rule_task.rs`（flush_pipes 真实 seq + notify）
- `M crates/wf-runtime/src/engine_task/window_lookup.rs`（provider 索引分支）
- `M crates/wf-runtime/src/lifecycle/spawn.rs`（set_join_key 覆盖 provider；Q13DIAG 已移除）
- `M crates/wf-runtime/src/engine_task/tests.rs`（pure_relay 断言 seq==0）
- `?? crates/wf-engine/src/match_engine/tests/q13b_join_bench.rs`（bench，已注册 mod.rs）
- `M crates/wf-engine/src/match_engine/tests/mod.rs`（+mod q13b_join_bench）

**warp-fusion**：oracle 中间管道 + verify 并查集分组（未提交）
**wf-examples**：q13.wfl + schemas + knowdb + side_input/ + `docs/CAPABILITY_GAP_MATRIX.md`（Q13→✅）（未提交）

## 下一步（重开 session 从这里继续）

1. （可选）`./bench.sh all replay 10m --verify` 全量 22 查询回归（~40min；风险面已知为零，见 ⚠️ 观察 2）
2. （可选）跟进 ⚠️ 观察 1：10m 下 bid_mod 中间窗 9.3GB RSS（over=2d 不驱逐属预期，仅记录）
3. 提交三个仓库（wp-reactor / warp-fusion / wf-examples）——commit message 建议：
   - wp-reactor: `perf(wf-runtime): knowdb CSV 数字列类型推断——provider join 索引键与 lookup 键同类型（q13 有界侧输入 join 920k/920k，10m oracle identical）`
   - warp-fusion: `feat(wfgen): oracle 中间输出 feed 下游 + verify 并查集分组（q13 双规则链对拍）`
   - wf-examples: `feat(nexmark_pk): q13 有界侧输入 join 权威语义对齐（双规则链 + side_input provider + 矩阵 🟡→✅）`
