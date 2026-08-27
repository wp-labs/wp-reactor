# Changelog

All notable changes to wp-reactor will be documented in this file.

## [Unreleased]

### Added

- **wf-lang: `shared` 公共允许列表（issue #73）**——`shared <name> = ("a", "b", ...)` 顶层声明 + 规则内 `expr in <name>` / `expr not in <name>` 引用，一处定义、多处引用。
  - 解析：`Expr::ListRef` 占位（仅 `in <ident>` 右值产出）；声明必须在规则之前（与 `yield preset`/`pattern` 同文法位置）。
  - 编译：`compile_wfl` / `compile_wfl_with_diagnostics` 先 `resolve_shared_list_refs` 展开为字面列表——checker/运行时只见字面 InList，元素类型检查与手写列表逐字节等价。
  - 错误面：未知名列表 → 编译错误（带规则名+列表名）；同名重复声明 → 编译错误；元素不支持嵌套 shared 引用。
  - prelude：`_global.wfl` 允许 `shared` 声明，prelude 合并并入每个规则文件（wfgen `load_wfl_files` + wf-runtime `lifecycle/compile.rs` 同步，重名冲突报错）。
  - lint：`wfl lint` 先展开再 check（不绕过编译期错误）。

- **wf-lang / wf-engine / wf-runtime**: HOP sliding window operator — `match<key:hop(size, slide)>`（`size % slide == 0`）。每个事件扇入 `size/slide` 个覆盖窗口（epoch slide 对齐），窗口在 `w_start + size` 收口（slide 对齐）。
  - 引擎：`advance_at_with_diagnostics` 尾部抽取为 `advance_window(scope_key, window_start)` 助手，HOP 逐窗口扇出并 `merge_step_outcome` 合并结果；`expire_time_for`/`close`/expiry 堆均按 hop 窗口收口。
  - 扫描：hop 规则 per-event 扫描用无界预算（每 slide 边界恰一个窗口到期，关闭数受窗口内键数约束）——1024 预算会把同一窗口关闭拆多批、inline conv 逐批 top-1 造成同窗口重复 EMIT。
  - conv：`conv` 块对 hop 窗口放开（checker），inline conv 每窗口一个收口批。
  - oracle（wfgen）：hop 扫描步长 = slide 边界 + 无界预算，与引擎口径一致。
  - 已知 v1 局限：hop + OR-mode `on event`（无 close 块）时，同一事件的多窗口 fire 经 `merge_step_outcome` 仅返回一个 Matched——每事件输出一个窗口命中（`and close` 形态不受影响，输出在窗口收口扫描逐窗口完整）。
  - **conv `top_ties(N)`（RANK 语义并列全输出）**：`sort(...) | top_ties(N)` 取前 N 条并保留所有与第 N 条排序键等值的条目（并列全出）。checker 要求前导 `sort`；Q5/Q7 并列语义对齐（Q5 窗口并列最高 count、Q7 窗口并列最高价 auction 全输出）。修复 `top_ties(0)` 越界 panic（退化为空输出）。
  - **测试**：新增代码（HOP 引擎/语法链/rule_task 扫描 + conv `top_ties`）测试覆盖率 **100%**（cargo llvm-cov 新增行口径，全 workspace 2587 测试全绿）；rule-task 级 hop 集成测试 4 例（slide 边界逐窗收口、多 key 独立收口、size==slide 等价 fixed、conv-sink 无界扫描路由 + flush drained），编译器/explain 补 hop 窗与 top_ties 格式/编译臂，lifecycle 补管道 hop stage `over=size` 臂。

## [1.2.0] — 2026-08-19

### Added

- **wf-engine / wf-runtime**: Sharded `match` rules are now deferable — `RulePush.shard_rows` broadcasts the shared batch + per-shard row subset (`events=None`), with columnar sharding byte-identical to the row path; `route_parse` no longer excludes sharded windows from deferred broadcast. Q2 **~8M → ~17.86M** (+123%), EMIT 747 816 exact.
- **wf-engine**: Key sharding moved off the window-actor hot path — `precompute_shard_rows` partitions batches in the parallel parse stage and the actor reuses the result (defensive re-partition fallback). Cuts actor-side partition CPU ~98%, but the wall is per-row partition cost, not its location — hence `ScopeKey`.
- **wf-engine**: `ScopeKey` — typed match-key enum (`Empty`/`Int`/`Float`/`Str`/`Pair`) read straight from columns, shared by columnar/row sharding (identical below `2^53`); replaces the `Vec<Value> → string → FNV` shard-hash chain.

### Changed

- **wf-engine**: Guard evaluation is now a whole-column vectorized kernel (each `ColumnExpr` node computes a typed `CVec` for the whole batch; byte-identical semantics — SQL three-valued `&&`/`||`, native `i64`, epsilon float, `>2^53` divergence, null propagation); per-row `eval_cx` removed.
- **wf-engine**: Q1 output commit is batched unconditionally (`WF_L3_BATCH` gate removed) — the per-row `Vec::push`×10 + `fill_row_gaps` scan becomes one `commit_each_rows_batch` per segment.
- **wf-engine**: Q1 last materialization removed — numeric `entity == yield` fields are written straight as raw `f64` (byte-identical to the `Value` path for Digit/Float/Chars/untyped targets).
- **wf-engine**: `InstanceKey` migrated to `ScopeKey` — instance lookup is serialization-free.
- **wf-runtime**: Deferred hit rows materialize via the `materialize_fields` projection (`materialize_rows_filtered`) instead of full `materialize_rows`.

### Fixed

- **wf-engine**: Block-level fill misplaced gaps in sparse segments (`[real0, real2, fill]` → `[real0, fill, real2]`), now row-order aligned with `fill_row_gaps`; pinned by equivalence tests.
- **wf-runtime**: Relay/eager push used the batch row count when `events=Some` but `batch=None`, hanging `downstream_close`.
- **wf-engine**: A null/missing entity on the columnar path now emits the empty string (aligned with the eager `Event` path) instead of erroring + continuing.

### Performance

- **wf-engine**: Q2 sharded deferral `~8M → ~17.86M` (+123%); regression suite `[clean]`.
- **wf-engine**: `ScopeKey` shard hash — Q2 `~18M → ~34M` (×2), Q5 `3.7M → 5.23M` (+41%), Q7 `3.5M → 5.02M` (+43%); Q1 unchanged.
- **wf-engine**: `InstanceKey → ScopeKey` — Q2 31.9M; Q5/Q7 4.7–5.0M under load (+35%+).
- **wf-engine**: Q1 on-each third round -13% (batch-level column index, typed `EntityCol`, batched wfx prefix) — single-thread E1 `621.6 → 140.6 ns/row` (-77%); entity==yield f64 write → 13.79M @100m.
- **wf-engine**: L3 batch commit — 100m EPS **+305%**, CPU -16%, RSS halved, EMIT 92M exact.
- **wf-engine**: P3 guard kernel — no Q2 gain (window-actor single-writer wall); the per-event CPU win is deferred until that wall falls.

### Documentation

- **Design**: Added `docs/design/columnar-match-state-machine.md` (sharded no-materialization design); corrected the Q2 acceptance target (~90M receive-only is not a sensible goal).
- **Design**: `q1-throughput-bisection.md` §23 — the EMIT end-of-run sampling artifact (81.65M vs 92M is exporter flush lag, not ~11% row loss); `[clean]` is the integrity authority.
- **Design**: `columnar-execution-progress.md` — residual-materialization A/B/C survey; none are current hot spots → no blind fixing.
- **Design**: Stale-doc cleanup — removed `reviewed.md`, fixed 8 dead links, corrected the `error-handling.md` crate-boundary table.

## [1.1.0] — 2026-08-18

### Added

- **wf-lang**: Added `expr_is_columnar` — a static, conservative columnar-expression gate. It classifies the pure field-arithmetic / comparison / constant subset (literals, flat `Simple`/`Qualified`/`Bracketed` field refs, `!`, and arithmetic / comparison / logic binary ops) as columnar; nested `FieldRef::Path`, `FuncCall`, structured literals, `InList`, `IfThenElse`, and meta/system/preset vars fall back to the interpreted path. It is a pure AST predicate (no per-row work), evaluated against a rule's immutable expressions.
- **wf-lang**: Added `defer_materialization` to the window field-usage analysis — a window is marked deferable only when every rule bound to it has a **columnar** bind filter.
- **wf-engine**: Added the columnar guard evaluator (`ColumnarBatch`, precompiled `ColumnExpr`, `eval_guard_columnar`, `GuardMasks`, `mask_to_indices`) — reads native Arrow columns directly (no per-row `HashMap`/`Value` materialization) and produces one boolean per row. `%` and comparison over two `Int64`/`Timestamp(Ns)` operands use native `i64`; `+ - * /` and any mixed `i64`/`f64` operand stay f64 (matching interpreted). `==`/`!=` over floats keep the interpreted epsilon comparison. Null / missing / non-boolean rows are emitted as **null slots**, so two-valued (`must be true`) and three-valued (`permissive`) consumers both get correct semantics.
- **wf-engine**: Added `RuleExecutor::branch_guard_masks` — precomputes columnar branch-guard masks for **event steps, close steps, and seq negation steps** keyed by `(step, branch)`, consumed by `advance_at_with_masks`.
- **wf-engine**: Added L2 deferred-materialization primitives — `materialize_rows` / `materialize_rows_filtered` (materialize only the listed row indices) and `batch_time_col_index` / `batch_event_time_nanos_at` / `batch_event_time_nanos` (read event time straight from the time column with the same f64 round-trip as the interpreted `extract_event_time` path).
- **wf-engine**: Added `ColumnarEvent` — a per-row view reading field values straight from Arrow columns, byte-identical to the eager `Event{HashMap}` because it shares the same `extract_field_value` conversion (Int64/Timestamp → f64 display round-trip, null → field missing). Added `sorted_fields_for` — the batch-level pre-sorted field table that the on-each hash renderer reuses instead of collecting + sorting per event.
- **wf-engine**: Added `each_plan_columnar_safe` + `execute_each_direct_batch_columnar` — the on-each columnar batch executor (gated to constant score / entity field-or-literal / flat yield fields / absent-or-columnar bind filters). It renders `wfx_id` straight from the pre-sorted field table + column bytes (`write_flat_column_scratch`) with zero `Value` construction and zero string clones; fired_at / score / emit share the eager implementations.

### Changed

- **wf-engine / wf-runtime**: End-to-end L2 deferred materialization. `route_parse` broadcasts the raw `RecordBatch` (zero-copy) for deferable non-sharded windows instead of always materializing `Vec<Event>`; `RuleTask::process_batch` scans the time column over **every** row (watermark / expiry) but materializes only the bind-filter hit rows and advances only those — preserving the per-row scan-then-advance interleaving for short windows that expire within a batch. Deferral applies to state-machine rules (bind-filter hit rows only) and `on each` rules (raw-batch broadcast; the rule task materializes the field whitelist itself) with debug detail logging off; sharded windows keep the eager path.
- **wf-engine**: `RulePush.events` is now `Option<Arc<Vec<Arc<Event>>>>`, `RulePush` carries the window's `materialize_fields` whitelist so a deferred rule task can rebuild the same field set as the eager path, and `RuleFanout` gains `broadcast_batch_only` (raw-batch broadcast; sharded subscriptions whose row indices no longer match the whole batch are excluded). `WindowParams` / `Window` carry `defer_materialization`.
- **wf-engine**: Columnar `Int64` / `Timestamp(Ns)` `%` and comparison are now native `i64` — **more precise than the interpreted f64 path for `>2^53` integers and nanosecond timestamps** (e.g. `2^53 == 2^53+1` is now `false`). This is a documented semantic divergence (§3.4 of the design doc), not a regression; below `2^53` the two paths are bit-for-bit identical.
- **wf-runtime**: `columnar_each` fast path in `RuleTask::process_batch` — for stateless `on each` rules with a columnar-safe plan, hit rows come from the (absent-or-columnar) bind-filter masks and execute straight off the columns, skipping `materialize_rows` (Q1 materializes 100% of rows) and the per-row event loop. Independent of `defer_materialize` (which requires a state machine).
- **wf-engine**: `content_bytes` for `Utf8` / `Binary` columns is now O(1) per batch (offsets difference) instead of summing `str::len` per row.
- **wf-config**: `HumanDuration` now accepts an `ms` suffix (previously integer seconds only) — unblocks sub-second metrics export intervals (`report_interval = "100ms"`).

### Performance

- **wf-engine**: Columnar guard evaluation is ~15× faster than interpreted per-event (14.3 ns/event vs 216.9 ns/event on the `guard_bench` micro-benchmark, release, 1M rows). End-to-end Q2 EPS is unchanged (the throughput gate is the window-actor single-writer wall, not guard cost), so the gain is per-event CPU, not EPS.
- **wf-engine**: Q1 `on each` fully columnar — end-to-end EPS **10M → 12.1M (+20%)**, three rounds stable under load 8.7–13.3. Bisection attribution (§15 of `q1-throughput-bisection.md`): builder output fill is **~63% of per-row work** (cut C upper bound 12.1M → 32.3M for the L3 output columnarization); wfx_id hashing ~1–2%, fired_at formatting ~2–5%, entity stringification ~0–2% — all already near-optimal.
- **wf-config / bench tooling**: Fixed metrics exporter quantization — the 43/55/84M plateaus were 1s export-interval aliasing of a ~95M true ingress (verified byte-identical CPU work across plateau rounds); with `report_interval = "100ms"` + 0.1s polling the stable reading is ~95M (88–108M band).
- **wf-engine**: `wfx_id` on the on-each path now hashes **rule name + event time (+ origin) only** — fields no longer participate (semantic change: real streams cannot produce two events in the same nanosecond). Per-row cost 249.6 → 30.1 ns (micro-bench, -88%); single-thread baseline -44%. Match rules keep the scope-key path.
- **wf-runtime**: `AlertColumnBuilder` is now **resident** across flushes (only the sealed columns leave the pending slot; layout cache re-resolved on the reused builder's first row) and `ALERT_BATCH_SIZE` raised 256 → 4096 (flush frequency ÷16) — the ~390k builder new/finish/drop cycles per 100M rows are gone.
- **wf-engine**: **Batch-level constant-yield caching** — literal yield fields (`alert_type = "..."`, `request_count = 1`) are coerced/exported once per batch and registered via `register_yield_column` (`YieldCol.const_value`); the per-row loop skips their staging and `fill_row_gaps` fills the constant. Micro-bench baseline 275.1 → 215.3 ns/row (single-thread total **621.6 → 215.3 ns, -65%** since the columnar landing).

### Documentation

- **Design**: Added `docs/design/columnar-execution-design.md` (overall columnar execution plan, L0–L5 layering, type-mapping / semantic-equivalence contract) and `docs/design/columnar-execution-progress.md` (step-by-step implementation log, guard coverage 85.3%, Q2 baseline and re-test data).
- **Design**: `docs/design/q1-throughput-bisection.md` extended to §15 — the cut-by-cut bisection map (recv → parse → dispatch → window actor → broadcast → rule `process_batch` → sink), the tool-fix evidence (§12), the on-each columnar landing (§14), and the Q1 per-row budget attribution (§15: output fill 63% / wfx_id 2% / fired_at 3% / entity 0% / diffuse 30%).
- **Design**: `docs/design/q1-throughput-bisection.md` §16–§21 — micro-benchmark methodology (`each_bench`, real-shape data + 10-thread concurrency simulation), parallelism scans (r=2/6/10, 6:6:6 — total throughput is near-linear in workers, 10:10:8 optimal), wfx_id semantic change, builder residency, constant-yield caching, and the CPU-over-subscription analysis (22+ engine threads on ~12–13 usable cores).

## [1.0.2] — 2026-08-17

### Changed

- **wf-runtime / wf-config**: The preread parse budget (`parse_buffer_bytes`) now charges a batch's **content** bytes — `wf_engine::window::content_bytes`, ≈ wire size, Arrow buffer padding excluded — instead of `get_array_memory_size`. Arrow IPC decode structurally over-counts the latter ~10× (measured 2026-08-17: a bid-like batch of 71 B/row wire content accounts as ~718 B/row, independent of field width — IPC reader buffer-view sharing), which starved the source → parse → commit pipeline to a handful of slots (the first wall) even though the real in-flight footprint is wire-sized. Charging content aligns the budget with the window mailbox accounting (`content_bytes + events_bytes`). Applies to `push_decoded_batch` and the Arrow IPC file-replay path. NB the budget now bounds *content* bytes in flight — decoded RSS under a downstream stall can approach ~10× the configured value.
- **wf-config**: Default `parse_buffer_bytes` lowered 256 MiB → 128 MiB (≈ 18 slots for 8 MiB frames). Under content accounting, 256 MiB (~36 slots) lifts q1 100M EPS to 6.25–6.66M but raises RSS to 12–14.5 GB (from 4.4 GB under the old decoded-accounting default), while 128 MiB lands at 6.13M / 5.88 GB — a small throughput gain at a modest RSS step-up, short of the plateau. Raise explicitly for more throughput (256 MiB ≈ 6.3–6.7M / 12–14 GB, 512 MiB ≈ 7.0M, 1–2 GiB ≈ 7.5M+; 4 GiB over-deepens and regresses).

### Fixed

- **wf-runtime**: The preread budget charged decoded Arrow allocation size rather than content bytes, structurally under-admitting batches by ~10× and collapsing the default budget to ~2 slots (P0-② first wall). A batch whose inflated accounting exceeds the whole (floored) budget but whose content fits is now admitted when exactly its content is free (regression: `preread_budget_charges_content_bytes_not_decoded_inflation`).

### Documentation

- **Design**: Reworked `docs/design/concurrency-scaling.md` around the stable double-wall model — P0-② resolves the first wall (decoded-size accounting) and recasts the budget as a pipeline-depth throttle (1–2 GiB content sweet spot; 4 GiB over-deepens window reorder and regresses to ~5.9M); corrected the decode-inflation coefficient (40× → ~15× for 100k frames: ~7.7 MB wire → ~116 MB decoded; ~10× per-batch accounting over-count); reclassified the pure-copy 18.2M probe as sustained-rate but non-steady-state; settled C-UCP=4 / W-RDP=4 with 100m resource curves and the 30m accounting trap; recorded the q1 100M stable baseline (EPS 5.93M ± 0.01M / RSS 4.4 GB / CPU 714–723%); showed balanced sharding is orthogonal to EPS (overturning the s0-straggler hypothesis); merged P0-① into P0-② in the priority table.
- **Design**: Added the P0-② experiment record to `docs/design/preread-budget-design.md` §6 (content-accounting budget curves: 256 MiB → 6.25–6.66M, 512 MiB → 7.02M, 1 GiB → 7.56M, 2 GiB → 7.58M q1 / 7.23M q2 (RSS 7.6 GB, half the old 14.8 GB), 4 GiB → 5.9M overshoot) and the 256 → 128 MiB default decision.

## [1.0.1] — 2026-08-17

### Fixed

- **wf-runtime**: Fixed a shutdown flush race in the alert/sink pipeline. `run_sink_consumer` previously exited after a single `try_recv` drain when the cancel token fired, dropping alerts emitted by rule tasks during the shutdown flush — rule tasks evaluate closes and emit their final alerts as part of graceful shutdown, after the sink consumer had already stopped. The sink consumer now keeps consuming the alert channel until all producers drop it (channel closed) or the `SINK_DRAIN_BUDGET` (1s) expires, so alerts produced by the shutdown flush reach the sinks. Reproducible with TCP source + manual shutdown (the previous `e2e_mvp`-style run lost the close alert while the same rule fired via window timeout in file+batch mode).
- **wf-runtime**: Fixed the same shutdown flush race in the sharded conv stage. `ConvStageTask`'s cancel path called `drain_and_drop` (a single `try_recv` drain) and exited before shards flushed their final `ConvCloseBatch`, losing complete buckets for rules with `conv_window` (rule-sharding P2c). The cancel path now consumes the close channel until all shard senders drop it (channel closed) or `CONV_DRAIN_BUDGET` (1s) expires, then drops still-unsealed (partial) buckets as before — P2④ semantics are preserved (partial top(N)/sort results are never emitted).

## [1.0.0] — 2026-08-17

### Added

- **wf-lang / wf-engine**: Added multi-level nested field extraction from `object` / `array` fields in yield expressions (`s.roles_obj.source.process.uid`, `s.roles_obj.related[0].process.name`). A `FieldRef::Path` validates the root field statically and walks nested members / integer indices at runtime; any missing member, out-of-bounds index, or intermediate type mismatch yields an omitted yield field (chars targets degrade to the empty string) without failing the record. Nested paths work inside structured `object { }` / `array [ ]` yield members in match/close rules too (their root fields are tracked into the eval context). Match keys and join conditions stay single-level — nested paths there are rejected by the checker, and `count` / `sum` / `avg` / `min` / `max` / `first` / `last` / `collect_*` reject nested paths as arguments (no column to aggregate); `window.has(nested.path)` infers the lookup column from the leaf member. (wp-labs/warp-fusion#64)
- **wf-lang / wf-engine**: Added `on event<accu>` — within-window accumulation. After the block fires, count and evidence keep accumulating without reset, and each subsequent qualifying event re-fires with the running cumulative values (`count 2, 3, 4, 5 …` with full evidence), until the window expires. Orthogonal to `seq` / `any`; scoped to a single `on event` step with no close block (the checker rejects `accu` with `on close` / `and close`, `on event seq` chain syntax, or multiple steps). A `max_throttle`-suppressed re-fire drops the alert but keeps the running accumulation (it does not reset the count). `wfl explain` renders the block as `on event<accu>`. (wp-labs/warp-fusion#65)
- **wf-engine**: Added cross-shard rate-limit / budget atomics (`SharedLimits`) for rule sharding (rule-sharding design P2b): a sharded rule's `max_throttle`, `max_instances`, and `max_memory_bytes` are enforced **collectively** across all shards via shared `Arc<Atomic…>` state — a shared sliding-window throttle (collective emits ≤ `count` per window), an exact CAS instance reservation, and a rule-wide fail-rule latch — instead of per-shard limits that multiplied the budget by the shard count. `CepStateMachine::with_limits_shared` builds a shard sharing one `SharedLimits`; `with_limits` / `new` are unchanged so `shards=1` keeps the exact per-machine path. `rule_instances` is now a delta-summed gauge across shards.
- **wf-lang**: Fixed-window `conv` rules now compile a `RulePlan.conv_window` (`ConvWindowPlan` — fixed bucket length `over` + scope keys), marking them shardable; sliding/session conv stays inline and non-shardable. (rule-sharding P2c)
- **wf-engine / wf-runtime**: Sharded `conv` rules aggregate raw close outputs **across shards** in a new `ConvStageTask` (transform operator): each shard routes raw qualifying closes (one aggregated batch per processed batch, max event-time watermark barrier) to the stage; the stage buckets by the fixed window `over`, seals a bucket only once every shard's watermark passes its end (a slow shard never loses closes), applies `apply_conv` over the merged batch (global top-N / sort), applies the shared rate limit, and emits to the sink. EOS / drained flush is the correct exit for complete data; cancel drops unsealed (partial) buckets instead of emitting wrong top(N)/sort results; a stalled barrier (30s without advance) drops stuck buckets with a warning to bound memory after a panicked shard. `close_is_qualified` / `apply_conv` are exported for the stage. (rule-sharding P2c)

### Fixed

- **wf-engine**: `max_instances` under sharding is now exact — admission uses a CAS reservation (`try_reserve_instance`) instead of a read-then-act check that could overshoot by up to `shard_count-1`; `DropOldest` evicts the local oldest and re-reserves, rejecting new keys when the shared budget is held by other shards. `max_memory_bytes` stays approximate under sharding (documented: memory grows non-atomically).
- **wf-runtime**: The conv stage now honors `on_exceed` when the shared throttle is exceeded — `FailRule` latches the shared rule (previously it silently degraded to Throttle), and a failed rule stops emitting.
- **wf-runtime**: The `rule_instances` gauge now sums across a rule's shards via delta reports (previously last-writer-wins) and reconciles to zero on drain.

### Performance

- **wf-runtime**: Conv-sink shards send one aggregated `ConvCloseBatch` per processed batch (max event-time watermark) instead of one per event — removing a per-event bounded(32) channel send + `.await` from the hot path.

### Chore

- **Tests**: Added SharedLimits unit + cross-shard integration coverage (collective `max_instances` / throttle / FailRule latch, exact DropOldest paths), conv-stage regression tests (FailRule, per-batch send, cancel-drops-unsealed, barrier watermark), and rule_instances delta-gauge coverage.
- **Clippy**: Workspace now passes `cargo clippy --all-targets --all-features -- -D warnings` (cleared pre-existing toolchain-version lints; intentional `Box`ed instance-state collections are `#[allow]`ed).
- **Dependencies**: Restored the `[patch.crates-io] wp-knowledge = { path = "../../wparse/wp-knowledge" }` override (it had been dropped) and bumped the `wp-knowledge` requirement to `0.16` so the local crate — which carries the `[fun.<name>]` named-query layer and requires `lru ^0.18` — is actually used. This resolves lru 0.16.4 → **0.18.2** and clears `cargo audit` RUSTSEC-2026-0253 (`LruCache::pop()` panic safety); `cargo audit` is now clean (0 vulnerabilities / 0 warnings).

## [0.3.0] — 2026-08-05

- Aligned the version scheme to `0.3.x` and upgraded dependencies (`wp-connector-api` 0.12, `wp-core-connectors` 0.8, `wp-model-core` 0.9, `wp-knowledge` 0.15). No language or runtime behavior changes beyond the 0.1.42 release notes below.

## [0.1.42] — 2026-08-04

### Fixed

- **wf-engine**: A yield field that references an optional input field missing from the event no longer fails the whole output record. Missing passthrough fields previously evaluated to the empty-string fallback and were rejected by type coercion (e.g. `yield security_alerts (attacker_latitude = s.attacker_latitude)` errored with "yield field ... expects a finite number" when the input had no `attacker_latitude`). Such fields are now omitted from the output (the column renders as null in Arrow / is absent in JSON), while other fields of the same record still emit. Explicit NaN / Infinity / type mismatches remain hard errors. Applies to `on each`, match, and close yield paths. (wp-labs/warp-fusion#62)

### Documentation

- **Language reference**: Documented the `join ... anti` mode (whitelist exclusion) alongside `snapshot` / `asof` / `asof within` — e.g. `join blocked_list anti on sip == blocked_list.ip` — including multi-condition joins (`&&`).

## [0.1.41] — 2026-08-02

### Added

- **wf-config / wf-runtime**: Added `[metrics] console_output` (default `true`) and gated the periodic `res`-domain metrics summary (`metrics snapshot`, interval table, and shutdown run-summary table) behind it. Previously `MetricsConfig` had no `console_output` field, so `console_output = false` was silently dropped by serde and the statistics log could not be disabled. Prometheus export, monitor-channel snapshots, and Top-N collection run regardless of the flag. (wp-labs/warp-fusion#61)
- **wf-lang / wf-engine / tree-sitter-wfl**: Added `on event seq { ... }` and `on event any { ... }` match bodies for ordered and unordered event correlation:
  - `on event seq { ... }` — ordered event chains for attack-chain detection. The engine's existing `current_step` progression enforces order; the `seq` mode adds per-step `within <dur>` time gaps, `not has <alias> within <dur>` negation steps, and `consec` strict-adjacency / `skip = past_last|to_next` modifiers (`to_next` deferred to L3).
  - `on event any { ... }` — unordered co-occurrence: all steps are evaluated in parallel and the rule fires once every step has satisfied its threshold, regardless of arrival order (a parallel-eval path in the state machine).
  - Bare `on event { ... }` defaults to `seq`, preserving backward compatibility; `has <alias>` (implicit `count >= 1`) is accepted in `seq`, `any`, and bare `on event` steps.
  - This replaces the earlier `chain { ... }` block syntax (removed). The tree-sitter grammar grew `on_event_mode_block` / `seq_rule_step`.
- **wf-lang**: Added `MatchMode::{Seq, Any}` to the AST and `MatchPlan.match_mode`; seq-mode `within`/`not`/`consec`/`skip` compile into `SeqPlan`, and `on event any` steps compile into the parallel-evaluated `event_steps`.
- **wf-lang**: Seq-mode step labels now register with the `stat.*` label registry, so a labeled seq step (e.g. `spam: a | count >= 5;`) can be referenced by `match_event(spam)` in yield.
- **wf-lang**: Checker rejects `on event seq`/`any` in pipeline stages (intermediate stage output schemas derive from `on event` steps only) and rejects `not` steps that reference a field aggregation (unsupported); `skip = to_next` warns that it is deferred to L3.

### Fixed

- **wf-engine**: Negation windows are active only after the preceding use-step completes — an event arriving before it no longer counts as a violation.
- **wf-engine**: A negative `within` gap (an out-of-order completion where a step completes before its predecessor) is now treated as a violation.
- **wf-engine**: `consec`-break and `within`-violation resets preserve the negation-violation flag, so an in-window violation cannot be wiped and the chain re-fire.
- **wf-engine**: `on event any` throttle handling now honors `on_exceed = fail_rule` (previously it was silently downgraded to throttle).
- **wf-runtime**: The periodic timeout scan now advances the effective watermark by the wall-clock time elapsed since the last event was processed (`watermark + idle wall time`). Instances therefore expire per their window TTL even when input is completely idle, instead of lingering until a new event advances the watermark (conforms to the window's time-based semantics).
- **wf-lang**: The unused-alias lint now counts `on event seq { ... }` step sources (`seq.steps[].branch.source`) as used, fixing a false-positive W001 when a rule referenced an alias only from seq-mode steps.

### Performance

- **wf-engine**: `RuleExecutor::event_matches_alias` uses a precomputed alias→filter map for rules with more than 24 binds, eliminating the O(binds) linear scan per (event × alias); rules with ≤24 binds keep the faster linear scan. The crossover was measured at ~24 binds (24: 5.1M vs 5.8M q/s; 16: linear still 1.3x faster).

### Documentation

- **User guide**: Aligned `docs/user-guide` with the implementation — `.wfs` window subscription uses `stream_tag`; window defaults/overrides moved to an external `windows.toml` (the `windows` field is now required in `wfusion.toml`); TCP sources use `connect = "tcp_src"` with `addr`/`port`; file sources document the `csv` format; the removed `wfusion run` / `wfusion config` subcommands are replaced by `wfusion daemon` / `wfusion batch` and `wfadm conf diff`; metrics are documented as monitor-sink NDJSON records instead of a Prometheus HTTP endpoint.
- **Examples**: Updated all examples to load with the current code — `.wfs` files switched from `stream` to `stream_tag`, `wfusion.toml` gained the required `windows = "windows.toml"` field with window config externalized, TCP sources use `connect = "tcp_src"`, and example READMEs (sinks / file_input) reflect the current sink-routing and CLI format.

## [0.1.39] — 2026-07-30

- Log-initialization updates only; no notable changelog entries.

## [0.1.38] — 2026-07-29

### Added

- **wf-lang / wf-runtime**: Added parameterized `yield preset` support with positional reference arguments, defaulted parameters, `$param` substitution inside preset bodies, and `_global.wfl` prelude loading without conflicting with `$VAR` preprocessing.

### Fixed

- **wf-lang / wf-runtime**: Improved `yield preset` diagnostic source lookup to handle declarations split across whitespace or `//` comments, matching the parser and preprocessor behavior for parameterized presets.

## [0.1.37] — 2026-07-29

### Changed

- **wf-runtime**: Changed DEBUG rule execution logging to use a bounded detail budget per batch/scan, with batch summaries preserving aggregate counts while suppressing high-cardinality per-event detail logs after the first 20 entries.
- **wf-runtime**: Pre-computed rule alias execution order and gated debug-only counters, output classification, event references, scope-key formatting, and instance counts behind DEBUG/detail checks to keep logging disabled paths lightweight.

### Added

- **wf-engine / wf-runtime**: Added state-machine progress diagnostics for DEBUG rule execution logs, including scope key, machine id, step/branch labels, threshold comparison details, measured values, and active instance counts.
- **wf-runtime**: Added rule execution funnel logs for bind rejects, accumulate/advance/match outcomes, close/match/each executor output paths, timeout/flush scans, internal pipeline writes/drops/errors, and alert sink dispatch/no-sink outcomes.

## [0.1.36] — 2026-07-24

### Added

- **wf-engine / docs**: Documented evidence output using `stat.count(window_event(alias))` with `collect_set(alias.event_id)`, keeping alias field collection bounded by the recent 1024-value cap.
- **wf-lang / wf-engine**: Added WFL string helper functions `sha1_n(text, length)`, `join(value, ...)`, and `join_by(separator, value, ...)`; `join` concatenates scalar values without intervention, while `join_by` inserts the explicit separator without trimming, case folding, or escaping; missing field value arguments are treated as empty string segments, while non-field expression failures still fail the function.

## [0.1.35] — 2026-07-23

### Added

- **wf-runtime**: Added the `_global.wfl` project prelude convention for rule directories, automatically loading project-level `yield preset` declarations while excluding the prelude from ordinary rule compilation; duplicate preset names inside the prelude or between prelude and rule files are rejected during rule loading.

### Fixed

- **wf-runtime**: Improved `_global.wfl` prelude diagnostics so preset field and expression errors point at the prelude source instead of the rule file that references the preset.

## [0.1.34] — 2026-07-22

### Changed

- **wf-lang / wf-engine**: Added yield-target type coercion for common output mappings, including numeric and boolean values into `chars` fields and validated coercion for numeric, IP, hex, and time targets.
- **wf-lang / wf-engine**: Changed `coalesce(...)` to skip blank strings as well as null values, and allow mixed scalar fallback types only when each candidate is assignable to the direct `yield` target field.
- **wf-runtime**: Treat `[output]` formatting changes as hot-reloadable so rule executors are rebuilt with the latest project output settings.
- **wf-lang / wf-engine**: Kept `chars` to `time` conversion explicit in both semantic checks and runtime yield coercion; string timestamps must be parsed with an explicit time expression.
- **wf-lang**: Improved source-aware diagnostics for `yield preset` errors so preset field errors point at the preset definition, while explicit rule yield fields still point at the rule's yield clause.
- **wf-runtime**: Normalized numeric time yields to Arrow `TimestampNanosecond` values when writing intermediate pipeline batches, preserving seconds/milliseconds/microseconds/nanoseconds inputs consistently.

### Added

- **wf-lang / wf-engine**: Added structured stream-input object/array support and `merge(obj1, obj2, ...)` for shallow left-to-right object enrichment in WFL expressions and yield outputs.
- **wf-lang**: Added `yield preset` declarations and `yield target : preset_a, preset_b (...)` references for composing reusable yield field sets with ordered override semantics; later presets override earlier fields, and explicit rule yield fields override presets.
- **wf-config / wf-engine**: Added project-level `[output]` configuration with `time_format` and UTC `time_zone`, used as the default format for one-argument `strftime(time)` calls.
- **wf-engine**: Added `RuleExecutorOptions` for passing output formatting and yield target type metadata without growing constructor argument lists.

### Documentation

- **User guide / design**: Added `merge(obj1, obj2, ...)` usage examples for object passthrough and incremental enrichment, and documented its shallow override order plus fallback behavior for missing object field references versus hard evaluation failures.

## [0.1.33] — 2026-07-19

### Changed

- **wf-engine**: Replaced the broad `ProviderWindow::rows_mut()` escape hatch with scoped `ProviderWindow::update_rows()` for provider-row mutation.

### Added

- **wf-lang / wf-engine / wf-runtime**: Added yield-only wfusion metadata references such as `@__wfu_rule_name` and `@__wfu_score`, allowing rules to map engine-managed metadata into ordinary output fields while keeping `__wfu_*` yield targets reserved.
- **wf-lang**: Added a centralized wfusion metadata field directory, including field names, types, yield availability, and a restricted intermediate-window metadata subset for automatic pipeline fields.
- **wf-runtime**: Added the built-in `__window_miss` provider window for recoverable dynamic-routing misses, including `unknown_stream_schema` and `missing_stream_tag_field` diagnostics, bounded payload samples, and `wf_receiver_window_miss_total` metrics.
- **wf-runtime**: Window-miss snapshots now keep recently updated keys when the bounded `__window_miss` provider reaches capacity, instead of evicting rows only by original insertion order.

### Documentation

- **Design docs**: Documented wfusion-managed metadata field semantics, including `rule_name = @__wfu_rule_name`, reserved `__wfu_*` output targets, yield-only access, and sink-stage `wf_meta_disable` behavior.

### Fixed

- **wf-runtime tests**: Added NDJSON, CSV, Arrow framed, external source, metrics, and capacity-eviction coverage for window-miss handling.

## [0.1.32] — 2026-07-14

### Changed

- **wf-engine**: Unified WFL `time` expression values on epoch milliseconds: `now()` and `now_ms()` now return milliseconds, `strptime()` and `time_bucket()` return milliseconds, and `strftime()` / `time_diff()` accept epoch seconds, milliseconds, microseconds, or nanoseconds by timestamp width.
- **wf-config / wf-engine / wf-runtime**: Changed `wf_meta_disable` handling to use compiled `wildmatch` matchers, supporting exact WarpFusion metadata field names and wildcard patterns such as `__wfu_*` and `__wfu_rule_*`.
- **wf-engine / wf-runtime**: Changed sink business routing to compile window patterns into runtime `wildmatch` matchers instead of pre-resolving routes only for startup-known window names.
- **wf-engine**: Reused a shared epoch timestamp normalization helper across alert export and both expression evaluators to keep second/millisecond/microsecond/nanosecond handling consistent.
- **wf-engine**: `time_bucket()` now rejects zero, negative, and non-finite intervals instead of producing surprising bucket results.

### Added

- **wf-lang / wf-engine**: Added yield-only time system variables `@event_first_time`, `@event_last_time`, `@evidence_start_time`, `@evidence_end_time`, `@window_start_time`, `@window_end_time`, and `@emit_time`.
- **wf-lang / wf-engine**: Added yield-only stable stat context functions: `stat.count(window_event(alias))`, `stat.count(match_event(label))`, `stat.count(match_distinct(label))`, `stat.value(trigger(label))`, and `stat.value(final(label))`.
- **wf-engine tests**: Added sink-runtime send-path coverage for `wf_meta_disable` wildcard matching, including projection-before-disable behavior.
- **wf-engine tests**: Removed cross-evaluation exact equality checks for `now()` / `now_ms()` to avoid millisecond-boundary flakes while preserving same-expression timestamp stability coverage.

### Documentation

- **User guide / design**: Documented the time system variables, stable stat context functions, and recommended business-field mappings such as `first_seen`, `last_seen`, `rule_window_start`, and `latest_analysis_time`.

## [0.1.31] — 2026-07-12

### Changed

- **wf-runtime**: Changed the default dynamic stream-tag payload carrier from `wp_stream_tag` to `wp_oml_name`, matching warp-parse OML output naming.

### Documentation

- **User guide**: Updated runtime source examples to use `stream_tag_field = "wp_oml_name"`.


## [0.1.30] — 2026-07-11

### Added

- **wf-lang**: Added explicit WFS window subscription syntax with `stream_tag = ...` and `stream_tag = [...]`.
- **wf-runtime**: Added dynamic stream-tag routing for file NDJSON/CSV replay and external NDJSON sources via `stream_tag_field`, defaulting to `wp_stream_tag` in this release.
- **wf-runtime**: Added per-batch Arrow framed tag tracking so multiple Arrow frames received in one source batch are routed by their own frame tags.
- **wf-config / wf-runtime**: Added sink-group scoped `wf_meta_disable = ["__wfu_*"]` output metadata control for disabling selected WarpFusion-managed alert fields per sink group.
- **wf-engine**: Added long-running and burst-then-drain tests for window memory eviction behavior.

### Changed

- **wf-config / wf-runtime**: Renamed fixed source routing from `stream` to `stream_tag`, and renamed the dynamic payload carrier option from `arrow_tag` to `stream_tag_field`.
- **wf-config**: Relaxed source validation so NDJSON/CSV sources can use dynamic `stream_tag_field` routing without a fixed `stream_tag`, while `arrow_ipc` sources still require an explicit `stream_tag`.
- **wf-engine**: `wf_meta_disable` now marks configured metadata fields as `DataType::Ignore` before sink output, aligning with the wp-motor output suppression model while preserving fields inside the record.
- **wf-engine tests**: Gated long-running memory tests behind the `mem_test` feature.

### Documentation

- **Design / user guide**: Documented `stream_tag`, `stream_tag_field`, Arrow framed tag routing, and JSON/CSV payload carrier routing.
- **Design docs**: Added window memory control design notes and clarified `evict_expired` naming.

## [0.1.29] — 2026-07-10

### Added

- **wf-lang**: Added structured WFL literals `object { ... }` and `array [ ... ]` for building nested output values in `yield`, including object field type hints and duplicate-field validation.
- **wf-lang**: Added WFS schema support for output-only `object`, untyped `array`, and typed `array/T` fields.
- **wf-engine**: Added runtime evaluation and alert export support for structured object and array values, including nested model values and deterministic JSON rendering for structured string output.
- **wf-runtime**: Added UTF-8 storage bridging for structured `object` / `array` fields so structured source values can be serialized into intermediate pipeline windows.

### Documentation

- **User guide**: Documented WFS `object`, `array`, and `array/T` field types and WFL `object { ... }` / `array [ ... ]` structured output syntax.

### Fixed

- **wf-lang**: Structured `object` / `array` field declarations are now rejected for stream and provider input windows; source JSON object/array values should be declared as `chars` and structured outputs built in `yield`.
- **wf-lang**: `mvcount`, `mvjoin`, `mvdedup`, `mvsort`, `mvreverse`, `mvindex`, and `mvappend` now accept untyped and empty array literals where runtime behavior supports them.
- **wf-lang**: `array/float` and `mvappend` type checks now consistently allow digit/float element promotion.
- **wf-runtime**: Structured pipeline values now fail fast on non-finite numeric values instead of silently serializing invalid JSON.

## [0.1.28] — 2026-07-10

### Added

- **wf-lang**: Added source-aware WFL diagnostics for parse and semantic compile failures, including file path, diagnostic category, rule/test context, line/column, and source snippets.
- **wf-runtime**: Rule bootstrap now surfaces source-aware WFL diagnostics for rule-file parse errors, semantic compile errors, and intermediate topology cycle errors.

### Fixed

- **wf-lang**: Improved WFL diagnostic location selection for `yield` errors so repeated tokens in `match`, `entity`, and `yield` clauses point to the failing `yield` argument, including inline single-line rules.

## [0.1.27] — 2026-07-09

### Added

- **wf-lang**: Added type checking and inference for new WFL helper functions: `now()`, `now_s()`, `now_ms()`, `now_us()`, `now_ns()`, `is_blank()`, `null_if_blank()`, `default_if_blank()`, `md5()`, `sha1()`, `sha256()`, `hex()`, and `stable_id()`.
- **wf-engine**: Added runtime support for current engine time helpers, blank-string helpers, hash/hex helpers, and stable alert ID generation in both L2 expression evaluation and yield/L3 evaluation paths.

### Fixed

- **wf-engine**: `now_*` helpers now share one cached timestamp within a single expression and across all yield fields for one output record, preventing `created_time` / `created_ns` drift inside the same alert.
- **wf-engine**: `stable_id()` now hashes typed, length-prefixed value segments instead of ambiguous separator-joined values, avoiding collisions when inputs contain separator-like bytes.

### Documentation

- **User guide**: Expanded the WFL language reference with current blank handling, current-time, time formatting/parsing, hash/encoding, stable ID, and multivalue function behavior.

## [0.1.26] — 2026-07-09

### Added

- **wf-config**: Added source-level `enable` support so disabled sources can stay in topology configuration without being started.
- **wf-data / wf-runtime**: Added shared time parsing support for source ingestion and runtime timestamp conversion.

### Fixed

- **wf-config tests**: Updated fusion config tests for source enable handling and time parsing coverage.

## [0.1.25] — 2026-07-02

### Added

- **External window config** (`windows.toml`): Window configurations (`[window.xxx]`) can now be defined in a separate `windows.toml` file instead of inline in `wfusion.toml`. The loader reads `windows = "conf/windows.toml"` (configurable path) and merges it with inline window configs. This enables cleaner separation of window topology from runtime/rule configuration.

### Changed

- **Config loader refactored**: `FusionConfigLoader` internals restructured to support the external windows file. `load_raw()` / `load_expanded_raw()` now correctly track origins for windows defined in external files, preserving reload diff accuracy.

### Fixed

- **Test adaptations**: `hot_reload` and `lifecycle` tests updated to work with the new external windows.toml config path, ensuring reload scenarios are tested against the same config layout used in production.

### Chore

- **Cargo audit**: Added ignore for RUSTSEC-2023-0071 (`rsa` Marvin Attack) — deep transitive dependency via `sqlx-mysql` → `sqlx` → `wp-knowledge`. No patched version available; attack requires network-level timing observation not applicable to our Redis usage.

---

## [0.1.24] — 2026-07-01

### Added

- **RuntimeControlHandle**: A clonable handle (`Send + Sync`) for external callers (e.g., admin API server) to trigger hot reload on a running `Reactor`. Uses `mpsc` + `oneshot` channel for serialised, async-safe control — no two reloads run concurrently.
- **Reactor::run()**: New method that combines `wait_for_signal`, `shutdown`, and `wait()` into a single lifecycle loop with integrated reload control. Replaces the old `wait_for_signal(reactor.cancel_token())` pattern.
- **Reactor::apply_reload()**: Core hot-reload implementation. Compiles new rules, diffs per-window topology, and swaps rule tasks at runtime while preserving windows, router, sinks, evictor, and metrics tasks.
- **Reactor::swap_rule_tasks()**: Orchestrates cancelling old rule tasks, draining/flushing pending emits (with configurable timeout via `DEFAULT_RELOAD_DRAIN_TIMEOUT`), and spawning the new rule generation.
- **ReloadRequest / ReloadOutcome**: Control-channel types:
  - `ReloadRequest::Reload` — carries `RawFusionConfigTree`, `FusionConfig`, and a `oneshot::Sender<RuntimeResult<ReloadOutcome>>` for the reply.
  - `ReloadOutcome::Applied(FusionReloadPlan)` — reload succeeded.
  - `ReloadOutcome::Blocked(FusionReloadPlan)` — reload refused; plan lists changes that require a full restart.
- **WindowRegistry now RwLock-protected**: Four internal `HashMap`s (`windows`, `provider_windows`, `subscriptions`, `notifiers`) switched from bare `HashMap` to `RwLock<HashMap<…>>`, enabling concurrent read access during routing/eviction while allowing runtime window additions/replacements.
- **WindowRegistry::try_add_window()**: Add a new window at runtime — supports incremental hot reload (L2) without rebuilding existing windows.
- **WindowRegistry::try_replace_window()**: Replace a specific window definition at runtime — supports modification hot reload (L3).
- **FusionConfigLoader::load_raw()**: Splits config loading into raw TOML tree + effective `FusionConfig`, preserving origin tracking needed for reload diff.
- **RawFusionConfigTree::from_toml_str()**: Convenience constructor to parse raw config from an inline TOML string (useful for embedders and tests).

### Changed

- **Reactor lifecycle fields restructured**: `watchers: Vec<JoinHandle<…>>` split into `head_watchers` (alert, evictor), `tail_watchers` (receiver, metrics), and `rule_watch` — enabling hot-swap of rule tasks in isolation while keeping the rest of the engine running.
- **Dedicated `rule_cancel` token**: Rules get their own child `CancellationToken` (nested under root `cancel`), so reload can cancel just the rule tasks without stopping the rest of the engine.
- **CLI daemon flow**: `Reactor::start()` + `wait_for_signal()` + `reactor.shutdown()` + `reactor.wait()` replaced with `Reactor::start()` + `reactor.run()`. The loader now yields raw config alongside effective config.
- **Metrics::sample_windows()**: Updated to match `WindowRegistry::get_window()`'s new `&str` parameter signature.

### Fixed

- **Rule task drain deadlock**: Rule tasks whose `emit()` blocks on a full alert channel (not cancellation-responsive) would hang the hot swap indefinitely. Mitigated with `DEFAULT_RELOAD_DRAIN_TIMEOUT` (5 s); stale tasks are detached and reaped at the next reload or at final `wait()`.
- **Clippy `large_enum_variant`**: `ReloadRequest::Reload`'s `FusionConfig` field boxed with `Box<FusionConfig>` to reduce enum size.

## [0.1.23] — 2026-06-26

### Added

- **wf-config**: Added `[project_remote]` configuration section for remote project/schema fetching.

## [0.1.22] — 2026-06-25

### Changed

- Bump version 0.1.21 → 0.1.22.
- Removed unused source files.
- Added `cargo tree` dependency snapshot for workspace auditing.

## [0.1.21] — 2026-06-28

### Added

- **wf-lang**: WFG scenario AST (`wfg_ast.rs`) and parser (`wfg_parser/`) moved
  from `wfgen` (warp-fusion) into `wf-lang`, consolidating all language
  definitions in one crate.  Exported as `parse_wfg` alongside `parse_wfl` and
  `parse_wfs`.
- **wf-lang**: Added `fail()` and `error()` helper functions to the public error
  API for constructing `LangError` values without needing to import
  `ToStructError`.
- **wf-config**: Added `AdminApiConf` / `AdminApiTlsConf` / `AdminApiAuthConf`
  config structs for the admin API (`[admin_api]` section in wfusion.toml).
  Default port is `127.0.0.1:19080` to avoid conflict with warp-parse's
  `19090`.

### Changed

- Bump version 0.1.20 → 0.1.21.

### Removed

- **wf-lang**: Removed W002 lint (`lint_missing_on_close`) — the warning was
  pattern-based and could not determine whether a rule actually needed a
  close block, producing noise for legitimately event-only rules.

### Documentation

- **WFG design**: Canonical WFG design document moved from `warp-fusion` to
  `wp-reactor/docs/design/wfg-design.md`, co-located with the parser/AST
  implementation in `wf-lang`.

## [0.1.19] — 2026-06-24

### Documentation

- **WFG**: Moved the canonical WFG design document to
  `warp-fusion/docs/design/wfg-design.md`, where `wfgen` is implemented, and
  left `wp-reactor/docs/design/wfg-design.md` as a pointer to the migrated
  document.
- **WFL/WFG design alignment**: Updated design notes to distinguish
  `wp-reactor` library capabilities from the CLI/tooling implemented in the
  sibling `warp-fusion` repository, including `wfl test/replay/verify` and
  `wfgen` generation/verification workflows.
- **WFG syntax**: Aligned the WFG design with current `wfgen` parser behavior
  (`with(count)`, optional `for RULE`, `then use(...)`, extended `expect`
  metrics) and marked `not(...) within(...)` as parser-supported but not yet
  datagen-supported.

## [0.1.17]

### Added

- **wf-lang**: `collect_bind_tracking_aliases` now collects aliases from plain
  `Expr::Field(FieldRef::Qualified/Bracketed)` expressions. Previously only
  series functions (e.g. `count(e)`) contributed to `tracked_bind_aliases`;
  now `e.dip` in yield expressions correctly adds alias `e` to the set.
- **wf-engine**: `build_eval_context` now exposes bind_data and step_data
  field values as plain field names (e.g. `dip`) in addition to the existing
  prefixed keys (`_bind_e_field_dip`, `_step_0_field_dip`). This allows
  yield expression evaluators that look up fields by plain name to find them.
- **wf-lang tests**: 4 new unit tests for `collect_bind_tracking_aliases`
  covering qualified/bracketed/simple field refs and full yield expressions.
- **wf-engine tests**: 1 new unit test for `build_eval_context` verifying
  plain field name exposure from bind_data.

### Changed

- **wf-lang**: `collect_rule_bind_tracking_aliases` and
  `collect_bind_tracking_aliases` visibility changed `fn` → `pub(crate)` for
  testability.

### Fixed

- **wf-engine**: Fixed 2 pre-existing clippy warnings in
  `match_engine/tests/l2/expr.rs` (collapsible-if + let-unit-value).

## [0.1.15] — 2026-06-18

### Added

- **wf-runtime**: `DataSourceBatchSource` adapter (`wf-runtime/src/source/mod.rs`) —
  bridges `wp_connector_api::DataSource` to `wf_connector_api::BatchSource`,
  handling Arrow IPC / NDJSON / wp_arrow framed decode and EOF mapping.
- **wf-runtime**: `ArrowFramed` format extracts the wp_arrow frame tag and uses
  it as the routing stream name when no explicit `stream` param is configured.
- **wf-engine**: `external()` WFL function support — `ExternalCallHandler` trait
  + global `dispatch_external_call` + `eval_external` shared helper
  (`wf-engine/src/external.rs`). Both eval paths (executor + match_engine)
  route `external()` to the global handler.
- **wf-runtime**: `ExternalRuntime` + `RedisBackend` (`wf-runtime/src/external/`)
  — bridges WFL `external()` calls to `wp_knowledge::facade` (Redis backend).
  Bootstrap installs the handler and initializes Redis from `knowdb.toml`.
  Error handling: `external_exists` returns `Bool(false)` on Redis failure
  (fail-closed); `external_value` returns `None`.

### Changed

- **wf-connector-api integration landed.** Runtime now consumes source data
  via the `BatchSource` trait, replacing inline Arrow IPC / NDJSON decode logic
  in `spawn_external_source_tasks`.
- **`wp-core-connectors` upgraded 0.5.0 → 0.5.2.** Source factories validate
  `data_format` in `validate_spec`; `WireFormat` enum
  (`Ndjson` / `ArrowStream` / `ArrowFramed`) replaces the runtime's custom
  `SourceFormat`. Decode logic delegates to connector-layer shared helpers
  (`decode_arrow_ipc_batches` / `decode_arrow_framed_batches`).
- **`wp-connectors` upgraded v0.15.4 → v0.15.5.**
- **Config parameter renamed:** `format` → `data_format` for source payload
  format declaration (TCP / file / syslog sources).
- **Removed `listen_addr` from `Reactor`.** TCP listen address is a connector
  implementation detail, not tracked at the Reactor level. `spawn_receiver_task`
  now returns `TaskGroup` instead of `(Option<SocketAddr>, TaskGroup)`.
- **Removed dead `Receiver` struct** and inline TCP handler
  (`handle_connection` / `handle_connection_stream` / `read_frame` for TCP)
  from `receiver.rs`. Production code uses the connector factory path.
- **`wf-config`**: file source validation now reads `data_format` instead of
  `format`.
- **`wf-runtime`**: file source replay path now reads `data_format` instead of
  `format`.

### Fixed

- **EOF handling.** `DataSource` returning `SourceReason::EOF` no longer
  causes infinite retry loops — the source task exits cleanly when the stream
  ends.
- **Schema resolution.** Arrow formats (`ArrowFramed` / `ArrowStream`) skip
  pre-resolved window schema at startup — the schema is embedded in the IPC
  stream itself, so resolving from a (possibly empty) stream name was
  incorrect.
- **`external()` error handling.** `call_bool` returning `Ok(None)` (exists=false)
  now directly returns `Bool(false)` instead of incorrectly falling through to
  `call_value`. Previously, "password not in weak-password DB" would trigger a
  spurious HGET query.
- **`external()` code dedup.** Both eval paths now share `eval_external()`
  helper instead of duplicating arg-parsing logic.
- **`external()` test pollution.** `OnceLock` global handler test adjusted to
  avoid cross-test state leakage.

### Documentation

- `docs/source-architecture.md` rewritten to reflect the three-layer
  architecture (connector SourceFactory + WireFormat + BatchSource).
- `docs/user-guide/runtime-config.md` updated for connector-based TCP source
  params (`addr` / `port` / `framing` / `data_format`).
- `docs/design/arrow-tcp-stream-compatibility.md` marked as implemented.
- `docs/design/warp-fusion.md` Reactor struct updated (removed `listen_addr`).
- `docs/design/external-function-design.md` §6.1 error handling updated with
  full dispatch logic; §10 Phase 0 implementation details updated; §11.1
  known P0 limitations table added (L1-L5).
- All example TOML configs updated `format` → `data_format`.

## [0.1.12] — 2026-06-15

### Added

- **wf-runtime**: Added external source startup through the `wp-core-connectors` source factory registry, including builtin `file`, `tcp`, and `syslog` source factory registration.
- **wf-runtime**: Added global sink factory import support so application-registered `wp-core-connectors` sink factories are available during sink dispatcher bootstrap.

### Changed

- **Workspace**: Kept `wp-core-connectors` on the published `0.5` crate dependency and added `async-broadcast` for source acceptor lifecycle control.
- **wf-runtime**: External source ingestion now reuses WFS-to-Arrow schema resolution before routing decoded NDJSON payloads.
- **wf-runtime**: External source parameters are converted to typed JSON values (`bool`, integer, float, or string) before factory validation/build.

### Fixed

- **wf-config**: Batch mode validation again rejects enabled non-file sources, preventing daemon-style receivers from starting in batch runs.
- **wf-config**: Enabled external sources now require a non-empty `stream` so schema subscription failures are caught during configuration validation.
- **wf-runtime**: Unknown external source kinds now fail bootstrap with a clear error instead of being silently skipped.
- **wf-runtime**: External source decode and route failures are now logged and reflected in receiver decode / route error metrics.
- **wf-runtime**: Source acceptors now receive a `ControlEvent::Stop` on runtime cancellation.

## [0.1.7] — 2026-06-12

### Added

- **wp-core-connectors**: File and TCP sinks now support Arrow output via `protocol = "arrow"`.
- **wp-core-connectors**: Arrow file sinks support append mode and optional `sync` fsync for durability.
- **wp-core-connectors**: Arrow TCP sink supports automatic reconnect with exponential backoff.

### Changed

- **wp-core-connectors**: Arrow sink configuration consolidated under `protocol` dispatch (`"arrow"` / `"txt"`).

### Fixed

- **wp-core-connectors**: Invalid `protocol` values now produce a clear configuration error instead of silently defaulting to text mode.

## [0.1.3] — 2025-11-15

### Added

- **wf-core**: Added `CoreReason` and `CoreResult<T>` as the structured error boundary for core APIs.
- **wf-runtime**: Added `RuntimeReason` and `RuntimeResult<T>` for runtime lifecycle, receiver, metrics, tracing, schema, sink, and task boundaries.
- **wf-config**: Added `ConfigReason` and `ConfigResult<T>` for configuration loading, validation, path resolution, and sink configuration errors.
- **wf-lang**: Added `LangReason` and `LangResult<T>` for parser, validator, and compiler entry points.
- **wf-vars**: Added `VarsReason` and `VarsResult<T>` for variable expansion and resolution APIs.
- **wf-engine**: Added `EngineReason` and `EngineResult<T>` for the CLI boundary.

### Changed

- **Workspace**: Upgraded `orion-error` to `0.8.1` and adopted `#[derive(OrionError)]` reason enums with stable identities.
- **Workspace**: Removed `anyhow` from workspace and crate manifests used by `wf-core`, `wf-runtime`, and `wf-engine`.
- **wf-engine**: CLI failures now render structured `DiagnosticReport` output directly instead of converting runtime failures to unstructured errors.
- **wf-runtime**: Runtime task handles now return `RuntimeResult<()>`, preserving structured task failures through shutdown and supervisor joins.
- **wf-config**: Sink configuration variable expansion now wraps lower variable errors at the sink boundary while carrying the source file path in structured context.
- **wf-config**: Configuration APIs now use structured conversion paths (`source_err`, `source_raw_err`, and `conv_err`) instead of ad hoc string wrapping.
- **wf-lang**: Parser and compiler APIs now return structured errors while keeping parser-combinator internal error handling local to the parser.
- **wf-vars**: Variable expansion APIs now return structured errors with explicit resolve, template, and TOML reasons.

### Fixed

- **wf-runtime**: Metrics HTTP response write timeouts are now reported as structured timeout errors instead of being silently ignored.
- **wf-runtime**: Supervisor shutdown failures now preserve the lower structured error source chain instead of flattening it into a string detail.
- **wf-config**: Sink defaults, connector, business route, and infra route preprocessing failures now include structured `path` context.

### Docs

- **Docs**: Updated the error-handling design notes to describe the structured error boundaries across `wf-core`, `wf-runtime`, `wf-config`, `wf-lang`, `wf-vars`, and `wf-engine`.
- **Docs**: Updated configuration variable resolution examples and dependency notes to use `ConfigResult`, `VarsError`, and `orion-error`.

## 0.1.0

### Added

- `wfusion` runtime config supports explicit `mode = "daemon" | "batch"`.
- Input sources are unified under `[[sources]]`; TCP ingress is configured as a source and no longer uses `[server]`.
- File source formats now include:
  - `ndjson`
  - `arrow_framed`
  - `arrow_ipc`
- `arrow_framed` file replay support was added for the current `wp_arrow` length-prefixed framed format.
- User documentation was reorganized into `docs/user-guide/` with topic pages:
  - `index.md`
  - `quick-start.md`
  - `language-reference.md`
  - `runtime-config.md`
  - `tooling.md`

### Changed

- Sink runtime integration now uses `wp-core-connectors`.
- Runtime output export is now record-first:
  - internal `OutputRecord` values are exported to `DataRecord` before sink dispatch
  - sink dispatch reuses the sink `send_record` path instead of a JSON-only path
- Structured runtime output now injects reserved engine fields with the `__wfu_` prefix:
  - `__wfu_id`
  - `__wfu_rule_name`
  - `__wfu_score`
  - `__wfu_entity_type`
  - `__wfu_entity_id`
  - `__wfu_origin`
  - `__wfu_close_reason`
  - `__wfu_fired_at`
  - `__wfu_emit_time`
  - `__wfu_summary`
- `yield_fields` are expanded into exported sink records alongside the fixed `__wfu_*` fields.
- `yield_fields` with array types are currently exported as compact JSON strings.

### Fixed

- Structured output export now preserves typed `yield_fields` for `ip`, `time`, and `hex` instead of degrading them to `chars`.
- Sink dispatch no longer relies on sink kind name prefixes such as `arrow-*` to decide the payload path.
- Reserved prefix conflicts are now rejected when user `yield_fields` attempt to emit fields under `__wfu_`.
- `wfgen verify` now accepts both legacy alert JSONL fields and the new structured `__wfu_*` runtime output fields.
- Close-path aggregate expressions in `score(...)` and `yield (...)` now evaluate against step context, including:
  - `count(alias)`
  - `count(step_label)`
  - `avg(alias.field)`
  - aggregate expressions nested inside `if ... then ... else ...` and builtin functions such as `concat(...)`
- Downstream `match + close` rules now aggregate intermediate float fields correctly from close-step data, so expressions such as `avg(x.__wfu_score)` and `avg(x.risk_score)` no longer collapse to `0.0`.
- When the same alias can resolve to both event-step and close-step context during close evaluation, aggregate lookup now prefers the close-step series to avoid double-counting.
- Close-path `count(alias)` and `avg/sum/min/max/first/last(alias.field)` now also work for filtered bind aliases declared in `events { ... }`, even when that alias is not used as a match step source.
- Event-path matches now process auxiliary filtered bind aliases before step-source aliases, so same-row expressions such as `count(hi)` and `avg(elevated.risk_score)` see the current row as well.
- `match`, `on each`, and `close` executor paths no longer silently drop `yield` fields when expression evaluation returns `None`; they now fail with explicit `RuleExec` errors.
- Checker validation now rejects ambiguous set-level aggregate expressions such as `avg(alias)`, `sum(alias)`, `min(alias)`, and `max(alias)`, while continuing to allow `count(alias)`.

### Docs

- User guide examples now document:
  - `arrow_framed` vs `arrow_ipc`
  - explicit file source format selection
  - structured output export semantics
  - `__wfu_*` reserved fields
  - array export behavior as JSON string
- Changelog now records the executor-side close aggregation fix, non-silent `yield` failures, and the checker restriction on ambiguous set-level aggregates.
