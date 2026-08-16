# Changelog

All notable changes to wp-reactor will be documented in this file.

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
