# Changelog

All notable changes to wp-reactor will be documented in this file.

## [0.1.40 Unreleased]

### Fixed

- **wf-config / wf-runtime**: Added `[metrics] console_output` (default `true`) and gated the periodic `res`-domain metrics summary (`metrics snapshot`, interval table, and shutdown run-summary table) behind it. Previously `MetricsConfig` had no `console_output` field, so `console_output = false` was silently dropped by serde and the statistics log could not be disabled. Prometheus export, monitor-channel snapshots, and Top-N collection run regardless of the flag. (wp-labs/warp-fusion#61)

### Added

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

### Performance

- **wf-engine**: `RuleExecutor::event_matches_alias` uses a precomputed alias→filter map for rules with more than 24 binds, eliminating the O(binds) linear scan per (event × alias); rules with ≤24 binds keep the faster linear scan. The crossover was measured at ~24 binds (24: 5.1M vs 5.8M q/s; 16: linear still 1.3x faster).

## [0.1.37 Unreleased]

### Changed

- **wf-lang / wf-engine**: Added yield-target type coercion for common output mappings, including numeric and boolean values into `chars` fields and validated coercion for numeric, IP, hex, and time targets.
- **wf-lang / wf-engine**: Changed `coalesce(...)` to skip blank strings as well as null values, and allow mixed scalar fallback types only when each candidate is assignable to the direct `yield` target field.
- **wf-engine / docs**: Documented evidence output using `stat.count(window_event(alias))` with `collect_set(alias.event_id)`, keeping alias field collection bounded by the recent 1024-value cap.
- **wf-runtime**: Treat `[output]` formatting changes as hot-reloadable so rule executors are rebuilt with the latest project output settings.
- **wf-runtime**: Changed the default dynamic stream-tag payload carrier from `wp_stream_tag` to `wp_oml_name`, matching warp-parse OML output naming.
- **User guide**: Updated runtime source examples to use `stream_tag_field = "wp_oml_name"`.
- **wf-runtime**: Changed DEBUG rule execution logging to use a bounded detail budget per batch/scan, with batch summaries preserving aggregate counts while suppressing high-cardinality per-event detail logs after the first 20 entries.
- **wf-runtime**: Pre-computed rule alias execution order and gated debug-only counters, output classification, event references, scope-key formatting, and instance counts behind DEBUG/detail checks to keep logging disabled paths lightweight.
- **wf-engine**: Unified WFL `time` expression values on epoch milliseconds: `now()` and `now_ms()` now return milliseconds, `strptime()` and `time_bucket()` return milliseconds, and `strftime()` / `time_diff()` accept epoch seconds, milliseconds, microseconds, or nanoseconds by timestamp width.
- **wf-config / wf-engine / wf-runtime**: Changed `wf_meta_disable` handling to use compiled `wildmatch` matchers, supporting exact WarpFusion metadata field names and wildcard patterns such as `__wfu_*` and `__wfu_rule_*`.
- **wf-engine / wf-runtime**: Changed sink business routing to compile window patterns into runtime `wildmatch` matchers instead of pre-resolving routes only for startup-known window names.
- **wf-engine**: Replaced the broad `ProviderWindow::rows_mut()` escape hatch with scoped `ProviderWindow::update_rows()` for provider-row mutation.

### Added

- **wf-lang / wf-engine**: Added WFL string helper functions `sha1_n(text, length)`, `join(value, ...)`, and `join_by(separator, value, ...)`; `join` concatenates scalar values without intervention, while `join_by` inserts the explicit separator without trimming, case folding, or escaping; missing field value arguments are treated as empty string segments, while non-field expression failures still fail the function.
- **wf-lang / wf-engine**: Added structured stream-input object/array support and `merge(obj1, obj2, ...)` for shallow left-to-right object enrichment in WFL expressions and yield outputs.
- **wf-lang**: Added `yield preset` declarations and `yield target : preset_a, preset_b (...)` references for composing reusable yield field sets with ordered override semantics; later presets override earlier fields, and explicit rule yield fields override presets.
- **wf-lang / wf-runtime**: Added parameterized `yield preset` support with positional reference arguments, defaulted parameters, `$param` substitution inside preset bodies, and `_global.wfl` prelude loading without conflicting with `$VAR` preprocessing.
- **wf-runtime**: Added the `_global.wfl` project prelude convention for rule directories, automatically loading project-level `yield preset` declarations while excluding the prelude from ordinary rule compilation; duplicate preset names inside the prelude or between prelude and rule files are rejected during rule loading.
- **wf-config / wf-engine**: Added project-level `[output]` configuration with `time_format` and UTC `time_zone`, used as the default format for one-argument `strftime(time)` calls.
- **wf-engine**: Added `RuleExecutorOptions` for passing output formatting and yield target type metadata without growing constructor argument lists.
- **wf-lang / wf-engine / wf-runtime**: Added yield-only wfusion metadata references such as `@__wfu_rule_name` and `@__wfu_score`, allowing rules to map engine-managed metadata into ordinary output fields while keeping `__wfu_*` yield targets reserved.
- **wf-lang**: Added a centralized wfusion metadata field directory, including field names, types, yield availability, and a restricted intermediate-window metadata subset for automatic pipeline fields.
- **wf-lang / wf-engine**: Added yield-only time system variables `@event_first_time`, `@event_last_time`, `@evidence_start_time`, `@evidence_end_time`, `@window_start_time`, `@window_end_time`, and `@emit_time`.
- **wf-lang / wf-engine**: Added yield-only stable stat context functions: `stat.count(window_event(alias))`, `stat.count(match_event(label))`, `stat.count(match_distinct(label))`, `stat.value(trigger(label))`, and `stat.value(final(label))`.
- **User guide / design**: Documented the time system variables, stable stat context functions, and recommended business-field mappings such as `first_seen`, `last_seen`, `rule_window_start`, and `latest_analysis_time`.
- **Design docs**: Documented wfusion-managed metadata field semantics, including `rule_name = @__wfu_rule_name`, reserved `__wfu_*` output targets, yield-only access, and sink-stage `wf_meta_disable` behavior.
- **wf-engine / wf-runtime**: Added state-machine progress diagnostics for DEBUG rule execution logs, including scope key, machine id, step/branch labels, threshold comparison details, measured values, and active instance counts.
- **wf-runtime**: Added rule execution funnel logs for bind rejects, accumulate/advance/match outcomes, close/match/each executor output paths, timeout/flush scans, internal pipeline writes/drops/errors, and alert sink dispatch/no-sink outcomes.
- **wf-engine tests**: Added sink-runtime send-path coverage for `wf_meta_disable` wildcard matching, including projection-before-disable behavior.
- **wf-runtime**: Added the built-in `__window_miss` provider window for recoverable dynamic-routing misses, including `unknown_stream_schema` and `missing_stream_tag_field` diagnostics, bounded payload samples, and `wf_receiver_window_miss_total` metrics.
- **wf-runtime tests**: Added NDJSON, CSV, Arrow framed, external source, metrics, and capacity-eviction coverage for window-miss handling.

### Documentation

- **User guide / design**: Added `merge(obj1, obj2, ...)` usage examples for object passthrough and incremental enrichment, and documented its shallow override order plus fallback behavior for missing object field references versus hard evaluation failures.

### Fixed

- **wf-lang / wf-engine**: Kept `chars` to `time` conversion explicit in both semantic checks and runtime yield coercion; string timestamps must be parsed with an explicit time expression.
- **wf-lang**: Improved source-aware diagnostics for `yield preset` errors so preset field errors point at the preset definition, while explicit rule yield fields still point at the rule's yield clause.
- **wf-runtime**: Improved `_global.wfl` prelude diagnostics so preset field and expression errors point at the prelude source instead of the rule file that references the preset.
- **wf-lang / wf-runtime**: Improved `yield preset` diagnostic source lookup to handle declarations split across whitespace or `//` comments, matching the parser and preprocessor behavior for parameterized presets.
- **wf-runtime**: Normalized numeric time yields to Arrow `TimestampNanosecond` values when writing intermediate pipeline batches, preserving seconds/milliseconds/microseconds/nanoseconds inputs consistently.
- **wf-engine**: Reused a shared epoch timestamp normalization helper across alert export and both expression evaluators to keep second/millisecond/microsecond/nanosecond handling consistent.
- **wf-engine**: `time_bucket()` now rejects zero, negative, and non-finite intervals instead of producing surprising bucket results.
- **wf-engine tests**: Removed cross-evaluation exact equality checks for `now()` / `now_ms()` to avoid millisecond-boundary flakes while preserving same-expression timestamp stability coverage.
- **wf-runtime**: Window-miss snapshots now keep recently updated keys when the bounded `__window_miss` provider reaches capacity, instead of evicting rows only by original insertion order.

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
