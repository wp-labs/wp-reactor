#[cfg(test)]
mod reload_tests {
    use std::path::{Path, PathBuf};

    use wf_config::{ConfigVarContext, FusionConfigLoader};

    use super::super::*;

    fn make_temp_dir(name: &str) -> PathBuf {
        let unique = format!(
            "wf-runtime-reactor-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create parent dir");
        }
        std::fs::write(path, content).expect("failed to write test file");
    }

    /// Minimal `wfusion.toml` pointing at `schemas/` + `rules/`, using a **file**
    /// source over an empty seed file. The file source reads to EOF and
    /// completes immediately, but in daemon mode (no auto-shutdown) the
    /// reactor stays up — so we can exercise `apply_reload` while it runs,
    /// and `wait()` returns cleanly on shutdown because the receiver task is
    /// already finished. (A TCP source would block the daemon `wait()` since
    /// its accept loop is not cancellation-aware.)
    fn fusion_toml(schemas: &str, rules: &str) -> String {
        format!(
            r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "{schemas}"
rules = "{rules}"

[vars]
FAIL_THRESHOLD = "3"
"#
        )
    }

    const SECURITY_SCHEMA: &str = r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#;

    const BRUTE_FORCE_RULE: &str = r#"
rule brute_force_then_scan {
  events {
    fail : auth_events && action == "failed"
  }

  match<sip:5m> {
    on event {
      fail | count >= ${FAIL_THRESHOLD:3};
    }
    and close {
      fail | count >= 1;
    }
  } -> score(70.0)

  entity(ip, fail.sip)

  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected", fail.sip)
  )
}
"#;

    /// A *rule-only* change: same name, same topology, different threshold
    /// logic (score 99 instead of 70). `prepare_reload` classifies this as
    /// `Ready` because neither the schema set nor the window layout changes.
    const BRUTE_FORCE_RULE_V2: &str = r#"
rule brute_force_then_scan {
  events {
    fail : auth_events && action == "failed"
  }

  match<sip:5m> {
    on event {
      fail | count >= ${FAIL_THRESHOLD:3};
    }
    and close {
      fail | count >= 1;
    }
  } -> score(99.0)

  entity(ip, fail.sip)

  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected", fail.sip)
  )
}
"#;

    /// Sink layout: one catch-all file group routed to every window. Without a
    /// real sink the bootstrap guard (`no sinks configured`) rejects startup.
    fn write_sink_layout(root: &Path) {
        write_file(
            &root.join("connectors/sink.d/file_json.toml"),
            r#"
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["file"]

[connectors.params]
fmt = "json"
file = "default.jsonl"
"#,
        );
        write_file(&root.join("sinks/defaults.toml"), "tags = [\"env:dev\"]\n");
        write_file(
            &root.join("sinks/business.d/catch_all.toml"),
            r#"
[sink_group]
name = "catch_all"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"
name = "all_alerts"

[sink_group.sinks.params]
file = "all.jsonl"
"#,
        );
    }

    /// Write the standard windows.toml referenced by `fusion_toml`.
    fn write_window_config(root: &Path) {
        write_file(
            &root.join("models/windows.toml"),
            r#"[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"
"#,
        );
    }

    /// Build a runnable reactor fixture: wfusion.toml + schema + one rule +
    /// sinks. Returns the dir and the loaded (raw, config) baseline.
    async fn bootstrap_reactor(rule: &'static str) -> (PathBuf, Reactor) {
        let root = make_temp_dir("reactor");
        write_file(
            &root.join("wfusion.toml"),
            &fusion_toml("schemas/*.wfs", "rules/*.wfl"),
        );
        write_file(&root.join("schemas/security.wfs"), SECURITY_SCHEMA);
        write_file(&root.join("rules/brute_force.wfl"), rule);
        // Empty seed file: file source reads EOF immediately and completes.
        write_file(&root.join("seed.ndjson"), "");
        write_sink_layout(&root);
        write_window_config(&root);

        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let raw = loader.load_raw().expect("load raw");
        let config = loader.load().expect("load config");
        let reactor = Reactor::start(config, raw, &root)
            .await
            .expect("reactor start");
        (root, reactor)
    }

    /// 带 `[metrics]` + monitor sink 的 reactor fixture：文件源喂 3 条 failed
    /// 事件（brute_force 阈值 3 → 至少 1 条 EMIT），metrics.ndjson 由 monitor
    /// sink 落盘。用于验证 `wait()` 的 shutdown 尾部 metrics 导出。
    async fn bootstrap_metrics_reactor() -> (PathBuf, Reactor) {
        let root = make_temp_dir("reactor-metrics");
        write_file(
            &root.join("wfusion.toml"),
            r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[metrics]
enabled = true
report_interval = "100ms"
prometheus_listen = "127.0.0.1:0"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"
"#,
        );
        write_file(&root.join("schemas/security.wfs"), SECURITY_SCHEMA);
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE);
        // 3 条 failed（同 sip）→ 第 3 条触发 on-event（count>=3）输出
        write_file(
            &root.join("seed.ndjson"),
            r#"{"sip":"10.0.0.1","username":"alice","action":"failed","event_time":"2026-01-01T00:00:00Z"}
{"sip":"10.0.0.1","username":"alice","action":"failed","event_time":"2026-01-01T00:00:01Z"}
{"sip":"10.0.0.1","username":"alice","action":"failed","event_time":"2026-01-01T00:00:02Z"}
"#,
        );
        write_sink_layout(&root);
        // monitor sink：metrics.ndjson 落盘目标（connector allow_override 仅 file，
        // 不能覆盖 base → 用默认 ./data/out_dat）
        write_file(
            &root.join("sinks/infra.d/monitor.toml"),
            r#"
[sink_group]
name = "monitor_infra"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"
name = "monitor_out"

[sink_group.sinks.params]
file = "metrics.ndjson"
"#,
        );
        write_window_config(&root);

        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let raw = loader.load_raw().expect("load raw");
        let config = loader.load().expect("load config");
        let reactor = Reactor::start(config, raw, &root)
            .await
            .expect("reactor start");
        (root, reactor)
    }

    #[tokio::test]
    async fn apply_reload_swaps_rules_when_topology_unchanged() {
        let (root, mut reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;

        // Reload with the v2 rule (score 99). Same schema/window topology → Ready.
        // prepare_reload recompiles from disk, so write the new rule first.
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let next_raw = loader.load_raw().expect("load next raw");
        let next_config = loader.load().expect("load next config");

        match reactor.apply_reload(next_raw, next_config).await {
            Ok(ReloadOutcome::Applied(_plan)) => {
                // Swap completed and the reactor remains servable.
            }
            other => panic!("expected Applied, got {other:?}"),
        }

        reactor.shutdown();
        reactor.wait().await.expect("clean shutdown after reload");

        let _ = std::fs::remove_dir_all(root);
    }

    /// Regression for M1: when the old rule generation cannot drain within the
    /// bound (here simulated by a 1ms drain timeout — too short for any real
    /// drain), `swap_rule_tasks` must abort+detach the stale supervisor rather
    /// than hang, and a subsequent `wait()` must still terminate (the detached
    /// task's `alert_tx` clone is released via `abort()`, so the alert channel
    /// can close). Before the fix this test hung forever on `wait()`.
    #[tokio::test]
    async fn apply_reload_aborts_stale_generation_and_wait_still_terminates() {
        let (root, mut reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;
        // Force the drain to always time out → exercise the abort/detach path.
        reactor.reload_drain_timeout = std::time::Duration::from_millis(1);

        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let next_raw = loader.load_raw().expect("load next raw");
        let next_config = loader.load().expect("load next config");

        match reactor.apply_reload(next_raw, next_config).await {
            Ok(ReloadOutcome::Applied(_)) => {}
            other => panic!("expected Applied even when drain times out, got {other:?}"),
        }
        // A stale generation was aborted+detached; it must be reaped here.
        reactor.shutdown();
        // Bound the whole wait defensively; before the M1 fix this hung.
        let waited = tokio::time::timeout(std::time::Duration::from_secs(15), reactor.wait()).await;
        assert!(
            waited.is_ok(),
            "wait() did not terminate within 15s — detached task likely leaked an alert_tx clone"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    /// A rule whose `|>` pipeline creates internal windows, altering the
    /// compiled runtime schema/window set. `prepare_reload` classifies the
    /// simple→pipeline switch as `Blocked` (topology change requires restart).
    /// Mirrors the proven trigger in `hot_reload::tests`.
    const PIPELINE_RULE: &str = r#"
rule repeated_fail_bursts {
  events {
    e : auth_events && action == "failed"
  }

  match<sip,username:5m:fixed> {
    on event {
      e | count >= 1;
    }
    and close {
      burst: e | count >= 3;
    }
  }
  |> match<sip:30m:fixed> {
    on event {
      _in | count >= 1;
    }
    and close {
      users: _in.username | distinct | count >= 2;
    }
  } -> score(85.0)

  entity(ip, _in.sip)

  yield security_alerts (
    sip = _in.sip,
    fail_count = 2,
    message = fmt("{} multi-user fail bursts", _in.sip)
  )
}
"#;

    #[tokio::test]
    /// Rules-only changes (different rule directory) should now be **applied**
    /// (not blocked), since L2 supports adding new windows at runtime. Pipeline
    /// rules that compile to a different window set are hot-swappable.
    async fn apply_reload_applied_when_rules_change() {
        // Two rule directories whose compiled window sets differ. The simple
        // rule uses only the declared windows; the pipeline rule's `|>` stage
        // creates internal pipeline windows. With L2 incremental reload, this
        // is now supported.
        let root = make_temp_dir("reactor-rules-change");
        write_file(
            &root.join("wfusion.toml"),
            &fusion_toml("schemas/*.wfs", "rules/v1/*.wfl"),
        );
        write_file(&root.join("schemas/security.wfs"), SECURITY_SCHEMA);
        write_file(&root.join("rules/v1/brute_force.wfl"), BRUTE_FORCE_RULE);
        write_file(
            &root.join("rules/v2/repeated_fail_bursts.wfl"),
            PIPELINE_RULE,
        );
        write_file(&root.join("seed.ndjson"), "");
        write_sink_layout(&root);
        write_window_config(&root);

        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let raw = loader.load_raw().expect("load raw");
        let config = loader.load().expect("load config");
        let mut reactor = Reactor::start(config, raw, &root)
            .await
            .expect("reactor start");

        // Next config: repoint rules glob at the v2 (pipeline) directory.
        write_file(
            &root.join("wfusion.toml"),
            &fusion_toml("schemas/*.wfs", "rules/v2/*.wfl"),
        );
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let next_raw = loader.load_raw().expect("load next raw");
        let next_config = loader.load().expect("load next config");

        let outcome = reactor
            .apply_reload(next_raw, next_config)
            .await
            .expect("apply_reload should succeed");
        assert!(
            matches!(outcome, ReloadOutcome::Applied(_)),
            "rules-only change should be hot-reloadable, got {outcome:?}"
        );

        reactor.shutdown();
        reactor
            .wait()
            .await
            .expect("clean shutdown after rules reload");

        let _ = std::fs::remove_dir_all(root);
    }

    /// wait() 的 shutdown 尾部 metrics 导出（q8 修复的集成保护）：
    ///
    /// 规则 shutdown flush 的 `emitted_total` 增量发生在 metrics 任务最后 tick
    /// 之后、且 metrics 任务（tail 组）在 rules 之前退出——wait() 必须在 rules
    /// join 后、head join 前把剩余计数器导出到 monitor sink，否则
    /// `verify-nexmark --engine-emit`（读 metrics.ndjson）漏计 EMIT
    /// （q8：30,785 + 51,661 = 82,446 = oracle 的修复路径）。
    #[tokio::test]
    async fn wait_exports_final_emitted_total_after_rules_flush() {
        let (root, reactor) = bootstrap_metrics_reactor().await;
        // 给足摄入时间：文件源读 3 条 failed → 规则 on-event 输出 + EOS flush
        // 的 close 输出，metrics 任务 100ms tick 采样（也覆盖 wait() 尾部导出
        // 路径——两条路径任一漏写都会让断言失败）。
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        reactor.shutdown();
        reactor
            .wait()
            .await
            .expect("clean shutdown with metrics tail export");

        let path = root.join("data/out_dat/metrics.ndjson");
        let data = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("metrics.ndjson not found at {}: {e}", path.display()));
        // 结构化解析（2026-08-31）：此前用 `rsplit("\"value\":\"")` 取 value，
        // 依赖 value 是行内最后一个 `"..."` 字段——metrics 加 `time` 墙钟时间戳后
        // value 后面多了字段导致解析错位（got total=0）。改为 serde_json 逐行
        // 解析，任何字段序都稳健。
        let total: u64 = data
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter(|v| {
                v.get("stage").and_then(|x| x.as_str()) == Some("alert")
                    && v.get("name").and_then(|x| x.as_str()) == Some("emitted_total")
                    && v.get("label").and_then(|x| x.as_str()) == Some("brute_force_then_scan")
            })
            .filter_map(|v| {
                v.get("value")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .sum();
        assert!(
            total >= 1,
            "shutdown 后 metrics.ndjson 必须含规则的 EMIT 计数（tick 或 wait() 尾部导出），got total={total}: {data}"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    // -- P1: control-channel / RuntimeControlHandle --------------------------

    /// Reload a config from a fixture dir, returning (raw, config). Reused by
    /// the control-channel tests.
    fn load_next(root: &Path) -> (wf_config::RawFusionConfigTree, wf_config::FusionConfig) {
        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(root));
        (
            loader.load_raw().expect("load next raw"),
            loader.load().expect("load next config"),
        )
    }

    /// Cross-task reload through `RuntimeControlHandle` over the control
    /// channel (P1): the handle is moved to a separate task, the reactor is
    /// driven by `run()` in another, and the reload reply round-trips back.
    #[tokio::test]
    async fn control_handle_apply_reload_round_trips_across_tasks() {
        let (root, reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;
        let control = reactor.control_handle();
        // Drive the control loop (signal watcher + reload select + wait).
        let run_task = tokio::spawn(async move { reactor.run().await });

        // From a distinct task, request a reload via the handle.
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let reload_root = root.clone();
        let ctrl = control.clone();
        let requester = tokio::spawn(async move {
            let (next_raw, next_config) = load_next(&reload_root);
            ctrl.apply_reload(next_raw, next_config).await
        });

        match requester.await.expect("requester panicked") {
            Ok(ReloadOutcome::Applied(_)) => {}
            other => panic!("expected Applied via control handle, got {other:?}"),
        }

        // Shut down via the handle's token and let `run` finish.
        control.cancel_token().cancel();
        run_task
            .await
            .expect("run task panicked")
            .expect("run returned an error after reload");

        let _ = std::fs::remove_dir_all(root);
    }

    /// Concurrent reload requests are serialised by the single Reactor control
    /// loop: both complete successfully and the channel/loop never deadlocks.
    #[tokio::test]
    async fn control_handle_serialises_concurrent_reloads() {
        let (root, reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;
        let control = reactor.control_handle();
        let run_task = tokio::spawn(async move { reactor.run().await });

        // Two concurrent reload requests for the same (rule-only) change.
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let (ra, ca) = (root.clone(), control.clone());
        let (rb, cb) = (root.clone(), control.clone());
        let t1 = tokio::spawn(async move {
            let (raw, cfg) = load_next(&ra);
            ca.apply_reload(raw, cfg).await
        });
        let t2 = tokio::spawn(async move {
            let (raw, cfg) = load_next(&rb);
            cb.apply_reload(raw, cfg).await
        });
        let (o1, o2) = (t1.await.expect("t1"), t2.await.expect("t2"));
        // Both must resolve (no deadlock). One is Applied; the other is either
        // Applied again (idempotent v2→v2) or Applied — both acceptable so long
        // as neither is an Err or a hang.
        for (i, o) in [o1, o2].into_iter().enumerate() {
            match o {
                Ok(ReloadOutcome::Applied(_)) | Ok(ReloadOutcome::Blocked(_)) => {}
                other => panic!("concurrent reload #{i} did not resolve cleanly: {other:?}"),
            }
        }

        control.cancel_token().cancel();
        run_task
            .await
            .expect("run task panicked")
            .expect("run error");

        let _ = std::fs::remove_dir_all(root);
    }
}
