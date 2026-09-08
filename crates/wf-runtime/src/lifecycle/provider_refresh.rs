//! 远端 A 定期刷新（S2-M3c）：引擎消费 KnowDB refresh 服务事件并搬入 join 缓存。
//!
//! 分工（2026-09-07，S2-M3b → S2-M3c 重构）：
//! - knowdb（`wp_knowledge::refresh`）**拥有**各数据源的周期更新与并发通知——
//!   事件载荷 = knowdb 原生行（`RefreshEvent{ name, rows }`）；
//! - 本模块（引擎侧）只做两件事：bootstrap 登记 [`RefreshSpec`]；daemon 启动
//!   [`wp_knowledge::refresh::RefreshService`] 并跑消费循环——收到事件 → 边界转
//!   引擎行（`bootstrap::engine_rows_from_knowdb`）→ `ProviderWindow::load()`
//!   （整表换行 + 重建 join 索引）。引擎不再自行读 CSV/计时/重载（v1 CSV 由
//!   knowdb loader 的 authority 单表重载承担；见 baseline-online-design.md §11）。
//!
//! 日志行 `provider refresh loaded table=… rows=…` 为刷新生效断言锚点（daemon
//! 日志/回归用，勿改格式）。

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use wf_engine::window::Router;

use super::bootstrap::engine_rows_from_knowdb;
use wp_knowledge::refresh::resolve_sql_vars;
use wp_knowledge::refresh::{RefreshService, RefreshSpec, RefreshVars};

/// 刷新规格库（bootstrap 装载时登记；daemon 刷新任务启动时一次性取出）。
static SPECS: OnceLock<Mutex<Vec<RefreshSpec>>> = OnceLock::new();

fn spec_store() -> &'static Mutex<Vec<RefreshSpec>> {
    SPECS.get_or_init(|| Mutex::new(Vec::new()))
}

/// 清空规格（每次引擎启动前调用，防跨 run 残留）。
pub(crate) fn reset_specs() {
    spec_store().lock().unwrap().clear();
}

/// 登记一条 KnowDB 刷新规格（bootstrap 解析 knowdb [[tables]] refresh 后调用）。
pub(crate) fn register_spec(spec: RefreshSpec) {
    spec_store().lock().unwrap().push(spec);
}

/// 取出全部规格（daemon 刷新任务启动时一次性消费）。
pub(crate) fn take_specs() -> Vec<RefreshSpec> {
    std::mem::take(&mut *spec_store().lock().unwrap())
}

// ---------------------------------------------------------------------------
// daemon 刷新循环：启动 knowdb 服务 + 消费事件（搬数据）
// ---------------------------------------------------------------------------

/// 启动 [`RefreshService`]（knowdb 侧各 spec 独立计时、并发通知），并把每次
/// 成功重载的原生行搬入对应 ProviderWindow。空规格 / 通道关闭 → 干净退出。
pub(crate) async fn run_provider_refresh(router: Arc<Router>, cancel: CancellationToken) {
    let specs = take_specs();
    if specs.is_empty() {
        return;
    }
    let mut service = RefreshService::spawn(specs);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            ev = service.events.recv() => match ev {
                Some(event) => apply_event(&router, event),
                None => break, // 全部 spec 任务退出 / 事件通道关闭
            }
        }
    }
}

/// 单表搬入：原生行 → 引擎行 → `ProviderWindow::load()`（写锁内整表替换，
/// load 自动重建 join 索引；读者按读锁在事件边界看到新表）。
fn apply_event(router: &Router, event: wp_knowledge::refresh::RefreshEvent) {
    let Some(provider) = router.registry().get_provider(&event.name) else {
        log::warn!("provider refresh: 窗口 {} 未注册", event.name);
        return;
    };
    let rows = engine_rows_from_knowdb(event.rows);
    match provider.write() {
        Ok(mut guard) => {
            guard.load(rows);
            log::info!(
                "provider refresh loaded table={} rows={}",
                event.name,
                guard.row_count()
            );
        }
        Err(_) => log::warn!("provider refresh: {} 锁中毒", event.name),
    }
}

// ---------------------------------------------------------------------------
// 供给动态变量（$cur/$next/$max_age）——引擎现算，knowdb 只做值替换
// ---------------------------------------------------------------------------

/// 相位标签：桶序号 → chars 标签（与供给窗/事件打标同口径 'p0'..）。
pub(crate) fn phase_bucket_label(idx: u64) -> String {
    format!("p{idx}")
}

/// 在给定时刻 `now_ns` 现算供给变量：
/// - `cur`  = 当前相位标签（`fold(now)`：`(now mod period) div bucket`，epoch 折叠）；
/// - `next` = 下一相位标签（`fold(now + bucket)`，周期末回绕到 p0）；
/// - `max_age` = 静态保留期字面量（写死，如 `30 days`）。
/// 语义 = A 通道处理时间近似：供给只取当前/下一相位对应格（N=period/bucket 由
/// 调用侧配置）；SQL 模板里的 `$cur/$next/$max_age` 由 wp_knowledge 在每次
/// 执行前替换（refresh::resolve_sql_vars）。
pub(crate) fn phase_vars_at(
    now_ns: u64,
    period_s: u64,
    bucket_s: u64,
    retention: &str,
) -> Vec<(String, String)> {
    let period_ns = period_s.saturating_mul(1_000_000_000);
    let bucket_ns = bucket_s.saturating_mul(1_000_000_000);
    // 0<桶≤周期 已由调用侧校验；此处防御性回退 p0（不 panic）。
    let fold = |t: u64| {
        if period_ns == 0 || bucket_ns == 0 || bucket_ns > period_ns {
            0
        } else {
            (t % period_ns) / bucket_ns
        }
    };
    vec![
        ("cur".to_string(), phase_bucket_label(fold(now_ns))),
        (
            "next".to_string(),
            phase_bucket_label(fold(now_ns.saturating_add(bucket_ns))),
        ),
        ("max_age".to_string(), retention.to_string()),
    ]
}

/// 构造"每 tick 现算"的 [`RefreshVars`]（wall-clock）；参数已在调用侧校验
/// （period/bucket/retention 齐全且 0<桶≤周期）。
pub(crate) fn phase_refresh_vars(period_s: u64, bucket_s: u64, retention: String) -> RefreshVars {
    RefreshVars::new(move || {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Ok(phase_vars_at(now_ns, period_s, bucket_s, &retention))
    })
}

/// boot 装载同源渲染：与 [`phase_refresh_vars`] 同一口径（此刻值）。
pub(crate) fn render_supply_sql(
    sql: &str,
    vars: Option<&RefreshVars>,
) -> wp_knowledge::KnowledgeResult<String> {
    match vars {
        Some(v) => Ok(resolve_sql_vars(sql, &v.compute()?)),
        None => Ok(sql.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::time::Duration;
    use wp_knowledge::refresh::RefreshSource;

    #[test]
    fn spec_store_push_take_roundtrip() {
        reset_specs();
        assert!(take_specs().is_empty());
        register_spec(RefreshSpec {
            name: "t".into(),
            interval: Duration::from_secs(5),
            source: RefreshSource::NamedSql {
                provider: "pg".into(),
                sql: "SELECT * FROM t".into(),
                vars: None,
            },
        });
        register_spec(RefreshSpec {
            name: "c".into(),
            interval: Duration::from_secs(1),
            source: RefreshSource::Authority {
                root: PathBuf::from("/nonexistent"),
                conf: PathBuf::from("knowdb.toml"),
                authority_uri: "file:/nonexistent/a.sqlite".into(),
                table: "c".into(),
            },
        });
        let specs = take_specs();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "t");
        assert_eq!(specs[0].interval.as_secs(), 5);
        assert_eq!(specs[1].name, "c");
        assert!(take_specs().is_empty(), "取后应清空");
    }

    #[test]
    fn phase_vars_at_folds_cur_next_and_keeps_static_max_age() {
        // period=240s/bucket=15s（演示档，N=16）：
        // 120s → 桶 8；+15s（下一格 135s）→ 桶 9；max_age 静态透传。
        let ns = |s: u64| s.saturating_mul(1_000_000_000);
        let v = phase_vars_at(ns(120), 240, 15, "30 days");
        assert_eq!(
            v,
            vec![
                ("cur".to_string(), "p8".to_string()),
                ("next".to_string(), "p9".to_string()),
                ("max_age".to_string(), "30 days".to_string()),
            ]
        );
    }

    #[test]
    fn phase_vars_at_next_wraps_at_period_boundary() {
        let ns = |s: u64| s.saturating_mul(1_000_000_000);
        // 225s = 周期末最后一格（桶 15）；下一格 240s 回绕到周期首（桶 0）。
        let v = phase_vars_at(ns(225), 240, 15, "2 hours");
        let map: std::collections::HashMap<&str, &str> = v
            .iter()
            .map(|(k, val)| (k.as_str(), val.as_str()))
            .collect();
        assert_eq!(map["cur"], "p15");
        assert_eq!(map["next"], "p0");
    }

    #[test]
    fn phase_vars_at_defensive_fallback_on_bad_params() {
        let ns = |s: u64| s.saturating_mul(1_000_000_000);
        // 桶 > 周期 / 0 参数（调用侧已拦，此处防御）：退化为 p0 不 panic。
        for (period_s, bucket_s) in [(15u64, 240u64), (240, 0), (0, 15)] {
            let v = phase_vars_at(ns(120), period_s, bucket_s, "2 hours");
            let map: std::collections::HashMap<&str, &str> = v
                .iter()
                .map(|(k, val)| (k.as_str(), val.as_str()))
                .collect();
            assert_eq!(map["cur"], "p0");
            assert_eq!(map["next"], "p0");
        }
    }
}
