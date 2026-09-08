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

use tokio_util::sync::CancellationToken;

use wf_engine::window::Router;

use super::bootstrap::engine_rows_from_knowdb;
use wp_knowledge::refresh::RefreshService;
use wp_knowledge::refresh::RefreshSpec;

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
// 供给动态变量代码（$cur/$next/$max_age）——knowdb 求值，引擎只透传配置
// ---------------------------------------------------------------------------

/// boot 装载同源渲染：与 knowdb 每次刷新的渲染是同一函数（[`RefreshSpec`]
/// NamedSql 携带 `code` 原样交给 knowdb；这里按此刻值渲染一次供启动装载）。
/// 空代码 = 原样返回。
pub(crate) fn render_supply_sql(sql: &str, code: &str) -> wp_knowledge::KnowledgeResult<String> {
    wp_knowledge::refresh::render_refresh_code(
        sql,
        code,
        wp_knowledge::refresh::current_wall_nanos(),
    )
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
                code: String::new(),
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
    fn render_supply_sql_empty_code_passes_through() {
        let sql = "SELECT * FROM t";
        assert_eq!(render_supply_sql(sql, "").unwrap(), sql);
        assert_eq!(render_supply_sql(sql, "  \n# comment\n").unwrap(), sql);
        // 非法代码 → 报错（boot 期即暴露配置错误，而非静默）
        assert!(render_supply_sql(sql, "$x = nope(1)").is_err());
    }
}
