//! 远端 A 定期刷新（S2-M3b，v1 = CSV 重载）。
//!
//! provider 静态窗（`window<provider>`，knowdb 装载）在 **daemon 运行期**按
//! `knowdb.toml [[tables]] refresh = "5m"` 周期重新读数据源并
//! `ProviderWindow::load()` 替换内存行（load 自动重建 join 索引）——解决长跑
//! 基线 stale（baseline-online-design.md §11.5 M3b；run_long 暴露项）。
//!
//! 分工：bootstrap 装载时经 [`register_spec`] 收集规格；daemon 主循环启动本模块
//! 的 [`run_provider_refresh`] 任务按各自周期触发。PG/外部 SQL 供给的重载
//! 留 TODO（`ReloadKind` 枚举已预留；当前仅 CSV）。

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use super::bootstrap::read_knowledge_csv;
#[cfg(test)]
use crate::error::RuntimeResult;

/// 数据源种类（v1 仅 CSV；PG 供给后续扩展）。
#[derive(Debug, Clone)]
pub(crate) enum ReloadKind {
    Csv(PathBuf),
    // TODO(S2-M3b): Postgres { uri, table } —— 复用 load_from_postgres 逻辑
}

/// 一条 provider 定期刷新规格。
#[derive(Debug, Clone)]
pub(crate) struct ReloadSpec {
    pub name: String,
    pub interval: Duration,
    pub kind: ReloadKind,
}

// ---------------------------------------------------------------------------
// 规格收集（bootstrap 装载时填充；daemon 启动时取出）
// ---------------------------------------------------------------------------

static SPECS: OnceLock<Mutex<Vec<ReloadSpec>>> = OnceLock::new();

fn spec_store() -> &'static Mutex<Vec<ReloadSpec>> {
    SPECS.get_or_init(|| Mutex::new(Vec::new()))
}

/// 清空规格（每次引擎启动前调用，防跨 run 残留）。
pub(crate) fn reset_specs() {
    spec_store().lock().unwrap().clear();
}

/// 注册一条刷新规格（bootstrap 解析 knowdb [[tables]] refresh 后调用）。
pub(crate) fn register_spec(spec: ReloadSpec) {
    spec_store().lock().unwrap().push(spec);
}

/// 取出全部规格（daemon 刷新任务启动时一次性消费）。
pub(crate) fn take_specs() -> Vec<ReloadSpec> {
    std::mem::take(&mut *spec_store().lock().unwrap())
}

// ---------------------------------------------------------------------------
// daemon 刷新循环
// ---------------------------------------------------------------------------

/// 主循环：按各 spec 周期整表重读并 load() 替换 ProviderWindow 内存行。
/// 采用"最小周期 tick + 到期判定"：refresh 各不同也只需一个定时器。
pub(crate) async fn run_provider_refresh(
    router: Arc<wf_engine::window::Router>,
    cancel: CancellationToken,
) {
    let specs = take_specs();
    if specs.is_empty() {
        return;
    }
    let base_tick = specs
        .iter()
        .map(|s| s.interval)
        .min()
        .unwrap_or(Duration::from_secs(1))
        .clamp(Duration::from_millis(200), Duration::from_secs(3600));
    let mut next_due: Vec<Instant> = specs.iter().map(|s| Instant::now() + s.interval).collect();

    let mut ticker = tokio::time::interval(base_tick);
    // 首次 tick 立即到期（tokio interval 首 tick 立即触发，正好用于初始空跑判定）。
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }
        let now = Instant::now();
        for (i, spec) in specs.iter().enumerate() {
            if now >= next_due[i] {
                reload_spec(&router, spec).await;
                next_due[i] = Instant::now() + spec.interval;
            }
        }
    }
}

/// 单表重载：读数据源 → 找到 provider → `load()` 替换（索引重建由 load 完成）。
async fn reload_spec(router: &wf_engine::window::Router, spec: &ReloadSpec) {
    let rows = match &spec.kind {
        ReloadKind::Csv(path) => read_knowledge_csv(path),
    };
    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            log::warn!("provider refresh 重读失败 {}: {e}", spec.name);
            return;
        }
    };
    let Some(provider) = router.registry().get_provider(&spec.name) else {
        log::warn!("provider refresh: 窗口 {} 未注册", spec.name);
        return;
    };
    match provider.write() {
        Ok(mut guard) => {
            guard.load(rows);
            log::info!(
                "provider refresh loaded table={} rows={}",
                spec.name,
                guard.row_count()
            );
        }
        Err(_) => log::warn!("provider refresh: {} 锁中毒", spec.name),
    }
}

/// 单表重载（同步、无 router 依赖的纯数据面读取——测试用）。
/// 返回该表当前数据行数。
#[cfg(test)]
pub(crate) fn reload_spec_rows(spec: &ReloadSpec) -> RuntimeResult<usize> {
    let rows = match &spec.kind {
        ReloadKind::Csv(path) => read_knowledge_csv(path)?,
    };
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_store_push_take_roundtrip() {
        reset_specs();
        assert!(take_specs().is_empty());
        register_spec(ReloadSpec {
            name: "t".into(),
            interval: Duration::from_secs(5),
            kind: ReloadKind::Csv(PathBuf::from("/nonexistent/x.csv")),
        });
        let specs = take_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "t");
        assert_eq!(specs[0].interval.as_secs(), 5);
    }

    #[test]
    fn reload_rows_reads_current_csv_content() {
        // 用 bootstrap 共享 reader：写 v1 → reload_rows=1；覆盖 v2（两行）→ reload_rows=2
        let dir =
            std::env::temp_dir().join(format!("wf_provider_refresh_ut_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.csv");
        std::fs::write(&path, "key,value\n1,a\n").unwrap();
        let spec = ReloadSpec {
            name: "t".into(),
            interval: Duration::from_secs(1),
            kind: ReloadKind::Csv(path.clone()),
        };
        assert_eq!(reload_spec_rows(&spec).unwrap(), 1);
        std::fs::write(&path, "key,value\n1,a\n2,b\n").unwrap();
        assert_eq!(reload_spec_rows(&spec).unwrap(), 2, "重读应反映最新文件");
    }

    #[test]
    fn min_tick_and_due_compute() {
        // base_tick = 最短 refresh；到期计算仅依赖 spec.interval（纯逻辑冒烟）。
        let specs = vec![
            ReloadSpec {
                name: "a".into(),
                interval: Duration::from_secs(30),
                kind: ReloadKind::Csv(PathBuf::new()),
            },
            ReloadSpec {
                name: "b".into(),
                interval: Duration::from_secs(5),
                kind: ReloadKind::Csv(PathBuf::new()),
            },
        ];
        let base = specs
            .iter()
            .map(|s| s.interval)
            .min()
            .unwrap()
            .clamp(Duration::from_millis(200), Duration::from_secs(3600));
        assert_eq!(base, Duration::from_secs(5), "tick 取最短周期");
    }
}
