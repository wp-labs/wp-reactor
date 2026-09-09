//! 远端 A 定期刷新：引擎消费 KnowDB refresh 服务（**数据在 knowdb，函数交付**）。
//!
//! 分工（S2-M3c → 换代-信号-拉取模型）：
//! - knowdb（`wp_knowledge::refresh`）**拥有**数据源周期更新与换代存储：每次 tick
//!   重载 → 换入共享 [`TableStore`]（O(1) Arc 换代）→ 只发**信号**（无数据载荷）；
//! - 本模块（引擎侧）做三件事：bootstrap 登记 [`RefreshSpec`] 并把启动装载 seed 进
//!   同一个 store；daemon 启动 [`wp_knowledge::refresh::RefreshService`] 并消费信号；
//!   收到信号 → [`TableStore::snapshot`] pull 当前代（Arc，零数据复制）→ 边界转
//!   引擎行（`bootstrap::engine_rows_from_knowdb`）→ `ProviderWindow::rebuilt()`
//!   **锁外整建**（rows + join 索引 + 预物化行，O(rows) 重活不占锁）→ 写锁内
//!   O(1) `swap_in` 整窗换入（double-buffer swap）。
//!
//! 交付统一为**函数调用**：启动（`load_rows` 同步装载 seed store）与刷新（信号后
//! `snapshot` pull）走同一 store/apply 面；引擎不再自行渲染 VEL/拼查询。
//!
//! 日志行 `provider refresh loaded table=… rows=…` 为刷新生效断言锚点（daemon
//! 日志/回归用，勿改格式）。

use std::sync::{Arc, Mutex, OnceLock};

use tokio_util::sync::CancellationToken;

use wf_engine::window::ProviderWindow;
use wf_engine::window::Router;

use super::bootstrap::engine_rows_from_knowdb;
use wp_knowledge::refresh::RefreshService;
use wp_knowledge::refresh::RefreshSpec;
use wp_knowledge::refresh::TableData;
use wp_knowledge::refresh::TableStore;

/// 刷新规格库（bootstrap 装载时登记；daemon 刷新任务启动时一次性取出）。
static SPECS: OnceLock<Mutex<Vec<RefreshSpec>>> = OnceLock::new();

/// 共享表快照库（bootstrap seed 与 daemon 换代共用同一实例 → 启动/刷新同交付面）。
static STORE: OnceLock<Mutex<Arc<TableStore>>> = OnceLock::new();

fn spec_store() -> &'static Mutex<Vec<RefreshSpec>> {
    SPECS.get_or_init(|| Mutex::new(Vec::new()))
}

fn store_mutex() -> &'static Mutex<Arc<TableStore>> {
    STORE.get_or_init(|| Mutex::new(Arc::new(TableStore::default())))
}

/// 取共享快照库句柄（Arc clone）。
pub(crate) fn store_handle() -> Arc<TableStore> {
    Arc::clone(&*store_mutex().lock().unwrap())
}

/// 清空规格并重建空快照库（每次引擎启动前调用，防跨 run 残留）。
pub(crate) fn reset_specs() {
    spec_store().lock().unwrap().clear();
    *store_mutex().lock().unwrap() = Arc::new(TableStore::default());
}

/// 登记一条 KnowDB 刷新规格（bootstrap 解析 knowdb [[tables]] refresh 后调用）。
pub(crate) fn register_spec(spec: RefreshSpec) {
    spec_store().lock().unwrap().push(spec);
}

/// 取出全部规格（daemon 刷新任务启动时一次性消费）。
pub(crate) fn take_specs() -> Vec<RefreshSpec> {
    std::mem::take(&mut *spec_store().lock().unwrap())
}

/// 启动装载 seed：把一代数据放进共享 store（与 daemon 换代同库）。
pub(crate) fn seed_store(data: Arc<TableData>) {
    store_mutex().lock().unwrap().insert(data);
}

// ---------------------------------------------------------------------------
// daemon 刷新循环：启动 knowdb 服务 + 消费信号（数据经 store 函数交付）
// ---------------------------------------------------------------------------

/// 启动 [`RefreshService`]（knowdb 侧各 spec 独立计时、换代并发通知），并消费
/// 信号：收到换代信号 → `store.snapshot` pull 当前代 → 搬入对应 ProviderWindow。
/// 空规格 / 通道关闭 → 干净退出。
pub(crate) async fn run_provider_refresh(router: Arc<Router>, cancel: CancellationToken) {
    let specs = take_specs();
    if specs.is_empty() {
        return;
    }
    run_provider_refresh_with(router, specs, store_handle(), cancel).await;
}

/// 与 [`run_provider_refresh`] 同构，但 spec 与 store 由调用方注入（测试用：
/// 自带 store/specs，不碰模块静态，天然可并行）。
async fn run_provider_refresh_with(
    router: Arc<Router>,
    specs: Vec<RefreshSpec>,
    store: Arc<TableStore>,
    cancel: CancellationToken,
) {
    let mut service = RefreshService::spawn_with_store(specs, store);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            sig = service.signals.recv() => match sig {
                Some(signal) => {
                    // 数据在 store（knowdb 已换代）；pull = 函数调用（Arc 零复制）。
                    // 信号丢一两次无害：store 始终是当前代，下个信号再 pull 即最新。
                    let Some(data) = service.store.snapshot(&signal.name) else {
                        log::warn!("provider refresh: 信号 {} 但 store 无当前代", signal.name);
                        continue;
                    };
                    apply_rows(&router, &signal.name, &data.rows);
                }
                None => break, // 全部 spec 任务退出 / 信号通道关闭
            }
        }
    }
}

/// 单表搬入：store 当前代原生行 → 引擎行 → **double-buffer swap**：
/// 1. 短暂读锁仅取 spawn 期固定配置（table/query/refresh/join key，O(1) clone）；
/// 2. 锁外整建新窗口（[`ProviderWindow::rebuilt`]：rows + join 索引 + 预物化行，
///    O(rows) 重活不占锁——构建期间读者继续用旧表）；
/// 3. 写锁内仅 O(1) [`ProviderWindow::swap_in`] 整窗换入。
///
/// 读者在事件边界看到完整旧表或完整新表，永不读到索引与行不配套的中间态。
fn apply_rows(router: &Router, name: &str, rows: &[wp_knowledge::mem::RowData]) {
    let Some(provider) = router.registry().get_provider(name) else {
        log::warn!("provider refresh: 窗口 {} 未注册", name);
        return;
    };
    // join key 等配置 spawn 期设置后刷新不改——短暂读锁 clone 后即释放，
    // 读锁不再覆盖锁外的 O(rows) 整建。
    let (table, query, refresh, join_key) = match provider.read() {
        Ok(guard) => (
            guard.table.clone(),
            guard.query.clone(),
            guard.refresh,
            guard.join_key().map(str::to_owned),
        ),
        Err(_) => {
            log::warn!("provider refresh: {} 锁中毒（读）", name);
            return;
        }
    };
    let engine_rows = engine_rows_from_knowdb(rows);
    let next = ProviderWindow::rebuilt(table, query, refresh, join_key, engine_rows);
    match provider.write() {
        Ok(mut guard) => {
            guard.swap_in(next);
            log::info!(
                "provider refresh loaded table={} rows={}",
                name,
                guard.row_count()
            );
        }
        Err(_) => log::warn!("provider refresh: {} 锁中毒", name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::time::Duration;
    use wp_knowledge::refresh::RefreshSource;

    /// provider_refresh 内部测试的串行锁：两个测试都改/读共享静态（SPECS/STORE），
    /// 并行跑会确定性互踩（各自 reset 重建静态）——串行化后行为确定。
    /// （生产路径单引擎单线程 boot→daemon，无并发 reset；此锁仅测试隔离用。）
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn spec_store_push_take_roundtrip() {
        let _guard = TEST_LOCK.lock().unwrap();
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
    fn shared_store_seed_and_reset_roundtrip() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_specs();
        assert!(
            store_handle().snapshot("x").is_none(),
            "reset 后应为空 store"
        );

        // seed → 同库 pull（与 daemon 换代共用同一实例——启动/刷新同交付面）。
        seed_store(Arc::new(TableData {
            name: "x".into(),
            rows: Vec::new(),
        }));
        assert!(
            store_handle().snapshot("x").is_some(),
            "seed 后当前代可 pull"
        );

        // reset 重建空库。
        reset_specs();
        assert!(store_handle().snapshot("x").is_none(), "reset 重建空库");
    }

    #[tokio::test]
    async fn daemon_signal_pull_applies_store_generation_into_provider_window() {
        // 端到端（daemon 消费循环）：knowdb tick 换代 store → 发信号 → 引擎
        // pull 当前代（函数取数）→ apply_rows（rebuilt + swap_in）→ ProviderWindow
        // 出现首代数据。锚点 = 窗口行数从 0 → 2。
        // 注入自有 store/specs（run_provider_refresh_with），不碰模块静态——
        // 与其它测试天然可并行。
        use wf_engine::window::WindowRegistry;
        use wp_knowledge::mem::memdb::MemDB;

        let db = MemDB::instance();
        db.execute("CREATE TABLE daemon_refresh_e2e (k TEXT, v TEXT)")
            .expect("create");
        db.execute("INSERT INTO daemon_refresh_e2e VALUES ('a', '1'), ('b', '2')")
            .expect("seed");
        wp_knowledge::facade::init_mem_provider(db).expect("init mem provider");

        let name = "daemon_refresh_e2e";
        let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
        registry
            .register_provider(
                name.to_string(),
                ProviderWindow::new(
                    name.to_string(),
                    format!("SELECT * FROM {name}"),
                    Some(Duration::from_millis(80)),
                ),
            )
            .expect("register provider");
        let router = Arc::new(Router::new(registry));
        let spec = RefreshSpec {
            name: name.to_string(),
            interval: Duration::from_millis(80),
            source: RefreshSource::NamedSql {
                provider: "default".to_string(),
                sql: format!("SELECT k, v FROM {name}"),
                code: String::new(),
            },
        };

        let store = Arc::new(TableStore::default());
        let cancel = CancellationToken::new();
        let daemon = tokio::spawn(run_provider_refresh_with(
            router.clone(),
            vec![spec],
            Arc::clone(&store),
            cancel.clone(),
        ));

        let deadline = std::time::Instant::now() + Duration::from_secs(4);
        let mut applied = false;
        while std::time::Instant::now() < deadline {
            let rows = router
                .registry()
                .get_provider(name)
                .expect("provider")
                .read()
                .expect("read lock")
                .snapshot();
            if rows.len() == 2 {
                applied = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(applied, "首代数据应经信号→pull→apply 落入窗口（期望 2 行）");

        // 第二阶段：join key 在 spawn 期配置（spawn_rule_tasks 同路径）后，必须
        // **跨刷新换代保留**：设索引 → 等下一次 store 换代（Arc 指针变化）→ 窗口
        // apply 后验证索引随新代重建、旧索引语义仍正确。
        let provider = router.registry().get_provider(name).expect("provider");
        provider
            .write()
            .expect("write lock")
            .set_join_key("k".into());
        let gen_before = store.snapshot(name).expect("首代");

        let deadline2 = std::time::Instant::now() + Duration::from_secs(4);
        let mut indexed = false;
        while std::time::Instant::now() < deadline2 {
            // 换代发生（新 Arc）且窗口已按新代重建索引 → join 命中。
            let Some(generation) = store.snapshot(name) else {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            let swapped = !Arc::ptr_eq(&generation, &gen_before);
            use wf_engine::match_engine::Value;
            let hits = provider
                .read()
                .expect("read lock")
                .join_rows_lookup(&Value::Str("b".into()));
            if swapped && hits.is_some() {
                indexed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(indexed, "join key 应在换代后保留且索引随新代重建");
        {
            use wf_engine::match_engine::Value;
            let guard = provider.read().expect("read lock");
            assert_eq!(guard.join_key(), Some("k"), "swap 后 join key 配置保留");
            assert_eq!(
                guard
                    .join_rows_lookup(&Value::Str("b".into()))
                    .expect("索引命中")
                    .len(),
                1,
                "k=b → 1 行"
            );
            assert_eq!(
                guard
                    .join_rows_lookup(&Value::Str("a".into()))
                    .expect("索引命中")
                    .len(),
                1,
                "k=a → 1 行"
            );
        }

        cancel.cancel();
        let _ = daemon.await;
    }
}
