//! 通用异步落盘机制（2026-08-27 M6，见 `docs/design/async-persist.md`）。
//!
//! ## 问题
//! 热路径的同步持久化写（redb 事务/文件 IO）会阻塞 ingest（q18 spill 驱逐
//! profile：写段占驱逐耗时 99%，2.7-25s/批）。本模块把"逻辑移除 + 入队"留在
//! 热路径（O(1)），持久化写交给后台 worker 线程——**内存释放同步、落盘异步**。
//!
//! ## 通用性
//! - [`BatchWriter`]：后端 trait（redb 单事务 / 文件 append / ...），一批一次写。
//! - [`AsyncPersister`]：有界队列 + 字节预算背压 + 攒批 + flush + 失败回调。
//! - 数据单元 `T` 任意（`Send`）；`est_bytes` 由调用方估算（字节背压）。
//!
//! ## 正确性
//! - **保序**：单 worker FIFO——同 key 多次驱逐的后写覆盖先写（redb 覆盖幂等）。
//! - **背压**：`queued_bytes + est > budget` → [`PersistError::Backpressure`]，
//!   调用方退化为同步写（内存有界不因积压复涨）。
//! - **flush**：close/读回前等队列排空（Condvar），保证"已提交 = 已可见"。
//! - **失败**：后端写失败 → `error_cb` 回调（策略注入；默认丢弃该批 + 告警）。

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// condvar 等待看门狗（2026-09 review）：worker 是 `std::thread`，系统过载时
/// 可能长时间得不到调度——`flush`/背压等待若无线索会**无限挂起**（测试侧表现
/// 为 runner 报 "running for over 60 seconds"）。超时后直接 panic 快速失败，
/// 把静默挂起变成可诊断错误。阈值 60s > 最坏单批写（q18 实测 25s/批）×2；
/// 测试态缩到 200ms 以便验证 watchdog 路径（见 `watchdog_timeout`）。
#[cfg(not(test))]
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(60);

fn watchdog_timeout() -> Duration {
    #[cfg(test)]
    {
        Duration::from_millis(200)
    }
    #[cfg(not(test))]
    {
        WATCHDOG_TIMEOUT
    }
}

/// 持久化提交错误。
#[derive(Debug)]
pub enum PersistError {
    /// 队列字节预算已满——调用方应退化为同步写（背压兜底）。
    Backpressure,
    /// 同步兜底写失败（后端错误）。
    Failed(String),
    /// 已在 shutdown 后提交（通道关闭）。
    Closed,
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistError::Backpressure => write!(f, "异步落盘队列超预算（背压）"),
            PersistError::Failed(e) => write!(f, "同步兜底写失败: {e}"),
            PersistError::Closed => write!(f, "异步落盘已停止"),
        }
    }
}

impl std::error::Error for PersistError {}

/// 写失败回调类型（策略注入：告警/拒收/重试）。
pub type ErrorCb = std::sync::Arc<dyn Fn(&str) + Send + Sync>;

/// 批量写后端：一批数据一次事务/一次 IO。
pub trait BatchWriter<T> {
    /// 写一批（成功 = 已持久化/已消化；失败 = 交给 error_cb 策略）。
    fn write_batch(&mut self, items: Vec<T>) -> Result<(), String>;
}

/// 通用异步落盘队列（见模块文档）。
///
/// 线程模型：`std::thread`（不依赖 tokio——热路径可能无 runtime；阻塞 IO
/// 放专用线程反而避免 executor 饥饿）。**多 worker + 路由**（2026-08-27）：
/// 提交时按 `route`（目标文件 hash）路由到 `hash % N` 的 worker——同一存储
/// 目标恒由同一 worker 串行写（redb 单写者约束），不同目标并行写（总吞吐
/// 不塌方）。对比：单 worker（无并行，小写量浪费并行度）与每目标一 worker
/// （N 路并发写共享磁盘 → 单批延迟退化 3.6x 实测）。
pub struct AsyncPersister<T, B> {
    txs: Vec<SyncSender<(Vec<T>, usize)>>,
    /// 队列字节预算（背压阈值；全局共享）。
    byte_budget: usize,
    /// 当前在途字节（已提交未写清；全局——所有 worker 消费同一预算）。
    queued_bytes: Arc<AtomicUsize>,
    /// 队列空标志（Condvar 通知；全部 worker 写清后置 true）。
    idle: Arc<(Mutex<bool>, Condvar)>,
    workers: Vec<JoinHandle<()>>,
    _backend: PhantomData<B>,
}

impl<T: Send + 'static, B: BatchWriter<T> + Send + 'static> AsyncPersister<T, B> {
    /// 创建并启动后台 worker（每 worker 一个 `backend` 实例）。
    ///
    /// - `backends`：每 worker 一个写后端（worker 线程持有）。
    /// - `byte_budget`：队列字节预算（全局，超限阻塞 = 背压）。
    /// - `max_pending_batches`：每 worker 通道批容量（防批数无限堆积）。
    /// - `max_batch_bytes`：攒批字节上限（0 = 不攒批；见结构注释）。
    /// - `error_cb`：写失败回调（None = 仅 `eprintln` 告警）。
    pub fn new(
        backends: Vec<B>,
        byte_budget: usize,
        max_pending_batches: usize,
        max_batch_bytes: usize,
        error_cb: Option<ErrorCb>,
    ) -> Self {
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let idle = Arc::new((Mutex::new(true), Condvar::new()));
        let mut txs = Vec::with_capacity(backends.len());
        let mut workers = Vec::with_capacity(backends.len());
        for (idx, backend) in backends.into_iter().enumerate() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<(Vec<T>, usize)>(max_pending_batches);
            txs.push(tx);
            let qb = Arc::clone(&queued_bytes);
            let idle_w = Arc::clone(&idle);
            let error_cb_w = error_cb.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("async-persist-{idx}"))
                    .spawn(move || {
                        let mut backend = backend;
                        loop {
                            // 首批阻塞等待；通道关闭 = shutdown，排空后退出。
                            let (mut items, mut est) = match rx.recv() {
                                Ok(batch) => batch,
                                Err(_) => break,
                            };
                            // 攒批：合并后续立即可得的批次（减少后端事务次数），
                            // 但受 `max_batch_bytes` 上限约束——无上限会合并出
                            // 超大批拖死小页缓存后端（q18 实测单批 197s）。
                            while let Ok((more, more_est)) = rx.try_recv() {
                                items.extend(more);
                                est += more_est;
                                if est >= max_batch_bytes {
                                    break;
                                }
                            }
                            if let Err(e) = backend.write_batch(items) {
                                (error_cb_w.as_ref().map(|cb| cb(&e)).unwrap_or_else(|| {
                                    eprintln!("[async-persist] 写失败(丢弃该批): {e}")
                                }));
                            }
                            qb.fetch_sub(est, Ordering::SeqCst);
                            let (lock, cvar) = &*idle_w;
                            if qb.load(Ordering::SeqCst) == 0 {
                                *lock.lock().expect("async-persist idle lock") = true;
                            }
                            // 无条件 notify：字节预算等待方（submit 背压）依赖它
                            // 醒后重查。
                            cvar.notify_all();
                        }
                    })
                    .expect("spawn async-persist worker"),
            );
        }
        Self {
            txs,
            byte_budget,
            queued_bytes,
            idle,
            workers,
            _backend: PhantomData,
        }
    }

    /// 提交一批待落盘数据（热路径）。
    ///
    /// `est_bytes` = 本批估算字节（背压记账；worker 写清后扣减）。
    /// 队列满（批数/字节预算）时**阻塞等待**（背压——不丢、保序、调用方
    /// 不需要自行写盘；worker 消化慢时热路径退化为等待，即内存有界的代价）。
    /// 仅 [`PersistError::Closed`]（已 shutdown）为非阻塞错误。
    /// 提交一批待落盘数据（热路径）。
    ///
    /// `route` = 路由键（如目标文件 hash）——`route % worker_count` 决定
    /// 写哪个 worker。**同一 route 恒路由到同一 worker**（该存储目标单写者，
    /// redb 事务无并发冲突）；不同 route 并行写。
    ///
    /// `est_bytes` = 本批估算字节（背压记账；worker 写清后扣减）。
    /// 队列满（批数/字节预算）时**阻塞等待**（背压——不丢、保序、调用方
    /// 不需要自行写盘；worker 消化慢时热路径退化为等待，即内存有界的代价）。
    /// 仅 [`PersistError::Closed`]（已 shutdown）为非阻塞错误。
    pub fn submit_batch(
        &self,
        route: u64,
        items: Vec<T>,
        est_bytes: usize,
    ) -> Result<(), PersistError> {
        if items.is_empty() {
            return Ok(());
        }
        if self.txs.is_empty() {
            return Err(PersistError::Closed);
        }
        // 字节预算：队列已超预算时等待 worker 消化（轮询 Condvar）。
        let (lock, cvar) = &*self.idle;
        loop {
            let cur = self.queued_bytes.load(Ordering::SeqCst);
            if cur + est_bytes <= self.byte_budget {
                break;
            }
            let mut idle_guard = lock.lock().expect("async-persist budget lock");
            if *idle_guard {
                // 队列已空但字节没扣完（竞态）——直接放行。
                break;
            }
            let (guard, timed_out) = cvar
                .wait_timeout(idle_guard, watchdog_timeout())
                .expect("async-persist budget wait");
            idle_guard = guard;
            if timed_out.timed_out() {
                panic!(
                    "async-persist 背压等待超过 {:?}：worker 线程可能饿死或死锁（queued_bytes={}）",
                    watchdog_timeout(),
                    self.queued_bytes.load(Ordering::SeqCst)
                );
            }
        }
        // 路由到 worker（同一 route 恒同 worker）；通道满 → 阻塞（背压）。
        let idx = route as usize % self.txs.len();
        match self.txs[idx].send((items, est_bytes)) {
            Ok(()) => {
                let (lock, _) = &*self.idle;
                *lock.lock().expect("async-persist idle lock") = false;
                self.queued_bytes.fetch_add(est_bytes, Ordering::SeqCst);
                Ok(())
            }
            Err(_) => Err(PersistError::Closed),
        }
    }

    /// 等待队列排空（close/读回前调用）：所有已提交批次已被 worker 写清
    /// （含失败丢弃——失败已走 error_cb）。返回后"已提交 = 已可见"。
    pub fn flush(&self) -> Result<(), PersistError> {
        let (lock, cvar) = &*self.idle;
        let mut idle_guard = lock.lock().expect("async-persist flush lock");
        while !*idle_guard {
            let (guard, timed_out) = cvar
                .wait_timeout(idle_guard, watchdog_timeout())
                .expect("async-persist flush wait");
            idle_guard = guard;
            if timed_out.timed_out() {
                panic!(
                    "async-persist flush 等待超过 {:?}：worker 线程可能饿死或死锁（queued_bytes={}）",
                    watchdog_timeout(),
                    self.queued_bytes.load(Ordering::SeqCst)
                );
            }
        }
        Ok(())
    }

    /// 队列是否为空（无在途批次）。
    pub fn is_idle(&self) -> bool {
        *self.idle.0.lock().expect("async-persist idle lock")
    }

    /// 当前在途字节（诊断）。
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::SeqCst)
    }

    /// 停止并排空：关闭全部通道 → worker 消化剩余批次 → join。
    pub fn shutdown(mut self) {
        self.txs.clear(); // drop 所有 sender → 各 worker 排空后退出
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

impl<T, B> Drop for AsyncPersister<T, B> {
    fn drop(&mut self) {
        // 优雅停止（避免 Drop 时 worker 线程悬挂）。
        self.txs.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// 共享内存后端：测试持有 `written` 引用验证写入内容与顺序。
    #[derive(Clone)]
    struct SharedBackend {
        written: Arc<StdMutex<Vec<Vec<u64>>>>,
        fail_next: Arc<StdMutex<Option<String>>>,
    }

    impl SharedBackend {
        fn new() -> Self {
            Self {
                written: Arc::new(StdMutex::new(Vec::new())),
                fail_next: Arc::new(StdMutex::new(None)),
            }
        }
        fn all(&self) -> Vec<u64> {
            self.written
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .copied()
                .collect()
        }
    }

    impl BatchWriter<u64> for SharedBackend {
        fn write_batch(&mut self, items: Vec<u64>) -> Result<(), String> {
            if let Some(e) = self.fail_next.lock().unwrap().take() {
                return Err(e);
            }
            self.written.lock().unwrap().push(items);
            Ok(())
        }
    }

    fn est(n: usize) -> usize {
        n * 8
    }

    #[test]
    fn submit_flush_writes_all_in_order() {
        let backend = SharedBackend::new();
        let p = AsyncPersister::<u64, SharedBackend>::new(
            vec![backend.clone()],
            1 << 20,
            16,
            1 << 16,
            None,
        );
        for i in 0..100u64 {
            p.submit_batch(0, vec![i], est(1)).unwrap();
        }
        p.flush().unwrap();
        assert_eq!(backend.all(), (0..100).collect::<Vec<_>>(), "保序写出");
        assert!(p.is_idle());
        p.shutdown();
    }

    #[test]
    fn byte_budget_backpressure_blocks_not_drops() {
        let backend = SharedBackend::new();
        // 预算 16B = 2 项；连提 3 批（24B）——第 3 批阻塞等 worker 消化后入队，不丢。
        let p =
            AsyncPersister::<u64, SharedBackend>::new(vec![backend.clone()], 16, 4, 1 << 16, None);
        p.submit_batch(0, vec![1], est(1)).unwrap();
        p.submit_batch(0, vec![2], est(1)).unwrap();
        p.submit_batch(0, vec![3], est(1)).unwrap(); // 阻塞直到预算可容纳
        p.flush().unwrap();
        assert_eq!(backend.all(), vec![1, 2, 3], "字节背压阻塞而非丢弃");
        p.shutdown();
    }

    #[test]
    fn error_cb_invoked_on_write_failure() {
        let backend = SharedBackend::new();
        let errors = Arc::new(StdMutex::new(Vec::new()));
        let err_cb: Arc<dyn Fn(&str) + Send + Sync> = {
            let errors = Arc::clone(&errors);
            Arc::new(move |e: &str| errors.lock().unwrap().push(e.to_string()))
        };
        let p = AsyncPersister::<u64, SharedBackend>::new(
            vec![backend.clone()],
            1 << 20,
            8,
            1 << 16,
            Some(err_cb),
        );
        *backend.fail_next.lock().unwrap() = Some("磁盘满".to_string());
        p.submit_batch(0, vec![1], est(1)).unwrap();
        p.flush().unwrap();
        let errs = errors.lock().unwrap();
        assert_eq!(errs.len(), 1, "写失败回调一次");
        assert!(errs[0].contains("磁盘满"));
        // 失败批被丢弃，后续批次正常。
        p.submit_batch(0, vec![2], est(1)).unwrap();
        p.flush().unwrap();
        assert_eq!(backend.all(), vec![2]);
        p.shutdown();
    }

    #[test]
    fn shutdown_drains_remaining() {
        let backend = SharedBackend::new();
        let p = AsyncPersister::<u64, SharedBackend>::new(
            vec![backend.clone()],
            1 << 20,
            16,
            1 << 16,
            None,
        );
        for i in 0..50u64 {
            p.submit_batch(0, vec![i], est(1)).unwrap();
        }
        p.shutdown(); // 不先 flush——shutdown 应排空剩余
        assert_eq!(backend.all().len(), 50, "shutdown 排空全部");
    }

    #[test]
    fn multi_worker_route_keeps_same_route_in_order() {
        // 多 worker + 路由：同一 route（同一存储目标）恒由同一 worker 串行写
        // （保序），不同 route 并行写。用 4 worker × 2 route 验证。
        let backend0 = SharedBackend::new();
        let backend1 = SharedBackend::new();
        let backend2 = SharedBackend::new();
        let backend3 = SharedBackend::new();
        let backends = vec![
            backend0.clone(),
            backend1.clone(),
            backend2.clone(),
            backend3.clone(),
        ];
        let p = AsyncPersister::<u64, SharedBackend>::new(backends, 1 << 20, 16, 1 << 16, None);
        // route=1 → worker1（backend1）；route=3 → worker3（backend3）。
        for i in 0..30u64 {
            p.submit_batch(1, vec![i], est(1)).unwrap();
            p.submit_batch(3, vec![i], est(1)).unwrap();
        }
        p.flush().unwrap();
        assert_eq!(backend1.all(), (0..30).collect::<Vec<_>>(), "route=1 保序");
        assert_eq!(backend3.all(), (0..30).collect::<Vec<_>>(), "route=3 保序");
        assert!(backend0.all().is_empty(), "route 未命中 worker0");
        assert!(backend2.all().is_empty(), "route 未命中 worker2");
        p.shutdown();
    }

    /// write_batch 永久阻塞的后端（模拟后端 IO 挂起 / worker 被饿死的极端形态）。
    struct HangBackend;

    impl BatchWriter<u64> for HangBackend {
        fn write_batch(&mut self, _items: Vec<u64>) -> Result<(), String> {
            std::thread::park(); // 永不返回
            unreachable!()
        }
    }

    #[test]
    #[should_panic(expected = "flush 等待超过")]
    fn flush_watchdog_panics_when_worker_stalls() {
        // worker 卡在写 → idle 恒 false → flush 看门狗（测试态 200ms）触发 panic，
        // 不再让 runner 无限挂（"running for over 60 seconds" 场景）。
        let p =
            AsyncPersister::<u64, HangBackend>::new(vec![HangBackend], 1 << 20, 4, 1 << 16, None);
        p.submit_batch(0, vec![1], 8).unwrap();
        p.flush().unwrap();
    }

    #[test]
    #[should_panic(expected = "背压等待超过")]
    fn submit_backpressure_watchdog_panics_when_worker_stalls() {
        // budget = 8B：第一批占满后第二批进入背压等待；worker 卡写永不扣减
        // → submit 看门狗触发 panic。
        let p = AsyncPersister::<u64, HangBackend>::new(vec![HangBackend], 8, 4, 1 << 16, None);
        p.submit_batch(0, vec![1], 8).unwrap();
        p.submit_batch(0, vec![2], 8).unwrap(); // 背压等待 → 200ms 后 panic
    }
}
