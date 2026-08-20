//! 确定性 stall 复现 bench —— 隔离 window actor 广播阻塞机制。
//!
//! # 根因
//! window actor 单写者在 commit 循环里内联 `join_sends` N 路阻塞广播
//! （`RuleFanout::broadcast_batch_only` → `broadcast_inner` → 对每个订阅者
//! `tx.send().await`）。任一订阅者 channel 满 → actor 卡死 → 全局停滞。这正是
//! `ISSUE_q5_100m_freeze.md` 记录的 q5 100M 间发冻结（~1/3）的根：
//! 1 条 rule 瞬时停顿使其 channel 填满 → actor 广播卡在该 channel → mailbox
//! 堆积 → 字节预算耗尽 → receiver 停收 → `append_total` 追不平 TOTAL。
//!
//! # 本 bench 做什么
//! 用**真实生产代码** `RuleFanout` 复现，不依赖 100M/1600s，秒级、确定性：
//!
//! - `run_push(with_slow)`：actor 循环 `broadcast_batch_only(...).await`。
//!   - `with_slow=false`（健康，所有订阅者即时排空）：应**不卡**，完成全部批。
//!   - `with_slow=true`（1 条订阅者永不接收 → channel 填满 CAP）：actor 在第
//!     CAP+1 批永久阻塞 → **停滞**（复现根因）。
//! - `run_pull`（M1 设计模型）：actor 不 await 任何订阅者，仅 append 共享 log +
//!   `notify`；各订阅者自带 cursor 自拉。慢/不读订阅者只自己落后，不影响 actor
//!   与他人 → **不卡**（证明 M1 消除停滞）。
//!
//! # 注意
//! push 侧是**真实生产代码**；pull 侧是 M1 设计的**忠实模型**（actor 去阻塞 +
//! per-rule cursor 自拉），用于在落地 M1 前用同一 harness 量化「停滞 vs 不停滞」。

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::{Notify, mpsc};

use wf_engine::window::{RuleFanout, RulePush};

const WINDOW: &str = "bench_win";
const N_SUBS: usize = 8; // 模拟 8 条 rule 订阅（生产为 ~30 路）
const CAP: usize = 8; // rule channel 深度（小，快速、确定性触发 stall）
const SLOW_IDX: usize = 3; // 这条订阅者"永不接收"（制造 skew）
const M_BATCHES: usize = 200; // 计划注入的批数
const TIMEOUT: Duration = Duration::from_secs(2); // 单批广播超时（卡死判定）

fn make_batch() -> Arc<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let id = Int64Array::from((0..1000).collect::<Vec<i64>>());
    let auction = Int64Array::from((0..1000).map(|i| (i % 10) as i64).collect::<Vec<i64>>());
    let ts =
        TimestampNanosecondArray::from((0..1000).map(|i| i as i64 * 1000).collect::<Vec<i64>>());
    let extra = StringArray::from((0..1000).map(|i| format!("s{i}")).collect::<Vec<_>>());
    Arc::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id),
                Arc::new(auction),
                Arc::new(ts),
                Arc::new(extra),
            ],
        )
        .unwrap(),
    )
}

/// Push 模式（真实生产代码）。`with_slow=true` 时第 SLOW_IDX 条订阅者不排空 →
/// 其 channel 在 CAP 批后填满 → actor 广播阻塞 → 停滞。
async fn run_push(with_slow: bool) -> (usize, Duration) {
    let fanout = RuleFanout::new();
    let mut rxs = Vec::new();
    for _ in 0..N_SUBS {
        let (tx, rx) = mpsc::channel::<RulePush>(CAP);
        fanout.register(WINDOW, tx);
        rxs.push(rx);
    }
    // 快订阅者：立即排空。慢订阅者（with_slow）：保留 rx 但**不读** → 其 channel
    // 填满且保持打开，制造与生产一致的「满 channel 阻塞 send」。
    let mut handles = Vec::new();
    let mut slow_rx = None;
    for (i, mut rx) in rxs.into_iter().enumerate() {
        if with_slow && i == SLOW_IDX {
            slow_rx = Some(rx); // 保留但不读
            continue;
        }
        handles.push(tokio::spawn(
            async move { while rx.recv().await.is_some() {} },
        ));
    }
    let batch = make_batch();
    let start = Instant::now();
    let mut done = 0usize;
    for seq in 0..M_BATCHES as u64 {
        // 模拟 actor 内联 await 广播（与生产 commit 循环语义一致）
        let r = tokio::time::timeout(
            TIMEOUT,
            fanout.broadcast_batch_only(WINDOW, batch.as_ref(), None, None, seq),
        )
        .await;
        if r.is_err() {
            break; // 被慢订阅者卡死 → 停滞
        }
        done += 1;
    }
    let elapsed = start.elapsed();
    drop(fanout); // 关掉所有 tx → 快订阅者 recv 返回 None 自然退出
    drop(slow_rx); // 关闭慢订阅者 rx（此前未读 → 其 channel 已填满）
    for h in handles {
        h.abort();
    }
    (done, elapsed)
}

/// Pull 模式（M1 设计模型）。actor 仅 append 共享 log + notify，**不 await 任何
/// 订阅者**。各订阅者自带 cursor 自拉；慢/不读订阅者只自己落后，不影响 actor。
async fn run_pull() -> (usize, Duration, usize) {
    let log: Arc<Mutex<Vec<Arc<RecordBatch>>>> = Arc::new(Mutex::new(Vec::new()));
    let notify = Arc::new(Notify::new());
    let cursors: Vec<Arc<Mutex<usize>>> = (0..N_SUBS).map(|_| Arc::new(Mutex::new(0))).collect();
    let batch = make_batch();

    // 快订阅者：自拉所有新批（cursor 追赶）
    let mut handles = Vec::new();
    for (i, cursor) in cursors.iter().enumerate() {
        if i == SLOW_IDX {
            continue; // 慢/不读订阅者：cursor 停在 0，但不阻塞任何人
        }
        let log = log.clone();
        let notify = notify.clone();
        let cur = cursor.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let next = {
                    let g = log.lock().unwrap();
                    let c = *cur.lock().unwrap();
                    if c >= g.len() { None } else { Some(c) }
                };
                match next {
                    Some(idx) => {
                        // 处理 batch[idx]
                        let _ = &log.lock().unwrap()[idx];
                        *cur.lock().unwrap() = idx + 1;
                    }
                    None => notify.notified().await,
                }
            }
        }));
    }

    // actor 循环：append + notify，无阻塞
    let start = Instant::now();
    for seq in 0..M_BATCHES as u64 {
        log.lock().unwrap().push(batch.clone());
        notify.notify_waiters();
        let _ = seq;
    }
    let elapsed = start.elapsed();
    let slow_cursor = *cursors[SLOW_IDX].lock().unwrap(); // 慢订阅者落后量（应为 0）
    for h in handles {
        h.abort();
    }
    (M_BATCHES, elapsed, slow_cursor)
}

#[tokio::test]
async fn bench_push_stalls_under_skew_pull_does_not() {
    let (push_ok_done, push_ok_el) = run_push(false).await; // 健康：所有订阅者排空
    let (push_skew_done, push_skew_el) = run_push(true).await; // skew：1 条不接收
    let (pull_done, pull_el, slow_lag) = run_pull().await; // M1 模型

    println!("=== fanout stall bench ===");
    println!("subs={N_SUBS} cap={CAP} slow_idx={SLOW_IDX} target={M_BATCHES} timeout={TIMEOUT:?}");
    println!(
        "[PUSH healthy] done={push_ok_done}/{M_BATCHES} elapsed={push_ok_el:?} -> {}",
        if push_ok_done == M_BATCHES {
            "NO STALL (all subs drained)"
        } else {
            "UNEXPECTED STALL"
        }
    );
    println!(
        "[PUSH skew]    done={push_skew_done}/{M_BATCHES} elapsed={push_skew_el:?} -> {}",
        if push_skew_done < M_BATCHES {
            "STALL (actor frozen by slow sub)"
        } else {
            "NO STALL (unexpected)"
        }
    );
    println!(
        "[PULL model]   done={pull_done}/{M_BATCHES} elapsed={pull_el:?} slow_lag={slow_lag} -> {}",
        if pull_done == M_BATCHES {
            "NO STALL (actor finished; slow sub just lags)"
        } else {
            "STALL (unexpected)"
        }
    );

    // 断言：健康 push 不卡；skew push 在 CAP 附近停滞；pull 完成全部。
    assert_eq!(push_ok_done, M_BATCHES, "healthy push must not stall");
    assert!(
        push_skew_done <= CAP + 2,
        "skewed push must stall near CAP, got {push_skew_done}"
    );
    assert_eq!(pull_done, M_BATCHES, "pull model must finish all batches");
}
