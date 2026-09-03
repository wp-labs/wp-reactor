//! Q9 形状 deferred join（`emit at`）评估触发语义：auction 驱动流挂起实例
//! 在事件时间 watermark 过 expiry 时输出胜者。覆盖: 延迟行与时间驱逐 × D4 保留
//! pin、pin floor 随 pending drain 推进、flush 在 frontier 不前进时解除挂起、
//! 跨源乱序/target lag 延迟评估、按 expiry 序输出、无 bid 静默、flush 不提前
//! 输出未到期尾部、EOS retry 恢复到期实例。

use super::*;

/// 时间驱逐 × D4 保留 pin 闭环（30M q4 over=30m 欠发的机制验证）：
///
/// auction 时长 > bid 窗 `over` 时，到期评估需要的右行早已越过时间驱逐线
/// （生产：`bid_events over=30m` + 驱逐 tick）。deferred 规则发布保留 pin
/// （= 存活挂起实例的 min(lo_ns)），时间驱逐与内存驱逐都不得删 `[lo, expiry]`
/// 内的行（2026-08-25 D4 闭环：`evict_expired_impl` 尊重 pin）。
///
/// 本用例验证**正路径**：pin 保住越过驱逐线的右行 → 评估命中输出。
/// （无 pin 侧由 wf-engine `evict_expired_respects_retention_pin` 覆盖。）
#[tokio::test]
async fn deferred_q9_time_eviction_pin_keeps_in_range_bids() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) =
        make_deferred_join_task_with_over(std::time::Duration::from_secs(10));

    // bid 先到（auction=5，price 100 @ T+5s）——落在 auction 5 的 [lo=T, expiry=T+30s] 内
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(5, 1, 100, T + 5_000_000_000)]))
        .unwrap();
    // auction=5：时长 30s > over 10s → 到期评估时右行已越过驱逐线
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 30_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // 挂起中（expiry=T+30s，watermark=T）；pin 已发布（lo_min=T）

    // 事件时间推进到 T+25s：cutoff = T+15s > bid @ T+5s → 时间驱逐线已覆盖右行。
    // pin（挂起实例 lo=T）必须挡住驱逐：batch(max=T+5s) ≥ pin(=T) → 保留。
    bid_window(&router).evict_expired(T + 25_000_000_000);
    assert_eq!(
        bid_window(&router).total_rows(),
        1,
        "pin 必须保住挂起实例需要的右行（时间驱逐不得删）"
    );

    // 驱动 watermark + 目标窗都追平 expiry：auction=6 @ T+31s、bid=6 @ T+31s
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 31_000_000_000,
            T + 61_000_000_000,
        )]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 3, 300, T + 31_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // auction 5 到期评估：右行在 → 命中输出
    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "pin 保住右行 → 到期评估命中"
    );
}

/// 内存机制回归：**lo_min 缓存必须随 pending drain 推进**（2026-08-25 修复）。
///
/// 旧实现把 lo_min 缓存为**历史最小** lo（插入时 min，drain 不更新）——任何
/// shard 只要 pending 非空，pin 就发布历史第一个实例的 lo（≈流起点）→ 时间
/// 驱逐全被挡（30M q4 over=30m：pin_floor=起点+1ms、evict=0、RSS 9.2GB = 整窗
/// 保留，探针实锤）。修复：scan drain 到期前缀后标 dirty，publish 重算当前
/// pending 的 min lo → pin 随评估前沿推进 → over 窗口外的旧行可驱逐。
#[tokio::test]
async fn deferred_q9_pin_floor_advances_with_pending_drain() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) =
        make_deferred_join_task_with_over(std::time::Duration::from_secs(10));

    // 三个短时长实例：1/2/3 号 auction，expiry = lo + 1s（随事件流推进评估）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 1_000_000_000)]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(5, 1, 100, T + 500_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    auction_window(&router)
        .append_with_watermark(auction_batch(&[(6, T + 2_000_000_000, T + 3_000_000_000)]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 3, 300, T + 2_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5"
    );

    auction_window(&router)
        .append_with_watermark(auction_batch(&[(7, T + 4_000_000_000, T + 5_000_000_000)]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(7, 7, 7, T + 4_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "6"
    );

    // 三个实例都已评估（pending 只剩 7 号，min lo = T+4s）。
    // 修复前：lo_min 缓存 = 历史最小 = T（1 号实例的 lo）→ pin = T。
    // 修复后：drain 标 dirty → publish 重算 → pin = T+4s。
    let pin_floor = bid_window(&router).retention_floor_ns();
    assert!(
        pin_floor >= T + 4_000_000_000,
        "pin 必须随 pending drain 推进（当前 pending min lo = T+4s），实际 pin_floor={pin_floor}"
    );

    // 时间驱逐：now=T+20s、over=10s → cutoff=T+10s。三条 bid 都在 cutoff 前。
    // 修复前 pin=T → 全被 pin 住 → 不驱逐（BUG：整窗保留）。
    // 修复后 pin=T+4s → T+0.5s / T+2s 两条 < pin → 驱逐；T+4s 那条 = pin
    //（auction 7 的 lo，挂起实例区间起点）→ 合法保留（正确性）。
    bid_window(&router).evict_expired(T + 20_000_000_000);
    assert_eq!(
        bid_window(&router).total_rows(),
        1,
        "pin 推进后 over 窗口外的旧右行必须可驱逐（仅剩当前挂起实例区间内的行）"
    );
}

/// flush 收口**不受健全前沿限制**：目标窗一直不提交（frontier 卡 i64::MIN）时，
/// 运行期 gate 挂起全部实例（不假 miss），flush（gate=false）仍按最终水位收口
/// 评估——否则尾部/静态目标场景会全部丢到 flush 之外。
#[tokio::test]
async fn deferred_q9_flush_unblocks_evaluation_when_frontier_never_advances() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();
    let src_a: Arc<str> = Arc::from("ingress#1");
    let bw = bid_window(&router);
    let _schema = bw.schema().clone();

    // auction 5 挂起（expiry=T+30s）；目标窗**没有任何提交**（per-source 空 →
    // frontier 回退 max_event_time = i64::MIN → 运行期 gate 挂起）。
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 30_000_000_000)]))
        .unwrap();
    // 驱动 wm 追平 expiry（auction 6 @ T+31s）——即使驱动已过 expiry，
    // frontier=i64::MIN → gate=i64::MIN → 不评估（不假 miss）。
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 31_000_000_000,
            T + 61_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "目标无提交 → 运行期保持挂起（不假 miss）"
    );

    // 目标窗随后提交右行（跨源延迟送达）
    bw.append_with_watermark_sized_from(
        bid_batch(&[(5, 1, 100, T + 5_000_000_000)]),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();

    // flush 收口：gate=false → 不受 frontier 限制 → 评估命中补输出
    task.flush().await;
    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "flush 收口必须绕过 frontier gate（右行已提交 → 命中）"
    );
}

/// 跨源提交乱序 × deferred 评估 gate（30M q4 over=30m -860 的机制回归）：
///
/// ingress `instances=8` + parse 并行派发下，窗口 actor 只保证 **source 内**
/// seq 有序，跨 source 提交顺序自由——全局 `max_event_time` 会被任一 source 的
/// 远未来 batch 提前推高。修复前 gate 用它 → 右行未落地就评估 → 假 miss →
/// 行随后被 over 时间驱逐 → flush 重试无法恢复（-860）。修复后 gate 用
/// `min(驱动 wm, 健全提交前沿 = 各源已提交 max 的 min)` → 右行真正落地才评估。
#[tokio::test]
async fn deferred_q9_cross_source_reorder_holds_evaluation_until_committed() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();
    let src_a: Arc<str> = Arc::from("ingress#1");
    let src_b: Arc<str> = Arc::from("ingress#2");
    let bw = bid_window(&router);
    let _schema = bw.schema().clone();

    // auction 5 挂起（lo=T, expiry=T+30s）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 30_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 跨源乱序：source A 先提交远未来 bid（全局 max → T+100s），
    // source B 只提交到 T+2s（无关 auction 98）→ 健全前沿 = T+2s。
    bw.append_with_watermark_sized_from(
        bid_batch(&[(99, 9, 9, T + 100_000_000_000)]),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();
    bw.append_with_watermark_sized_from(
        bid_batch(&[(98, 8, 8, T + 2_000_000_000)]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();

    // 驱动 wm 追平 expiry（auction 6 @ T+31s）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 31_000_000_000,
            T + 61_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "修复前：eff_wm=全局 max=T+100s ≥ expiry → 提前评估假 miss；\
         修复后：eff_wm=min(驱动 T+31s, 前沿 T+2s)=T+2s < expiry → 保持挂起"
    );

    // source B 提交 auction 5 的右行（@T+5s）→ 前沿 = T+5s < expiry → 仍挂起
    bw.append_with_watermark_sized_from(
        bid_batch(&[(5, 1, 100, T + 5_000_000_000)]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "前沿（T+5s）未过 expiry（T+30s）前保持挂起"
    );

    // source B 提交到 T+40s → 前沿 = T+40s ≥ expiry；auction 7 @ T+45s 触发
    // 下一次扫描：eff_wm = min(驱动 T+45s, 前沿 T+40s) = T+40s ≥ T+30s →
    // 评估命中（右行已提交）
    bw.append_with_watermark_sized_from(
        bid_batch(&[(97, 7, 7, T + 40_000_000_000)]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            7,
            T + 45_000_000_000,
            T + 75_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "右行提交后评估命中"
    );
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "winner_bidder"),
        "1",
        "maxrow(price) 胜者 = bidder 1"
    );
}

#[tokio::test]
async fn deferred_q9_hit_outputs_winner_when_watermark_passes_expiry() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // bid 先到（auction=5，price 100/200，dateTime T+10s / T+20s）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[
            (5, 1, 100, T + 10_000_000_000),
            (5, 2, 200, T + 20_000_000_000),
        ]))
        .unwrap();
    // auction 到达：挂起（expiry = T+60s），watermark = T
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // 未到期 → 无输出
    assert!(alert_rx.try_recv().is_err(), "not due yet — no output");

    // 第二个 auction（ts=T+61s）推进 watermark ≥ expiry → 第一个到期输出胜者
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    // join 目标窗口同步追平（2026-08-25 评估 gate：目标 max_event_time ≥ expiry
    // 才评估——生产流中 bid/auction 交错 append，目标天然追平；单测需显式补）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 3, 300, T + 61_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_origin"),
        "deferred",
        "deferred join output must carry origin=deferred"
    );
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "winner for auction 5"
    );
    // 胜者 = price 200 的 bid（bidder=2）
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "winner_bidder"),
        "2",
        "maxrow(price) must pick the highest bid"
    );
}

#[tokio::test]
async fn deferred_q9_no_bid_no_output() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction=7 无任何 bid
    auction_window(&router)
        .append(auction_batch(&[(7, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 推进 watermark 超过 expiry（flush 到期）
    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "no bid in [dateTime, expires] → no deferred output"
    );
}

/// 乱序驱动（2026-08-25 q4 100M 回归）：auction 到达顺序与 expires 顺序
/// **相反**（二分插入保持 pending 按 expiry 有序）——到期扫描只取前缀，
/// 输出顺序仍按到期时间正确。回归防网：pending 改有序前缀前，全量扫不
/// 依赖顺序（等价）；改后若插入有序性被破坏会漏输出。
#[tokio::test]
async fn deferred_q9_out_of_order_driver_emits_by_expiry_order() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // 3 个 auction 乱序到达（expires 顺序：auction 11 最先到期）：
    //   auction=11: dateTime=T,     expires=T+30s
    //   auction=13: dateTime=T+2s,  expires=T+90s
    //   auction=12: dateTime=T+1s,  expires=T+60s
    // 每个 auction 各一个 bid（在各自区间内），保证到期评估命中。
    bid_window(&router)
        .append_with_watermark(bid_batch(&[
            (11, 1, 100, T + 5_000_000_000),
            (12, 2, 200, T + 10_000_000_000),
            (13, 3, 300, T + 15_000_000_000),
        ]))
        .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (11, T, T + 30_000_000_000),
            (13, T + 2_000_000_000, T + 90_000_000_000),
            (12, T + 1_000_000_000, T + 60_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "全部未到期，无输出");

    // 推进 watermark 到 T+31s：只有 auction 11 到期 → 输出 1 条（id=11）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            14,
            T + 31_000_000_000,
            T + 91_000_000_000,
        )]))
        .unwrap();
    // 目标窗口追平（bid 14 随 auction 14 到达，max_event_time 推过 T+30s）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(14, 4, 400, T + 31_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    task.flush().await;
    let a1 = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&a1, "__wfu_entity_id"),
        "11",
        "最先到期（T+30s）的 auction 先输出"
    );

    // 推进到 T+61s：auction 12 到期（T+60s），auction 13 未到期（T+90s）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            15,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    // 目标窗口追平（bid 15 随 auction 15 到达，max_event_time 推过 T+60s）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(15, 5, 500, T + 61_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    task.flush().await;
    let a2 = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&a2, "__wfu_entity_id"),
        "12",
        "第二个到期（T+60s）的 auction 输出"
    );
    // 此时已无其它到期（auction 13 expires=T+90s > T+61s）
    assert!(alert_rx.try_recv().is_err(), "auction 13 未到期，不应输出");
}

/// EOS/关闭 flush 只收口**已到期**实例：尾部 expiry > 最终事件时间 watermark 的
/// 实例窗口未完成（事件时间域），不输出——与 oracle 一致（oracle/mod.rs EOS
/// 水位注释：按 slice 边界强扫会多出尾部桶，Q8 实证 82446 → 83274 +828）。
#[tokio::test]
async fn deferred_q9_flush_does_not_emit_unexpired_tail() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction=8（T，expires=T+60s）窗口内有 bid，但无后续事件推进 watermark
    // → 最终 watermark = T < expiry：尾部未到期，flush 不输出
    bid_window(&router)
        .append(bid_batch(&[(8, 3, 50, T + 10_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append(auction_batch(&[(8, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "尾部未到期实例（expiry > 最终 watermark）flush 不输出"
    );
}

/// 已到期实例（expiry ≤ 最终 watermark）由 EOS 重试补出：窗口内 bid 存在、
/// 但评估发生在 watermark 过 expiry 之后（missed → EOS 重试命中）。
#[tokio::test]
async fn deferred_q9_eos_retry_recovers_due_instance() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction=8 挂起（T，expires=T+60s）；此时 bid 窗口为空 → 到期评估 miss
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(8, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // auction=9（T+61s）推进 watermark 过 expiry → auction 8 到期，bid 为空 → miss
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            9,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "bid 窗口为空 → 到期 miss，等 EOS 重试"
    );

    // bid 迟到进入右窗（append 滞后）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(8, 3, 50, T + 10_000_000_000)]))
        .unwrap();
    task.flush().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "8"
    );
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "winner_bidder"),
        "3"
    );
}

/// 2026-08-25 q4 100M 欠发根治：运行期评估 gate——join 目标窗口 append 位置未
/// 过 expiry 时实例**保持挂起**（不评估、不 miss），目标追平后随下一次扫描命中
/// 输出（无需 flush/EOS 重试）。修复前目标未追平就评估 → 运行期 miss → missed
/// 积压（RSS 随总量增长）+ 100M 下 EOS 重试时早段右行已被 over 驱逐 → 欠发
/// ~63%（oracle 5.58M vs 2.07M）。
#[tokio::test]
async fn deferred_q9_target_lag_holds_evaluation_until_target_catches_up() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction 5（T，expires=T+60s）挂起；bid 5 在窗内（T+10s）；auction 6
    // （T+61s）推 watermark 过 expiry——但 bid 窗口 max_event_time 还停在
    // T+10s（目标 append 滞后）→ 评估 gate 把实例保持挂起（旧行为：立即
    // 评估 → 窗口缺后续 bid → miss 进 missed）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(5, 1, 100, T + 10_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "目标窗口未追平（max_event_time=T+10s < expiry T+60s）→ 实例保持挂起，不评估"
    );

    // 目标窗口追平：bid 6 随 auction 6 到达 → bid 窗口 max_event_time 推过
    // T+60s；auction 7 随后的驱动事件触发批次尾扫描 → auction 5 命中输出
    // （无需 flush/EOS 重试）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 2, 200, T + 61_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            7,
            T + 62_000_000_000,
            T + 122_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5"
    );

    // 运行期已命中（missed 为空）→ flush 收口不重复输出
    task.flush().await;
    assert!(
        drain_alert_entity_ids(&mut alert_rx).is_empty(),
        "运行期已命中，flush 不重复输出"
    );
}
