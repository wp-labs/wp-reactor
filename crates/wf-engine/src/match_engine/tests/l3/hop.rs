use super::*;

// ===========================================================================
// HOP (size, slide): one event fans out to every covering window
// ===========================================================================
//
// 已知 v1 局限：hop + OR-mode `on event`（无 close 块，事件级输出）时，同一
// 事件的多个覆盖窗口各自 fire，但 `advance_window` 扇出后 `merge_step_outcome`
// 只返回一个 Matched——每事件仅输出一个窗口的命中（Q5/Q7 均为 `and close`
// 形态：on-event 只标记 event_ok，输出发生在窗口收口扫描，逐窗口完整，不受
// 影响）。如需 OR-mode 多窗口输出需引擎 API 返回多 Matched，v1 未支持。

#[test]
fn hop_event_fans_out_to_all_covering_windows() {
    // hop(10s, 2s): window k = [k*2s, k*2s+10s). t=5s covers k ∈ {-2..=2} (5 windows).
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    sm.advance_at("fail", &e, 5_000_000_000);
    // k_min = (5-10)/2 + 1 = -3+1 = -2; k_max = 5/2 = 2 → 5 instances.
    assert_eq!(sm.instance_count(), 5);
}

#[test]
fn hop_same_slide_bucket_shares_windows() {
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // t=5s: k=-2..=2; t=7s: k=-1..=3。并集 k=-2..=3 = 6 个窗口（各有一个独有窗口）。
    sm.advance_at("fail", &e, 5_000_000_000);
    sm.advance_at("fail", &e, 7_000_000_000);
    assert_eq!(sm.instance_count(), 6);
}

#[test]
fn hop_upper_bound_is_exclusive() {
    // 窗口上界开区间：t 恰为 w+size 时属于下一个窗口（对齐 Flink HOP）。
    // t=0 → k=-4..0（含 [-8,2)）；t=2.0s 恰为 [-8,2) 上界 → 不含 k=-4，
    // 新窗口 k=-3..1。并集 k=-4..1 = 6。
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, 0);
    assert_eq!(sm.instance_count(), 5);
    sm.advance_at("fail", &e, 2_000_000_000);
    // k=-4（[-8,2)）不覆盖 t=2.0（开区间）；新窗口 k=1（[2,12)）加入。
    assert_eq!(sm.instance_count(), 6);
    // 过期验证：k=-4 在 t=2.0 到期（w_start+size = -8+10 = 2s）。
    let expired = sm.scan_expired_at(2_000_000_000);
    assert_eq!(expired.len(), 1);
}

#[test]
fn hop_square_window_equals_fixed() {
    // hop(size, size)：size/slide = 1 → 每事件恰 1 个窗口，窗口 = epoch 对齐
    // fixed size 桶（与 fixed 语义逐位一致）。
    let hop = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(10),
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop_sq".to_string(), hop, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, 5_000_000_000); // bucket [0, 10)
    sm.advance_at("fail", &e, 15_000_000_000); // bucket [10, 20)
    assert_eq!(
        sm.instance_count(),
        2,
        "hop(10,10) 每事件 1 窗口，等价 fixed"
    );
    let expired = sm.scan_expired_at(10_000_000_000);
    assert_eq!(expired.len(), 1);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn hop_close_removes_oldest_bucket() {
    // close(scope) 复用 fixed 分支：关同一 scope 最旧的窗口实例。
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, 5_000_000_000); // k=-2..=2（created -4s..4s）
    assert_eq!(sm.instance_count(), 5);
    let closed = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(closed.is_some(), "最旧窗口 k=-2（created=-4s）被关闭");
    assert_eq!(sm.instance_count(), 4);
}

#[test]
fn hop_window_expires_at_size_boundaries() {
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(5.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    sm.advance_at("fail", &e, 5_000_000_000);
    // Windows k=-2..=2 end at 6s, 8s, 10s, 12s, 14s.
    // Watermark 5s: nothing expired.
    assert!(sm.scan_expired_at(5_000_000_000).is_empty());
    assert_eq!(sm.instance_count(), 5);
    // Watermark 6s: window [-4s, 6s) expired.
    assert_eq!(sm.scan_expired_at(6_000_000_000).len(), 1);
    assert_eq!(sm.instance_count(), 4);
    // Watermark 14s: all five expired.
    let rest = sm.scan_expired_at(14_000_000_000);
    assert_eq!(rest.len(), 4);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn hop_oracle_style_boundary_scans() {
    // 复现 oracle 的扫描模式：t=0 首扫（无实例）→ 2s/4s slide 边界扫 → eos 扫。
    // 期望：2s 扫关 k=-4（expire 2.0），4s 扫关 k=-3（expire 4.0），eos 不再有关闭。
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, 0);
    sm.advance_at("fail", &e, 2_000_000_000);
    sm.advance_at("fail", &e, 4_000_000_000);
    let c1 = sm.scan_expired_at(0);
    let c2 = sm.scan_expired_at(2_000_000_000);
    let c3 = sm.scan_expired_at(4_000_000_000);
    let c4 = sm.scan_expired_at(4_999_900_000);
    eprintln!(
        "closes: 0s={} 2s={} 4s={} eos={}",
        c1.len(),
        c2.len(),
        c3.len(),
        c4.len()
    );
    assert_eq!(
        c1.len() + c2.len() + c3.len() + c4.len(),
        2,
        "k=-4(2s) + k=-3(4s) 共 2 个关闭"
    );
}

#[test]
fn hop_on_event_fires_in_every_covering_window() {
    // Q5 形状：每窗口 count >= 3。3 个事件（t=1s）同属 5 个窗口 → 每窗口计数 3 → 全部 fire。
    let plan = hop_plan(
        vec![simple_key("sip")],
        Duration::from_secs(10),
        Duration::from_secs(2),
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("r_hop".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert_eq!(
        sm.advance_at("fail", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("fail", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    // 第 3 个事件：所有 5 个窗口计数达标 → Matched（合并结果取最高优先级）。
    assert!(matches!(
        sm.advance_at("fail", &e, 1_000_000_000),
        StepResult::Matched(_)
    ));
}
