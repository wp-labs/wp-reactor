use super::*;

fn pending(lo_ns: i64) -> DeferredPending {
    DeferredPending {
        key_field: "auction".into(),
        key: wf_engine::match_engine::Value::Number(1.0),
        lo_ns,
        hi_ns: lo_ns + 1_000_000_000,
        lo_open: false,
        hi_open: false,
        expiry_nanos: lo_ns + 1_000_000_000,
        left: wf_engine::match_engine::DeferredLeft::Event(Event {
            fields: Default::default(),
        }),
    }
}

fn runtime(pin: &Arc<AtomicI64>) -> DeferredRuntime {
    DeferredRuntime {
        pending: Vec::new(),
        missed: Vec::new(),
        watermark: i64::MIN,
        join_idx: 0,
        retention_pin: Some(Arc::clone(pin)),
        lo_min: i64::MAX,
        lo_min_dirty: false,
    }
}

/// 未见过驱动事件时必须发布 `i64::MIN`（全保留），**不能**发布 `i64::MAX`。
///
/// 回归防网：曾把“watermark 未初始化”映射成“无所需”，启动时的定时扫描（1s
/// 间隔）先于首批驱动事件触发，把刚预注册的 pin 立即释放 → q4 30M 丢 0.67%
/// 输出（2026-08-24）。
#[test]
fn uninitialized_watermark_publishes_fully_pinned() {
    let pin = Arc::new(AtomicI64::new(i64::MIN));
    let mut rt = runtime(&pin);
    rt.publish_retention_floor();
    assert_eq!(
        pin.load(Ordering::Acquire),
        i64::MIN,
        "还没见过驱动事件 → 不知道自己的前沿 → 必须全保留"
    );
}

/// 无挂起且 watermark 已推进 → 前沿 = watermark（更旧的行未来实例也用不到）。
#[test]
fn empty_pending_publishes_watermark() {
    let pin = Arc::new(AtomicI64::new(i64::MIN));
    let mut rt = runtime(&pin);
    rt.watermark = 5_000;
    rt.publish_retention_floor();
    assert_eq!(pin.load(Ordering::Acquire), 5_000);
}

/// 前沿 = 仅 `pending`（未评估）的 `min(lo_ns)`——`missed` 不再参与。
///
/// 2026-08-25（评估 gate 落地后）：运行期评估时目标窗已追平（target_wm ≥
/// expiry）→ 运行期 miss 即真 miss（右行确实不在区间内），EOS 重试只做确认、
/// 不需要保留右行；missed 的 lo 分布全流，参与 pin 会把时间驱逐拖死。
/// 曾将 missed 计入（目标 append 滞后时代的假 miss 保护），已随 gate 退役。
#[test]
fn floor_covers_pending_only_ignores_missed() {
    let pin = Arc::new(AtomicI64::new(i64::MIN));
    let mut rt = runtime(&pin);
    rt.watermark = 9_000;
    rt.pending = vec![pending(7_000), pending(8_000)];
    rt.missed = vec![pending(3_000)];
    rt.publish_retention_floor();
    assert_eq!(
        pin.load(Ordering::Acquire),
        7_000,
        "missed 的 lo_ns（3_000）不参与前沿——EOS 重试只做确认，不需要保留右行"
    );

    // missed 清空不影响缓存（lo_min 只由 pending 维护）。
    rt.missed.clear();
    rt.publish_retention_floor();
    assert_eq!(pin.load(Ordering::Acquire), 7_000);
}

/// EOS 释放 → `i64::MAX`（窗口恢复完全可驱逐）。
#[test]
fn release_unpins_the_window() {
    let pin = Arc::new(AtomicI64::new(1_234));
    let rt = runtime(&pin);
    rt.release_retention_floor();
    assert_eq!(pin.load(Ordering::Acquire), i64::MAX);
}
