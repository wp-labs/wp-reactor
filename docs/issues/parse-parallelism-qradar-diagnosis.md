# parse_parallelism qradar 诊断数据快照（2026-08-31）

> 性能数据引用纪律：本文是 **2026-08-31 诊断结论的数字快照**，供
> `docs/design/decode-route-merge-design.md` 前情引用回溯。
> 原始 bench 输出未归档；复测前请勿在其他对外材料中扩大引用范围。

## 1. 受控 A/B（qradar 1M，2026-08-31）

| 配置 | EPS | 备注 |
|---|---|---|
| `parse_parallelism = 1` | 75k | 现状默认（P0 已改默认 1） |
| `parse_parallelism = 2` | 50k | **-33%**，多轮受控 A/B 复现 |

归因：共享 `Arc<tokio::sync::Mutex<Receiver>>` 使接收严格串行
（parse_pool.rs 测试 `mutex_receiver_recv_serialized_processing_parallel` 自证），
多 worker 只增加调度与乱序成本 → actor reorder park。

## 2. 负载画像（同一诊断）

- 规则任务 376 个；规则求值实测仅占 ~1 核，其余核被"机制开销"（通道泵送、调度、锁）喂满
  → 移除中间层（parse 池）= 直接去机制开销。
- qradar 探针 `parse_parallelism` 10→16 无增益。

## 3. 复测入口

- qradar_pk：`wf-examples/performance/qradar_pk/run.sh`（#18 门禁：eviction=0 + emitted 阈值）
- A/B 纪律沿用 nexmark 口径：不限速、同时段交错、按 RSS 相位配对、单轮 ±8% 噪声。

## 4. 结论去向

结论已由 `decode-route-merge-design.md` 采纳：P1 将 route 内联进源任务、
整体移除 parse 池层（`parse_parallelism` 随 P2 废弃）。
