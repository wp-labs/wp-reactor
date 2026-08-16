# Match 引擎过期语义：pending_expiry 去重与 fire/reset 生命周期

> **状态：Implemented（语义冻结——正确性验证的基准）**
>
> 2026-08-16 · **代码**：`wf-engine/src/match_engine/match_engine/mod.rs`
> （`push_expiry_candidate` / `scan_expired_at` / `advance_at_with_diagnostics`）
> · 验证工具：`wf-examples/performance/nexmark_pk/scripts/verify_ground_truth.py`
> （确定性模拟器）+ `q5_diff_v2.py`（逐 alert 对拍）
>
> ⚠️ **本文档描述的是「引擎实际行为」而非「理想语义」**。该行为已被 30M
> ground truth 验证采纳为基准——修改过期/去重逻辑前必读 §4，否则会引入
> 正确性回归（见 9.7 反例：少算 3.4 万条）。

---

## 1. 适用范围

`match<key:D>` 滑动/固定窗口实例的过期与 fire 生命周期。Session 窗口
（last_event + d）不在本文范围。

## 2. pending_expiry：每 key 单条目去重

### 2.1 规则

`push_expiry_candidate` 只在以下三种情况推送过期候选：

1. **is_new** —— 实例首次创建；
2. **reset** —— fire 后重置；
3. **seq_broken** —— 乱序恢复。

**去重**：每 key 在 heap 中只保留**一个**条目（`pending_expiry` 集合）。若该 key
已有未弹出的条目，后续 push **被丢弃**。

### 2.2 后果：过期由「旧条目」驱动

fire/reset 后的 push 被去重丢弃 → 实例的实际过期时间由**上次 push 时的
`created + SPAN`**（一个旧条目）驱动，**不是**当前 created_at + SPAN。

乱序流中（fire 事件时间可能远晚于实例创建时间），这个旧条目晚于语义上的过期
时刻 → **实例系统性多活 → fire 比朴素 created_at 追踪多 ~2%**（nexmark q5
30M 实测：1,712,532 vs 朴素 1,678,523）。

### 2.3 pop 时的自愈：re-read + requeue

`scan_expired_at`（每个事件处理前调用，watermark = 当前事件时间）弹出条目后：

```
pop (expire_at, key)
  re-read 实例当前 created → current_expire = created + SPAN
  if current_expire <= watermark:  实例真正过期，移除
  else:                            requeue (current_expire, key)
```

即旧条目被弹出时**以实例当前状态修正**并重新入队——去重只造成「过期延迟」，
不会造成「永不过期」（heap 中始终有该 key 的一个条目，直到真正过期）。

## 3. fire/reset 生命周期（plain `on event`，非 accu）

fire 后 `instance.reset(plan, fire_time)`：

- **实例保留**（不从表中删除）；
- `created_at = fire 事件时间`（不是下一条事件的时间！）；
- 聚合状态（count/max 等）清零；
- 非 accu 规则不 rearm；
- 聚合求值**含当前事件**（先 bind 再求值再 fire）。

## 4. 修改前必读：验证基准依赖此语义

nexmark 30M 正确性验证（2026-08-16）以本文语义为基准：

- 8/9 规则引擎输出与确定性 ground truth 精确一致；
- q5_bidcount_10：引擎 1,712,470 vs 模拟 1,712,532（差 62 条 = 0.0036%，
  来源于 `scan_timeouts` 的墙钟推进非确定性）；
- 28k 探针逐 alert 对拍 2,679/2,679 全量精确吻合（含 fire 时刻）。

**反例存档**：模拟器若在 fire/reset 时无条件 push 新条目（而非镜像去重），
会**少算 3.4 万条**（0.3%+）。任何「顺手修复」这个看似奇怪的过期延迟的行为，
都会改变输出计数——如需变更，必须同步更新 verify_ground_truth.py 并全量重验。

## 5. scan_timeouts：墙钟推进（非确定性来源）

**代码**：`wf-runtime/src/engine_task/rule_task.rs`

周期 tick 用 `watermark + 墙钟 elapsed` 推进过期——引擎空闲期用墙钟满足窗口
TTL 的设计。这是**输出非确定性的唯一已知来源**（30M 量级 0.0036%），确定性
模拟器原理上无法复现。

## 6. 关联

- [hot-path-vectorization-design.md](hot-path-vectorization-design.md) —— 同一
  executor 的性能侧设计
- `wf-examples/performance/nexmark_pk/scripts/verify_ground_truth.py` —— 本文
  语义的精确可执行镜像（docstring 记录验证依据）
- 仓库根 `TASK_PK_FLINK.md` §9.7/9.8 —— 验证闭合过程
