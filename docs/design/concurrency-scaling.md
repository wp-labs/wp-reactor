# 并发处理能力提升(总纲)

> 状态:Active——本文是「如何提升系统并发处理能力」的**主文档**。
>
> 2026-08-17 重组:整合三篇旧设计文档(已归档进本文各节):
> - **规则分片**(原 `rule-sharding-and-aggregation-window.md`):进程内单规则 key 分片
>   (P2a/P2b/P2c,**已实施**)→ §3.2;
> - **实例分片**(原 `instance-sharding-design.md`):部署级多实例水平扩展(企业版预研)→ §3.4;
> - **决定性实验**(原 `window-shard-prototype-experiment.md`):window actor 分片证伪 +
>   六维并行度模型 + 入流瓶颈证据 → §1/§2。
>
> 关联:[window-push-model-design.md](window-push-model-design.md)(push 架构)、
> [window-channel-actor-design.md](window-channel-actor-design.md)(window 单写者 actor)、
> [rule-sharding-p2a-plan.md](rule-sharding-p2a-plan.md)(P2a 实施计划)

---

## 1. 吞吐模型:六维并行度

整条链路抽象成**六个独立可调维度**。命名规范:**域前缀 + 阶段**——`C-` = 采集侧
(Client),`W-` = 引擎侧(wfusion);主缩写按链路顺序记忆(U→R→W→P→E→S)。

| 维度 | 简写 | 域 | 配置项 | 实测状态(唯一数据稳态) | 结论 |
|---|---|---|---|---|---|
| **数据上报并发** | **C-UCP** | 采集侧 | 连接数(= 客户端进程数,send-arrow 每进程 1 连接) | c2/4/16 = 5.56/5.92/5.93M,**c4 即饱和** | **非杠杆**(早期"主杠杆"为纯 copy 探针口径,已废弃) |
| 引擎读取并行 | **W-RDP** | 引擎 | `instances`(wp-core-connectors 已有,1..=16) | 16 连接下 i4≈i16(4.95 vs 4.98M) | **零代码;4 即够** |
| 窗口计算并行 | **W-WCP** | 引擎 | 原型 `WINDOW_SHARD_RR`(已移除)/ 生产 `shard_by` | 1→8 车道零增益(q1 低 fanout) | **后置**;唯一保序敏感维度 |
| 引擎解析并行 | **W-PDP** | 引擎 | `parse_parallelism` | 10→20 无增益(双层墙下) | 待墙打破后重测 |
| 规则计算并行 | **W-EDP** | 引擎 | `rule_parallelism` + P2a 规则分片 | 10→20 无增益(双层墙下) | rules100(450 规则)才是它的舞台 |
| Sink 发送并行 | **W-SDP** | 引擎 | sink `parallel` | 未单独测 | q1 每事件 1 alert,值得确认 |

```
C-UCP → W-RDP → W-WCP → W-PDP → W-EDP → W-SDP
 采集侧      引擎读取     窗口计算    解析      规则计算     发送
```

**关键性质**:

1. **瓶颈在「最窄档」**:下游维度在低供给下「测出来无效」是假象——但 q1 唯一数据稳态下
   连接/实例/解析/规则并行度全部饱和在 ~5.9M,**墙在引擎内部**(§3.1 双层墙)。
2. **W-WCP 默认=1 有语义价值**:单写者是保序/无锁/消费感知驱逐/精确记账的共同前提,
   不是「没来得及并行」;若启用,生产形态必须按 key 拆(per-key 顺序不破)。
3. **早前"C-UCP 主杠杆 / 入流供给是第一瓶颈 / 4.2→11.15M 单调"的结论已废弃**:
   那是纯 copy 供给(重复数据)的爬坡瞬态口径,不随真实负载成立(§2.2)。

---

## 2. 瓶颈证据(2026-08-17 决定性实验)

### 2.1 window actor 车道证伪

预注册判据:「8 车道拿到单车道 ~7× EPS → window 是核心瓶颈;曲线 4 车道就弯 →
第二瓶颈现形」。实测(nexmark_pk q1 30M,prototype 已移除):

| Feed | 车道 1 | 车道 8 | 结论 |
|---|---|---|---|
| TCP | 4.2M | 4.3~5.0M(不单调) | 无因果作用 |
| file 重放 | 6.08M | 6.10M | **车道 ×8 零增益** |

正确性全 clean(alert 数与基线逐位一致)。**「window 单写者是核心瓶颈」被证伪**
(至少 q1 低 fanout 负载);W-WCP key-split 杠杆**后置**。

### 2.2 入流供给(纯 copy 探针,结论已废弃)

> **2026-08-17 复盘废弃**:早期"入流供给是第一瓶颈 / C-UCP 主杠杆 /
> 4.2→11.15M 单调"的结论来自**纯 copy 供给口径**——每条连接推完整帧文件
> (重复数据)、`engine_appended` 到阈值即 kill,测的是爬坡瞬态(RSS ~0.75GB =
> 窗口未积累)。该口径不能代表真实负载,**结论已废弃,不再作为调参依据**。
> 唯一数据稳态(分片文件注入)下连接数不是杠杆(c2/4/16 = 5.56/5.92/5.93M),
> 墙在引擎内部(§3.1 双层墙)。

排除清单(全部无效,两轮独立验证一致):

| 项 | 结果 |
|---|---|
| parse 并行度 10→20 | 无效 |
| rule 并行度 10→20 | 无效 |
| preread 预算 ×4(256MB→1GB) | 仅 +3.4% |
| 屏蔽 alert 输出(commit_each_row) | CPU 728→381%,EPS 不变(稳态下仅 +7%) |
| decode | profile 0.3% |
| 批大小 25k/100k/400k | 5.56/5.92/5.57M,无影响 |
| staging 256→512KB / 日志 / 客户端单连接 | 无效 |

### 2.3 二分法逐段切断定位(2026-08-17 晚)

**方法**:从管线尾部逐段屏蔽,每刀保留 `append_total` 可测(切到窗口 append 本身会
使 bench 无法收敛——第 4 刀前的错误尝试)。测试条件:q1 100m / 4 连接 / 100k 帧 /
**输出屏蔽态**(见 §3.1 注:输出已硬切,规则经 `WF_NO_RULE` 门控)。

| 刀 | 切断/改动 | EPS | CPU | 结论 |
|---|---|---|---|---|
| — | 基线(有输出,256MB) | 5.93M | 717% | — |
| ① | 切输出(stage+commit no-op) | 5.94M | 335% | 不是墙(占一半 CPU,非吞吐瓶颈) |
| ② | 切规则(process_batch no-op) | 5.94M | 129% | 不是墙(规则整段空转,吞吐不动) |
| ③ | **preread 预算 256MB→1GB→4GB** | 6.57M→**8.31M** | 132→195% | **第一道墙:预算槽位** |
| ④ | 砍 fanout(窗口→规则推送) | 6.29M + RSS 20GB | — | 必要机制(不 ack→窗口不驱逐),不可砍 |
| ⑤ | 窗口 mailbox 预算 64MB→1GB | 6.6M(反而差) | — | 1 槽串行是当前形态;放大加深乱序重排 |
| ⑥ | parse p=1 | 6.14M(差) | — | route_parse 并行有用 |

**源侧计时**(4GB + 输出/规则切):recv(等 TCP 数据)p50=18ms / conv(解码)1.4ms——
解码不是墙(累计 8%),源大部分时间在等反压回传。**PUSH 拆分**:acquire(等 preread
槽)p90=43ms 尖峰 / send(等 parse 通道)≈0——预算信号量偶发打满,是流量阀。

**状态查询验证(4GB,输出屏蔽态)**:q2(stateful bid match)5.84→**7.21M**(+23%),
q9(join)≈0——预算槽位卡住**所有走 92M bid 流的负载**(有/无状态一视同仁);
q9 瓶颈在 join/窗口读,在预算下游。

## 两道墙模型(二分法实证)

1. **第一道墙:preread 预算槽位**(配置可解)。IPC 解码把 2.9MB wire 膨胀成
   ~116MB Arrow 内存(40×),预算按解码内存计 → 256MB 仅 ~2 槽 → 源被节流。
   `parse_buffer_bytes=4GB` → q1 +40%(5.94→8.31M)、q2 +23%。
2. **第二道墙:窗口 actor 单写者串行**(结构性)。每窗口一个 actor:重排(等
   (source,seq) 乱序到齐)+ append(BTreeMap)+ 广播 + ack;窗口 mailbox 预算
   64MB < 批 116MB → 1 槽 → dispatch 串行 → 反压回 preread 预算。对无状态
   on-each(q1)窗口是纯开销(可旁路,但真实负载覆盖率低);对有状态规则是
   语义核心(保序/驱逐/记账),只能实现层优化;key 分片(W-WCP)只对高 fanout
   窗口读有效。

---

## 3. 提升杠杆(按层次)

### 3.1 入流侧(P0,零代码可用)

- **C-UCP**:连接数 4 即甜点(2026-08-17 实测 100m q1:单连接 4.50M →
  c2 5.56M → **c4 5.91M** → c16 5.93M,4 之后纯开销)。上限在 acceptor
  `max_connections=1000`。
- **W-RDP**:`instances = 4` 即够(实测 q1@16 连接 i=4=4.95M ≈ i=16=4.98M,
  实例数不是瓶颈;连接 round-robin 分发,每实例多连接无并行度损失)。
  生产配置示例:

  ```toml
  # topology/sources/ingress.toml
  connect = "tcp_src"
  addr = "127.0.0.1"
  port = "9800"
  framing = "len"
  data_format = "arrow_framed"
  instances = 4        # W-RDP=4 即够(实测 4/8/16 无差异);上游并发连接 4
  ```

- **基准注入工具(`wfgen`,三层流水线)**:
  1. `dump-frames`:JSONL → 预编码帧文件;
  2. **`shard-frames --shards N --shard-keys "bid_events:auction,..."`**:一次切分,
     产出 N 个分片帧文件(同 key 同文件,键闭包);按行攒批(TARGET_ROWS=100k)
     保持分片帧大小 ≈ 原始帧(实测:小帧分片 376KB → q1 掉到 5.2M,攒批后 7.7MB);
  3. `send-arrow --connections=N`(无状态,纯 copy)或 `--shard-files f0,f1,...`
     (生成时已分片,纯 copy 多连接,零解码)。
  实测(q1 100M):**4 分片 5.91M ≈ 16 分片 5.93M > 单连接 4.5M**(+31%);
  q2(stateful)分片文件注入 emitted 与单连接**逐位一致**(74698 == 74698);
  发送时动态 `--shard-keys`(decode 分桶)有 ~6% 丢数缺陷,不推荐——
  **生成时分片(`shard-frames`)+ `--shard-files` 是正确形态**;
  `bench.sh` 一键:`./bench.sh all cont 100m`(默认 CONNECTIONS=4 +
  SHARD_KEYS="bid_events:auction" 即 4 分片,自动 shard-frames + 缓存复用;
  纯 copy 天花板:`SHARD_KEYS="" CONNECTIONS=16 ./bench.sh q1 cont 100m`)。

  **基准口径注意**:30m 小总量下固定开销(~2s 发送器启动/append 滞后/轮询
  粒度)会稀释 EPS,且 c2/c4 因每条连接数据量小被稀释更狠(30m 下曾出现
  "拆 4=3.85M < 单连接 4.25M" 的假象)——**稳态吞吐统一用 100m 口径**。

  **实测修正(2026-08-17,含一次推翻)**:

  - **tail-tag bug(曾导致 bench "卡死")**:攒批尾部 flush 一度把不足一帧的
    行打成 `tail` 标签,引擎按 tag 路由时整帧丢弃(100M − 1.4M = rx 停在
    98.6M),`engine_appended` 永远追不平 → bench 等待循环空转到超时(1600s)。
    修复:flush 沿用原流 tag,同分片内按 (tag, schema) 分组。
  - **分片均衡与 EPS 无关(推翻了早先的 s0 straggler 假设)**:未列入
    `--shard-keys` 的流(auction_events 6M + person_events 2M)整帧全走 s0,
    s0 达 13.75M 行(其他 5.75M,2.4× 不均衡)。但改用均衡键
    (`bid_events:auction,auction_events:seller,person_events:id`,16 文件
    全部 ~463MB)复测 100m:q1 5.93M(旧 5.95)、q3 11.3M(旧 12.0)、
    q4 5.3M(旧 5.0)、q9 11.3M(旧 11.2)——全部在 ±5% 噪声内。
    **文件均衡与否不改变任何查询的 EPS**。
  - **纯 copy 的 18.2M 是真实持续速率,但不是稳态吞吐(再次修正 2026-08-17)**：
    逐秒 row/s 为 15.1/20.1/16.9/16.8/16.9/16.9M(持续 ~17M/s,非首秒爆发)。
    但它的负载形态特殊:16 条连接推**同一份**数据(重复),引擎 6s 内只消化了
    每条连接的前 ~6.3M 行(append 计数器到 100M 即 kill 客户端),窗口几乎不
    积累(内存 746MB、evict=1531,重复数据冲刷)。对照 shard-files(16 条连接
    推 **100M 唯一**数据、引擎跑满):窗口积累 2.9GB,稳态 append ~5.8M/s
    (首秒 10.5M 后回落 5.6~5.9M)。**唯一数据下 16 连接只有单连接(4.7M)的
    1.24×——连接数不能线性扩展吞吐**。C-UCP 的 "19.8M 天花板" 修正为:
    重复注入+低内存条件下的乐观数字,真实唯一负载稳态 ~5.8M。

  **管线埋点定位(2026-08-17,实验 D + A/B/C)——双层墙模型**。

  q1(100M,4 连接,100k 行/批)逐段计时(临时 env `WF_TRACE_PIPE` 埋点,已移除):

  | 段 | p50 | 并行度 | 结论 |
  |---|---|---|---|
  | route_parse(事件物化) | 18.8ms/批 | 10 并行 | 容量 530 批/s,有余量 |
  | window append | 48us | 窗口内 | us 级,不是门 |
  | broadcast(窗口→规则) | 3~4us | — | 通道从不阻塞 |
  | rule_batch(规则处理) | 122ms/批 | 10 任务、整批单任务 | 每行 ~1.2us,最大成本 |
  | rule_wait(规则空闲) | 18ms | — | 规则被生产侧饿着 |
  | source_push(源→parse) | 20.5ms | 4 源 | **preread 预算闸门** |

  排除实验(均不改变 ~5.9M 均衡点):
  - **输出路径**(屏蔽 `commit_each_row`):规则计算快 2.4×(122→49ms),EPS 不动——
    预算先把规则饿着,减输出无收益;预算放开后屏蔽输出仅 +7%(输出占规则计算 ~57%);
  - **批大小** 25k/100k/400k:EPS 5.56/5.92/5.57M,无影响(解码后内存 29/116/464MB
    与预算槽数反比,相互抵消);
  - **preread 预算** 256MB→1GB:仅 +3.4%(解码后每批 ~29MB,256MB ≈ 9 槽;
    放开后规则从饥饿变饱和,均衡点仍在);
  - **连接数** 2/4/16:5.56/5.9/5.93M,4 后饱和(预算共享)。

  **结论:q1 的墙是双层的**——生产侧 ~6.5M/s(源读取+解码驻留+预算,预算放开后
  仍有 ~15ms source 等待)与规则侧 ~6.1M/s(每行 ~1.7us:eval+staging+输出构建,
  输出占 57%;10 任务饱和即 6.1M)。**没有单一参数能突破 ~6.5M**;要再上需要
  引擎级工程(规则输出列批量构建、生产侧解码并行),非基准调参。
  - **均衡键的正确性已验证(规则零改动)**:30m 单连接对照,q2=224,289 /
    q3=1,800,000 / q4=27,600,000 / q7=10,350,961 / q9=1,800,000 全部**逐位
    一致**,q5 差 235 条(0.014%,count>=10 边界对 state eviction 敏感)。
    auction_events 按 `seller` 分片可行,因为:①q3 的 key 是 `id`,而该
    数据集每个 auction id 只出现一次(单事件),天然单连接;②snapshot join
    走共享窗口(q4 的 bid→auction 跨分片 join 100% 命中证明)。
    若未来数据集出现同 id 多事件,q3 语义会被破坏——均衡键是数据性质依赖。

  全量复测(100m,16 连接 shard-files,2026-08-17):q1=5.9M / q2=5.84M /
  q3=12.0M / q4=5.0M / q5=3.7M / q7=3.55M / q9=11.2M,全部 `appended=100M/100M`
  + SUMMARY clean(无 dropped_late / serialize_failed)。q2 与单连接基线逐位一致;
  q3/q4/q5/q7 的 EMIT 计数有 run 间波动,来自 rule-state(512MB)与 auction
  window(64MB)的内存 eviction 时序,非键闭包破坏。
- 注意:`bench.sh` 分片文件缓存 key(`data/shard_${TOTAL}_c${CONNECTIONS}`)
  已加 shard-keys 指纹(2026-08-17)——换 SHARD_KEYS 不再静默复用旧分片文件。
- **基准参考(q1 100M 稳定口径,2026-08-17 四轮复测)**:默认配置
  (CONNECTIONS=4 / SHARD_KEYS=bid_events:auction / instances=4 / p=10 r=10 /
  100k 帧)下 **EPS = 5.93M ± 0.01M**(5.92~5.94M,波动 ±0.2%)、
  **RSS 峰值 ≈ 4.4GB**、**CPU 均值 ≈ 714~723%(16 核用 ~7.2 核)**、
  evict 55~62,全部 `appended=100M/100M` + clean。早前 5.57~5.75M 的波动
  全部来自实验配置(批大小/预算/屏蔽输出),非机器噪声。
- 待办(引擎级,非基准参数):规则输出列**批量构建**(commit_each_row 逐行
  Arc 分配/格式化,占规则计算 ~57%)——**只省 CPU,非吞吐杠杆**(二分法 ① 已证,
  见 §2.3);吞吐突破点 = preread 预算槽位与解码膨胀(§2.3 P0-①/②)。
- **⚠ 当前探针态(2026-08-17 调查中)**:输出路径已硬切(`stage_yield_cell` /
  `commit_each_row` no-op,标注 TEMP-PROBE,工作区未提交),规则 per-event 处理
  经 `WF_NO_RULE` 门控,parse 经 `WF_NO_PARSE` 门控,源计时经 `WF_TRACE_SRC`。
  上述二分法数字(§2.3)均在该减少逻辑因子的状态下测得;**恢复输出后需重测**。

### 3.2 规则计算侧(已实施:P2a/P2b/P2c)

**目标**:把单个规则的 per-key match 并行化(规则分片),对任意规则(含 conv/限流)
保持语义正确。

**核心洞察**:CEP 规则状态分两类——

| 状态 | 作用域 | 频率 | 处理 |
|---|---|---|---|
| 实例状态(count/avg/max 累加器) | per-key | 热(每事件每 key) | **分片并行**(P2a) |
| 全局状态(conv 结果、限流、预算) | 跨 key | 冷(close/周期) | **聚合窗口 + 共享原子**(P2b/P2c) |

```
源 → window → 按 key 哈希分区 ─┬─► shard 0(per-key match)─┐
                              ├─► shard 1(per-key match)─┤ close 原始输出
                              └─► shard N(per-key match)─┘
                                                          ▼
                                    自动生成的「conv 聚合窗口」(中间窗口)
                                                          ▼
                                    conv 阶段(sort/top/dedup + 限流 + emit)
                                                          ▼
                                                       sink fanout
```

- **P2a 分片机制**:key 提取 hoist 到 dispatch 层(编译期 `key_extractor`,与状态机共用);
  批内按 `hash(key) % N` 分区投到 N 个 shard worker(复用 `RuleTask`,每 shard 一份
  `CepStateMachine`,实例表只装本 shard key)。
- **P2b 共享原子**:限流(`Arc<AtomicU64>` + 共享窗口起点)、预算(`Arc<AtomicUsize>`)、
  指标聚合(按类型:gauge 求和/取 max,counter 求和)。只在 close/限流检查时访问,低频。
- **P2c conv 两阶段**:close 输出进自动生成的 conv 窗口,`ConvStageTask` 按
  `window_start` 落桶,**barrier(每 shard AtomicI64 单写者)等齐所有 shard 的 watermark
  才 seal 输出**——这正是「合并前确认所有分片分量到齐」的两阶段聚合模式。
- **可行性判定**:有 match key → 可分片;无 key → 退单 worker;`each`(无状态)→
  事件级并行另一条路;conv → 走聚合窗口;joins/has() → 待确认。
- **回退**:规则级 `shards=1` 即单 worker;分片不可行自动退单 worker。
- **遗留待定**:key 倾斜(热 key)、低基数 key(基数 < N 退单 worker)、跨 shard emit
  顺序语义、reload 时 shard 数变化(视为拓扑变更拒绝)、close_all 原子竞争、指标
  gauge/counter 区分。

### 3.3 窗口计算侧(W-WCP,后置)

实验证伪(q1 低 fanout 下车道无因果作用)。若未来在**高 fanout 负载**(如 rules100
450 规则,actor 广播成本 ∝ 规则数)下重测成立,生产形态必须是**按 key 拆 actor**
(per-key 保序不破),配合窗口定义层 `shard_by` 声明 + 规则静态检查,而非 round-robin。

### 3.4 部署级:实例分片(企业版预研)

**定位**:N 个独立引擎进程各持全部规则、各吃一个键子集。与进程内分片正交可叠加。
当前版本不实现。

**正确性原理(键闭包)**:规则全部状态(窗口、per-key 实例、限流)挂在实体键上;事件
解析与规则求值是「键内闭包」。分片器保证同一实体键的所有事件永远进同一实例 →
每条规则算出的结果与单机完全一致。分布式 CEP 的经典难题(跨节点顺序、共享状态、
分布式快照)在**状态模型**里被消灭在上游,不是被协调协议解决。
**判据:分片方案好不好,看它的分片器有多简单。**

**维度选择**:key 维(1000+ 键均摊流量,均匀),不用时间维(实时流任一时刻事件几乎
全落在当前 1-2 个窗口 → 有效并行 1-2)。时间分片的正确场景是离线回放(历史全跨度
窗口同时活跃)。

**shard key**:网络遥测默认源 IP(QRadar 认证负载 250k 唯一源 IP,基数 ≫ 分片数);
dns(domain)/file(user) 键源独立分片组。

**设计两支柱(定案)**:
1. **window 定义层声明 `shard_by`**(每源声明,非全局):`[window.conn_events] shard_by = "sip"`;
2. **规则静态检查(加载期 fail-fast)**:读 RulePlan 元数据,毫秒级分类——
   SAFE(键闭包,直接分片,预计 rules100 ~95%)/ TWO_PHASE(可合并聚合,分片 + 合并层)/
   UNSAFE(跨分片组引用、全局统计、双实体关联,加载即拒)。**「宁可拒载让人改规则,
   不要运行期算出静默错误的告警」**。

**TWO_PHASE 两阶段聚合**:count/sum 直接加;avg 拆 sum+count;min/max/topN 各分片
top 合并;精确 distinct 集合可并(代价大)、近似 HLL 天生可合并。引擎内已有该模式的
实例:P2c 的 `ConvStageTask`(barrier 等齐再 seal)。

**HA**:无主备,故障域隔离;单实例坏 = 该键子集停摆,其余不受影响。补齐路径:
上游采集器磁盘缓存 + 重放,或每分片双副本。

**验收门禁**:分片前后逐规则 emitted 对拍(rules100 293 条触发规则告警数应逐规则一致)
——静态检查管「设计上安全」,对拍管「实现上正确」。辅助:分片间 EPS 偏差监控、
缺 shard 字段事件计数 = 0。

**性能预期(外推)**:40 核服务器 4-5 实例,rules100 450 规则口径 ~400-480k EPS
(单实例 ~96.7k);内存 5 × 2.24GB ≈ 11GB。

---

## 4. 正确性约束(哪些并行安全)

| 并行形态 | 正确性前提 | 机制 |
|---|---|---|
| 解析/计算/发送并行(无状态) | 无跨事件状态 | 直接并行,零风险 |
| 规则 key 分片(P2a) | 同 key 同 shard | 编译期 `key_extractor` + hash 分区 |
| 跨 key 聚合(conv/统计) | 分片分量可合并 | 两阶段聚合 + barrier 等齐(TWO_PHASE 模式) |
| window actor 拆片(W-WCP) | per-key 保序 | 必须按 key 拆,round-robin 只对 stateless 成立 |
| 实例分片(部署级) | 键闭包 | shard_by 声明 + 规则静态检查 fail-fast |

**统一原则**:无状态可任意并行;保序状态必须「同 key 同 shard」;跨 key 聚合必须
「可合并 + 等齐」;任何违反在加载期拒绝,运行期零协调。

---

## 5. 验证方法

- **bench 矩阵**:`wf-examples/performance/nexmark_pk/scripts/window_shard_bench.sh`
  (TCP 基线 / file 重放对照 / `instances` 4/8/16——即 C-UCP × W-RDP 解耦矩阵)。
- **正确性门禁**:`SUMMARY clean`(致命计数器为零)+ `EMIT q1_bid_passthrough =
  TOTAL×92%`;实例分片用逐规则 emitted 对拍。
- **profile**:`scripts/analyze_profile.py`(macOS `sample` 输出解析:等待/忙占比、
  热点符号)。

---

## 6. 优先级与决策日志

| 优先级 | 项 | 状态 |
|---|---|---|
| **P0-①** | `parse_buffer_bytes` 放大(256MB→1~4GB) | 已验证:q1 5.94→8.31M(+40%)、q2 5.84→7.21M(+23%),待转正 |
| **P0-②** | **解码内存膨胀修复**(预算按 wire/内容字节计,槽位翻 ~40×) | 二分法定位的第一道墙根因,真实负载最大普适杠杆 |
| P0-③ | 窗口 actor 实现层优化(重排/1 槽流控) | 第二道墙,对有状态规则是语义核心,只做实现层 |
| P0-④ | 规则输出列**批量构建** | 只省 CPU(占规则计算 ~57%),非吞吐杠杆(二分法 ① 已证) |
| P1 | W-PDP / W-EDP / W-SDP 在墙打破后重测 | 待测 |
| 已实施 | P2a 规则 key 分片 / P2b 共享限流预算 / P2c conv 两阶段 | 已上线 |
| 预研 | 实例分片(企业版) | 设计定案,待立项 |
| 后置 | W-WCP 按 key 拆 window actor | 实验证伪(q1 低 fanout);高 fanout 窗口读未测 |

**决策时间线**:

1. 2026-08-13:P2a/b/c 规则分片设计定案,已实施(见原 rule-sharding 文档)。
2. 2026-08-17:多实例分片定位企业版,当前不实现;单实例口径即基准。
3. 2026-08-17(决定性实验):window actor 车道 ×8 零增益 → W-WCP 后置;
   早期判定的"入流供给第一瓶颈 / C-UCP-W-RDP 主杠杆"为纯 copy 探针口径,
   已废弃(见 §2.2)。
4. 2026-08-17(模型):六维并行度模型定案(C-UCP/W-RDP/W-WCP/W-PDP/W-EDP/W-SDP);
   原型代码(env `WINDOW_SHARD_RR`)移除。
5. 2026-08-17(晚):管线埋点定位双层墙;稳定基准 q1 5.93M / RSS 4.4GB / CPU ~7.2 核。
6. 2026-08-17(二分法):逐段切断实锤——输出/规则/fanout 非墙,preread 预算槽位为
   第一道墙(256MB→4GB:q1 +40%、q2 +23%),窗口 actor 单写者为第二道墙(§2.3)。
