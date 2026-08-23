# 并发处理能力提升(总纲)

> 状态:Active——本文是「如何提升系统并发处理能力」的**主文档**。
>
> 整合自:规则分片(进程内单规则 key 分片 P2a/P2b/P2c,已实施)、
> 实例分片(部署级多实例水平扩展,企业版预研)、窗口并行证伪(六维并行度模型)。
>
> 关联:[window-push-model-design.md](archive/window-push-model-design.md)(push 架构)、
> [window-channel-actor-design.md](window-channel-actor-design.md)(window 单写者 actor)、
> [rule-sharding-p2a-plan.md](rule-sharding-p2a-plan.md)(P2a 实施计划)、
> [preread-budget-design.md](preread-budget-design.md)(预算/深度节流)、
> [PERF_BISECTION_METHOD.md](../PERF_BISECTION_METHOD.md)(瓶颈定位方法论)

---

## 1. 吞吐模型:六维并行度

整条链路抽象成**六个独立可调维度**。命名规范:**域前缀 + 阶段**——`C-` = 采集侧
(Client),`W-` = 引擎侧(wfusion);主缩写按链路顺序记忆(U→R→W→P→E→S)。

| 维度 | 简写 | 域 | 配置项 | 结论 |
|---|---|---|---|---|
| **数据上报并发** | **C-UCP** | 采集侧 | 连接数(= 客户端进程数) | **非杠杆**;连接数 4 即饱和,之后纯开销 |
| 引擎读取并行 | **W-RDP** | 引擎 | `instances`(1..=16) | **零代码;4 即够**;连接级 round-robin 分发 |
| 窗口计算并行 | **W-WCP** | 引擎 | `shard_by`(窗口定义层声明) | **后置**;唯一保序敏感维度,须按 key 拆 |
| 引擎解析并行 | **W-PDP** | 引擎 | `parse_parallelism` | 待墙打破后重测 |
| 规则计算并行 | **W-EDP** | 引擎 | `rule_parallelism` + P2a 规则分片 | 高规则数负载才是它的舞台 |
| Sink 发送并行 | **W-SDP** | 引擎 | sink `parallel` | 每事件 1 alert 的负载值得确认 |

```
C-UCP → W-RDP → W-WCP → W-PDP → W-EDP → W-SDP
 采集侧      引擎读取     窗口计算    解析      规则计算     发送
```

**关键性质**:

1. **瓶颈在「最窄档」**:下游维度在低供给下「测出来无效」是假象;多个下游维度
   同时饱和到同一数字,说明墙在更上游(引擎内部的两道墙,见 §2)。
2. **W-WCP 默认=1 有语义价值**:单写者是保序/无锁/消费感知驱逐/精确记账的共同前提,
   不是「没来得及并行」;若启用,生产形态必须按 key 拆(per-key 顺序不破)。
3. **结论须用「唯一数据稳态」口径**:重复数据注入 / 短瞬态测出的「连接数主杠杆」
   类结论不随真实负载成立——吞吐结论一律以唯一事件数据 + 稳态为基准(§5)。

---

## 2. 瓶颈模型:两道墙

用「二分法逐段切除」定位(方法论见 `docs/PERF_BISECTION_METHOD.md`):从管线尾部
逐段屏蔽,每刀保留输入计数可测(切到窗口 append 本身会使基准无法收敛),叠加
切除逼近最前端。定位结果:

**第一道墙:preread 预算(记账修正后成为「深度节流器」)**。

- 记账单位经历一次修正:IPC 解码后按解码内存记账对真实数据**结构性高估 ~10×**
  (wire 级真实在途,与字段宽度无关)。改按 `content_bytes`(≈ wire)记账后,
  同样预算的槽位 ×~10。
- 预算槽位是**闭回路(Little's law):吞吐 = 槽位 ÷ 驻留**。驻留随槽位增长(排队
  效应),故有峰值后衰减——**预算必须有界**:过小则源被节流(饿着规则),过大则
  过深缓冲放大窗口重排(退化)。甜点在 ~2GB content(数百槽)。
- 预算同时是**内存阀**与**管线深度节流器**,两者由同一参数约束。

**第二道墙:窗口 actor 单写者串行(结构性)**。

- 每窗口一个 actor:重排(等 (source,seq) 乱序到齐)+ append + 广播 + ack;
  dispatch 串行 → 反压回第一道墙。
- **预算加深会放大这道墙**:管线缓冲越深,actor 重排待补集越大、缺口等待气泡越多。
- 对无状态 on-each 窗口是纯开销(可旁路);对有状态规则是语义核心(保序/驱逐/
  记账),只能实现层优化;按 key 拆 actor 只对高 fanout 窗口读有效。

**两道墙的关系**:预算放开后源侧仍有等待(第一道墙消融后剩深度节流语义);
规则侧按事件成本 × 并行任务数构成第二均衡点。没有单一参数能突破均衡点,
需要引擎级工程(输出批量构建、解码并行、actor 实现层优化)。

---

## 3. 提升杠杆(按层次)

### 3.1 入流侧(零代码可用)

- **C-UCP**:连接数 4 即甜点(4 之后纯开销);上限在 acceptor `max_connections`。
- **W-RDP**:`instances = 4` 即够(连接 round-robin 分发,每实例多连接无并行度
  损失)。生产配置示例:

  ```toml
  # topology/sources/ingress.toml
  connect = "tcp_src"
  addr = "127.0.0.1"
  port = "9800"
  framing = "len"
  data_format = "arrow_framed"
  instances = 4        # W-RDP=4 即够;上游并发连接 4
  ```

- **基准注入工具(`wfgen`,三层流水线)**:
  1. `dump-frames`:JSONL → 预编码帧文件;
  2. **`shard-frames --shards N --shard-keys "bid_events:auction,..."`**:一次切分,
     产出 N 个分片帧文件(同 key 同文件,键闭包);按行攒批保持分片帧大小 ≈ 原始帧
     (小帧分片会掉吞吐,需攒批到 MB 级);
  3. `send-arrow --connections=N`(无状态,纯 copy)或 `--shard-files f0,f1,...`
     (生成时已分片,纯 copy 多连接,零解码)。
  - **生成时分片(`shard-frames`)+ `--shard-files` 是正确形态**;发送时动态
    `--shard-keys`(decode 分桶)有丢数缺陷,不推荐。
  - **基准口径注意**:小总量下固定开销会稀释 EPS,且多连接因每条连接数据量小被
    稀释更狠——**稳态吞吐统一用大总量口径**。

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
  才 seal 输出**——「合并前确认所有分片分量到齐」的两阶段聚合模式。
- **可行性判定**:有 match key → 可分片;无 key → 退单 worker;`each`(无状态)→
  事件级并行另一条路;conv → 走聚合窗口;joins/has() → 待确认。
- **回退**:规则级 `shards=1` 即单 worker;分片不可行自动退单 worker。
- **遗留待定**:key 倾斜(热 key)、低基数 key(基数 < N 退单 worker)、跨 shard emit
  顺序语义、reload 时 shard 数变化(视为拓扑变更拒绝)、close_all 原子竞争、指标
  gauge/counter 区分。

### 3.3 窗口计算侧(W-WCP,后置)

窗口并行在低 fanout 负载下无因果作用(车道 ×8 零增益)——单写者不是当前瓶颈。
若未来在**高 fanout 负载**(广播成本 ∝ 规则数)下成立,生产形态必须是**按 key 拆
actor**(per-key 保序不破),配合窗口定义层 `shard_by` 声明 + 规则静态检查,
而非 round-robin。

### 3.4 部署级:实例分片(企业版预研)

**定位**:N 个独立引擎进程各持全部规则、各吃一个键子集。与进程内分片正交可叠加。
当前版本不实现。

**正确性原理(键闭包)**:规则全部状态(窗口、per-key 实例、限流)挂在实体键上;事件
解析与规则求值是「键内闭包」。分片器保证同一实体键的所有事件永远进同一实例 →
每条规则算出的结果与单机完全一致。分布式 CEP 的经典难题(跨节点顺序、共享状态、
分布式快照)在**状态模型**里被消灭在上游,不是被协调协议解决。
**判据:分片方案好不好,看它的分片器有多简单。**

**维度选择**:key 维(千级键均摊流量,均匀),不用时间维(实时流任一时刻事件几乎
全落在当前 1-2 个窗口 → 有效并行 1-2)。时间分片的正确场景是离线回放(历史全跨度
窗口同时活跃)。

**shard key**:网络遥测默认源 IP(认证负载高唯一源 IP 基数,基数 ≫ 分片数);
dns(domain)/file(user) 键源独立分片组。

**设计两支柱(定案)**:
1. **window 定义层声明 `shard_by`**(每源声明,非全局):`[window.conn_events] shard_by = "sip"`;
2. **规则静态检查(加载期 fail-fast)**:读 RulePlan 元数据,毫秒级分类——
   SAFE(键闭包,直接分片)/ TWO_PHASE(可合并聚合,分片 + 合并层)/
   UNSAFE(跨分片组引用、全局统计、双实体关联,加载即拒)。**「宁可拒载让人改规则,
   不要运行期算出静默错误的告警」**。

**TWO_PHASE 两阶段聚合**:count/sum 直接加;avg 拆 sum+count;min/max/topN 各分片
top 合并;精确 distinct 集合可并(代价大)、近似 HLL 天生可合并。引擎内已有该模式的
实例:P2c 的 `ConvStageTask`(barrier 等齐再 seal)。

**HA**:无主备,故障域隔离;单实例坏 = 该键子集停摆,其余不受影响。补齐路径:
上游采集器磁盘缓存 + 重放,或每分片双副本。

**验收门禁**:分片前后逐规则 emitted 对拍——静态检查管「设计上安全」,对拍管
「实现上正确」。辅助:分片间 EPS 偏差监控、缺 shard 字段事件计数 = 0。

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
- **正确性门禁**:`SUMMARY clean`(致命计数器为零)+ EMIT 计数对齐;实例分片用
  逐规则 emitted 对拍。
- **profile**:`scripts/analyze_profile.py`(macOS `sample` 输出解析:等待/忙占比、
  热点符号)。

---

## 6. 优先级

| 优先级 | 项 | 状态 |
|---|---|---|
| P0-② | **解码内存记账修正**(预算按 content/wire 字节计) | **已实施**:`push_decoded_batch`/IPC replay 改收 `content_bytes`。甜点 ~2GB content,过大过度缓冲退化(见 §2 第一道墙) |
| P0-③ | 窗口 actor 实现层优化(重排/1 槽流控) | 第二道墙,对有状态规则是语义核心,只做实现层 |
| P0-④ | 规则输出列**批量构建** | 只省 CPU,非吞吐杠杆(二分法已证) |
| P1 | W-PDP / W-EDP / W-SDP 在墙打破后重测 | 待测 |
| 已实施 | P2a 规则 key 分片 / P2b 共享限流预算 / P2c conv 两阶段 | 已上线 |
| 预研 | 实例分片(企业版) | 设计定案,待立项 |
| 后置 | W-WCP 按 key 拆 window actor | 低 fanout 证伪;高 fanout 窗口读未测 |

**待办(引擎级,非基准参数)**:规则输出列**批量构建**(commit_each_row 逐行
Arc 分配/格式化,占规则计算大半)——只省 CPU,非吞吐杠杆;吞吐下一个突破点 =
第二道墙(窗口 actor 重排/单写者)。
