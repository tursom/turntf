# 集群在线连接容量优化方案

本文档把“如何提高当前集群模式下的真实在线连接上限”整理为可执行方案。它不讨论泛泛的扩容建议，而是基于当前代码路径、最近补充的多节点在线容量 benchmark，以及我们已经观测到的具体瓶颈来排序优化项。

相关基线与 benchmark 见：

- [performance-baseline.md](/root/dev/sys/turntf/turntf/docs/performance-baseline.md)
- [internal/api/client_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_benchmark_test.go)

## 1. 当前结论

在当前实现下，我们已经补了更贴近真实在线负载的 benchmark：

- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers`

这组 benchmark 的语义是：

- 先在 `3` 节点 / `7` 节点线性拓扑上建立大量**真实已登录 WebSocket 会话**。
- 这些会话保持后台轮询和正常登录态。
- 再测一条跨节点 `transient` 消息的端到端延迟。

### 1.1 当前已验证结果

以下结果来自 **2026-04-26** 的本地基准采样，运行环境与 [performance-baseline.md](/root/dev/sys/turntf/turntf/docs/performance-baseline.md) 中的基线环境一致：

- CPU：`12th Gen Intel(R) Core(TM) i5-12400`
- `goos=linux`
- `goarch=amd64`
- 本轮在线容量 benchmark 暂时只跑 `SQLite` 场景，用于优先隔离“在线连接与会话模型”的开销，而不是存储引擎差异

关键结果：

| 场景 | 结果 | accept_ms/op | push_ms/op |
| --- | --- | ---: | ---: |
| `3-nodes / 1000-online / 256B` | 通过 | `0.1137` | `0.2099` |
| `3-nodes / 5000-online / 256B` | 通过 | `0.0971` | `0.2264` |
| `7-nodes / 5000-online / 256B` | 通过 | `0.0717` | `0.2873` |
| `3-nodes / 10000-online / 256B` | 未在 `2m` 内进入稳态 | - | - |
| `7-nodes / 10000-online / 256B` | 未在 `2m` 内进入稳态 | - | - |

### 1.2 当前可对外使用的保守口径

- 当前实现下，`5000` 总在线用户/集群可以视为**已验证稳定量级**。
- `10000` 总在线用户/集群目前**不能视为已验证稳定量级**。
- 如果没有进一步优化，我不建议直接承诺五位数以上的稳态在线连接能力。

这里说的“在线用户”是更接近真实生产的定义：

- 已完成登录
- 持有真实 WebSocket 连接
- 后台会继续跑当前实现里的 push loop
- 集群仍然需要承受跨节点 transient 消息的正常端到端路径

## 2. 当前瓶颈

当前在线连接上限主要不是被 `transient` 消息写入本身卡住，而是被“每个已登录会话的后台维护成本”卡住。

### 2.1 每个会话都有 1Hz 事件轮询

当前 `clientWSSession.pushLoop()` 为每个已登录客户端启动一个独立 ticker，每秒执行一次：

- `ListEvents(afterSequence, 100)`
- 对结果逐条做可见性判断
- 把可见的持久消息推给客户端

代码位置：

- [client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go:12)
- [client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go:30)

这意味着在线连接成本近似是：

- `O(连接数)` 个 goroutine
- `O(连接数)` 个 ticker
- `O(连接数)` 次/秒的 event log 轮询

对于“纯 transient 即时消息”场景，这部分成本几乎全部是额外负担。

### 2.2 登录后的 push 路径默认会从旧事件开始追

当前 WebSocket 登录后会先：

- `pushInitialMessages()`
- 然后启动 `pushLoop()`

代码位置：

- [client_session.go](/root/dev/sys/turntf/turntf/internal/api/client_session.go:123)
- [client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go:17)

而 `pushLoop()` 默认从 `afterSequence = 0` 开始，[client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go:31)。

这会导致两个问题：

- 新连接刚登录时，即使它只关心 transient 消息，也会扫描一遍当前事件流。
- 大规模连接爬升时，前面先登录的连接可能已经在空转轮询，后面还在建连，整体进入稳态的时间被显著拉长。

### 2.3 本地在线用户表是“动态遍历 sessions map”

当前本地在线用户列表来自 `HTTP.ListLoggedInUsers()`，它会：

- 持 `sessionsMu.RLock()`
- 遍历 `map[UserKey]map[*clientWSSession]struct{}`
- 构造并排序结果 slice

代码位置：

- [http.go](/root/dev/sys/turntf/turntf/internal/api/http.go:866)

这对“偶发管理查询”还可以接受，但当在线连接数变大时：

- 查询成本与在线用户数线性相关
- 列表构造和排序都会产生额外分配
- 远端节点查询在线用户时，也会放大这条路径的成本

### 2.4 本地 transient 投递仍依赖全局 session 桶复制

当前 `HTTP.ReceiveTransientPacket()` 会：

- 在 `sessionsMu` 下找到目标用户会话桶
- 把桶里的 session 复制到新 slice
- 释放锁后逐个 `pushPacket`

代码位置：

- [http.go](/root/dev/sys/turntf/turntf/internal/api/http.go:841)

这在单目标少会话时问题不大，但用户量继续增长后：

- 登录/登出与 transient 投递会在同一把锁上竞争
- 每次投递都要分配一个新的 `sessions` slice

### 2.5 连接模型没有区分“transient-only 客户端”和“持久消息客户端”

当前客户端只要登录到 `/ws/client`，就会自动获得：

- 持久消息初始补推
- 持久事件轮询
- transient 包直推

也就是说，**纯 transient** 场景仍然被迫承担持久消息那一整套后台成本。

这在语义上是兼容的，但在容量上非常不划算。

## 3. 优化方向总览

建议把优化分成三批推进。

### 第一批：先去掉最明显的“空转成本”

- 工作点 1：引入 `transient-only` 会话模式
- 工作点 2：普通登录默认从“当前事件水位”开始，而不是从 `0` 开始扫
- 工作点 3：本地在线用户 registry 常驻化，避免每次全量遍历
- 工作点 4：session registry 分片，降低锁竞争

目标：

- 在不大改 wire protocol 的前提下，把 `5000` 稳态在线拉到更高。
- 优先争取让 `10000` 在线能够在 benchmark 的稳态窗口内通过。

### 第二批：从“每连接轮询”改成“节点级共享分发”

- 工作点 5：把 per-session `pushLoop` 改成节点级 shared event tailer
- 工作点 6：把持久消息可见性判断做缓存和索引化
- 工作点 7：为 transient 和 persistent 拆分独立客户端路径

目标：

- 从根上把 `O(连接数)` 的轮询模型改掉。
- 为更高数量级在线连接打基础。

### 第三批：架构层在线容量扩展

- 工作点 8：接入层与 cluster 节点角色拆分
- 工作点 9：按用户或 session 做 sticky placement
- 工作点 10：在线连接分级、配额与背压

目标：

- 让“连接数扩展”与“节点间复制/转发”解耦。
- 解决单节点既做存储又做 API/连接承载的天然上限。

## 4. 第一批工作点

## 4.1 工作点 1：引入 `transient-only` 会话模式

### 目标

- 让纯即时消息客户端不再承担持久消息补推和 event log 轮询成本。

### 当前问题

- 只要登录 `/ws/client`，就会自动启动 `pushInitialMessages()` 和 `pushLoop()`。
- 对于只发送/接收 transient 包的客户端，这两步完全不是必须。

### 具体改动

- 在 `LoginRequest` 中新增一个轻量字段，表达客户端能力或订阅模式，例如：
  - `transient_only = true`
  - 或 `subscription_mode = TRANSIENT_ONLY | FULL`
- 当处于 `transient_only` 模式时：
  - 跳过 `pushInitialMessages()`
  - 不启动 `pushLoop()`
  - 保留 `PacketPushed` 和 RPC 能力
- 保持默认行为不变；只有显式声明时才走轻量模式。

### 受影响模块

- [proto/client.proto](/root/dev/sys/turntf/turntf/proto/client.proto)
- [client_session.go](/root/dev/sys/turntf/turntf/internal/api/client_session.go)
- [client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go)
- `turntf-js` / `turntf-go` 客户端

### 预期收益

- 对纯 transient 业务，在线连接后台成本可显著下降。
- 这是最有可能直接把 `10000` 在线 benchmark 拉回稳态窗口的改动。

### 风险

- 需要明确协议语义，避免客户端误以为还能收到持久消息推送。

## 4.2 工作点 2：普通登录默认从“当前事件水位”开始

### 目标

- 避免新连接从 `afterSequence = 0` 开始扫描全历史事件。

### 当前问题

- `pushLoop()` 的起始游标是 `0`，[client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go:31)。
- 这会把“首次登录成本”与“稳态在线成本”绑在一起。

### 具体改动

- 登录成功后，给 session 初始化一个默认 `afterSequence`：
  - 如果客户端提供了 `seen_messages`，按现有语义处理。
  - 如果客户端没有提供任何游标，默认从 `Store.LastEventSequence()` 开始。
- 这样“没有历史补推需求”的客户端不会扫历史 event log。
- 若仍需要历史持久消息，可以继续保留 `pushInitialMessages()` 路径，或在协议层增加显式的“登录后补历史”开关。

### 受影响模块

- [client_session.go](/root/dev/sys/turntf/turntf/internal/api/client_session.go)
- [client_push.go](/root/dev/sys/turntf/turntf/internal/api/client_push.go)

### 预期收益

- 明显降低大规模连接爬升时的进入稳态时间。
- 对 5k+ 在线 benchmark 会有直接帮助。

### 风险

- 要非常明确“默认不补历史”的兼容性边界，避免现有依赖登录补历史的客户端被悄悄改语义。

## 4.3 工作点 3：本地在线用户 registry 常驻化

### 目标

- 让本地在线用户查询从“遍历所有 session”变成“读现成 registry”。

### 当前问题

- `HTTP.ListLoggedInUsers()` 每次都要全量遍历 [http.go](/root/dev/sys/turntf/turntf/internal/api/http.go:866)。

### 具体改动

- 在 `HTTP` 内维护一个常驻 presence registry：
  - `onlineUsers map[UserKey]onlineUserState`
  - `onlineUserCount int64`
- 登录成功时注册，连接关闭时注销。
- `ListLoggedInUsers()` 直接读取 registry 并返回。
- 后续如有需要，可再增加：
  - `ListLoggedInUsersCount()`
  - `IterLoggedInUsers(func(...))`

### 预期收益

- 降低在线用户查询和容量 benchmark 的辅助开销。
- 为后续 cluster presence 同步打基础。

### 风险

- 需要保证重复连接、重复登录、异常断开下的计数一致性。

## 4.4 工作点 4：session registry 分片

### 目标

- 降低登录/登出和 transient 本地投递之间的锁竞争。

### 当前问题

- 目前所有客户端 session 共用 `sessionsMu`，[http.go](/root/dev/sys/turntf/turntf/internal/api/http.go:28)。

### 具体改动

- 把 `sessions` 改为固定分片，例如 `256` 个 shard。
- 每个 shard 单独持有：
  - `map[UserKey]map[*clientWSSession]struct{}`
  - `RWMutex`
- `registerClientSession`、`unregisterClientSession`、`ReceiveTransientPacket`、`ListLoggedInUsers` 全部按 shard 访问。

### 预期收益

- 降低高在线数下的热点锁竞争。
- 减少 transient 包本地投递时的暂停时间。

### 风险

- `ListLoggedInUsers()` 需要遍历所有 shard，代码会稍复杂。

## 5. 第二批工作点

## 5.1 工作点 5：用节点级 shared event tailer 替代 per-session `pushLoop`

### 目标

- 把“每个连接每秒一次 `ListEvents`”改成“每个节点一个事件 tailer，再按订阅关系 fanout”。

### 当前问题

- 这是当前在线容量的最大结构性瓶颈。

### 具体改动

- 新增节点级 dispatcher：
  - 后台只有一个或少量 goroutine 追 event log
  - 把新事件投递给订阅该类消息的本地 session
- session 本身不再持有独立 ticker。
- 对 direct / broadcast / channel 分别建立更细粒度的订阅索引：
  - direct: `recipient -> sessions`
  - broadcast: 全局广播会话集
  - channel: `channel -> subscriber sessions`

### 预期收益

- 轮询复杂度从 `O(连接数)` 降到接近 `O(节点数)`。
- 这是一项真正能把在线连接上限从几千拉到更高量级的结构性优化。

### 风险

- 实现复杂度明显高于第一批。
- 需要谨慎处理 direct / channel / broadcast 的授权语义。

## 5.2 工作点 6：持久消息可见性判断缓存化

### 目标

- 降低 shared tailer 或现有 push path 中的授权判断成本。

### 当前问题

- `canSeeMessage()` 会触发：
  - `IsMessageBlockedByBlacklist`
  - `GetUser`
  - `IsSubscribedToChannel`
- 这些在高 fanout 下会很贵。

### 具体改动

- 为 session 增加短 TTL 可见性缓存：
  - `channel subscription cache`
  - `sender blacklist cache`
  - `target role cache`
- 失效策略先采用短 TTL，不先做复杂主动失效。

### 预期收益

- 降低 persistent 消息推送在高在线数下的 DB/存储读放大。

### 风险

- 会引入“短 TTL 近实时”而不是“严格实时”的授权视图。

## 5.3 工作点 7：拆分 transient 与 persistent 客户端路径

### 目标

- 在协议和服务实现上彻底承认“在线即时消息”和“持久消息补推”是两条不同产品路径。

### 具体改动

- 提供两个明确的接入模式：
  - `full client stream`
  - `transient realtime stream`
- 两者都可以复用现有登录语义，但服务端行为不同。
- 后者只保留：
  - 登录
  - transient send / receive
  - ping/pong
  - 必要的 presence 能力

### 预期收益

- 把“纯 transient 用户”从持久事件系统里解耦出来。
- 是长期在线容量优化里收益最高的一类方案。

## 6. 第三批工作点

## 6.1 工作点 8：接入层与 cluster 节点角色拆分

### 目标

- 不再让每个节点既承担存储与 mesh，又承担大量终端 WS 连接。

### 具体改动

- 引入 dedicated API edge 节点：
  - 只承载客户端连接和认证
  - 内部把 transient/persistent 请求路由到 cluster 节点
- cluster 节点主要承载：
  - store
  - mesh runtime
  - replication / snapshot / query

### 预期收益

- 在线连接扩展和数据面扩展可以分别规划。
- 更适合走到五位数甚至更高在线连接。

### 风险

- 架构复杂度明显上升。
- 需要重新定义 edge 与 cluster 之间的内部协议。

## 6.2 工作点 9：用户或 session 粘性放置

### 目标

- 让某个用户的大部分连接与消息尽量停留在同一入口节点，减少跨节点跳转。

### 具体改动

- 对登录用户做：
  - consistent hash
  - sticky routing
  - 或 gateway affinity

### 预期收益

- 降低跨节点 transient 比例。
- 在线连接越大，粘性越有价值。

## 6.3 工作点 10：在线连接分级、配额与背压

### 目标

- 避免单节点在连接风暴或异常客户端下被拖死。

### 具体改动

- 增加：
  - 每节点最大客户端连接数
  - 每用户最大并发连接数
  - 每连接最大待发送队列
  - 短时连接爬升速率限制

### 预期收益

- 提高系统在接近上限时的可控性。
- 让“上限附近的退化”可预期，而不是直接雪崩。

## 7. 推荐执行顺序

建议按下面顺序推进：

1. `transient-only` 会话模式
2. 登录游标从“当前事件水位”起步
3. 本地在线用户 registry 常驻化
4. session registry 分片
5. shared event tailer
6. 可见性缓存
7. transient / persistent 路径拆分
8. 架构级角色拆分与 sticky routing

这个顺序的考虑是：

- 前四项基本不需要改整体架构，风险相对可控。
- 前两项最可能直接改善 `10000` 在线 benchmark 的稳态通过率。
- shared tailer 是中期真正决定上限的工作，但应该在我们先把协议和轻量模式收敛之后再做。

## 8. 验收口径

建议把“在线连接容量”分成两层验收。

### 8.1 稳态在线验收

基于 `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers`：

- `3-nodes / 10000-online / 256B` 可以在 `2m` 内进入稳态并完成 1 次测量
- `7-nodes / 10000-online / 256B` 可以在 `2m` 内进入稳态并完成 1 次测量
- `push_ms/op < 0.5ms`

### 8.2 后续目标

在第一批完成后，再尝试把目标提高到：

- `3-nodes / 15000-online`
- `7-nodes / 20000-online`

如果 shared tailer 落地，再重新设更高目标。

## 9. 当前最推荐的答案

如果只让我从这份文档里选一个“最值得先做”的优化项，我会选：

- **工作点 1：`transient-only` 会话模式**

原因：

- 它最直接命中当前测试场景。
- 它不需要先做大的架构重写。
- 它能把纯 transient 业务从持久消息轮询里解放出来。
- 它最有希望让当前 `10000` 在线 benchmark 从“不稳定”变成“可稳定进入稳态”。

如果让我选一个“长期最重要”的优化项，我会选：

- **工作点 5：shared event tailer**

原因：

- 这是当前 per-session 轮询模型的根本替代方案。
- 它决定的是系统的长期在线连接天花板，而不是某个局部 patch 的小幅改善。
