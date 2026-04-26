# 吞吐优化落地方案

本文档把“如何提高当前系统吞吐”整理为可执行的工作清单，按每个工作点组织。它基于当前代码路径和已采集的性能基线，不是泛泛而谈的优化建议。

当前性能基线见 [performance-baseline.md](/root/dev/sys/turntf/turntf/docs/performance-baseline.md)。

## 1. 背景与目标

当前系统的吞吐瓶颈主要来自三类路径：

- `cluster` 层的复制与恢复控制面偏“细粒度”，每条事件、每次 `Ack`、每次 digest 检查都单独推进，放大了多节点写放大成本。
- `store` 层在 `Pebble` 模式下仍然保留了大量 SQLite 热路径，而且 `Pebble` 写入大量使用同步提交，导致持久写入成本偏高。
- `read` 路径仍以“临时拼装结果”为主，例如消息列表需要聚合 direct、broadcast、subscription，再做去重与排序。

本文档目标：

- 提高持久写入吞吐。
- 降低多节点复制和恢复路径的控制面成本。
- 提高典型读路径吞吐。
- 保持默认语义边界不被悄悄改变；任何会影响一致性或本地可见性的改动都单列为独立工作点。

不在本轮优先级内的方向：

- 先优化 HTTP handler 的细枝末节。
- 先扩 mixed transport 对比。
- 先做认证路径专项优化。

## 2. 优先级与执行顺序

建议分三批推进。

### 第一批：直接影响写吞吐的低风险工作

- 工作点 1：复制批量化与 `Ack` 合并
- 工作点 2：snapshot digest 去抖与脏标记调度
- 工作点 3：Pebble group commit 与同步策略收敛
- 工作点 4：Pebble projection 分片锁与延迟裁剪

目标：

- 先把当前“每条消息跨节点持久复制”的基线显著抬高。
- 不改外部 API，不引入语义变化。

### 第二批：继续压缩本地热路径

- 工作点 5：Pebble 模式热点元数据去 SQLite 化
- 工作点 6：本地消息投影异步化
- 工作点 7：收件箱预聚合读投影

目标：

- 降低单节点写放大。
- 明显改善 `Pebble` 下的读写吞吐差异。

### 第三批：架构级吞吐扩展

- 工作点 8：在线用户查询短 TTL 缓存
- 工作点 9：sticky write / shard owner
- 工作点 10：持久消息与瞬时消息通道拆分

目标：

- 提升跨节点查询和写入规模上限。
- 解决“任意节点可写 + 强持久复制”天然带来的吞吐上限。

## 3. 工作点 1：复制批量化与 `Ack` 合并

### 目标

- 把当前“单事件复制”改成“小批量复制”。
- 降低每条消息对应的 envelope、路由、`Ack` 和日志开销。

### 当前瓶颈

- `broadcastEvent()` 当前按单个事件直接发送复制包，[manager_replication.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_replication.go:14)。
- `handleEventBatch()` 会逐条 `ApplyReplicatedEvent()`，然后按本批次发送一次 `Ack`，[manager_replication.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_replication.go:173)。
- 当前虽然 wire protocol 支持 `EventBatch.Events`，但源端没有真正利用“批量化”的能力。

### 具体改动

- 在 `Manager.publishCh` 下游增加按 peer 的复制聚合器。
- 聚合器按“单个 origin + 单个 peer”维度收集事件，触发条件为：
  - 达到固定事件数上限。
  - 达到固定 payload 字节上限。
  - 达到固定时间窗口上限。
- 第一版建议使用包内常量，不暴露配置：
  - `maxBatchEvents = 32`
  - `maxBatchBytes = 64KiB`
  - `maxBatchDelay = 2ms`
- 保持 wire protocol 不变，仍然发送 `EventBatch`，只是让 `Events` 真正承载多条事件。
- `Ack` 仍按 origin 维度返回“已应用到哪个 event_id”，但合并到批量 flush 后发送。

### 受影响模块

- [internal/cluster/manager_replication.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_replication.go)
- [internal/cluster/manager.go](/root/dev/sys/turntf/turntf/internal/cluster/manager.go)
- 现有 `cluster` benchmark 与恢复 benchmark

### 验收标准

- `BenchmarkMeshReplicationPebbleLinear3Nodes` 的 `ns/op`、`ack_ms/op` 明显下降。
- `Ack` 语义不变，现有复制回归测试全部通过。
- 不引入“同一批次内事件乱序应用”。

### 风险与回滚

- 风险：批量窗口会增加单条消息尾延迟。
- 控制：窗口严格限制在毫秒级，且支持立即 flush 的 fast path。
- 回滚：保留单事件直发实现，通过内部开关快速切回。

## 4. 工作点 2：snapshot digest 去抖与脏标记调度

### 目标

- 降低复制收尾时的控制面噪声。
- 避免每批事件后都尝试发送 digest。

### 当前瓶颈

- `handleEventBatch()` 在没有 pending pull 时会继续走 `sendSnapshotDigest()`，[manager_replication.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_replication.go:223)。
- 这会让高频复制下出现大量 digest 尝试，即使实际没有需要修复的分片。

### 具体改动

- 为每个 peer 增加 snapshot dirty 标记。
- 只在以下场景把 peer 标为 dirty：
  - 完成一轮 pull/catchup。
  - 应用了 snapshot chunk。
  - 本地进入“可能存在分片差异”的状态。
- 新增定时调度器，每个 peer 在固定最小间隔内最多发送一次 digest。
- 第一版建议：
  - `snapshotDigestMinInterval = 250ms`
  - `snapshotDigestImmediateAfterRepair = true`

### 受影响模块

- [internal/cluster/snapshot.go](/root/dev/sys/turntf/turntf/internal/cluster/snapshot.go)
- [internal/cluster/manager_replication.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_replication.go)

### 验收标准

- 复制 benchmark 的 `allocs/op` 和 `ns/op` 有可见下降。
- `BenchmarkMeshSnapshotRepairPebbleLinear3Nodes` 不出现功能回退。
- snapshot repair 仍然能在恢复路径下自动触发。

### 风险与回滚

- 风险：digest 频率过低会延长分片修复的发现时间。
- 控制：保留“恢复路径后立即发送一次”的特例。
- 回滚：仅回退调度器，保留 dirty 标记代码。

## 5. 工作点 3：Pebble group commit 与同步策略收敛

### 目标

- 降低 `Pebble` 模式下每次写入的 fsync 成本。
- 让高频写入不再被“每条消息一次同步提交”主导。

### 当前瓶颈

- `pebbleEventLogRepository.appendStored()` 在多个 `Set` 和 `Commit` 上都使用 `pebble.Sync`，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:86)。
- `trimMessagesForUser()` 删除索引也使用 `pebble.Sync`，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:539)。
- 当前 `Pebble` 写路径吞吐明显落后于 SQLite 单点路径，基线已经体现了这一点。

### 具体改动

- 统一写入规则：
  - 单个 `Batch` 内部的 `Set/Delete` 一律不带 `Sync`。
  - 只在 `Batch.Commit()` 决定是否 `Sync`。
- 引入写入协调器：
  - 聚合多个事件日志写入与投影索引写入。
  - 在小时间窗口内合并到一个 commit。
- 第一版仍不暴露 public config，采用内部常量：
  - `groupCommitMaxOps = 64`
  - `groupCommitMaxDelay = 2ms`
- 对需要强制刷盘的路径保留 `forceSync` 能力，但默认普通消息写入走 group commit。

### 受影响模块

- [internal/store/pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go)
- `internal/store` 中 Pebble event log 实现

### 验收标准

- `BenchmarkStoreCreateMessage/*/pebble` 吞吐显著提升。
- `BenchmarkStorePruneEventLogOnce/*/pebble` 不出现异常回退。
- 崩溃恢复语义仍符合当前 durable event log 承诺。

### 风险与回滚

- 风险：极端崩溃时会增大“已返回但尚未真正 sync”的窗口。
- 控制：第一版仅在明确接受的路径上启用 group commit。
- 回滚：保留 `Commit(pebble.Sync)` 直写模式。

## 6. 工作点 4：Pebble projection 分片锁与延迟裁剪

### 目标

- 让不同用户的消息写入不再互相阻塞。
- 降低“每次写入就全量裁剪”的代价。

### 当前瓶颈

- `pebbleMessageProjectionRepository` 使用全局 `mu`，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:30)。
- `ApplyMessageCreated()` 在全局锁下执行 `messageExists`、`putMessage` 和 `trimMessagesForUser`，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:271)。
- `trimMessagesForUser()` 每次都会列出该用户全部消息并裁剪，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:530)。

### 具体改动

- 把全局锁改成固定数量的分片锁，按 `recipient node_id/user_id` hash。
- `messageExists` 与 `putMessage` 仍在同一分片锁里，保证幂等。
- 把“写后立即 trim”改为：
  - 先记录该用户为 dirty。
  - 由后台 trim worker 批量处理。
- 第一版建议：
  - `projectionLockShards = 256`
  - `trimWorkerPeriod = 50ms`
  - `trimImmediateThreshold = windowSize + 32`

### 验收标准

- `BenchmarkStoreCreateMessage/*/pebble` 的 `ns/op` 和 `allocs/op` 下降。
- 多用户并发写入压测时，不再被单个全局锁卡死。
- `message_window_size` 语义保持不变。

### 风险与回滚

- 风险：延迟 trim 会让短时间窗口内的消息数大于 N。
- 控制：只允许短暂超窗，读取时仍按最终排序截断。
- 回滚：保留同步 trim 实现。

## 7. 工作点 5：Pebble 模式热点元数据去 SQLite 化

### 目标

- 去掉 `Pebble` 模式下最主要的单点串行瓶颈。
- 让“事件日志和消息投影在 Pebble，但热点元数据还在 SQLite”这一混合状态结束。

### 当前瓶颈

- `Store.Open()` 无论什么 engine 都创建 SQLite 连接，并把连接数固定为 `1`，[store.go](/root/dev/sys/turntf/turntf/internal/store/store.go:258)。
- `CreateMessage()` 的用户检查、黑名单检查、`message_sequence_counters` 更新都仍在 SQLite 事务里，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:21)。

### 第一阶段迁移目标

- `message_sequence_counters`
- `origin_cursors`
- `peer_ack_cursors`
- `pending_projections`

不在第一阶段迁移的内容：

- `users`
- `subscriptions`
- `blacklists`
- `discovered_peers`

### 具体改动

- 为上述元数据增加 Pebble repository 实现。
- 在 `EnginePebble` 下优先走 Pebble repository，SQLite 只保留尚未迁移的实体。
- 保持外部接口不变，迁移只发生在 repository 选择层。

### 验收标准

- `BenchmarkStoreCreateMessage/*/pebble` 继续下降。
- `cluster` 复制 benchmark 不因 cursor/ack 路径重复跨 SQLite 而被卡住。
- 现有 `store`、`cluster` 回归测试通过。

### 风险与回滚

- 风险：元数据 repository 双栈期复杂度明显升高。
- 控制：按实体分阶段迁移，不一次性替换所有 SQLite 元数据。

## 8. 工作点 6：本地消息投影异步化

### 目标

- 把“用户请求线程同时负责 durable event log 和 read projection”拆开。
- 进一步提高写吞吐。

### 当前瓶颈

- `CreateMessage()` 在事务提交后同步调用 `projectMessageEvent()`，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:76)。
- 这意味着写请求会直接承担本地 projection 成本。

### 语义影响

- 这是一个会影响“本地写后读立即可见性”的工作点。
- 它不应与前几项低风险优化捆绑上线。

### 具体改动

- 写请求只保证：
  - durable event log 已提交
  - 本地 message identity 已生成
- projection 进入后台 worker 队列。
- 读路径在 projection 尚未追平时，可选择：
  - 继续读取已投影数据，接受短暂延迟。
  - 或做“projection pending”补偿查询。

### 验收标准

- `BenchmarkStoreCreateMessage` 与 `BenchmarkHTTPCreateMessageAuthenticated` 有显著下降。
- 文档明确声明本地读可见性的变化。
- 不破坏复制语义和恢复语义。

### 风险与回滚

- 风险：语义变化最大，业务可能依赖“写后立即可读”。
- 建议：单独版本推进，且默认关闭。

## 9. 工作点 7：收件箱预聚合读投影

### 目标

- 提升 `ListMessagesByUser` 和对应 HTTP 接口的吞吐。
- 避免每次读取都重新拼装 direct、broadcast 和 subscription 消息。

### 当前瓶颈

- `ListMessagesByUser()` 当前会读取 direct、broadcast、subscription 三类来源，再做去重和排序，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:294)。

### 具体改动

- 新增“用户 inbox 投影”：
  - 每条 direct message 直接写入 recipient inbox。
  - channel / broadcast message 在 fanout 时写入订阅者 inbox。
- `ListMessagesByUser()` 优先读 inbox。
- 原始 message 投影继续保留，供恢复、诊断和重建 inbox 使用。

### 验收标准

- `BenchmarkStoreListMessagesByUser` 与 `BenchmarkHTTPListMessagesByUserAuthenticated` 显著下降。
- 用户看到的排序、去重和黑名单边界不发生语义回退。

### 风险与回滚

- 风险：写放大会上升，尤其是高 fanout channel。
- 建议：仅在读远大于写的场景启用。

## 10. 工作点 8：在线用户查询短 TTL 缓存

### 目标

- 提升 `QueryLoggedInUsers` 多跳查询吞吐。
- 降低热点节点被重复查询时的跨节点控制面压力。

### 当前瓶颈

- 当前 `QueryLoggedInUsers` 是实时多跳 RPC，没有缓存层，基线已显示它的吞吐低于单节点本地读。

### 具体改动

- 在源节点为远端 `node_id` 增加短 TTL 查询缓存。
- 第一版建议：
  - `ttl = 250ms`
  - `maxEntries = 1024`
- 对明确失败的响应也做极短 negative cache，防止热点节点被持续重试。

### 验收标准

- `BenchmarkMeshQueryLoggedInUsersPebbleLinear` 明显下降。
- 250ms 内允许结果略旧，但不得越权或串节点。

### 风险与回滚

- 风险：在线用户列表会变成“短暂近实时”，不再是严格实时。
- 控制：仅缓存远端查询，不缓存本地 provider。

## 11. 工作点 9：sticky write / shard owner

### 目标

- 从架构层提升持久写入吞吐。
- 降低“任意节点可写”带来的复制和冲突处理成本。

### 当前瓶颈

- 当前每个写节点都可能成为 origin，再通过复制、`Ack`、快照与修复收敛。
- 这是当前架构最大的吞吐上限来源，不是单个函数级优化能完全解决的。

### 具体改动

- 对用户或 channel 引入 owner 规则。
- 默认把持久写请求转发到 owner 节点执行。
- 非 owner 节点保留：
  - 读能力
  - 瞬时包能力
  - owner 不可达时的降级策略

### 验收标准

- 持久写 benchmark 与真实压测的节点容量上限明显提升。
- 冲突与反熵压力显著下降。

### 风险与回滚

- 风险：会弱化“任意节点可写”的原始承诺。
- 建议：作为单独架构版本推进，不与当前语义版本混发。

## 12. 工作点 10：持久消息与瞬时消息通道拆分

### 目标

- 让不需要 durable event log 的流量不要占用持久复制路径。
- 提升整体系统可承载吞吐。

### 当前瓶颈

- 当前持久消息和瞬时消息共享大量 manager / mesh / store 热路径设计思路，尽管语义已经区分，但优化层面还不够彻底。

### 具体改动

- 持久消息继续走 event log + projection + replication。
- 瞬时消息尽量只走：
  - route
  - relay
  - in-memory retry
- 单独做 observability、排队和限流。

### 验收标准

- transient throughput 可以单独提升，不再被 durable path 拖累。
- 持久消息的稳定性和瞬时消息的吞吐目标不再互相牵制。

## 13. 推荐实施顺序与交付物

### Phase A：两周内可交付

- 工作点 1
- 工作点 2
- 工作点 3
- 工作点 4

交付物：

- 吞吐优化第一批代码
- 更新后的 benchmark 与基线文档
- 新增 metrics：
  - replication batch size
  - group commit flush latency
  - snapshot digest skipped / sent
  - projection trim backlog

### Phase B：一到两个版本内交付

- 工作点 5
- 工作点 7
- 工作点 8

交付物：

- `Pebble` 模式下更纯粹的本地热路径
- 更强的读吞吐基线

### Phase C：单独立项

- 工作点 6
- 工作点 9
- 工作点 10

原因：

- 这些工作点要么改变语义，要么改变架构边界，不适合与“低风险提吞吐”一并上线。

## 14. 验证与度量

每完成一个工作点，都至少执行以下验证：

- 回归：
  - `go test ./internal/cluster ./internal/store ./internal/api -count=1`
- `cluster` benchmark：
  - `go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1`
- `store/api` benchmark：
  - `go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP)' -benchmem -count=1`

每批优化至少要观察这些指标变化：

- `BenchmarkMeshReplicationPebbleLinear3Nodes`
- `BenchmarkMeshSnapshotRepairPebbleLinear3Nodes`
- `BenchmarkMeshTruncatedCatchupRepairPebble`
- `BenchmarkStoreCreateMessage`
- `BenchmarkStoreListMessagesByUser`
- `BenchmarkStorePruneEventLogOnce`
- `BenchmarkHTTPCreateMessageAuthenticated`
- `BenchmarkHTTPListMessagesByUserAuthenticated`

## 15. 当前建议

如果只选 3 个最值得先做的工作点，建议顺序是：

1. 工作点 1：复制批量化与 `Ack` 合并
2. 工作点 3：Pebble group commit 与同步策略收敛
3. 工作点 4：Pebble projection 分片锁与延迟裁剪

原因：

- 这 3 项最直接打在当前写吞吐热点上。
- 它们不要求修改对外 API。
- 风险相对可控，且可以直接通过现有 benchmark 证明收益。
