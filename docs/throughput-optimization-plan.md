# 吞吐优化落地方案

本文档记录当前 `turntf` 吞吐优化的已落地基线、仍然存在的真实瓶颈，以及后续 backlog。文件名沿用早期 `plan` 命名，但正文以当前实现和现有 benchmark 集合为准，不再把已经完成的优化继续描述成“未来工作”。

当前性能基线见 [performance-baseline.md](performance-baseline.md)。

## 1. 适用范围

在阅读这份方案前，需要先明确当前 benchmark 与 mesh / mixed transport 的覆盖边界：

- 持久写、复制、snapshot repair、truncated catchup 的主基线，当前仍以纯 `WebSocket` 线性 mesh 为准：
  - [mesh_benchmark_test.go](../internal/cluster/mesh_benchmark_test.go)
  - [mesh_recovery_benchmark_test.go](../internal/cluster/mesh_recovery_benchmark_test.go)
- 服务端 transient 点对点吞吐基线已经覆盖纯 `WebSocket`、纯 `libp2p`，以及 `WebSocket -> libp2p` bridge；带 `zeromq` build tag 时再追加纯 `ZeroMQ` 和 `WebSocket -> ZeroMQ` bridge：
  - [mesh_point_to_point_throughput_benchmark_test.go](../internal/cluster/mesh_point_to_point_throughput_benchmark_test.go)
  - [mesh_point_to_point_throughput_benchmark_zeromq_test.go](../internal/cluster/mesh_point_to_point_throughput_benchmark_zeromq_test.go)
- 客户端 transient 点对点吞吐当前覆盖纯 `WebSocket/libp2p` 与纯 `ZeroMQ`，但不包含 mixed bridge：
  - [client_point_to_point_throughput_benchmark_test.go](../internal/api/client_point_to_point_throughput_benchmark_test.go)
  - [client_point_to_point_throughput_benchmark_zeromq_test.go](../internal/api/client_point_to_point_throughput_benchmark_zeromq_test.go)
  - [client_point_to_point_throughput_zeromq_client_benchmark_test.go](../internal/api/client_point_to_point_throughput_zeromq_client_benchmark_test.go)
- mixed transport bridge 当前只承载 `control_critical`、`control_query`、`transient_interactive`；`replication_stream` 和 `snapshot_bulk` 不会跨 bridge 转发，因此 mixed transport 的 transient 结果不能直接外推到持久复制吞吐：
  - [libp2p-plan.md](libp2p-plan.md)
  - [mesh_mixed_transport_integration_test.go](../internal/cluster/mesh_mixed_transport_integration_test.go)
  - [mesh_mixed_transport_zeromq_integration_test.go](../internal/cluster/mesh_mixed_transport_zeromq_integration_test.go)

## 2. 当前已经落地的吞吐优化基线

### 2.1 复制控制面不再是“单事件直推 + 每次都发 digest”

早期版本把“复制批量化与 `Ack` 合并”列为第一优先级，但当前代码已经完成这条主线：

- `publishLoop` 会把本地事件送入复制批处理器，并按 `maxBatchDelay` 周期性 flush，而不是直接把每条事件逐条广播：
  - [manager_lifecycle.go](../internal/cluster/manager_lifecycle.go)
  - [replication_batcher.go](../internal/cluster/replication_batcher.go)
- 复制批处理器已经按 `(peerID, originNodeID)` 聚合，并使用当前实现常量：
  - `maxBatchEvents = 32`
  - `maxBatchBytes = 64KiB`
  - `maxBatchDelay = 2ms`
- `handleEventBatch()` 已按整批应用后返回同一 `origin` 的连续 `Ack` 游标，而不是“每条事件一个 `Ack` 包”：
  - [manager_replication.go](../internal/cluster/manager_replication.go)

这意味着“工作点 1：复制批量化与 `Ack` 合并”已经从待实现项变成当前吞吐基线的一部分。

### 2.2 snapshot digest 已经改成 dirty 标记 + 定时 sweep

文档旧版把“snapshot digest 去抖与脏标记调度”列为未来工作，但当前实现已经具备：

- 空 pull 完成后只标记 peer 的 snapshot digest 为 dirty，不再在每批 steady-state push 后立刻尝试 digest：
  - [manager_replication.go](../internal/cluster/manager_replication.go)
- `snapshotDigestLoop` 会按固定 sweep 周期扫描 dirty peer，再受 `snapshotDigestMinInterval` 限流发送：
  - [manager_lifecycle.go](../internal/cluster/manager_lifecycle.go)
  - [snapshot.go](../internal/cluster/snapshot.go)
- 当前实现常量已经固定为：
  - `snapshotDigestMinInterval = 250ms`
  - `snapshotDigestSweepInterval = 25ms`

因此，“工作点 2：snapshot digest 去抖与脏标记调度”也已经落地，不应继续作为现状缺口描述。

### 2.3 Pebble 写路径已经有 group commit、profile 和本地批写

旧版方案里把“Pebble group commit 与同步策略收敛”当作下一步，但当前代码已经完成了三层基线能力：

- 通用 Pebble 写入已经由 [pebble_write_coordinator.go](../internal/store/pebble_write_coordinator.go) 负责 group commit，当前实现常量为：
  - `groupCommitMaxOps = 128`
  - `groupCommitMaxDelay = 5ms`
- 本地 `CreateMessage` 在 `Pebble` 后端下不再是“每次单条消息单独落盘”，而是由 [pebble_local_message_writer.go](../internal/store/pebble_local_message_writer.go) 做同 `sync_mode` 批写，单批最多 `128` 条。
- `Pebble` 已经具备 `balanced/throughput` profile 和 `no_sync/force_sync` 两类消息同步模式，对应 benchmark 名称也已经拆开：
  - [store_benchmark_test.go](../internal/store/store_benchmark_test.go)
  - [http_benchmark_test.go](../internal/api/http_benchmark_test.go)

因此，“当前 `Pebble` 写入主要被每条消息一次同步提交主导”已经不是准确的现状描述。

### 2.4 `Pebble` 范围已经明显扩大，不再只是事件日志 + 消息投影

旧版方案里把 “`message_sequence_counters` / `origin_cursors` / `peer_ack_cursors` / `pending_projections` 去 SQLite 化”写成第一阶段迁移目标，但这批能力现在已经在 `Pebble` 基线中：

- `message_sequence_counters`
- `peer_ack_cursors`
- `origin_cursors`
- `pending_projections`

对应实现位于：

- [store_backend.go](../internal/store/store_backend.go)
- [pebble_message_sequence.go](../internal/store/pebble_message_sequence.go)
- [pebble_metadata.go](../internal/store/pebble_metadata.go)

当前仍保留在 SQLite 的主要是：

- `users`
- `login_names`
- `subscriptions`
- `attachments`
- `blacklists`
- `user_metadata`

所以，当前 `Pebble` 的真实瓶颈已经从“核心元数据全都还在 SQLite”收缩为“权限 / 用户 / 附件 / metadata 相关热点仍依赖 SQLite 读写”。

### 2.5 消息投影已经有分片锁、后台 trim 和 inbox 快路径

旧版方案里的“Pebble projection 分片锁与延迟裁剪”“收件箱预聚合读投影”也不再是空白：

- 消息投影锁已经从全局锁变成 `256` 个 shard lock：
  - [pebble_message_state.go](../internal/store/pebble_message_state.go)
- trim 已经走后台 worker，而不是每次都同步全量裁剪：
  - `pebbleMessageTrimWorkerDelay = 25ms`
  - `pebbleMessageTrimWorkerMaxUsers = 64`
- 登录用户读消息时会优先走 inbox 投影，仅在候选不足时才回退到 legacy merge 路径：
  - [pebble_projection.go](../internal/store/pebble_projection.go)

这意味着当前读路径也不再是“完全依赖 direct / broadcast / subscription 临时拼装”的老状态。

### 2.6 `QueryLoggedInUsers` 已经是本地 mirror 读取，不是实时远程 RPC

旧版方案把“在线用户查询短 TTL 缓存”列为未来工作，但当前远端查询已经不是“每次多跳实时 RPC”：

- `QueryLoggedInUsers()` 对远端节点直接读取本地持有的 presence mirror snapshot：
  - [manager_queries.go](../internal/cluster/manager_queries.go)
- 测试已经明确验证第二次查询不会再次触发远端 provider：
  - [mesh_data_plane_integration_test.go](../internal/cluster/mesh_data_plane_integration_test.go)

所以这一块后续更合适的方向不是“再叠一层 TTL cache”，而是优化 mirror 的刷新、传播、payload 体积和观测。

## 3. 当前真实瓶颈

结合当前实现和 benchmark 集合，现阶段更准确的瓶颈判断如下。

### 3.1 `Pebble` 写路径仍被 SQLite 侧权限/实体读取牵制

尽管事件日志、消息投影、消息序号与 cursor 已经迁到 `Pebble`，但本地写入仍会经过这些 SQLite 相关热点：

- `GetUser()` / 登录用户校验
- 黑名单读取
- 订阅 / broadcast 可见性依赖
- 附件与 `user_metadata` 相关权限语义

对应入口主要在：

- [store_backend.go](../internal/store/store_backend.go)
- [projection.go](../internal/store/projection.go)
- [blacklists.go](../internal/store/blacklists.go)

因此，当前最真实的写热点已经不是“没有批写”，而是“批写之前和读可见性之后，仍有一圈 SQLite 语义依赖”。

### 3.2 本地批写已经存在，但热点用户和 `force_sync` 仍然昂贵

`Pebble` 本地消息写入已经按批处理，但当前仍有两个硬边界：

- 批次按连续 `sync_mode` 分段，`force_sync` 会打断 relaxed 批次；
- 同一热点用户仍会落在同一组 shard lock 与同一批次收敛点上。

这类成本会直接体现在：

- `BenchmarkStoreCreateMessage`
- `BenchmarkStoreCreateMessageSteadyState`
- `BenchmarkStoreCreateMessageParallel`
- `BenchmarkHTTPCreateMessageAuthenticated`

### 3.3 inbox 快路径已经存在，但高 fanout / broadcast 仍有 fallback 成本

当前登录用户读消息优先使用 inbox，但它不是“所有场景都完全命中”的终态：

- 候选不足时仍会回退 legacy merge；
- broadcast 仍要单独并入；
- 黑名单、订阅生效时间和 sender 角色过滤仍会参与可见性判断。

因此，`ListMessagesByUser` 的主要剩余空间已经从“完全没有读投影”变成“inbox 完整性、broadcast 合并和高 fanout 场景的额外放大”。

### 3.4 当前 benchmark 还有覆盖盲区

当前 benchmark 集合已经比旧版方案丰富很多，但仍有几个需要明确标出来的缺口：

- 持久复制 / snapshot repair / truncated catchup 的主基线仍是纯 `WebSocket` 线性 mesh，暂时没有纯 `libp2p` 或纯 `ZeroMQ` 的同类 durable benchmark。
- mixed transport benchmark 当前主要服务于 transient 数据面；bridge 不承载 `replication_stream` / `snapshot_bulk`，因此不能用 mixed transient 结果替代 durable 结论。
- 客户端点对点吞吐当前没有 mixed bridge 子场景。
- full-client 登录与在线稳态分发已经有独立 benchmark，但尚未形成可引用的正式历史样本表。
- `performance-baseline.md` 里已有的历史样本表格仍以旧的 `tmp` 采集为主；新增的 steady-state、parallel、客户端点对点、full-client 和 `zeromq` 子场景还缺更完整的历史对照表。

## 4. 现阶段建议的优化阶段

## Phase A：继续压缩当前真实热点

### 工作点 A1：继续推进写热点去 SQLite 化

优先级最高的不是重做 batcher，而是减少 `Pebble` 写路径前后的 SQLite 语义依赖。建议优先关注：

- 登录用户 / `login_name` 读取热点
- 黑名单判定
- 订阅 / attachment 读取
- `user_metadata` 与权限校验相关热点

目标不是一次性把所有关系型实体彻底 KV 化，而是优先把 `CreateMessage`、消息可见性和高频权限检查里的热路径收缩掉。

### 工作点 A2：补齐 durable benchmark 的非 WebSocket 基线

当前文档已经不能再写“先不扩 mixed transport 对比”。更准确的说法是：

- mixed transport transient benchmark 已经存在；
- 但 durable benchmark 还没有纯 `libp2p` / 纯 `ZeroMQ` 的对照基线；
- bridge 不承载 durable traffic，所以不应把 “mixed transport bridge benchmark” 当成 durable 替代。

这一阶段建议补齐：

- 纯 `libp2p` 多跳复制 benchmark
- 纯 `ZeroMQ` 多跳复制 benchmark（`-tags zeromq`）
- 纯 `libp2p` / `ZeroMQ` snapshot repair 与 truncated catchup benchmark

### 工作点 A3：把现有批写/批复制的可观测性补全

当前 batcher 和 group commit 已经存在，但文档旧版列出的观测项还没有形成稳定基线。建议补这些指标或 benchmark 对照：

- replication batch size / bytes
- snapshot digest dirty backlog / skip ratio
- local message batch size / `force_sync` 比例
- inbox fallback ratio

这一步的目标不是改变语义，而是让后续优化有更稳定的回归面。

## Phase B：继续优化读路径与 presence mirror

### 工作点 B1：提高 inbox 命中率，收缩 broadcast / subscription fallback

当前 inbox 已经是主快路径，后续优化重点应改成：

- 减少“候选不足 -> 回退 legacy merge”的频率
- 评估是否需要进一步预物化 broadcast / subscription 结果
- 降低高 fanout 场景下的额外分配和排序成本

### 工作点 B2：优化 logged-in users mirror，而不是再叠 TTL cache

因为远端 `QueryLoggedInUsers` 已经读取本地 mirror，后续更值得做的是：

- mirror 刷新粒度
- payload 大小
- fanout 频率
- 更新传播与观测

而不是在查询侧再加一层“短 TTL 缓存”。

## Phase C：仅在接受语义/架构变化时再考虑

### 工作点 C1：本地消息投影进一步异步化

这会影响“本地写后立即可见”的现有语义。当前 `Pebble` 路径已经把 event log 和 projection 放在同一批写里；如果要继续把 projection 移出主请求路径，应单独作为语义变更推进，而不是继续伪装成低风险吞吐优化。

### 工作点 C2：sticky write / shard owner

这仍然是可能显著提升持久写入上限的架构方案，但它会改变“任意节点可写”的边界，应独立立项，不适合与当前基线优化混发。

### 工作点 C3：进一步硬分 durable / transient 通道

当前瞬时包和持久消息已经在语义与 benchmark 上分开，但如果要继续把排队、限流、观测甚至 worker 彻底拆分，也属于架构级改动，而不是“修一个热点函数”。

## 5. benchmark 与优化点的对应关系

为了避免把 benchmark 结果读错，当前建议按下面的映射理解：

- 持久写 / 复制控制面：
  - `BenchmarkMeshReplicationPebbleLinear3Nodes`
  - `BenchmarkMeshSnapshotRepairPebbleLinear3Nodes`
  - `BenchmarkMeshTruncatedCatchupRepairPebble`
- 本地 `Pebble` 写路径：
  - `BenchmarkStoreCreateMessage`
  - `BenchmarkStoreCreateMessageSteadyState`
  - `BenchmarkStoreCreateMessageParallel`
  - `BenchmarkHTTPCreateMessageAuthenticated`
- 登录用户消息读路径：
  - `BenchmarkStoreListMessagesByUser`
  - `BenchmarkHTTPListMessagesByUserAuthenticated`
- 服务端 transient mesh 数据面：
  - `BenchmarkMeshTransientRoutePebbleLinear`
  - `BenchmarkMeshTransientPointToPointThroughput`
- 客户端 transient 数据面：
  - `BenchmarkClientWebSocketTransientSendMessageAuthenticated`
  - `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh`
  - `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers`
  - `BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput`
  - `BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput`
- 客户端 full-client 登录与持久分发：
  - `BenchmarkClientWebSocketPersistentLoginAuthenticated`
  - `BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers`
- 在线用户查询：
  - `BenchmarkMeshQueryLoggedInUsersPebbleLinear`
  - 它测的是“mesh presence mirror 收敛后的读取成本”，不是“每次远程 RPC 查询”的 TTL cache 成本。

关于客户端 benchmark，还需要记住两个实现边界：

- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh` 当前发送端和接收端都走 `/ws/realtime`。
- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers` 当前只跑 `SQLite`，背景在线会话通过 `/ws/realtime` 建立，被测发送/接收连接使用 `TransientOnly` 登录。
- `BenchmarkClientWebSocketPersistentLoginAuthenticated` 与 `BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers` 当前只跑 `SQLite`；后者使用标准 `/ws/client`，并以所有预期会话收到同一条持久消息作为 fanout 完成条件。

## 6. 验证命令

回归：

```bash
go test ./internal/cluster ./internal/store ./internal/api -count=1
```

持久复制 / 恢复基线：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1
```

服务端 transient 点对点吞吐：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMeshTransientPointToPointThroughput' -benchmem -count=1
go test -tags zeromq ./internal/cluster -run '^$' -bench 'BenchmarkMeshTransientPointToPointThroughput' -benchmem -count=1
```

`store` / `api` / 客户端 transient 基线：

```bash
go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP|ClientWebSocketTransient)' -benchmem -count=1
go test ./internal/api -run '^$' -bench 'BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1
go test -tags zeromq ./internal/api -run '^$' -bench 'BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1
go test -tags zeromq ./internal/api -run '^$' -bench 'BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1
```

full-client 基线需要显式单独运行：

```bash
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentLoginAuthenticated$' -benchmem -count=1
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers$' -benchmem -benchtime=1x -count=1
```

如果只想快速确认常规场景没有漂移，可以统一加 `-benchtime=1x` 做轻量探针；full-client 容量矩阵始终应保留 `-benchtime=1x`，并用子场景正则缩小开发期运行范围。

## 7. 当前建议

如果只选 3 个最值得继续推进的方向，当前建议顺序是：

1. 继续推进 `Pebble` 写热点去 SQLite 化，先打 `CreateMessage` 和消息可见性相关热点。
2. 补齐 durable benchmark 的纯 `libp2p` / 纯 `ZeroMQ` 基线，并把 mixed transport 的适用边界写清楚。
3. 以 inbox 命中率、mirror fanout 和批写可观测性为抓手，继续压缩读放大与控制面噪声。

这样做的原因是：

- 复制批量、digest 去抖、group commit、分片锁和 inbox 快路径都已经是当前基线，继续把它们写成“待做项”会误导后续判断。
- 当前最真实的剩余瓶颈已经转移到 SQLite 依赖、可观测性缺口和 benchmark 覆盖盲区。
- mixed transport 已经进入测试和吞吐基线，但 durable traffic 与 bridge 的边界必须继续分开看，不能混成一个结论。
