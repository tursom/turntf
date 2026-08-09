# 性能基线

本文记录当前仓库的性能基线，基于 Go 原生 `testing.B` benchmark。它的用途是给后续改动提供可重复的对比参考，不代表生产 SLA，也不作为当前 CI 的硬阈值。

## 覆盖范围

- 大多数 `cluster` 延迟 / 恢复基线仍限定默认 WebSocket 传输；新增的点对点 transient 吞吐基线会额外比较 libp2p 和 ZeroMQ。
- `cluster` 基线现在同时覆盖稳态多节点复制、多跳路由，以及 retention 截断后的 snapshot repair / catchup repair。
- `store` 与 `api` 层新增 `SQLite` / `Pebble` 对照，用于补充低层热点差异。
- `Pebble` 范围按当前实现定义：事件日志、消息投影、消息序号计数、peer ack / origin cursor、pending projection 都走 Pebble；用户、登录名、订阅、黑名单、附件和 `user_metadata` 仍保留在 SQLite。
- benchmark 实现在：
  - [internal/cluster/mesh_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_benchmark_test.go)
  - [internal/cluster/mesh_point_to_point_throughput_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_point_to_point_throughput_benchmark_test.go)
  - [internal/cluster/mesh_point_to_point_throughput_benchmark_zeromq_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_point_to_point_throughput_benchmark_zeromq_test.go)
  - [internal/cluster/mesh_recovery_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_recovery_benchmark_test.go)
  - [internal/cluster/online_presence_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/cluster/online_presence_benchmark_test.go)
  - [internal/store/store_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/store/store_benchmark_test.go)
  - [internal/store/store_degradation_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/store/store_degradation_benchmark_test.go)
  - [internal/api/http_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/http_benchmark_test.go)
  - [internal/api/client_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_benchmark_test.go)
  - [internal/api/client_persistent_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_persistent_benchmark_test.go)
  - [internal/api/client_point_to_point_throughput_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_point_to_point_throughput_benchmark_test.go)
  - [internal/api/client_point_to_point_throughput_benchmark_zeromq_test.go](/root/dev/sys/turntf/turntf/internal/api/client_point_to_point_throughput_benchmark_zeromq_test.go)
  - [internal/api/client_point_to_point_throughput_zeromq_client_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_point_to_point_throughput_zeromq_client_benchmark_test.go)

当前基线覆盖以下场景：

- `BenchmarkMeshReplicationPebbleLinear3Nodes`：3 节点线性拓扑下的持久消息复制，校验最远端节点已应用且源节点已收到 `Ack`。
- `BenchmarkMeshQueryLoggedInUsersPebbleLinear`：3 节点 / 7 节点线性拓扑下的多跳在线用户查询，校验返回条数和代表性 payload。
- `BenchmarkMeshTransientRoutePebbleLinear`：3 节点 / 7 节点线性拓扑下的瞬时包多跳转发，校验 `packet_id`、payload 和最终 TTL。
- `BenchmarkMeshTransientPointToPointThroughput`：服务端 transient 数据面点对点吞吐；固定使用 `SQLite`。默认构建覆盖单节点、2 节点纯协议直连、7 节点纯协议线性 `WebSocket/libp2p`，以及 `3/5` 节点 mixed bridge（`ws -> libp2p`）；带 `-tags zeromq` 时再追加 `ZeroMQ` 直连 / 线性与 `ws -> zeromq` mixed bridge。
- `BenchmarkMeshSnapshotRepairPebbleLinear3Nodes`：3 节点线性拓扑下的 snapshot repair，校验目标节点通过快照修复收敛。
- `BenchmarkMeshTruncatedCatchupRepairPebble`：retention 截断后的 truncated pull + snapshot repair 恢复路径。
- `BenchmarkOnlinePresenceSync10K`：固定 10k 用户，分别测旧式全量、单用户增量和单个权威分片的发送端构建与接收端应用成本，用于直接比较 `B/op` 和 `allocs/op`。
- `BenchmarkStoreCreateMessage`：`SQLite` / `Pebble` 下直接消息写入；`Pebble` 子场景会继续细分 `balanced/throughput` 与 `no_sync/force_sync`。
- `BenchmarkStoreCreateMessageSteadyState`：先把单用户历史写到 `2 * message_window_size`，再继续写入，观察消息窗口进入稳态后的持续写入成本。
- `BenchmarkStoreCreateMessageParallel`：固定 `256B` payload 的并行写入，对比 `hotspot` 与 `uniform-1000` 两种用户分布。
- `BenchmarkStoreListMessagesByUser`：`SQLite` / `Pebble` 下典型读路径。
- `BenchmarkStorePruneEventLogOnce`：`SQLite` / `Pebble` 下 retention 截断成本。
- `BenchmarkDegradationStoreListMessagesByUser`：按历史消息量分层，观察 `SQLite` / `Pebble` 读路径的退化倍数和单位规模增量成本。
- `BenchmarkDegradationStorePruneEventLogOnce`：按 event log 规模分层，观察 `SQLite` / `Pebble` 截断路径的退化倍数和单位规模增量成本。
- `BenchmarkHTTPCreateMessageAuthenticated`：带鉴权的 `POST /nodes/{node_id}/users/{user_id}/messages`；`Pebble` 子场景会继续细分 `balanced/throughput` 与 `no_sync/force_sync`。
- `BenchmarkHTTPListMessagesByUserAuthenticated`：带鉴权的 `GET /nodes/{node_id}/users/{user_id}/messages?limit=50`。
- `BenchmarkClientWebSocketTransientSendMessageAuthenticated`：带鉴权的 `WS /ws/client` transient `SendMessage` RPC，发送端校验 `TransientAccepted`，接收端校验 `PacketPushed`，并确认消息未落盘。
- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh`：3 节点 / 7 节点线性拓扑下的带鉴权 WebSocket transient `SendMessage` RPC；发送端和接收端都走 `/ws/realtime`，校验多跳 mesh 后的 `TransientAccepted` / `PacketPushed` 端到端路径，并确认消息未落盘。
- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers`：当前仅跑 `SQLite`；在 3 节点 / 7 节点线性拓扑下先建立大批常驻在线的实时会话，再测一条跨节点 transient `SendMessage`，用于观察大规模在线连接负载下的端到端延迟。背景连接走 `/ws/realtime`，被测发送/接收连接使用 `TransientOnly` 登录，不经过持久化补发路径。
- `BenchmarkClientWebSocketPersistentLoginAuthenticated`：标准 `/ws/client` 持久化登录基线，固定使用 `SQLite` 和 `256B` 消息，按 `0/100/1000` 条历史分层。每轮重新建立连接并校验登录响应、历史消息数量、顺序和内容，分别记录收到登录响应与完成历史补发的时间。
- `BenchmarkClientWebSocketPersistentReconnectTokenAuthenticated`：与密码登录基线使用相同连接、历史和校验路径，但每轮使用上次登录刷新出的短期 `reconnect_token`，用于隔离测量跳过 bcrypt 后的重连成本。
- `BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers`：标准 `/ws/client` 持久化稳态容量基线，固定使用 `SQLite`，覆盖 3 节点 / 7 节点以及 `1000/5000/10000` 个普通在线会话，另有一个管理员发送会话。背景客户端按协议发送低频 `Ping` 保持长期连接；每个容量场景共享一次普通连接 setup，再分别测 direct、broadcast 和确定性分布到所有节点的 10% channel 订阅者；结果以所有预期目标收到同一条消息为完成条件，并记录各节点实际枚举的候选会话总数。
- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput`：客户端 transient 端到端点对点吞吐；固定使用 `SQLite`。默认构建覆盖单节点、2 节点纯协议直连和 7 节点纯协议线性 `WebSocket/libp2p`；带 `-tags zeromq` 时再追加 `ZeroMQ` 直连 / 线性。当前不包含 mixed bridge 子场景，并且统一走实时客户端路径，不切 `tmp/disk` 子场景。
- `BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput`：客户端通过 ZeroMQ 长连接接入时的 transient 端到端点对点吞吐；固定使用 `SQLite`，需要 `-tags zeromq`。客户端先发送 `ZeroMQMuxHello{role=CLIENT}` 再登录，覆盖 `2` 节点直连与 `7` 节点纯 `ZeroMQ` 线性拓扑，用于把“客户端 ZeroMQ 入口开销”和“节点间 ZeroMQ mesh hop 开销”单独拉平观察。

## 采集策略

- benchmark 名称现在会显式带上介质 mode：`/tmp/...` 或 `/disk/...`。
- 例外：点对点 transient 吞吐 benchmark 不再切 `tmp/disk`，输出名称也不会带 `/tmp` 或 `/disk`。
- `tmp` 子场景始终运行，继续使用默认临时目录语义。
- 如果默认临时目录所在文件系统是内存文件系统，例如当前机器的 `/tmp` 是 `tmpfs`，同一条 `go test` 命令会自动补跑 `disk` 子场景。
- `disk` 子场景固定写入仓库根目录下的 `./.benchdata`。
- 常规 benchmark 会在正式计时前做一轮不计时 warmup；恢复类 benchmark 也会先做一轮缩小版 dry-run，避免 `-benchtime=1x` 时把首轮控制路径冷启动完全混进结果。
- full-client 容量 benchmark 会真实建立最多 `10000` 个连接并等待高 fanout 完成，完整矩阵可使用 `-benchtime=1x` 手动验证；需要观察周期工作时，应精确选择 direct 子场景并使用 `-benchtime=10s -count=3`。
- 读取结论时，优先看本次采集中第一个非内存文件系统结果：
  - 出现 `disk` 时，以 `disk` 为主。
  - 未出现 `disk` 时，说明 `tmp` 本身已经跑在非内存文件系统上。

## 运行方式

`cluster` benchmark：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1
```

10k 在线状态同步 benchmark：

```bash
go test ./internal/cluster -run '^$' -bench '^BenchmarkOnlinePresenceSync10K$' -benchmem -benchtime=3s -count=3
```

验收时分别以 `sender/legacy-full` 和 `receiver/legacy-full` 为同侧基线；`delta-one-user` 与 `authoritative-shard` 的 `B/op`、`allocs/op` 都应至少下降 90%。

点对点吞吐 benchmark：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMeshTransientPointToPointThroughput' -benchmem -count=1
go test ./internal/api -run '^$' -bench 'BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1
```

带 `ZeroMQ` 的点对点吞吐 benchmark：

```bash
go test -tags zeromq ./internal/cluster -run '^$' -bench 'BenchmarkMeshTransientPointToPointThroughput' -benchmem -count=1
go test -tags zeromq ./internal/api -run '^$' -bench 'BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1
go test -tags zeromq ./internal/api -run '^$' -bench 'BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1
```

不带 `-tags zeromq` 时，这些点对点吞吐 benchmark 只会覆盖 `WebSocket/libp2p` 子场景；带 tag 后才会追加 `ZeroMQ` 子场景。

这些点对点吞吐 benchmark 现在固定只跑一组临时目录场景，因此结果名称不会再出现 `/tmp` 或 `/disk`。

`store/api` benchmark：

```bash
go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP|ClientWebSocketTransient)' -benchmem -count=1
```

这条命令当前会命中 `BenchmarkStoreCreateMessage*`、`BenchmarkStoreListMessagesByUser`、`BenchmarkStorePruneEventLogOnce`、`BenchmarkHTTP*` 和默认构建下的 `BenchmarkClientWebSocketTransient*`；`BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput` 仍需单独加 `-tags zeromq` 执行。

full-client 登录与稳态容量 benchmark：

```bash
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentLoginAuthenticated$' -benchmem -count=1
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentReconnectTokenAuthenticated$' -benchmem -count=1
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers$' -benchmem -benchtime=1x -count=1
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers/tmp/sqlite/3-nodes/10000-online/direct/256B$' -benchmem -benchtime=10s -count=3
```

登录基线可在同一机器上使用更高 `-count` 做前后统计对比。容量矩阵的 setup 和 fanout 成本较高，功能验证应保留完全相同的 `-benchtime=1x -count=1` 命令和原始输出。旧式 `-benchtime=10x` 可能因测量窗口短于 5 秒而错过周期 ticker；周期尾延迟对比必须使用上面的持续 10 秒 direct 命令。

退化曲线 benchmark：

```bash
go test ./internal/store -run '^$' -bench 'BenchmarkDegradationStore(ListMessagesByUser|PruneEventLogOnce)' -benchmem -count=1 -benchtime=1x
```

这组 benchmark 主要用来看“规模增长时慢了多少”，默认不并入上面的常规 `store/api` 基线命令。

除点对点吞吐 benchmark 外，以上命令保持不变；是否额外出现 `/disk/...` 子场景，由 benchmark 在运行时按文件系统类型自动决定。

回归命令：

```bash
go test ./internal/cluster ./internal/store ./internal/api -count=1
```

如果只想快速确认 benchmark 场景和断言仍然可运行，可以使用：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1 -benchtime=1x
go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP|ClientWebSocketTransient)' -benchmem -count=1 -benchtime=1x
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentLoginAuthenticated/tmp/sqlite/history-(0|100|1000)/256B$' -benchmem -count=1 -benchtime=1x
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers/tmp/sqlite/3-nodes/1000-online/(direct|broadcast|channel-10pct)/256B$' -benchmem -count=1 -benchtime=1x
go test ./internal/cluster -run '^$' -bench 'BenchmarkMeshTransientPointToPointThroughput' -benchmem -count=1 -benchtime=1x
go test ./internal/api -run '^$' -bench 'BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1 -benchtime=1x
go test -tags zeromq ./internal/cluster -run '^$' -bench 'BenchmarkMeshTransientPointToPointThroughput' -benchmem -count=1 -benchtime=1x
go test -tags zeromq ./internal/api -run '^$' -bench 'BenchmarkClientWebSocketTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1 -benchtime=1x
go test -tags zeromq ./internal/api -run '^$' -bench 'BenchmarkClientZeroMQTransientSendMessageAuthenticatedPointToPointThroughput' -benchmem -count=1 -benchtime=1x
```

## 指标说明

- `ns/op`：Go benchmark 的标准单次操作耗时。
- `MB/s`：对调用了 `SetBytes` 的吞吐 benchmark，Go benchmark 会额外给出按 payload 大小换算后的吞吐率。
- `B/op`：每次操作平均分配的内存字节数。
- `allocs/op`：每次操作平均分配次数。
- `bytes/op`：场景里单次操作的业务 payload 大小，便于横向对比不同消息体。
- `ack_ms/op`：复制场景从本地创建消息并广播，到最远端应用完成且源节点看到 `Ack` 推进的平均耗时。
- `accept_ms/op`：客户端 WebSocket transient 场景中，从发送请求到发送端收到 `TransientAccepted` 的平均耗时。
- `push_ms/op`：客户端 WebSocket transient 场景中，从发送请求到接收端收到 `PacketPushed` 的平均耗时。
- `login_ms/op`：full-client 登录场景从开始建立 WebSocket 到收到 `LoginResponse` 的平均耗时。
- `catchup_ms/op`：full-client 登录场景从开始建立 WebSocket 到收到最后一条历史 `MessagePushed` 的平均耗时；无历史时等于 `login_ms/op`。
- `history_messages/op`：full-client 登录场景每次连接校验的历史消息条数。
- `write_ms/op`：持久消息场景从发送请求到收到包含已落库消息的 `SendMessageResponse` 的平均耗时。
- `first_push_ms/op` / `last_push_ms/op`：持久消息场景从发送请求到首个 / 最后一个预期目标收到 `MessagePushed` 的平均耗时。
- `write_p95_ms/op` / `write_p99_ms/op` / `write_max_ms/op`：同一次采集中持久写响应的 p95、p99 和最大延迟。
- `last_push_p95_ms/op` / `last_push_p99_ms/op` / `last_push_max_ms/op`：同一次采集中最后目标到达的 p95、p99 和最大延迟。
- `delivered/op`：持久消息场景每次操作必须收到并校验的目标会话数；该场景的 `MB/s` 按 `payload * delivered` 计算。
- `candidates/op`：持久消息场景每次操作在所有节点上实际枚举的候选会话总数；channel 场景应接近订阅会话数加管理员会话数，而不是总在线会话数。
- `query_ms/op`：查询场景单次多跳 `QueryLoggedInUsers` 的平均耗时。
- `delivery_ms/op`：瞬时包场景从源节点发起路由到目标节点本地 handler 收到包的平均耗时。
- `snapshot_ms/op`：snapshot repair 场景从发送 digest 到目标节点收敛的平均耗时。
- `truncated_repair_ms/op`：truncated response 触发 snapshot repair 并完成恢复的平均耗时。
- `history_00064_ns/op` / `events_00256_ns/op`：退化曲线 benchmark 中，对应规模层的平均耗时。
- `*_vs_*_x`：相对首层规模的退化倍数。
- `*_delta_ns_per_1k`：相对首层规模，每额外 `1000` 条消息 / event 带来的平均增量耗时。

## 基线环境

### 2026-08-09 在线状态同步定向样本

同机使用 `-benchtime=3s -count=3` 采样，表中取三轮稳定值：

| 路径 | 场景 | B/op | allocs/op | 相对同侧旧全量 |
| --- | --- | ---: | ---: | ---: |
| sender | legacy-full | 3,530,739 | 30,010 | 基线 |
| sender | delta-one-user | 416 | 6 | B/op -99.99%，allocs/op -99.98% |
| sender | authoritative-shard（625 users） | 171,240 | 1,882 | B/op -95.15%，allocs/op -93.73% |
| receiver | legacy-full | 6,250,132 | 20,005 | 基线 |
| receiver | delta-one-user | 624 | 2 | B/op -99.99%，allocs/op -99.99% |
| receiver | authoritative-shard（625 users） | 485,355 | 1,274 | B/op -92.23%，allocs/op -93.63% |

sender 的权威分片场景包含生产路径中的分片索引遍历、用户切片分配、排序和消息构建。这组结果验证了同步算法的分配门槛，不代表网络广播、完整 10k client setup 或生产延迟 SLA。

### 2026-08-09 10k full-client direct 周期样本

使用文档推荐的 `-benchtime=10s -count=3` 精确 direct 子场景命令。fixture 会先验证三节点所有 origin 在线镜像完整，等待 8.5 秒覆盖一整轮权威分片，再执行 setup 后 GC；随后的正式 10 秒测量窗口仍包含完整的周期分片校验。

| 轮次 | write avg | write p95 | write p99 | write max | last_push avg | last_push p95 | last_push p99 | last_push max | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.6916ms | 1.481ms | 1.604ms | 5.600ms | 4.162ms | 5.380ms | 5.740ms | 20.69ms | 115,995 | 1,719 |
| 2 | 0.7125ms | 1.491ms | 1.628ms | 18.71ms | 4.236ms | 5.415ms | 5.831ms | 77.92ms | 205,521 | 3,676 |
| 3 | 0.7003ms | 1.496ms | 1.615ms | 18.74ms | 4.190ms | 5.376ms | 5.766ms | 71.35ms | 156,914 | 2,665 |

三轮均未出现 `last_push >= 1s`。旧样本只有约 `1.4s` 的单次 `last_push` 观测值、没有 p95/p99/max，因此不能据此证明同口径的“尾延迟下降 80%”；后续应在同机、同命令、同采集窗口下保留修改前后两组分位数再计算降幅。本次结果只确认新路径在三轮 10 秒窗口内满足“不出现 `last_push >= 1s`”。

以下结果来自 **2026-04-25** 的一次本地基线采集。
这批数据发生在“自适应 `tmp` / `disk` 基线”引入之前，应按当前语义视为一组 **`tmp` 历史样本**，不能直接代表今天文档里所说的“官方非内存文件系统结果”。

下面这组 **2026-04-25** 的历史样本只整理了当时采集入表的 `cluster`、`store` 和 `HTTP` 数据；当前 benchmark 集合里的 `BenchmarkStoreCreateMessageSteadyState`、`BenchmarkStoreCreateMessageParallel`、全部 `BenchmarkClientWebSocket*`（包括 full-client 登录与容量场景），以及 `-tags zeromq` 才会出现的点对点吞吐场景，都还没有对应的历史样本表格。

- `cluster` 命令：`go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1`
- `cluster` 总耗时：`103.271s`
- `store/api` 命令：`go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP)' -benchmem -count=1`
- `store` 包总耗时：`196.503s`
- `api` 包总耗时：`11.313s`
- `goos=linux`
- `goarch=amd64`
- CPU：`12th Gen Intel(R) Core(TM) i5-12400`

这些数字主要用于同机型、同环境、同命令下的前后对比。跨机器、跨内核版本或不同负载条件下的绝对值不应直接横向比较。

## 历史 tmp 样本

以下表格均对应上面的 `tmp` 历史样本。后续重新采集时，如果命令输出同时出现 `/tmp/...` 和 `/disk/...`，文档主结论应优先采用 `/disk/...`。

### 复制

| 场景 | ns/op | ack_ms/op | bytes/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| 3-nodes / 256B | 4,224,904 | 4.225 | 256 | 3,896,695 | 35,199 |
| 3-nodes / 4KiB | 8,411,973 | 8.412 | 4,096 | 18,998,932 | 27,096 |
| 3-nodes / 16KiB | 11,782,484 | 11.780 | 16,384 | 33,706,095 | 15,354 |

### 查询

| 场景 | ns/op | query_ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 3-nodes / 1-user | 1,267,250 | 1.267 | 40,591 | 558 |
| 3-nodes / 100-users | 1,021,520 | 1.021 | 150,415 | 872 |
| 7-nodes / 1-user | 1,297,273 | 1.297 | 197,655 | 2,678 |
| 7-nodes / 100-users | 1,110,339 | 1.110 | 452,219 | 3,027 |

### 瞬时包路由

| 场景 | ns/op | delivery_ms/op | bytes/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| 3-nodes / 256B | 308,399 | 0.308 | 256 | 27,187 | 274 |
| 3-nodes / 4KiB | 337,362 | 0.337 | 4,096 | 183,695 | 294 |
| 7-nodes / 256B | 571,088 | 0.571 | 256 | 116,672 | 1,338 |
| 7-nodes / 4KiB | 557,827 | 0.558 | 4,096 | 517,024 | 1,398 |

### 恢复路径

| 场景 | ns/op | 自定义延迟 | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| snapshot repair / users-100 | 10,345,328 | 10.340 `snapshot_ms/op` | 3,655,097 | 58,577 |
| snapshot repair / messages-500 | 15,309,762 | 15.310 `snapshot_ms/op` | 7,915,400 | 103,887 |
| truncated repair / retain-2-generate-32 | 2,015,240 | 2.015 `truncated_repair_ms/op` | 222,025 | 4,621 |
| truncated repair / retain-8-generate-256 | 5,250,662 | 5.250 `truncated_repair_ms/op` | 1,404,020 | 26,166 |

### Store 热点

#### CreateMessage

| 场景 | ns/op | payload | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| sqlite / 256B | 374,495 | 256B | 20,777 | 537 |
| sqlite / 4KiB | 327,046 | 4KiB | 32,911 | 537 |
| pebble / 256B | 1,139,831 | 256B | 463,911 | 4,263 |
| pebble / 4KiB | 2,920,456 | 4KiB | 2,299,220 | 4,200 |

这份历史样本仍是旧版聚合结果，没有拆出当前 `pebble/balanced|throughput/no_sync|force_sync` 四类子场景，也没有包含 `BenchmarkStoreCreateMessageSteadyState` / `BenchmarkStoreCreateMessageParallel`。

#### ListMessagesByUser

| 场景 | ns/op | history | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| sqlite / 100 | 299,658 | 100 | 106,614 | 1,910 |
| sqlite / 1000 | 1,209,590 | 1000 | 594,439 | 10,122 |
| pebble / 100 | 152,886 | 100 | 103,383 | 1,272 |
| pebble / 1000 | 756,905 | 1000 | 579,535 | 6,587 |

#### PruneEventLogOnce

| 场景 | ns/op | retention/events | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| sqlite / 128-256 | 320,303 | 128 / 256 | 4,575 | 126 |
| sqlite / 128-4096 | 3,536,333 | 128 / 4096 | 4,612 | 129 |
| pebble / 128-256 | 837,917 | 128 / 256 | 107,365 | 2,881 |
| pebble / 128-4096 | 15,350,808 | 128 / 4096 | 2,384,014 | 68,074 |

### HTTP 热点

| 场景 | ns/op | 说明 | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| create message / sqlite | 285,606 | `POST /nodes/{node_id}/users/{user_id}/messages` 256B payload | 31,587 | 591 |
| create message / pebble | 884,864 | `POST /nodes/{node_id}/users/{user_id}/messages` 256B payload；新版 benchmark 会拆成 `pebble/balanced|throughput/no_sync|force_sync` | 477,979 | 4,340 |
| list messages / sqlite | 381,376 | `GET /nodes/{node_id}/users/{user_id}/messages?limit=50` | 144,555 | 2,219 |
| list messages / pebble | 257,638 | `GET /nodes/{node_id}/users/{user_id}/messages?limit=50` | 140,814 | 1,561 |

### Client WebSocket 热点

`BenchmarkClientWebSocketTransientSendMessageAuthenticated` 已加入当前 benchmark 集合，但这份 **2026-04-25** 的历史 `tmp` 样本还没有它的结果。
后续重新采集时，应补录发送端 `TransientAccepted` 的 `accept_ms/op` 和接收端 `PacketPushed` 的 `push_ms/op`，并继续按 `tmp` / `disk` 语义记录。

`BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMesh` 也已加入当前 benchmark 集合，但同样还没有这份历史样本里的结果。
后续重新采集时，应按 `3-nodes` / `7-nodes` 和 payload 分层补录多跳 mesh 下的 `accept_ms/op`、`push_ms/op`、`ns/op`、`B/op` 和 `allocs/op`。

`BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers` 则用于观察“大量常驻在线实时会话存在时”的前台跨节点 transient 延迟，而不是 HTTP 轮询或历史补发路径。
后续重新采集时，应至少按总在线用户数、节点数和 `256B` payload 记录它的 `accept_ms/op`、`push_ms/op` 与是否能在设定时间内进入稳态。

`BenchmarkClientWebSocketPersistentLoginAuthenticated` 和 `BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers` 已补上 full-client 登录与稳态分发边界，但当前仍没有正式历史样本表格。后续采集时应把登录历史层与 `direct/broadcast/channel-10pct` 容量层分开保存，不能用 transient-only 结果替代。

## 如何使用这份基线

- 做性能回归时，优先比较同一子场景在同一机器上的变化幅度，而不是只看单个绝对值。
- 现在的 benchmark 输出会带 mode；同一轮结果里应优先比较首个非内存文件系统子场景，再把 `tmp` 结果作为开发期快速回归参考。
- `cluster`、`store`、`api` 三层的 benchmark 不应直接混合比较；它们回答的是不同层级的问题。
- 如果后续新增混合传输、auth 专项或更重的断链重连场景，建议继续按章节追加，而不是混写进现有表格。
- 如果未来决定引入性能门禁，建议先连续采集多轮结果，确认波动区间后再为少数关键场景设置宽松阈值。
