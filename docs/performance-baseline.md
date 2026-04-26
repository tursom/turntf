# 性能基线

本文记录当前仓库的性能基线，基于 Go 原生 `testing.B` benchmark。它的用途是给后续改动提供可重复的对比参考，不代表生产 SLA，也不作为当前 CI 的硬阈值。

## 覆盖范围

- `cluster` 层继续限定默认 WebSocket 传输，不比较 libp2p 或 ZeroMQ。
- `cluster` 基线现在同时覆盖稳态多节点复制、多跳路由，以及 retention 截断后的 snapshot repair / catchup repair。
- `store` 与 `api` 层新增 `SQLite` / `Pebble` 对照，用于补充低层热点差异。
- `Pebble` 范围按当前实现定义：事件日志和消息投影走 Pebble，元数据仍保留在 SQLite。
- benchmark 实现在：
  - [internal/cluster/mesh_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_benchmark_test.go)
  - [internal/cluster/mesh_recovery_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_recovery_benchmark_test.go)
  - [internal/store/store_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/store/store_benchmark_test.go)
  - [internal/store/store_degradation_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/store/store_degradation_benchmark_test.go)
  - [internal/api/http_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/http_benchmark_test.go)

当前基线覆盖以下场景：

- `BenchmarkMeshReplicationPebbleLinear3Nodes`：3 节点线性拓扑下的持久消息复制，校验最远端节点已应用且源节点已收到 `Ack`。
- `BenchmarkMeshQueryLoggedInUsersPebbleLinear`：3 节点 / 7 节点线性拓扑下的多跳在线用户查询，校验返回条数和代表性 payload。
- `BenchmarkMeshTransientRoutePebbleLinear`：3 节点 / 7 节点线性拓扑下的瞬时包多跳转发，校验 `packet_id`、payload 和最终 TTL。
- `BenchmarkMeshSnapshotRepairPebbleLinear3Nodes`：3 节点线性拓扑下的 snapshot repair，校验目标节点通过快照修复收敛。
- `BenchmarkMeshTruncatedCatchupRepairPebble`：retention 截断后的 truncated pull + snapshot repair 恢复路径。
- `BenchmarkStoreCreateMessage`：`SQLite` / `Pebble` 下直接消息写入；`Pebble` 子场景会继续细分 `no_sync` / `force_sync`。
- `BenchmarkStoreListMessagesByUser`：`SQLite` / `Pebble` 下典型读路径。
- `BenchmarkStorePruneEventLogOnce`：`SQLite` / `Pebble` 下 retention 截断成本。
- `BenchmarkDegradationStoreListMessagesByUser`：按历史消息量分层，观察 `SQLite` / `Pebble` 读路径的退化倍数和单位规模增量成本。
- `BenchmarkDegradationStorePruneEventLogOnce`：按 event log 规模分层，观察 `SQLite` / `Pebble` 截断路径的退化倍数和单位规模增量成本。
- `BenchmarkHTTPCreateMessageAuthenticated`：带鉴权的 `POST /nodes/{node_id}/users/{user_id}/messages`；`Pebble` 子场景会继续细分 `no_sync` / `force_sync`。
- `BenchmarkHTTPListMessagesByUserAuthenticated`：带鉴权的 `GET /nodes/{node_id}/users/{user_id}/messages?limit=50`。

## 采集策略

- benchmark 名称现在会显式带上介质 mode：`/tmp/...` 或 `/disk/...`。
- `tmp` 子场景始终运行，继续使用默认临时目录语义。
- 如果默认临时目录所在文件系统是内存文件系统，例如当前机器的 `/tmp` 是 `tmpfs`，同一条 `go test` 命令会自动补跑 `disk` 子场景。
- `disk` 子场景固定写入仓库根目录下的 `./.benchdata`。
- 常规 benchmark 会在正式计时前做一轮不计时 warmup；恢复类 benchmark 也会先做一轮缩小版 dry-run，避免 `-benchtime=1x` 时把首轮控制路径冷启动完全混进结果。
- 读取结论时，优先看本次采集中第一个非内存文件系统结果：
  - 出现 `disk` 时，以 `disk` 为主。
  - 未出现 `disk` 时，说明 `tmp` 本身已经跑在非内存文件系统上。

## 运行方式

`cluster` benchmark：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1
```

`store/api` benchmark：

```bash
go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP)' -benchmem -count=1
```

退化曲线 benchmark：

```bash
go test ./internal/store -run '^$' -bench 'BenchmarkDegradationStore(ListMessagesByUser|PruneEventLogOnce)' -benchmem -count=1 -benchtime=1x
```

这组 benchmark 主要用来看“规模增长时慢了多少”，默认不并入上面的常规 `store/api` 基线命令。

以上命令保持不变；是否额外出现 `/disk/...` 子场景，由 benchmark 在运行时按文件系统类型自动决定。

回归命令：

```bash
go test ./internal/cluster ./internal/store ./internal/api -count=1
```

如果只想快速确认 benchmark 场景和断言仍然可运行，可以使用：

```bash
go test ./internal/cluster -run '^$' -bench 'BenchmarkMesh(Replication|QueryLoggedInUsers|TransientRoute|SnapshotRepair|TruncatedCatchup)' -benchmem -count=1 -benchtime=1x
go test ./internal/store ./internal/api -run '^$' -bench 'Benchmark(Store|HTTP)' -benchmem -count=1 -benchtime=1x
```

## 指标说明

- `ns/op`：Go benchmark 的标准单次操作耗时。
- `B/op`：每次操作平均分配的内存字节数。
- `allocs/op`：每次操作平均分配次数。
- `bytes/op`：场景里单次操作的业务 payload 大小，便于横向对比不同消息体。
- `ack_ms/op`：复制场景从本地创建消息并广播，到最远端应用完成且源节点看到 `Ack` 推进的平均耗时。
- `query_ms/op`：查询场景单次多跳 `QueryLoggedInUsers` 的平均耗时。
- `delivery_ms/op`：瞬时包场景从源节点发起路由到目标节点本地 handler 收到包的平均耗时。
- `snapshot_ms/op`：snapshot repair 场景从发送 digest 到目标节点收敛的平均耗时。
- `truncated_repair_ms/op`：truncated response 触发 snapshot repair 并完成恢复的平均耗时。
- `history_00064_ns/op` / `events_00256_ns/op`：退化曲线 benchmark 中，对应规模层的平均耗时。
- `*_vs_*_x`：相对首层规模的退化倍数。
- `*_delta_ns_per_1k`：相对首层规模，每额外 `1000` 条消息 / event 带来的平均增量耗时。

## 基线环境

以下结果来自 **2026-04-25** 的一次本地基线采集。
这批数据发生在“自适应 `tmp` / `disk` 基线”引入之前，应按当前语义视为一组 **`tmp` 历史样本**，不能直接代表今天文档里所说的“官方非内存文件系统结果”。

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
| create message / pebble | 884,864 | `POST /nodes/{node_id}/users/{user_id}/messages` 256B payload；新版 benchmark 会拆成 `pebble/no_sync` 与 `pebble/force_sync` | 477,979 | 4,340 |
| list messages / sqlite | 381,376 | `GET /nodes/{node_id}/users/{user_id}/messages?limit=50` | 144,555 | 2,219 |
| list messages / pebble | 257,638 | `GET /nodes/{node_id}/users/{user_id}/messages?limit=50` | 140,814 | 1,561 |

## 如何使用这份基线

- 做性能回归时，优先比较同一子场景在同一机器上的变化幅度，而不是只看单个绝对值。
- 现在的 benchmark 输出会带 mode；同一轮结果里应优先比较首个非内存文件系统子场景，再把 `tmp` 结果作为开发期快速回归参考。
- `cluster`、`store`、`api` 三层的 benchmark 不应直接混合比较；它们回答的是不同层级的问题。
- 如果后续新增混合传输、auth 专项或更重的断链重连场景，建议继续按章节追加，而不是混写进现有表格。
- 如果未来决定引入性能门禁，建议先连续采集多轮结果，确认波动区间后再为少数关键场景设置宽松阈值。
