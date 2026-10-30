# 共识 RPC 流量优化

本次实现基于线上 `377258f`，目标是减少稳定集群空 AppendEntries 的编码开销，并修复 RPC 匹配及关闭边界。Raft 选举、心跳、提交间隔保持默认配置；FSM 命令、持久日志、BoltStore 和持久快照格式保持原有格式。

## 编码与滚动升级

`MeshNodeHello.consensus_binary_v1_supported`（字段 8）声明支持新编码。只采用直接握手确认的能力；同一节点存在多条连接时，必须全部声明支持。未知、多跳且没有直接握手、已断开及旧版本节点继续使用 JSON。

新编码以 `TFR\x01` 开头，后接 `proto/raft.proto` 定义的 Protobuf 信封。信封显式区分 request / response，保留方法、请求 ID、来源及错误。原生 Raft 的五种请求及响应均有映射：AppendEntries、RequestVote、RequestPreVote、InstallSnapshot、TimeoutNow。日志保留索引、任期、类型、Data、Extensions 和 AppendedAt；二进制内容直接传输，时间使用 `time.Time.MarshalBinary`，保留纳秒和时区偏移。

接收端同时支持新编码与旧 JSON；响应跟随请求的编码，无需等待反向握手完成。旧 JSON 响应缺少 Source 时，以外层 mesh 来源完成匹配。新 JSON 响应附带 Source，旧接收端可以忽略此字段。未知的二进制版本明确拒绝。

能力仅改变发送编码，不创建额外探测或周期任务；连接变化与升级期间每次调用重新读取能力。不会因一次发送失败就重复发送同一请求。

## RPC 正确性与生命周期

- 先识别请求/响应，再查询 pending。双方独立分配相同 ID 的请求互不占用 pending。
- 响应匹配请求 ID、目标节点、方法和本次调用的编码；错误来源或不匹配响应不移除 pending。重复或超时后的响应直接忽略。
- 生产入口校验共识组以及外层 mesh 来源与内层来源的一致性，使用 `DeliverFrom` 绑定来源。`Deliver` 仅保留给受信任的进程内适配器。
- 编码失败、发送失败、超时、pipeline 取消和传输关闭都会退出并清理 pending。生产发送使用带 context 的接口，10 秒超时覆盖发送和等待响应；Manager 的取消也能中止调用。
- 入站请求最多 128 个并发处理任务，释放 mesh 读取循环后再等待 Raft。排队及响应等待均受超时和关闭控制；回复通道有一个缓冲，Raft 的迟到响应不会阻塞。关闭幂等，等待所有入站任务结束后才关闭 Consumer；应先关闭 Raft，再关闭 Transport。
- AppendEntries pipeline 最多两个在途请求，按提交顺序发布完成结果；背压等待有超时，关闭取消在途 RPC。失败结果会终止 pipeline 并唤醒发送者。遵循 Raft 的现有约定，pipeline Consumer 不在关闭时关闭，由调用者通过独立停止信号退出。

保留的旧 `ConsensusSender` 签名要求回调自行保证有界返回；生产 Manager 已使用可取消的新适配接口。任意阻塞的旧式回调无法被 Transport 强制终止。

## 快照正文

原实现的 InstallSnapshot 会读取并丢弃正文，只发送元数据，无法正确恢复落后节点。本次二进制请求携带正文，接收端校验声明长度并设置 `raft.RPC.Reader`。读取发生在本地快照存储，按声明长度读取并拒绝截断正文。

旧节点没有快照正文传输能力。对这类节点发起非空快照会明确报错，避免继续静默丢弃数据；旧节点向新节点发送缺失正文的快照同样返回错误。滚动升级期间日志追赶可继续使用 JSON，但快照恢复要求双方升级，且发送端已通过直接 mesh 握手确认接收端的二进制能力。仅有多跳可达且没有直接握手的目标仍视为未知能力，不能接收非空快照。

快照当前使用单个 mesh 帧。正文声明大小超过 8 MiB 时，在读取本地正文之前直接报错，避免无界内存缓冲；接收端也校验此上限。实际可发送大小还受既有传输帧大小和信封开销限制（TCP mTLS 默认帧大小 8 MiB），没有增加分块传输；超限会通过发送错误返回。大型快照分块属于后续工作。

## 本地测量

代表性样本使用 17 位节点 ID、任期 71、日志索引 345 和请求 ID 123456。以下为完整内部 RPC 信封长度，包含新编码前缀，不包含外层 mesh、TLS 和 TCP/IP 开销。

| 内容 | 已部署 JSON | 二进制 v1 | 减少 |
| --- | ---: | ---: | ---: |
| 空 AppendEntries 请求 | 277 B | 108 B | 61.0% |
| AppendEntries 响应 | 189 B | 88 B | 53.4% |

新 JSON 响应因补充 Source 为 218 B；表中仍以实际已部署的旧响应作基准。

一次本地基准（Intel i5-12400，Go 默认基准工具）：

| 操作 | JSON | 二进制 v1 |
| --- | ---: | ---: |
| 编码耗时 | 1566 ns/op | 737 ns/op |
| 解码耗时 | 2433 ns/op | 746 ns/op |
| 编码内存 | 816 B/op，4 次分配 | 736 B/op，7 次分配 |
| 解码内存 | 584 B/op，6 次分配 | 912 B/op，11 次分配 |

二进制解码需要构造 Protobuf 及原生 Raft 两层对象，分配次数和解码内存有所增加。编码字节和 CPU 时间下降；这不是线上总流量或 RSS 的实测结论。线上实际降幅需要部署后按同等业务负载采样。

## 验证

新增测试覆盖五类 RPC 完整字段往返、日志时间与任意字节、双向相同 ID、错误来源/方法/编码、迟到重复响应、发送及编码错误清理、超时、关闭、入站上限、pipeline 背压和失败退出、新旧编码混用及正文截断。

真实三节点持久 Raft 测试验证默认配置选主、数据库写入、离线 follower 重启追赶；纯二进制场景主动压缩日志，强制经 InstallSnapshot 恢复正文。JSON 混合场景拒收向旧节点发送的任何二进制消息。

复现核心测试和基准：

```sh
GOWORK=off go test -race ./internal/kv ./internal/mesh ./internal/cluster -count=1
GOWORK=off go test ./internal/kv -run TestConsensusEmptyAppendSize -v -count=1
GOWORK=off go test ./internal/kv -run '^$' -bench BenchmarkConsensusCodec -benchmem
GOWORK=off go test ./internal/kv -run '^$' -fuzz FuzzConsensusCodec -fuzztime=12s -parallel=4
```

本次还执行完整 Go 测试、生产二进制构建、smoke，以及相同版本和当前线上旧版本各一轮进程级 WebSocket 预检。进程级预检覆盖认证、可信时钟、复制、断线重连和重启；共识协议本身由上述真实 Raft 测试覆盖。
