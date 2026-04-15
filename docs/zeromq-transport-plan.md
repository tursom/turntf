# ZeroMQ 传输试点接入方案

本文档把 roadmap 中“评估并试点 `zeromq`”细化为可执行设计。它描述如何在不改变现有复制语义的前提下，把 ZeroMQ 作为集群节点间的实验性第二传输接入。

本文档是实施方案，不表示当前仓库已经完成 ZeroMQ 接入。当前稳定主链路仍是 WebSocket + Protobuf。

## 目标与边界

- ZeroMQ 作为可选服务监听入口；单节点模式可服务业务客户端，集群模式下也可承载节点间传输，默认主链路继续使用 WebSocket。
- ZeroMQ v1 只承载已经签名的 protobuf `Envelope` bytes，不引入 PUB/SUB 复制模型。
- `Hello`、HMAC、校时、`Ack`、`EventBatch`、补拉、反熵、快照修复、动态路由和 membership 语义保持不变。
- 不改客户端 WebSocket；`GET /ws/client` 仍服务业务客户端。
- 不新增 `advertise_url`。ZeroMQ 的本机监听地址和远端可拨号地址分开处理：本机只配置 `bind_url`，远端拨号仍来自 `[[cluster.peers]].url` 或 discovery 记录。
- ZeroMQ v1 不提供链路机密性。生产环境必须部署在可信内网、VPN、专线或外部 TLS 隧道之后。
- ZeroMQ v1 只支持 TCP：本机监听使用 `tcp://host:port`，peer/discovery 使用 `zmq+tcp://host:port`。

## 配置设计

配置新增可选的 `[services.zeromq]` 段。未配置或 `enabled = false` 时，行为必须与当前版本完全一致。

```toml
[services.zeromq]
enabled = false
bind_url = "tcp://0.0.0.0:9090"
security = "none"

[services.zeromq.curve]
server_public_key = ""
server_secret_key = ""
client_public_key = ""
client_secret_key = ""
allowed_client_public_keys = []

[cluster]
secret = "secret"

[[cluster.peers]]
url = "ws://127.0.0.1:9081/internal/cluster/ws"

[[cluster.peers]]
url = "zmq+tcp://127.0.0.1:9091"
zeromq = { curve_server_public_key = "" }
```

字段语义：

| 字段 | 含义 |
| --- | --- |
| `services.zeromq.enabled` | 是否允许本节点使用 ZeroMQ transport。默认 `false`。 |
| `services.zeromq.bind_url` | 本节点 ROUTER socket 的本地监听地址，只允许 `tcp://host:port`。可选；为空时进入 outbound-only 模式。 |
| `services.zeromq.security` | ZeroMQ 安全模式，支持 `none` 或 `curve`，默认 `none`。 |
| `services.zeromq.curve.*` | CURVE 模式下的本节点 server/client key 与允许接入的 client public key 白名单。 |
| `cluster.peers.url` | 远端可拨号地址。WebSocket 使用 `ws://` 或 `wss://`；ZeroMQ 使用 `zmq+tcp://host:port`。 |
| `cluster.peers.zeromq.curve_server_public_key` | CURVE 模式下静态 `zmq+tcp` peer 的远端 server public key。 |

ZeroMQ 启动语义：

- `enabled = false`：完全禁用 ZeroMQ；静态和 discovered 的 `zmq+tcp` peer 都不会参与拨号。
- `enabled = true` 且 `bind_url` 非空：启动 ZeroMQ listener，同时允许对静态和 discovered 的 `zmq+tcp` peer 主动拨号。
- `enabled = true` 且 `bind_url` 为空：不启动 listener，但允许对静态和 discovered 的 `zmq+tcp` peer 主动拨号。
- `security = "curve"`：ROUTER 使用 server secret key，DEALER 使用远端 server public key 与本节点 client key；listener 通过 ZAP 白名单验证 `allowed_client_public_keys`。

`bind_url` 与 `cluster.peers.url` 的职责区分保持不变：

- `bind_url` 面向本机监听，允许 `tcp://0.0.0.0:9090`。
- `cluster.peers.url` 面向远端拨号，不能使用 `0.0.0.0`、`::`、空 host、path、query 或 fragment。
- membership 不主动广播本机 `bind_url`，避免把通配地址、容器内地址或本机-only 地址传播给其他节点。

## URL 规范

现有 peer URL 规范化逻辑需要扩展为支持三类 scheme：

| scheme | 用途 | 主要校验 |
| --- | --- | --- |
| `ws` | WebSocket 明文集群 peer | host 非空，fragment 为空。 |
| `wss` | WebSocket TLS 集群 peer | host 非空，fragment 为空。 |
| `zmq+tcp` | ZeroMQ TCP 集群 peer | host 非空，不是通配地址，path/query/fragment 为空。 |

ZeroMQ 本机监听 URL 使用 `tcp` scheme，由 `[services.zeromq].bind_url` 单独校验：

- 必须是 `tcp://host:port`。
- host 可以是 `0.0.0.0` 或 `::`。
- port 必须存在。
- path、query、fragment 必须为空。

实现时建议提供两个明确的校验函数：

- `normalizePeerURL(raw string) (string, error)`：用于静态 peer、discovery 和 membership advertisement。
- `normalizeZeroMQBindURL(raw string) (string, error)`：只用于 `[services.zeromq].bind_url`。

## 传输抽象

集群协议层需要先从 `websocket.Conn` 中解耦。建议新增内部传输接口，接口只表达“传输已签名 Envelope bytes”的能力。

```go
type TransportConn interface {
	Send(ctx context.Context, payload []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
	LocalAddr() string
	RemoteAddr() string
	Direction() string
	Transport() string
}

type Dialer interface {
	Dial(ctx context.Context, peerURL string) (TransportConn, error)
}

type Listener interface {
	Start(ctx context.Context, accept func(TransportConn)) error
	Close() error
}
```

协议层职责：

- 负责 marshal、HMAC 签名、unmarshal、HMAC 校验。
- 负责 `Hello` 前置校验、身份绑定、方向裁决、校时、补拉、反熵和路由。
- 不直接感知 WebSocket frame、ZeroMQ identity、socket option 或 listener 细节。

传输层职责：

- 负责建立连接、收发 bytes、关闭连接、暴露远端地址和传输类型。
- WebSocket adapter 保持现有 binary frame、ping/pong、deadline 和 close control 行为。
- ZeroMQ adapter 封装 ROUTER/DEALER、identity、发送队列、接收分发和 socket option。

## WebSocket 适配器迁移

第一阶段只做行为等价迁移：

1. 把现有 `websocket.Upgrader`、`websocket.Dialer`、`ReadMessage`、`WriteMessage`、ping/pong、deadline 和 close control 逻辑搬入 WebSocket adapter。
2. `Manager` 的 `session` 持有 `TransportConn`，不再持有 `*websocket.Conn`。
3. `dialLoop` 根据 peer URL scheme 选择传输 dialer。
4. `handleWebSocket` 只负责完成 HTTP upgrade，然后把 WebSocket adapter conn 交给 `runSession`。
5. 保持所有现有 WebSocket 测试先全绿，再进入 ZeroMQ 实现。

这一步不应修改集群协议、proto、存储 schema 或业务 API。

## ZeroMQ 适配器设计

ZeroMQ v1 使用 ROUTER/DEALER 拓扑：

- 每个启用 ZeroMQ 的节点绑定一个 ROUTER socket。
- 静态 peer 和动态 discovered peer 使用 DEALER socket 主动拨号。
- DEALER 每次发送一个完整 payload frame，内容为已签名 protobuf `Envelope` bytes。
- ROUTER 接收 `[identity, payload]` 后，把该 identity 映射为一个入站 `TransportConn`。
- ROUTER 向入站连接发送时使用 `[identity, payload]`。

关键实现规则：

- 每个 ZeroMQ socket 只能由 adapter 内部固定 goroutine 操作，避免跨 goroutine 直接读写 socket。
- ROUTER 写入必须经过单一发送队列串行化。
- DEALER 写入也通过连接内发送队列串行化。
- `Close()` 需要让 goroutine 退出，并设置 `linger = 0`，避免进程关闭时阻塞。
- 建议设置 `immediate = 1`，避免未连接时无限积压。
- 建议启用 TCP keepalive，并暴露发送失败、接收失败、队列满、连接关闭等日志事件。
- 单条 payload 上限保持与当前 WebSocket read limit 对齐，建议 v1 继续使用 8 MiB。

推荐 socket option：

| 选项 | 建议值 | 目的 |
| --- | --- | --- |
| `LINGER` | `0` | 关闭时不等待未发送消息。 |
| `IMMEDIATE` | `1` | 未连接时尽早暴露发送失败。 |
| `SNDHWM` / `RCVHWM` | 与现有队列规模同量级 | 避免内存无界增长。 |
| `TCP_KEEPALIVE` | `1` | 让底层 TCP 尽早发现异常链路。 |

Go 绑定默认采用 `github.com/pebbe/zmq4`。该绑定依赖本机 `libzmq` 和 CGO。

## 连接身份与自连处理

ZeroMQ 试点不通过配置预先排除自连，而是在协议握手阶段处理：

1. 连接建立后双方仍先交换 `Hello`。
2. 如果收到的 `Hello.node_id` 等于本节点 `node_id`，关闭 session，并记录 `self_peer_ignored` 日志。
3. 如果是静态或动态拨号连接，`Hello.node_id` 仍会写回对应 `configuredPeer.nodeID`。
4. 如果同一 URL 后续返回不同 `node_id`，继续按现有 peer identity mismatch 规则拒绝。

这样可以避免为 ZeroMQ 增加 `advertise_url`，也避免通过本机 bind 地址推导公网可拨号地址。

## Membership 与 discovery

ZeroMQ 接入后，membership 的语义仍然是“传播已知可拨号 peer 地址”，不是“广播本机 listener 地址”。

需要调整的规则：

- `normalizePeerURL` 允许 `zmq+tcp`。
- `discovered_peers.url` 可以保存 `zmq+tcp://host:port`。
- membership update 可以传播已经绑定 `node_id` 的静态 ZeroMQ peer URL。
- membership update 可以传播状态为 `connected` 的 discovered ZeroMQ peer URL。
- membership update 可以继续传播其他 peer 广告过、且 `node_id` 等于当前节点的 self-known URL。
- membership 不主动把 `[services.zeromq].bind_url` 加入广告。
- 收到指向本节点 `node_id` 的 `zmq+tcp` advertisement 时，仍只写入 `selfKnownURLs`，不拨号自己。
- 动态 discovered peer 调度按 URL scheme 选择 transport；outbound-only 模式下仍允许回拨 discovered 的 `zmq+tcp` peer。
- 当 ZeroMQ 完全禁用时，已持久化的 `zmq+tcp` discovered peer 只保留记录，不启动动态拨号。

不做的事情：

- 不从 ZeroMQ `RemoteAddr` 推断 peer 的公网可拨号地址。
- 不做 NAT 穿透。
- 不把 `tcp://0.0.0.0:9090` 改写成任何自动广告地址。

## 配置与部署

配置解析需要新增：

- `services.zeromq.enabled`。
- `services.zeromq.bind_url`。
- `services.zeromq.security` 与 `services.zeromq.curve.*`。
- `cluster.peers.zeromq.curve_server_public_key`。
- 静态 peer URL 支持 `zmq+tcp`。

Docker 需要新增运行依赖：

- builder 阶段安装 `zeromq-dev`。
- runtime 阶段安装 `zeromq`。
- 默认 Compose 不暴露 ZeroMQ 端口；文档给出可选示例。

示例：

```yaml
services:
  notifier:
    ports:
      - "8080:8080"
      # 仅在启用 ZeroMQ 时打开
      # - "9090:9090"
```

## 观测与运维

建议新增或扩展以下日志字段：

- `transport`：`websocket` 或 `zeromq`。
- `peer_url`：静态或发现到的拨号 URL。
- `bind_url`：ZeroMQ listener 本机监听地址。
- `remote_addr`：传输层观察到的远端地址或 ZeroMQ identity 摘要。
- `connection_id`：沿用当前 session connection id。

建议新增或扩展以下指标：

- 当前按 transport 聚合的活跃 peer session 数。
- ZeroMQ listener 是否运行。
- discovery 中按 URL scheme 聚合的候选数量。
- ZeroMQ 发送队列长度或队列满次数。
- ZeroMQ 收发错误总数。

排障优先级：

1. 确认 `[services.zeromq].enabled`、`zeromq_mode` 和 `zeromq_listener_running`。
2. 确认远端 `[[cluster.peers]].url` 使用 `zmq+tcp://host:port`，且 host 不是通配地址。
3. 确认容器或防火墙暴露了对应 TCP 端口。
4. 确认两端 `cluster.secret` 一致。
5. 查看 `Hello` 是否通过、是否被 self peer 或 identity mismatch 拒绝。
6. 查看校时状态和写闸门是否进入可信或观察状态。

## 测试计划

配置测试：

- 未配置 `[services.zeromq]` 时默认禁用。
- `enabled = false` 且存在 `zmq+tcp` 静态 peer 时拒绝启动。
- `enabled = true` 且缺失 `bind_url` 时允许启动，并进入 outbound-only 模式。
- `bind_url = "tcp://0.0.0.0:9090"` 合法。
- `bind_url` 使用非 `tcp` scheme、缺失 port、包含 path/query/fragment 时拒绝。
- `cluster.peers.url` 允许 `ws`、`wss` 和合法 `zmq+tcp`。
- `cluster.peers.url` 中的 `zmq+tcp://0.0.0.0:9090`、带 path、带 query、带 fragment 均拒绝。
- `security = "curve"` 时，静态 `zmq+tcp` peer 缺少 `zeromq.curve_server_public_key` 应拒绝启动。
- `security = "curve"` 且 `bind_url` 非空时，`allowed_client_public_keys` 不能为空。
- 重复 peer URL 继续拒绝。
- 未声明的 TOML 字段继续拒绝。

传输契约测试：

- WebSocket adapter 与 ZeroMQ adapter 都能发送和接收单条 payload。
- 两个 adapter 都能传输接近当前上限的大 payload。
- 并发 enqueue 不导致 socket 并发访问。
- 远端关闭后 `Receive` 和 `Send` 返回明确错误。
- 本地关闭后 goroutine 能退出。
- ZeroMQ ROUTER 能把不同 identity 的入站消息分发到正确连接。
- CURVE 模式下，合法 client public key 可以完成收发，未在白名单内的 client public key 不会进入 mux accept。
- CURVE 模式下，membership advertisement 携带 ZeroMQ server public key，discovered peer 可安全动态拨号。

协议与集成测试：

- 传输抽象后现有 `Hello`、`Ack`、补拉、快照、校时和路由测试全部保持通过。
- 双节点 ZeroMQ 静态 peer 可以完成握手和实时复制。
- 双节点 ZeroMQ 断线恢复后可以按 `origin_cursors` 补拉。
- 三节点混合 WebSocket 与 ZeroMQ peer 时，membership、动态路由和状态接口中的 transport/mode 字段符合预期。
- 连接到自己时通过 `Hello.node_id` 识别并关闭，不形成活跃 peer。
- ZeroMQ 连接中断后恢复，`unconfirmed_events` 最终下降，状态收敛。

验收命令：

```bash
go test ./...
go test -tags zeromq ./internal/cluster ./internal/api ./cmd/notifier
go build -tags zeromq ./cmd/notifier
docker build --build-arg ENABLE_ZEROMQ=true -t turntf-notifier:zeromq-smoke .
```

如果本地未安装 `libzmq`，实现阶段应在文档中明确跳过 ZeroMQ adapter 集成测试的方式，或使用构建标签隔离需要 `libzmq` 的测试。

## 分阶段实施顺序

1. 新增传输抽象和 WebSocket adapter，保持现有功能与测试全绿。
2. 扩展配置结构和 URL 校验，但默认仍不启用 ZeroMQ。
3. 引入 `github.com/pebbe/zmq4`，实现 ZeroMQ ROUTER/DEALER adapter。
4. 把 `dialLoop` 改为按 peer URL scheme 选择 transport。
5. 在 `Manager.Start` 中只在 `enabled = true` 且 `bind_url` 非空时启动 ZeroMQ listener。
6. 扩展 discovery 对 `zmq+tcp` 的支持，但不广播 `bind_url`。
7. 补测试、Docker 依赖、README、运维文档和 peer discovery 文档。

## 回滚策略

ZeroMQ 是实验性传输，必须支持配置级回滚：

1. 设置 `services.zeromq.enabled = false`，或删除 `[services.zeromq]` 段。
2. 从 `[[cluster.peers]]` 中移除 `zmq+tcp` peer。
3. 保留至少一个可达的 `ws` 或 `wss` 静态 peer 作为集群入口。
4. 重启节点后，节点应通过 WebSocket 恢复连接，并按现有补拉和反熵机制追平状态。

如果已经持久化了 `zmq+tcp` discovered peer，关闭 ZeroMQ 后应忽略或标记这些候选，不应影响 WebSocket peer 建链。

## 风险与后续方向

- `pebbe/zmq4` 依赖 CGO 和系统 `libzmq`，会增加镜像、交叉编译和本地开发环境要求。
- CURVE 依赖 libzmq 构建时启用 CURVE；如果 `zmq4.HasCurve()` 为 false，CURVE 模式必须启动失败。
- ZeroMQ 不提供应用内 TLS socket 选项；需要 TLS 证书体系时，应在外层使用 TCP TLS 隧道或改用 WebSocket `wss`。
- ZeroMQ 没有直接等价于 WebSocket ping/pong 的应用层心跳；仍需要依赖现有 `TimeSyncRequest`、TCP keepalive 和传输错误观测。
- ROUTER/DEALER 的 identity 管理比 WebSocket 复杂，必须通过 adapter 隔离，不能泄漏到协议层。
- 如果后续要支持 `ipc://` 或 `inproc://`，必须重新评估配置校验、Docker 权限和 discovery 语义。

## 参考

- ZeroMQ Go 官方页面列出 `pebbe/zmq4` 与 `goczmq` 等 Go 路线：https://zeromq.org/languages/go/
- `pebbe/zmq4` 是 `libzmq` wrapper，需要本机 ZeroMQ 开发文件和 CGO：https://github.com/pebbe/zmq4
- ZMTP 是 ZeroMQ 的帧化消息传输协议，安全能力不应被视为默认具备：https://rfc.zeromq.org/spec/13/
