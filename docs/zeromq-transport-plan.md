# ZeroMQ 传输接入说明

本文档记录当前 ZeroMQ 接入的已落地基线、mux 协议、client / cluster 入口、mixed transport / mesh 边界，以及仍未实现的部分。文件名沿用早期 `plan` 命名，但正文以当前实现为准，不再把已经落地的 ZeroMQ 能力当成“未来计划”描述。ZeroMQ 是 WebSocket / libp2p 之外的并行 transport，不改变 `ClientEnvelope/ServerEnvelope`、cluster HMAC、`MeshNodeHello`、校时、补拉、反熵、快照或路由的基础语义。

## 配置

ZeroMQ 默认关闭：

```toml
[services.zeromq]
enabled = false
bind_url = "tcp://0.0.0.0:9090"
forwarding_enabled = true
security = "none"

[services.zeromq.curve]
server_public_key = ""
server_secret_key = ""
client_public_key = ""
client_secret_key = ""
allowed_client_public_keys = []
```

启用后，静态 cluster peer 可以使用 `zmq+tcp://host:port`：

```toml
[[cluster.peers]]
url = "zmq+tcp://10.0.0.12:9090"
zeromq = { curve_server_public_key = "" }
```

当前配置实现有几个容易误判的边界：

- `services.zeromq.bind_url` 只表示本机 ROUTER socket 的监听地址，必须是 `tcp://host:port`；它不会直接写入 discovery，也不会被自动改写成公网地址。
- `cluster.peers[].url`、membership 和 discovery 中的 ZeroMQ 地址统一使用 `zmq+tcp://host:port`；服务端在真正调用 libzmq `Connect()` 时，会在内部把它转换回 `tcp://host:port`。
- 只要 `cluster.peers[].url` 使用 `zmq+tcp://`，就必须同时启用 `services.zeromq.enabled = true`，否则配置校验会直接失败。
- `enabled = true` 且 `bind_url = ""` 时，ZeroMQ 进入 `outbound_only` 模式：不会启动本地 listener，因此也没有 ZeroMQ 客户端入口，但仍保留“可作为 cluster 出站 transport”的配置能力。
- `services.zeromq.forwarding_enabled` 为 `nil` 时会跟随全局 `cluster.forwarding.enabled`；显式设为 `false` 时，ZeroMQ 不再参与 mesh transport capability、不会为 `zmq+tcp` 静态或 discovered peer 建立出站 mesh 拨号，也不会对外广告 ZeroMQ endpoint。
- `forwarding_enabled = false` 不会阻止 `bind_url` 上的 mux listener 启动；它只会让 ZeroMQ 的 cluster 侧能力失效。也就是说，这种模式下可以保留业务客户端 ZeroMQ 入口，但不应再把它当作 cluster transport 使用。
- `security = "curve"` 时，当前实现要求本节点同时提供 server/client 两组 Z85 key；如果 `bind_url` 非空，还要求 `allowed_client_public_keys` 非空。
- 在 `security = "curve"` 下，静态 `zmq+tcp` peer 还必须携带 `cluster.peers[].zeromq.curve_server_public_key`；发现到的 `zmq+tcp` 候选也只有在记录了远端 server public key 后才会参与动态拨号。

## mux 协议与连接入口

当前 ZeroMQ 不是分离的“客户端端口”和“集群端口”，而是通过一个共享的 `ZeroMQMuxListener` 在同一个 `bind_url` 上复用两类连接。第一帧必须先发送 `notifier.transport.v1.ZeroMQMuxHello`：

```protobuf
ZeroMQMuxHello {
  role: ZERO_MQ_ROLE_CLUSTER | ZERO_MQ_ROLE_CLIENT
  protocol_version: "zeromq-mux-v1"
}
```

当前 mux 协议版本常量是 `zeromq-mux-v1`。`role` 只负责把入站连接路由到 cluster 或 client 处理栈；它本身不是节点身份协商，也不替代后续的 `MeshNodeHello`、HMAC 或业务登录。

### cluster 入口

- cluster 出站连接使用 `zmq+tcp://host:port` 配置和 discovery URL，底层通过 DEALER socket 建链。
- cluster 入站连接统一落在 `services.zeromq.bind_url` 对应的 ROUTER socket 上，首帧必须是 `ZeroMQMuxHello{role=ZERO_MQ_ROLE_CLUSTER}`。
- mux 分流完成后，连接会进入 `Manager.AcceptZeroMQConn()`，再交给 mesh runtime 继续处理；当前没有“绕过 mesh 的旧式 ZeroMQ 集群会话栈”。
- 真正的 cluster 身份绑定、协议版本检查、HMAC、校时和路由能力协商都发生在 mux 之后，仍沿用现有 mesh / cluster 语义。
- 如果 mesh runtime 不可用，或者 `forwarding_enabled = false` 使 ZeroMQ adapter 未注册，cluster 角色连接会在 mux 层接入后被关闭，而不是自动降级到其他旧链路。

### client 入口

- 业务客户端同样复用 `services.zeromq.bind_url` 对应的 ROUTER socket，首帧必须是 `ZeroMQMuxHello{role=ZERO_MQ_ROLE_CLIENT}`。
- mux hello 之后，第二帧必须是 `ClientEnvelope.login`，后续直接复用与 WebSocket 标准流相同的 `ClientEnvelope / ServerEnvelope` 协议。
- mux 分流完成后，客户端必须在 45 秒内完成登录；超时会关闭连接，且不会注册在线状态或 `session_ref`。
- 当前客户端协议版本常量是 `client-v1alpha5`；mux hello 之后的 `LoginRequest` 必须显式声明该值，空值或不匹配版本会在认证和会话注册前被拒绝。
- ZeroMQ 客户端连接进入的处理栈等价于 WebSocket `GET /ws/client` 标准流，而不是 `GET /ws/realtime`。
- 如果客户端只想关闭历史补发和后续 `MessagePushed`，应在 `LoginRequest` 中设置 `transient_only = true`；这不会把 ZeroMQ 连接切换成 `/ws/realtime` 那种“受限 RPC 集”。
- `/ws/client` 和 `/ws/realtime` 仍然保留为 WebSocket 路径；ZeroMQ 是并行入口，不替代现有 WebSocket 长连接路径。

如果服务端启用了 `services.zeromq.security = "curve"`，无论是 cluster 还是 client 角色，底层连接在发送 mux hello 之前都必须先完成 CURVE socket 配置。CURVE 只提供链路加密和传输层公钥白名单，不替代 cluster HMAC，也不替代业务客户端的 `LoginRequest` 身份验证。

## 连接存活与清理

- ZeroMQ ROUTER 和 DEALER 数据 socket 每 15 秒发送一次 ZMTP heartbeat，链路在 45 秒内没有响应时由 libzmq 判定断开；TCP keepalive 仍作为更底层的补充。
- ROUTER 使用 `ROUTER_NOTIFY(DISCONNECT)` 将 TCP 断开映射到具体 routing identity，并立即关闭对应的 `TransportConn`；`ROUTER_MANDATORY` 使向已不可达 identity 的发送显式失败。
- 出站 DEALER 使用一对一 socket monitor 监听 `EVENT_DISCONNECTED`。断线后旧逻辑连接会结束，由 mesh runtime 清除邻接并按既有 seed 重拨流程重新发送 mux hello 和 `MeshNodeHello`。
- 已登录业务连接关闭时，会话层会注销本地在线用户、集群 presence、`session_ref` 和持久化推送注册。正常存活但没有应用层消息的客户端不会因业务空闲被关闭，ZMTP heartbeat 不改变应用层 Ping/Pong 语义。

## mesh 与 mixed transport 边界

ZeroMQ 现在不是旁路试验入口，而是 mesh runtime 的正式 transport 之一。`Manager.Start()` 成功后，WebSocket、ZeroMQ 和 libp2p 会一起进入 mesh 拓扑、路由与观测面。

不过 mixed transport 不是“所有流量都可随意跨桥”：

- 只要存在纯 ZeroMQ 直连或纯 ZeroMQ 多跳路径，`control_critical`、`control_query`、`transient_interactive`、`replication_stream`、`snapshot_bulk` 都可以按现有 mesh 语义路由。
- 真正受限的是 `cross_transport_bridge` 场景，也就是路径必须跨 `websocket -> zeromq`、`zeromq -> libp2p` 这类 transport bridge 时。
- 当前跨 transport bridge 只允许 `control_critical`、`control_query`、`transient_interactive`。
- `replication_stream` 和 `snapshot_bulk` 不会因为开启了 `bridge_enabled = true` 就自动跨 transport bridge；如果一条路径只有 `ws -> zeromq` 这类桥接链路，复制和快照会返回 `no route`，而不是自动降级成桥接转发。

这意味着当前 mixed transport 更适合控制面、查询和瞬时包转发；如果要让补拉复制或快照修复走通，目标节点之间仍需要存在“不依赖跨 transport bridge”的可达路径，例如纯 `websocket` 多跳链路，或纯 `zeromq` 多跳链路。

`services.zeromq.forwarding_enabled = false` 是另一个需要特别写清楚的边界：

- 它会让 ZeroMQ 从 mesh transport capability 中消失，不再对外广告 `zmq+tcp://...` endpoint。
- 它会阻止 `zmq+tcp` 静态 peer 和 discovered peer 参与 mesh 出站拨号。
- 它不会阻止共享 mux listener 服务业务客户端，因此可以作为“保留 ZeroMQ client 入口、关闭 ZeroMQ cluster transport”的配置方式。

## 地址传播与 discovery

ZeroMQ 地址传播遵循“传播已知可拨号地址，而不是广播本机监听地址”的规则：

- `bind_url` 只用于本地 ROUTER socket 绑定，不会直接进入 membership advertisement。
- 当且仅当 ZeroMQ forwarding 允许、且 `bind_url` 非空时，本地 mesh transport capability 才会广告由 `bind_url` 派生出的 `zmq+tcp://host:port` endpoint。
- discovery 持久化的 ZeroMQ 候选地址也是 `zmq+tcp://host:port`，不是裸 `tcp://...`。
- 如果某条 ZeroMQ advertisement 指向当前节点自己的 `node_id`，当前节点只会把它记入 `selfKnownURLs` 用于继续传播“别人眼中的我”，不会尝试自连。
- 当前实现不会从入站 `RemoteAddr`、NAT 观测地址、容器内地址或 `0.0.0.0` / `::` 之类通配监听地址推断可拨号 URL。
- 在 CURVE 模式下，如果 discovered peer 缺失 `zeromq_curve_server_public_key`，记录会保留，但不会启动动态拨号。

## 构建与部署

当前 ZeroMQ 代码受 `zeromq` build tag 保护，仓库同时保留了 `transport_zeromq_disabled.go` 存根实现：

- 手工构建需要显式带 tag，例如 `go build -tags zeromq ./cmd/turntf`。
- ZeroMQ 相关测试也需要显式带 tag，例如 `go test -tags zeromq ./internal/cluster ./internal/api ./cmd/turntf -count=1`。
- 如果二进制没有带 `zeromq` tag，但配置里实际需要 ZeroMQ listener 或 ZeroMQ 拨号，启动时会因为 transport 不可用而失败。
- 启用 ZeroMQ 的构建和运行环境必须提供 libzmq 4.3+，并支持 draft `ROUTER_NOTIFY` socket option；能力不可用时 listener 会明确启动失败，不会降级成无法感知 identity 断线的模式。

仓库自带 Dockerfile 已经接线了这套约束：

- builder 阶段在 `ENABLE_ZEROMQ=true` 时安装 `zeromq-dev`，并用 `-tags zeromq` 构建。
- runtime 阶段在 `ENABLE_ZEROMQ=true` 时安装 `zeromq` 运行库。
- 默认镜像构建参数就是 `ENABLE_ZEROMQ=true`，因此官方容器路径下 ZeroMQ 已经不是“未接线计划”。

部署时还需要注意：

- 业务客户端和 cluster 角色共用同一个 ZeroMQ 监听端口，因此 ACL、内网隔离和审计边界要按“共享入口”设计。
- ZeroMQ 当前不提供应用内 TLS，也没有 `zmq+tls` 之类新 scheme。需要 TLS 证书体系时，应在外层使用 TCP TLS 隧道，或者改用 WebSocket `wss`。
- CURVE 只解决 ZeroMQ 链路上的公钥握手与白名单，不会把公钥自动绑定成 `node_id` 或业务用户身份。

## 运维观测

`GET /ops/status` 的 `discovery` 字段会直接暴露 ZeroMQ 运行状态：

- `zeromq_mode`：`disabled`、`outbound_only` 或 `listening`
- `zeromq_security`：`none` 或 `curve`
- `zeromq_listener_running`：本地共享 mux listener 是否实际在运行

`GET /ops/status.mesh` 则是 ZeroMQ mixed transport 排障主入口，重点看：

- `transport_capabilities[*]`：确认本节点是否真的暴露了 `zeromq` capability，以及当前是否有 `advertised_endpoints`
- `routes[*]`：按目标节点和 `traffic_class` 查看 `reachable`、`outbound_transport`、`path_class`，判断路径是 `direct`、`same_transport_forward` 还是 `cross_transport_bridge`
- `metrics.bridge_forwards` 与 `metrics.routing_no_path`：前者适合确认控制面或瞬时包是否经过跨 transport bridge，后者适合确认复制/快照为什么被显式挡住

`/metrics` 当前已经暴露这些与 ZeroMQ 直接相关的指标：

- `notifier_zeromq_listener_running{node_id,mode,security}`
- 带 `transport="zeromq"` label 的 peer / origin / clock 指标
- `bridge_forward_total{node_id,traffic_class}`，用于观察跨 transport bridge 转发量

排障时可以优先关注这些日志事件：

- `zeromq_listener_started`
- `peer_inbound_accepted`
- `client_transport_connected`
- `client_login_failed`

## 回滚

完全回滚 ZeroMQ 的方式很直接：

1. 设置 `services.zeromq.enabled = false`，或删除整个 `[services.zeromq]` 段。
2. 从 `[[cluster.peers]]` 中移除 `zmq+tcp://...` 静态 peer。
3. 保留至少一条可达的 `ws://`、`wss://` 或 libp2p 静态入口，避免节点失去 cluster 种子。
4. 重启节点后，由其他 transport 按现有补拉、反熵和 mesh 路由语义恢复收敛。

如果只想关闭 ZeroMQ 的 cluster / mesh 角色，但继续保留业务客户端 ZeroMQ 长连接入口，可以采用更温和的方式：

1. 保持 `services.zeromq.enabled = true`。
2. 保持 `bind_url`，让共享 mux listener 继续服务客户端。
3. 设置 `services.zeromq.forwarding_enabled = false`。
4. 同时移除或停用所有 `zmq+tcp` cluster peer，避免其他节点继续把该端口当作 cluster transport 尝试接入。
