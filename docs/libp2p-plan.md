# libp2p P2P 集群接入说明

本文档记录当前 libp2p 接入的已落地基线、mixed transport / mesh 边界、地址传播规则和回滚方式。文件名沿用早期 `plan` 命名，但正文以当前实现为准，不再把已经落地的 libp2p 接入能力当成“未来计划”描述。libp2p 是 WebSocket/ZeroMQ 之外的并行集群 transport，不改变 `Envelope`、HMAC、`Hello`、校时、补拉、反熵、快照或动态路由的基础语义。

## 配置

libp2p 默认关闭：

```toml
[services.libp2p]
enabled = false
private_key_path = "./data/libp2p.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/4001"]
bootstrap_peers = []
enable_dht = true
enable_mdns = false
relay_peers = []
enable_hole_punching = true
gossipsub_enabled = true
native_relay_client_enabled = false
native_relay_service_enabled = false
```

启用后，静态 peer 可以继续使用 `ws://`、`wss://`、`zmq+tcp://`，也可以使用原生 libp2p multiaddr：

```toml
[[cluster.peers]]
url = "/ip4/10.0.0.12/tcp/4001/p2p/12D3KooW..."
```

静态 libp2p peer 必须包含 `/p2p/<peer_id>`，并且只要 `cluster.peers[].url` 使用 libp2p multiaddr，就必须同时启用 `services.libp2p.enabled = true`，否则配置校验会直接失败。`listen_addrs` 是本机绑定地址，不能包含 `/p2p`，也不会直接进入 membership 广告。

当前配置实现还有几个容易误判的边界：

- 如果 `services.libp2p.enabled = true`，并且没有显式打开任何 libp2p 附加能力，运行时会回填 `enable_dht = true`、`enable_hole_punching = true`、`gossipsub_enabled = true`；`enable_mdns` 仍默认关闭。
- `relay_peers` 控制是否在 host 侧打开 libp2p relay；`enable_hole_punching` 只有在 `relay_peers` 非空时才会真正生效。
- `native_relay_client_enabled` 和 `native_relay_service_enabled` 已经进入配置解析、mesh transport capability 和 `/ops/status` 暴露，但当前 `transport_libp2p.go` 的 host 启动参数还没有据此切换行为，因此它们现在更像“能力宣告/观测字段”，而不是已经接线到 libp2p host 的功能开关。

## 传输语义

libp2p stream 使用协议 ID `/turntf/cluster/stream/1.0.0`，包装为现有 `TransportConn`：

- `Send` 和 `Receive` 使用 4 字节 big-endian 长度前缀保存消息边界。
- 单帧上限沿用 WebSocket 的 8 MiB 限制。
- `Transport()` 返回 `libp2p`，状态接口和指标按该 transport 聚合。
- 入站和出站 stream 都会记录远端 PeerID，并在 `Hello` 通过时建立 `node_id <-> PeerID` 绑定。

Gossipsub topic 使用 `/turntf/<cluster_hash>/events/v1`，其中 `cluster_hash = hex(sha256("turntf/libp2p/" + cluster.secret))[:16]`。在当前 mesh 主链路下，控制面、查询、瞬时包、复制流和快照流统一通过 mesh runtime 在 transport stream 上逐跳转发；Gossipsub 不再承担生产复制主路径，只保留为兼容期能力和后续实验入口。Gossipsub 收到的消息仍必须通过 HMAC 校验，并且只接受已有可信 stream 绑定的 `node_id/PeerID`，否则丢弃。

## mesh 与 mixed transport 边界

libp2p 现在不是旁路实验入口，而是 mesh runtime 的正式 transport 之一。`Manager.Start()` 启动后，WebSocket、libp2p、ZeroMQ 会一起进入 mesh 拓扑与路由决策，`control_critical`、`control_query`、`transient_interactive`、`replication_stream`、`snapshot_bulk` 五类流量都通过同一套路由面判定是否可达。

不过 mixed transport 不是“所有流量都可随意跨桥”：

- 全局 `bridge_enabled` 当前默认开启。
- 但跨 transport bridge 只允许 `control_critical`、`control_query`、`transient_interactive`。
- `replication_stream` 和 `snapshot_bulk` 即使在 `bridge_enabled = true` 时也不会跨 transport bridge；如果一条路径只有 `ws -> libp2p`、`ws -> zeromq` 这类跨传输桥接链路，复制和快照会返回 `no route`，而不是自动降级成 bridge 转发。

这意味着当前 mixed transport 更适合控制面、查询和瞬时包转发；如果要让复制补拉或快照修复走通，目的节点之间仍需要存在“不依赖跨 transport bridge”的可达路径，例如纯 `websocket` 多跳链路，或纯 `libp2p` 多跳链路。

## 地址传播

libp2p 地址传播遵循“别人眼中的我”模型：

- 本节点不会从 `listen_addrs`、入站 `RemoteAddr`、NAT 观测地址或容器内地址推断自己的可传播地址。
- 如果节点 A 配置并成功拨通节点 B 的 `/ip4/.../p2p/<B>`，且 B 通过 `Hello/HMAC/校时`，A 可以向集群传播 B 的这个 multiaddr。
- 如果某个 advertisement 指向本节点自己的 `node_id`，本节点只把它放入 `selfKnownURLs`，用于继续传播其他节点验证过的“别人眼中的我”，不会触发自连。
- DHT、mDNS 或 relay 得到的 multiaddr 先作为内存候选，并加入 mesh dial seed；只有 stream 打开并通过握手、校时和身份绑定后，才会持久化为 discovered peer。

## 私有发现

DHT 使用由 `cluster.secret` 派生的 protocol prefix，不连接公共默认 bootstrap。`bootstrap_peers` 和 `relay_peers` 都必须显式配置为集群内可信入口。mDNS 默认关闭，适合受控局域网测试或部署时按需开启。

relay 和 hole punching 只有在 `relay_peers` 非空时才启用；默认不会启动公共 relay、公共 DHT 或任意公网 bootstrap。

## 运维观测

`GET /ops/status` 的 `discovery` 字段会暴露：

- `libp2p_mode`：当前取值是 `disabled`、`outbound_only`、`listening`
- `libp2p_peer_id`
- `libp2p_listen_addrs`：这里暴露的是运行时地址，会附带 `/p2p/<peer_id>`，不是原始配置里的 `listen_addrs`
- `libp2p_verified_addrs`
- `libp2p_dht_enabled`
- `libp2p_dht_bootstrapped`
- `libp2p_gossipsub_topic`
- `libp2p_gossipsub_peers`
- `libp2p_relay_enabled`
- `libp2p_hole_punching`

`GET /ops/status` 的 `mesh` 字段则是 mixed transport 排障主入口，重点看：

- `transport_capabilities[*]`：确认本节点当前是否真的暴露了 `libp2p` 能力、有哪些 `advertised_endpoints`，以及 `native_relay_*` 标记是否被带入 capability。
- `traffic_rules[*]`：确认当前各流量类别的 `allow` / `discourage` / `deny` 处置。
- `routes[*]`：按目标节点和 `traffic_class` 查看 `reachable`、`outbound_transport`、`path_class`、`estimated_cost`，判断某条 mixed path 是 `direct`、`same_transport_forward`、`cross_transport_bridge` 还是 `native_relay`。
- `metrics.routing_no_path` 与 `metrics.bridge_forwards`：前者适合确认复制/快照在 mixed bridge 上为什么失败，后者适合确认控制面/瞬时包是否真的经过跨 transport bridge。

`/metrics` 目前仍只直接暴露 `notifier_libp2p_enabled`、`notifier_libp2p_gossipsub_peers` 和 `notifier_libp2p_dht_bootstrapped` 这几项 libp2p 指标；mixed route 细节主要看 `GET /ops/status.mesh`。

## 回滚

将 `services.libp2p.enabled = false` 后重启即可回滚。已有 WebSocket/ZeroMQ 配置、发现记录、补拉、反熵和复制语义保持可用。`private_key_path` 文件可以保留；再次启用 libp2p 时会复用同一个 PeerID。
