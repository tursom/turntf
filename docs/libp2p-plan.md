# libp2p P2P 集群接入说明

本文档记录当前 libp2p 接入的配置、传输语义、地址传播边界和回滚方式。libp2p 是 WebSocket/ZeroMQ 之外的并行集群 transport，不改变 `Envelope`、HMAC、`Hello`、校时、补拉、反熵、快照或动态路由语义。

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
```

启用后，静态 peer 可以继续使用 `ws://`、`wss://`、`zmq+tcp://`，也可以使用原生 libp2p multiaddr：

```toml
[[cluster.peers]]
url = "/ip4/10.0.0.12/tcp/4001/p2p/12D3KooW..."
```

静态 libp2p peer 必须包含 `/p2p/<peer_id>`。`listen_addrs` 是本机绑定地址，不能包含 `/p2p`，也不会直接进入 membership 广告。

## 传输语义

libp2p stream 使用协议 ID `/turntf/cluster/stream/1.0.0`，包装为现有 `TransportConn`：

- `Send` 和 `Receive` 使用 4 字节 big-endian 长度前缀保存消息边界。
- 单帧上限沿用 WebSocket 的 8 MiB 限制。
- `Transport()` 返回 `libp2p`，状态接口和指标按该 transport 聚合。
- 入站和出站 stream 都会记录远端 PeerID，并在 `Hello` 通过时建立 `node_id <-> PeerID` 绑定。

Gossipsub topic 使用 `/turntf/<cluster_hash>/events/v1`，其中 `cluster_hash = hex(sha256("turntf/libp2p/" + cluster.secret))[:16]`。本地写入成功后，事件会同时发往现有 stream session 和 Gossipsub topic。Gossipsub 收到的消息必须通过 HMAC 校验，并且只接受已有可信 stream 绑定的 `node_id/PeerID`，否则丢弃；Ack、PullEvents、SnapshotDigest、SnapshotChunk、TimeSync、MembershipUpdate、RoutingUpdate、QueryLoggedInUsers 和 TransientPacket 仍只走 stream。

## 地址传播

libp2p 地址传播遵循“别人眼中的我”模型：

- 本节点不会从 `listen_addrs`、入站 `RemoteAddr`、NAT 观测地址或容器内地址推断自己的可传播地址。
- 如果节点 A 配置并成功拨通节点 B 的 `/ip4/.../p2p/<B>`，且 B 通过 `Hello/HMAC/校时`，A 可以向集群传播 B 的这个 multiaddr。
- 如果某个 advertisement 指向本节点自己的 `node_id`，本节点只把它放入 `selfKnownURLs`，用于继续传播其他节点验证过的“别人眼中的我”，不会触发自连。
- DHT、mDNS 或 relay 得到的 multiaddr 先作为内存候选；只有 stream 打开并通过握手、校时和身份绑定后，才会持久化为 discovered peer。

## 私有发现

DHT 使用由 `cluster.secret` 派生的 protocol prefix，不连接公共默认 bootstrap。`bootstrap_peers` 和 `relay_peers` 都必须显式配置为集群内可信入口。mDNS 默认关闭，适合受控局域网测试或部署时按需开启。

relay 和 hole punching 只有在 `relay_peers` 非空时才启用；默认不会启动公共 relay、公共 DHT 或任意公网 bootstrap。

## 运维观测

`GET /ops/status` 的 `discovery` 字段会暴露：

- `libp2p_mode`
- `libp2p_peer_id`
- `libp2p_listen_addrs`
- `libp2p_verified_addrs`
- `libp2p_dht_enabled`
- `libp2p_dht_bootstrapped`
- `libp2p_gossipsub_topic`
- `libp2p_gossipsub_peers`
- `libp2p_relay_enabled`
- `libp2p_hole_punching`

`/metrics` 同时暴露 `notifier_libp2p_enabled`、`notifier_libp2p_gossipsub_peers` 和 `notifier_libp2p_dht_bootstrapped`。

## 回滚

将 `services.libp2p.enabled = false` 后重启即可回滚。已有 WebSocket/ZeroMQ 配置、发现记录、补拉、反熵和复制语义保持可用。`private_key_path` 文件可以保留；再次启用 libp2p 时会复用同一个 PeerID。
