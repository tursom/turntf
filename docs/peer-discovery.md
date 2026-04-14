# Peer 自动发现

本文档描述当前 peer 自动发现的实现边界、协议流程、持久化状态和运维观测方式。自动发现只解决“节点如何从已连接 peer 处获得更多可拨号地址，并尝试建立连接”的 membership/bootstrap 问题；它不改变事件复制、补拉、反熵、动态路由、HMAC 鉴权或校时保护的语义。

## 目标与边界

- 自动发现默认随集群模式启用，运行时配置文件暂不提供单独开关；测试中可通过 `cluster.Config.DiscoveryDisabled` 关闭。
- 自动发现与静态 `cluster.peers` 并存。静态 peer 仍是最可靠的种子入口，发现到的 peer 只作为动态拨号候选。
- 自动发现不会从入站连接的 `RemoteAddr` 推断公网地址，也不会做 NAT 穿透、DNS-SD、注册中心或外部服务发现。
- 自动发现只传播已经绑定 `node_id` 的可拨号 WebSocket URL。节点至少需要通过静态 peer 或本地历史持久化记录知道某个可用入口，集群才有传播起点。
- 自动发现广告不能绕过身份校验。候选地址真正连上后仍必须通过 `Hello`、协议版本、HMAC、校时和 peer identity 绑定检查。
- 当前协议不支持与旧快照/旧协议版本节点混跑；节点不会主动向未声明 `supports_membership` 的 session 发送 membership update。

## 术语

- 静态 peer：来自 `[[cluster.peers]]` 的配置项，状态中 `source = "static"`。
- 发现 peer：从 membership advertisement 或本地 `discovered_peers` 表恢复出来的候选地址，状态中 `source = "discovered"`。
- 入站 peer：对端主动连入，但当前节点没有该 peer 的可拨号 URL，状态中 `source = "inbound"`。
- membership update：节点间通过 `Envelope.membership_update` 发送的成员广告消息。
- peer advertisement：membership update 中的一条候选地址，包含 `node_id`、`url`、`generation` 和观测时间。
- 动态拨号器：自动发现为候选地址启动的 `dialLoop`，当前每个节点最多保留 8 个。

## 协议流程

1. 节点启用集群模式后创建 `Manager`，从本地 SQLite `discovered_peers` 表加载历史发现记录。
2. 节点启动静态 peer 的拨号循环；如果存在历史发现记录，发现循环也会尝试为未标记 `expired` 的候选启动动态拨号循环。
3. WebSocket 建立后，双方先交换 `Hello`。`Hello.supports_membership = true` 表示该 session 支持自动发现。
4. `Hello` 通过后，session 会绑定远端 `node_id`。如果是静态或动态拨号连接，绑定结果会写回对应 `configuredPeer.nodeID`，并防止同一 URL 与不同 `node_id` 混用。
5. bootstrap 阶段完成校时和同步准备后，节点会把该 peer 记录为 `connected`，并立即向支持 membership 的 session 发送一次 membership update。
6. 发现循环每 5 秒执行一次：过期候选、补齐动态拨号器、向所有支持 membership 的活跃 session 广播 membership update。
7. 收到 membership update 后，节点先做 envelope 校验，并要求 `membership_update.origin_node_id` 等于当前 session 的 `peerID`。
8. 每条 peer advertisement 会被规范化和验证：URL 不能为空，scheme 只能是 `ws` 或 `wss`，host 不能为空，fragment 不允许出现。
9. 如果 advertisement 指向当前节点自己的 `node_id`，节点只把该 URL 放入内存中的 `selfKnownURLs`，用于后续继续传播“别人眼中的我”；不会拨号自己。
10. 如果 advertisement 指向其他节点，节点会记录或更新发现候选，持久化到 `discovered_peers`，并在下一轮 reconcile 中按规则启动动态拨号。

membership update 当前会广播三类地址：

- 已完成握手并绑定 `node_id` 的静态 peer URL。
- 状态为 `connected` 的发现 peer URL。
- 其他 peer 曾经广告过、且 `node_id` 等于当前节点的 URL，也就是 `selfKnownURLs`。

## 状态机

发现记录的 `state` 使用字符串保存，便于直接暴露到 `/ops/status`、`/metrics` 和 SQLite：

| 状态 | 含义 |
| --- | --- |
| `candidate` | 已从 membership update 或持久化记录得到候选地址，尚未开始拨号或等待下一轮 reconcile。 |
| `dialing` | 已为候选地址启动动态拨号循环，正在尝试建立 WebSocket 连接。 |
| `connected` | 候选地址已经成功建立 session，并通过 bootstrap。 |
| `failed` | 动态拨号失败或 session 关闭，`last_error` 会记录最近错误。 |
| `expired` | 候选地址超过 10 分钟没有再次被观测到，暂不继续拨号。 |

状态转换的关键规则：

- 新广告默认进入 `candidate`；如果原状态是 `failed` 或 `expired`，再次收到广告会重新回到 `candidate`。
- 动态拨号启动时进入 `dialing`；拨号失败进入 `failed`；bootstrap 成功进入 `connected`。
- 非 `connected` 且非 `expired` 的候选，如果 `last_seen_at` 超过 10 分钟没有刷新，会进入 `expired` 并记录 `candidate expired`。
- 已连接 peer 不会因为没有继续收到广告而被自动过期；连接生命周期仍由 WebSocket、心跳、校时和复制状态机管理。

## 候选筛选与拨号

发现循环在启动动态拨号器前会过滤候选：

- 跳过 `node_id <= 0` 或等于本节点 `node_id` 的记录。
- 跳过 URL 已存在于静态 `cluster.peers` 的记录。
- 跳过已经存在动态拨号器、正在拨号、已过期或同 `node_id` 已有活跃 session 的记录。
- 候选按 `last_connected_at`、`last_seen_at` 和 URL 排序，优先拨最近成功连接过、最近被观测到的地址。
- 每个节点最多启动 8 个动态发现拨号循环，避免 membership 抖动时无限扩张连接数。

动态拨号连接和静态拨号连接使用同一套 `dialLoop`、`Hello`、HMAC、校时、复制和反熵逻辑。广告中的 `node_id` 会成为动态拨号时的期望身份；如果真正握手返回的 `node_id` 不一致，连接会失败。

## 持久化

自动发现结果保存在 SQLite 表 `discovered_peers` 中。即使 `store.engine = "pebble"`，该表也仍然位于 SQLite，因为它属于节点本地控制面状态。

主要字段：

| 字段 | 含义 |
| --- | --- |
| `node_id` | 被发现 peer 的节点身份。 |
| `url` | 规范化后的 WebSocket URL。 |
| `source_peer_node_id` | 最近一次提供该广告的 peer。 |
| `state` | 当前发现状态。 |
| `first_seen_at_hlc` | 首次写入该发现记录的 HLC 时间戳。 |
| `last_seen_at_hlc` | 最近一次观测、拨号或状态更新的 HLC 时间戳。 |
| `last_connected_at_hlc` | 最近一次成功连接时间；后续失败不会清空该字段。 |
| `last_error` | 最近一次发现、拨号或过期错误。 |
| `generation` | membership 代数，更新时保留较大值。 |

写入规则：

- `UpsertDiscoveredPeer` 以 `(node_id, url)` 为主键做幂等更新。
- 如果新状态没有携带 `last_connected_at`，已有的最近连接时间会保留。
- `generation` 只会向前推进，不会被较小值覆盖。
- 节点重启时会重新加载表内记录，并继续尝试未过期、可拨号的候选。

## 运维接口

`GET /cluster/nodes` 返回当前节点视角下已连接的节点，字段包括：

- `node_id`：节点身份。
- `is_local`：是否为当前节点。
- `configured_url`：兼容旧字段名。对静态 peer 是配置 URL；对发现 peer 是发现到并已连接的 URL。
- `source`：`static`、`discovered` 或空值。

`GET /ops/status` 的顶层 `discovery` 字段包括：

- `discovered_peers`：本节点内存中发现记录数量。
- `dynamic_peers`：当前由发现机制启动的动态拨号器数量。
- `membership_updates_sent`：已发送 membership update 次数。
- `membership_updates_received`：已接收 membership update 次数。
- `rejected_total`：被拒绝的 peer advertisement 数量。
- `persist_failures_total`：发现记录持久化失败次数。
- `peers_by_state`：按发现状态聚合的记录数。

`GET /ops/status` 的每个 peer 也会额外暴露：

- `source`：peer 来源，可能是 `static`、`discovered` 或 `inbound`。
- `discovered_url`：发现记录中的 URL。
- `discovery_state`：发现状态。
- `last_discovered_at`：最近观测到该候选的时间。
- `last_connected_at`：最近成功连接该候选的时间。
- `last_discovery_error`：最近发现或拨号错误。

## Prometheus 指标

当前 `/metrics` 暴露以下自动发现指标：

- `notifier_discovered_peers{node_id}`：本节点已知发现记录数。
- `notifier_discovered_peers_by_state{node_id,state}`：按状态聚合的发现记录数。
- `notifier_dynamic_peer_dialers{node_id}`：动态发现拨号器数量。
- `notifier_membership_updates_sent_total{node_id}`：membership update 发送总数。
- `notifier_membership_updates_received_total{node_id}`：membership update 接收总数。
- `notifier_membership_advertisements_rejected_total{node_id}`：被拒绝的广告总数。
- `notifier_discovered_peer_persist_failures_total{node_id}`：发现记录持久化失败总数。

## 日志事件

排查自动发现时可优先搜索这些日志事件：

- `membership_update_received`：收到并处理 membership update。
- `membership_advertisement_ignored`：某条广告被拒绝，日志中会包含被广告的 `node_id`、`url` 和原因。
- `peer_discovery_recorded`：session bootstrap 后记录发现状态。
- `discovered_peer_persist_failed`：写入 `discovered_peers` 失败。
- `peer_dial_started`、`peer_dial_failed`、`peer_dial_succeeded`：动态和静态拨号都会使用这些事件，可结合 `peer_url` 与 `direction` 排查。

## 部署建议

- 至少配置一组可连通的静态种子 peer。自动发现可以减少全量配置，但不能在完全没有入口的情况下凭空发现节点。
- 推荐让每个节点至少能通过一个静态 peer 或历史发现记录进入集群，再由 membership update 补齐其他节点。
- `cluster.peers.url` 必须是其他节点可实际拨通的完整 `ws://` 或 `wss://` URL，路径通常等于对端 `cluster.advertise_path`。
- 反向代理必须支持 WebSocket 升级，并保持集群内部 HMAC secret 一致。
- 如果节点的对外地址发生变化，至少需要有一个已连接 peer 广告新 URL；旧 URL 会保留为失败或过期记录，当前没有自动删除表记录的运维 API。
- 备份 SQLite 时会同时备份 `schema_meta.node_id` 和 `discovered_peers`。恢复节点身份时不要把同一份 SQLite 同时启动成两个实例。

## 常见排查

- `discovered_peers = 0` 且 `membership_updates_received = 0`：先确认至少有一个 peer 已连接并完成 bootstrap，再检查协议版本、`supports_membership`、`cluster.secret` 和网络访问。
- `rejected_total` 持续增长：检查广告 URL 是否为空、是否使用非 `ws/wss` scheme、是否缺少 host、是否包含 fragment，或广告来源是否与 session peer 身份不一致。
- 候选长期停在 `candidate`：检查是否已达到 8 个动态拨号器上限，或同 `node_id` 是否已经存在活跃连接。
- 候选进入 `failed`：查看 `last_discovery_error` 和 `peer_dial_failed` 日志，通常是网络不可达、TLS/代理配置错误、HMAC 不一致、协议版本不一致或握手返回的 `node_id` 不符合广告。
- 候选进入 `expired`：说明 10 分钟内没有再次收到该候选广告。检查提供该广告的源 peer 是否仍在线，或该地址是否已经不再被任何已连接节点传播。
- `/cluster/nodes` 能看到 `source = "discovered"` 但复制进度不前进：自动发现只负责建链，后续仍按复制、补拉、反熵和校时状态排查。
