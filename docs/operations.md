# 运维与上线手册

本文档记录分布式通知服务的小规模上线建议、备份策略、节点恢复流程和核心监控项。默认每个节点使用本地 SQLite，节点间通过 WebSocket + Protobuf 复制；事件日志和消息投影也可配置为 Pebble 后端。复制语义边界请先参考 [复制语义专题文档](/root/dev/sys/turntf/docs/replication-semantics.md)，时钟保护细节请参考 [时钟保护算法](/root/dev/sys/turntf/docs/clock-protection.md)。

peer 自动发现的协议、状态机和排查细节见 [peer 自动发现专题文档](/root/dev/sys/turntf/docs/peer-discovery.md)。

## 小规模部署建议

- 每个节点首次启动时会自动生成唯一的数字 `node_id` 并保存到 SQLite `schema_meta`；该 `node_id` 会完整进入 HLC 时间戳，同一集群内不能重复。
- 所有节点使用相同的 `auth.token_secret`，否则跨节点登录 token 无法互认。
- 所有节点使用相同的 `cluster.secret`，并确保它不同于 `auth.token_secret`。
- 生产环境建议所有节点使用相同的 `store.message_window_size`，避免消息反熵因窗口不一致而跳过消息分片修复。
- `store.engine` 默认 `sqlite`；配置为 `pebble` 时，事件日志和消息投影写入 Pebble，但用户、订阅、游标、pending projection 和运维统计仍写入 SQLite。
- 将 `api.listen_addr` 暴露给业务调用方，同时只允许可信节点访问 `cluster.advertise_path`。
- 至少保留一组可连通的静态 `cluster.peers` 作为自动发现种子；自动发现可以减少全量配置，但不能在没有入口的情况下凭空发现节点。
- 保持节点系统时钟同步。集群模式下，节点首次成功校时前会拒绝写入；时钟偏差超过 `cluster.max_clock_skew_ms` 会导致 peer 被拒绝或断开。
- 目标用户瞬时包依赖内存动态路由表和内存重试队列；节点重启后，未送达的 `route_retry` 瞬时包会直接丢失。
- 如果同一 peer 未来存在多条并行连接，瞬时包出口会按连接级 RTT 和抖动择优，不保证固定走某一条物理连接。
- 将 `GET /healthz` 接入存活探针，将 `GET /metrics` 接入带管理员 Bearer token 的指标抓取。
- 控制台日志为易读文本；如需持久化结构化日志，配置 `logging.file_path` 写入 JSON 行文件，并由 logrotate、容器运行时或日志平台负责轮转。

## 运维接口

- `GET /healthz`：公开存活检查，只返回服务进程是否可响应。
- `GET /cluster/nodes`：已登录接口，返回当前节点视角下已连接的集群节点列表，包含 `node_id`、`is_local`、`configured_url` 和 peer 来源 `source`。发现 peer 会把发现到的 URL 放在兼容字段 `configured_url` 中。
- `GET /cluster/nodes/{node_id}/logged-in-users`：已登录接口，查询某个节点当前 WebSocket 已登录用户列表，返回 `node_id`、`user_id`、`username`。
- `GET /ops/status`：管理员接口，返回本节点事件进度、peer 连接状态、自动发现状态、未确认事件数、反熵状态、冲突数和消息裁剪统计。
- `GET /metrics`：管理员接口，返回 Prometheus text exposition 格式指标。
- `GET /events?after=0&limit=100`：管理员接口，用于调试本地事件日志。

示例：

```bash
TOKEN="$(curl -sS -X POST http://127.0.0.1:8080/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"node_id":4096,"user_id":1,"password":"root"}' | jq -r .token)"

curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/ops/status
curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/metrics
curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/cluster/nodes
curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/cluster/nodes/4096/logged-in-users
```

## 预期延迟与异常判断

- 本地写接口返回成功，只代表当前节点本地持久状态已提交；不能据此判断 peer 已追平。
- `Ack` 只代表对端本地已应用到对应 `origin_node_id/event_id`，不代表全局提交完成。
- 节点重连后的短暂 `pending_catchup`、`unconfirmed_events` 或 `pending_snapshot_partitions` 非零，属于预期最终一致延迟。
- 反熵快照修复期间，不同节点短时间看到不同消息集合或订阅可见性差异，属于允许暂态；修复长期不完成才应视为异常。
- 若集群节点 `message_window_size` 不一致，用户与订阅仍应收敛，但消息窗口不承诺得到相同结果；这属于配置差异，不应按消息复制故障处理。
- 发往 `(node_id, 3)` 的瞬时包未送达时，应先按“在线路由尽力转发失败”排查，而不是按持久复制故障处理，因为它不参与事件日志、补拉或快照。
- peer 自动发现只负责补齐可拨号连接，不代表事件复制已经完成；发现 peer 建链后仍需继续观察 `Ack`、补拉、反熵和校时指标。

## 备份策略

- 备份对象至少包括每个节点自己的 SQLite 文件，默认 `./data/turntf.db`。
- 如果 `store.engine = "pebble"`，还需要同时备份 Pebble 数据目录，默认 `./data/turntf.pebble`。
- 推荐使用 SQLite 在线备份能力或文件系统快照，不建议在写入中直接复制裸 DB 文件。
- 如果使用 `sqlite3` 命令，可执行：

```bash
sqlite3 ./data/turntf.db ".backup './backup/turntf-$(date +%Y%m%d%H%M%S).db'"
```

- 至少备份配置文件、SQLite 数据库文件，以及 Pebble 模式下的 Pebble 数据目录；SQLite `schema_meta` 中的 `node_id`、`discovered_peers` 中的历史发现记录，以及配置里的 `auth.token_secret`、`cluster.secret` 是恢复时的关键材料。
- 单节点备份只代表该节点本地状态。集群仍依赖事件补拉和快照反熵来修复节点间差异。

## 节点恢复流程

1. 停止故障节点进程。
2. 确认恢复时仍使用原来的 SQLite 数据库或至少保留 `schema_meta.node_id`，不要把同一个身份同时启动两份。
3. 从最近备份恢复 SQLite 数据库文件；Pebble 模式下同时恢复 Pebble 数据目录。
4. 使用原配置启动节点，确保 `cluster.peers` 或已恢复的 `discovered_peers` 至少包含一个当前可用入口。
5. 观察 `/ops/status` 中该节点的 `discovery`、各 peer 下各 `origin_node_id` 的 `unconfirmed_events`、`pending_catchup`，以及 peer 顶层的 `pending_snapshot_partitions` 是否逐步归零。
6. 观察 `/metrics` 中 `notifier_peer_connected`、`notifier_discovered_peers`、`notifier_peer_origin_applied_event_id`、`notifier_peer_pending_snapshot_partitions`。
7. 如果长时间无法追平，检查集群 HMAC 密钥、peer URL、发现候选状态、时钟偏差和网络连通性。

目标用户瞬时包额外排查点：

1. 确认目标用户当前在线，且登录在目标节点的 `GET /ws/client` 连接上。
2. 确认基础 peer 网络仍连通；动态路由只解决多跳寻路，不替代底层 `cluster.peers` 建链。
3. 如果依赖 `route_retry`，确认节点没有重启，且问题发生在内存 TTL 窗口内。

## 核心指标

- `notifier_event_log_last_sequence{node_id}`：本地事件日志最新 sequence。持续增长代表本节点有写入或复制事件入库。
- `notifier_peer_connected{node_id,peer_node_id}`：peer 是否已连接。正常值为 `1`。
- `notifier_peer_origin_unconfirmed_events{node_id,peer_node_id,origin_node_id}`：本地某个 origin 的事件尚未被 peer `Ack` 的数量，对应“本地已写入但尚未收到该 peer 本地应用确认”的复制阶段。持续升高通常表示对端断开或复制阻塞。
- `notifier_peer_origin_applied_event_id{node_id,peer_node_id,origin_node_id}`：本地对该 origin 已应用到的最新 `event_id`，对应“本节点本地复制应用进度”。
- `notifier_peer_origin_remote_last_event_id{node_id,peer_node_id,origin_node_id}`：最近从 peer 观察到的该 origin 最新 `event_id`。
- `notifier_peer_pending_snapshot_partitions{node_id,peer_node_id}`：待完成的反熵快照分片数量，对应“反熵快照修复阶段”。短暂非零正常，长期非零需要排查快照修复。
- `notifier_user_conflicts_total{node_id}`：累计用户冲突记录数。
- `notifier_message_trimmed_total{node_id}`：累计被本地消息窗口裁剪的消息数。
- `notifier_clock_offset_ms{node_id,peer_node_id}`：最近一次可信校时偏移。
- `notifier_write_gate_ready{node_id}`：本节点是否允许本地写入，对应“写闸门是否已进入可信复制状态”。集群模式下为 `0` 通常表示尚未完成可信校时。
- `notifier_discovered_peers{node_id}`：本节点已知发现记录数。
- `notifier_discovered_peers_by_state{node_id,state}`：按 `candidate`、`dialing`、`connected`、`failed`、`expired` 聚合的发现记录数。
- `notifier_discovered_peers_by_scheme{node_id,scheme}`：按 URL scheme 聚合的发现记录数，可用来区分 `ws`、`wss`、`zmq+tcp` 候选。
- `notifier_dynamic_peer_dialers{node_id}`：由自动发现启动的动态 peer 拨号器数量，当前每节点最多 8 个。
- `notifier_zeromq_listener_running{node_id,mode}`：本地 ZeroMQ listener 运行状态；`mode` 为 `disabled`、`outbound_only` 或 `listening`。
- `notifier_membership_updates_sent_total{node_id}` / `notifier_membership_updates_received_total{node_id}`：membership update 收发总数。
- `notifier_membership_advertisements_rejected_total{node_id}`：被发现逻辑拒绝的广告总数，持续增长时优先检查广告 URL、协议版本和来源身份。
- `notifier_discovered_peer_persist_failures_total{node_id}`：发现记录写入 SQLite 失败总数。

当前版本暂未单独暴露瞬时包路由指标。排查时优先结合 `/ops/status`、peer 连通性和应用层日志定位。

## 日志配置

服务日志使用 zerolog。默认仅向 `stderr` 输出控制台文本日志；配置文件中可增加：

```toml
[logging]
level = "info"
file_path = "./data/notifier.log"
```

- `logging.level` 默认 `info`，支持 `debug`、`info`、`warn`、`error`。
- `logging.file_path` 为空时不写日志文件；配置后会自动创建父目录并追加写入 JSON 行日志。
- 服务本身不做日志轮转、压缩或清理，生产环境应交给外部日志组件处理。

## 常见告警排查

- peer 长期未连接：检查 `cluster.peers.url`、发现到的 `discovered_url`、防火墙、反向代理 WebSocket 支持和 `cluster.advertise_path`。
- `discovery.discovered_peers = 0` 且 membership update 计数不增长：确认至少有一个静态 peer 已连接，双方协议支持 membership，且 `cluster.secret` 一致。
- 发现候选长期 `failed` 或 `expired`：查看 peer 的 `last_discovery_error`、`notifier_discovered_peers_by_state` 和 `peer_dial_failed` 日志；常见原因是 URL 不可达、反向代理不支持 WebSocket、HMAC 不一致、候选不再被任何在线节点广告。
- `notifier_write_gate_ready` 为 `0`：检查是否至少有一个 peer 完成校时，或是否时钟偏差超过 `cluster.max_clock_skew_ms`。如果只是刚启动且尚未完成首次校时，属于预期延迟。
- `notifier_peer_origin_unconfirmed_events` 持续升高：先区分是短暂补拉中的预期积压，还是长时间无下降的异常；异常时检查对端是否在线、是否能应用该 origin 的事件、日志中是否有 HMAC、HLC 或 schema 错误。
- `notifier_peer_pending_snapshot_partitions` 长期非零：检查快照版本是否一致、消息窗口大小是否一致、目标用户是否已被墓碑删除。短时间非零不代表故障，长期不归零才需要处理。
- `notifier_clock_offset_ms` 接近阈值：检查 NTP 或宿主机时间源，必要时先修复系统时间再恢复写入。
- 目标用户瞬时包未送达：先确认目标用户是否在线，再检查目标节点是否仍可达；如果业务需要离线补发，不应使用 `transient` 瞬时包模式。不要把它当作事件复制或快照修复失败处理。
