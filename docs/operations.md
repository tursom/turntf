# 运维与上线手册

本文档记录分布式通知服务的小规模上线建议、备份策略、节点恢复流程和核心监控项。默认每个节点使用本地 SQLite，节点间通过 WebSocket + Protobuf 复制。

## 小规模部署建议

- 每个节点首次启动时会自动生成唯一的数字 `node_id` 并保存到 SQLite `schema_meta`；该 `node_id` 内嵌的 slot 会进入全局 ID 与 HLC 时间戳，同一集群内不能重复。
- 所有节点使用相同的 `auth.token_secret`，否则跨节点登录 token 无法互认。
- 所有节点使用相同的 `cluster.secret`，并确保它不同于 `auth.token_secret`。
- 生产环境建议所有节点使用相同的 `store.message_window_size`，避免消息反熵因窗口不一致而跳过消息分片修复。
- 将 `api.listen_addr` 暴露给业务调用方，同时只允许可信节点访问 `cluster.advertise_path`。
- 保持节点系统时钟同步。集群模式下，节点首次成功校时前会拒绝写入；时钟偏差超过 `cluster.max_clock_skew_ms` 会导致 peer 被拒绝或断开。
- 将 `GET /healthz` 接入存活探针，将 `GET /metrics` 接入带管理员 Bearer token 的指标抓取。
- 控制台日志为易读文本；如需持久化结构化日志，配置 `logging.file_path` 写入 JSON 行文件，并由 logrotate、容器运行时或日志平台负责轮转。

## 运维接口

- `GET /healthz`：公开存活检查，只返回服务进程是否可响应。
- `GET /ops/status`：管理员接口，返回本节点事件进度、peer 连接状态、未确认事件数、反熵状态、冲突数和消息裁剪统计。
- `GET /metrics`：管理员接口，返回 Prometheus text exposition 格式指标。
- `GET /events?after=0&limit=100`：管理员接口，用于调试本地事件日志。

示例：

```bash
TOKEN="$(curl -sS -X POST http://127.0.0.1:8080/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"password":"root"}' | jq -r .token)"

curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/ops/status
curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/metrics
```

## 备份策略

- 备份对象是每个节点自己的 SQLite 文件，例如 `./data/node-a.db`。
- 推荐使用 SQLite 在线备份能力或文件系统快照，不建议在写入中直接复制裸 DB 文件。
- 如果使用 `sqlite3` 命令，可执行：

```bash
sqlite3 ./data/node-a.db ".backup './backup/node-a-$(date +%Y%m%d%H%M%S).db'"
```

- 至少备份配置文件和数据库文件；SQLite `schema_meta` 中的 `node_id` 以及配置里的 `auth.token_secret`、`cluster.secret` 是恢复时的关键材料。
- 单节点备份只代表该节点本地状态。集群仍依赖事件补拉和快照反熵来修复节点间差异。

## 节点恢复流程

1. 停止故障节点进程。
2. 确认恢复时仍使用原来的 SQLite 数据库或至少保留 `schema_meta.node_id`，不要把同一个身份同时启动两份。
3. 从最近备份恢复 SQLite 数据库文件。
4. 使用原配置启动节点，确保 `cluster.peers` 指向当前可用节点。
5. 观察 `/ops/status` 中该节点对各 peer 的 `unconfirmed_events`、`pending_catchup`、`pending_snapshot_partitions` 是否逐步归零。
6. 观察 `/metrics` 中 `notifier_peer_connected`、`notifier_peer_applied_sequence`、`notifier_peer_pending_snapshot_partitions`。
7. 如果长时间无法追平，检查集群 HMAC 密钥、peer URL、时钟偏差和网络连通性。

## 核心指标

- `notifier_event_log_last_sequence{node_id}`：本地事件日志最新 sequence。持续增长代表本节点有写入或复制事件入库。
- `notifier_peer_connected{node_id,peer_node_id}`：peer 是否已连接。正常值为 `1`。
- `notifier_peer_unconfirmed_events{node_id,peer_node_id}`：本地事件尚未被 peer ack 的数量。持续升高通常表示对端断开或复制阻塞。
- `notifier_peer_applied_sequence{node_id,peer_node_id}`：本地已经应用该 peer 的事件 sequence。
- `notifier_peer_remote_last_sequence{node_id,peer_node_id}`：最近从 peer 观察到的远端最新 sequence。
- `notifier_peer_pending_snapshot_partitions{node_id,peer_node_id}`：待完成的反熵快照分片数量。短暂非零正常，长期非零需要排查快照修复。
- `notifier_user_conflicts_total{node_id}`：累计用户冲突记录数。
- `notifier_message_trimmed_total{node_id}`：累计被本地消息窗口裁剪的消息数。
- `notifier_clock_offset_ms{node_id,peer_node_id}`：最近一次可信校时偏移。
- `notifier_write_gate_ready{node_id}`：本节点是否允许本地写入。集群模式下为 `0` 通常表示尚未完成可信校时。

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

- peer 长期未连接：检查 `cluster.peers.url`、防火墙、反向代理 WebSocket 支持和 `cluster.advertise_path`。
- `notifier_write_gate_ready` 为 `0`：检查是否至少有一个 peer 完成校时，或是否时钟偏差超过 `cluster.max_clock_skew_ms`。
- `notifier_peer_unconfirmed_events` 持续升高：检查对端是否在线、是否能应用事件、日志中是否有 HMAC、HLC 或 schema 错误。
- `notifier_peer_pending_snapshot_partitions` 长期非零：检查快照版本是否一致、消息窗口大小是否一致、目标用户是否已被墓碑删除。
- `notifier_clock_offset_ms` 接近阈值：检查 NTP 或宿主机时间源，必要时先修复系统时间再恢复写入。
