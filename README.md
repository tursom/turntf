# 分布式通知服务

一个全新的分布式通知服务项目，目标是支持：

- 任意节点可写
- 节点间通过 WebSocket 长连接互联
- 节点同步协议使用 Protobuf
- 用户数据最终完全一致
- 消息数据按每用户最近 N 条最终一致

当前仓库已经完成实施计划的前 5 步：本地存储内核、单节点 HTTP/JSON API、WebSocket + Protobuf 的最小集群同步链路、断线后的事件日志补发，以及基于 `user_id` 的用户多主冲突收敛。

## 技术栈

- Go 1.26
- SQLite 作为每节点本地数据库
- `github.com/gorilla/websocket`
- `github.com/mattn/go-sqlite3`
- `google.golang.org/protobuf`
- 标准库

## 项目结构

```text
.
├── cmd/notifier/main.go            # 当前 CLI 入口
├── docs/distributed-system-plan.md # 分布式实施计划
├── internal/api                    # 应用服务层
├── internal/auth                   # token 与鉴权
├── internal/clock                  # HLC 和全局 ID
├── internal/cluster                # 集群配置与同步骨架
├── internal/proto                  # 集群协议类型
├── internal/store                  # SQLite 本地存储内核
├── proto/cluster.proto             # Protobuf 协议定义
└── README.md
```

## 当前状态

当前仓库不再承载旧的单机通知服务实现，默认目标就是新的分布式项目。

实施计划见 [docs/distributed-system-plan.md](/root/dev/sys/turntf/docs/distributed-system-plan.md)。

当前第四步已经收紧到以下边界：

- 集群模式必须同时提供 `cluster-listen-addr`、`cluster-advertise-addr`、`cluster-secret`
- 节点间启用 `Envelope`、`Hello`、`Ack`、`EventBatch`、`PullEvents`
- `Hello` 用于交换节点身份、协议版本、广播地址和当前本地 `last_sequence`
- `EventBatch` 既用于在线广播，也用于断线后的增量补发
- `Ack` 和 `peer_cursors` 一起记录每个 peer 的确认进度与已应用进度
- WebSocket 连接具备握手校验、心跳保活、自动重连和单连接方向裁决
- 节点重连后会按持久化游标自动补拉缺失事件，并通过 `applied_events` 做幂等去重

当前还没有实现：

- `cluster_secret` 的 HMAC 鉴权
- 快照修复和反熵同步

## 启动 API 服务

```bash
go run ./cmd/notifier serve -addr :8080 -db ./data/notifier.db -node-id node-a -node-slot 1
```

启动双节点最小集群时，可增加 cluster 参数：

```bash
go run ./cmd/notifier serve \
  -addr :8080 \
  -db ./data/node-a.db \
  -node-id node-a \
  -node-slot 1 \
  -cluster-listen-addr :9080 \
  -cluster-advertise-addr ws://127.0.0.1:9080/internal/cluster/ws \
  -cluster-secret secret \
  -peer node-b=ws://127.0.0.1:9081/internal/cluster/ws
```

第二个节点可用对应参数启动：

```bash
go run ./cmd/notifier serve \
  -addr :8081 \
  -db ./data/node-b.db \
  -node-id node-b \
  -node-slot 2 \
  -cluster-listen-addr :9081 \
  -cluster-advertise-addr ws://127.0.0.1:9081/internal/cluster/ws \
  -cluster-secret secret \
  -peer node-a=ws://127.0.0.1:9080/internal/cluster/ws
```

说明：

- 只要传入任意 cluster 参数，就应把三项必填参数一起传全：`-cluster-listen-addr`、`-cluster-advertise-addr`、`-cluster-secret`
- `-peer` 可以重复传入多个 peer，但 `node-id` 不能和本节点相同
- 当前第 4 步只支持新初始化的 SQLite 库，不包含旧第 3 步库的自动迁移

当前已提供：

- `POST /users`
- `GET /users/{id}`
- `PATCH /users/{id}`
- `DELETE /users/{id}`
- `POST /messages`
- `GET /users/{id}/messages?limit=N`
- `GET /events?after=0&limit=100`
- `GET /healthz`
- `GET /internal/cluster/ws` 作为节点间 WebSocket 同步端点

当前集群同步行为：

- 本地 `POST /users`、`PATCH /users/{id}`、`DELETE /users/{id}`、`POST /messages` 成功后，会异步广播对应事件
- 对端节点成功应用事件后返回 `Ack`
- 两节点在线时，创建用户和写消息可以自动同步
- 节点短时离线后重连，会基于 `peer_cursors` 自动补拉未追平的事件
- 拉取重放与实时广播重叠时，重复事件会被 `applied_events` 幂等吸收
- 用户复制按 `user_id` 做字段级 LWW 合并，用户名允许重复
- 删除通过 `tombstones` 传播，旧的创建/更新事件不会把已删除用户重新复活
- 这一步仍不承诺跨节点鉴权或快照修复

## 初始化本地存储

```bash
go run ./cmd/notifier init-store -db ./data/notifier.db -node-id node-a -node-slot 1
```

该命令会初始化第 1 步需要的本地 SQLite schema，包括：

- `users`
- `messages`
- `event_log`
- `peer_cursors`
- `applied_events`
- `user_conflicts`
- `tombstones`

## 下一步

- 第 6 步：实现消息扩散与最近 N 条一致
