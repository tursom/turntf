# 分布式通知服务

一个全新的分布式通知服务项目，目标是支持：

- 任意节点可写
- 节点间通过 WebSocket 长连接互联
- 节点同步协议使用 Protobuf
- 用户数据最终完全一致
- 消息数据按每用户最近 N 条最终一致

当前仓库已经完成实施计划的前 3 步：本地存储内核、单节点 HTTP/JSON API，以及 WebSocket + Protobuf 的最小集群同步链路。

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

当前第三步已经收紧到以下边界：

- 集群模式必须同时提供 `cluster-listen-addr`、`cluster-advertise-addr`、`cluster-secret`
- 节点间只启用 `Envelope`、`Hello`、`Ack`、`EventBatch`
- `Hello` 用于交换节点身份、协议版本、广播地址和当前本地 `last_sequence`
- `EventBatch` 目前按“一个本地事件一批”广播
- `Ack` 只表示当前在线链路上某个 `EventBatch` 已成功应用
- WebSocket 连接具备握手校验、心跳保活、自动重连和单连接方向裁决

当前还没有实现：

- 断线后的游标增量拉取和未确认事件补发
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
- 当前实现不会在断线后自动补历史事件，只有连接恢复后的新写入会继续同步

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
- 这一步不承诺断线补发、幂等重放追赶或跨节点鉴权

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

- 第 4 步：实现断线后的事件日志补发与游标持久化
