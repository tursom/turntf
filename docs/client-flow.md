# 客户端全流程接入文档

本文档面向业务客户端和接入方，描述从准备账号到稳定收发消息的完整流程。长连接字段、枚举和响应体定义以 [客户端长连接接口](./client-websocket.md) 与 `proto/client.proto`、`proto/transport.proto` 为准；HTTP 路由和会话处理的当前实现分别位于 `internal/api/http.go`、`internal/api/client_ws.go`、`internal/api/client_session.go`、`internal/api/client_ws_rpc.go`。

## 角色与接口

客户端会用到两类接口：

- HTTP JSON API：主要用于登录、脚本、管理后台和调试。
- 长连接 Protobuf API：覆盖除 HTTP 登录之外的客户端能力，可用于收发消息，也可用于用户管理、消息查询、metadata、附件关系、集群状态和在线会话查询。传输可选 WebSocket 或 ZeroMQ。

当前核心入口：

- `POST /auth/login`：HTTP 登录。请求里必须二选一提供 `(node_id,user_id)` 或 `login_name`，返回 Bearer token、`expires_at` 和当前用户信息。
- `POST /users`：管理员创建普通用户或 `channel`。仅可登录用户支持 `login_name`。
- `GET /nodes/{node_id}/users/{user_id}/messages?limit=N`：HTTP 查询消息，响应中的 `body` 是 base64 字节。
- `POST /nodes/{node_id}/users/{user_id}/messages`：HTTP 发送消息。默认写持久消息；当 `delivery_kind = "transient"` 时走瞬时包，不落库。
- `GET /nodes/{node_id}/users/{user_id}/metadata`、`GET|PUT|DELETE /nodes/{node_id}/users/{user_id}/metadata/{key}`：读取、扫描和维护用户 metadata。
- `GET /nodes/{node_id}/users/{user_id}/attachments`、`PUT|DELETE /nodes/{node_id}/users/{user_id}/attachments/{attachment_type}/{subject_node_id}/{subject_user_id}`：低层附件接口，用于 `channel_manager`、`channel_writer`、`channel_subscription`、`user_blacklist` 四类关系。
- `POST|GET|DELETE /nodes/{node_id}/users/{user_id}/subscriptions...`：HTTP 订阅管理快捷接口。
- `POST|GET|DELETE /nodes/{node_id}/users/{user_id}/blacklist...`：HTTP 黑名单管理快捷接口。
- `GET /ws/client`：标准客户端 WebSocket 长连接。连接升级后第一帧必须是 binary protobuf `ClientEnvelope.login`。
- `GET /ws/realtime`：实时流 WebSocket。不会补发历史消息，也不会接收持久化推送；`send_message` 只接受 `delivery_kind = TRANSIENT`。
- `zmq+tcp://host:port`：客户端 ZeroMQ 长连接。首帧必须是 `ZeroMQMuxHello{role=CLIENT, protocol_version="zeromq-mux-v1"}`，第二帧必须是 `ClientEnvelope.login`。

长连接鉴权和 HTTP 不同：

- WebSocket / ZeroMQ 不使用 query token，也不读取 HTTP `Authorization` header。
- 首次登录通过首个 protobuf `LoginRequest` 提交密码；成功后保存服务端返回的短期 `reconnect_token`，后续重连优先用它替代密码。
- `LoginRequest` 也必须二选一提供 `user` 或 `login_name`；两者同时提供或同时缺失都会被拒绝。
- 每次初连和重连都必须声明 `LoginRequest.protocol_version = "client-v1alpha5"`；服务端在凭据校验和会话注册前严格拒绝空值或其他版本。

关于 ZeroMQ：

- `services.zeromq.bind_url` 是服务端本地监听地址，配置层通常写成 `tcp://host:port`；客户端对外使用 `zmq+tcp://host:port`。
- 如果服务端启用 `services.zeromq.security = "curve"`，客户端连接前还必须配置服务端 `server_public_key` 和自己的 CURVE client key；客户端 public key 必须在服务端白名单中。
- ZeroMQ TLS 不在应用内实现。需要 TLS 证书体系时，应使用外部 TCP TLS 隧道，或改走 WebSocket `wss`。

对“本人作用域”的 HTTP 接口，路径中的 `{node_id,user_id}` 可以写成 `0/0`，服务端会按当前 Bearer token 解析为当前登录用户。仅完整的 `0/0` 组合有效；`0/x` 和 `x/0` 仍会报错。当前主要覆盖：

- 查询本人用户信息、本人消息。
- 查询、写入和删除本人 metadata。
- 查询本人附件。
- 以当前用户为 owner 管理 `channel_subscription` / `user_blacklist` 附件。
- 查询和维护本人 subscriptions / blacklist。

`POST /nodes/{node_id}/users/{user_id}/messages`、更新用户、删除用户仍需要显式写出真实 `node_id` 和 `user_id`。

## 端到端流程

1. 管理员通过 HTTP 登录，创建普通用户；如需“用户名式登录”，同时分配 `login_name`。
2. 可选：管理员创建 `channel`。如果由某个创建者创建，服务端会自动把创建者加入该 channel 的 `channel_manager` 和 `channel_writer`。
3. 可选：为其他发送者补充 `channel_writer` 授权。
4. 可选：用户本人或管理员维护 channel 订阅和黑名单。
5. 客户端初始化本地消息表、游标表；如果业务只使用瞬时包，可选初始化 `packet_id` 去重表。
6. 选择接入方式：
   - 标准长连接：`GET /ws/client` 或 ZeroMQ。
   - 实时流 WebSocket：`GET /ws/realtime`。
   - 仅关闭持久化补发但仍保留标准 RPC：在标准长连接登录时设置 `LoginRequest.transient_only = true`。
7. 如果使用 ZeroMQ CURVE，先完成 CURVE socket 配置；随后发送 `ZeroMQMuxHello{role=CLIENT, protocol_version="zeromq-mux-v1"}`。
8. 客户端发送 `ClientEnvelope.login`，携带固定的 `protocol_version = "client-v1alpha5"`、`user` 或 `login_name`、首次登录用的 `password` 或后续登录用的 `reconnect_token`、可选的 `seen_messages`，以及可选的 `transient_only`。
9. 服务端返回 `LoginResponse`，其中包含 `user`、固定的 `protocol_version = "client-v1alpha5"`、当前连接的 `session_ref`、最新 `reconnect_token` 及其过期时间；客户端先校验版本并安全持久化新 token，再进入已登录状态。
10. 如果当前连接不是 transient-only，服务端会先补发“当前用户可见且不在 `seen_messages` 中”的持久消息，然后继续推送实时消息。
11. 客户端收到 `MessagePushed` 后，按 `(node_id, seq)` 做幂等检查，先落库，再保存游标，最后可选发送 `AckMessage`。
12. 客户端收到 `PacketPushed` 时不要写消息游标；如需去重，可按 `packet_id` 做短期应用层去重。
13. 客户端可继续在同一条长连接上执行 RPC，例如 `send_message`、`get_user`、`list_messages`、`list_users`、`get_user_metadata`、`scan_user_metadata`、`upsert_user_attachment`、`list_user_attachments`、`list_cluster_nodes`、`list_node_logged_in_users`、`resolve_user_sessions`、`list_events`、`operations_status`、`metrics`。
14. 如果需要把瞬时包定向到某个在线会话，先用 `resolve_user_sessions` 获取目标用户的 `session_ref`，再把它放进 `SendMessageRequest.target_session`。
15. 网络断开后，客户端用短期 `reconnect_token` 和本地游标重连，服务端按新的 `seen_messages` 跳过已持久化消息；token 失效时清除它，再显式进行一次密码登录。

## 服务端准备

### 创建管理员 token

管理员先通过 HTTP 登录获取 token：

```bash
ADMIN_TOKEN="$(
  curl -sS -X POST http://127.0.0.1:8080/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"node_id":4096,"user_id":1,"password":"root"}' \
  | jq -r .token
)"
```

### 创建普通用户

如果后续希望通过 `login_name` 登录 WebSocket / ZeroMQ 或 HTTP，可以在创建用户时直接分配：

```bash
curl -sS -X POST http://127.0.0.1:8080/users \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "username":"alice",
    "login_name":"alice.login",
    "password":"alice-password",
    "role":"user"
  }'
```

响应中的 `node_id`、`user_id` 和可选 `login_name` 都可以用于后续登录：

- 长连接登录可用 `user { node_id, user_id }`。
- 也可只填 `login_name` + `password`。

如果后续要使用 `/nodes/0/users/0/...` 这类“当前登录用户”快捷路径，先为该用户换取自己的 token：

```bash
ALICE_TOKEN="$(
  curl -sS -X POST http://127.0.0.1:8080/auth/login \
    -H 'Content-Type: application/json' \
    -d '{"login_name":"alice.login","password":"alice-password"}' \
  | jq -r .token
)"
```

### 创建 channel

channel 是不可登录的组播地址：

```bash
curl -sS -X POST http://127.0.0.1:8080/users \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"username":"orders","role":"channel"}'
```

如果消息发送者不是 channel 创建者，还需要由管理员或 channel manager 额外授予 `channel_writer`：

```bash
curl -sS -X PUT \
  http://127.0.0.1:8080/nodes/4096/users/1026/attachments/channel_writer/4096/1025 \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"config_json":{}}'
```

### 订阅 channel

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/0/users/0/subscriptions \
  -H "Authorization: Bearer ${ALICE_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"channel_node_id":4096,"channel_user_id":1026}'
```

订阅只影响订阅时间之后的 channel 消息。订阅前的 channel 历史不会补给该用户。

### 配置黑名单

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/0/users/0/blacklist \
  -H "Authorization: Bearer ${ALICE_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"blocked_node_id":4096,"blocked_user_id":1027}'
```

黑名单只影响后续新的普通用户直发消息，不删除历史消息，也不影响 channel、broadcast 或 node 地址。

## 客户端本地状态

持久消息客户端至少需要维护两类状态：

- 消息表：保存完整 `Message`，包括 `recipient`、`node_id`、`seq`、`sender`、`body`、`created_at_hlc`。
- 游标表：保存已成功持久化消息的 `(node_id, seq)`。

如果业务要做瞬时包本地去重，可选再维护一张短期去重表记录 `packet_id`。如果业务完全只用瞬时包，则消息表和游标表可以简化甚至省略，但这意味着不具备持久消息补发能力。

推荐本地唯一键：

```text
messages primary key: (node_id, seq)
```

如果客户端需要按目标地址检索，可额外建立索引：

```text
target index: (recipient_node_id, recipient_user_id, created_at_hlc)
```

如果业务需要“按会话定向瞬时包”，建议额外缓存：

- 当前连接登录响应里的 `LoginResponse.session_ref`。
- 通过 `resolve_user_sessions` 拿到的远端 `ResolvedSession.session`。

处理顺序必须是：

1. 收到 `MessagePushed`。
2. 按 `(node_id, seq)` 做幂等检查。
3. 将消息写入本地数据库。
4. 将 `(node_id, seq)` 写入本地游标表。
5. 可选发送 `AckMessage`。

不要先 ack 再落库，否则断线后可能丢失客户端尚未持久化的消息。

同时要注意：

- `AckMessage` 只是连接内去重提示，服务端不会把 ack 状态写入数据库。
- 可靠重连依赖的是下次登录时重新上报 `seen_messages`。

## 标准长连接登录

标准 WebSocket 地址：

```text
ws://127.0.0.1:8080/ws/client
```

连接升级后，第一帧必须是 binary protobuf：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    password: "alice-password"
    protocol_version: "client-v1alpha5"
    seen_messages: []
  }
}
```

也可以改为使用 `login_name`：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    login_name: "alice.login"
    password: "alice-password"
    transient_only: true
    protocol_version: "client-v1alpha5"
  }
}
```

其中：

- `user` 与 `login_name` 必须二选一。
- `protocol_version` 必须精确等于 `client-v1alpha5`；这是一套 wire schema 的严格 epoch，不是多版本协商。
- `seen_messages` 可以为空，也可以包含来自多个生产节点的游标。
- `transient_only = true` 会关闭持久化补发和后续持久消息推送，但不会像 `/ws/realtime` 那样强制限制大多数 RPC。

服务端成功返回：

```protobuf
ServerEnvelope {
  login_response: LoginResponse {
    user: {
      node_id: 4096
      user_id: 1025
      username: "alice"
      role: "user"
      login_name: "alice.login"
    }
    protocol_version: "client-v1alpha5"
    session_ref: {
      serving_node_id: 4096
      session_id: "..."
    }
  }
}
```

登录成功后：

- 标准连接默认会立即开始历史补发。
- `transient_only = true` 的标准连接不会收到历史补发，也不会进入持久化推送集合。
- 如果其他节点或本节点把瞬时包转发给当前用户，客户端还可能收到 `PacketPushed`；这类数据包不会进入历史补发。

## `/ws/realtime` 与 ZeroMQ

`GET /ws/realtime` 是硬实时流路径：

- 不补发历史消息。
- 不接收持久化 `MessagePushed`。
- `send_message` 只允许 `delivery_kind = CLIENT_DELIVERY_KIND_TRANSIENT`。
- 用户管理、消息历史、metadata、附件、`list_events`、`operations_status`、`metrics` 等大多数 RPC 会返回 `invalid_request`。
- `list_cluster_nodes`、`list_node_logged_in_users`、`resolve_user_sessions`、`ping` 仍可使用。

ZeroMQ 客户端连接流程：

1. 拨号 `zmq+tcp://host:port`。
2. 第一帧发送 `ZeroMQMuxHello{role=CLIENT, protocol_version="zeromq-mux-v1"}`。
3. 第二帧发送与 WebSocket 相同的 `ClientEnvelope.login`。

ZeroMQ 没有单独的 `/ws/realtime` 式路径；如果只想关闭历史补发，可以在登录时设置 `transient_only = true`。

## 接收消息与 ack / 游标语义

服务端持久消息推送：

```protobuf
ServerEnvelope {
  message_pushed: MessagePushed {
    message: {
      recipient: { node_id: 4096, user_id: 1025 }
      node_id: 4096
      seq: 3
      sender: { node_id: 4096, user_id: 1 }
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

客户端处理逻辑：

```text
cursor = (message.node_id, message.seq)
if cursor exists locally:
    ignore message
else:
    persist message
    persist cursor
    send AckMessage(cursor) if connection is still open
```

可见性规则：

- 普通用户能看到发给自己的消息。
- 普通用户能看到所有仍在本地消息窗口内的 broadcast 消息。
- 普通用户能看到订阅后发送到已订阅 channel 的消息。
- 如果用户已拉黑某个普通用户，则拉黑之后来自该发送方的新直发消息不会出现在列表或实时推送中。
- 管理员能看到任意目标地址的消息。

瞬时包推送：

```protobuf
ServerEnvelope {
  packet_pushed: PacketPushed {
    packet: {
      packet_id: 77
      source_node_id: 4096
      target_node_id: 8192
      recipient: { node_id: 8192, user_id: 1025 }
      sender: { node_id: 4096, user_id: 1 }
      body: "\xff\x00payload"
      delivery_mode: CLIENT_DELIVERY_MODE_BEST_EFFORT
      target_session: {
        serving_node_id: 8192
        session_id: "..."
      }
    }
  }
}
```

`PacketPushed` 规则：

- 只用于 `delivery_kind = TRANSIENT` 的瞬时包。
- 没有 `(node_id, seq)` 游标，不参与 `seen_messages` 和历史补发。
- 如果请求里指定了 `target_session`，只有目标用户的那个在线会话会收到它。
- `CLIENT_DELIVERY_MODE_ROUTE_RETRY` 只表示节点间会做短时重新寻路，仍然不是可靠送达。

## 发送消息

发送普通持久消息：

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 42
    target: { node_id: 4096, user_id: 1025 }
    body: "\xff\x00payload"
    sync_mode: CLIENT_MESSAGE_SYNC_MODE_FORCE_SYNC
  }
}
```

成功返回：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 42
    message: {
      recipient: { node_id: 4096, user_id: 1025 }
      node_id: 4096
      seq: 4
      sender: { node_id: 4096, user_id: 1025 }
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

客户端应把 `send_message_response.message` 也按普通消息落库，并保存 `(node_id, seq)`。服务端当前会在同连接中把该消息标记为已见，通常不会再重复推送；客户端仍要按 `(node_id, seq)` 幂等处理。

发送目标用户瞬时包：

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 43
    target: { node_id: 8192, user_id: 1025 }
    body: "\xff\x00payload"
    delivery_kind: CLIENT_DELIVERY_KIND_TRANSIENT
    delivery_mode: CLIENT_DELIVERY_MODE_ROUTE_RETRY
  }
}
```

如果要把瞬时包定向到某个已知会话，把 `target_session` 一起带上：

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 44
    target: { node_id: 8192, user_id: 1025 }
    body: "targeted"
    delivery_kind: CLIENT_DELIVERY_KIND_TRANSIENT
    delivery_mode: CLIENT_DELIVERY_MODE_BEST_EFFORT
    target_session: {
      serving_node_id: 8192
      session_id: "..."
    }
  }
}
```

对应响应中的 `transient_accepted` 也会带回 `target_session`：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 44
    transient_accepted: {
      packet_id: 77
      source_node_id: 4096
      target_node_id: 8192
      recipient: { node_id: 8192, user_id: 1025 }
      delivery_mode: CLIENT_DELIVERY_MODE_BEST_EFFORT
      target_session: {
        serving_node_id: 8192
        session_id: "..."
      }
    }
  }
}
```

字段约束：

- `delivery_mode` 只允许出现在瞬时消息里。
- `sync_mode` 只允许出现在持久消息里。
- `target_session` 只允许出现在瞬时消息里。
- `body` 是原始字节，不要求 UTF-8。

发送权限：

- 普通用户可以给任意可登录用户（包括自己）发送消息。
- 普通用户可以给已授权写入的 channel 发送消息。
- 管理员可以给任意用户、channel 或 broadcast 地址发送消息。
- 以上规则同时适用于持久消息和瞬时消息。

## 长连接上的查询与管理能力

登录成功后，标准长连接还可以执行以下 RPC：

- 用户管理：`create_user`、`get_user`、`update_user`、`delete_user`、`list_users`
- 消息与用户元数据：`list_messages`、`get_user_metadata`、`upsert_user_metadata`、`delete_user_metadata`、`scan_user_metadata`
- 附件关系：`upsert_user_attachment`、`delete_user_attachment`、`list_user_attachments`
- 集群与在线态：`list_cluster_nodes`、`list_node_logged_in_users`、`resolve_user_sessions`
- 运维：`list_events`、`operations_status`、`metrics`
- 基础连接：`ping`、可选 `ack_message`

注意当前协议没有专门的 `subscribe_channel`、`block_user`、`list_subscriptions`、`list_blocked_users` RPC：

- 订阅关系通过 `upsert_user_attachment` / `delete_user_attachment` + `ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION` 表达。
- 黑名单通过 `upsert_user_attachment` / `delete_user_attachment` + `ATTACHMENT_TYPE_USER_BLACKLIST` 表达。
- 查询订阅或黑名单通过 `list_user_attachments` 并带上对应 `attachment_type` 完成。

空 owner / user 的“当前登录用户回退”语义也存在于长连接：

- `get_user.user`、`list_messages.user`、metadata 请求里的 `owner` 为空或 `{0,0}` 时，会回退到当前登录用户。
- `list_user_attachments.owner` 也支持当前用户回退。
- `upsert_user_attachment.owner`、`delete_user_attachment.owner` 只有在 `attachment_type = CHANNEL_SUBSCRIPTION` 或 `USER_BLACKLIST` 时才允许省略 owner。

## HTTP 消息接口

HTTP 消息接口也使用 bytes body，但 JSON 中以 base64 表示。

写一条持久消息：

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/4096/users/1025/messages \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"body":"/wBwYXlsb2Fk"}'
```

其中 `/wBwYXlsb2Fk` 是字节 `ff 00 70 61 79 6c 6f 61 64` 的 base64。

持久消息也可以显式指定 Pebble 提交方式：

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/4096/users/1025/messages \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "body":"/wBwYXlsb2Fk",
    "sync_mode":"force_sync"
  }'
```

`sync_mode` 只对持久消息有效；在 SQLite 后端会被接受，但不会产生 Pebble 特有的刷盘语义。

发送目标用户瞬时包：

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/8192/users/1025/messages \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "body":"/wBwYXlsb2Fk",
    "delivery_kind":"transient",
    "delivery_mode":"route_retry"
  }'
```

该接口返回 `202 Accepted`。如果目标节点不可达、短时无法寻路或目标用户离线，瞬时包仍可能最终被丢弃。HTTP 当前不支持 `target_session` 定向投递；这项能力只在长连接 `send_message` 里提供。

查询消息示例：

```bash
curl -sS -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  'http://127.0.0.1:8080/nodes/4096/users/1025/messages?limit=20'
```

响应里的 `body` 同样是 base64 字符串。

## 断线重连

客户端断线后：

1. 保留本地已持久化消息和游标。
2. 使用指数退避重连 `GET /ws/client` 或 ZeroMQ。
3. 第一帧重新发送 `LoginRequest`。
4. 重新声明 `protocol_version = "client-v1alpha5"`，并把本地游标表中的 `(node_id, seq)` 放入 `seen_messages`。
5. 对重连后收到的所有持久消息继续按 `(node_id, seq)` 幂等处理。

示例：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    password: "alice-password"
    protocol_version: "client-v1alpha5"
    seen_messages: [
      { node_id: 4096, seq: 1 },
      { node_id: 4096, seq: 2 },
      { node_id: 4097, seq: 8 }
    ]
  }
}
```

`seen_messages` 可以包含来自多个生产节点的消息游标。

注意：

- 瞬时包不参与上述恢复流程。
- 如果登录时使用 `transient_only = true` 或 `/ws/realtime`，本次连接本身不会收到历史补发。
- 若业务需要“瞬时包断线恢复”，必须由应用层自行持久化和补偿。

## Channel、Broadcast 与瞬时包

channel：

1. 管理员创建 `role=channel` 用户。
2. 创建者会自动获得该 channel 的 `channel_manager` 和 `channel_writer`。
3. 普通用户订阅该 channel。
4. 获得 `channel_writer` 授权的用户或管理员向 channel 地址发消息。
5. 订阅者通过标准长连接收到订阅时间之后的 channel 消息。

broadcast：

1. 每个节点启动时会创建系统 broadcast 地址，通常是 `user_id = 2`。
2. 管理员可以向任意 broadcast 地址发送消息。
3. 普通用户读取或连接标准长连接时，会看到仍在本地消息窗口内的 broadcast 消息。

瞬时包：

1. 客户端直接把消息发给最终目标用户。
2. 服务端按动态路由把瞬时包转发到目标节点。
3. 只有目标用户当前在线时，目标会话才会收到 `PacketPushed`。
4. 如果指定了 `target_session`，只有那个会话会收到。
5. 瞬时包不落库，也不会在后续登录时补发。

## 错误处理

服务端错误统一使用 `ServerEnvelope.error`：

```protobuf
ServerEnvelope {
  error: Error {
    code: "forbidden"
    message: "forbidden"
    request_id: 42
  }
}
```

客户端建议：

- `unauthorized`：登录失败、混用 `user` 与 `login_name`、首帧不是登录消息或密码错误。应停止盲目重试并提示重新认证。
- `invalid_request`：目标缺失、body 为空、只填了半个 `UserRef`、非法枚举、在持久消息中带了 `delivery_mode`、在瞬时消息中带了 `sync_mode`、在持久消息中带了 `target_session` 等。
- `forbidden`：当前用户没有权限。必要时刷新订阅/附件关系或联系管理员。
- `not_found`：目标用户、channel、broadcast 地址或附件资源不存在。
- `service_unavailable`：当前节点暂时不可写或不可达，例如某些集群状态未准备就绪。
- `internal_error`：服务端内部错误。可以保留连接并稍后重试；如果连接断开，则按断线重连流程处理。

登录阶段的错误会导致服务端关闭连接。登录成功后的请求级错误通常不会关闭连接。

## 跨节点连接

集群中任意节点都可以提供标准客户端接入：

- 用户 token 只用于 HTTP；长连接使用密码首帧登录。
- 同一用户可以同时在多个节点上有多个在线会话。
- 切换连接节点时，客户端仍按 `(node_id, seq)` 去重。
- 如需把瞬时包定向到某个具体在线会话，应先通过 `resolve_user_sessions` 获取那条会话的 `session_ref`。
- 不同节点在短时间内可能因为复制延迟或消息窗口裁剪而看到不同集合，稳定后会按集群规则收敛。

## 最小客户端状态机

```text
Disconnected
  -> connect transport (ws client / ws realtime / zeromq)
Connecting
  -> send LoginRequest(protocol_version=client-v1alpha5, user or login_name, seen_messages, transient_only?)
Authenticating
  -> receive LoginResponse(client-v1alpha5, user, session_ref), validate version
  -> receive unsupported_protocol_version or mismatched LoginResponse: terminal failure
Online
  -> receive MessagePushed: persist + cursor + optional ack
  -> receive PacketPushed: optional packet_id dedupe, no cursor write
  -> send SendMessageRequest: wait matching request_id response
  -> receive Error: handle by code
  -> socket/transport closed: Disconnected with backoff
```

## 验收清单

- 能创建普通用户，并记录 `(node_id, user_id)` 和可选 `login_name`。
- 能用 `user` 或 `login_name` 至少一种方式完成首帧登录。
- 能在初连和重连声明并验证 `client-v1alpha5`，版本不兼容时停止重连。
- 能理解 `/ws/client`、`/ws/realtime` 和 `transient_only` 的差异。
- 能接收历史补发消息。
- 能接收实时消息和瞬时包。
- 能发送非 UTF-8 `bytes body` 消息。
- 能把 `(node_id, seq)` 持久化为本地游标。
- 重连时能携带 `seen_messages` 并避免重复展示。
- 需要定向瞬时包时，能使用 `session_ref` / `target_session`。
- 能订阅 channel 并只收到订阅后的 channel 消息。
- 能收到 broadcast 消息。
- 能正确处理 `Error.code`。
