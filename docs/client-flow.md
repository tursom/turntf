# 客户端全流程接入文档

本文档面向业务客户端和接入方，描述从准备账号到稳定收发消息的完整流程。底层 WebSocket 协议字段见 [客户端 WebSocket 接口](/root/dev/sys/turntf/docs/client-websocket.md)。

## 角色与接口

客户端会用到两类接口：

- HTTP JSON API：用于管理员初始化用户、创建 channel、维护订阅，也可用于调试查询历史消息。
- WebSocket Protobuf API：用于普通客户端登录、接收历史补发、接收实时消息和发送消息。

核心地址：

- `POST /auth/login`：HTTP 登录，返回 Bearer token，主要用于管理后台或 HTTP 客户端。
- `POST /users`：管理员创建普通用户或 channel。
- `POST /nodes/{node_id}/users/{user_id}/subscriptions`：维护用户对 channel 的订阅。
- `GET /nodes/{node_id}/users/{user_id}/messages?limit=N`：HTTP 查询消息，`body` 是 base64 字节。
- `POST /nodes/{node_id}/users/{user_id}/messages`：HTTP 写消息，`body` 是 base64 字节。
- `GET /ws/client`：客户端长连接，连接后第一帧必须是 protobuf `LoginRequest`。

## 端到端流程

1. 服务端或管理后台创建登录用户。
2. 可选：管理员创建 channel 用户。
3. 可选：用户订阅 channel。
4. 客户端本地初始化消息表和游标表。
5. 客户端连接 `GET /ws/client`。
6. 客户端发送第一帧 `ClientEnvelope.login`，携带 `node_id`、`user_id`、`password` 和本地已持久化游标 `seen_messages`。
7. 服务端返回 `LoginResponse`，随后补发当前用户可见且未见过的历史消息。
8. 客户端收到 `MessagePushed` 后先落库，再保存 `(node_id, seq)` 游标，最后可选发送 `AckMessage`。
9. 客户端通过同一条 WebSocket 发送 `SendMessageRequest` 写消息。
10. 网络断开后，客户端用本地游标重连，服务端按 `seen_messages` 跳过已持久化消息。

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

```bash
curl -sS -X POST http://127.0.0.1:8080/users \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"alice-password","role":"user"}'
```

响应中的 `node_id` 和 `user_id` 是客户端 WebSocket 登录时使用的身份。

### 创建 channel

channel 是不可登录的组播地址：

```bash
curl -sS -X POST http://127.0.0.1:8080/users \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"username":"orders","role":"channel"}'
```

### 订阅 channel

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/4096/users/1025/subscriptions \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"channel_node_id":4096,"channel_user_id":1026}'
```

订阅只影响订阅时间之后的 channel 消息。订阅前的 channel 历史不会补给该用户。

## 客户端本地状态

客户端至少需要持久化两类数据：

- 消息表：保存完整 `Message`，包括 `user_node_id`、`user_id`、`node_id`、`seq`、`sender`、`body`、`created_at_hlc`。
- 游标表：保存已成功持久化消息的 `(node_id, seq)`。

推荐本地唯一键：

```text
messages primary key: (node_id, seq)
```

如果客户端需要区分消息目标，可额外建立索引：

```text
target index: (user_node_id, user_id, created_at_hlc)
```

处理顺序必须是：

1. 收到 `MessagePushed`。
2. 按 `(node_id, seq)` 做幂等检查。
3. 将消息写入本地数据库。
4. 将 `(node_id, seq)` 写入本地游标表。
5. 可选发送 `AckMessage`。

不要先 ack 再落库，否则断线后可能丢失客户端尚未持久化的消息。

## WebSocket 首次连接

客户端连接：

```text
ws://127.0.0.1:8080/ws/client
```

连接升级后，第一帧必须是 binary protobuf：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    node_id: 4096
    user_id: 1025
    password: "alice-password"
    seen_messages: []
  }
}
```

服务端成功返回：

```protobuf
ServerEnvelope {
  login_response: LoginResponse {
    user: {
      node_id: 4096
      user_id: 1025
      username: "alice"
      role: "user"
    }
    protocol_version: "client-v1alpha1"
  }
}
```

登录成功后，服务端立即开始补发历史消息。客户端要准备好在 `LoginResponse` 后连续处理多个 `MessagePushed`。

## 接收消息

服务端推送：

```protobuf
ServerEnvelope {
  message_pushed: MessagePushed {
    message: {
      user_node_id: 4096
      user_id: 1025
      node_id: 4096
      seq: 3
      sender: "orders"
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
- 普通用户能看到所有 broadcast 消息。
- 普通用户能看到订阅后发送到已订阅 channel 的消息。
- 管理员能看到任意目标地址的消息。

## 发送消息

客户端通过同一条 WebSocket 发送：

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 42
    target: { node_id: 4096, user_id: 1025 }
    sender: "mobile"
    body: "\xff\x00payload"
  }
}
```

成功返回：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 42
    message: {
      user_node_id: 4096
      user_id: 1025
      node_id: 4096
      seq: 4
      sender: "mobile"
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

客户端应把 `send_message_response.message` 也按普通消息落库，并保存 `(node_id, seq)`。服务端当前会在同连接中把该消息标记为已见，通常不会再重复推送；客户端仍要按 `(node_id, seq)` 幂等处理。

发送权限：

- 普通用户可以给自己发送。
- 普通用户可以给已订阅 channel 发送。
- 管理员可以给任意用户、channel 或 broadcast 发送。

## HTTP 消息接口

HTTP 消息接口也使用 bytes body，但 JSON 中以 base64 表示。

写消息示例：

```bash
curl -sS -X POST http://127.0.0.1:8080/nodes/4096/users/1025/messages \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"sender":"ops","body":"/wBwYXlsb2Fk"}'
```

其中 `/wBwYXlsb2Fk` 是字节 `ff 00 70 61 79 6c 6f 61 64` 的 base64。

查询消息示例：

```bash
curl -sS -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  'http://127.0.0.1:8080/nodes/4096/users/1025/messages?limit=20'
```

响应里的 `body` 同样是 base64 字符串。

## 断线重连

客户端断线后：

1. 保留本地已持久化消息和游标。
2. 使用指数退避重连 `GET /ws/client`。
3. 第一帧重新发送 `LoginRequest`。
4. 把本地游标表中的 `(node_id, seq)` 放入 `seen_messages`。
5. 对重连后收到的所有消息继续按 `(node_id, seq)` 幂等处理。

示例：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    node_id: 4096
    user_id: 1025
    password: "alice-password"
    seen_messages: [
      { node_id: 4096, seq: 1 },
      { node_id: 4096, seq: 2 },
      { node_id: 4097, seq: 8 }
    ]
  }
}
```

`seen_messages` 可以包含来自多个生产节点的消息游标。

## Channel 与 Broadcast 流程

channel：

1. 管理员创建 `role=channel` 用户。
2. 普通用户订阅该 channel。
3. 普通用户或管理员向 channel 地址发消息。
4. 订阅者通过 WebSocket 收到订阅时间之后的 channel 消息。

broadcast：

1. 每个节点启动时会创建系统 broadcast 地址，通常是 `user_id = 2`。
2. 管理员可以向任意 broadcast 地址发送消息。
3. 普通用户读取或连接 WebSocket 时，会看到仍在本地消息窗口内的 broadcast 消息。

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

- `unauthorized`：停止自动重试，提示用户重新输入密码或重新获取账号信息。
- `invalid_request`：检查客户端构造的 protobuf 字段，通常是目标缺失、正文为空或参数非法。
- `forbidden`：提示没有权限，必要时刷新订阅关系或联系管理员。
- `not_found`：目标用户、channel 或 broadcast 地址不存在。
- `internal_error`：保留连接并稍后重试；如果连接断开，按断线重连流程处理。

登录阶段的错误会导致服务端关闭连接。登录成功后的请求级错误通常不会关闭连接。

## 跨节点连接

集群中任意节点都可以提供 `GET /ws/client`：

- 用户 token 只用于 HTTP；WebSocket 使用密码首帧登录。
- 用户和消息通过集群复制最终一致。
- 切换连接节点时，客户端仍按 `(node_id, seq)` 去重。
- 不同节点在短时间内可能因为复制延迟或消息窗口裁剪而看到不同集合，稳定后会按集群规则收敛。

## 最小客户端状态机

```text
Disconnected
  -> connect websocket
Connecting
  -> send LoginRequest(seen_messages)
Authenticating
  -> receive LoginResponse
Online
  -> receive MessagePushed: persist + cursor + optional ack
  -> send SendMessageRequest: wait matching request_id response
  -> receive Error: handle by code
  -> socket closed: Disconnected with backoff
```

## 验收清单

- 能创建普通用户并记录 `(node_id, user_id)`。
- 能用 WebSocket 第一帧登录成功。
- 能接收历史补发消息。
- 能接收实时消息。
- 能发送非 UTF-8 `bytes body` 消息。
- 能把 `(node_id, seq)` 持久化为本地游标。
- 重连时能携带 `seen_messages` 并避免重复展示。
- 能订阅 channel 并只收到订阅后的 channel 消息。
- 能收到 broadcast 消息。
- 能正确处理 `Error.code`。
