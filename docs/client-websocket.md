# 客户端长连接接口

本文档描述业务客户端使用的长连接 + Protobuf 接口。客户端可以通过 WebSocket 或 ZeroMQ 连接服务端；节点间集群同步仍使用 `GET /internal/cluster/ws` 或 `services.zeromq.bind_url` 对应的集群链路，不要和本文的客户端接口混用。完整接入步骤见 [客户端全流程接入文档](client-flow.md)。

当前长连接接口的“标准流”是 `GET /ws/client` 和 ZeroMQ 客户端连接。它覆盖了当前客户端的大部分能力：持久化消息收发、瞬时包收发、用户/附件/元数据管理、历史查询和运维查询；但它与 HTTP API 不是逐个路由一比一映射，订阅和黑名单能力在长连接协议中统一通过 `user_attachment` RPC 暴露。`GET /ws/realtime` 只暴露受限子集，见下文“实现边界”。

## 连接与传输

- 标准 WebSocket：`GET /ws/client`
- 受限实时流：`GET /ws/realtime`
- ZeroMQ 客户端：`zmq+tcp://host:port`，其中 `host:port` 对应服务端 `services.zeromq.bind_url`
- WebSocket binary frame 或 ZeroMQ message payload：每一帧都是一个完整 protobuf message
- 客户端发送类型：`notifier.client.v1.ClientEnvelope`
- 服务端发送类型：`notifier.client.v1.ServerEnvelope`
- 协议定义：`proto/client.proto`、`proto/transport.proto`

服务端不使用 query token 或 HTTP `Authorization` header 做长连接鉴权。

- WebSocket：连接升级成功后，客户端发送的第一帧必须是 `ClientEnvelope.login`
- ZeroMQ：第一帧必须先发送 `notifier.transport.v1.ZeroMQMuxHello{role=ZERO_MQ_ROLE_CLIENT, protocol_version="zeromq-mux-v1"}`，第二帧必须是 `ClientEnvelope.login`
- 客户端必须在进入业务会话处理后的 45 秒内完成登录；超时后服务端关闭连接，不产生在线状态或 `session_ref`
- 当前客户端协议版本常量是 `client-v1alpha5`
- 当前 ZeroMQ mux 协议版本常量是 `zeromq-mux-v1`

`client-v1alpha5` 是当前 `ClientEnvelope` / `ServerEnvelope` wire schema 的严格 epoch，不是可协商的版本列表。服务端只有这一套客户端协议实现；客户端必须在每次初连和重连的 `LoginRequest.protocol_version` 中精确声明该值，并在收到 `LoginResponse` 后再次确认服务端返回相同值。

历史版本曾复用已经删除的 protobuf tag：例如 `ClientEnvelope` 的 10-12 从订阅 RPC 改作 attachment RPC，18-20 也被赋予新的 RPC；`ServerEnvelope` 同样存在历史名称与当前字段共享编号的情况。已被当前字段占用的旧编号无法再声明为 `reserved`，因此不能仅依靠 protobuf 的未知字段规则安全混跑旧客户端。`client-v1alpha5` 的严格门禁用于把这段不可逆的 wire 历史隔离在新的 epoch 之外。

协议中仍可追溯的历史字段名和未复用编号已经声明为 `reserved`。今后删除任何 protobuf 字段或 enum value 时，必须同时保留其名称和编号；禁止把已删除编号重新赋给其他语义。

如果服务端启用了 `services.zeromq.security = "curve"`，客户端在连接 ZeroMQ 前还必须配置服务端 `server_public_key` 以及自己的 CURVE `client_public_key` / `client_secret_key`。客户端 public key 必须出现在服务端 `services.zeromq.curve.allowed_client_public_keys` 中；CURVE 只完成链路加密和传输层公钥白名单，业务身份仍以 `ClientEnvelope.login` 为准。

ZeroMQ TLS 不在应用内实现，也不新增 `zmq+tls` URL。需要 TLS 证书体系时，应在 ZeroMQ TCP 端口外层使用 TCP TLS 隧道，或选择 WebSocket `wss`。

## 当前消息类型

当前 `ClientEnvelope` oneof 包含：

- `login`
- `send_message`
- `ack_message`
- `ping`
- `create_user`
- `get_user`
- `update_user`
- `delete_user`
- `list_messages`
- `upsert_user_attachment`
- `delete_user_attachment`
- `list_user_attachments`
- `list_events`
- `operations_status`
- `metrics`
- `list_cluster_nodes`
- `list_node_logged_in_users`
- `resolve_user_sessions`
- `get_user_metadata`
- `upsert_user_metadata`
- `delete_user_metadata`
- `scan_user_metadata`
- `list_users`

当前 `ServerEnvelope` oneof 包含：

- `login_response`
- `message_pushed`
- `send_message_response`
- `error`
- `pong`
- `packet_pushed`
- `create_user_response`
- `get_user_response`
- `update_user_response`
- `delete_user_response`
- `list_messages_response`
- `upsert_user_attachment_response`
- `delete_user_attachment_response`
- `list_user_attachments_response`
- `list_events_response`
- `operations_status_response`
- `metrics_response`
- `list_cluster_nodes_response`
- `list_node_logged_in_users_response`
- `resolve_user_sessions_response`
- `get_user_metadata_response`
- `upsert_user_metadata_response`
- `delete_user_metadata_response`
- `scan_user_metadata_response`
- `list_users_response`

当前协议里已经没有专用的 `subscribe_channel`、`unsubscribe_channel`、`list_subscriptions`、`block_user`、`unblock_user`、`list_blocked_users` 消息名：

- 频道订阅管理改为 `upsert_user_attachment` / `delete_user_attachment` / `list_user_attachments` + `ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION`
- 黑名单管理改为 `upsert_user_attachment` / `delete_user_attachment` / `list_user_attachments` + `ATTACHMENT_TYPE_USER_BLACKLIST`

## 登录流程

客户端第一帧示例：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    password: "alice-password"
    protocol_version: "client-v1alpha5"
    seen_messages: [
      { node_id: 4096, seq: 1 },
      { node_id: 4096, seq: 2 }
    ]
  }
}
```

也可以改用登录名：

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

首次密码登录成功后，后续重连可以保留相同的用户选择器并用短期凭据替代密码：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    reconnect_token: "..."
    protocol_version: "client-v1alpha5"
    seen_messages: [
      { node_id: 4096, seq: 2 }
    ]
  }
}
```

字段说明：

- `user` 与 `login_name` 二选一，而且必须恰好提供一个。两者同时提供、两者都不提供，都会按登录失败处理。
- `password` / `reconnect_token`：首次登录提交密码；成功后优先保存并使用短期 `reconnect_token`。只有可登录用户能通过长连接登录；`role=channel`、`role=broadcast`、`role=node` 仍不可登录。
- 同时提供 `reconnect_token` 和 `password` 时，服务端只校验 token，不在 token 失效时回退到 bcrypt。客户端收到 `unauthorized` 后应清除 token，再显式发起一次密码登录。
- `protocol_version`：必填语义字段，必须精确等于 `client-v1alpha5`。空值、旧版本和未知版本都会在认证前被拒绝。
- `seen_messages`：客户端已经持久化的消息游标集合。每个游标是消息生产节点和该节点消息序号的二元组 `(node_id, seq)`。
- `transient_only`：只关闭持久化历史补发与后续 `MessagePushed` 推送，不会把该连接变成 `/ws/realtime` 那种“受限消息集”。

`/ws/realtime` 的登录仍然使用同一个 `LoginRequest`，但服务端会强制采用 `transient_only` 语义：不补发历史消息，也不注册持久化推送。

登录成功后，服务端返回：

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
    reconnect_token: "..."
    reconnect_token_expires_at_unix: 1786252742
  }
}
```

字段说明：

- `login_response.user` 是当前登录用户的可见信息；如果该用户设置了 `login_name`，这里会返回。
- `protocol_version` 固定为 `client-v1alpha5`。客户端必须先验证该值，再发布已登录状态、保存 `session_ref` 或触发登录成功回调。
- `session_ref` 是本次连接的全局会话引用，后续可作为瞬时包的 `target_session` 使用。
- `session_ref` 是不透明标识，客户端应原样保存和回传，不要自行拼接或猜测语义。
- `reconnect_token` 是专用于 WebSocket / ZeroMQ 后续登录的短期签名凭据，不能作为 HTTP Bearer token 使用。每次成功登录都会刷新 token 和 `reconnect_token_expires_at_unix`。
- token 同时绑定用户和密码版本；密码变更、用户删除或角色变为不可登录后立即失效。默认有效期为 5 分钟，可通过 `auth.reconnect_token_ttl_minutes` 调整。

服务端解析出登录首帧后，会先检查协议版本，再读取用户选择器、验证凭据、处理游标、生成 `session_ref` 或注册会话：

- `protocol_version` 缺失或不等于 `client-v1alpha5` 时，返回 `ServerEnvelope.error{code="unsupported_protocol_version", request_id=0}`。`message` 包含收到值和服务端要求值，随后关闭连接；不会认证、注册会话或补发历史消息。
- 版本正确但登录帧格式、用户选择器或凭据不合法时，仍返回 `ServerEnvelope.error{code="unauthorized", request_id=0}`，然后关闭连接。
- 首次密码验证仍使用原 bcrypt 成本；重连 token 使用 HMAC 验签，不降低密码哈希强度。冷启动或 token 已过期的大规模首次登录仍需要在客户端侧限制并发。

## 持久化消息、游标与 ack

客户端用 `MessageCursor{node_id, seq}` 维护已收消息进度：

- `node_id`：生产这条消息的节点。
- `seq`：该生产节点为这条目标消息分配的序号。
- 客户端收到并持久化消息后，应保存 `(node_id, seq)`。
- 客户端重连登录时，把已持久化游标放入 `LoginRequest.seen_messages`，服务端会在连接内去重时跳过这些消息。

标准流在登录成功后会这样处理持久化消息：

- `/ws/client` 和 ZeroMQ 标准连接在 `transient_only=false` 时，会补发当前用户可见、且不在 `seen_messages` 内的历史消息。
- 当前登录补发批量上限是 `1000` 条，发送顺序是从旧到新。
- 补发结束后，连接会继续收到新的 `MessagePushed`。
- `transient_only=true` 或 `/ws/realtime` 都不会收到历史补发，也不会再收到新的 `MessagePushed`。

当前实现里的可见范围：

- 登录用户自己的消息。
- 所有仍在本地消息窗口内的 `role=broadcast` 消息。
- 登录用户已订阅频道，且订阅时间之后的 `role=channel` 消息。
- 管理员用户可见任意目标地址的持久化消息。
- 普通用户自己的直发消息仍受黑名单时序约束影响：被拉黑后的新直发消息不会进入该用户可见结果。

服务端推送持久化消息：

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

客户端收到并落盘后，可以发送：

```protobuf
ClientEnvelope {
  ack_message: AckMessage {
    cursor: { node_id: 4096, seq: 3 }
  }
}
```

`AckMessage` 的语义边界：

- 它只是当前连接内的可选去重提示。
- 服务端会把这个游标写进当前会话内存中的 `seen` 集合。
- 服务端不会把 `AckMessage` 状态持久化到数据库。
- 可靠重连仍然依赖客户端在下次 `LoginRequest.seen_messages` 中重新上报。

## 瞬时包

服务端推送瞬时包时使用 `PacketPushed`：

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

`PacketPushed` 与 `MessagePushed` 的区别：

- `PacketPushed` 只用于 `delivery_kind = CLIENT_DELIVERY_KIND_TRANSIENT` 的瞬时包。
- 瞬时包没有 `(node_id, seq)` 游标，不参与 `seen_messages` 和 `AckMessage`。
- 瞬时包不会在重连后补发。
- `packet.target_session` 只在发送端显式指定了目标会话时出现。

## 发送消息

发送持久化消息：

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

发送定向瞬时包：

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 43
    target: { node_id: 8192, user_id: 1025 }
    body: "\xff\x00payload"
    delivery_kind: CLIENT_DELIVERY_KIND_TRANSIENT
    delivery_mode: CLIENT_DELIVERY_MODE_ROUTE_RETRY
    target_session: {
      serving_node_id: 8192
      session_id: "..."
    }
  }
}
```

字段说明：

- `request_id`：客户端生成的请求 ID，服务端在响应或错误中原样返回。
- `target`：消息目标用户、频道或 broadcast 地址；但瞬时包只允许目标是“可登录用户”。
- `body`：原始字节数组，不能为空；不要求 UTF-8。
- `delivery_kind`：可选 `CLIENT_DELIVERY_KIND_PERSISTENT` 或 `CLIENT_DELIVERY_KIND_TRANSIENT`；省略时按持久化消息处理。
- `delivery_mode`：只允许用于瞬时包；可选 `CLIENT_DELIVERY_MODE_BEST_EFFORT` 或 `CLIENT_DELIVERY_MODE_ROUTE_RETRY`。
- `sync_mode`：只允许用于持久化消息；可选 `CLIENT_MESSAGE_SYNC_MODE_FORCE_SYNC`、`CLIENT_MESSAGE_SYNC_MODE_NO_SYNC`。当前在 Pebble 后端上会影响消息写入同步策略；SQLite 后端接受该字段，但不会提供额外同步语义。
- `target_session`：只允许用于瞬时包；必须是先前登录或 `resolve_user_sessions` 返回的有效 `session_ref`。

权限规则与 HTTP 写消息接口一致：

- 普通用户可以给自己发消息。
- 普通用户可以给其他可登录用户发消息。
- 普通用户可以给自己具备写权限的 `role=channel` 地址发消息。
- 管理员可以给任意用户、频道或 broadcast 地址发持久化消息。
- 瞬时包只允许目标是可登录用户；给 `channel`、`broadcast`、`node` 发瞬时包会返回 `invalid_request`。

成功响应：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 42
    message: {
      recipient: { node_id: 4096, user_id: 1025 }
      node_id: 4096
      seq: 4
      sender: { node_id: 4096, user_id: 1 }
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

持久化发送的实现边界：

- 服务端会把响应里的 `(node_id, seq)` 立即标记为当前连接已见。
- 因此“给自己发持久化消息”的同一条连接通常只会收到 `send_message_response.message`，不会再额外收到一份重复的 `message_pushed`。

目标用户瞬时包受理响应：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 43
    transient_accepted: {
      packet_id: 77
      source_node_id: 4096
      target_node_id: 8192
      recipient: { node_id: 8192, user_id: 1025 }
      delivery_mode: CLIENT_DELIVERY_MODE_ROUTE_RETRY
      target_session: {
        serving_node_id: 8192
        session_id: "..."
      }
    }
  }
}
```

`transient_accepted` 的语义边界：

- 只表示瞬时包已经进入本地路由层。
- 不代表目标用户已经收到。
- 若指定了 `target_session`，但该会话不存在、已离线，或不属于目标用户，会返回 `not_found`。
- `CLIENT_DELIVERY_MODE_ROUTE_RETRY` 只表示节点间会短时间尝试重新寻路；它仍然是非持久化、非可靠投递。

## 查询与管理 RPC

标准流支持的 RPC 请求如下：

- 用户管理：`create_user`、`get_user`、`update_user`、`delete_user`、`list_users`
- 消息查询：`list_messages`
- 附件管理：`upsert_user_attachment`、`delete_user_attachment`、`list_user_attachments`
- 用户元数据：`get_user_metadata`、`upsert_user_metadata`、`delete_user_metadata`、`scan_user_metadata`
- 集群与在线态：`list_cluster_nodes`、`list_node_logged_in_users`、`resolve_user_sessions`
- 运维查询：`list_events`、`operations_status`、`metrics`
- 辅助消息：`ack_message`、`ping`

`/ws/realtime` 只支持以下子集：

- `send_message`，但只允许 `delivery_kind=CLIENT_DELIVERY_KIND_TRANSIENT`
- `list_cluster_nodes`
- `list_node_logged_in_users`
- `resolve_user_sessions`
- `ack_message`
- `ping`

其余请求在 `/ws/realtime` 上都会返回 `invalid_request`。

ZeroMQ 没有独立的“realtime path”：

- ZeroMQ 普通连接默认等价于 `/ws/client`
- `LoginRequest.transient_only=true` 只关闭持久化推送
- 它不会把 ZeroMQ 连接变成 `/ws/realtime` 那种受限 RPC 集

### 订阅与黑名单的当前映射

- 订阅频道：`upsert_user_attachment{attachment_type=ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION}`
- 取消订阅：`delete_user_attachment{attachment_type=ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION}`
- 查询订阅：`list_user_attachments{attachment_type=ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION}`
- 拉黑用户：`upsert_user_attachment{attachment_type=ATTACHMENT_TYPE_USER_BLACKLIST}`
- 取消拉黑：`delete_user_attachment{attachment_type=ATTACHMENT_TYPE_USER_BLACKLIST}`
- 查询黑名单：`list_user_attachments{attachment_type=ATTACHMENT_TYPE_USER_BLACKLIST}`

示例：订阅频道

```protobuf
ClientEnvelope {
  upsert_user_attachment: UpsertUserAttachmentRequest {
    request_id: 1001
    owner: { node_id: 4096, user_id: 1025 }
    subject: { node_id: 4096, user_id: 2048 }
    attachment_type: ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
    config_json: "{}"
  }
}
```

### 自作用目标兼容

- `get_user.user`、`list_messages.user`、用户元数据请求中的 `owner` 为空时，服务端会自动解释为当前登录用户。
- `list_user_attachments.owner` 为空时，也支持回退为当前登录用户；当前请求里的 `attachment_type` 允许为空、`ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION` 或 `ATTACHMENT_TYPE_USER_BLACKLIST`。
- `upsert_user_attachment.owner`、`delete_user_attachment.owner` 只在 `attachment_type=ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION` 或 `ATTACHMENT_TYPE_USER_BLACKLIST` 时支持空值回退；`ATTACHMENT_TYPE_CHANNEL_MANAGER` / `ATTACHMENT_TYPE_CHANNEL_WRITER` 仍必须显式指定频道 owner。
- `node_id`、`user_id` 只填一个、填负数或其他无效组合仍会返回 `invalid_request`，不会回退到当前登录用户。

### `resolve_user_sessions`

`resolve_user_sessions` 用于拿到某个用户当前在线的节点和会话细节，供定向瞬时包使用：

```protobuf
ClientEnvelope {
  resolve_user_sessions: ResolveUserSessionsRequest {
    request_id: 1002
    user: { node_id: 4096, user_id: 1025 }
  }
}
```

```protobuf
ServerEnvelope {
  resolve_user_sessions_response: ResolveUserSessionsResponse {
    request_id: 1002
    user: { node_id: 4096, user_id: 1025 }
    presence: [
      { serving_node_id: 4096, session_count: 2, transport_hint: "ws" }
    ]
    items: [
      {
        session: { serving_node_id: 4096, session_id: "sess-a" }
        transport: "ws"
        transient_capable: true
      }
    ]
    count: 1
  }
}
```

实现边界：

- 该 RPC 的权限校验与 `send_message` 共用同一套“能否给目标发消息”的规则。
- 返回结果里的 `session` 可直接作为后续瞬时包 `target_session` 的输入。
- `presence` 表示节点级在线存在性，`items` 表示已解析到的具体会话。

### 权限与可见性边界

- `list_cluster_nodes`、`list_node_logged_in_users`：任意已登录用户都可调用。
- `resolve_user_sessions`：任意已登录用户可调用，但目标必须满足与 `send_message` 一致的权限约束。
- `list_events`、`operations_status`、`metrics`：只允许 `role=admin` 和 `role=super_admin`。
- `create_user`、`update_user`、`delete_user`：只允许 `role=admin` 和 `role=super_admin`；其中创建 `admin`、把用户提升为 `admin`、把 `admin` 降权回普通角色、修改或删除 `admin`，仅 `super_admin` 可执行。
- 系统保留用户（bootstrap `super_admin`、`broadcast`、`node`）仍不能通过管理 RPC 修改或删除。
- `list_users`：普通用户返回当前可通讯用户集合；管理员和超级管理员返回全量活跃用户。
- 普通用户的 `list_users.name` 匹配 `username` 与 `profile_json.display_name/displayName`；管理员额外匹配 `login_name`。
- 普通用户通过 `list_users` 看到他人时，`login_name` 会被隐藏；查看自己或管理员查看任意用户时，`login_name` 保持可见。
- 若目标用户或频道显式写入 metadata `system.visible_to_others=false`，它会从普通用户的 `list_users` 结果中隐藏；管理员和本人仍可看到，且该属性不影响已知 `uid` 后继续发消息。
- `list_messages`：对可登录用户允许本人或管理员；对频道或 broadcast 目标仅管理员可直接查询。
- 用户元数据：普通用户对自己可读写；管理员可读写任意普通用户；频道 owner 的元数据允许频道管理员或管理员访问。
- 附件权限按类型区分：订阅/黑名单允许本人或管理员；频道管理/写入关系仍要求显式频道 owner 和原有频道管理权限。
- `list_node_logged_in_users_response.items` 当前会直接带回 `login_name` 字段，不再额外做 `list_users` 那套可见性裁剪。

## Ping/Pong

客户端可发送应用层 ping：

```protobuf
ClientEnvelope {
  ping: Ping { request_id: 7 }
}
```

服务端返回：

```protobuf
ServerEnvelope {
  pong: Pong { request_id: 7 }
}
```

应用层 Ping/Pong 用于客户端自行观测请求链路，不是 ZeroMQ 物理断线检测的唯一来源。ZeroMQ TCP 连接同时使用 15 秒间隔、45 秒超时的 ZMTP heartbeat；服务端允许已登录连接长期没有业务消息，不设置应用层空闲超时。物理断线或 heartbeat 超时后，服务端会注销在线状态和该连接的 `session_ref`。

## 错误语义

错误统一使用：

```protobuf
ServerEnvelope {
  error: Error {
    code: "invalid_request"
    message: "target is required"
    request_id: 42
  }
}
```

登录阶段：

- `protocol_version` 缺失或不等于 `client-v1alpha5`：映射为 `code="unsupported_protocol_version"`、`request_id=0`，然后关闭连接

- 第一帧不是 `login`
- 第一帧不是 binary protobuf
- 登录帧无法解码
- `user` 与 `login_name` 选择器非法
- 用户名/密码错误

除版本不兼容外，这些情况都会被映射成 `code="unauthorized"`，然后关闭连接。

登录成功后的常见错误码：

- `invalid_frame`：WebSocket 收到了非 binary frame
- `invalid_protobuf`：binary frame 不是合法 `ClientEnvelope`
- `invalid_message`：消息体类型不受当前实现支持
- `already_authenticated`：登录成功后再次发送 `login`
- `invalid_request`：字段缺失、参数非法、`body` 为空、`target_session` 格式不合法、在错误的 `delivery_kind` 下使用 `sync_mode` / `delivery_mode` / `target_session` 等
- `forbidden`：当前用户没有执行该操作的权限，或被黑名单拦截
- `not_found`：目标用户、目标资源或目标会话不存在
- `conflict`：资源状态冲突
- `service_unavailable`：当前节点暂时不可写或相关集群查询能力暂不可用
- `internal_error`：服务端内部错误

`request_id` 规则：

- 对请求级 RPC，服务端会在成功响应和 `error` 中原样回传客户端的 `request_id`
- 对登录、`ack_message`、协议层错误等无 `request_id` 的场景，返回 `0`

连接关闭规则：

- 登录阶段返回错误后，服务端会关闭连接
- 登录成功后的请求级错误通常不会立即关闭连接；客户端可以继续发送后续合法请求

## 客户端实现建议

- 持久化消息时至少保存完整 `Message` 和游标 `(node_id, seq)`。
- 重连时把本地已持久化游标放入 `LoginRequest.seen_messages`；不要依赖单独的 `AckMessage`。
- 成功登录后安全保存最新 `reconnect_token` 及其过期时间；重连优先使用它，认证失败时清除后再进行一次密码登录。
- 收到重复 `(node_id, seq)` 时应幂等忽略。
- 如果业务会缓存瞬时包，应自行按 `packet_id` 做去重；不要把它混进持久化消息游标表。
- `session_ref` 与 `target_session` 都应按不透明标识保存和回传。
- `body` 是原始字节，不要按字符串处理；需要文本时由业务层自行约定编码。
- 如果客户端切换连接节点，仍应按 `(node_id, seq)` 去重；不同节点的本地消息窗口可能暂时不完全一致，集群最终会收敛。
- 不要依赖服务端保存无限历史；当前登录自动补发最多 `1000` 条，历史范围还受本地消息窗口影响。
- 如果你只想禁用持久化推送但仍要保留完整 RPC 能力，使用 `/ws/client` 或 ZeroMQ，并在登录时设置 `transient_only=true`。
- 如果你选择 `/ws/realtime`，要预期它只能做瞬时收发和在线态查询，不能执行 `list_messages`、用户/元数据/附件管理或运维查询。
