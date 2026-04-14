# 客户端长连接接口

本文档描述业务客户端使用的长连接 + Protobuf 接口。客户端可以通过 WebSocket 或 ZeroMQ 连接服务端；节点间集群同步仍使用 `GET /internal/cluster/ws` 或 `cluster.zeromq.bind_url` 对应的集群链路，不要和本文的客户端接口混用。完整接入步骤见 [客户端全流程接入文档](/root/dev/sys/turntf/docs/client-flow.md)。

当前客户端长连接已覆盖除 `POST /auth/login` 之外的全部 HTTP 客户端能力：既能收发消息，也能执行用户管理、订阅管理、历史查询和运维查询。

## 连接地址

- WebSocket：`GET /ws/client`
- ZeroMQ：`zmq+tcp://host:port`，其中 `host:port` 对应服务端 `cluster.zeromq.bind_url`
- WebSocket frame 或 ZeroMQ message 内容：每一帧都是一个完整 protobuf message
- 客户端发送类型：`notifier.client.v1.ClientEnvelope`
- 服务端发送类型：`notifier.client.v1.ServerEnvelope`
- ZeroMQ 首包：`notifier.transport.v1.ZeroMQMuxHello`
- 协议定义：`proto/client.proto`、`proto/transport.proto`

服务端不使用 query token 或 HTTP `Authorization` header 做长连接鉴权。

- WebSocket：连接升级成功后，客户端发送的第一帧必须是 `ClientEnvelope.login`
- ZeroMQ：第一帧必须发送 `ZeroMQMuxHello{role=CLIENT, protocol_version="zeromq-mux-v1"}`，第二帧必须是 `ClientEnvelope.login`

## 登录流程

客户端第一帧：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    node_id: 4096
    user_id: 1025
    password: "alice-password"
    seen_messages: [
      { node_id: 4096, seq: 1 },
      { node_id: 4096, seq: 2 }
    ]
  }
}
```

字段说明：

- `node_id` 和 `user_id`：登录用户身份，对应 HTTP 登录接口中的同名字段。
- `password`：用户密码。`role=channel`、`role=broadcast` 和 `role=node` 用户不可登录。
- `seen_messages`：客户端已经持久化的消息游标集合。每个游标是消息生产节点和该节点消息序号的二元组 `(node_id, seq)`。

登录成功后，服务端返回：

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

登录失败时，服务端返回 `ServerEnvelope.error`，然后关闭连接。

## 消息身份与客户端游标

客户端用 `MessageCursor{node_id, seq}` 维护已收消息进度：

- `node_id`：生产该消息的节点。
- `seq`：该生产节点为目标用户生成的消息序号。
- 客户端收到并持久化消息后，应保存 `(node_id, seq)`。
- 客户端重连登录时，把已持久化的游标放入 `LoginRequest.seen_messages`，服务端会跳过这些消息。
- 服务端推送的 `Message` 通过 `recipient: UserRef` 标识该消息归属的目标用户、channel 或 broadcast 地址。

注意：当前服务端只在连接内存中使用 `AckMessage` 更新去重集合，不会把客户端 ack 状态写入数据库。可靠重连依赖客户端在下次 `LoginRequest.seen_messages` 中上报已持久化游标。

## 接收消息

登录成功后，服务端会先补发当前用户可见且不在 `seen_messages` 中的历史消息，然后继续推送实时消息。

服务端消息：

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

目标用户瞬时包推送：

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
    }
  }
}
```

可见消息范围：

- 登录用户自己的消息。
- 所有仍在本地窗口内的 `role=broadcast` 消息。
- 登录用户已订阅 channel 且订阅时间之后的 channel 消息。
- 管理员用户可见任意目标地址的消息。

`PacketPushed` 与 `MessagePushed` 的区别：

- `PacketPushed` 只用于 `delivery_kind = TRANSIENT` 的瞬时包。
- 瞬时包没有 `(node_id, seq)` 游标，不参与 `seen_messages` 和 `AckMessage`。
- 瞬时包不会在重连后补发；只有目标用户当前在线时才能收到。

客户端收到并落盘后，可以发送：

```protobuf
ClientEnvelope {
  ack_message: AckMessage {
    cursor: { node_id: 4096, seq: 3 }
  }
}
```

`AckMessage` 是可选的连接内去重提示。即使不发送 ack，只要下次登录带上 `seen_messages`，服务端也会跳过已见消息。

## 发送消息

客户端发送：

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 42
    target: { node_id: 4096, user_id: 1025 }
    body: "\xff\x00payload"
  }
}
```

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

字段说明：

- `request_id`：客户端生成的请求 ID，服务端在响应或错误中原样返回。
- `target`：消息目标用户、channel 或 broadcast 地址。
- `sender`：不再由客户端传入，服务端会根据已认证连接生成 `UserRef { node_id, user_id }`。
- `body`：原始字节数组，不能为空；不要求 UTF-8。
- `delivery_kind`：可选 `CLIENT_DELIVERY_KIND_PERSISTENT` 或 `CLIENT_DELIVERY_KIND_TRANSIENT`，默认是持久化消息。
- `delivery_mode`：仅在 `delivery_kind = CLIENT_DELIVERY_KIND_TRANSIENT` 时生效；可选 `CLIENT_DELIVERY_MODE_BEST_EFFORT` 或 `CLIENT_DELIVERY_MODE_ROUTE_RETRY`。

权限规则与 HTTP 写消息接口一致：

- 普通用户只能给自己发消息。
- 普通用户可以给自己已订阅的 `role=channel` 地址发消息。
- 管理员可以给任意用户、channel 或 broadcast 地址发消息。
- 瞬时消息只能发给可登录用户；普通用户只能发给自己，管理员可以发给任意可登录用户。

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
    }
  }
}
```

`transient_accepted` 只表示瞬时包已进入本地路由层，不代表目标用户已经收到。

## 查询与管理 RPC

登录成功后，客户端还可以在同一条 WS 连接上发送 RPC 请求，补齐原先 HTTP JSON API 的能力。所有这类请求都使用独立的 `request_id`，成功响应和 `ServerEnvelope.error` 都会原样回传该值。

当前支持：

- 用户管理：`create_user`、`get_user`、`update_user`、`delete_user`
- 消息与订阅查询：`list_messages`、`list_subscriptions`
- 订阅管理：`subscribe_channel`、`unsubscribe_channel`
- 黑名单管理：`block_user`、`unblock_user`、`list_blocked_users`
- 集群与运维查询：`list_cluster_nodes`、`list_node_logged_in_users`、`list_events`、`operations_status`、`metrics`

示例：管理员创建用户

```protobuf
ClientEnvelope {
  create_user: CreateUserRequest {
    request_id: 1001
    username: "alice"
    password: "alice-password"
    profile_json: "{\"display_name\":\"Alice\"}"
    role: "user"
  }
}
```

```protobuf
ServerEnvelope {
  create_user_response: CreateUserResponse {
    request_id: 1001
    user: {
      node_id: 4096
      user_id: 1025
      username: "alice"
      role: "user"
      profile_json: "{\"display_name\":\"Alice\"}"
    }
  }
}
```

示例：查询自己的订阅列表

```protobuf
ClientEnvelope {
  list_subscriptions: ListSubscriptionsRequest {
    request_id: 1002
    subscriber: { node_id: 4096, user_id: 1025 }
  }
}
```

示例：拉黑某个普通用户

```protobuf
ClientEnvelope {
  block_user: BlockUserRequest {
    request_id: 1006
    owner: { node_id: 4096, user_id: 1025 }
    blocked: { node_id: 4096, user_id: 1026 }
  }
}
```

```protobuf
ServerEnvelope {
  block_user_response: BlockUserResponse {
    request_id: 1006
    entry: {
      owner: { node_id: 4096, user_id: 1025 }
      blocked: { node_id: 4096, user_id: 1026 }
      blocked_at: "..."
    }
  }
}
```

```protobuf
ServerEnvelope {
  list_subscriptions_response: ListSubscriptionsResponse {
    request_id: 1002
    items: [
      {
        subscriber: { node_id: 4096, user_id: 1025 }
        channel: { node_id: 4096, user_id: 1026 }
        subscribed_at: "..."
      }
    ]
    count: 1
  }
}
```

示例：管理员查询 metrics

```protobuf
ClientEnvelope {
  metrics: MetricsRequest { request_id: 1003 }
}
```

```protobuf
ServerEnvelope {
  metrics_response: MetricsResponse {
    request_id: 1003
    text: "# HELP notifier_event_log_last_sequence ..."
  }
}
```

示例：普通用户查询已连接集群节点

```protobuf
ClientEnvelope {
  list_cluster_nodes: ListClusterNodesRequest { request_id: 1004 }
}
```

示例：普通用户查询某个节点当前已登录用户

`node_id` 表示最终目标节点，可以是非直连节点。只要集群路由可达，服务端会在节点间多跳转发该查询；查询与响应都会按当前节点路由继续转发，不要求原路返回。该查询链路带有内部最大跳数限制，超过限制或无可达路由时会返回 `service_unavailable`。

```protobuf
ClientEnvelope {
  list_node_logged_in_users: ListNodeLoggedInUsersRequest {
    request_id: 1005
    node_id: 4096
  }
}
```

```protobuf
ServerEnvelope {
  list_node_logged_in_users_response: ListNodeLoggedInUsersResponse {
    request_id: 1005
    target_node_id: 4096
    items: [
      { node_id: 4096, user_id: 1025, username: "alice" },
      { node_id: 4096, user_id: 1026, username: "bob" }
    ]
    count: 2
  }
}
```

```protobuf
ServerEnvelope {
  list_cluster_nodes_response: ListClusterNodesResponse {
    request_id: 1004
    items: [
      { node_id: 4096, is_local: true },
      { node_id: 8192, is_local: false, configured_url: "ws://127.0.0.1:9081/internal/cluster/ws", source: "static" },
      { node_id: 12288, is_local: false, configured_url: "ws://127.0.0.1:9082/internal/cluster/ws", source: "discovered" }
    ]
    count: 3
  }
}
```

权限边界与 HTTP 完全一致：

- `create_user`、`update_user`、`delete_user`、`list_events`、`operations_status`、`metrics` 仅管理员可用。
- `list_cluster_nodes` 只要求当前连接已登录，普通用户可用。
- `list_node_logged_in_users` 只要求当前连接已登录，普通用户可用；目标节点不可达时返回错误，不返回空列表。
- `get_user` 允许本人或管理员。
- `list_messages` 对可登录用户允许本人或管理员；对 channel/broadcast 目标仅管理员可直接查询。
- `subscribe_channel`、`unsubscribe_channel`、`list_subscriptions` 允许本人或管理员。
- `block_user`、`unblock_user`、`list_blocked_users` 允许本人或管理员；只能针对 `role=user` 的目标用户配置。
- `send_message` 的权限规则保持不变。

字段约定：

- `profile_json` 和 `event_json` 是原始 JSON 字节。
- `block_user` 只会拦截后续新的普通用户直发消息，不会删除已存在历史消息，也不会影响 channel/broadcast/node 地址。
- 列表响应统一包含 `items` 和 `count`。
- `metrics_response.text` 直接返回 Prometheus 文本，与 HTTP `/metrics` 内容一致。

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

## 错误

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

常见 `code`：

- `unauthorized`：登录失败、第一帧不是登录消息或登录帧无法解码。
- `invalid_frame`：客户端发送了非 binary frame。
- `invalid_protobuf`：binary frame 不是合法 `ClientEnvelope`。
- `invalid_message`：不支持的客户端消息类型。
- `already_authenticated`：登录成功后再次发送 login。
- `invalid_request`：请求字段缺失、正文为空或参数非法。
- `forbidden`：当前用户没有执行该操作的权限。
- `not_found`：请求的用户、订阅或其他资源不存在。
- `conflict`：资源状态冲突。
- `service_unavailable`：当前节点暂时不可写，例如集群模式下仍未完成首轮校时。
- `not_found`：目标资源不存在。
- `internal_error`：服务端内部错误。

登录阶段返回错误后服务端会关闭连接。登录成功后的请求级错误通常不会立即关闭连接。

## 客户端实现建议

- 持久化消息时至少保存完整 `Message` 和游标 `(node_id, seq)`。
- 重连时把本地已持久化游标放入 `LoginRequest.seen_messages`。
- 收到重复 `(node_id, seq)` 时应幂等忽略。
- 收到 `PacketPushed` 时不要写入消息游标表；如果业务要本地暂存，应自行按 `packet_id` 做去重。
- `body` 是原始字节，不要按字符串处理；需要文本时由业务层自行约定编码。
- 如果客户端切换连接节点，仍应按 `(node_id, seq)` 去重；不同节点的可见窗口可能暂时不完全一致，集群最终会收敛。
- 当前服务端补发历史消息上限来自本地消息窗口和一次登录补发批量，客户端不要依赖服务端保存无限历史。
- `CLIENT_DELIVERY_MODE_ROUTE_RETRY` 只表示节点间在短时间内尝试重新寻路；它仍然是非持久化、非可靠投递。
