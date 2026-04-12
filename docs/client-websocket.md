# 客户端 WebSocket 接口

本文档描述业务客户端使用的 WebSocket + Protobuf 长连接接口。节点间集群同步仍使用 `GET /internal/cluster/ws`，不要和本文的客户端接口混用。完整接入步骤见 [客户端全流程接入文档](/root/dev/sys/turntf/docs/client-flow.md)。

## 连接地址

- 路径：`GET /ws/client`
- WebSocket frame 类型：只支持 binary frame
- frame 内容：每个 binary frame 是一个完整 protobuf message
- 客户端发送类型：`notifier.client.v1.ClientEnvelope`
- 服务端发送类型：`notifier.client.v1.ServerEnvelope`
- 协议定义：`proto/client.proto`

服务端不使用 query token 或 HTTP `Authorization` header 做 WebSocket 鉴权。连接升级成功后，客户端发送的第一帧必须是 `ClientEnvelope.login`。

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
- `password`：用户密码。`role=channel` 和 `role=broadcast` 用户不可登录。
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
- 服务端推送的 `Message` 仍包含 `user_node_id` 和 `user_id`，用于说明该消息归属的目标用户、channel 或 broadcast 地址。

注意：当前服务端只在连接内存中使用 `AckMessage` 更新去重集合，不会把客户端 ack 状态写入数据库。可靠重连依赖客户端在下次 `LoginRequest.seen_messages` 中上报已持久化游标。

## 接收消息

登录成功后，服务端会先补发当前用户可见且不在 `seen_messages` 中的历史消息，然后继续推送实时消息。

服务端消息：

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

可见消息范围：

- 登录用户自己的消息。
- 所有仍在本地窗口内的 `role=broadcast` 消息。
- 登录用户已订阅 channel 且订阅时间之后的 channel 消息。
- 管理员用户可见任意目标地址的消息。

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
    sender: "orders"
    body: "\xff\x00payload"
  }
}
```

字段说明：

- `request_id`：客户端生成的请求 ID，服务端在响应或错误中原样返回。
- `target`：消息目标用户、channel 或 broadcast 地址。
- `sender`：发送方或来源标签，不能为空。
- `body`：原始字节数组，不能为空；不要求 UTF-8。

权限规则与 HTTP 写消息接口一致：

- 普通用户只能给自己发消息。
- 普通用户可以给自己已订阅的 `role=channel` 地址发消息。
- 管理员可以给任意用户、channel 或 broadcast 地址发消息。

成功响应：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 42
    message: {
      user_node_id: 4096
      user_id: 1025
      node_id: 4096
      seq: 4
      sender: "orders"
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

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
- `not_found`：目标资源不存在。
- `internal_error`：服务端内部错误。

登录阶段返回错误后服务端会关闭连接。登录成功后的请求级错误通常不会立即关闭连接。

## 客户端实现建议

- 持久化消息时至少保存完整 `Message` 和游标 `(node_id, seq)`。
- 重连时把本地已持久化游标放入 `LoginRequest.seen_messages`。
- 收到重复 `(node_id, seq)` 时应幂等忽略。
- `body` 是原始字节，不要按字符串处理；需要文本时由业务层自行约定编码。
- 如果客户端切换连接节点，仍应按 `(node_id, seq)` 去重；不同节点的可见窗口可能暂时不完全一致，集群最终会收敛。
- 当前服务端补发历史消息上限来自本地消息窗口和一次登录补发批量，客户端不要依赖服务端保存无限历史。
