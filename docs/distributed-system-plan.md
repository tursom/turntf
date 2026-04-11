# 分布式通知服务实施计划

这份文档记录一个全新的分布式通知服务方案，不要求兼容当前仓库里的单机实现。目标是为后续实现提供可以直接执行的架构和落地顺序。

## 目标

- 任意节点都能接收用户写入和消息写入。
- 节点之间通过 WebSocket 长连接互联。
- 节点间同步协议使用 Protobuf。
- 用户数据要求最终完全一致。
- 消息数据要求较弱，只保证每个节点配置的每用户最近 N 条消息最终可见；当各节点 N 相同时，窗口内容也最终一致。
- 不引入强一致共识协议，不使用中心权威 owner。

## 核心设计

### 节点通信

- 每个节点维护到 peers 的 WebSocket 长连接。
- 节点间消息全部使用 Protobuf 编解码。
- 节点启动后通过握手交换节点 ID、协议版本、已确认游标和快照摘要版本。
- 同步链路同时支持实时广播和断线后的增量补发。

### 数据库选择

- 默认使用 SQLite 作为每节点本地数据库。
- SQLite 只负责本地事务、索引和查询。
- 分布式复制、冲突收敛、反熵和最终一致性全部由应用层负责。

### 用户数据一致性

- 用户写入采用多主模式，任意节点都能本地提交。
- 每次用户变更都会生成事件并写入本地 `event_log`。
- 本地提交成功后，事件通过 WebSocket 向 peers 广播。
- 用户冲突规则固定如下：
  - 字段级 LWW
  - 删除优先
  - 用户以 `(node_id, user_id)` 为唯一身份标识，用户名允许重复
- 删除通过墓碑传播，避免离线节点把旧数据重新同步回来。

### 消息数据一致性

- 消息采用广播扩散和幂等去重。
- 每条消息由 `(user_node_id, user_id, node_id, seq)` 四元组唯一定位，其中 `node_id` 表示消息生产节点。
- 节点最终只保留每个用户最近 N 条消息，默认 N = 500。
- 消息同步不追求全历史强一致，只保证每个节点本地窗口内消息最终收敛。

### 认证与管理约束

- 集群内部通信使用共享 `cluster_secret` 做 HMAC 鉴权。
- 外部登录态采用自包含签名 token，任意节点都能校验。
- 系统初始化时创建固定保底超级管理员：
  - 不允许删除
  - 不允许降权
  - 允许修改密码
- 集群运行后关闭公开注册，用户由管理员创建。

## 协议与数据模型

### 外部 API

建议保留对外 HTTP/JSON API，和集群内部同步协议解耦。

- `POST /users`
- `PATCH /nodes/{node_id}/users/{user_id}`
- `DELETE /nodes/{node_id}/users/{user_id}`
- `GET /nodes/{node_id}/users/{user_id}`
- `POST /nodes/{node_id}/users/{user_id}/messages`
- `GET /nodes/{node_id}/users/{user_id}/messages?limit=N`

### 内部 Protobuf 协议

定义 `cluster.proto`，至少包含以下消息：

- `Envelope`
- `Hello`
- `Ack`
- `EventBatch`
- `PullEvents`
- `SnapshotDigest`
- `SnapshotChunk`
- `UserEvent`
- `MessageEvent`

`Envelope` 统一携带：

- `node_id`
- `sequence`
- `sent_at_hlc`
- `oneof body`
- `hmac`

### 本地存储结构

每节点最少包含以下表：

- `users`
- `messages`
- `event_log`
- `peer_cursors`
- `applied_events`
- `user_conflicts`
- `tombstones`

其中：

- `event_log` 是复制事实来源。
- `peer_cursors` 记录每个 peer 的确认进度。
- `applied_events` 用于幂等去重。
- `tombstones` 用于删除传播和离线节点恢复。

### 用户记录字段

用户表建议包含：

- `user_id`
- `node_id`
- `username`
- `password_hash`
- `profile`
- `deleted_at`
- `version_username`
- `version_password_hash`
- `version_profile`
- `version_deleted`
- `origin_node_id`

### 消息记录字段

消息表建议包含：

- `user_id`
- `user_node_id`
- `node_id`
- `seq`
- `sender`
- `body`
- `metadata`
- `created_at_hlc`

## 分步实施

### 第 1 步：项目骨架与本地存储内核

- 建立基础模块：`api`、`store`、`cluster`、`proto`、`clock`、`auth`。
- 定义 SQLite schema。
- 实现全局唯一 `int64` ID 生成。
- 实现 HLC 逻辑时钟。
- 定义 `cluster.proto` 和基础消息类型。

完成标准：

- 单节点可以本地完成用户增改删、消息写入、事件落盘和查询。

### 第 2 步：外部业务 API

- 实现用户和消息的 HTTP/JSON API。
- 统一写入流程：先写业务数据，再写 `event_log`。
- 所有接口默认按“本地提交成功后异步复制”设计。

完成标准：

- 单节点 API 行为稳定，所有写入都可以通过事件日志重放。

### 第 3 步：WebSocket 集群连接与 protobuf 广播

- 实现节点配置：
  - `node_id`
  - `api.listen_addr`
  - `advertise_path`
  - `cluster_secret`
  - `peers`
- 实现 WebSocket 握手、心跳和重连。
- 实现 protobuf `Envelope`、`Hello`、`Ack`、`EventBatch`。
- 实现最小广播链路和事件应用。

完成标准：

- 两个节点之间创建用户和写消息能够自动同步。

### 第 4 步：事件日志复制与断线补发

- 实现按游标增量拉取。
- 实现基于 `peer_cursors` 的未确认事件补发。
- 引入 `applied_events` 去重，保证重复投递不会重复生效。

完成标准：

- 节点短时断连恢复后，事件最终补齐且无重复应用。

### 第 5 步：用户多主冲突收敛

- 为用户字段引入独立版本。
- 落地字段级 LWW 合并。
- 落地删除优先和墓碑传播。
- 允许不同用户使用相同用户名，用户身份只通过 `(node_id, user_id)` 判定。
- `user.created` 和 `user.updated` 都按 `(node_id, user_id)` 走 upsert + 字段级合并。

完成标准：

- 三节点并发修改同一 `(node_id, user_id)` 对应的用户后，所有节点最终收敛为同一状态，且删除不会被旧更新重新复活。

### 第 6 步：消息扩散与最近 N 条一致

- 定义消息事件和幂等规则。
- 实现消息广播、增量补拉和本地去重。
- 实现每用户最近 N 条消息裁剪。
- `Hello` 握手交换 `message_window_size`，不一致时记录告警并继续连接。

完成标准：

- 任意节点写消息后，其他节点最终都能看到自己本地窗口内的该用户最近 N 条消息；当各节点 N 相同，窗口内容一致。

### 第 7 步：反熵同步与快照修复

- 实现 `SnapshotDigest` 和 `SnapshotChunk`。
- 用户快照按分片摘要校验。
- 消息快照按每节点配置的每用户最近 N 条 hash 校验。
- 摘要不一致时拉取快照分片并重放到本地。

完成标准：

- 长时间离线节点重新加入后，最终可恢复到全网一致状态。

### 第 8 步：认证、保底管理员与安全控制

- 实现集群内部 HMAC 鉴权。
- 实现跨节点可校验的自包含登录 token。
- 实现固定保底超级管理员规则。
- 集群模式下关闭公开注册。

完成标准：

- 任意节点签发的登录 token 可以在其他节点使用，保底管理员规则稳定生效。

### 第 9 步：测试体系与故障演练

- 单元测试覆盖：
  - ID 生成
  - HLC 排序
  - protobuf 编解码
  - 用户字段级 LWW
  - 删除优先
  - 用户名冲突裁决
  - 消息去重与裁剪
- 集成测试覆盖：
  - 两节点同步
  - 三节点并发写
  - 断线恢复
  - 重复投递
  - 长连接重连
  - 快照修复

完成标准：

- 核心一致性场景都有自动化验证。

### 第 10 步：运维能力与上线准备

- 增加管理和观测接口：
  - peer 状态
  - 未确认事件数
  - 反熵进度
  - 冲突次数
  - 消息裁剪统计
- 输出核心指标和结构化日志。
- 补充部署建议、备份策略和节点恢复手册。

完成标准：

- 服务具备最小可运维能力，可进行小规模上线验证。

## 验收标准

- 用户数据在网络恢复后，各节点最终完全一致。
- 消息数据在网络恢复后，各节点对每个用户收敛到各自配置的最近 N 条消息集合；当各节点 N 相同，消息集合一致。
- 任意节点都能独立处理外部写请求。
- 集群内部重复投递和乱序投递不会破坏最终一致性。

## 默认假设

- 节点发现方式为静态 `peers`。
- 外部 API 使用 HTTP/JSON，内部同步固定使用 WebSocket + Protobuf。
- 用户冲突规则固定为：
  - 字段级 LWW
  - 删除优先
  - 用户名先创建优先
- 消息默认保留每用户最近 `500` 条。
- 不引入 Raft、Kafka 或中心数据库。
