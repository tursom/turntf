# 分布式通知服务

一个全新的分布式通知服务项目，目标是支持：

- 任意节点可写
- 节点间通过 WebSocket 长连接互联
- 节点同步协议使用 Protobuf
- 用户数据最终完全一致
- 消息数据按每节点配置的每用户最近 N 条最终一致；当各节点 N 相同，窗口内容也一致

当前仓库已经完成实施计划的前 10 步：本地存储内核、单节点 HTTP/JSON API、WebSocket + Protobuf 的最小集群同步链路、断线后的事件日志补发、基于 `user_id` 的用户多主冲突收敛、消息窗口扩散与按节点 N 收敛、反熵同步与快照修复、认证与安全控制、核心一致性测试，以及最小运维观测能力。

## 技术栈

- Go 1.26
- SQLite 作为每节点本地数据库
- `github.com/gorilla/websocket`
- `github.com/mattn/go-sqlite3`
- `github.com/rs/zerolog`
- `google.golang.org/protobuf`
- 标准库

## 项目结构

```text
.
├── cmd/notifier/main.go            # 当前 CLI 入口
├── docs/distributed-system-plan.md # 分布式实施计划
├── docs/operations.md              # 运维与上线手册
├── internal/api                    # 应用服务层
├── internal/auth                   # token 与鉴权
├── internal/clock                  # HLC 和全局 ID
├── internal/cluster                # 集群配置与同步骨架
├── internal/proto                  # 集群协议类型
├── internal/store                  # SQLite 本地存储内核
├── proto/cluster.proto             # Protobuf 协议定义
└── README.md
```

## 术语速览

- `slot`：节点的数值编号，用于参与本节点生成全局唯一 ID 和 HLC 时间戳。它不是对外展示名称，而是写进 ID 的机器编号位；同一集群内必须唯一。
- `HLC`：Hybrid Logical Clock，混合逻辑时钟。它把物理时间和逻辑计数结合起来，既尽量贴近真实时间，又能在并发写入、时钟轻微漂移时提供稳定排序。
- `最终一致`：写入不会要求所有节点同步成功后才返回，而是先在本地提交，再异步复制到其他节点；网络恢复后，数据应最终收敛到预期状态。
- `收敛`：不同节点经过复制、重放、冲突处理后，最终得到相同或规则允许范围内一致的结果。这个项目里，用户数据追求完全收敛，消息数据按每节点窗口大小收敛。
- `LWW`：Last Write Wins，最后写入获胜。这里是字段级 LWW，不是整行覆盖；例如用户名和资料字段会分别比较版本时间，较新的值覆盖较旧的值。
- `幂等`：同一事件重复投递多次，最终效果仍只生效一次。集群复制必须具备幂等性，否则断线重连或重放时会产生重复数据。
- `反熵`：anti-entropy，同步双方定期比对摘要并修补差异的机制。它用于补偿“实时广播 + 增量补拉”仍可能遗漏的边角情况。
- `快照修复`：当事件补拉不足以修复状态差异时，直接按分片传输当前数据快照并增量合并到本地状态的机制。
- `peer`：当前节点已知的其他节点配置项。每个 peer 至少包含一个数字身份 `node_id` 和一个可拨号的 `url`。

## 当前状态

当前仓库不再承载旧的单机通知服务实现，默认目标就是新的分布式项目。

实施计划见 [docs/distributed-system-plan.md](/root/dev/sys/turntf/docs/distributed-system-plan.md)。

当前同步实现已经收紧到以下边界：

- 集群模式必须同时提供 `cluster.advertise_path`、`cluster.secret`
- 节点间启用 `Envelope`、`Hello`、`Ack`、`EventBatch`、`PullEvents`
- `Envelope`：集群协议的统一外层消息，里面再装握手、确认、事件批次或补拉请求
- `Hello`：连接建立后的第一条握手消息，用于交换节点身份、协议版本、快照版本、广播路径、当前本地 `last_sequence` 和 `message_window_size`
- `TimeSyncRequest` / `TimeSyncResponse`：握手后的校时消息，用来估算节点时钟偏移并决定是否允许该 peer 进入可信复制状态
- `EventBatch`：事件批次消息，既用于在线广播，也用于断线后的增量补发
- `Ack`：确认消息，表示“我已经应用到了你的哪条 sequence”；它会和 `peer_cursors` 一起记录每个 peer 的确认进度与已应用进度
- `peer_cursors`：本地持久化的“对每个 peer 的复制进度表”，至少记录 acked/applied sequence，供重连后继续追平
- WebSocket 连接具备握手校验、心跳保活、自动重连和单连接方向裁决
- 集群模式下，节点首次成功校时前会拒绝本地写请求，避免未校准时钟污染字段级 LWW
- 节点重连后会按持久化游标自动补拉缺失事件，并通过 `applied_events` 做幂等去重
- `applied_events`：本地已应用复制事件的去重表，用来避免同一事件在广播和补拉重叠时被重复执行
- 消息在本地写入和复制应用时都会按每用户最近 N 条裁剪，默认 `N=500`
- 若 peer 的 `message_window_size` 不一致，连接会继续建立并记录告警；各节点最终按自己的 N 收敛

## 启动 API 服务

```bash
cp ./config.example.toml ./config.toml
go run ./cmd/notifier serve
```

也可以显式指定配置文件路径：

```bash
go run ./cmd/notifier serve -config ./config.toml
```

当前 `serve` 只接受一个运行时参数：

- `-config`：TOML 配置文件路径；缺省时读取 `./config.toml`

也可以直接生成 bcrypt 密码哈希：

```bash
go run ./cmd/notifier hash -password 'secret'
printf 'secret' | go run ./cmd/notifier hash -stdin
```

## 配置文件示例

```bash
cp ./config.example.toml ./config.toml
```

`config.example.toml` 结构如下：

```toml
[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"
message_window_size = 500

[auth]
token_secret = "replace-me"
token_ttl_minutes = 1440

[auth.bootstrap_admin]
username = "root"
password_hash = "$2a$10$1gGoT/pdOu8vX1W28skBPOB7ICjISmVgt9lMyZf9c6re6cMHU6mAa"

[logging]
level = "info"
file_path = "./data/notifier.log"

[cluster]
advertise_path = "/internal/cluster/ws"
secret = "secret"
max_clock_skew_ms = 1000

[[cluster.peers]]
node_id = 8192
url = "ws://127.0.0.1:9081/internal/cluster/ws"
```

字段说明：

- 本地 `node_id`：节点首次启动时自动生成的稳定数字身份，保存到 SQLite `schema_meta` 的 `node_id` key；该 ID 内嵌的 slot 用于生成全局唯一 ID 和 HLC
- `api.listen_addr`：外部 HTTP API 监听地址，同时承载内部 `GET /internal/cluster/ws`
- `store.db_path`：本地 SQLite 数据库路径。每个节点各自维护本地库，复制链路负责把状态传播到别的节点
- `store.message_window_size`：每节点每用户本地保留的消息窗口，默认 `500`。超过窗口的旧消息会在本地写入或复制应用时被裁剪
- `auth.token_secret`：外部登录 token 的共享签名密钥；所有节点必须一致，任意节点签发的 token 才能被其他节点校验
- `auth.token_ttl_minutes`：登录 token 的有效期，默认 `1440`
- `auth.bootstrap_admin.username`：固定保底超级管理员用户名；启动时会修复到该值
- `auth.bootstrap_admin.password_hash`：保底超级管理员的初始 bcrypt 密码哈希；仅首次创建时使用，后续不会在启动时强制覆盖。上面的示例值对应明文密码 `root`
- `logging.level`：服务日志级别，默认 `info`，可选 `debug`、`info`、`warn`、`error`
- `logging.file_path`：可选的 JSON 行日志文件路径；省略或留空时只输出控制台日志
- `cluster` 整段可省略；省略时按单节点模式运行
- 配置了 `cluster` 后，需要同时提供 `cluster.advertise_path`、`cluster.secret`
- `cluster.advertise_path`：节点对外暴露的集群 WebSocket 路径，必须以 `/` 开头；peer 的 `url` 需要带上这个路径
- `cluster.secret`：集群内部 `Envelope` 的共享 HMAC 密钥；节点间握手、广播、补拉和反熵消息都会验签
- `cluster.max_clock_skew_ms`：允许的最大时钟偏差，默认 `1000` 毫秒；正数启用超限拒绝，`0` 表示关闭“超限拒绝”，但节点仍会在首次成功校时前拒绝本地写入
- `[[cluster.peers]]`：静态 peer 列表，可重复出现多个条目，但 `node_id` 不能和本节点相同
- `cluster.peers.node_id`：远端节点的稳定数字身份，可从远端日志、`/ops/status` 或 `/metrics` 中查看
- `cluster.peers.url`：当前节点主动拨号到远端时使用的完整 WebSocket URL，例如 `ws://127.0.0.1:9081/internal/cluster/ws`
- 当前协议不支持旧快照版本节点混跑；升级后需整集群使用新协议版本和快照版本

当前已提供：

- `POST /auth/login`
- `POST /users`
- `GET /users/{id}`
- `PATCH /users/{id}`
- `DELETE /users/{id}`
- `POST /messages`
- `GET /users/{id}/messages?limit=N`
- `GET /events?after=0&limit=100`
- `GET /ops/status`
- `GET /metrics`
- `GET /healthz`
- `GET /internal/cluster/ws` 作为节点间 WebSocket 同步端点，仅在启用集群模式时挂载到 API 监听器

当前认证与授权边界：

- `GET /healthz` 和 `POST /auth/login` 公开
- `POST /users`、`PATCH /users/{id}`、`DELETE /users/{id}`、`GET /events`、`GET /ops/status`、`GET /metrics` 需要管理员或保底超级管理员
- `GET /users/{id}`、`GET /users/{id}/messages` 允许本人或管理员访问
- `POST /messages` 需要登录；普通用户只能给自己的 `user_id` 写消息，管理员可给任意用户写消息
- 登录请求固定使用 `user_id + password`
- 保底超级管理员固定为 `user_id = 1`，不可删除、不可降权、不可改名，允许修改密码

当前集群同步行为：

- 本地 `POST /users`、`PATCH /users/{id}`、`DELETE /users/{id}`、`POST /messages` 成功后，会异步广播对应事件
- 对端节点成功应用事件后返回 `Ack`
- 两节点在线时，创建用户和写消息可以自动同步
- 集群模式下，节点只有在首次成功校时后才接受本地写入；未校时时写接口会返回 `503`
- 节点短时离线后重连，会基于 `peer_cursors` 自动补拉未追平的事件
- 节点会在握手完成后和运行过程中进行反熵摘要比对；用户快照使用全量单分片 `users/full`，消息快照按生产节点 `messages/{node_id}` 分片
- 摘要不一致时，节点会请求对应快照分片并增量合并到本地；用户仍按字段级 LWW 和墓碑删除优先收敛，消息仍按本地 `message_window_size` 裁剪
- 当两个节点 `message_window_size` 不一致时，反熵只修复用户分片，避免不同窗口大小导致消息分片反复互拉
- 拉取重放与实时广播重叠时，重复事件会被 `applied_events` 幂等吸收
- 用户复制按 `user_id` 做字段级 LWW 合并，用户名允许重复
- 删除通过 `tombstones` 传播，旧的创建/更新事件不会把已删除用户重新复活
- 启用了 `cluster.max_clock_skew_ms` 时，时钟偏差超限或未来时间戳事件会导致该 peer 被拒绝/断开，避免污染 LWW
- `tombstones`：删除墓碑表，用来记录“这个对象已经被删过”，避免旧事件在延迟到达时把数据错误复活
- 消息复制按 `(user_id, node_id, seq)` 幂等去重，并在本地和复制应用时都裁剪到最近 N 条
- 当集群所有节点使用相同的 `message_window_size` 时，同一用户的最近 N 条消息会收敛到相同结果
- 任意节点签发的登录 token 都可以在其他节点使用，只要它们共享同一 `auth.token_secret`
- 所有集群 `Envelope` 在收发两端都使用 `cluster.secret` 做 HMAC 鉴权
- `/ops/status` 提供管理员可访问的本节点运维快照，包括 peer 状态、未确认事件、反熵进度、冲突数和消息裁剪统计
- `/metrics` 提供无额外依赖的 Prometheus 文本指标，指标名使用 `notifier_*` 前缀
- 服务日志使用 zerolog；控制台输出易读文本，配置 `logging.file_path` 后同时写入 JSON 行日志文件

## 运维与上线

运维接口、指标说明、部署建议、备份策略和节点恢复流程见 [docs/operations.md](/root/dev/sys/turntf/docs/operations.md)。
