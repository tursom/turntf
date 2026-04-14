# 分布式通知服务

一个全新的分布式通知服务项目，目标是支持：

- 任意节点可写
- 节点间通过 WebSocket 长连接互联
- 节点同步协议使用 Protobuf
- 用户数据最终完全一致
- 消息数据按每节点配置的每用户最近 N 条最终一致；当各节点 N 相同，窗口内容也一致

当前仓库已经完成实施计划的前 10 步：本地存储内核、单节点 HTTP/JSON API、客户端 WebSocket + Protobuf 接口、WebSocket + Protobuf 的最小集群同步链路、断线后的事件日志补发、基于 `(node_id, user_id)` 的用户多主冲突收敛、消息窗口扩散与按节点 N 收敛、反熵同步与快照修复、认证与安全控制、核心一致性测试，以及最小运维观测能力。

## 技术栈

- Go 1.26
- SQLite 作为每节点本地状态数据库；事件日志和消息投影可配置使用 SQLite 或 Pebble
- `github.com/gorilla/websocket`
- `github.com/mattn/go-sqlite3`
- `github.com/cockroachdb/pebble`
- `github.com/rs/zerolog`
- `google.golang.org/protobuf`
- 标准库

## 项目结构

```text
.
├── cmd/notifier/main.go            # 当前 CLI 入口
├── docs/distributed-system-roadmap.md # 分布式系统未来演进路线图
├── docs/distributed-test-framework-enhancement.md # 分布式测试框架增强计划
├── docs/distributed-test-framework-enhancement-results.md # 分布式测试框架增强执行结果
├── docs/replication-semantics.md   # 复制语义专题文档
├── docs/operations.md              # 运维与上线手册
├── internal/api                    # 应用服务层
├── internal/auth                   # token 与鉴权
├── internal/clock                  # HLC 和本地事件 ID
├── internal/cluster                # 集群配置与同步骨架
├── internal/proto                  # 集群与客户端 Protobuf 协议类型
├── internal/store                  # 本地存储内核，支持 SQLite/Pebble repository 后端
├── proto/cluster.proto             # 集群 Protobuf 协议定义
├── proto/client.proto              # 客户端 WebSocket Protobuf 协议定义
└── README.md
```

## 术语速览

- `node_id`：节点的稳定数字身份，首次启动时生成并保存在 SQLite `schema_meta` 中。它会完整写入 HLC 时间戳，同一集群内必须唯一。
- `HLC`：Hybrid Logical Clock，混合逻辑时钟。它把物理时间、逻辑计数和完整 `node_id` 结合起来，既尽量贴近真实时间，又能在并发写入、时钟轻微漂移时提供稳定排序。
- `最终一致`：写入不会要求所有节点同步成功后才返回，而是先在本地提交，再异步复制到其他节点；网络恢复后，数据应最终收敛到预期状态。
- `收敛`：不同节点经过复制、重放、冲突处理后，最终得到相同或规则允许范围内一致的结果。这个项目里，用户数据追求完全收敛，消息数据按每节点窗口大小收敛。
- `LWW`：Last Write Wins，最后写入获胜。这里是字段级 LWW，不是整行覆盖；例如用户名和资料字段会分别比较版本时间，较新的值覆盖较旧的值。
- `幂等`：同一事件重复投递多次，最终效果仍只生效一次。集群复制必须具备幂等性，否则断线重连或重放时会产生重复数据。
- `反熵`：anti-entropy，同步双方定期比对摘要并修补差异的机制。它用于补偿“实时广播 + 增量补拉”仍可能遗漏的边角情况。
- `快照修复`：当事件补拉不足以修复状态差异时，直接按分片传输当前数据快照并增量合并到本地状态的机制。
- `peer`：当前节点已知的其他节点配置项。每个 peer 至少包含一个可拨号的 `url`；远端 `node_id` 会在握手时自动读取并缓存。

## 当前状态

当前仓库不再承载旧的单机通知服务实现，默认目标就是新的分布式项目。

未来演进路线图见 [docs/distributed-system-roadmap.md](/root/dev/sys/turntf/docs/distributed-system-roadmap.md)。
分布式测试框架增强计划见 [docs/distributed-test-framework-enhancement.md](/root/dev/sys/turntf/docs/distributed-test-framework-enhancement.md)。
分布式测试框架增强执行结果见 [docs/distributed-test-framework-enhancement-results.md](/root/dev/sys/turntf/docs/distributed-test-framework-enhancement-results.md)。
复制语义规范见 [docs/replication-semantics.md](/root/dev/sys/turntf/docs/replication-semantics.md)。

当前同步实现已经收紧到以下边界：

- 集群模式必须同时提供 `cluster.advertise_path`、`cluster.secret`
- 节点间启用 `Envelope`、`Hello`、`Ack`、`EventBatch`、`PullEvents`，并为瞬时包启用 `RoutingUpdate`、`TransientPacket`
- `Envelope`：集群协议的统一外层消息，里面再装握手、确认、事件批次或补拉请求
- `Hello`：连接建立后的第一条握手消息，用于交换节点身份、协议版本、快照版本、广播路径、当前本地按 `origin_node_id` 聚合的 `origin_progress`、`message_window_size`，以及该连接的 routing 能力与链路观测摘要
- `TimeSyncRequest` / `TimeSyncResponse`：握手后的校时消息，用来估算节点时钟偏移并决定是否允许该 peer 进入可信复制状态
- `EventBatch`：事件批次消息，既用于在线广播，也用于按 `origin_node_id` 的增量补发；补拉响应会携带 `pull_request_id`
- `Ack`：确认消息，表示“我已经应用到了某个 `origin_node_id` 的哪条 `event_id`”；它会写入 `peer_ack_cursors`
- `RoutingUpdate`：节点间交换目的节点可达性、累计代价和残余代价的动态路由摘要，仅服务瞬时包
- `TransientPacket`：发往 `(node_id, 3)` 的非持久化数据包，可经动态路由多跳转发到目标节点在线用户
- `peer_ack_cursors` / `origin_cursors`：前者记录“某 peer 已确认到哪个 origin/event_id”，后者记录“本地对某 origin 已应用到哪个 event_id”，供重连后继续追平
- WebSocket 连接具备握手校验、心跳保活、自动重连和单连接方向裁决
- 同一 `peer_node_id` 允许存在多条并行连接；瞬时包路由会按连接观测的延迟和抖动择优选出口
- 集群模式下，节点首次成功校时前会拒绝本地写请求，避免未校准时钟污染字段级 LWW
- 节点重连后会按持久化游标自动补拉缺失事件，并通过 `applied_events` 做幂等去重
- `applied_events`：本地已应用复制事件的去重表，用 `(source_node_id, event_id)` 组合键避免广播和补拉重叠时重复执行
- 消息在本地写入和复制应用时都会按每用户最近 N 条裁剪，默认 `N=500`
- 若 peer 的 `message_window_size` 不一致，连接会继续建立并记录告警；各节点最终按自己的 N 收敛
- 写接口返回成功只代表“本地写入成功并已写入本地持久状态”，不代表集群已经全局提交或所有 peer 都已追平
- `Ack` 只代表“对端已经把该 `origin_node_id/event_id` 应用到自己的本地状态”，不代表其他节点也已应用，更不代表快照修复已经完成
- 用户、订阅、事件日志和持久消息属于最终一致持久状态；`TransientPacket` 属于瞬时包，不写事件日志，不参与 `Ack`、补拉、快照或重启恢复

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

## Docker 部署

项目根目录提供了 [Dockerfile](/root/dev/sys/turntf/Dockerfile) 和 [docker-compose.yml](/root/dev/sys/turntf/docker-compose.yml)。

镜像默认不会内置配置文件，启动时需要由用户自行挂载：

- `/app/config.toml`：运行配置文件，建议以只读方式挂载
- `/app/data`：SQLite、Pebble 和日志等运行数据目录

先准备配置文件：

```bash
cp ./config.example.toml ./config.toml
mkdir -p ./data
```

直接使用 `docker run`：

```bash
docker build -t turntf-notifier .
docker run --rm -p 8080:8080 \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  -v "$PWD/data:/app/data" \
  turntf-notifier
```

或者使用 Compose：

```bash
docker compose up --build
```

## GitHub Container Registry

仓库包含 [docker publish workflow](/root/dev/sys/turntf/.github/workflows/docker-publish.yml)，会在以下场景构建并推送镜像到 `ghcr.io/tursom/turntf`：

- push 到 `main`
- push `v*` 版本标签
- 手动触发 `workflow_dispatch`

默认标签规则：

- `main` 分支推送会生成 `main`、`sha-<commit>`，以及默认分支上的 `latest`
- 版本标签如 `v1.2.3` 会生成同名镜像标签

工作流使用仓库自带的 `GITHUB_TOKEN` 登录 GHCR，因此仓库需要允许 GitHub Actions 写入 packages。

## 配置文件示例

```bash
cp ./config.example.toml ./config.toml
```

`config.example.toml` 结构如下：

```toml
[api]
listen_addr = ":8080"

[store]
engine = "sqlite"
message_window_size = 500

[store.sqlite]
db_path = "./data/turntf.db"

[store.pebble]
path = "./data/turntf.pebble"

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
url = "ws://127.0.0.1:9081/internal/cluster/ws"
```

字段说明：

- 本地 `node_id`：节点首次启动时自动生成的稳定数字身份，保存到 SQLite `schema_meta` 的 `node_id` key；该 ID 会完整进入 HLC 时间戳
- `api.listen_addr`：外部 HTTP API 监听地址，同时承载内部 `GET /internal/cluster/ws`
- `store.engine`：事件日志和消息投影 repository 引擎，可选 `sqlite` 或 `pebble`，默认 `sqlite`
- `store.sqlite.db_path`：本地 SQLite 数据库路径，默认 `./data/turntf.db`。即使 `store.engine = "pebble"`，用户、订阅、游标、pending projection 和运维统计等状态仍保存在 SQLite
- `store.pebble.path`：Pebble 数据目录，默认 `./data/turntf.pebble`。仅在 `store.engine = "pebble"` 时用于事件日志和消息投影
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
- `[[cluster.peers]]`：静态 peer 列表，可重复出现多个条目；当前仅需配置可拨号地址，远端 `node_id` 会在首次握手后自动识别
- `cluster.peers.url`：当前节点主动拨号到远端时使用的完整 WebSocket URL，例如 `ws://127.0.0.1:9081/internal/cluster/ws`
- 当前协议不支持旧快照版本节点混跑；升级后需整集群使用新协议版本和快照版本

当前已提供：

- `POST /auth/login`
- `POST /users`
- `GET /nodes/{node_id}/users/{user_id}`
- `PATCH /nodes/{node_id}/users/{user_id}`
- `DELETE /nodes/{node_id}/users/{user_id}`
- `POST /nodes/{node_id}/users/{user_id}/messages`
- `GET /nodes/{node_id}/users/{user_id}/messages?limit=N`
- `GET /ws/client` 作为客户端 WebSocket Protobuf 长连接端点；连接后第一帧必须发送 `LoginRequest`。登录成功后，客户端可在同一连接上执行原先 HTTP JSON API 的全部已登录能力，包括消息收发、用户管理、订阅管理、历史查询和运维查询；接入流程见 [客户端全流程接入文档](/root/dev/sys/turntf/docs/client-flow.md)，协议见 [客户端 WebSocket 接口](/root/dev/sys/turntf/docs/client-websocket.md)
- `POST /nodes/{node_id}/users/{user_id}/subscriptions`
- `DELETE /nodes/{node_id}/users/{user_id}/subscriptions/{channel_node_id}/{channel_user_id}`
- `GET /nodes/{node_id}/users/{user_id}/subscriptions`
- `GET /cluster/nodes`
- `GET /events?after=0&limit=100`
- `GET /ops/status`
- `GET /metrics`
- `GET /healthz`
- `GET /internal/cluster/ws` 作为节点间 WebSocket 同步端点，仅在启用集群模式时挂载到 API 监听器

当前认证与授权边界：

- `GET /healthz` 和 `POST /auth/login` 公开
- `POST /users`、`PATCH /nodes/{node_id}/users/{user_id}`、`DELETE /nodes/{node_id}/users/{user_id}`、`GET /events`、`GET /ops/status`、`GET /metrics` 需要管理员或保底超级管理员
- `GET /cluster/nodes` 需要登录，但普通用户即可访问；只返回当前节点视角下已连接的集群节点列表
- `GET /nodes/{node_id}/users/{user_id}` 允许本人或管理员访问
- `GET /nodes/{node_id}/users/{user_id}/messages` 对可登录用户允许本人或管理员访问；对 `role=channel` 或 `role=broadcast` 地址仅管理员可直接查询原始消息
- `POST /nodes/{node_id}/users/{user_id}/messages` 需要登录；普通用户只能给自己或已订阅的 `role=channel` 地址写消息，管理员可给任意地址写消息，包括广播地址
- 当目标是 `(node_id, 3)` 时，请求进入“节点入口瞬时包”模式：`relay_target` 必填，任意已登录用户都可以发送；该数据包不会持久化，只会尽力转发给目标节点上当前在线的指定用户
- 订阅接口允许普通用户维护自己的 channel 订阅，管理员可维护任意用户订阅
- 登录请求固定使用 `node_id + user_id + password`
- `GET /ws/client` 登录后复用同一套权限边界：管理员可使用全部 WS RPC；普通用户只能访问本人、本人订阅和原有消息发送权限允许的资源
- HTTP JSON 消息接口的 `body` 是 base64 编码字节；客户端 WebSocket 和集群协议中的 `body` 是 protobuf `bytes`
- 用户身份由 `(node_id, user_id)` 二元组定位；`user_id = 1` 是每个节点的 root 候选，但当前已存储且 `node_id` 最小的那条才会保留 `super_admin + system_reserved` 身份，其他节点的 `1` 号用户会在收敛时降级为普通用户；唯一保底超级管理员不可删除、不可降权、不可改名，允许修改密码
- `user_id = 2` 是每个节点的系统广播地址，启动时会创建/修复为 `role=broadcast + system_reserved`，不可登录、不可删除、不可由外部 API 创建或修改为该角色；该保留标记会通过事件复制和快照修复在集群内保持一致
- `user_id = 3` 是每个节点的系统节点入口地址，启动时会创建/修复为 `role=node + system_reserved`，不可登录、不可删除、不可由外部 API 创建或修改为该角色；该保留标记也会通过事件复制和快照修复在集群内保持一致
- 每个节点的前 `1024` 个 `user_id` 都作为保留用户区间，普通用户和普通 channel 会从 `1025` 开始分配
- `role=channel` 是不可登录的组播地址，管理员可通过 `POST /users` 创建，其他用户订阅后可接收订阅时间之后发送到该 channel 的消息

当前集群同步行为：

本地成功与复制确认：

- 本地 `POST /users`、`PATCH /nodes/{node_id}/users/{user_id}`、`DELETE /nodes/{node_id}/users/{user_id}`、订阅变更、`POST /nodes/{node_id}/users/{user_id}/messages` 成功后，会先提交本地状态，再异步广播对应事件
- 对端节点成功应用事件后返回 `Ack`；该确认只说明“对端本地已应用”，不是全局提交确认
- 集群模式下，节点只有在首次成功校时后才接受本地写入；未校时时写接口会返回 `503`
- 两节点在线时，创建用户和写消息通常会很快同步，但允许存在短暂复制延迟

允许的暂态：

- 节点短时离线后重连，会按 `origin_cursors` 对比远端 `origin_progress`，逐个 `origin_node_id` 自动补拉未追平的事件
- 节点会在握手完成后和运行过程中进行反熵摘要比对；用户快照使用全量单分片 `users/full`，消息快照按生产节点 `messages/{node_id}` 分片
- 摘要不一致时，节点会请求对应快照分片并增量合并到本地；在快照修复完成前，不同节点可能短暂看到不同数据集合
- 拉取重放与实时广播重叠时，重复事件会被 `applied_events` 按 `(source_node_id, event_id)` 幂等吸收
- 当两个节点 `message_window_size` 不一致时，反熵只修复用户分片，避免不同窗口大小导致消息分片反复互拉
- 启用了 `cluster.max_clock_skew_ms` 时，时钟偏差超限或未来时间戳事件会导致该 peer 被拒绝/断开，直到校时恢复正常

最终收敛与非承诺边界：

- 用户复制按 `(node_id, user_id)` 做字段级 LWW 合并，用户名允许重复
- 删除通过 `tombstones` 传播，旧的创建/更新事件不会把已删除用户重新复活
- channel 订阅关系通过事件日志和快照复制；订阅后只合并订阅时间之后的 channel 消息，取消订阅后不再合并该 channel 消息
- `tombstones`：删除墓碑表，用来记录“这个对象已经被删过”，避免旧事件在延迟到达时把数据错误复活
- 消息复制按 `(user_node_id, user_id, node_id, seq)` 幂等去重，并在本地和复制应用时都裁剪到最近 N 条
- 广播消息发送到任意 `role=broadcast` 地址后只存一份；普通用户读取消息时会动态合并所有广播地址的消息，因此未来新用户也能看到仍在本地窗口内的广播消息
- 当集群所有节点使用相同的 `message_window_size` 时，同一用户的最近 N 条消息会收敛到相同结果
- 发往 `(node_id, 3)` 的瞬时包不会写入事件日志，不参与 `Ack`、补拉、快照和消息窗口；它们只在内存中经 `RoutingUpdate` 维护的动态路由表转发
- 瞬时包动态路由只服务 `(node_id, 3)` 地址，主代价按链路平滑 RTT 加抖动惩罚计算；hop 数仅用于 TTL 防环，不作为主选路指标
- 瞬时包支持 `best_effort` 和 `route_retry` 两种模式；`route_retry` 只使用内存 TTL 重试队列，节点重启后即丢失
- 任意节点签发的登录 token 都可以在其他节点使用，只要它们共享同一 `auth.token_secret`
- 所有集群 `Envelope` 在收发两端都使用 `cluster.secret` 做 HMAC 鉴权
- `/cluster/nodes` 提供已登录用户可访问的已连接集群节点列表，仅包含 `node_id`、`is_local`、`configured_url`
- `/ops/status` 提供管理员可访问的本节点运维快照，包括 peer 状态、未确认事件、反熵进度、冲突数和消息裁剪统计
- `/metrics` 提供无额外依赖的 Prometheus 文本指标，指标名使用 `notifier_*` 前缀
- 服务日志使用 zerolog；控制台输出易读文本，配置 `logging.file_path` 后同时写入 JSON 行日志文件

## 运维与上线

复制阶段判断、指标说明、部署建议、备份策略和节点恢复流程见 [docs/operations.md](/root/dev/sys/turntf/docs/operations.md)。
