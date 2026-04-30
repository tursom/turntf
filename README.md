# 分布式通知服务

支持多主可写、WebSocket 长连接互联、Protobuf 协议同步的分布式通知服务。

**设计目标：**

- 任意节点可写，本地提交后异步复制
- 用户数据最终完全一致；消息数据按每节点每用户最近 N 条（默认 500）最终一致
- 字段级 LWW 冲突收敛 + 反熵快照修复 + 断线事件日志补发
- 多传输层支持（WebSocket / ZeroMQ / libp2p）

当前已完成本地存储内核、HTTP/JSON API、客户端 WebSocket + Protobuf 接口、集群同步链路、事件日志补发、多主冲突收敛、消息窗口收敛、反熵同步与快照修复、认证安全控制，以及运维观测能力。

## 目录

- [快速开始](#快速开始)
- [术语速览](#术语速览)
- [功能与接口](#功能与接口)
- [认证与授权](#认证与授权)
- [集群同步](#集群同步)
- [Docker 部署](#docker-部署)
- [本地验证](#本地验证)
- [zsh 补全](#zsh-补全)
- [配置说明](#配置说明)
- [文档索引](#文档索引)

## 快速开始

```bash
cp ./config.example.toml ./config.toml   # 按需编辑
go run ./cmd/turntf serve
```

也可显式指定配置文件：

```bash
go run ./cmd/turntf serve -c ./config.toml
```

生成 bcrypt 密码哈希：

```bash
go run ./cmd/turntf hash --password 'secret'
printf 'secret' | go run ./cmd/turntf hash --stdin
```

## 术语速览

| 术语 | 说明 |
|---|---|
| `node_id` | 节点首次启动时生成的稳定数字身份，完整写入 HLC 时间戳，集群内必须唯一 |
| **HLC** | Hybrid Logical Clock，结合物理时间、逻辑计数和 `node_id`，提供贴近真实时间的稳定排序 |
| **最终一致** | 写入在本地提交后异步复制；网络恢复后数据最终收敛 |
| **收敛** | 不同节点经复制、重放、冲突处理后得到一致结果 |
| **LWW** | Last Write Wins，字段级比较版本时间，较新值覆盖较旧值 |
| **幂等** | 同一事件重复投递只生效一次，通过 `applied_events` 按 `(source_node_id, event_id)` 去重 |
| **反熵** | anti-entropy，定期比对摘要并修补差异，补偿实时广播 + 增量补拉的遗漏 |
| **快照修复** | 按分片传输当前数据快照并增量合并，修复事件补拉不足以解决的差异 |
| **peer** | 当前节点已知的其他节点，至少包含一个可拨号 `url`；远端 `node_id` 在握手时自动识别 |

## 功能与接口

### HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/auth/login` | 公开登录 |
| `POST` | `/users` | 管理员创建用户 |
| `GET` | `/nodes/{node_id}/users/{user_id}` | 查询用户（本人或管理员） |
| `PATCH` | `/nodes/{node_id}/users/{user_id}` | 管理员更新用户 |
| `DELETE` | `/nodes/{node_id}/users/{user_id}` | 管理员删除用户 |
| `POST` | `/nodes/{node_id}/users/{user_id}/messages` | 发送消息（需登录） |
| `GET` | `/nodes/{node_id}/users/{user_id}/messages?limit=N` | 查询消息 |
| `POST` | `/nodes/{node_id}/users/{user_id}/subscriptions` | 管理订阅 |
| `DELETE` | `/nodes/{node_id}/users/{user_id}/subscriptions/{channel_node_id}/{channel_user_id}` | 取消订阅 |
| `GET` | `/nodes/{node_id}/users/{user_id}/subscriptions` | 查询订阅 |
| `GET` | `/cluster/nodes` | 已连接集群节点列表（需登录） |
| `GET` | `/events?after=0&limit=100` | 事件列表（管理员） |
| `GET` | `/ops/status` | 节点运维快照（管理员） |
| `GET` | `/metrics` | Prometheus 文本指标（管理员） |
| `GET` | `/healthz` | 健康检查（公开） |

支持通过 `sync_mode: force_sync` 或 `no_sync` 控制单条消息的 Pebble 持久化方式（仅 `store.engine = "pebble"` 时生效）。

### 客户端长连接

- **WebSocket**：`/ws/client`，首帧发送 `LoginRequest`，登录后可复用全部已登录 HTTP API 能力
- **ZeroMQ**：启用后通过 `zmq+tcp://host:port` 连接，首包发送 `ZeroMQMuxHello{role=CLIENT}`，之后复用与 WebSocket 相同的 `ClientEnvelope/ServerEnvelope` 协议

详见 [客户端全流程接入文档](docs/client-flow.md) 和 [客户端 WebSocket 接口](docs/client-websocket.md)。

### 集群入口

- **WebSocket**：`/internal/cluster/ws`（固定路径，集群模式自动挂载）
- **ZeroMQ**：与客户端共用 `services.zeromq.bind_url`，首包发送 `ZeroMQMuxHello{role=CLUSTER}`
- **libp2p**：仅服务节点间通信，不新增业务客户端入口

### 瞬时包

发往 `(node_id, 3)` 的消息为瞬时包：不写事件日志，不参与 Ack / 补拉 / 快照，经 mesh 逐跳转发到目标节点在线用户。支持 `best_effort` 和 `route_retry` 两种模式。

## 认证与授权

- 登录使用 `node_id + user_id + password`，签发的 token 在所有共享 `auth.token_secret` 的节点间通用
- `user_id = 1`：保底超级管理员，不可删除、不可降权、不可改名，允许改密码
- `user_id = 2`：系统广播地址 (`role=broadcast`)，不可登录、不可删除
- `user_id = 3`：系统节点入口地址 (`role=node`)，不可登录、不可删除
- `user_id ≤ 1024`：保留区间，普通用户和 channel 从 1025 开始分配
- `role=channel`：不可登录的组播地址，订阅后接收订阅时间之后的 channel 消息
- 集群内部 `Envelope` 收发两端均使用 `cluster.secret` 做 HMAC 鉴权

**权限矩阵：**

| 操作 | 权限要求 |
|---|---|
| 健康检查、登录 | 公开 |
| 查询本人信息、本人消息、管理本人订阅 | 登录用户 |
| 发送消息到可登录用户或已授权 channel | 登录用户 |
| 发送瞬时包到 `(node_id, 3)` | 登录用户 |
| 查询集群节点 | 登录用户 |
| 创建用户、修改/删除他人信息、事件查询、运维/监控 | 管理员 |

## 集群同步

- 本地写入成功后异步广播对应事件；对端应用后返回 `Ack`（仅表示"对端已应用"，非全局提交）
- 未校时时拒绝本地写入（返回 503）；首次校时成功后才放开
- 重连后按 `origin_cursors` 对比远端 `origin_progress`，自动补拉未追平事件
- 历史事件被裁剪时，优先请求快照分片再从新 cursor 继续补拉
- 重复事件被 `applied_events` 幂等吸收
- 反熵按用户全量分片 `users/full` 和消息分片 `messages/{node_id}` 对比摘要
- `message_window_size` 不一致的节点间只修复用户分片，避免消息分片反复互拉
- 消息在本地写入和复制应用时均裁剪到最近 N 条（默认 N=500）

## Docker 部署

```bash
# 准备配置和数据目录
cp ./config.example.toml ./config.toml
mkdir -p ./data

# 构建并启动
docker build -t turntf .
docker run --rm -p 8080:8080 \
  -v "$PWD/config.toml:/app/config.toml:ro" \
  -v "$PWD/data:/app/data" \
  turntf

# 或使用 Compose
docker compose up -d --build
```

预构建镜像：`ghcr.io/tursom/turntf:latest`

镜像标签规则：`main` 分支推送生成 `main`、`sha-<commit>` 和 `latest`；版本标签 `v1.2.3` 生成同名标签。

## 本地验证

```bash
go test ./... -count=1
./scripts/smoke.sh
```

`smoke.sh` 会编译临时二进制，启动单节点，验证 `GET /healthz`、登录、运维状态、指标、创建用户、自收消息和事件列表等最小可用路径。

## zsh 补全

```bash
source <(go run ./cmd/turntf completion zsh)
```

持久安装：

```bash
mkdir -p ~/.zsh/completions
turntf completion zsh > ~/.zsh/completions/_turntf
```

然后确保 `~/.zshrc` 中启用：

```zsh
fpath=(~/.zsh/completions $fpath)
autoload -Uz compinit
compinit
```

同时支持 `bash`、`fish`、`powershell` 补全。

## 配置说明

```bash
cp ./config.example.toml ./config.toml   # 完整配置模板
```

主要配置段：

| 配置段 | 说明 |
|---|---|
| `services.http` | HTTP API 监听地址，默认 `:8080`；集群入口 `/internal/cluster/ws` 固定挂载在此监听器 |
| `services.zeromq` | ZeroMQ 传输，需 `enabled = true`，支持 `none` 和 `curve` 安全模式 |
| `services.libp2p` | libp2p 集群传输，支持 DHT、mDNS、relay、hole punching、Gossipsub |
| `store` | 存储引擎 (`sqlite` / `pebble`)、消息窗口大小（默认 500）、事件日志裁剪策略 |
| `auth` | token 签名密钥、有效期、保底超级管理员 bcrypt 密码哈希 |
| `cluster` | 集群密钥、时钟偏差容忍、静态 peer 列表；省略则单节点运行 |
| `logging` | 日志级别 (`debug`/`info`/`warn`/`error`) 和可选 JSON 行日志文件路径 |

完整字段说明见 `config.example.toml` 中的注释。

## 文档索引

- [分布式系统演进路线图](docs/distributed-system-roadmap.md)
- [复制语义](docs/replication-semantics.md)
- [性能基线](docs/performance-baseline.md)
- [吞吐优化](docs/throughput-optimization-plan.md)
- [节点自动发现](docs/peer-discovery.md)
- [时钟保护](docs/clock-protection.md)
- [客户端接入流程](docs/client-flow.md)
- [客户端 WebSocket 接口](docs/client-websocket.md)
- [运维手册](docs/operations.md)
- [libp2p 接入计划](docs/libp2p-plan.md)
- [ZeroMQ 传输计划](docs/zeromq-transport-plan.md)
- [Mesh 路由邻接表计划](docs/mesh-routing-adjacency-list-plan.md)
