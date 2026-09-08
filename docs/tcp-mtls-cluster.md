# 原生 TCP+mTLS 集群传输

## 范围与默认行为

这是服务端集群专用传输，不新增客户端入口，不要求修改 SDK。默认 `services.tcp_mtls.enabled = false`：不监听、不读取证书、不注册 TCP mesh 适配器，静态 TCP peer 会导致配置校验失败；收到 TCP membership 广告可以保留候选信息，但不会生成拨号种子或发起网络连接。节点白名单同样在动态发现与实际拨号入口检查。

部署目标如下，名称不硬编码进程序，使用各节点真实持久化 `node_id` 配置：

| 节点 | 原生 TCP+mTLS | WSS | TCP 允许节点 |
| --- | --- | --- | --- |
| kiwi | 启用，主用 | 热备用 | cc、kr 的 node_id |
| cc | 启用，主用 | 热备用 | kiwi、kr 的 node_id |
| kr | 启用，主用 | 热备用 | kiwi、cc 的 node_id |
| home | 禁用 | 唯一集群传输 | 无 |
| cn | 禁用 | 唯一集群传输 | 无 |

home/cn 同时保持 ZeroMQ、libp2p 关闭，静态 peers 只使用 `wss://`。即使 kiwi/cc/kr 将 TCP 地址传播到 home/cn，也不会触发 TCP 拨号。WSS 的 HTTPS 证书验证沿用现有 WebSocket 实现，不会因原生 TCP 启用而降级；生产备用地址必须使用 WSS，而不是明文 WS。

## 配置

以下 **4096、8192、12288 和域名仅为示例**，分别代表 kiwi、cc、kr。必须替换为已有数据目录对应的真实 node_id；不要为匹配证书删除数据库、重建身份或改 HLC 节点 ID。

kiwi 配置片段，合并到已有 TOML，不重复定义已有的 `[cluster]`：

```toml
[services.tcp_mtls]
enabled = true
listen_addr = "0.0.0.0:9443"
advertised_endpoints = ["tcp+tls://kiwi.example.net:9443/4096"]
ca_file = "/etc/turntf/cluster-tls/ca.pem"
cert_file = "/etc/turntf/cluster-tls/kiwi-chain.pem"
key_file = "/etc/turntf/cluster-tls/kiwi.key"
allowed_node_ids = [8192, 12288]
handshake_timeout_ms = 5000
max_frame_bytes = 8388608

[cluster]
secret = "替换为现有集群密钥"

[[cluster.peers]]
url = "tcp+tls://cc.example.net:9443/8192"
[[cluster.peers]]
url = "wss://cc.example.net/internal/cluster/ws"
[[cluster.peers]]
url = "tcp+tls://kr.example.net:9443/12288"
[[cluster.peers]]
url = "wss://kr.example.net/internal/cluster/ws"
```

cc/kr 使用各自监听广告、证书、真实 node_id 及其他两个节点的白名单，并保留对其他节点的 WSS 地址。home/cn 的配置片段：

```toml
[services.tcp_mtls]
enabled = false

[[cluster.peers]]
url = "wss://kiwi.example.net/internal/cluster/ws"
[[cluster.peers]]
url = "wss://cc.example.net/internal/cluster/ws"
[[cluster.peers]]
url = "wss://kr.example.net/internal/cluster/ws"
```

字段约束：

- `listen_addr` 留空为仅出站；不允许同时填写广告地址。绑定 `:0` 适合测试，但不会自动生成可供生产发现的端点。
- `advertised_endpoints` 必须使用 `tcp+tls://host:port/nodeID`，包括明确的非零端口与规范十进制正整数节点 ID；支持 DNS、IPv4、方括号 IPv6。禁止用户信息、查询、fragment、通配主机以及编码身份路径。
- 同一节点可以配置多个不同 TCP 地址和 WSS 地址。重复的相同规范化 URL 仍被拒绝。
- `allowed_node_ids` 启用时必填。出站 URL 身份必须位于白名单，入站证书身份也必须位于白名单；禁止自身连接。
- TLS 握手及拨号总超时默认 5000ms，范围 1..60000ms。mesh NodeHello 沿用独立的默认 5 秒读写超时。
- 帧上限默认 8MiB，与现有 WebSocket 接收上限一致；允许 1..64MiB，0 使用默认值。提高到超过 8MiB 会失去大帧的 WSS 回退兼容性，不建议生产提高。
- 证书与 CA 路径相对于进程工作目录解析，建议使用绝对路径；禁用时不加载，启用时加载错误会阻止启动。

## 证书与身份

严格使用 TLS 1.3 和双向证书认证。没有明文 TCP、跳过验证、系统 CA 回退或 TCP 到 WS 的安全降级开关。

每个节点的叶子证书必须满足：

1. 由 `ca_file` 中的集群 CA 签发，链条完整且在有效期内；证书文件按叶子、中间 CA 顺序存放。
2. 允许 `serverAuth` 和 `clientAuth` 两种 EKU；启动时分别验证本地证书的两种用途。
3. URI SAN 包含唯一的 `urn:turntf:node:<真实node_id>`。不使用 CN 作为节点身份，不允许重复或歧义身份。
4. DNS SAN/IP SAN 覆盖所有出站访问本节点的主机名/IP。使用 IP 地址连接时必须有对应 IP SAN，仅 DNS SAN 不够。
5. 私钥匹配证书，权限建议 `0600`，只允许服务账号读取；CA 私钥不要放在服务节点。

示例 CSR 扩展（签发端必须保留这些扩展）：

```ini
[cluster_node]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:kiwi.example.net,URI:urn:turntf:node:4096
```

验证链条后，出站还会将证书节点 ID 与 URL 路径节点 ID 比较；入站检查白名单。TLS 成功后，mesh 再验证原有 `cluster.secret` HMAC、协议版本、能力，并将 NodeHello 的 node_id 与证书身份比较。即使持有共享 HMAC，也不能用节点 A 的证书冒充节点 B。

首次配置时，先从已有 `/ops/status` 读取稳定 node_id，再签发证书。证书加载发生在存储初始化、真实 node_id 确定之后，不使用 TOML 校验阶段的临时 ID。

当前不支持热重载、CRL/OCSP 或证书吊销列表。轮换采用短有效期证书，CA 轮换先分发包含新旧 CA 的信任集合，再逐节点替换证书并受控重启，最后移除旧 CA。不要在未同步信任的情况下直接替换 CA。本次实现不执行任何部署、重启或证书签发操作。

## 优先、回退与恢复

复用现有 mesh Runtime、成员发现、HMAC、路由及重试框架。新增 protobuf 枚举 `TRANSPORT_KIND_TCP_MTLS = 4`，不复用 ZeroMQ 的 `zmq+tcp` scheme，也不改变客户端协议。

- 各地址独立拨号和重试（默认重试间隔 1 秒）。WSS 与 TCP 同时保持可用，不采用“连接 WSS 后停止探测 TCP”。
- 对本节点发出的流量，目标节点存在健康 TCP 直连时，路由优先该 TCP 直连，不因 WSS RTT 更低或其他中继路径更便宜而抢占；五种流量类别都适用。非直连目标继续使用原有路径规划，避免把 WSS-only 目标的复制流引入无法跨传输桥接的路径。
- TCP 连接断开并被 mesh 检测后，拓扑更新使 WSS 立即重新参与路由。TCP 重试成功并完成证书与 Hello 验证后，拓扑更新恢复 TCP 优先。
- 保留现有转发/桥接策略。来自 WSS 的复制流仍不能跨传输桥接；不会因为中继节点有 TCP 就禁止原本合法的 WSS 同传输中继路径。
- 多地址按节点、传输、端点索引，同一个 TCP 地址退出不发布覆盖另一条健康 TCP 邻接的断连墓碑；只有全部直接邻接丢失才触发节点失联怀疑。
- 发现调度允许已通过 WSS 连接的节点继续补建 TCP。TCP 广告中的 node_id 必须与 URL 身份一致。每轮最多选择 8 个动态 URL：优先未覆盖的节点/传输组合，再平衡传输和节点覆盖，同等覆盖度下 TCP、WebSocket、其他传输依次优先。静态地址的已知节点/传输计入覆盖但不占动态名额；额外地址在后，已有动态 URL 仅在同等覆盖度下优先，不能永久挡住新的主用或备用传输。超过预算时不保证所有节点都有双传输，关键主备应配置为静态 peers。

回退发生在故障检测和路由重算之后，不是单次 `Send` 失败时盲目重发。正在传送的帧可能失败；复制、查询、瞬时消息各自沿用既有重试和投递语义，不新增 exactly-once 保证。

TCP 建链后的活性检测复用 mesh `TimeSyncRequest/Response`，不是 TCP keepalive，也不是每次收到任意帧就刷新空闲期限。默认每 2 秒测量，每条邻接最多一个待应答请求；必须收到同一连接上匹配请求 ID 的应答。请求发出后 3 个测量周期（默认 6 秒）仍未应答，在后续测量 tick 关闭 TCP，唤醒阻塞的 `Receive`，移除邻接并发布拓扑。计时使用本地单调时钟，不受 HLC 或墙钟校正影响。只读丢弃的对端即使让本地写入一直成功，也会被淘汰；其他业务帧不能续期。

从故障发生到发出下一次探测最多另等一个周期，超时检查有最多一个周期的 tick 粒度；阻塞发送另受 mesh 默认 5 秒发送期限约束，实际切换还涉及调度与拓扑处理，因此不是固定 6 秒切换 SLA。TCP 发送探测失败也关闭连接。其他传输只淘汰过期测量并继续探测，不新增断线判定；WSS 沿用既有保活。连接取消或关闭时立即取消并等待其测量循环退出，清空待应答请求，不把该循环留到下次重试。

## 帧与生命周期

帧格式为 4 字节大端无符号长度，再跟原有已签名的 mesh protobuf 信封。长度 0 或超过配置上限在分配载荷前拒绝；部分帧错误、读取/发送取消会关闭整个流，禁止继续解析错位字节。发送和接收各自串行化，可以全双工并行。

入站最多并发 64 个 TLS 握手，接受队列复用 mesh 的 128 容量。握手占用受超时和根 context 约束；停止时关闭监听器、未完成握手、已建连接并等待入站协程退出。Runtime 启动失败同样取消种子并释放适配器，避免错误证书导致关闭挂起。

## 可观测性

沿用鉴权后的 `/ops/status` 和 `/metrics`：

- `mesh.transport_capabilities` 增加 `tcp_mtls`，展示入站、出站和广告地址。
- `mesh.routes[].outbound_transport` 显示当前每类流量的实际选择，可观察 `tcp_mtls -> websocket -> tcp_mtls`。
- `mesh.tcp_mtls` 包含 enabled、dial_attempts、handshake_rejected、established_total、active_adjacencies。
- Prometheus：`notifier_tcp_mtls_enabled`、`notifier_tcp_mtls_active_adjacencies`、`notifier_tcp_mtls_dial_attempts_total`、`notifier_tcp_mtls_handshake_rejected_total`、`notifier_tcp_mtls_established_total`。

`established_total` 统计 TLS 建链，不代表 Hello 已通过；`active_adjacencies` 才是完成 mesh 身份验证的连接数。证书拒绝计数不包含 TCP connection-refused，后者可通过持续增长的 dial_attempts 与无邻接判断。计数器在进程重启后归零，不输出私钥或证书内容。

## 验证

```bash
go test ./... -count=1
go test -race ./internal/cluster ./internal/mesh -run 'TCPMTLS' -count=3
GOCACHE="$(go env GOCACHE)" ./scripts/smoke.sh
```

专项测试使用临时 CA、真实回环 TCP、严格验证的 httptest WSS，覆盖双向证书拒绝、错误主机/节点身份、Hello 冒充、真实帧传输、帧限制、取消、握手超时、失败启动清理、同节点双地址、TCP 停止后的 WSS 传递与 TCP 恢复切回、Manager 发现升级和 disabled 节点收到广告无拨号，以及 HTTP 状态和指标。测试不会访问 kiwi/cc/kr/home/cn。

专项测试还覆盖真实 TCP 对端握手后持续读取但不应答、健康 WSS 实际投递、超时回退与恢复、关闭后的连接与协程清理、待应答 Ping 有界，以及 WSS/TCP 多地址分别占满 8 个动态名额时的跨传输与新节点调度、disabled 和白名单禁拨。

混合版本上线应先升级服务端代码且保持 TCP 关闭，再在三个目标节点启用；旧服务端不能建立原生 TCP 邻接，保留 WSS 做兼容路径。仓库 smoke 验证现有登录、API 和基础启动，不能替代真实跨机防火墙、证书分发及故障注入验收。
