# 异构转发与路由规划

本文档文件名沿用早期 `plan` 命名，但正文以当前 `turntf/` 实现为准。它不再把 mesh、mixed transport 和 forwarding 主链路描述成“未来方案”，而是记录当前基线、已落地边界，以及后续仍值得继续收口的演进方向。

## 目标与非目标

当前架构目标已经明确：

- 节点间控制面和跨节点数据面统一走 mesh runtime。
- 路由图以 `(node_id, transport)` 为状态，支持 `websocket`、`libp2p`、`zeromq` 三类 transport。
- forwarding 是节点级显式能力，默认开启；bridge 也是显式能力，默认开启。
- 路由决策同时考虑链路状态、`node_fee_weight`、每类流量的 disposition，以及是否允许跨 transport bridge。
- 数据面采用逐跳重算，不使用源路由。

本文档仍然不覆盖以下内容：

- 旧协议与新协议的长期双栈混部方案。
- `relay-only` 独立节点角色设计。
- 动态计费系统或实时账单反馈。
- 默认开启“复制流 / 快照流跨 transport bridge”这一类高风险策略。

## 当前仓库状态

### mesh 已是主链路

当前生产路径里，真正承担节点间控制面和多跳数据面语义的是 mesh runtime，而不是旧的 `RoutingUpdate` 模型：

- `Manager.Start()` 会自动启动 mesh runtime，并把它作为主控制面。
- WebSocket、libp2p、ZeroMQ 的入站连接都会优先注入 mesh runtime。
- `NodeHello`、topology flooding、查询、瞬时包、复制批次、补拉、`Ack`、快照摘要/分片、membership、presence、connectivity rumor 都已经有 mesh 路由入口。
- 旧 `cluster.proto` 里仍保留部分历史字段和消息定义，但当前主链路不再依赖旧 `RoutingUpdate` 或旧的 `supports_membership` 协商来驱动拓扑。

因此，这份文档讨论的重点不再是“如何把 mesh 接进来”，而是“当前基线已经是什么、哪些边界是故意保守、后续还要往哪里收口”。

### 协议与 runtime 基线已落地

`proto/mesh.proto` 已经不是草稿状态，而是当前 mesh 主链路的正式协议基础：

- `TransportKind` 已包含 `LIBP2P`、`ZEROMQ`、`WEBSOCKET`。
- `ClusterEnvelope` 已包含 `NodeHello`、`TimeSyncRequest/Response`、`TopologyUpdate`、query、`ForwardedPacket`、复制批次、补拉、`Ack`、快照、membership、presence、connectivity rumor。
- mesh 信封已经具备 `hmac` 字段，并在 runtime 中接入了签名与验签。
- `MeshRouteDiagnostic` 目前只停留在 proto / classifier 类型层；当前生产排障主要还是依赖 `/ops/status` 与 `/metrics`，没有单独的诊断消息闭环。

`internal/mesh/runtime.go` 当前已经实现了运行时主骨架，而不是“待建立”状态：

- transport adapter 生命周期管理。
- `NodeHello` 握手与邻接建立。
- generation 维护、topology flooding、stale update 去重。
- `TimeSyncRequest/Response` RTT / jitter 测量。
- 链路 tombstone 发布与拓扑收敛。
- 逐跳 forwarding、TTL、防环、去重和按流量类别选路。

### forwarding、bridge 和费用策略已定型

当前默认行为已经在代码和测试里固定，不再属于待定项：

- `cluster.forwarding.enabled` 默认启用。
- `cluster.forwarding.bridge_enabled` 默认启用。
- `cluster.forwarding.node_fee_weight <= 0` 会规范化为 `1`。
- 当 `node_fee_weight == 1` 时，五类流量默认都允许 transit。
- 当 `node_fee_weight > 1` 时：
  - `control_critical`：`ALLOW`
  - `control_query`：`ALLOW`
  - `transient_interactive`：`DISCOURAGE`
  - `replication_stream`：`DENY`
  - `snapshot_bulk`：`DENY`
- bridge 白名单当前只允许：
  - `control_critical`
  - `control_query`
  - `transient_interactive`
- `replication_stream` 与 `snapshot_bulk` 目前默认不允许跨 transport bridge。

这意味着“高费用 transit 不承载复制/快照”和“复制/快照默认不跨协议桥接”已经是当前实现语义，而不是未来才要接入的规则。

### transport 能力与 mixed transport 已落地

当前仓库已经不是“只支持两类 transport 的规划稿”，而是三类 transport 都已进入 mesh 视角：

- WebSocket 是正式的 mesh transport，不再是临时过渡层。
- libp2p capability 已暴露入站 / 出站 / native relay client / native relay service。
- ZeroMQ capability 会受 `services.zeromq.enabled` 和 `ZeroMQForwardingEnabled()` 共同约束。
- 当 ZeroMQ forwarding 关闭时：
  - ZeroMQ capability 仍可生成，但入站 / 出站 forwarding 会被标记为关闭。
  - advertised endpoint 会被隐藏。
  - mesh runtime 不会装配 ZeroMQ adapter。

mixed transport 当前已具备真实回归覆盖，而不是只有 planner 层单测：

- `websocket -> libp2p` bridge：控制查询和瞬时包可达。
- `websocket -> zeromq` bridge：在 `zeromq` build tag 下可达。
- 交替 mixed transport 大拓扑下，控制面 / query / transient 可跨 bridge 收敛。
- mixed bridge 场景下，复制流和快照流会显式返回 `ErrNoRoute`，并记录 no-path 指标。

### 数据面映射已经接线

当前 mesh runtime 不只是承载“控制面”，业务流量也已经接到新分类模型：

- `TimeSyncRequest/Response`、拓扑更新、`Ack`、membership、presence、connectivity rumor 走 `control_critical`。
- 查询类 RPC（包括 `resolve_user_sessions`）走 `control_query`。
- 瞬时包走 `transient_interactive`。
- 复制批次与补拉走 `replication_stream`。
- 快照摘要与快照分片走 `snapshot_bulk`。

需要特别强调的当前边界：

- 线性同 transport 多跳场景下，复制和快照已经可以通过 mesh 主链路传输。
- mixed transport 场景下，复制和快照目前不会跨 bridge；这是当前实现边界，不是文档遗漏。
- 多跳只改变传输路径，不改变复制、补拉、`Ack` 与快照语义。

### discovery 与拨号种子已接到 mesh runtime

当前发现能力并不是“控制面改完后再考虑”的后置项：

- 静态 `cluster.peers` 会在启动时转成 mesh dial seeds。
- 当前可拨号的 discovered peers 也会转成 mesh dial seeds。
- discovered peer 状态变化会驱动 `AddDialSeed` / `RemoveDialSeed`。
- topology generation 已持久化到 store，重启后会恢复并继续递增。

这意味着 mesh runtime 已经直接消费静态 peer 和 discovery 的结果，而不是停留在内部 fake adapter 演练阶段。

### 观测与安全能力已接线

`/ops/status` 与 `/metrics` 已经面向 mesh 主链路输出观测信息：

- `/ops/status.mesh` 会暴露：
  - `enabled`
  - `forwarding_enabled`
  - `bridge_enabled`
  - `node_fee_weight`
  - `topology_generation`
  - `transport_capabilities`
  - `traffic_rules`
  - 每个目标节点按流量类别计算出的 route
  - forwarding / no-path / decision-cost / bridge 指标快照
- `/metrics` 已经暴露：
  - `forwarded_packets_total`
  - `forwarded_bytes_total`
  - `routing_decision_cost`
  - `routing_no_path_total`
  - `bridge_forward_total`

安全边界方面，mesh runtime 当前也不是“预留 signer/verifier 钩子但默认 no-op”：

- mesh `ClusterEnvelope` 已经通过 `meshEnvelopeAuthenticator` 做 HMAC-SHA256 签名与验签。
- runtime 在发送时会签名，在读取 `NodeHello` 和后续 envelope 时都会验签。
- 相关安全测试已经覆盖合法、密钥不一致、重复 HMAC 字段、空 HMAC 字段和 legacy/appended wire 兼容。

### 时钟测量边界

mesh `TimeSyncRequest/Response` 当前已用于链路测量，但边界需要写清楚：

- RTT / jitter 会进入邻接观测和 link advertisement。
- mesh time sync 当前只服务路由测量与会话 RTT 观测。
- 它不会把 peer 时钟直接提升为可信时钟来源，也不会打开旧 clock write gate。
- 当前写 gate / trusted clock 规则仍然需要单独看 `clock-protection.md` 的说明。

## 已落地边界

以下能力已经在当前仓库中落地，不应继续按“后续阶段”描述：

### 已完成

- mesh runtime 已成为 `turntf` 的主控制面与主跨节点数据面。
- `proto/mesh.proto` 已纳入 WebSocket / libp2p / ZeroMQ 三类 transport。
- forwarding / bridge / fee-aware 默认值已在配置与测试中固定。
- WebSocket、libp2p、ZeroMQ 入站连接都能进入 mesh runtime。
- `cluster.peers` 与 discovered peers 都能转成 mesh dial seeds。
- 查询、瞬时包、复制、补拉、`Ack`、快照、membership、presence、connectivity rumor 都已接到 mesh 路由分类。
- `/ops/status` 和 `/metrics` 已能输出 mesh 路由与 bridge 观测。
- mesh wire-level HMAC 已实现并接入 runtime。

### 当前实现明确限制

- 复制流与快照流默认不跨 transport bridge。
- 高费用 transit 默认不承载复制流与快照流。
- ZeroMQ mixed transport 相关回归依赖 `zeromq` build tag 与本地 libzmq 环境。
- mesh time sync 只测链路，不参与 trusted clock 建立。

这些限制是当前实现的明确边界，不应误写成“尚未接入导致缺失”。

## 尚未落地或仍待收口的部分

虽然主链路已经切到 mesh，但并不意味着这条线已经完全收尾。当前仍适合继续推进的事项主要有以下几类：

### 1. 诊断解释能力还可以更细

当前排障主要依赖 `/ops/status.mesh.routes` 和 mesh 指标；proto 虽然已有 `MeshRouteDiagnostic`，但还没有形成单独的生产诊断闭环。后续如果要继续提升运维可解释性，更合理的方向是：

- 明确 no-path 的具体原因输出。
- 区分“被 transit 禁止”“被 bridge 禁止”“被高费用策略拒绝”“拓扑尚未收敛”等场景。
- 视需要再决定是否把 `MeshRouteDiagnostic` 接到 API 或运维接口。

### 2. 规模验证与 mixed transport 压测仍要继续

当前仓库已经有 runtime scale test、large-cluster integration test 和 mixed transport benchmark / integration test，但这部分仍属于持续演进区：

- 需要继续跟进 50/100 节点 flooding、重路由和拓扑收敛。
- 需要继续补 mixed transport 下的压测、回归和 rollout 纪律。
- 需要持续观察 `bridge_forward_total`、`routing_no_path_total` 和 `routing_decision_cost` 在大规模拓扑中的解释性。

### 3. relay-only 角色与动态计费仍未进入实现

以下事项依旧是未落地区域，而不是“当前实现的隐藏能力”：

- `relay-only` 独立节点角色。
- 动态计费系统、实时账单反馈或按链路实时调价。
- 基于费用或 bridge 的更复杂运营策略编排。

当前实现只有静态 `node_fee_weight` 与静态 disposition 规则，不要把它误解成已经具备动态费用控制面。

### 4. 是否允许复制 / 快照跨 bridge 仍应保持审慎

如果未来业务确实要求“WebSocket <-> libp2p”或“WebSocket <-> ZeroMQ”之间转发复制流 / 快照流，需要把它视为新能力，而不是现状：

- 当前默认行为是不允许。
- 当前 mixed transport 回归也明确断言 `ErrNoRoute`。
- 若未来要开放，需要单独补策略、benchmark、故障恢复和运维文档，而不是只改 bridge 白名单。

## 后续演进建议

基于当前实现状态，更合理的后续顺序是：

1. 继续收紧 `/ops/status` 与 metrics 的解释能力，必要时再考虑 route diagnostic 的独立接口。
2. 继续做规模回归、mixed transport 压测和 bridge / no-route 边界验证。
3. 在安全、身份治理和 rollout 纪律上继续收口，而不是回退到旧路由模型。
4. 只有在业务明确需要时，才单独评估复制 / 快照跨 bridge 或 relay-only 这类高风险扩展。

## 测试关注点

当前文档对应的核心回归面应理解为：

- `internal/mesh`：
  - runtime 建邻、flooding、拨号种子、TTL、防环、bridge、native relay path class、scale / reroute。
- `internal/cluster`：
  - mesh runtime 自动启动。
  - configured peer / discovered peer 与 dial seed 接线。
  - mixed transport query / transient 成功路径。
  - 同 transport 多跳复制 / 快照成功路径。
  - mixed transport 复制 / 快照 no-route 边界。
  - mesh HMAC 安全测试。
- `zeromq` build tag：
  - WebSocket <-> ZeroMQ mixed bridge 与相关 benchmark / integration test。

## 结论

`turntf/` 当前已经完成 mesh 主链路切换，并且把 forwarding、mixed transport 和主要业务数据面都接到了新模型上。需要更新的认知不是“mesh 什么时候落地”，而是“哪些能力已经是现状、哪些限制是有意保守、哪些后续工作应该围绕观测、规模和 rollout 继续推进”。后续如果继续演进，重点应放在观测解释、规模验证、安全收口和审慎扩展，而不是恢复旧 `RoutingUpdate` 路由模型。
