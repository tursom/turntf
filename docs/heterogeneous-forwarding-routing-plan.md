# 新服务异构转发路由架构实施计划

本文档基于当前仓库实现状态，整理“多 transport 拓扑控制平面 + 费用感知策略路由 + 逐跳数据平面”这一新架构的落地计划。它不是纯概念设计稿，而是面向当前代码库的实施说明，用于指导后续 proto 定稿、模块拆分、里程碑排期和验收。

## 目标与边界

目标架构如下：

- 不保留旧路由和旧兼容层，直接采用新的 mesh 协议与转发模型。
- 原生支持两类 transport：`libp2p`、`zeromq`。
- forwarding 是节点级显式能力，默认开启。
- 引入 `node_fee_weight` 和按消息类别的策略路由。
- 路由核心采用链路状态多层图最短路，图状态为 `(node_id, transport)`。
- 数据面采用逐跳重算，不使用源路由。

本文档不覆盖以下内容：

- 旧协议与新协议长期混部方案。
- relay-only 独立节点角色设计。
- 动态计费系统或实时账单反馈。
- 为兼容旧 `RoutingUpdate` 协议而保留的长期双栈运行模式。

## 当前仓库状态

### 已有基础件

仓库里已经存在一组新的 mesh 雏形实现：

- `proto/mesh.proto` 已定义新的枚举、`ClusterEnvelope`、`NodeHello`、`TopologyUpdate`、`ForwardedPacket` 等消息。
- `internal/mesh/types.go` 已定义 `TransportAdapter`、`TopologyStore`、`RoutePlanner`、`ForwardingEngine`、`TrafficClassifier` 等核心接口。
- `internal/mesh/topology.go` 已提供内存版 `TopologyStore`。
- `internal/mesh/planner.go` 已实现基于 `(node_id, transport)` 状态的路径搜索。
- `internal/mesh/forwarding.go` 已实现 TTL、去重、逐跳转发和简单防环。
- `internal/mesh/policy.go` 已实现费用权重、`DISCOURAGE`/`DENY`、bridge 白名单等基础规则。
- `internal/mesh/*_test.go` 已覆盖部分图路由、bridge、费用和转发行为。

这说明新架构不是从零开始，已经有了 proto、策略和算法层面的初始骨架。

### 当前主链路仍是旧实现

当前真正驱动集群运行的仍是旧 `cluster` 协议栈：

- `internal/cluster/manager_protocol.go` 仍以旧 `Envelope` 为主协议入口。
- `internal/cluster/routing.go` 仍基于 `RoutingUpdate + routeAdvertisement` 维护旧动态路由。
- `internal/cluster/config.go` 还没有新架构所需的 `forwarding`、`bridge_enabled`、`node_fee_weight` 和分流策略配置。
- `internal/api/operations.go` 与 `internal/cluster/status.go` 还没有新架构的运维观测字段。
- `internal/cluster/transport.go` 目前提供的是旧 transport 抽象，还没有切换到 `internal/mesh` 定义的新 adapter 接口。

因此，当前最合适的推进方式不是继续在旧路由上打补丁，而是保留旧实现作为开发期参考和回归基线，在新 mesh 栈闭环后整体切主。

## 关键差距

### 1. 协议已起草，但还未完成主链路接入

`proto/mesh.proto` 已具备主要消息结构，但还没有替换现有 `cluster` 主协议循环，也没有成为 transport 握手和业务复制的唯一承载格式。

### 2. 算法层已存在，但运行时尚未建立

`internal/mesh` 已经有：

- 拓扑存储
- 路由规划
- 策略默认值
- 逐跳转发引擎

但还缺少：

- 邻接建立与 `NodeHello` 交换
- `TopologyUpdate` flooding runtime
- 链路 RTT/jitter 测量与更新
- transport 能力发布
- 实际业务消息到 `TrafficClassifier` 和 `ForwardingEngine` 的接线

### 3. 配置、观测、业务接入都尚未完成

新架构要求的不只是协议重写，还包括：

- 新配置树
- 新 `/ops/status`
- 新 Prometheus 指标
- 查询流、瞬时包、复制流、快照流迁移到新分类和转发模型

### 4. 当前默认值需要与集群设计目标对齐

当前 `internal/mesh/policy.go` 中 `DefaultForwardingPolicy` 已默认开启 `TransitEnabled`，但还没有把 `BridgeEnabled`、高费用策略和配置默认值整体对齐到“广域网单集群”的设计目标。这部分需要在实施最前期统一，避免协议、配置和测试各自维护不同默认值。

## 实施原则

- 先冻结协议与默认语义，再接运行时，避免边实现边改枚举和默认策略。
- 优先构建新 mesh 闭环，不继续扩大旧 `RoutingUpdate` 模型的能力范围。
- transport 先适配，业务后迁移；先让 transport 成为 mesh runtime 的输入，再让复制、查询、快照等业务消息接入。
- 控制面先行，数据面后续分批切换。
- 旧实现只作为过渡期基线和回归参照，不作为新能力长期承载层。

## 分阶段实施

## Phase 1：协议与默认值定稿

目标：把新架构的“语言”和“默认行为”先固定下来。

工作项：

- 定稿 `proto/mesh.proto` 中的枚举、消息字段和命名。
- 明确 `ClusterEnvelope` 是否作为新 mesh 协议唯一入口。
- 固化 `TrafficClass`、`ForwardingDisposition`、`PathClass` 的语义。
- 修正 forwarding 默认值：
  - `cluster.forwarding.enabled = true`
  - `cluster.forwarding.bridge_enabled = true`
  - `cluster.forwarding.node_fee_weight = 1`
- 固化高费用节点默认策略：
  - `TRAFFIC_CONTROL_CRITICAL = ALLOW`
  - `TRAFFIC_CONTROL_QUERY = ALLOW`
  - `TRAFFIC_TRANSIENT_INTERACTIVE = DISCOURAGE`
  - `TRAFFIC_REPLICATION_STREAM = DENY`
  - `TRAFFIC_SNAPSHOT_BULK = DENY`
- 固化跨协议 bridge 白名单：
  - 允许：`TRAFFIC_CONTROL_CRITICAL`、`TRAFFIC_CONTROL_QUERY`、`TRAFFIC_TRANSIENT_INTERACTIVE`
  - 禁止：`TRAFFIC_REPLICATION_STREAM`、`TRAFFIC_SNAPSHOT_BULK`

交付物：

- 定稿后的 `proto/mesh.proto`
- 重新生成的 `internal/proto/mesh.pb.go`
- 同步后的 `internal/mesh/policy.go`

验收标准：

- 新 proto 可稳定作为后续实现唯一依据。
- 默认值与设计文档完全一致。
- 所有后续模块不再自行推导 forwarding 默认行为。

## Phase 2：配置模型与策略落地

目标：让节点能够以配置方式表达 forwarding、bridge、费用和 transport 能力。

工作项：

- 在 `internal/cluster/config.go` 增加最小配置集合：
  - `[cluster]`
    - `node_id`
  - `[cluster.forwarding]`
    - `enabled`
    - `bridge_enabled`
    - `node_fee_weight`
  - `[cluster.forwarding.traffic]`
    - `control_critical`
    - `control_query`
    - `transient_interactive`
    - `replication_stream`
    - `snapshot_bulk`
  - `[services.libp2p]`
    - `native_relay_client_enabled`
    - `native_relay_service_enabled`
  - `[services.zeromq]`
    - `forwarding_enabled`
- 在 `config.example.toml` 增加对应示例。
- 提供从配置到 `mesh.ForwardingPolicy`、`mesh.TransportCapability` 的转换函数。
- 为配置默认值、非法枚举值、策略覆盖关系补单测。

交付物：

- 更新后的配置结构与校验逻辑
- 示例配置文档
- 配置转换单测

验收标准：

- 节点重启后可以稳定得到一致的 forwarding/bridge/fee 策略对象。
- 不同 transport 的启用状态与本地 capability 能正确映射。

## Phase 3：transport 适配层

目标：把现有 `libp2p`、`zeromq` 运行能力包装成 mesh runtime 可直接消费的 adapter。

工作项：

- 在现有 `cluster` transport 基础上实现 `mesh.TransportAdapter`。
- 为 `libp2p` 适配器提供：
  - `Kind()`
  - `Accept()`
  - `Dial()`
  - `LocalCapabilities()`
  - native relay client/service 能力发布
- 为 `zeromq` 适配器提供：
  - `Kind()`
  - `Accept()`
  - `Dial()`
  - `LocalCapabilities()`
  - 应用层 forwarding 能力表达
- 明确 `RemoteNodeHint()` 的来源和语义。
- 建立 adapter 生命周期与 `Manager`/runtime 生命周期的绑定。

注意事项：

- 这一步先做 transport 适配，不直接修改业务复制语义。
- libp2p 的 relay/hole punching 细节仍留在 transport 层；路由层只消费“链路是否可用”和“链路代价”。
- zeromq 不引入 broker 角色，仍采用业务节点上的逐跳 forwarding。

交付物：

- `libp2p` mesh adapter
- `zeromq` mesh adapter
- adapter 层测试

验收标准：

- mesh runtime 可以不依赖旧 `cluster.TransportConn` 直接收发新协议消息。
- 两个 transport 都能建立邻接并交换新 `NodeHello`。

## Phase 4：控制平面运行时

目标：把“静态算法模块”补成真正工作的控制平面。

工作项：

- 实现 mesh runtime，负责：
  - 启动 transport adapter
  - 接收入站连接
  - 建立邻接
  - 交换 `NodeHello`
  - 周期性发送 `TopologyUpdate`
  - 处理 flooding 去重
  - 更新 `TopologyStore`
- 在本地构建多层图：
  - 顶点：`(node_id, transport)`
  - 边：跨节点链路边 + 节点内 bridge 边
- 增加链路测量：
  - `cost_ms`
  - `jitter_ms`
  - `established`
- 让 `libp2p` 的直连与 native relay 都能体现在 `path_class` 上。
- 让 generation 前进规则明确、单调、可观测。

交付物：

- mesh 控制平面 runtime
- `NodeHello` / `TopologyUpdate` 处理链路
- flooding 测试

验收标准：

- 多节点之间能稳定建立全局拓扑视图。
- generation 单调推进。
- 链路失效后拓扑状态会收敛更新。

## Phase 5：策略路由与逐跳转发接线

目标：让 `internal/mesh` 中已有的 `RoutePlanner` 和 `ForwardingEngine` 真正进入消息路径。

工作项：

- 建立统一 `TrafficClassifier` 入口。
- 所有需要跨节点转发的消息，在发送前都先分类。
- 将以下消息映射到固定流量类别：
  - `Hello`、`Keepalive/TimeSync`、`TopologyUpdate` -> `TRAFFIC_CONTROL_CRITICAL`
  - 查询类 RPC -> `TRAFFIC_CONTROL_QUERY`
  - 非持久化即时包 -> `TRAFFIC_TRANSIENT_INTERACTIVE`
  - 复制批次与补拉 -> `TRAFFIC_REPLICATION_STREAM`
  - 快照清单与快照分片 -> `TRAFFIC_SNAPSHOT_BULK`
- 将旧瞬时包路由逻辑从 `internal/cluster/routing.go` 迁移到 `mesh.ForwardingEngine`。
- 统一执行：
  - TTL 递减
  - `(source_node_id, packet_id)` 去重
  - `last_hop_node_id` 防立即回环
  - 每跳独立重算

注意事项：

- 不继续扩展旧 `RoutingUpdate` distance-vector 逻辑。
- 在新转发链路完成前，旧路由仍作为临时回归基线存在。

交付物：

- mesh 数据平面消息分发入口
- 新瞬时包/查询包 forwarding 路径
- 转发与防环测试

验收标准：

- 控制面、查询流、瞬时包可以基于新图路由正常转发。
- forwarding 关闭的节点不会被错误地选为 transit。
- bridge 关闭或策略 `DENY` 时，路径会立即失效。

## Phase 6：复制流与快照流迁移

目标：把大流量业务流量接入新策略路由，并严格执行费用与桥接限制。

工作项：

- 将复制批次、补拉请求接入 `TRAFFIC_REPLICATION_STREAM`。
- 将快照清单、快照分片接入 `TRAFFIC_SNAPSHOT_BULK`。
- 确保高费用节点默认不承载复制流和快照流 transit。
- 确保复制和快照默认不允许跨协议 bridge。
- 当仅有高费用 transit 可达时：
  - 控制面允许通过
  - 复制流与快照流不走该路径，等待更便宜路径恢复

交付物：

- 复制与快照的数据面迁移
- 费用策略和 bridge 限制测试

验收标准：

- 大流量路径不会误走高费用 transit。
- A 仅 `libp2p`、B 双栈、C 仅 `zeromq` 的场景下：
  - 控制面可 A -> B -> C
  - 复制流不可经由 B 做跨协议桥接

## Phase 7：切换主链路并清理旧实现

目标：完成从旧路由模型到新 mesh 模型的切主。

工作项：

- 让新的 mesh 协议栈成为默认控制面和数据面入口。
- 从旧 `cluster` 主循环中移除对旧 `RoutingUpdate` 的依赖。
- 停止扩展旧 `routeAdvertisement` 和 `routingTable` 模型。
- 清理不再使用的旧路由代码、旧测试和冗余状态字段。
- 更新开发文档与运维文档，移除“新旧路由并存”的临时描述。

交付物：

- 新 mesh 主链路
- 清理后的旧路由代码
- 更新后的文档与测试

验收标准：

- 新路径成为唯一有效转发实现。
- 旧 `RoutingUpdate` 不再承担生产语义。
- 回归测试仅验证新路由模型。

## Phase 8：运维观测与压测

目标：补齐新架构上线所需的可观测性和容量验证。

工作项：

- 在 `/ops/status` 暴露：
  - 节点 transport 能力
  - forwarding 开关
  - bridge 开关
  - `node_fee_weight`
  - 每类流量准入策略
  - 当前拓扑 generation
  - 每个目的节点按流量类别的当前路由
- 在 `/metrics` 暴露至少以下指标：
  - `forwarded_packets_total{traffic_class,path_class}`
  - `forwarded_bytes_total{traffic_class,path_class}`
  - `routing_decision_cost{traffic_class}`
  - `routing_no_path_total{traffic_class}`
  - `topology_generation`
  - `node_fee_weight`
  - `bridge_forward_total{traffic_class}`
- 增加稳定性与压测场景：
  - 50/100 节点 flooding 收敛
  - 高费用节点字节增长受控
  - next-hop 失效后重路由
  - topology generation 前进后旧路径失效

交付物：

- `/ops/status` 新字段
- 新指标
- 压测脚本或测试方案

验收标准：

- 运维可以直接观察“为什么无路由”“为什么选了该路径”“为什么未发生 bridge”。
- 在规模测试下控制平面可以稳定收敛。

## 推荐首批实施范围

如果只启动第一批开发工作，建议把范围控制在以下四项：

1. 协议与默认值修正。
2. 新配置模型与策略转换。
3. `libp2p`/`zeromq` 的 mesh adapter 骨架。
4. mesh 控制平面 runtime 骨架。

这样做的原因是：

- 这四项能先把新架构从“算法草图”推进成“可启动、可建邻、可同步拓扑”的最小闭环。
- 这四项完成后，后续数据面迁移会清晰很多。
- 这四项也最能提前暴露接口设计问题，避免复制流和快照流接线时返工。

## 测试计划

### 路由图单测

- 单 transport 最短路正确。
- `forwarding=false` 的节点不会被选为 transit。
- `bridge_enabled=false` 时不会生成跨协议路径。
- `DISCOURAGE` 路径仅在无 `ALLOW` 路径时被选中。

### 费用感知单测

- 控制面消息可走高费用节点。
- 复制流与快照流不能走高费用 transit。
- 高费用节点对 `TRAFFIC_TRANSIENT_INTERACTIVE` 仅作为兜底路径。

### transport 场景测试

- libp2p 直连失败但 native relay 成功时，控制面仍能收敛。
- 路由结果可正确标记 `NATIVE_RELAY`。
- zeromq 两跳和三跳 hop-by-hop forwarding 成功。
- 关闭 forwarding 后路径立即失效。

### 跨协议桥接测试

- A 仅 `libp2p`，B 双栈，C 仅 `zeromq`。
- 控制面允许 A -> B -> C。
- 复制流不能通过 B 跨协议桥接。

### 防环与恢复测试

- 有环拓扑下 TTL 与去重能阻止包循环。
- next-hop 失效后重新选路。
- topology generation 前进后旧路径不再沿用。

### 压测

- 控制面 flooding 在 50/100 节点规模下稳定收敛。
- 高费用节点不会因复制流误路由而出现大字节增长。

## 风险与注意事项

- 新旧路由模型长期并存会显著提高复杂度，因此过渡期必须短。
- forwarding 默认值一旦修错，会直接影响路径选择、运维预期和测试结论。
- 如果在 transport adapter 尚未稳定前就接入业务流量，排障会混合 transport 问题与路由问题。
- `/ops/status` 和 metrics 若跟不上主链路切换，上线后将很难解释路径选择结果。
- 文档中的“默认禁止复制流和快照流走高费用 transit”必须在测试中锁死，不能依赖口头约定。

## 结论

当前仓库已经具备新 mesh 架构的 proto、策略和算法基础，但主运行时仍停留在旧 `cluster` 路由模型。最合理的实施路径是：先冻结协议与默认值，再完成配置模型、transport adapter 和控制平面 runtime，随后分批把瞬时包、查询、复制和快照迁移到新的策略路由与逐跳转发链路，最终整体切主并移除旧 `RoutingUpdate` 模型。

这条路径能最大程度复用现有 `internal/mesh` 成果，同时避免在旧实现上继续投入会被后续推翻的增量复杂度。
