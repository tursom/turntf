# mesh 路由邻接表改造方案

## 1. 结论

这项改造已经在当前 `turntf` 实现里落地，本文现在更适合作为“当前基线 + 剩余优化项”的说明，而不是未实现的提案。

当前状态可以概括为：

- `internal/mesh` 已把本地拓扑索引改成邻接表形式，但没有修改 wire protocol / proto
- `TopologySnapshot` 仍然保留 `Links` 作为观测面，同时新增内部 `outgoingLinks` 索引供 planner 使用
- `Planner.expand()` 已不再扫描全量 `snapshot.Links`，而是按 `(from_node_id, transport)` 读取当前状态的真实出边
- `mesh runtime` 的 `DescribeRoute()`、`forwarding` 的 `Engine.Forward()`、以及 `cluster` 侧 mesh 路由查询，当前都复用同一套 `TopologyStore().Snapshot() + Planner.Compute()` 逻辑

仍然值得继续做的工作只剩两类：

- 为批量路由查询复用同一份 snapshot，减少 `/ops/status` 之类场景的重复快照获取
- 如有性能追踪需要，再补更专门的 planner benchmark 或 pprof 对照

## 2. 当前实现与原始目标的对应关系

### 2.1 拓扑快照已经具备邻接表索引

当前 `internal/mesh/topology.go` 中：

- `TopologySnapshot` 已包含 `outgoingLinks map[topologyAdjacencyKey][]LinkState`
- `MemoryTopologyStore.rebuildSnapshotLocked()` 在重建 `Links` 时会同步调用 `addOutgoingLink()` 建好索引
- `TopologySnapshot.ensureOutgoingLinks()` 仍保留懒构建兜底，覆盖“手写 `TopologySnapshot{Links: ...}`”的单测和临时构造路径

这意味着原方案里“生产路径预构建索引，手写 snapshot 懒构建补齐”的两层策略，当前已经实现。

### 2.2 planner 已经切换为邻接表扩边

当前 `internal/mesh/planner.go` 中：

- `Planner.Compute()` 入口会先调用 `snapshot.ensureOutgoingLinks()`
- `Planner.expand()` 已直接遍历 `snapshot.outgoing(current.nodeID, current.transport)`
- 扩边时只在当前状态的真实出边上继续判断 `Established`、relay、cost、fee、bridge

也就是说，这份文档最初要推动的核心改造点已经完成，planner 不再通过“每弹出一个状态就扫一遍全量 `Links`”来找下一跳。

### 2.3 路由语义并没有因为索引改造而改变

邻接表改造只替换了“如何枚举当前状态的出边”，没有改变下面这些语义：

- 仍然是 `(nodeID, transport)` 状态空间上的多源 Dijkstra
- `transitPenalty()` 的费用模型保持不变
- `PathClassNativeRelay` 仍会追加 relay penalty
- `betterMeta()` 的 tie-break 顺序保持不变
- `BridgeAllowedForTrafficClass()` 仍只允许 `control_critical`、`control_query`、`transient_interactive` 三类流量跨 transport bridge

这也意味着：

- `replication_stream` 和 `snapshot_bulk` 当前依旧不能跨 transport bridge
- mixed transport 场景下，复制流和快照流的“不可跨桥”约束仍由现有策略生效，而不是被邻接表改造改变

## 3. 与 mesh runtime / routing / forwarding 的真实对应关系

### 3.1 runtime 的路由查询已经走新 planner

当前 `internal/mesh/runtime.go` 中：

- `Runtime.DescribeRoute()` 直接调用 `p.planner.Compute(r.store.Snapshot(), destinationNodeID, trafficClass, TransportUnspecified)`

因此运行时对外暴露的“当前路由决策”已经建立在邻接表索引之上。

### 3.2 forwarding 数据面也复用同一套求路逻辑

当前 `internal/mesh/forwarding.go` 中：

- `Engine.Forward()` 会先抓取最新 `snapshot := e.snapshotFn()`
- 然后调用 `e.planner.Compute(snapshot, packet.TargetNodeId, packet.TrafficClass, ingress)`

这说明邻接表改造并不只影响诊断接口，而是已经进入瞬时包转发的主路径。路由查询与数据面转发当前共享同一套 planner 行为。

### 3.3 cluster 侧的 mesh 路由描述是 runtime 的薄封装

当前 `internal/cluster/mesh_runtime.go` 中：

- `MeshRuntimeBinding.DescribeRoute()` 只是透传到 `runtime.DescribeRoute()`

因此 `cluster` 层没有维护第二套独立的 mesh 求路实现，本文讨论的 planner 行为会直接反映到：

- `Manager` 的 mesh 数据面投递
- `/ops/status` 中的 route 枚举
- 混合传输场景下的路由可达性判断

### 3.4 仍然存在的二次优化点

当前 `internal/cluster/status.go` 中，`meshStatusSnapshot()` 会：

1. 先取一次 `snapshot := binding.TopologyStore().Snapshot()` 来枚举节点
2. 再通过 `binding.DescribeRoute()` 为每个目标节点、每个流量类别逐条求路

而 `DescribeRoute()` 内部又会再次抓取 `r.store.Snapshot()`。

所以这份文档原先提到的“批量路由查询复用同一份 snapshot”在今天仍然是成立的优化点，只是它已经从“邻接表改造的前置步骤”变成“当前实现上的可选进一步优化”。

## 4. 复杂度与收益的当前表述

这份文档最初关注的问题，是 planner 在扩边阶段对 `TopologySnapshot.Links` 的重复全量扫描，热点复杂度接近：

- 建路阶段：`O(S * L)`
- 再叠加堆操作：`O((L + B) * log S)`

其中：

- `S` = 可达状态数，约等于“有 transport 能力的节点状态数”
- `L` = 拓扑中的链路数
- `B` = bridge 产生的同节点跨 transport 转移数

对照当前实现，更准确的说法应改成：

- 生产路径里，`MemoryTopologyStore` 在重建 snapshot 时已经一次性建立邻接索引
- 手写 snapshot 路径里，`Planner.Compute()` 会在入口懒构建索引
- planner 的边遍历现在只发生在真实出边上，不再对每个状态重复扫描全量 `Links`

因此，原文“应该做的复杂度优化”现在已经是现状，而不是未来收益假设。

不过，仓库当前并没有在这个文档里保留“改造前后 planner 扫描方式”的定量对照结果，因此不能再把某个固定 CPU 降幅写成已验证结论。更稳妥的表述是：

- 邻接表索引已经进入主实现
- 现有 `runtime_scale_test.go`、`forwarding_test.go`、`cluster` mesh benchmark 可以覆盖功能和部分性能面
- 若后续要发布专门的性能结论，仍建议补 planner 级 benchmark 或 pprof 截图作为依据

## 5. Phase 状态更新

### Phase 1：拓扑快照补齐邻接索引

状态：已完成。

当前落点：

- `TopologySnapshot` 已增加内部邻接表字段
- `TopologySnapshot.ensureOutgoingLinks()` / `outgoing()` 已存在
- `MemoryTopologyStore.rebuildSnapshotLocked()` 已同步构建索引

### Phase 2：planner 切换为邻接表扩边

状态：已完成。

当前落点：

- `Planner.Compute()` 已确保索引可用
- `Planner.expand()` 已从扫描 `snapshot.Links` 切换到读取指定出边切片
- 现有 `planner_test.go` 继续验证 next hop、path class、estimated cost、bridge 与 relay 语义

### Phase 3：补验证与压测

状态：部分完成。

当前已具备：

- `topology_test.go` 已覆盖 snapshot 索引构建与手写 snapshot 懒构建
- `planner_test.go` 已覆盖 bridge、relay、fee、traffic rule 等求路语义
- `forwarding_test.go`、`runtime_integration_test.go`、`runtime_scale_test.go` 已覆盖端到端转发、重路由和 50/100 节点收敛
- 当前仓库还存在 `BenchmarkMeshForwardingHotPath` 以及多组 `cluster` mesh benchmark

当前仍未形成的内容：

- 没有单独保留“改造前 vs 改造后”的 planner 专项 benchmark 对照材料

### Phase 4：批量查询复用 snapshot

状态：未完成，且仍是可选优化项。

当前原因：

- `/ops/status` 的 route 枚举还会通过 `DescribeRoute()` 重复抓取 snapshot
- 这不影响正确性，但会让批量诊断场景无法完全复用同一份拓扑视图

## 6. 风险与注意事项

### 6.1 `TopologySnapshot` 仍应视为只读快照

当前实现把 `outgoingLinks` 视为 `Links` 的派生缓存。

这意味着：

- 如果后续有人在拿到 `Snapshot()` 结果后直接修改 `snapshot.Links`
- 那么 `Links` 与 `outgoingLinks` 可能失去一致性

因此文档和实现都应继续把 `TopologySnapshot` 视为只读数据，不应在构建后再手改其中内容。

### 6.2 邻接表改造没有放宽 mixed transport 语义

虽然 planner 现在能更高效地在 `(node, transport)` 状态图上求路，但这并不意味着所有流量都能跨桥：

- bridge 仍只对 `control_critical`、`control_query`、`transient_interactive` 开放
- `replication_stream`、`snapshot_bulk` 当前仍受既有策略限制，不能因为邻接表存在就跨 transport 转发

### 6.3 批量诊断的一致性仍要单独处理

邻接表已经解决“单次求路如何枚举出边”的问题，但没有自动解决“同一页面批量查询是否使用同一份拓扑视图”的问题。

如果后续对 `/ops/status` 的一致性和开销有更高要求，仍需要单独把 snapshot 复用下沉到批量求路流程里。

## 7. 建议的后续顺序

如果后续还要继续推进这份文档对应的工作，建议顺序调整为：

1. 保持当前邻接表实现不变，继续以现有 `planner` / `forwarding` / `runtime` 共享路径为基线
2. 视需要为 `status` 页面或批量诊断增加“同一次渲染复用同一份 snapshot”的能力
3. 如果需要对外发布性能结论，再补 planner 专项 benchmark 或 pprof 证据

这样更符合当前代码现状：核心邻接表改造已经完成，剩下的是围绕观测、批量查询和性能量化的增量优化。
