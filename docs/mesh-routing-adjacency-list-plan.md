# mesh 路由邻接表改造方案

## 1. 结论

可以改，而且值得改。

当前 `internal/mesh/planner.go` 中的求路逻辑本质上已经是“按状态做 Dijkstra”，但在扩展下一跳时仍然对 `TopologySnapshot.Links` 做全量扫描。随着节点数、链路数和 transport 组合增长，这一步会把复杂度拉高到接近“每弹出一个状态就扫一遍全图边”。

建议把路由协议的**本地拓扑索引**改为邻接表实现，但**不修改 wire protocol / proto**。也就是说：

- `TopologyUpdate`、`LinkAdvertisement`、flooding 语义保持不变
- 改动只发生在 `internal/mesh` 的内存拓扑快照和 planner
- 对外观测仍保留 `TopologySnapshot.Links`，避免影响现有诊断和测试

## 2. 当前问题

### 2.1 现状实现

当前 planner 的状态是：

- `plannerState{nodeID, transport}`

这意味着求路图并不是单纯的“节点图”，而是“节点 x transport”的状态图。这个建模本身没有问题，bridge 场景也必须这么做。

问题出在扩边阶段：

- `Planner.Compute()` 从优先队列弹出一个状态
- `Planner.expand()` 遍历整个 `snapshot.Links`
- 只挑出 `FromNodeID == current.nodeID && Transport == current.transport` 的边

也就是：

- 当前已经按状态建图
- 但没有按状态索引出边

### 2.2 复杂度表现

记：

- `S` = 可达状态数，约等于“有 transport 能力的节点状态数”
- `L` = 拓扑中的链路数
- `B` = bridge 产生的同节点跨 transport 转移数

当前热点复杂度近似为：

- 建路阶段：`O(S * L)`
- 再叠加堆操作：`O((L + B) * log S)`

其中真正浪费的是 `O(S * L)` 这一项，因为大量链路会被重复扫描，但大部分都不是当前状态的出边。

## 3. 目标

目标不是改协议格式，而是把本地求路从“扫全图边”改成“只读当前状态出边”。

预期收益：

- 单次 `Compute()` 从“状态数 x 全量边数”下降到“建索引一次 + 按出度遍历”
- 50/100 节点场景下，重路由和 `/ops/status` 路由枚举的 CPU 压力明显下降
- 保持现有路由策略、费用模型、bridge 规则和 tie-break 行为不变

## 4. 方案设计

### 4.1 总体思路

在 `TopologySnapshot` 内部维护一个按 `(from_node_id, transport)` 建立的邻接表索引：

- key: `from_node_id + transport`
- value: `[]LinkState`

planner 扩边时不再遍历 `snapshot.Links`，而是直接读取当前状态的出边切片。

### 4.2 数据结构

建议新增内部索引结构：

```go
type topologyLinkAdjacencyKey struct {
    fromNodeID int64
    transport  TransportKind
}
```

在 `TopologySnapshot` 增加一个**非导出字段**：

```go
type TopologySnapshot struct {
    Nodes              map[int64]NodeState
    Links              []LinkState
    TopologyGeneration uint64

    outgoingLinks map[topologyLinkAdjacencyKey][]LinkState
}
```

设计要点：

- `Links` 继续保留，保证现有观测接口和测试代码不受影响
- `outgoingLinks` 只作为求路内部索引使用，不暴露到包外 API
- 使用非导出字段，避免外部依赖索引实现细节

### 4.3 索引构建策略

建议分两层构建：

1. `MemoryTopologyStore.Snapshot()` 在复制 `s.links` 时同步填充 `outgoingLinks`
2. `Planner.Compute()` 在收到“手写构造”的 `TopologySnapshot` 时做一次懒构建兜底

这样可以同时覆盖两类场景：

- 生产路径：通过 `TopologyStore().Snapshot()` 获取，默认已经带好邻接表
- 单测路径：很多测试直接写 `TopologySnapshot{Links: ...}`，不需要立即改全部测试

### 4.4 planner 改动

将 `Planner.expand()` 的这一段：

- 遍历 `snapshot.Links`
- 过滤 `FromNodeID == current.nodeID`
- 再过滤 `Transport == current.transport`

改为：

- 直接读取 `snapshot.outgoing(current.nodeID, current.transport)`
- 只在该切片上继续判断 `Established`、relay、cost、fee、bridge

保留不变的逻辑：

- `canTransit()` 的策略判定
- `transitPenalty()` 的费用计算
- `PathClassNativeRelay` 的额外 penalty
- `BridgeAllowedForTrafficClass()` 的 bridge 限制
- `betterMeta()` 的 tie-break 顺序

### 4.5 复杂度变化

改造后复杂度近似为：

- 建索引：`O(L)`
- 求路：`O((L + B) * log S)`，其中边遍历只发生在真实出边上

相比当前：

- 从 `O(S * L)` 的重复扫描
- 降到 `O(L)` 的一次建索引 + 正常 Dijkstra 出边遍历

这也是邻接表对图搜索的标准收益。

## 5. 兼容性与影响面

### 5.1 不涉及的内容

本次方案**不涉及**：

- `proto/mesh.proto` 变更
- flooding 协议语义变更
- `TopologyUpdate` 内容字段变更
- 路由费用模型变更
- bridge 策略或流量类别规则变更

因此不需要执行 `./scripts/gen-proto.sh`。

### 5.2 受影响模块

核心影响文件预计为：

- `internal/mesh/topology.go`
- `internal/mesh/planner.go`
- `internal/mesh/topology_test.go`
- `internal/mesh/planner_test.go`

可选优化影响文件：

- `internal/cluster/status.go`

说明：

`status.go` 当前先拿一次 `snapshot := binding.TopologyStore().Snapshot()`，但后面又通过 `binding.DescribeRoute()` 逐条求路；而 `DescribeRoute()` 内部还会再次抓取新 snapshot。这个问题不是邻接表方案的主路径阻塞项，但可以作为第二阶段优化，把“同一次状态页渲染”的多次求路复用同一个 snapshot。

## 6. 实施步骤

### Phase 1：拓扑快照补齐邻接索引

工作项：

- 在 `TopologySnapshot` 增加内部邻接表字段
- 增加 `ensureOutgoingLinks()` / `outgoing()` 之类的内部辅助方法
- `MemoryTopologyStore.Snapshot()` 构建 `Links` 时同步构建索引

完成标准：

- 生产生成的 snapshot 默认带邻接索引
- 手写 snapshot 也能被懒构建补齐

### Phase 2：planner 切换为邻接表扩边

工作项：

- `Planner.Compute()` 入口确保索引可用
- `Planner.expand()` 从扫描 `snapshot.Links` 改为读取指定出边切片
- 保持现有 path class、fee、bridge、relay 行为不变

完成标准：

- 现有 planner 行为测试全部通过
- 同拓扑下的 next hop、path class、estimated cost 不发生语义回归

### Phase 3：补验证与压测

工作项：

- 增加拓扑索引单测
- 跑 `internal/mesh` 现有路由测试和 scale test
- 如有必要补一个 benchmark，对比改造前后的 `Compute()` 性能

完成标准：

- 功能回归通过
- 大规模拓扑下求路耗时下降

### Phase 4：可选的二次优化

工作项：

- 为批量路由查询复用同一份 snapshot
- 避免 `status` 页面或批量诊断中重复抓取快照

说明：

这一步不是邻接表改造的前置条件，但能把收益从“单次求路更快”进一步扩展到“批量求路也更省”。

## 7. 风险与注意事项

### 7.1 手写 snapshot 的兼容性

风险：

- 现有测试大量直接构造 `TopologySnapshot{Links: ...}`

应对：

- planner 入口做懒构建兜底，不强制一次性改完所有测试

### 7.2 索引与 `Links` 的一致性

风险：

- 如果后续有人直接修改 `snapshot.Links`，可能与邻接索引不一致

应对：

- 将 `TopologySnapshot` 继续视为只读快照
- 在代码注释中明确：索引是 `Links` 的派生缓存，不应单独维护

### 7.3 只优化出边扫描，不触碰策略语义

风险：

- 改造时顺手改动 transit / bridge / relay 逻辑，容易引入行为回归

应对：

- 本次实现只替换“边枚举方式”
- 不修改费用、bridge 和 tie-break 逻辑

## 8. 验收标准

满足以下条件即可认为方案落地成功：

- 在相同拓扑输入下，`RouteDecision` 结果与改造前一致
- `internal/mesh/planner_test.go`、`internal/mesh/forwarding_test.go`、`internal/mesh/runtime_scale_test.go` 回归通过
- 50/100 节点场景下，求路热点不再表现为重复扫描全量 `Links`
- `/ops/status` 和常规 forwarding 场景下的 CPU 开销下降

## 9. 建议的实施顺序

建议按下面顺序推进：

1. 先实现 `TopologySnapshot` 的内部邻接索引
2. 再切换 `Planner.expand()` 到邻接表
3. 跑 `internal/mesh` 全量测试
4. 再决定是否继续做 snapshot 复用和 benchmark

这样可以把改动范围稳定控制在 `internal/mesh` 内，先拿到最直接的复杂度收益，再做外围收尾优化。
