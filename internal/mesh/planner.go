package mesh

import (
	"container/heap"
)

// Planner 使用多源 Dijkstra 算法在 (nodeID, transport) 组合状态空间上计算路由。
// 该算法本质是在一个扩展状态图上运行标准 Dijkstra：将每个 (节点, 传输) 二元组视
// 为一个独立状态，状态之间的转移包括沿出站链路的移动和同一节点内的跨传输桥接。
// 初始化时需要本地节点 ID。
type Planner struct {
	// localNodeID 本地节点 ID，用作所有路由计算的起点，同时也是中继检查的边界。
	localNodeID int64
}

// plannerState 表示 Dijkstra 搜索中的一个状态：(节点, 传输) 二元组。
// 一个节点如果有多种可用传输，就会对应多个不同的 plannerState。
type plannerState struct {
	nodeID    int64         // 当前所处的节点。
	transport TransportKind // 当前使用的传输类型。
}

// plannerMeta 记录从起点到当前状态的路径元数据，用于代价比较和最终路由决策的构建。
type plannerMeta struct {
	cost           int64         // 累计路径总代价（毫秒），包括链路代价、桥接/中继惩罚和中转费用。
	usedBridge     bool          // 路径中是否使用了跨传输桥接（会影响路径分类）。
	usedRelay      bool          // 路径中是否使用了本地中继（会增加惩罚代价）。
	externalHops   int           // 经过的外部跳数（离开本地节点后的链路跳数计数）。
	firstHopNode   int64         // 路径的第一跳节点 ID（用于构建 RouteDecision.NextHopNodeID）。
	firstTransport TransportKind // 路径第一跳使用的传输类型（用于构建 RouteDecision.OutboundTransport）。
}

// queueItem 是优先队列中的元素，包含状态、其元数据和在堆中的索引。
type queueItem struct {
	state plannerState // Dijkstra 的状态。
	meta  plannerMeta  // 状态对应的累计路径元数据。
	index int          // 在堆数组中的索引，由 heap.Interface 维护。
}

// stateQueue 实现 container/heap 的 Interface，用于多源 Dijkstra 的优先队列。
// 队列按 betterMeta 排序，优先 pop 代价最小、桥接少、中继少、跳数小的路径。
type stateQueue []*queueItem

// NewPlanner 创建一个以 localNodeID 为起点的 Planner。
func NewPlanner(localNodeID int64) *Planner {
	return &Planner{localNodeID: localNodeID}
}

// Compute 使用多源 Dijkstra 算法在 (nodeID, transport) 状态空间中计算最佳路径。
//
// 算法步骤：
//  1. 初始化多源起点：如指定了入站传输，则仅从该传输的本地状态开始；
//     否则从所有 OutboundEnabled = true 的本地传输开始。
//  2. 使用优先队列按代价-桥接-中继-跳数顺序扩展状态。
//  3. 每次从队列中取出最优状态，若已到达目标节点则立即返回 RouteDecision。
//  4. 扩展：沿出站链路生成相邻状态转移，超出发送节点的中转限制时停止扩展。
//  5. 跨传输桥接：若当前节点启用了桥接且流量类别允许，生成桥接转移。
//
// 返回路由决策与是否可达。不可达时 ok 为 false。
func (p *Planner) Compute(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind) (RouteDecision, bool) {
	if p == nil || p.localNodeID <= 0 || destinationNodeID <= 0 {
		return RouteDecision{}, false
	}
	snapshot.ensureOutgoingLinks()
	localNode, ok := snapshot.Node(p.localNodeID)
	if !ok {
		return RouteDecision{}, false
	}

	// 构建初始状态：从本地节点所有启用了出站能力的传输（或指定入站传输）开始。
	starts := make([]plannerState, 0, len(localNode.TransportCaps))
	if ingressTransport != TransportUnspecified {
		if localNode.HasTransport(ingressTransport) {
			starts = append(starts, plannerState{nodeID: p.localNodeID, transport: ingressTransport})
		}
	} else {
		for kind, capability := range localNode.TransportCaps {
			if capability != nil && capability.OutboundEnabled {
				starts = append(starts, plannerState{nodeID: p.localNodeID, transport: kind})
			}
		}
	}
	if len(starts) == 0 {
		return RouteDecision{}, false
	}

	// 多源 Dijkstra：从本地节点所有启用了出站的传输出发，在 (节点, 传输) 状态空间中搜索。
	best := make(map[plannerState]plannerMeta)
	pq := make(stateQueue, 0, len(starts))
	for _, start := range starts {
		meta := plannerMeta{}
		best[start] = meta
		heap.Push(&pq, &queueItem{state: start, meta: meta})
	}

	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*queueItem)
		// 跳过已过时的状态（代价或第一跳信息已变更）。
		currentBest, ok := best[item.state]
		if !ok || currentBest.cost != item.meta.cost || currentBest.firstHopNode != item.meta.firstHopNode || currentBest.firstTransport != item.meta.firstTransport {
			continue
		}
		// 到达目标节点（且不是本地节点自身）。
		if item.state.nodeID == destinationNodeID && item.state.nodeID != p.localNodeID {
			return buildRouteDecision(destinationNodeID, snapshot.TopologyGeneration, item.meta), true
		}
		for _, next := range p.expand(snapshot, destinationNodeID, trafficClass, ingressTransport, item.state, item.meta) {
			current, ok := best[next.state]
			if !ok || betterMeta(next.meta, current) {
				best[next.state] = next.meta
				heap.Push(&pq, &queueItem{state: next.state, meta: next.meta})
			}
		}
	}
	return RouteDecision{}, false
}

// transition 表示一次从当前状态到下一个状态的合法转移，包含转移后的状态和累计元数据。
type transition struct {
	state plannerState // 转移后的目标状态。
	meta  plannerMeta  // 转移后的累计路径元数据。
}

// expand 生成从当前状态出发的所有合法转移，包括两类：
//
//  1. 出站链路转移：沿 outbound 的出站链表遍历，跳过未建立的链路。
//     每经过一条链路：累加 CostMs + JitterMs；经过中间节点时施加中转惩罚；
//     中继链路额外施加 RelayPenaltyMs。
//
//  2. 跨传输桥接转移：当当前节点启用桥接且流量类别允许桥接时，
//     对当前节点拥有的每个不同出站传输生成一个桥接转移（代价 + BridgePenaltyMs）。
//
// 转移前先通过 canTransit 检查当前节点是否允许为该流量类别中转。
// 不允许中转时返回空列表。
func (p *Planner) expand(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind, current plannerState, meta plannerMeta) []transition {
	transitions := make([]transition, 0)
	if !p.canTransit(snapshot, destinationNodeID, trafficClass, ingressTransport, current.nodeID) {
		return transitions
	}
	// 沿出站链路转移，累加链路的 (代价 + 抖动) 和中转惩罚。
	for _, link := range snapshot.outgoing(current.nodeID, current.transport) {
		if !link.Established {
			continue
		}
		nextState := plannerState{nodeID: link.ToNodeID, transport: link.Transport}
		nextMeta := meta
		nextMeta.cost += link.CostMs + link.JitterMs
		if current.nodeID != p.localNodeID && current.nodeID != destinationNodeID {
			node, ok := snapshot.Node(current.nodeID)
			if !ok {
				continue
			}
			nextMeta.cost += transitPenalty(node.ForwardingPolicy, trafficClass)
		}
		if link.PathClass == PathClassNativeRelay {
			nextMeta.cost += RelayPenaltyMs
			nextMeta.usedRelay = true
		}
		nextMeta.externalHops++
		if nextMeta.firstHopNode == 0 {
			nextMeta.firstHopNode = link.ToNodeID
			nextMeta.firstTransport = link.Transport
		}
		transitions = append(transitions, transition{state: nextState, meta: nextMeta})
	}

	// 如果此流量类别允许桥接且当前节点启用了桥接，则对节点拥有的每个不同输出传输生成桥接转移。
	if !BridgeAllowedForTrafficClass(trafficClass) {
		return transitions
	}
	node, ok := snapshot.Node(current.nodeID)
	if !ok || node.ForwardingPolicy == nil || !node.ForwardingPolicy.BridgeEnabled {
		return transitions
	}
	for kind, capability := range node.TransportCaps {
		if kind == current.transport || capability == nil || !capability.OutboundEnabled {
			continue
		}
		nextState := plannerState{nodeID: current.nodeID, transport: kind}
		nextMeta := meta
		nextMeta.cost += BridgePenaltyMs
		nextMeta.usedBridge = true
		transitions = append(transitions, transition{state: nextState, meta: nextMeta})
	}
	return transitions
}

// canTransit 检查指定节点是否允许为给定流量类别中转流量。
// 中转条件：
//   - 目标节点（destinationNodeID）和本地节点（localNodeID）不允许中转（它们分别是终点和起点）。
//   - 本地节点的中转需额外检查入站传输：若指定了入站传输（非 unspecified），
//     则必须启用了入站 TransitEnabled 且策略不允许拒绝该流量类别。
//   - 远端节点必须 TransitEnabled = true 且 Disposition 不为 Deny。
func (p *Planner) canTransit(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind, nodeID int64) bool {
	if nodeID == destinationNodeID {
		return false
	}
	if nodeID == p.localNodeID {
		if ingressTransport == TransportUnspecified {
			return true
		}
		node, ok := snapshot.Node(nodeID)
		if !ok || node.ForwardingPolicy == nil {
			return false
		}
		if !node.ForwardingPolicy.TransitEnabled {
			return false
		}
		return DispositionForTraffic(node.ForwardingPolicy, trafficClass) != DispositionDeny
	}
	node, ok := snapshot.Node(nodeID)
	if !ok || node.ForwardingPolicy == nil {
		return false
	}
	if !node.ForwardingPolicy.TransitEnabled {
		return false
	}
	return DispositionForTraffic(node.ForwardingPolicy, trafficClass) != DispositionDeny
}

// transitPenalty 计算经过中间节点中转的额外代价值。
// 公式：NodeFeeWeight × TrafficClassFactor + 若策略为 Discourage 则额外加 DiscouragePenaltyMs。
// NodeFeeWeight 是节点的收费权重，TrafficClassFactor 是流量类别的权重系数。
// 流量类别权重越大（如快照批量），中转代价越高，路径选择越倾向于避开该节点。
func transitPenalty(policy *ForwardingPolicy, trafficClass TrafficClass) int64 {
	if policy == nil {
		return 0
	}
	penalty := policy.NodeFeeWeight * TrafficClassFactor(trafficClass)
	if DispositionForTraffic(policy, trafficClass) == DispositionDiscourage {
		penalty += DiscouragePenaltyMs
	}
	return penalty
}

// betterMeta 是路径的优先度比较函数，用于优先队列排序和路径替换决策。
// 比较优先级（从高到低）：
//  1. 总代价（cost）更小优先。
//  2. 未使用桥接（usedBridge）优先于使用桥接。
//  3. 未使用中继（usedRelay）优先于使用中继。
//  4. 跳数（externalHops）更少优先。
//  5. 第一跳节点 ID（firstHopNode）更小优先（确定性决胜）。
//  6. 第一传输（firstTransport）更小优先（确定性决胜）。
//
// 这种多级排序确保了路径选择的确定性和可预测性。
func betterMeta(candidate, current plannerMeta) bool {
	if candidate.cost != current.cost {
		return candidate.cost < current.cost
	}
	if candidate.usedBridge != current.usedBridge {
		return !candidate.usedBridge
	}
	if candidate.usedRelay != current.usedRelay {
		return !candidate.usedRelay
	}
	if candidate.externalHops != current.externalHops {
		return candidate.externalHops < current.externalHops
	}
	if candidate.firstHopNode != current.firstHopNode {
		return candidate.firstHopNode < current.firstHopNode
	}
	return candidate.firstTransport < current.firstTransport
}

// buildRouteDecision 根据规划器元数据构建 RouteDecision（路由决策）。
// PathClass 的判定逻辑：
//   - usedBridge = true → PathClassCrossTransportBridge（跨传输桥接）。
//   - usedRelay = true → PathClassNativeRelay（本地中继路径）。
//   - externalHops > 1 → PathClassSameTransportForward（同传输多跳转发）。
//   - 否则 → PathClassDirect（直连路径，仅一跳）。
//
// firstHopNode 和 firstTransport 来自元数据中记录的第一跳信息。
func buildRouteDecision(destinationNodeID int64, generation uint64, meta plannerMeta) RouteDecision {
	pathClass := PathClassDirect
	if meta.usedBridge {
		pathClass = PathClassCrossTransportBridge
	} else if meta.usedRelay {
		pathClass = PathClassNativeRelay
	} else if meta.externalHops > 1 {
		pathClass = PathClassSameTransportForward
	}
	return RouteDecision{
		DestinationNodeID:  destinationNodeID,
		NextHopNodeID:      meta.firstHopNode,
		OutboundTransport:  meta.firstTransport,
		PathClass:          pathClass,
		EstimatedCost:      meta.cost,
		TopologyGeneration: generation,
	}
}

// ---------------- 优先队列（container/heap 接口） ----------------

// Len 返回队列长度，实现 heap.Interface。
func (q stateQueue) Len() int { return len(q) }

// Less 比较两个队列元素的优先级，按 betterMeta 排序（代价优先）。
// 实现 heap.Interface。
func (q stateQueue) Less(i, j int) bool {
	return betterMeta(q[i].meta, q[j].meta)
}

// Swap 交换两个队列元素的位置，并更新它们的 index，实现 heap.Interface。
func (q stateQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

// Push 向队尾添加元素，实现 heap.Interface。
func (q *stateQueue) Push(x any) {
	item := x.(*queueItem)
	item.index = len(*q)
	*q = append(*q, item)
}

// Pop 移除并返回队尾元素（堆的最后一个），实现 heap.Interface。
// 注意：heap.Pop 实际调用此方法前会先将堆顶交换到队尾。
func (q *stateQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*q = old[:n-1]
	return item
}
