package mesh

import (
	"container/heap"
)

// Planner 使用多源 Dijkstra 算法在 (nodeID, transport) 状态空间上计算路由。
// 初始化时需要本地节点 ID。
type Planner struct {
	localNodeID int64
}

type plannerState struct {
	nodeID    int64
	transport TransportKind
}

type plannerMeta struct {
	cost           int64
	usedBridge     bool
	usedRelay      bool
	externalHops   int
	firstHopNode   int64
	firstTransport TransportKind
}

type queueItem struct {
	state plannerState
	meta  plannerMeta
	index int
}

type stateQueue []*queueItem

// NewPlanner 创建一个以 localNodeID 为起点的 Planner。
func NewPlanner(localNodeID int64) *Planner {
	return &Planner{localNodeID: localNodeID}
}

// Compute 使用多源 Dijkstra 算法在 (nodeID, transport) 状态空间中计算最佳路径。
// 如未指定入站传输，则从所有启用了出站能力的本地传输开始搜索。
// 返回路由决策与是否可达。
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

type transition struct {
	state plannerState
	meta  plannerMeta
}

// expand 生成从当前状态出发的合法转移：
// 出站链路转移和跨传输桥接转移。
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

// canTransit 检查节点是否允许为指定流量类别中转流量。
// 目标和本地节点允许驻留但不允许中转。
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

// transitPenalty 计算中转代价值：NodeFeeWeight × TrafficClassFactor + 若策略为 Discourage 则加 DiscouragePenaltyMs。
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

// betterMeta 实现多级排序决胜局：
// 总代价 → 非桥接优先 → 非中继优先 → 跳数少 → 第一跳 ID 小 → 第一传输 ID 小。
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

// buildRouteDecision 根据规划器元数据设置 PathClass：
// 桥接 → CrossTransportBridge，中继 → NativeRelay，多跳 → SameTransportForward，否则 → Direct。
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

func (q stateQueue) Len() int { return len(q) }

func (q stateQueue) Less(i, j int) bool {
	return betterMeta(q[i].meta, q[j].meta)
}

func (q stateQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *stateQueue) Push(x any) {
	item := x.(*queueItem)
	item.index = len(*q)
	*q = append(*q, item)
}

func (q *stateQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*q = old[:n-1]
	return item
}
