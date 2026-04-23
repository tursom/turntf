package mesh

import (
	"container/heap"
)

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
	path           []plannerState
}

type queueItem struct {
	state plannerState
	meta  plannerMeta
	index int
}

type stateQueue []*queueItem

func NewPlanner(localNodeID int64) *Planner {
	return &Planner{localNodeID: localNodeID}
}

func (p *Planner) Compute(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind) (RouteDecision, bool) {
	if p == nil || p.localNodeID <= 0 || destinationNodeID <= 0 {
		return RouteDecision{}, false
	}
	snapshot.ensureOutgoingLinks()
	localNode, ok := snapshot.Node(p.localNodeID)
	if !ok {
		return RouteDecision{}, false
	}
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
	best := make(map[plannerState]plannerMeta)
	pq := make(stateQueue, 0, len(starts))
	for _, start := range starts {
		meta := plannerMeta{
			path: []plannerState{start},
		}
		best[start] = meta
		heap.Push(&pq, &queueItem{state: start, meta: meta})
	}

	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*queueItem)
		currentBest, ok := best[item.state]
		if !ok || currentBest.cost != item.meta.cost || currentBest.firstHopNode != item.meta.firstHopNode || currentBest.firstTransport != item.meta.firstTransport {
			continue
		}
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

func (p *Planner) expand(snapshot TopologySnapshot, destinationNodeID int64, trafficClass TrafficClass, ingressTransport TransportKind, current plannerState, meta plannerMeta) []transition {
	transitions := make([]transition, 0)
	if !p.canTransit(snapshot, destinationNodeID, trafficClass, ingressTransport, current.nodeID) {
		return transitions
	}
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
		nextMeta.path = appendPath(meta.path, nextState)
		transitions = append(transitions, transition{state: nextState, meta: nextMeta})
	}
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
		nextMeta.path = appendPath(meta.path, nextState)
		transitions = append(transitions, transition{state: nextState, meta: nextMeta})
	}
	return transitions
}

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

func appendPath(path []plannerState, next plannerState) []plannerState {
	cloned := make([]plannerState, len(path)+1)
	copy(cloned, path)
	cloned[len(path)] = next
	return cloned
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
