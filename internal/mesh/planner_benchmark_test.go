package mesh

import "testing"

// benchmarkPlannerSnapshot 包含线性多跳和超过临时缓冲容量的高邻接度拓扑。
// 在计时前建好邻接索引，与生产 MemoryTopologyStore 的只读快照保持一致。
func benchmarkPlannerSnapshot(branches, hops int) (TopologySnapshot, int64) {
	snapshot := TopologySnapshot{Nodes: make(map[int64]NodeState)}
	snapshot.Nodes[1] = testNode(1, false, 1, true, TransportWebSocket)
	target := int64(2 + branches*hops)
	snapshot.Nodes[target] = testNode(target, false, 1, true, TransportWebSocket)
	for branch := 0; branch < branches; branch++ {
		previous := int64(1)
		for hop := 0; hop < hops; hop++ {
			nodeID := int64(2 + branch*hops + hop)
			snapshot.Nodes[nodeID] = testNode(nodeID, false, 1, true, TransportWebSocket)
			snapshot.Links = append(snapshot.Links, LinkState{
				FromNodeID: previous, ToNodeID: nodeID, Transport: TransportWebSocket,
				PathClass: PathClassDirect, CostMs: 1, Established: true,
			})
			previous = nodeID
		}
		snapshot.Links = append(snapshot.Links, LinkState{
			FromNodeID: previous, ToNodeID: target, Transport: TransportWebSocket,
			PathClass: PathClassDirect, CostMs: 1, Established: true,
		})
	}
	snapshot.ensureOutgoingLinks()
	return snapshot, target
}

func BenchmarkPlannerCPU(b *testing.B) {
	for _, tc := range []struct {
		name     string
		branches int
		hops     int
	}{
		{name: "linear-3", branches: 1, hops: 1},
		{name: "linear-7", branches: 1, hops: 5},
		{name: "fanout-16", branches: 16, hops: 2},
	} {
		b.Run(tc.name, func(b *testing.B) {
			snapshot, target := benchmarkPlannerSnapshot(tc.branches, tc.hops)
			planner := NewPlanner(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decision, ok := planner.Compute(snapshot, target, TrafficTransientInteractive, TransportUnspecified)
				if !ok || decision.NextHopNodeID != 2 {
					b.Fatalf("unexpected route: ok=%v decision=%+v", ok, decision)
				}
			}
		})
	}
}

func TestPlannerHighFanoutThenDeadEnd(t *testing.T) {
	snapshot, target := benchmarkPlannerSnapshot(16, 2)
	planner := NewPlanner(1)
	decision, ok := planner.Compute(snapshot, target, TrafficTransientInteractive, TransportUnspecified)
	if !ok || decision.NextHopNodeID != 2 || decision.EstimatedCost != 15 {
		t.Fatalf("unexpected high-fanout route: ok=%v decision=%+v", ok, decision)
	}
	// 删除所有通往目标的链路，使每条分支最终都进入无出边的状态。
	// 复用临时列表时，这些状态必须产生空列表，不能重用上一状态的转移。
	links := make([]LinkState, 0, len(snapshot.Links))
	for _, link := range snapshot.Links {
		if link.ToNodeID != target {
			links = append(links, link)
		}
	}
	snapshot.Links = links
	snapshot.outgoingLinks = nil
	snapshot.ensureOutgoingLinks()
	if decision, ok := planner.Compute(snapshot, target, TrafficTransientInteractive, TransportUnspecified); ok {
		t.Fatalf("expected unreachable target after removing final edges, got %+v", decision)
	}
}
