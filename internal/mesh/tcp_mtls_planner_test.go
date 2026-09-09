package mesh

import "testing"

func TestPlannerTCPMTLSPreservesWSSOnlyDestination(t *testing.T) {
	snapshot := testSnapshotWithNodes(testNode(1, true, 1, true, TransportTCPMTLS, TransportWebSocket), testNode(2, true, 1, true, TransportTCPMTLS, TransportWebSocket), testNode(3, false, 1, true, TransportWebSocket))
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportTCPMTLS, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 20, Established: true},
		{FromNodeID: 2, ToNodeID: 3, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 20, Established: true},
	}
	p := NewPlanner(1)
	for _, class := range []TrafficClass{TrafficReplicationStream, TrafficSnapshotBulk} {
		d, ok := p.Compute(snapshot, 3, class, TransportUnspecified)
		if !ok || d.OutboundTransport != TransportWebSocket || d.NextHopNodeID != 2 {
			t.Fatalf("WSS-only target unreachable: %+v %t", d, ok)
		}
	}
}

func TestPlannerTCPMTLSDirectPreferredOverCheaperRelay(t *testing.T) {
	snapshot := testSnapshotWithNodes(testNode(1, true, 1, true, TransportTCPMTLS, TransportWebSocket), testNode(2, true, 1, true, TransportTCPMTLS, TransportWebSocket), testNode(3, false, 1, true, TransportWebSocket))
	snapshot.Links = []LinkState{
		{FromNodeID: 1, ToNodeID: 2, Transport: TransportTCPMTLS, PathClass: PathClassDirect, CostMs: 900, Established: true},
		{FromNodeID: 1, ToNodeID: 3, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{FromNodeID: 3, ToNodeID: 2, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
	}
	d, ok := NewPlanner(1).Compute(snapshot, 2, TrafficControlQuery, TransportUnspecified)
	if !ok || d.OutboundTransport != TransportTCPMTLS || d.NextHopNodeID != 2 {
		t.Fatalf("TCP preference lost to relay: %+v %t", d, ok)
	}
}

func TestPlannerTCPMTLSPriorityFallbackAndIngressPolicy(t *testing.T) {
	for _, healthy := range []bool{true, false, true} {
		snapshot := testSnapshotWithNodes(testNode(1, true, 1, true, TransportTCPMTLS, TransportWebSocket), testNode(2, true, 1, true, TransportTCPMTLS, TransportWebSocket))
		snapshot.Links = []LinkState{{FromNodeID: 1, ToNodeID: 2, Transport: TransportTCPMTLS, PathClass: PathClassDirect, CostMs: 900, Established: healthy}, {FromNodeID: 1, ToNodeID: 2, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true}}
		p := NewPlanner(1)
		for _, class := range []TrafficClass{TrafficControlCritical, TrafficControlQuery, TrafficTransientInteractive, TrafficReplicationStream, TrafficSnapshotBulk} {
			want := TransportWebSocket
			if healthy {
				want = TransportTCPMTLS
			}
			d, ok := p.Compute(snapshot, 2, class, TransportUnspecified)
			if !ok || d.OutboundTransport != want {
				t.Fatalf("healthy=%t class=%v route=%+v ok=%t", healthy, class, d, ok)
			}
		}
		// WSS 入站的复制流不能跨传输桥接；TCP 健康也不能抑制其合法 WSS 中继路径。
		d, ok := p.Compute(snapshot, 2, TrafficReplicationStream, TransportWebSocket)
		if !ok || d.OutboundTransport != TransportWebSocket {
			t.Fatalf("WSS replication transit broken: %+v %t", d, ok)
		}
	}
}
