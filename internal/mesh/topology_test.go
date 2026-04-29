package mesh

import "testing"

func TestTopologyStoreApplyTopologyUpdateStoresPolicyTransportsAndLinks(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	policy := DefaultForwardingPolicy(9)
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     42,
		Generation:       7,
		ForwardingPolicy: policy,
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true, OutboundEnabled: true, AdvertisedEndpoints: []string{"wss://mesh.example/ws"}},
			{Transport: TransportLibP2P, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 42, ToNodeId: 7, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 15, JitterMs: 4, Established: true},
		},
	})

	snapshot := store.Snapshot()
	if snapshot.TopologyGeneration != 7 {
		t.Fatalf("unexpected topology generation: got=%d want=7", snapshot.TopologyGeneration)
	}
	node, ok := snapshot.Node(42)
	if !ok {
		t.Fatal("expected origin node in snapshot")
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionDeny {
		t.Fatalf("unexpected replication disposition: got=%v want=%v", got, DispositionDeny)
	}
	websocketCap := node.TransportCaps[TransportWebSocket]
	if websocketCap == nil {
		t.Fatal("expected websocket transport capability in snapshot")
	}
	if len(websocketCap.AdvertisedEndpoints) != 1 || websocketCap.AdvertisedEndpoints[0] != "wss://mesh.example/ws" {
		t.Fatalf("unexpected websocket endpoints: %+v", websocketCap.AdvertisedEndpoints)
	}
	if len(snapshot.Links) != 1 {
		t.Fatalf("unexpected link count: got=%d want=1", len(snapshot.Links))
	}
	link := snapshot.Links[0]
	if link.OriginNodeID != 42 || link.Transport != TransportWebSocket || !link.Established {
		t.Fatalf("unexpected link state: %+v", link)
	}
}

func TestTopologyStoreApplyTopologyUpdateIgnoresStaleGeneration(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     9,
		Generation:       5,
		ForwardingPolicy: DefaultForwardingPolicy(9),
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 9, ToNodeId: 1, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 30, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     9,
		Generation:       4,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportZeroMQ, InboundEnabled: true, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 9, ToNodeId: 2, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})

	snapshot := store.Snapshot()
	node, ok := snapshot.Node(9)
	if !ok {
		t.Fatal("expected origin node in snapshot")
	}
	if node.TransportCaps[TransportWebSocket] == nil {
		t.Fatal("expected stale update to preserve websocket capability")
	}
	if node.TransportCaps[TransportZeroMQ] != nil {
		t.Fatalf("expected stale update to be ignored, got zeromq capability: %+v", node.TransportCaps[TransportZeroMQ])
	}
	if len(snapshot.Links) != 1 || snapshot.Links[0].ToNodeID != 1 {
		t.Fatalf("unexpected links after stale update: %+v", snapshot.Links)
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionDeny {
		t.Fatalf("expected stale update to preserve high-fee policy, got=%v", got)
	}
}

func TestTopologyStoreRejectsNonOriginLinks(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     9,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 9, ToNodeId: 10, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 10, Established: true},
			{FromNodeId: 42, ToNodeId: 10, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})

	snapshot := store.Snapshot()
	if len(snapshot.Links) != 1 {
		t.Fatalf("expected only origin-owned links, got %+v", snapshot.Links)
	}
	link := snapshot.Links[0]
	if link.FromNodeID != 9 || link.ToNodeID != 10 {
		t.Fatalf("unexpected link accepted: %+v", link)
	}
}

func TestTopologyStoreApplyTopologyUpdateRejectsConflictingSameGeneration(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     3,
		Generation:       8,
		ForwardingPolicy: DefaultForwardingPolicy(9),
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true},
			{Transport: TransportLibP2P, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 3, ToNodeId: 4, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 12, Established: true},
			{FromNodeId: 3, ToNodeId: 5, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 20, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     3,
		Generation:       8,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportZeroMQ, InboundEnabled: true, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 3, ToNodeId: 6, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 6, Established: true},
		},
	})

	snapshot := store.Snapshot()
	node, ok := snapshot.Node(3)
	if !ok {
		t.Fatal("expected origin node in snapshot")
	}
	if len(node.TransportCaps) != 2 || node.TransportCaps[TransportWebSocket] == nil || node.TransportCaps[TransportLibP2P] == nil {
		t.Fatalf("expected conflicting same-generation update to be ignored, got %+v", node.TransportCaps)
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionDeny {
		t.Fatalf("expected conflicting same-generation update to preserve policy, got=%v", got)
	}
	if len(snapshot.Links) != 2 {
		t.Fatalf("expected conflicting same-generation update to preserve links, got %+v", snapshot.Links)
	}
	if !hasTopologyLink(snapshot.Links, 4, TransportWebSocket) || !hasTopologyLink(snapshot.Links, 5, TransportLibP2P) {
		t.Fatalf("unexpected preserved links after conflicting same-generation update: %+v", snapshot.Links)
	}
}

func TestTopologyStoreApplyTopologyUpdateNilPolicySameGenerationIsIgnored(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     5,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(9),
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId: 5,
		Generation:   1,
	})

	snapshot := store.Snapshot()
	node, ok := snapshot.Node(5)
	if !ok {
		t.Fatal("expected origin node in snapshot")
	}
	if got := node.ForwardingPolicy.NodeFeeWeight; got != 9 {
		t.Fatalf("expected same-generation nil policy update to preserve prior fee weight, got=%d", got)
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionDeny {
		t.Fatalf("expected same-generation nil policy update to preserve prior policy, got=%v", got)
	}
	if len(node.TransportCaps) != 1 || node.TransportCaps[TransportWebSocket] == nil {
		t.Fatalf("expected same-generation nil transport update to preserve prior transports, got %+v", node.TransportCaps)
	}
}

func TestTopologyStoreSnapshotRebuildsAuthoritativeStateAfterUpdate(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId: 1,
		Generation:   2,
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true, AdvertisedEndpoints: []string{"wss://original.example/ws"}},
		},
	})

	snapshot := store.Snapshot()
	node, ok := snapshot.Node(1)
	if !ok {
		t.Fatal("expected origin node in snapshot")
	}
	// Returned snapshots are read-only views. Mutating them must not be
	// treated as supported API, but the next authoritative rebuild should
	// still recover from accidental caller-side mutation.
	node.TransportCaps[TransportWebSocket].AdvertisedEndpoints[0] = "wss://mutated.example/ws"
	node.TransportCaps[TransportWebSocket].InboundEnabled = false
	store.ApplyHello(1, &NodeHello{
		NodeId:           1,
		ProtocolVersion:  ProtocolVersion,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true, AdvertisedEndpoints: []string{"wss://original.example/ws"}},
		},
	})

	refreshed := store.Snapshot()
	capability := refreshed.Nodes[1].TransportCaps[TransportWebSocket]
	if capability == nil {
		t.Fatal("expected websocket capability after snapshot rebuild")
	}
	if capability.AdvertisedEndpoints[0] != "wss://original.example/ws" {
		t.Fatalf("expected snapshot rebuild to restore stored endpoints, got %+v", capability.AdvertisedEndpoints)
	}
	if !capability.InboundEnabled {
		t.Fatalf("expected snapshot rebuild to restore stored capability, got %+v", capability)
	}
}

func TestTopologyStoreSnapshotIndexesOutgoingLinks(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     1,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true},
			{Transport: TransportZeroMQ, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 10, Established: true},
			{FromNodeId: 1, ToNodeId: 3, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 20, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 4, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 30, Established: true},
		},
	})

	snapshot := store.Snapshot()
	if len(snapshot.Links) != 3 {
		t.Fatalf("unexpected link count: got=%d want=3", len(snapshot.Links))
	}
	assertOutgoingLinks(t, snapshot, 1, TransportLibP2P, 2)
	assertOutgoingLinks(t, snapshot, 1, TransportZeroMQ, 3)
	assertOutgoingLinks(t, snapshot, 2, TransportLibP2P, 4)
	if links := snapshot.outgoing(2, TransportZeroMQ); len(links) != 0 {
		t.Fatalf("expected no zeromq outgoing links for node 2, got %+v", links)
	}
}

func TestTopologySnapshotEnsureOutgoingLinksIndexesManualSnapshot(t *testing.T) {
	t.Parallel()

	snapshot := TopologySnapshot{
		Links: []LinkState{
			{FromNodeID: 1, ToNodeID: 2, Transport: TransportLibP2P, Established: true},
			{FromNodeID: 1, ToNodeID: 3, Transport: TransportLibP2P, Established: false},
			{FromNodeID: 1, ToNodeID: 4, Transport: TransportWebSocket, Established: true},
			{FromNodeID: 2, ToNodeID: 5, Transport: TransportLibP2P, Established: true},
		},
	}

	snapshot.ensureOutgoingLinks()
	assertOutgoingLinks(t, snapshot, 1, TransportLibP2P, 2, 3)
	assertOutgoingLinks(t, snapshot, 1, TransportWebSocket, 4)
	assertOutgoingLinks(t, snapshot, 2, TransportLibP2P, 5)
}

func assertOutgoingLinks(t *testing.T, snapshot TopologySnapshot, nodeID int64, transport TransportKind, toNodeIDs ...int64) {
	t.Helper()
	links := snapshot.outgoing(nodeID, transport)
	if len(links) != len(toNodeIDs) {
		t.Fatalf("unexpected outgoing link count for node=%d transport=%v: got=%d want=%d links=%+v", nodeID, transport, len(links), len(toNodeIDs), links)
	}
	for _, toNodeID := range toNodeIDs {
		if !hasTopologyLink(links, toNodeID, transport) {
			t.Fatalf("expected outgoing link node=%d transport=%v to=%d, got %+v", nodeID, transport, toNodeID, links)
		}
	}
}

func hasTopologyLink(links []LinkState, toNodeID int64, transport TransportKind) bool {
	for _, link := range links {
		if link.ToNodeID == toNodeID && link.Transport == transport {
			return true
		}
	}
	return false
}
