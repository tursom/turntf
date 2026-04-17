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

func TestTopologyStoreApplyTopologyUpdateReplacesOriginStateOnSameGeneration(t *testing.T) {
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
	if len(node.TransportCaps) != 1 || node.TransportCaps[TransportZeroMQ] == nil {
		t.Fatalf("expected same-generation update to replace transports, got %+v", node.TransportCaps)
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionAllow {
		t.Fatalf("expected same-generation update to replace policy, got=%v", got)
	}
	if len(snapshot.Links) != 1 {
		t.Fatalf("expected same-generation update to replace links, got %+v", snapshot.Links)
	}
	if link := snapshot.Links[0]; link.ToNodeID != 6 || link.Transport != TransportZeroMQ {
		t.Fatalf("unexpected replacement link: %+v", link)
	}
}

func TestTopologyStoreApplyTopologyUpdateNilPolicyNormalizesDefaults(t *testing.T) {
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
	if got := node.ForwardingPolicy.NodeFeeWeight; got != 1 {
		t.Fatalf("expected nil policy to normalize to default fee weight, got=%d", got)
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionAllow {
		t.Fatalf("expected nil policy to normalize to default policy, got=%v", got)
	}
	if len(node.TransportCaps) != 0 {
		t.Fatalf("expected nil transports to clear prior transports, got %+v", node.TransportCaps)
	}
}

func TestTopologyStoreSnapshotClonesCapabilities(t *testing.T) {
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
	node.TransportCaps[TransportWebSocket].AdvertisedEndpoints[0] = "wss://mutated.example/ws"
	node.TransportCaps[TransportWebSocket].InboundEnabled = false

	refreshed := store.Snapshot()
	capability := refreshed.Nodes[1].TransportCaps[TransportWebSocket]
	if capability == nil {
		t.Fatal("expected websocket capability in refreshed snapshot")
	}
	if capability.AdvertisedEndpoints[0] != "wss://original.example/ws" {
		t.Fatalf("expected snapshot clone to protect stored endpoints, got %+v", capability.AdvertisedEndpoints)
	}
	if !capability.InboundEnabled {
		t.Fatalf("expected snapshot clone to protect stored capability, got %+v", capability)
	}
}
