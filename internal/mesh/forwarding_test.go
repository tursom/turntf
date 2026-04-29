package mesh

import (
	"context"
	"errors"
	"testing"
)

type recordingSender struct {
	packet    *ForwardedPacket
	nextHop   int64
	transport TransportKind
	count     int
	err       error
}

func (s *recordingSender) SendPacket(_ context.Context, nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) error {
	if s.err != nil {
		return s.err
	}
	s.count++
	s.nextHop = nextHopNodeID
	s.transport = transport
	s.packet = cloneForwardedPacket(packet)
	return nil
}

type noopSender struct{}

func (noopSender) SendPacket(context.Context, int64, TransportKind, *ForwardedPacket) error {
	return nil
}

func testTransientForwardedPacket(packetID uint64, sourceNodeID, targetNodeID int64, ttlHops uint32) *ForwardedPacket {
	return &ForwardedPacket{
		PacketId:     packetID,
		SourceNodeId: sourceNodeID,
		TargetNodeId: targetNodeID,
		TrafficClass: TrafficTransientInteractive,
		TtlHops:      ttlHops,
		TransientPacket: &TransientPacket{
			PacketId:     packetID,
			SourceNodeId: sourceNodeID,
			TargetNodeId: targetNodeID,
			Body:         []byte("transient"),
		},
	}
}

func TestEngineDeliversLocalPacket(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyHello(1, &NodeHello{
		NodeId:           1,
		ProtocolVersion:  ProtocolVersion,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	})
	delivered := 0
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), &recordingSender{}, func(_ context.Context, packet *ForwardedPacket) error {
		delivered++
		if packet.TargetNodeId != 1 {
			t.Fatalf("unexpected target node: %d", packet.TargetNodeId)
		}
		return nil
	}, nil)

	err := engine.HandleInbound(context.Background(), &ForwardedPacket{
		PacketId:         1,
		SourceNodeId:     2,
		TargetNodeId:     1,
		TrafficClass:     TrafficControlCritical,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
		Payload:          []byte("control"),
	})
	if err != nil {
		t.Fatalf("deliver local packet: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("unexpected deliver count: %d", delivered)
	}
}

func TestEngineForwardsPacket(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2, 3} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId: 1,
		Generation:   1,
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})
	sender := &recordingSender{}
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), sender, nil, nil)

	err := engine.Forward(context.Background(), &ForwardedPacket{
		PacketId:     1,
		SourceNodeId: 1,
		TargetNodeId: 3,
		TrafficClass: TrafficControlCritical,
		TtlHops:      3,
		Payload:      []byte("control"),
	})
	if err != nil {
		t.Fatalf("forward packet: %v", err)
	}
	if sender.nextHop != 2 || sender.transport != TransportLibP2P {
		t.Fatalf("unexpected send target: next=%d transport=%v", sender.nextHop, sender.transport)
	}
	if sender.packet == nil || sender.packet.TtlHops != 2 || sender.packet.LastHopNodeId != 1 {
		t.Fatalf("unexpected forwarded packet: %+v", sender.packet)
	}
}

func TestEngineRejectsTransientPacketWithoutTypedBody(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyHello(1, &NodeHello{
		NodeId:           1,
		ProtocolVersion:  ProtocolVersion,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	})
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), noopSender{}, nil, nil)

	err := engine.HandleInbound(context.Background(), &ForwardedPacket{
		PacketId:         9,
		SourceNodeId:     2,
		TargetNodeId:     1,
		TrafficClass:     TrafficTransientInteractive,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
		Payload:          []byte("legacy-transient"),
	})
	if err == nil || err.Error() != "mesh: transient forwarded packet must carry transient_packet" {
		t.Fatalf("unexpected transient validation error: %v", err)
	}
}

func TestEngineRejectsNonTransientPacketWithTransientBody(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyHello(1, &NodeHello{
		NodeId:           1,
		ProtocolVersion:  ProtocolVersion,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	})
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), noopSender{}, nil, nil)

	err := engine.HandleInbound(context.Background(), &ForwardedPacket{
		PacketId:         10,
		SourceNodeId:     2,
		TargetNodeId:     1,
		TrafficClass:     TrafficControlCritical,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
		Payload:          []byte("query"),
		TransientPacket:  &TransientPacket{Body: []byte("unexpected")},
	})
	if err == nil || err.Error() != "mesh: non-transient forwarded packet must not carry transient_packet" {
		t.Fatalf("unexpected non-transient validation error: %v", err)
	}
}

func TestEngineRejectsImmediateLoop(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2, 3} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId: 1,
		Generation:   1,
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})

	engine := NewEngine(1, store.Snapshot, NewPlanner(1), &recordingSender{}, nil, nil)
	err := engine.HandleInbound(context.Background(), &ForwardedPacket{
		PacketId:         1,
		SourceNodeId:     9,
		TargetNodeId:     3,
		TrafficClass:     TrafficControlCritical,
		LastHopNodeId:    2,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
		Payload:          []byte("control"),
	})
	if !errors.Is(err, ErrLoopDetected) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEngineRejectsDuplicatePacket(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	store.ApplyHello(1, &NodeHello{
		NodeId:           1,
		ProtocolVersion:  ProtocolVersion,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
	})
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), &recordingSender{}, func(context.Context, *ForwardedPacket) error {
		return nil
	}, nil)
	packet := &ForwardedPacket{
		PacketId:         7,
		SourceNodeId:     2,
		TargetNodeId:     1,
		TrafficClass:     TrafficControlCritical,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
		Payload:          []byte("control"),
	}

	if err := engine.HandleInbound(context.Background(), packet); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	if err := engine.HandleInbound(context.Background(), packet); !errors.Is(err, ErrDuplicatePacket) {
		t.Fatalf("unexpected duplicate error: %v", err)
	}
}

func TestEngineDeduplicatesInboundTransitPacket(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2, 3} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})
	sender := &recordingSender{}
	engine := NewEngine(2, store.Snapshot, NewPlanner(2), sender, nil, nil)
	packet := testTransientForwardedPacket(42, 1, 3, 3)
	packet.IngressTransport = TransportLibP2P

	if err := engine.HandleInbound(context.Background(), packet); err != nil {
		t.Fatalf("first inbound forward failed: %v", err)
	}
	if err := engine.HandleInbound(context.Background(), packet); !errors.Is(err, ErrDuplicatePacket) {
		t.Fatalf("unexpected duplicate error: %v", err)
	}
	if sender.count != 1 {
		t.Fatalf("expected exactly one forwarded packet, got %d", sender.count)
	}
}

func TestEngineOutboundNoRouteDoesNotMarkPacketSeen(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	sender := &recordingSender{}
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), sender, nil, nil)
	packet := testTransientForwardedPacket(99, 1, 2, 3)

	if err := engine.Forward(context.Background(), packet); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected no route, got %v", err)
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     1,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})
	if err := engine.Forward(context.Background(), packet); err != nil {
		t.Fatalf("expected retry after route recovery to succeed, got %v", err)
	}
	if sender.count != 1 {
		t.Fatalf("expected one forwarded retry, got %d", sender.count)
	}
}

func BenchmarkMeshForwardingHotPath(b *testing.B) {
	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2, 3} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     1,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})

	snapshot := store.Snapshot()
	planner := NewPlanner(1)
	ctx := context.Background()

	b.Run("snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got := store.Snapshot()
			if _, ok := got.Node(1); !ok {
				b.Fatal("expected local node in snapshot")
			}
		}
	})

	b.Run("planner", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			decision, ok := planner.Compute(snapshot, 3, TrafficControlCritical, TransportUnspecified)
			if !ok || decision.NextHopNodeID != 2 {
				b.Fatalf("unexpected planner decision: ok=%v decision=%+v", ok, decision)
			}
		}
	})

	b.Run("forward", func(b *testing.B) {
		b.ReportAllocs()
		engine := NewEngine(1, store.Snapshot, planner, noopSender{}, nil, nil)
		packet := &ForwardedPacket{
			SourceNodeId: 1,
			TargetNodeId: 3,
			TrafficClass: TrafficControlCritical,
			TtlHops:      3,
			Payload:      []byte("bench"),
		}
		for i := 0; i < b.N; i++ {
			packet.PacketId = uint64(i + 1)
			if err := engine.Forward(ctx, packet); err != nil {
				b.Fatalf("forward: %v", err)
			}
		}
	})
}

func TestEngineOutboundDuplicateDoesNotResend(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     1,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})
	sender := &recordingSender{}
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), sender, nil, nil)
	packet := testTransientForwardedPacket(100, 1, 2, 3)

	if err := engine.Forward(context.Background(), packet); err != nil {
		t.Fatalf("first forward failed: %v", err)
	}
	if err := engine.Forward(context.Background(), packet); !errors.Is(err, ErrDuplicatePacket) {
		t.Fatalf("expected duplicate packet error, got %v", err)
	}
	if sender.count != 1 {
		t.Fatalf("expected duplicate outbound packet to be suppressed before send, got %d sends", sender.count)
	}
}

func TestEngineOutboundSendFailureAllowsRetry(t *testing.T) {
	t.Parallel()

	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2} {
		store.ApplyHello(nodeID, &NodeHello{
			NodeId:           nodeID,
			ProtocolVersion:  ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1),
			Transports: []*TransportCapability{
				{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
			},
		})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     1,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, OutboundEnabled: true, InboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})
	sender := &recordingSender{err: ErrNoRoute}
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), sender, nil, nil)
	packet := testTransientForwardedPacket(101, 1, 2, 3)

	if err := engine.Forward(context.Background(), packet); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected first forward to fail with no route, got %v", err)
	}
	sender.err = nil
	if err := engine.Forward(context.Background(), packet); err != nil {
		t.Fatalf("expected retry after send failure to succeed, got %v", err)
	}
	if sender.count != 1 {
		t.Fatalf("expected one successful send after retry, got %d", sender.count)
	}
}
