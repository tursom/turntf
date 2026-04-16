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
}

func (s *recordingSender) SendPacket(_ context.Context, nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) error {
	s.nextHop = nextHopNodeID
	s.transport = transport
	s.packet = cloneForwardedPacket(packet)
	return nil
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
	})

	err := engine.HandleInbound(context.Background(), &ForwardedPacket{
		PacketId:         1,
		SourceNodeId:     2,
		TargetNodeId:     1,
		TrafficClass:     TrafficControlCritical,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
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
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	})
	sender := &recordingSender{}
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), sender, nil)

	err := engine.Forward(context.Background(), &ForwardedPacket{
		PacketId:     1,
		SourceNodeId: 1,
		TargetNodeId: 3,
		TrafficClass: TrafficControlCritical,
		TtlHops:      3,
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
		Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})
	store.ApplyTopologyUpdate(&TopologyUpdate{
		OriginNodeId:     2,
		Generation:       1,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})

	engine := NewEngine(1, store.Snapshot, NewPlanner(1), &recordingSender{}, nil)
	err := engine.HandleInbound(context.Background(), &ForwardedPacket{
		PacketId:         1,
		SourceNodeId:     9,
		TargetNodeId:     3,
		TrafficClass:     TrafficControlCritical,
		LastHopNodeId:    2,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
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
	})
	packet := &ForwardedPacket{
		PacketId:         7,
		SourceNodeId:     2,
		TargetNodeId:     1,
		TrafficClass:     TrafficControlCritical,
		IngressTransport: TransportLibP2P,
		TtlHops:          3,
	}

	if err := engine.HandleInbound(context.Background(), packet); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	if err := engine.HandleInbound(context.Background(), packet); !errors.Is(err, ErrDuplicatePacket) {
		t.Fatalf("unexpected duplicate error: %v", err)
	}
}
