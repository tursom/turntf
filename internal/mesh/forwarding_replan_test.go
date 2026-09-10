package mesh

import (
	"context"
	"errors"
	"testing"
)

func loopReplanTopology() TopologySnapshot {
	snapshot := TopologySnapshot{Nodes: make(map[int64]NodeState), Links: []LinkState{
		{OriginNodeID: 2, FromNodeID: 2, ToNodeID: 1, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{OriginNodeID: 1, FromNodeID: 1, ToNodeID: 3, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{OriginNodeID: 2, FromNodeID: 2, ToNodeID: 3, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 50, Established: true},
		// Removing only 2->1 still leaves the tempting detour 2->4->1->3.
		{OriginNodeID: 2, FromNodeID: 2, ToNodeID: 4, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
		{OriginNodeID: 4, FromNodeID: 4, ToNodeID: 1, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 1, Established: true},
	}}
	for _, id := range []int64{1, 2, 3, 4} {
		snapshot.Nodes[id] = NodeState{NodeID: id, ForwardingPolicy: DefaultForwardingPolicy(id), TransportCaps: map[TransportKind]*TransportCapability{TransportWebSocket: {Transport: TransportWebSocket, OutboundEnabled: true, InboundEnabled: true}}}
	}
	snapshot.ensureOutgoingLinks()
	return snapshot
}

func TestEngineReplanKeepsPolicyAndTTL(t *testing.T) {
	for _, mode := range []string{"no_alternative", "ttl", "transit_denied", "bridge_denied"} {
		t.Run(mode, func(t *testing.T) {
			s := loopReplanTopology()
			ttl := uint32(4)
			want := ErrLoopDetected
			switch mode {
			case "no_alternative":
				s.Links = s.Links[:2]
			case "ttl":
				ttl = 1
				want = ErrTTLExceeded
			case "transit_denied":
				s.Nodes[2].ForwardingPolicy.TransitEnabled = false
				want = ErrNoRoute
			case "bridge_denied":
				s.Links[2].Transport = TransportTCPMTLS
				s.Nodes[2].TransportCaps[TransportTCPMTLS] = &TransportCapability{Transport: TransportTCPMTLS, OutboundEnabled: true, InboundEnabled: true}
				s.Nodes[3].TransportCaps[TransportTCPMTLS] = &TransportCapability{Transport: TransportTCPMTLS, OutboundEnabled: true, InboundEnabled: true}
				s.Nodes[2].ForwardingPolicy.BridgeEnabled = false
			}
			s.outgoingLinks = nil
			sender := &recordingSender{}
			e := NewEngine(2, func() TopologySnapshot { return s }, NewPlanner(2), sender, nil, nil)
			err := e.HandleInbound(context.Background(), &ForwardedPacket{SourceNodeId: 1, LastHopNodeId: 1, TargetNodeId: 3, PacketId: 10, TrafficClass: TrafficControlQuery, IngressTransport: TransportWebSocket, TtlHops: ttl, Payload: []byte("query")})
			if !errors.Is(err, want) || sender.count != 0 {
				t.Fatalf("policy bypass: %v sends=%d", err, sender.count)
			}
		})
	}
}

func TestEngineReplanAvoidsIndirectReturn(t *testing.T) {
	s := loopReplanTopology()
	s.Links = s.Links[1:]
	s.outgoingLinks = nil
	planner := NewPlanner(2)
	before, ok := planner.Compute(s, 3, TrafficControlQuery, TransportWebSocket)
	if !ok || before.NextHopNodeID != 4 {
		t.Fatal("fixture must return through a third node")
	}
	sender := &recordingSender{}
	e := NewEngine(2, func() TopologySnapshot { return s }, planner, sender, nil, nil)
	if err := e.HandleInbound(context.Background(), &ForwardedPacket{SourceNodeId: 1, LastHopNodeId: 1, TargetNodeId: 3, PacketId: 11, TrafficClass: TrafficControlQuery, IngressTransport: TransportWebSocket, TtlHops: 4, Payload: []byte("query")}); err != nil {
		t.Fatal(err)
	}
	if sender.nextHop != 3 {
		t.Fatalf("route still visits source indirectly: %d", sender.nextHop)
	}
}

func TestEngineReplansInsteadOfReturningToPreviousHop(t *testing.T) {
	snapshot := loopReplanTopology()
	planner := NewPlanner(2)
	route, ok := planner.Compute(snapshot, 3, TrafficControlQuery, TransportWebSocket)
	if !ok || route.NextHopNodeID != 1 {
		t.Fatal("fixture must prefer returning to sender")
	}
	sender := &recordingSender{}
	var observation ForwardingObservation
	engine := NewEngine(2, func() TopologySnapshot { return snapshot }, planner, sender, nil, func(o ForwardingObservation) { observation = o })
	packet := &ForwardedPacket{SourceNodeId: 1, LastHopNodeId: 1, TargetNodeId: 3, PacketId: 9, TrafficClass: TrafficControlQuery, IngressTransport: TransportWebSocket, TtlHops: 4, Payload: []byte("query")}
	if err := engine.HandleInbound(context.Background(), packet); err != nil {
		t.Fatal(err)
	}
	if sender.nextHop != 3 || sender.packet.TtlHops != 3 || sender.packet.LastHopNodeId != 2 {
		t.Fatalf("invalid alternative: %+v", sender)
	}
	if !observation.LoopAvoided || observation.LastHopNodeID != 1 || observation.NextHopNodeID != 3 || observation.PacketID != 9 {
		t.Fatalf("missing recovery observation: %+v", observation)
	}
	route, ok = planner.Compute(snapshot, 3, TrafficControlQuery, TransportWebSocket)
	if !ok || route.NextHopNodeID != 1 {
		t.Fatal("replan mutated shared topology")
	}
}
