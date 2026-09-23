package mesh

import (
	"context"
	"testing"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func TestTracedPacketKeepsIdentityAndRecordsObservedHop(t *testing.T) {
	store := NewMemoryTopologyStore()
	for _, nodeID := range []int64{1, 2} {
		store.ApplyHello(nodeID, &NodeHello{NodeId: nodeID, ProtocolVersion: ProtocolVersion,
			ForwardingPolicy: DefaultForwardingPolicy(1), Transports: []*TransportCapability{
				{Transport: TransportWebSocket, InboundEnabled: true, OutboundEnabled: true},
			}})
	}
	store.ApplyTopologyUpdate(&TopologyUpdate{OriginNodeId: 1, Generation: 1,
		ForwardingPolicy: DefaultForwardingPolicy(1), Transports: []*TransportCapability{
			{Transport: TransportWebSocket, InboundEnabled: true, OutboundEnabled: true},
		}, Links: []*LinkAdvertisement{
			{FromNodeId: 1, ToNodeId: 2, Transport: TransportWebSocket, PathClass: PathClassDirect, CostMs: 2, Established: true},
		}})
	var observations []ForwardingObservation
	sender := &recordingSender{}
	engine := NewEngine(1, store.Snapshot, NewPlanner(1), sender, nil, func(value ForwardingObservation) {
		observations = append(observations, value)
	})
	packet := &ForwardedPacket{PacketId: 7, SourceNodeId: 1, SourceRuntimeEpoch: 8,
		TargetNodeId: 2, TraceId: "aabbccddeeff00112233445566778899", TrafficClass: TrafficTransientInteractive,
		TtlHops: 4, TransientPacket: &TransientPacket{Body: []byte("payload")}}
	if err := engine.Forward(context.Background(), packet); err != nil {
		t.Fatal(err)
	}
	if sender.packet == nil || sender.packet.TraceId != packet.TraceId || sender.packet.SourceRuntimeEpoch != packet.SourceRuntimeEpoch {
		t.Fatalf("trace not propagated: %+v", sender.packet)
	}
	if len(observations) != 1 || observations[0].Stage != "forwarded" || observations[0].NextHopNodeID != 2 ||
		observations[0].TraceID != packet.TraceId || observations[0].OutboundTransport != TransportWebSocket {
		t.Fatalf("unexpected observed hop: %+v", observations)
	}
	packet.TraceId = ""
	if err := engine.Forward(context.Background(), packet); err == nil {
		t.Fatal("duplicate packet accepted")
	}
}

func TestRouteProbeRejectsBusinessPayloadAndPreservesMarker(t *testing.T) {
	probe := &ForwardedPacket{PacketId: 9, SourceNodeId: 1, TargetNodeId: 2, TtlHops: 4,
		TrafficClass: TrafficTransientInteractive, TraceId: "aabbccddeeff00112233445566778899",
		RouteProbe: true, TransientPacket: &TransientPacket{}}
	if err := validateForwardedPacket(probe); err != nil {
		t.Fatal(err)
	}
	if clone := cloneForwardedPacket(probe); !clone.GetRouteProbe() {
		t.Fatal("route probe marker lost during forwarding clone")
	}
	for _, invalid := range []*ForwardedPacket{
		{TrafficClass: TrafficTransientInteractive, TraceId: probe.TraceId, RouteProbe: true,
			TransientPacket: &TransientPacket{Body: []byte("business")}},
		{TrafficClass: TrafficTransientInteractive, TraceId: probe.TraceId, RouteProbe: true,
			TransientPacket: &TransientPacket{Recipient: &internalproto.ClusterUserRef{NodeId: 2, UserId: 1}}},
		{TrafficClass: TrafficControlQuery, TraceId: probe.TraceId, RouteProbe: true, Payload: []byte("query")},
	} {
		if err := validateForwardedPacket(invalid); err == nil {
			t.Fatalf("business or control packet accepted as a probe: %+v", invalid)
		}
	}
}
