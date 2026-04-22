package cluster

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
)

func TestMeshEnvelopeAuthenticatorSignsAndVerifies(t *testing.T) {
	t.Parallel()

	authenticator := newMeshEnvelopeAuthenticator("mesh-secret")
	if authenticator == nil {
		t.Fatal("expected mesh authenticator")
	}

	tests := []struct {
		name     string
		envelope *mesh.ClusterEnvelope
	}{
		{
			name: "hello",
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_NodeHello{
				NodeHello: &mesh.NodeHello{
					NodeId:           1,
					ProtocolVersion:  mesh.ProtocolVersion,
					ForwardingPolicy: mesh.DefaultForwardingPolicy(1),
					Transports: []*mesh.TransportCapability{
						{Transport: mesh.TransportWebSocket, InboundEnabled: true, OutboundEnabled: true},
					},
				},
			}},
		},
		{
			name: "topology",
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_TopologyUpdate{
				TopologyUpdate: &mesh.TopologyUpdate{
					OriginNodeId:     2,
					Generation:       9,
					ForwardingPolicy: mesh.DefaultForwardingPolicy(1),
					Transports: []*mesh.TransportCapability{
						{Transport: mesh.TransportLibP2P, InboundEnabled: true, OutboundEnabled: true},
					},
					Links: []*mesh.LinkAdvertisement{
						{FromNodeId: 2, ToNodeId: 3, Transport: mesh.TransportLibP2P, Established: true},
					},
				},
			}},
		},
		{
			name: "forwarded packet",
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ForwardedPacket{
				ForwardedPacket: &mesh.ForwardedPacket{
					PacketId:     7,
					SourceNodeId: 1,
					TargetNodeId: 3,
					TrafficClass: mesh.TrafficControlQuery,
					TtlHops:      mesh.DefaultTTLHops,
					Payload:      []byte("mesh-payload"),
				},
			}},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			signed, err := authenticator.Sign(tc.envelope, nil)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}

			var decoded mesh.ClusterEnvelope
			if err := proto.Unmarshal(signed, &decoded); err != nil {
				t.Fatalf("unmarshal signed envelope: %v", err)
			}
			if len(decoded.GetHmac()) == 0 {
				t.Fatal("expected signed mesh envelope hmac")
			}
			if err := authenticator.Verify(&decoded, signed); err != nil {
				t.Fatalf("verify: %v", err)
			}

			other := newMeshEnvelopeAuthenticator("different-secret")
			if err := other.Verify(&decoded, signed); err == nil {
				t.Fatal("expected verification with different secret to fail")
			}
		})
	}
}
