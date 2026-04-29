package cluster

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
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

func TestMeshEnvelopeAuthenticatorVerifyAcceptsLegacyAndAppendedHMACFrames(t *testing.T) {
	t.Parallel()

	authenticator := newMeshEnvelopeAuthenticator("mesh-secret")
	if authenticator == nil {
		t.Fatal("expected mesh authenticator")
	}

	tests := []struct {
		name     string
		bodyTag  protowire.Number
		envelope *mesh.ClusterEnvelope
	}{
		{
			name:    "forwarded_packet",
			bodyTag: 7,
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ForwardedPacket{
				ForwardedPacket: &mesh.ForwardedPacket{
					PacketId:     1,
					SourceNodeId: 11,
					TargetNodeId: 12,
					TrafficClass: mesh.TrafficTransientInteractive,
					TtlHops:      mesh.DefaultTTLHops,
					Payload:      []byte("fwd"),
				},
			}},
		},
		{
			name:    "replication_batch",
			bodyTag: 8,
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ReplicationBatch{
				ReplicationBatch: &mesh.ReplicationBatch{
					OriginNodeId: 21,
					Sequence:     22,
					SentAtHlc:    "1000-1",
				},
			}},
		},
		{
			name:    "replication_ack",
			bodyTag: 13,
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ReplicationAck{
				ReplicationAck: &mesh.ReplicationAck{
					Ack: &internalproto.Ack{
						NodeId:       31,
						OriginNodeId: 32,
						AckedEventId: 33,
					},
				},
			}},
		},
		{
			name:    "membership_update",
			bodyTag: 15,
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_MembershipUpdate{
				MembershipUpdate: &mesh.MembershipUpdate{
					MembershipUpdate: &internalproto.MembershipUpdate{
						OriginNodeId: 41,
						Generation:   42,
					},
				},
			}},
		},
		{
			name:    "presence_update",
			bodyTag: 16,
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_PresenceUpdate{
				PresenceUpdate: &mesh.MeshPresenceUpdate{
					PresenceUpdate: &internalproto.OnlinePresenceSnapshot{
						OriginNodeId: 51,
						Generation: 52,
					},
				},
			}},
		},
		{
			name:    "connectivity_rumor",
			bodyTag: 17,
			envelope: &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ConnectivityRumor{
				ConnectivityRumor: &mesh.MeshConnectivityRumor{
					ConnectivityRumor: &internalproto.NodeConnectivityRumor{
						TargetNodeId:         61,
						TargetRuntimeEpoch:   62,
						ReporterNodeId:       63,
						ReporterRuntimeEpoch: 64,
						ObservedAtMs:         65,
					},
				},
			}},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			appended, err := authenticator.Sign(tc.envelope, nil)
			if err != nil {
				t.Fatalf("sign appended frame: %v", err)
			}
			legacy, err := legacySignedMeshEnvelope(authenticator, tc.envelope)
			if err != nil {
				t.Fatalf("sign legacy frame: %v", err)
			}

			appendedHMACPos := positionOfMeshField(t, appended, meshEnvelopeHMACFieldNumber)
			appendedBodyPos := positionOfMeshField(t, appended, tc.bodyTag)
			if appendedHMACPos <= appendedBodyPos {
				t.Fatalf("expected appended hmac after body tag %d: hmac=%d body=%d", tc.bodyTag, appendedHMACPos, appendedBodyPos)
			}

			legacyHMACPos := positionOfMeshField(t, legacy, meshEnvelopeHMACFieldNumber)
			legacyBodyPos := positionOfMeshField(t, legacy, tc.bodyTag)
			if tc.bodyTag > meshEnvelopeHMACFieldNumber {
				if legacyHMACPos >= legacyBodyPos {
					t.Fatalf("expected legacy hmac before body tag %d: hmac=%d body=%d", tc.bodyTag, legacyHMACPos, legacyBodyPos)
				}
			}

			for _, raw := range [][]byte{appended, legacy} {
				var decoded mesh.ClusterEnvelope
				if err := proto.Unmarshal(raw, &decoded); err != nil {
					t.Fatalf("unmarshal signed frame: %v", err)
				}
				if err := authenticator.Verify(&decoded, raw); err != nil {
					t.Fatalf("verify signed frame: %v", err)
				}
			}
		})
	}
}

func TestMeshEnvelopeAuthenticatorVerifyRejectsMalformedHMACFrames(t *testing.T) {
	t.Parallel()

	authenticator := newMeshEnvelopeAuthenticator("mesh-secret")
	if authenticator == nil {
		t.Fatal("expected mesh authenticator")
	}

	envelope := &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ForwardedPacket{
		ForwardedPacket: &mesh.ForwardedPacket{
			PacketId:     71,
			SourceNodeId: 72,
			TargetNodeId: 73,
			TrafficClass: mesh.TrafficTransientInteractive,
			TtlHops:      mesh.DefaultTTLHops,
			Payload:      []byte("mesh"),
		},
	}}

	encoded, err := meshEnvelopeBytes(envelope)
	if err != nil {
		t.Fatalf("mesh envelope bytes: %v", err)
	}
	signed, err := authenticator.Sign(envelope, encoded)
	if err != nil {
		t.Fatalf("sign envelope: %v", err)
	}

	tests := []struct {
		name string
		raw  []byte
	}{
		{
			name: "duplicate_hmac",
			raw: append(
				append([]byte(nil), signed...),
				protowire.AppendBytes(protowire.AppendTag(nil, meshEnvelopeHMACFieldNumber, protowire.BytesType), []byte("dup"))...,
			),
		},
		{
			name: "empty_hmac",
			raw: protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), encoded...), meshEnvelopeHMACFieldNumber, protowire.BytesType), nil),
		},
		{
			name: "wrong_wire_type",
			raw: protowire.AppendVarint(protowire.AppendTag(append([]byte(nil), encoded...), meshEnvelopeHMACFieldNumber, protowire.VarintType), 1),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var decoded mesh.ClusterEnvelope
			if err := proto.Unmarshal(tc.raw, &decoded); err != nil {
				t.Fatalf("unmarshal malformed frame: %v", err)
			}
			if err := authenticator.Verify(&decoded, tc.raw); err == nil {
				t.Fatal("expected malformed frame verification failure")
			}
		})
	}
}

func BenchmarkMeshEnvelopeAuthenticatorSignVerify(b *testing.B) {
	authenticator := newMeshEnvelopeAuthenticator("mesh-secret")
	if authenticator == nil {
		b.Fatal("expected mesh authenticator")
	}
	envelope := &mesh.ClusterEnvelope{Body: &mesh.ClusterEnvelope_ConnectivityRumor{
		ConnectivityRumor: &mesh.MeshConnectivityRumor{
			ConnectivityRumor: &internalproto.NodeConnectivityRumor{
				TargetNodeId:         81,
				TargetRuntimeEpoch:   82,
				ReporterNodeId:       83,
				ReporterRuntimeEpoch: 84,
				ObservedAtMs:         85,
				Reason:               "bench",
			},
		},
	}}
	encoded, err := meshEnvelopeBytes(envelope)
	if err != nil {
		b.Fatalf("mesh envelope bytes: %v", err)
	}
	signed, err := authenticator.Sign(envelope, encoded)
	if err != nil {
		b.Fatalf("sign envelope: %v", err)
	}
	var decoded mesh.ClusterEnvelope
	if err := proto.Unmarshal(signed, &decoded); err != nil {
		b.Fatalf("unmarshal signed envelope: %v", err)
	}

	b.Run("sign", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := authenticator.Sign(envelope, encoded); err != nil {
				b.Fatalf("sign: %v", err)
			}
		}
	})

	b.Run("verify", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := authenticator.Verify(&decoded, signed); err != nil {
				b.Fatalf("verify: %v", err)
			}
		}
	})
}

func legacySignedMeshEnvelope(authenticator *meshEnvelopeAuthenticator, envelope *mesh.ClusterEnvelope) ([]byte, error) {
	if authenticator == nil {
		return nil, errors.New("mesh authenticator cannot be nil")
	}
	clone, ok := proto.Clone(envelope).(*mesh.ClusterEnvelope)
	if !ok {
		return nil, errors.New("clone mesh envelope")
	}
	signature, err := authenticator.signatureFor(clone)
	if err != nil {
		return nil, err
	}
	clone.Hmac = signature
	return meshMarshalOptions.Marshal(clone)
}

func positionOfMeshField(t testing.TB, raw []byte, fieldNum protowire.Number) int {
	t.Helper()
	for offset := 0; len(raw) > 0; {
		currentOffset := offset
		gotField, wireType, tagLen := protowire.ConsumeTag(raw)
		if tagLen < 0 {
			t.Fatalf("consume tag: %v", protowire.ParseError(tagLen))
		}
		fieldLen := protowire.ConsumeFieldValue(gotField, wireType, raw[tagLen:])
		if fieldLen < 0 {
			t.Fatalf("consume field %d: %v", gotField, protowire.ParseError(fieldLen))
		}
		totalLen := tagLen + fieldLen
		if gotField == fieldNum {
			return currentOffset
		}
		raw = raw[totalLen:]
		offset += totalLen
	}
	t.Fatalf("field %d not found", fieldNum)
	return -1
}
