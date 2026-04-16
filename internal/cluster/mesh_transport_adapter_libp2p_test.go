package cluster

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
)

func TestLibP2PMeshTransportAdapterExchangesNodeHello(t *testing.T) {
	t.Parallel()

	adapterA := NewLibP2PMeshTransportAdapter(Config{
		ClusterSecret: "mesh-adapter-secret",
		LibP2P: LibP2PConfig{
			Enabled:            true,
			PrivateKeyPath:     filepath.Join(t.TempDir(), "node-a.key"),
			ListenAddrs:        []string{"/ip4/127.0.0.1/tcp/0"},
			EnableDHT:          false,
			EnableMDNS:         false,
			EnableHolePunching: false,
			GossipSubEnabled:   false,
		},
	})
	adapterB := NewLibP2PMeshTransportAdapter(Config{
		ClusterSecret: "mesh-adapter-secret",
		LibP2P: LibP2PConfig{
			Enabled:            true,
			PrivateKeyPath:     filepath.Join(t.TempDir(), "node-b.key"),
			ListenAddrs:        []string{"/ip4/127.0.0.1/tcp/0"},
			EnableDHT:          false,
			EnableMDNS:         false,
			EnableHolePunching: false,
			GossipSubEnabled:   false,
		},
	})
	if adapterA == nil || adapterB == nil {
		t.Fatal("expected libp2p mesh adapters")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer adapterA.Close()
	defer adapterB.Close()

	if err := adapterA.Start(ctx); err != nil {
		t.Fatalf("start libp2p mesh adapter A: %v", err)
	}
	if err := adapterB.Start(ctx); err != nil {
		t.Fatalf("start libp2p mesh adapter B: %v", err)
	}

	capabilityA := adapterA.LocalCapabilities()
	if capabilityA == nil || len(capabilityA.AdvertisedEndpoints) != 1 {
		t.Fatalf("unexpected libp2p local capability: %+v", capabilityA)
	}
	if !strings.Contains(capabilityA.AdvertisedEndpoints[0], "/p2p/") {
		t.Fatalf("expected started libp2p capability to advertise peer id: %+v", capabilityA.AdvertisedEndpoints)
	}

	outboundConn, err := adapterB.Dial(ctx, capabilityA.AdvertisedEndpoints[0])
	if err != nil {
		t.Fatalf("dial libp2p mesh endpoint: %v", err)
	}
	defer outboundConn.Close()

	wantHint := libP2PPeerIDFromAddr(capabilityA.AdvertisedEndpoints[0])
	if outboundConn.RemoteNodeHint() != wantHint {
		t.Fatalf("unexpected outbound remote node hint: got=%q want=%q", outboundConn.RemoteNodeHint(), wantHint)
	}

	if err := sendMeshNodeHello(ctx, outboundConn, 22); err != nil {
		t.Fatalf("send outbound node hello: %v", err)
	}
	inboundConn := waitForAcceptedMeshTransport(t, adapterA.Accept())
	defer inboundConn.Close()
	if inboundConn.RemoteNodeHint() == "" {
		t.Fatal("expected inbound libp2p remote node hint")
	}
	if outboundConn.Transport() != mesh.TransportLibP2P || inboundConn.Transport() != mesh.TransportLibP2P {
		t.Fatalf("unexpected libp2p transport kinds: outbound=%v inbound=%v", outboundConn.Transport(), inboundConn.Transport())
	}
	gotHello, err := receiveMeshNodeHello(ctx, inboundConn)
	if err != nil {
		t.Fatalf("receive inbound node hello: %v", err)
	}
	if gotHello.GetNodeId() != 22 {
		t.Fatalf("unexpected inbound node hello id: got=%d want=22", gotHello.GetNodeId())
	}

	if err := sendMeshNodeHello(ctx, inboundConn, 11); err != nil {
		t.Fatalf("send inbound node hello: %v", err)
	}
	gotReply, err := receiveMeshNodeHello(ctx, outboundConn)
	if err != nil {
		t.Fatalf("receive outbound node hello: %v", err)
	}
	if gotReply.GetNodeId() != 11 {
		t.Fatalf("unexpected outbound node hello id: got=%d want=11", gotReply.GetNodeId())
	}
}

func TestMeshTransportAdapterCapabilitiesAreCloned(t *testing.T) {
	t.Parallel()

	libp2pAdapter := NewLibP2PMeshTransportAdapter(Config{
		ClusterSecret: "secret",
		LibP2P: LibP2PConfig{
			Enabled:        true,
			PrivateKeyPath: filepath.Join(t.TempDir(), "node.key"),
			ListenAddrs:    []string{"/ip4/127.0.0.1/tcp/0"},
		},
	})
	if libp2pAdapter == nil {
		t.Fatal("expected libp2p adapter")
	}
	libp2pCapability := libp2pAdapter.LocalCapabilities()
	libp2pCapability.AdvertisedEndpoints[0] = "changed"
	if got := libp2pAdapter.LocalCapabilities().AdvertisedEndpoints[0]; got == "changed" {
		t.Fatal("expected libp2p capability clone")
	}

	zeroMQAdapter := NewZeroMQMeshTransportAdapter(Config{
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
			BindURL: "tcp://127.0.0.1:9090",
		},
	}, nil)
	if zeroMQAdapter == nil {
		t.Fatal("expected zeromq adapter")
	}
	zeroMQCapability := zeroMQAdapter.LocalCapabilities()
	zeroMQCapability.AdvertisedEndpoints[0] = "changed"
	if got := zeroMQAdapter.LocalCapabilities().AdvertisedEndpoints[0]; got == "changed" {
		t.Fatal("expected zeromq capability clone")
	}
}

func sendMeshNodeHello(ctx context.Context, conn mesh.TransportConn, nodeID int64) error {
	data, err := proto.Marshal(&mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_NodeHello{
			NodeHello: &mesh.NodeHello{
				NodeId:          nodeID,
				ProtocolVersion: mesh.ProtocolVersion,
			},
		},
	})
	if err != nil {
		return err
	}
	return conn.Send(ctx, data)
}

func receiveMeshNodeHello(ctx context.Context, conn mesh.TransportConn) (*mesh.NodeHello, error) {
	data, err := conn.Receive(ctx)
	if err != nil {
		return nil, err
	}
	var envelope mesh.ClusterEnvelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return envelope.GetNodeHello(), nil
}

func waitForAcceptedMeshTransport(t *testing.T, accepted <-chan mesh.TransportConn) mesh.TransportConn {
	t.Helper()

	select {
	case conn := <-accepted:
		return conn
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for accepted mesh transport connection")
		return nil
	}
}
