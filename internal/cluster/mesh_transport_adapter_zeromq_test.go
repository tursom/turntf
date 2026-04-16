//go:build zeromq

package cluster

import (
	"context"
	"testing"

	"github.com/tursom/turntf/internal/mesh"
)

func TestZeroMQMeshTransportAdapterExchangesNodeHello(t *testing.T) {
	adapterA := NewZeroMQMeshTransportAdapter(Config{
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
			BindURL: nextZeroMQTCPAddress(t),
		},
	}, nil)
	adapterB := NewZeroMQMeshTransportAdapter(Config{
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
		},
	}, nil)
	if adapterA == nil || adapterB == nil {
		t.Fatal("expected zeromq mesh adapters")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer adapterA.Close()
	defer adapterB.Close()

	if err := adapterA.Start(ctx); err != nil {
		t.Fatalf("start zeromq mesh adapter A: %v", err)
	}
	if err := adapterB.Start(ctx); err != nil {
		t.Fatalf("start zeromq mesh adapter B: %v", err)
	}

	capabilityA := adapterA.LocalCapabilities()
	if capabilityA == nil || len(capabilityA.AdvertisedEndpoints) != 1 {
		t.Fatalf("unexpected zeromq local capability: %+v", capabilityA)
	}
	if capabilityA.AdvertisedEndpoints[0] != "zmq+"+adapterA.cfg.BindURL {
		t.Fatalf("unexpected zeromq advertised endpoint: %+v", capabilityA.AdvertisedEndpoints)
	}

	outboundConn, err := adapterB.Dial(ctx, capabilityA.AdvertisedEndpoints[0])
	if err != nil {
		t.Fatalf("dial zeromq mesh endpoint: %v", err)
	}
	defer outboundConn.Close()

	inboundConn := waitForAcceptedMeshTransport(t, adapterA.Accept())
	defer inboundConn.Close()

	if outboundConn.RemoteNodeHint() != capabilityA.AdvertisedEndpoints[0] {
		t.Fatalf("unexpected outbound zeromq remote node hint: got=%q want=%q", outboundConn.RemoteNodeHint(), capabilityA.AdvertisedEndpoints[0])
	}
	if inboundConn.RemoteNodeHint() == "" {
		t.Fatal("expected inbound zeromq remote node hint")
	}
	if outboundConn.Transport() != mesh.TransportZeroMQ || inboundConn.Transport() != mesh.TransportZeroMQ {
		t.Fatalf("unexpected zeromq transport kinds: outbound=%v inbound=%v", outboundConn.Transport(), inboundConn.Transport())
	}

	if err := sendMeshNodeHello(ctx, outboundConn, 202); err != nil {
		t.Fatalf("send outbound zeromq node hello: %v", err)
	}
	gotHello, err := receiveMeshNodeHello(ctx, inboundConn)
	if err != nil {
		t.Fatalf("receive inbound zeromq node hello: %v", err)
	}
	if gotHello.GetNodeId() != 202 {
		t.Fatalf("unexpected inbound zeromq node hello id: got=%d want=202", gotHello.GetNodeId())
	}

	if err := sendMeshNodeHello(ctx, inboundConn, 101); err != nil {
		t.Fatalf("send inbound zeromq node hello: %v", err)
	}
	gotReply, err := receiveMeshNodeHello(ctx, outboundConn)
	if err != nil {
		t.Fatalf("receive outbound zeromq node hello: %v", err)
	}
	if gotReply.GetNodeId() != 101 {
		t.Fatalf("unexpected outbound zeromq node hello id: got=%d want=101", gotReply.GetNodeId())
	}
}
