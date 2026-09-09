package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/tursom/turntf/internal/mesh"
)

func TestNewMeshTransportAdaptersIncludesOnlyEnabledTransports(t *testing.T) {
	t.Parallel()

	adapters := NewMeshTransportAdapters(Config{}, nil)
	if len(adapters) != 1 || adapters[0].Kind() != mesh.TransportWebSocket {
		t.Fatalf("unexpected default mesh adapters: %+v", adapters)
	}

	adapters = NewMeshTransportAdapters(Config{
		ZeroMQ: ZeroMQConfig{Enabled: true},
	}, nil)
	if len(adapters) != 2 || adapters[0].Kind() != mesh.TransportWebSocket || adapters[1].Kind() != mesh.TransportZeroMQ {
		t.Fatalf("unexpected websocket/zeromq adapters: %+v", adapters)
	}
}

func TestMeshInboundAdapterDialAndInjectLifecycle(t *testing.T) {
	t.Parallel()

	raw := &recordingTransportConn{transport: transportZeroMQ}
	adapter := newMeshInboundAdapter(mesh.TransportZeroMQ, &mesh.TransportCapability{
		Transport:       mesh.TransportZeroMQ,
		OutboundEnabled: true,
	}, func(context.Context, string) (TransportConn, error) {
		return raw, nil
	})
	if adapter.Kind() != mesh.TransportZeroMQ || adapter.Accept() == nil {
		t.Fatalf("unexpected inbound adapter identity: kind=%v accept=%v", adapter.Kind(), adapter.Accept())
	}
	capability := adapter.LocalCapabilities()
	capability.OutboundEnabled = false
	if !adapter.LocalCapabilities().OutboundEnabled {
		t.Fatal("caller mutated adapter capability")
	}
	if adapter.InjectInbound(raw) {
		t.Fatal("adapter accepted inbound connection before start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("start inbound adapter: %v", err)
	}
	dialed, err := adapter.Dial(context.Background(), "tcp://127.0.0.1:9090")
	if err != nil {
		t.Fatalf("dial through inbound adapter: %v", err)
	}
	if dialed.Transport() != mesh.TransportZeroMQ || dialed.RemoteNodeHint() != "tcp://127.0.0.1:9090" {
		t.Fatalf("unexpected dialed connection: transport=%v hint=%q", dialed.Transport(), dialed.RemoteNodeHint())
	}

	inbound := &recordingTransportConn{transport: transportZeroMQ}
	if !adapter.InjectInbound(inbound) {
		t.Fatal("started adapter rejected inbound connection")
	}
	if accepted := <-adapter.Accept(); accepted.Transport() != mesh.TransportZeroMQ {
		t.Fatalf("unexpected accepted transport: %v", accepted.Transport())
	}

	cancel()
	canceled := &recordingTransportConn{transport: transportZeroMQ}
	if adapter.InjectInbound(canceled) {
		t.Fatal("canceled adapter accepted inbound connection")
	}
	if got := canceled.closeReason(); got != "shutdown" {
		t.Fatalf("unexpected canceled connection close reason: %q", got)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("close inbound adapter: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("close inbound adapter again: %v", err)
	}
}

func TestEnqueueMeshTransportConnClosesWhenQueueIsFull(t *testing.T) {
	t.Parallel()

	acceptCh := make(chan mesh.TransportConn, 1)
	acceptCh <- wrapMeshTransportConn(mesh.TransportWebSocket, &recordingTransportConn{transport: transportWebSocket})
	rejected := &recordingTransportConn{transport: transportWebSocket}
	enqueueMeshTransportConn(context.Background(), acceptCh, mesh.TransportWebSocket, rejected)
	if got := rejected.closeReason(); got != "mesh accept queue full" {
		t.Fatalf("unexpected full queue close reason: %q", got)
	}
}

func TestEnqueueMeshTransportConnClosesWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	acceptCh := make(chan mesh.TransportConn, 1)
	rejected := &recordingTransportConn{transport: transportWebSocket}
	enqueueMeshTransportConn(ctx, acceptCh, mesh.TransportWebSocket, rejected)
	if got := rejected.closeReason(); got != "shutdown" {
		t.Fatalf("unexpected canceled enqueue close reason: %q", got)
	}
	select {
	case conn := <-acceptCh:
		_ = conn.Close()
		t.Fatal("canceled enqueue accepted a connection")
	default:
	}
}

func TestMeshTransportAdaptersRejectUnavailableOperations(t *testing.T) {
	t.Parallel()

	var libp2pAdapter *LibP2PMeshTransportAdapter
	if err := libp2pAdapter.Start(context.Background()); err != nil {
		t.Fatalf("start nil libp2p adapter: %v", err)
	}
	if _, err := libp2pAdapter.Dial(context.Background(), "endpoint"); err == nil {
		t.Fatal("expected nil libp2p adapter dial error")
	}
	if libp2pAdapter.Accept() != nil || libp2pAdapter.LocalCapabilities() != nil {
		t.Fatal("expected nil libp2p adapter accessors to return nil")
	}
	if err := libp2pAdapter.Close(); err != nil {
		t.Fatalf("close nil libp2p adapter: %v", err)
	}

	var zeroMQAdapter *ZeroMQMeshTransportAdapter
	if err := zeroMQAdapter.Start(context.Background()); err != nil {
		t.Fatalf("start nil zeromq adapter: %v", err)
	}
	if _, err := zeroMQAdapter.Dial(context.Background(), "endpoint"); err == nil {
		t.Fatal("expected nil zeromq adapter dial error")
	}
	if zeroMQAdapter.Accept() != nil || zeroMQAdapter.LocalCapabilities() != nil {
		t.Fatal("expected nil zeromq adapter accessors to return nil")
	}
	if err := zeroMQAdapter.Close(); err != nil {
		t.Fatalf("close nil zeromq adapter: %v", err)
	}

	noDialer := newMeshInboundAdapter(mesh.TransportWebSocket, nil, nil)
	if _, err := noDialer.Dial(context.Background(), "endpoint"); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected missing dialer error, got %v", err)
	}
}

func TestMeshTransportKindMapsLegacyTransports(t *testing.T) {
	t.Parallel()

	if got := meshTransportKind(nil); got != mesh.TransportUnspecified {
		t.Fatalf("unexpected nil transport kind: %v", got)
	}
	for legacy, want := range map[string]mesh.TransportKind{
		transportLibP2P:    mesh.TransportLibP2P,
		transportZeroMQ:    mesh.TransportZeroMQ,
		transportWebSocket: mesh.TransportUnspecified,
	} {
		if got := meshTransportKind(&recordingTransportConn{transport: legacy}); got != want {
			t.Fatalf("unexpected transport mapping for %q: got=%v want=%v", legacy, got, want)
		}
	}
}
