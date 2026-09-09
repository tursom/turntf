package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tursom/turntf/internal/mesh"
)

func TestWebSocketMeshTransportAdapterRejectsBeforeStart(t *testing.T) {
	t.Parallel()

	adapter := NewWebSocketMeshTransportAdapter(Config{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/cluster/ws", nil)

	adapter.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected response status: got=%d want=%d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestWebSocketMeshTransportAdapterExchangesNodeHello(t *testing.T) {
	t.Parallel()

	adapterA := NewWebSocketMeshTransportAdapter(Config{AdvertisePath: "/cluster/ws"})
	adapterB := NewWebSocketMeshTransportAdapter(Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer adapterA.Close()
	defer adapterB.Close()

	if err := adapterA.Start(ctx); err != nil {
		t.Fatalf("start websocket mesh adapter A: %v", err)
	}
	if err := adapterB.Start(ctx); err != nil {
		t.Fatalf("start websocket mesh adapter B: %v", err)
	}

	capability := adapterA.LocalCapabilities()
	if capability == nil || capability.Transport != mesh.TransportWebSocket || !capability.InboundEnabled || !capability.OutboundEnabled {
		t.Fatalf("unexpected websocket capability: %+v", capability)
	}
	if len(capability.AdvertisedEndpoints) != 1 || capability.AdvertisedEndpoints[0] != "/cluster/ws" {
		t.Fatalf("unexpected websocket advertised endpoints: %+v", capability.AdvertisedEndpoints)
	}
	capability.AdvertisedEndpoints[0] = "changed"
	if got := adapterA.LocalCapabilities().AdvertisedEndpoints[0]; got != "/cluster/ws" {
		t.Fatalf("websocket capability was mutated through caller-owned result: got=%q", got)
	}

	server := newIPv4WebSocketTestServer(t, adapterA.Handler())
	defer server.Close()
	endpoint := websocketURL(server.URL) + "/cluster/ws"
	outboundConn, err := adapterB.Dial(ctx, endpoint)
	if err != nil {
		t.Fatalf("dial websocket mesh endpoint: %v", err)
	}
	defer outboundConn.Close()

	inboundConn := waitForAcceptedMeshTransport(t, adapterA.Accept())
	defer inboundConn.Close()
	if outboundConn.Transport() != mesh.TransportWebSocket || inboundConn.Transport() != mesh.TransportWebSocket {
		t.Fatalf("unexpected websocket transport kinds: outbound=%v inbound=%v", outboundConn.Transport(), inboundConn.Transport())
	}
	if outboundConn.RemoteNodeHint() != endpoint {
		t.Fatalf("unexpected outbound remote node hint: got=%q want=%q", outboundConn.RemoteNodeHint(), endpoint)
	}
	if inboundConn.RemoteNodeHint() == "" {
		t.Fatal("expected inbound websocket remote node hint")
	}

	if err := sendMeshNodeHello(ctx, outboundConn, 22); err != nil {
		t.Fatalf("send outbound node hello: %v", err)
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

	if err := adapterA.Close(); err != nil {
		t.Fatalf("close websocket adapter: %v", err)
	}
	if err := adapterA.Close(); err != nil {
		t.Fatalf("close websocket adapter again: %v", err)
	}
}

func TestWebSocketMeshTransportAdapterStopsAcceptingAfterContextCancellation(t *testing.T) {
	t.Parallel()

	adapter := NewWebSocketMeshTransportAdapter(Config{})
	ctx, cancel := context.WithCancel(context.Background())
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("start websocket mesh adapter: %v", err)
	}
	cancel()

	server := newIPv4WebSocketTestServer(t, adapter.Handler())
	defer server.Close()
	client, err := adapter.Dial(context.Background(), websocketURL(server.URL))
	if err == nil {
		_ = client.Close()
		t.Fatal("expected cancelled adapter to reject the websocket upgrade")
	}
	select {
	case conn := <-adapter.Accept():
		_ = conn.Close()
		t.Fatal("cancelled adapter accepted an inbound connection")
	default:
	}
}
