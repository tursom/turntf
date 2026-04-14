//go:build zeromq

package cluster

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

func TestZeroMQTransportSendReceivePayload(t *testing.T) {
	t.Parallel()

	listener, peerURL := newZeroMQTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan TransportConn, 1)
	if err := listener.Start(ctx, func(conn TransportConn) {
		accepted <- conn
	}); err != nil {
		t.Fatalf("start zeromq listener: %v", err)
	}
	defer listener.Close()

	clientConn, err := newZeroMQDialer().Dial(ctx, peerURL)
	if err != nil {
		t.Fatalf("dial zeromq transport: %v", err)
	}
	defer clientConn.Close()

	clientPayload := []byte("client payload")
	if err := clientConn.Send(ctx, clientPayload); err != nil {
		t.Fatalf("send client payload: %v", err)
	}

	serverConn := waitForAcceptedTransport(t, accepted, nil)
	defer serverConn.Close()

	gotClientPayload, err := serverConn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive client payload: %v", err)
	}
	if !bytes.Equal(gotClientPayload, clientPayload) {
		t.Fatalf("unexpected client payload: got=%q want=%q", gotClientPayload, clientPayload)
	}

	serverPayload := []byte("server payload")
	if err := serverConn.Send(ctx, serverPayload); err != nil {
		t.Fatalf("send server payload: %v", err)
	}
	gotServerPayload, err := clientConn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive server payload: %v", err)
	}
	if !bytes.Equal(gotServerPayload, serverPayload) {
		t.Fatalf("unexpected server payload: got=%q want=%q", gotServerPayload, serverPayload)
	}
	if clientConn.Transport() != transportZeroMQ || serverConn.Transport() != transportZeroMQ {
		t.Fatalf("unexpected transport names: client=%q server=%q", clientConn.Transport(), serverConn.Transport())
	}
	if clientConn.Direction() != "outbound" || serverConn.Direction() != "inbound" {
		t.Fatalf("unexpected directions: client=%q server=%q", clientConn.Direction(), serverConn.Direction())
	}
}

func TestZeroMQTransportLargePayload(t *testing.T) {
	t.Parallel()

	listener, peerURL := newZeroMQTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan TransportConn, 1)
	if err := listener.Start(ctx, func(conn TransportConn) {
		accepted <- conn
	}); err != nil {
		t.Fatalf("start zeromq listener: %v", err)
	}
	defer listener.Close()

	clientConn, err := newZeroMQDialer().Dial(ctx, peerURL)
	if err != nil {
		t.Fatalf("dial zeromq transport: %v", err)
	}
	defer clientConn.Close()

	payload := bytes.Repeat([]byte("z"), 2<<20)
	if err := clientConn.Send(ctx, payload); err != nil {
		t.Fatalf("send large payload: %v", err)
	}

	serverConn := waitForAcceptedTransport(t, accepted, nil)
	defer serverConn.Close()

	gotPayload, err := serverConn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive large payload: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("large payload mismatch: got=%d want=%d", len(gotPayload), len(payload))
	}
}

func TestZeroMQTransportCloseStopsConnection(t *testing.T) {
	t.Parallel()

	listener, peerURL := newZeroMQTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan TransportConn, 1)
	if err := listener.Start(ctx, func(conn TransportConn) {
		accepted <- conn
	}); err != nil {
		t.Fatalf("start zeromq listener: %v", err)
	}
	defer listener.Close()

	clientConn, err := newZeroMQDialer().Dial(ctx, peerURL)
	if err != nil {
		t.Fatalf("dial zeromq transport: %v", err)
	}
	defer clientConn.Close()

	if err := clientConn.Send(ctx, []byte("hello")); err != nil {
		t.Fatalf("prime zeromq session: %v", err)
	}
	serverConn := waitForAcceptedTransport(t, accepted, nil)
	defer serverConn.Close()

	if err := clientConn.Close(); err != nil {
		t.Fatalf("close zeromq client: %v", err)
	}
	if _, err := clientConn.Receive(context.Background()); err == nil {
		t.Fatal("expected receive error after local close")
	}
	if err := clientConn.Send(context.Background(), []byte("later")); err == nil {
		t.Fatal("expected send error after local close")
	}
}

func TestZeroMQRouterDistributesIdentitiesToDistinctConnections(t *testing.T) {
	t.Parallel()

	listener, peerURL := newZeroMQTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan TransportConn, 4)
	if err := listener.Start(ctx, func(conn TransportConn) {
		accepted <- conn
	}); err != nil {
		t.Fatalf("start zeromq listener: %v", err)
	}
	defer listener.Close()

	clientA, err := newZeroMQDialer().Dial(ctx, peerURL)
	if err != nil {
		t.Fatalf("dial zeromq client A: %v", err)
	}
	defer clientA.Close()
	clientB, err := newZeroMQDialer().Dial(ctx, peerURL)
	if err != nil {
		t.Fatalf("dial zeromq client B: %v", err)
	}
	defer clientB.Close()

	if err := clientA.Send(ctx, []byte("from-a")); err != nil {
		t.Fatalf("send client A payload: %v", err)
	}
	if err := clientB.Send(ctx, []byte("from-b")); err != nil {
		t.Fatalf("send client B payload: %v", err)
	}

	connA := waitForAcceptedTransport(t, accepted, nil)
	connB := waitForAcceptedTransport(t, accepted, nil)
	defer connA.Close()
	defer connB.Close()

	if connA.RemoteAddr() == connB.RemoteAddr() {
		t.Fatalf("expected distinct router identities, got %q", connA.RemoteAddr())
	}

	payloadA, err := connA.Receive(ctx)
	if err != nil {
		t.Fatalf("receive payload from first router conn: %v", err)
	}
	payloadB, err := connB.Receive(ctx)
	if err != nil {
		t.Fatalf("receive payload from second router conn: %v", err)
	}
	if bytes.Equal(payloadA, payloadB) {
		t.Fatalf("expected distinct payloads per identity, got %q and %q", payloadA, payloadB)
	}
}

func TestManagerStartDialsZeroMQAndWebSocketStaticPeers(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: 128,
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
			BindURL: "tcp://127.0.0.1:9090",
		},
		Peers: []Peer{
			{URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
			{URL: "zmq+tcp://127.0.0.1:9091"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	wsDialer := &recordingDialer{urls: make(chan string, 4)}
	zmqDialer := &recordingDialer{urls: make(chan string, 4)}
	mgr.dialers[transportWebSocket] = wsDialer
	mgr.dialers[transportZeroMQ] = zmqDialer
	mgr.zeroMQListenerFactory = func(bindURL string) Listener {
		return &fakeListener{}
	}

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer mgr.Close()

	select {
	case rawURL := <-wsDialer.urls:
		if rawURL != "ws://127.0.0.1:9081/internal/cluster/ws" {
			t.Fatalf("unexpected websocket dial url: %q", rawURL)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected websocket static peer to start dialing")
	}

	select {
	case rawURL := <-zmqDialer.urls:
		if rawURL != "zmq+tcp://127.0.0.1:9091" {
			t.Fatalf("unexpected zeromq dial url: %q", rawURL)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected zeromq static peer to start dialing")
	}
}

type fakeListener struct{}

func (l *fakeListener) Start(context.Context, func(TransportConn)) error {
	return nil
}

func (l *fakeListener) Close() error {
	return nil
}

func newZeroMQTestListener(t *testing.T) (Listener, string) {
	t.Helper()

	addr := nextZeroMQTCPAddress(t)
	return newZeroMQListener(addr), "zmq+" + addr
}

func nextZeroMQTCPAddress(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for zeromq test port: %v", err)
	}
	defer ln.Close()
	return "tcp://" + ln.Addr().String()
}
