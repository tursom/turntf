//go:build zeromq

package cluster

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/store"
)

func TestZeroMQTransportSendReceivePayload(t *testing.T) {
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

func TestManagerStartOutboundOnlyZeroMQSkipsListener(t *testing.T) {
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: 128,
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
		},
		Peers: []Peer{
			{URL: "zmq+tcp://127.0.0.1:9091"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	zmqDialer := &recordingDialer{urls: make(chan string, 4)}
	listenerStarted := false
	mgr.dialers[transportZeroMQ] = zmqDialer
	mgr.zeroMQListenerFactory = func(bindURL string) Listener {
		listenerStarted = true
		return &fakeListener{}
	}

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer mgr.Close()

	select {
	case rawURL := <-zmqDialer.urls:
		if rawURL != "zmq+tcp://127.0.0.1:9091" {
			t.Fatalf("unexpected zeromq dial url: %q", rawURL)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected zeromq static peer to start dialing")
	}

	if listenerStarted {
		t.Fatal("expected outbound-only zeromq mode to skip listener startup")
	}
}

func TestZeroMQReplicationAndLateJoinerCatchup(t *testing.T) {
	t.Run("realtime-replication", func(t *testing.T) {
		nodeA, nodeB := newZeroMQTestNodePair(t)

		startClusterTestNodes(t, nodeA, nodeB)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, nodeB.id),
			expectClusterPeer(nodeB, nodeA.id),
		)

		user, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "alice-zmq",
			PasswordHash: "hash-1",
		})
		if err != nil {
			t.Fatalf("create user: %v", err)
		}

		userKey := user.Key()
		waitForReplicatedUser(t, 5*time.Second, nodeB, userKey)

		if _, _, err := nodeA.service.CreateMessage(context.Background(), store.CreateMessageParams{
			UserKey: userKey,
			Sender:  clusterSenderKey(1, store.BootstrapAdminUserID),
			Body:    []byte("replicated over zeromq"),
		}); err != nil {
			t.Fatalf("create message: %v", err)
		}

		waitForMessagesEqual(t, 5*time.Second, nodeB, userKey, 10, "replicated over zeromq")
	})

	t.Run("late-joiner-catchup", func(t *testing.T) {
		nodeA, nodeB := newZeroMQTestNodePair(t)

		nodeA.Start(t)

		user, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "offline-zmq",
			PasswordHash: "hash-1",
		})
		if err != nil {
			t.Fatalf("create offline user: %v", err)
		}
		if _, _, err := nodeA.store.CreateMessage(context.Background(), store.CreateMessageParams{
			UserKey: user.Key(),
			Sender:  clusterSenderKey(9, 1),
			Body:    []byte("caught up over zeromq"),
		}); err != nil {
			t.Fatalf("create offline message: %v", err)
		}

		nodeB.Start(t)
		waitForReplicatedUser(t, 5*time.Second, nodeB, user.Key())
		waitForMessagesEqual(t, 5*time.Second, nodeB, user.Key(), 10, "caught up over zeromq")
	})
}

func TestMixedWebSocketAndZeroMQDiscoveryRoutes(t *testing.T) {
	nodeAHTTP := mustListen(t)
	nodeBHTTP := mustListen(t)
	nodeCHTTP := mustListen(t)
	nodeBBind := nextZeroMQTCPAddress(t)
	nodeCBind := nextZeroMQTCPAddress(t)

	nodeA := newClusterTestNodeFixtureWithListener(t, clusterTestNodeConfig{
		Name:              "node-a",
		Slot:              1,
		Peers:             []Peer{{URL: wsURL(nodeBHTTP)}},
		ZeroMQEnabled:     true,
		DiscoveryEnabled:  true,
		MessageWindowSize: store.DefaultMessageWindowSize,
	}, nodeAHTTP)
	nodeB := newClusterTestNodeFixtureWithListener(t, clusterTestNodeConfig{
		Name:              "node-b",
		Slot:              2,
		Peers:             []Peer{{URL: wsURL(nodeAHTTP)}, {URL: "zmq+" + nodeCBind}},
		ZeroMQEnabled:     true,
		ZeroMQBindURL:     nodeBBind,
		DiscoveryEnabled:  true,
		MessageWindowSize: store.DefaultMessageWindowSize,
	}, nodeBHTTP)
	nodeC := newClusterTestNodeFixtureWithListener(t, clusterTestNodeConfig{
		Name:              "node-c",
		Slot:              3,
		Peers:             []Peer{{URL: "zmq+" + nodeBBind}},
		ZeroMQEnabled:     true,
		ZeroMQBindURL:     nodeCBind,
		DiscoveryEnabled:  true,
		MessageWindowSize: store.DefaultMessageWindowSize,
	}, nodeCHTTP)

	startClusterTestNodes(t, nodeA, nodeB, nodeC)
	waitForPeersActive(t, 10*time.Second,
		expectClusterPeer(nodeA, nodeB.id),
		expectClusterPeer(nodeB, nodeC.id),
	)
	waitForClusterRoutes(t, 15*time.Second, nodeA, nodeC.id)

	user, _, err := nodeA.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "mixed-topology",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create mixed-topology user: %v", err)
	}
	waitForReplicatedUser(t, 10*time.Second, nodeC, user.Key())

	eventually(t, 15*time.Second, func() bool {
		statusA, err := nodeA.manager.Status(context.Background())
		if err != nil {
			return false
		}
		statusB, err := nodeB.manager.Status(context.Background())
		if err != nil {
			return false
		}

		hasWebSocketHop := false
		for _, peer := range statusA.Peers {
			if peer.NodeID == nodeB.id && peer.Transport == transportWebSocket {
				hasWebSocketHop = true
				break
			}
		}
		if !hasWebSocketHop || statusA.Discovery.ZeroMQMode != "outbound_only" {
			return false
		}

		for _, peer := range statusB.Peers {
			if peer.NodeID == nodeC.id && peer.Transport == transportZeroMQ {
				return true
			}
		}
		return false
	})
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

func newZeroMQTestNodePair(t *testing.T) (*clusterTestNode, *clusterTestNode) {
	t.Helper()

	nodeABind := nextZeroMQTCPAddress(t)
	nodeBBind := nextZeroMQTCPAddress(t)
	nodeA := newClusterTestNodeFixture(t, clusterTestNodeConfig{
		Name:              "node-a",
		Slot:              1,
		Peers:             []Peer{{URL: "zmq+" + nodeBBind}},
		ZeroMQEnabled:     true,
		ZeroMQBindURL:     nodeABind,
		MessageWindowSize: store.DefaultMessageWindowSize,
	})
	nodeB := newClusterTestNodeFixture(t, clusterTestNodeConfig{
		Name:              "node-b",
		Slot:              2,
		ZeroMQEnabled:     true,
		ZeroMQBindURL:     nodeBBind,
		MessageWindowSize: store.DefaultMessageWindowSize,
	})
	return nodeA, nodeB
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
