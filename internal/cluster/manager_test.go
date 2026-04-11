package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"notifier/internal/api"
	internalproto "notifier/internal/proto"
	"notifier/internal/store"

	"google.golang.org/protobuf/proto"
)

func TestActivateSessionPrefersExpectedDirection(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:        "node-a",
		NodeSlot:      1,
		ListenAddr:    "127.0.0.1:9080",
		AdvertiseAddr: "ws://127.0.0.1:9080/internal/cluster/ws",
		ClusterSecret: "secret",
		Peers: []Peer{
			{NodeID: "node-b", URL: "ws://127.0.0.1:9999/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	inbound := &session{manager: mgr, conn: nil, peerID: "node-b", outbound: false, send: make(chan *internalproto.Envelope, 1)}
	outbound := &session{manager: mgr, conn: nil, peerID: "node-b", outbound: true, send: make(chan *internalproto.Envelope, 1)}

	if !mgr.activateSession(inbound) {
		t.Fatalf("expected first session to activate")
	}
	if !mgr.activateSession(outbound) {
		t.Fatalf("expected preferred outbound session to replace inbound")
	}

	mgr.mu.Lock()
	active := mgr.peers["node-b"].active
	mgr.mu.Unlock()
	if active != outbound {
		t.Fatalf("expected outbound session to be active")
	}
}

func TestHandleHelloRejectsInvalidHandshake(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)

	tests := []struct {
		name             string
		session          *session
		hello            *internalproto.Hello
		envelopeNodeID   string
		wantActivePeerID string
	}{
		{
			name: "unknown peer",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:          "node-c",
				AdvertiseAddr:   "ws://127.0.0.1:9082/internal/cluster/ws",
				ProtocolVersion: internalproto.ProtocolVersion,
			},
			envelopeNodeID: "node-c",
		},
		{
			name: "protocol mismatch",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:          "node-b",
				AdvertiseAddr:   "ws://127.0.0.1:9081/internal/cluster/ws",
				ProtocolVersion: "v0",
			},
			envelopeNodeID: "node-b",
		},
		{
			name: "self peer",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:          "node-a",
				AdvertiseAddr:   "ws://127.0.0.1:9080/internal/cluster/ws",
				ProtocolVersion: internalproto.ProtocolVersion,
			},
			envelopeNodeID: "node-a",
		},
		{
			name: "outbound peer mismatch",
			session: &session{
				manager:          mgr,
				outbound:         true,
				configuredPeerID: "node-c",
				send:             make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:          "node-b",
				AdvertiseAddr:   "ws://127.0.0.1:9081/internal/cluster/ws",
				ProtocolVersion: internalproto.ProtocolVersion,
			},
			envelopeNodeID: "node-b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envelope := mustHelloEnvelope(t, tt.envelopeNodeID, tt.hello)
			if err := mgr.handleHello(tt.session, envelope); err == nil {
				t.Fatalf("expected handshake error")
			}
			if mgr.hasActivePeer("node-b") {
				t.Fatalf("expected invalid handshake to leave peer inactive")
			}
		})
	}
}

func TestHandleEventBatchSendsAckAfterSuccessfulApply(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	user, event, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Alice"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	payload, err := proto.Marshal(&internalproto.EventBatch{
		Events: []*internalproto.ReplicatedEvent{store.ToReplicatedEvent(event)},
	})
	if err != nil {
		t.Fatalf("marshal event batch: %v", err)
	}

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	envelope := &internalproto.Envelope{
		NodeId:      "node-a",
		Sequence:    uint64(event.Sequence),
		SentAtHlc:   event.HLC.String(),
		MessageType: string(internalproto.MessageTypeEventBatch),
		Payload:     payload,
	}

	if err := mgr.handleEventBatch(sess, envelope); err != nil {
		t.Fatalf("handle event batch: %v", err)
	}

	replicatedUser, err := targetStore.GetUser(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("expected replicated user: %v", err)
	}
	if replicatedUser.Username != user.Username {
		t.Fatalf("unexpected replicated user: %+v", replicatedUser)
	}

	select {
	case ackEnvelope := <-sess.send:
		if ackEnvelope.NodeId != "node-b" {
			t.Fatalf("unexpected ack envelope node id: %s", ackEnvelope.NodeId)
		}
		if ackEnvelope.MessageType != string(internalproto.MessageTypeAck) {
			t.Fatalf("unexpected ack envelope type: %s", ackEnvelope.MessageType)
		}

		var ack internalproto.Ack
		if err := proto.Unmarshal(ackEnvelope.Payload, &ack); err != nil {
			t.Fatalf("unmarshal ack: %v", err)
		}
		if ack.NodeId != "node-b" {
			t.Fatalf("unexpected ack node id: %s", ack.NodeId)
		}
		if ack.AckedSequence != uint64(event.Sequence) {
			t.Fatalf("unexpected ack sequence: got=%d want=%d", ack.AckedSequence, event.Sequence)
		}
	default:
		t.Fatalf("expected ack to be enqueued")
	}
}

func TestHandleEventBatchDoesNotAckFailedApply(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	payload, err := proto.Marshal(&internalproto.EventBatch{
		Events: []*internalproto.ReplicatedEvent{nil},
	})
	if err != nil {
		t.Fatalf("marshal event batch: %v", err)
	}

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	envelope := &internalproto.Envelope{
		NodeId:      "node-a",
		Sequence:    42,
		SentAtHlc:   "2026-01-01T00:00:00.000000000Z/2/1",
		MessageType: string(internalproto.MessageTypeEventBatch),
		Payload:     payload,
	}

	if err := mgr.handleEventBatch(sess, envelope); err == nil {
		t.Fatalf("expected failed apply to return error")
	}

	select {
	case ackEnvelope := <-sess.send:
		t.Fatalf("did not expect ack after failed apply: %+v", ackEnvelope)
	default:
	}
}

func TestTwoNodeReplicationOverWebSocket(t *testing.T) {
	t.Parallel()

	clusterLnA := mustListen(t)
	clusterLnB := mustListen(t)
	apiLnA := mustListen(t)
	apiLnB := mustListen(t)

	clusterURLA := wsURL(clusterLnA)
	clusterURLB := wsURL(clusterLnB)

	nodeA := newClusterTestNode(t, "node-a", 1, clusterLnA, apiLnA, clusterURLA, []Peer{
		{NodeID: "node-b", URL: clusterURLB},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, clusterLnB, apiLnB, clusterURLB, []Peer{
		{NodeID: "node-a", URL: clusterURLA},
	})

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeB.activePeer("node-a")
	})

	createUserBody := map[string]any{
		"username":      "alice",
		"password_hash": "hash-1",
		"profile": map[string]any{
			"display_name": "Alice",
		},
	}
	var createdUser struct {
		ID int64 `json:"id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", createUserBody, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), createdUser.ID)
		return err == nil
	})

	createMessageBody := map[string]any{
		"user_id": createdUser.ID,
		"sender":  "orders",
		"body":    "replicated payload",
		"metadata": map[string]any{
			"order_id": "A1001",
		},
	}
	doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/messages", createMessageBody, http.StatusCreated)

	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), createdUser.ID, 10)
		return err == nil && len(messages) == 1 && messages[0].Body == "replicated payload"
	})

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.lastAck("node-b") > 0
	})

	oldSession := nodeA.currentSession("node-b")
	if oldSession == nil {
		t.Fatalf("expected active session before reconnect test")
	}
	oldSession.close()

	waitFor(t, 5*time.Second, func() bool {
		newSession := nodeA.currentSession("node-b")
		return newSession != nil && newSession != oldSession
	})
}

type testNode struct {
	id            string
	store         *store.Store
	manager       *Manager
	apiBaseURL    string
	apiServer     *http.Server
	clusterServer *http.Server
	apiLn         net.Listener
	clusterLn     net.Listener
}

func newClusterTestNode(t *testing.T, nodeID string, slot uint16, clusterLn, apiLn net.Listener, advertiseAddr string, peers []Peer) *testNode {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), nodeID+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:   nodeID,
		NodeSlot: slot,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	manager, err := NewManager(Config{
		NodeID:        nodeID,
		NodeSlot:      slot,
		ListenAddr:    clusterLn.Addr().String(),
		AdvertiseAddr: advertiseAddr,
		ClusterSecret: "secret",
		Peers:         peers,
	}, st)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	svc := api.New(st, manager)
	apiServer := &http.Server{Handler: api.NewHTTP(svc).Handler()}
	clusterServer := &http.Server{Handler: manager.Handler()}

	node := &testNode{
		id:            nodeID,
		store:         st,
		manager:       manager,
		apiBaseURL:    "http://" + apiLn.Addr().String(),
		apiServer:     apiServer,
		clusterServer: clusterServer,
		apiLn:         apiLn,
		clusterLn:     clusterLn,
	}

	t.Cleanup(func() {
		_ = node.manager.Close()
		_ = node.apiServer.Close()
		_ = node.clusterServer.Close()
		_ = node.apiLn.Close()
		_ = node.clusterLn.Close()
		_ = node.store.Close()
	})
	return node
}

func (n *testNode) start(t *testing.T) {
	t.Helper()

	n.manager.Start(context.Background())

	go func() {
		err := n.clusterServer.Serve(n.clusterLn)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
	go func() {
		err := n.apiServer.Serve(n.apiLn)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
}

func (n *testNode) activePeer(peerID string) bool {
	return n.manager.hasActivePeer(peerID)
}

func (n *testNode) lastAck(peerID string) uint64 {
	n.manager.mu.Lock()
	defer n.manager.mu.Unlock()

	peer, ok := n.manager.peers[peerID]
	if !ok {
		return 0
	}
	return peer.lastAck
}

func (n *testNode) currentSession(peerID string) *session {
	n.manager.mu.Lock()
	defer n.manager.mu.Unlock()

	peer, ok := n.manager.peers[peerID]
	if !ok {
		return nil
	}
	return peer.active
}

func mustListen(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

func wsURL(ln net.Listener) string {
	return "ws://" + ln.Addr().String() + websocketPath
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func doJSON(t *testing.T, baseURL, method, path string, body any, wantStatus int) []byte {
	t.Helper()

	var requestBody *bytes.Reader
	if body == nil {
		requestBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		requestBody = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, baseURL+path, requestBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	data := new(bytes.Buffer)
	if _, err := data.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != wantStatus {
		t.Fatalf("unexpected status for %s %s: got=%d want=%d body=%s", method, path, resp.StatusCode, wantStatus, data.String())
	}
	return data.Bytes()
}

func mustJSON(t *testing.T, data []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("unmarshal json: %v body=%s", err, string(data))
	}
}

func newHandshakeTestManager(t *testing.T) *Manager {
	t.Helper()

	mgr, err := NewManager(Config{
		NodeID:        "node-a",
		NodeSlot:      1,
		ListenAddr:    "127.0.0.1:9080",
		AdvertiseAddr: "ws://127.0.0.1:9080/internal/cluster/ws",
		ClusterSecret: "secret",
		Peers: []Peer{
			{NodeID: "node-b", URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr
}

func newReplicationTestStore(t *testing.T, nodeID string, slot uint16) *store.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), nodeID+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:   nodeID,
		NodeSlot: slot,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func newReplicationTestManager(t *testing.T, st *store.Store) *Manager {
	t.Helper()

	mgr, err := NewManager(Config{
		NodeID:        "node-b",
		NodeSlot:      2,
		ListenAddr:    "127.0.0.1:9081",
		AdvertiseAddr: "ws://127.0.0.1:9081/internal/cluster/ws",
		ClusterSecret: "secret",
		Peers: []Peer{
			{NodeID: "node-a", URL: "ws://127.0.0.1:9080/internal/cluster/ws"},
		},
	}, st)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() {
		if mgr.cancel != nil {
			mgr.cancel()
		}
	})
	return mgr
}

func mustHelloEnvelope(t *testing.T, envelopeNodeID string, hello *internalproto.Hello) *internalproto.Envelope {
	t.Helper()

	payload, err := proto.Marshal(hello)
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	return &internalproto.Envelope{
		NodeId:      envelopeNodeID,
		MessageType: string(internalproto.MessageTypeHello),
		Payload:     payload,
	}
}
