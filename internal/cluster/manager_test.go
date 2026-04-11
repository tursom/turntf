package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"notifier/internal/api"
	"notifier/internal/clock"
	internalproto "notifier/internal/proto"
	"notifier/internal/store"
)

func TestActivateSessionPrefersExpectedDirection(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            "node-a",
		NodeSlot:          1,
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: "node-b", URL: "ws://127.0.0.1:9999/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
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
				NodeId:            "node-c",
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				MessageWindowSize: store.DefaultMessageWindowSize,
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
				NodeId:            "node-b",
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   "v0",
				MessageWindowSize: store.DefaultMessageWindowSize,
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
				NodeId:            "node-a",
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				MessageWindowSize: store.DefaultMessageWindowSize,
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
				NodeId:            "node-b",
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				MessageWindowSize: store.DefaultMessageWindowSize,
			},
			envelopeNodeID: "node-b",
		},
		{
			name: "invalid message window size",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:          "node-b",
				AdvertiseAddr:   "",
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

func TestHandleHelloAllowsMismatchedMessageWindowSizes(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            "node-a",
		NodeSlot:          1,
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: 5,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: "node-b", URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}

	sess := &session{
		manager: mgr,
		send:    make(chan *internalproto.Envelope, 1),
	}
	envelope := mustHelloEnvelope(t, "node-b", &internalproto.Hello{
		NodeId:            "node-b",
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		MessageWindowSize: 2,
	})

	if err := mgr.handleHello(sess, envelope); err != nil {
		t.Fatalf("handle hello with mismatched window: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		return mgr.hasActivePeer("node-b")
	})
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

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId:    "node-a",
		Sequence:  uint64(event.Sequence),
		SentAtHlc: event.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: []*internalproto.ReplicatedEvent{store.ToReplicatedEvent(event)},
			},
		},
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

	cursor, err := targetStore.GetPeerCursor(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("get peer cursor: %v", err)
	}
	if cursor.AppliedSequence != int64(event.Sequence) {
		t.Fatalf("unexpected applied sequence: got=%d want=%d", cursor.AppliedSequence, event.Sequence)
	}

	select {
	case ackEnvelope := <-sess.send:
		if ackEnvelope.NodeId != "node-b" {
			t.Fatalf("unexpected ack envelope node id: %s", ackEnvelope.NodeId)
		}
		ack := ackEnvelope.GetAck()
		if ack == nil {
			t.Fatalf("expected ack envelope body")
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

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId:    "node-a",
		Sequence:  42,
		SentAtHlc: "2026-01-01T00:00:00.000000000Z/2/1",
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: []*internalproto.ReplicatedEvent{nil},
			},
		},
	}

	if err := mgr.handleEventBatch(sess, envelope); err == nil {
		t.Fatalf("expected failed apply to return error")
	}

	select {
	case ackEnvelope := <-sess.send:
		t.Fatalf("did not expect ack after failed apply: %+v", ackEnvelope)
	default:
	}

	cursor, err := targetStore.GetPeerCursor(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("get peer cursor: %v", err)
	}
	if cursor.AppliedSequence != 0 {
		t.Fatalf("expected failed apply to keep applied sequence at 0, got %d", cursor.AppliedSequence)
	}
}

func TestHandleHelloEnqueuesPullEventsWhenBehind(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	if err := targetStore.RecordPeerApplied(context.Background(), "node-a", 2); err != nil {
		t.Fatalf("record peer applied: %v", err)
	}
	mgr := newReplicationTestManager(t, targetStore)

	sess := &session{
		manager: mgr,
		send:    make(chan *internalproto.Envelope, 1),
	}
	hello := &internalproto.Hello{
		NodeId:            "node-a",
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		LastSequence:      4,
		MessageWindowSize: store.DefaultMessageWindowSize,
	}

	if err := mgr.handleHello(sess, mustHelloEnvelope(t, "node-a", hello)); err != nil {
		t.Fatalf("handle hello: %v", err)
	}
	select {
	case envelope := <-sess.send:
		pull := envelope.GetPullEvents()
		if pull == nil {
			t.Fatalf("expected pull events body")
		}
		if pull.AfterSequence != 2 {
			t.Fatalf("unexpected pull after sequence: got=%d want=2", pull.AfterSequence)
		}
		if pull.Limit != pullBatchSize {
			t.Fatalf("unexpected pull limit: got=%d want=%d", pull.Limit, pullBatchSize)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected catch-up pull request")
	}
}

func TestHandlePullEventsReturnsRequestedRange(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, sourceStore)

	ctx := context.Background()
	user, _, err := sourceStore.CreateUser(ctx, store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "first",
	}); err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "second",
	}); err != nil {
		t.Fatalf("create second message: %v", err)
	}

	expected, err := sourceStore.ListEvents(ctx, 1, 2)
	if err != nil {
		t.Fatalf("list expected events: %v", err)
	}

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId: "node-a",
		Body: &internalproto.Envelope_PullEvents{
			PullEvents: &internalproto.PullEvents{
				AfterSequence: 1,
				Limit:         2,
			},
		},
	}

	if err := mgr.handlePullEvents(sess, envelope); err != nil {
		t.Fatalf("handle pull events: %v", err)
	}

	select {
	case batchEnvelope := <-sess.send:
		batch := batchEnvelope.GetEventBatch()
		if batch == nil {
			t.Fatalf("expected event batch body")
		}
		if batchEnvelope.Sequence != uint64(expected[len(expected)-1].Sequence) {
			t.Fatalf("unexpected batch sequence: got=%d want=%d", batchEnvelope.Sequence, expected[len(expected)-1].Sequence)
		}
		if len(batch.Events) != len(expected) {
			t.Fatalf("unexpected event count: got=%d want=%d", len(batch.Events), len(expected))
		}
		for i, event := range batch.Events {
			if event.EventId != expected[i].EventID {
				t.Fatalf("unexpected event at index %d: got=%d want=%d", i, event.EventId, expected[i].EventID)
			}
		}
	default:
		t.Fatalf("expected event batch response")
	}
}

func TestHandleAckPersistsPeerCursor(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId: "node-a",
		Body: &internalproto.Envelope_Ack{
			Ack: &internalproto.Ack{
				NodeId:        "node-a",
				AckedSequence: 7,
			},
		},
	}

	if err := mgr.handleAck(sess, envelope); err != nil {
		t.Fatalf("handle ack: %v", err)
	}

	cursor, err := targetStore.GetPeerCursor(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("get peer cursor: %v", err)
	}
	if cursor.AckedSequence != 7 {
		t.Fatalf("unexpected acked sequence: got=%d want=7", cursor.AckedSequence)
	}
}

func TestHandleEventBatchRejectsFutureTimestampWhenSkewCheckEnabled(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	_, event, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "future-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	replicated := store.ToReplicatedEvent(event)
	futureHLC := clock.Timestamp{
		WallTimeMs: mgr.clock.WallTimeMs() + DefaultMaxClockSkewMs + 2000,
		Logical:    0,
		NodeID:     1,
	}
	replicated.Hlc = futureHLC.String()

	sess := &session{
		manager: mgr,
		peerID:  "node-a",
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()

	envelope := &internalproto.Envelope{
		NodeId:   "node-a",
		Sequence: uint64(event.Sequence),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: []*internalproto.ReplicatedEvent{replicated},
			},
		},
	}

	if err := mgr.handleEventBatch(sess, envelope); err == nil {
		t.Fatalf("expected future timestamp event batch to fail")
	}

	select {
	case ackEnvelope := <-sess.send:
		t.Fatalf("did not expect ack after future timestamp rejection: %+v", ackEnvelope)
	default:
	}
}

func TestWriteRequestsBlockedUntilInitialClockSync(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	})

	nodeA.start(t)
	doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username":      "gated-user",
		"password_hash": "hash-1",
	}, http.StatusServiceUnavailable)

	nodeB.start(t)
	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeB.activePeer("node-a")
	})

	doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username":      "synced-user",
		"password_hash": "hash-1",
	}, http.StatusCreated)
}

func TestSkewedPeerRejectedWhenMaxClockSkewExceeded(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindowAndSkew(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	}, store.DefaultMessageWindowSize, 0, DefaultMaxClockSkewMs)
	nodeB := newClusterTestNodeWithWindowAndSkew(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	}, store.DefaultMessageWindowSize, 5000, DefaultMaxClockSkewMs)

	nodeA.start(t)
	nodeB.start(t)

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if nodeA.activePeer("node-b") || nodeB.activePeer("node-a") {
			t.Fatalf("expected skewed peer to be rejected")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSkewedPeerAllowedWhenMaxClockSkewDisabled(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindowAndSkew(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	}, store.DefaultMessageWindowSize, 0, 0)
	nodeB := newClusterTestNodeWithWindowAndSkew(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	}, store.DefaultMessageWindowSize, 5000, 0)

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeB.activePeer("node-a")
	})
}

func TestTwoNodeReplicationOverWebSocket(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
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

func TestTwoNodeMessageWindowConvergesWhenSizesMatch(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindow(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	}, 2)
	nodeB := newClusterTestNodeWithWindow(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	}, 2)

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeB.activePeer("node-a")
	})

	var createdUser struct {
		ID int64 `json:"id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username":      "window-user",
		"password_hash": "hash-1",
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), createdUser.ID)
		return err == nil
	})

	for i := 1; i <= 4; i++ {
		doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/messages", map[string]any{
			"user_id": createdUser.ID,
			"sender":  "orders",
			"body":    "message-" + strconv.Itoa(i),
		}, http.StatusCreated)
	}

	waitFor(t, 5*time.Second, func() bool {
		expected := []string{"message-4", "message-3"}
		for _, node := range []*testNode{nodeA, nodeB} {
			messages, err := node.store.ListMessagesByUser(context.Background(), createdUser.ID, 10)
			if err != nil || len(messages) != len(expected) {
				return false
			}
			for i, body := range expected {
				if messages[i].Body != body {
					return false
				}
			}
		}
		return true
	})
}

func TestTwoNodeMessageWindowMismatchKeepsPerNodeWindows(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindow(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	}, 3)
	nodeB := newClusterTestNodeWithWindow(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	}, 2)

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeB.activePeer("node-a")
	})

	var createdUser struct {
		ID int64 `json:"id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username":      "window-mismatch-user",
		"password_hash": "hash-1",
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), createdUser.ID)
		return err == nil
	})

	for i := 1; i <= 4; i++ {
		doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/messages", map[string]any{
			"user_id": createdUser.ID,
			"sender":  "orders",
			"body":    "message-" + strconv.Itoa(i),
		}, http.StatusCreated)
	}

	waitFor(t, 5*time.Second, func() bool {
		aMessages, err := nodeA.store.ListMessagesByUser(context.Background(), createdUser.ID, 10)
		if err != nil || len(aMessages) != 3 {
			return false
		}
		bMessages, err := nodeB.store.ListMessagesByUser(context.Background(), createdUser.ID, 10)
		if err != nil || len(bMessages) != 2 {
			return false
		}
		return aMessages[0].Body == "message-4" &&
			aMessages[1].Body == "message-3" &&
			aMessages[2].Body == "message-2" &&
			bMessages[0].Body == "message-4" &&
			bMessages[1].Body == "message-3"
	})
}

func TestLateJoiningNodeCatchesUpWithoutDuplicates(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	})

	nodeA.start(t)

	user, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create offline user: %v", err)
	}
	if _, _, err := nodeA.store.CreateMessage(context.Background(), store.CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "missed while offline",
	}); err != nil {
		t.Fatalf("create offline message: %v", err)
	}

	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), user.ID)
		return err == nil
	})
	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), user.ID, 10)
		return err == nil && len(messages) == 1 && messages[0].Body == "missed while offline"
	})

	events, err := nodeB.store.ListEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("list events after reconnect: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 replicated events without duplicates, got %d", len(events))
	}
}

func TestLateJoiningNodeCatchesUpAcrossMultiplePullBatches(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	})

	nodeA.start(t)

	ctx := context.Background()
	user, _, err := nodeA.store.CreateUser(ctx, store.CreateUserParams{
		Username:     "bulk-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	for i := 0; i < pullBatchSize+12; i++ {
		if _, _, err := nodeA.store.CreateMessage(ctx, store.CreateMessageParams{
			UserID: user.ID,
			Sender: "orders",
			Body:   "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create backlog message %d: %v", i, err)
		}
	}

	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), user.ID)
		return err == nil
	})
	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), user.ID, 500)
		return err == nil && len(messages) == pullBatchSize+12
	})

	events, err := nodeB.store.ListEvents(context.Background(), 0, 500)
	if err != nil {
		t.Fatalf("list replicated events: %v", err)
	}
	if len(events) != pullBatchSize+13 {
		t.Fatalf("unexpected replicated event count: got=%d want=%d", len(events), pullBatchSize+13)
	}
}

func TestLateJoiningNodeTrimsCatchupToLocalWindow(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindow(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
	}, 5)
	nodeB := newClusterTestNodeWithWindow(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
	}, 2)

	nodeA.start(t)

	user, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "late-window-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create offline user: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, _, err := nodeA.store.CreateMessage(context.Background(), store.CreateMessageParams{
			UserID: user.ID,
			Sender: "orders",
			Body:   "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create offline message %d: %v", i, err)
		}
	}

	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), user.ID, 10)
		if err != nil || len(messages) != 2 {
			return false
		}
		return messages[0].Body == "message-5" && messages[1].Body == "message-4"
	})
}

func TestThreeNodeFieldLevelConvergence(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)
	lnC := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
		{NodeID: "node-c", URL: wsURL(lnC)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
		{NodeID: "node-c", URL: wsURL(lnC)},
	})
	nodeC := newClusterTestNode(t, "node-c", 3, lnC, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
		{NodeID: "node-b", URL: wsURL(lnB)},
	})

	nodeA.start(t)
	nodeB.start(t)
	nodeC.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeA.activePeer("node-c") &&
			nodeB.activePeer("node-a") && nodeB.activePeer("node-c") &&
			nodeC.activePeer("node-a") && nodeC.activePeer("node-b")
	})

	var createdUser struct {
		ID int64 `json:"id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username":      "cluster-user",
		"password_hash": "hash-1",
		"profile": map[string]any{
			"display_name": "Before Merge",
		},
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, errB := nodeB.store.GetUser(context.Background(), createdUser.ID)
		_, errC := nodeC.store.GetUser(context.Background(), createdUser.ID)
		return errB == nil && errC == nil
	})

	errCh := make(chan error, 2)
	start := make(chan struct{})

	go func() {
		<-start
		_, err := doJSONRequest(nodeB.apiBaseURL, http.MethodPatch, "/users/"+strconv.FormatInt(createdUser.ID, 10), map[string]any{
			"username": "cluster-user-renamed",
		}, http.StatusOK)
		errCh <- err
	}()

	go func() {
		<-start
		_, err := doJSONRequest(nodeC.apiBaseURL, http.MethodPatch, "/users/"+strconv.FormatInt(createdUser.ID, 10), map[string]any{
			"profile": map[string]any{
				"display_name": "Merged Profile",
			},
		}, http.StatusOK)
		errCh <- err
	}()

	close(start)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent patch request failed: %v", err)
		}
	}

	waitFor(t, 5*time.Second, func() bool {
		for _, node := range []*testNode{nodeA, nodeB, nodeC} {
			user, err := node.store.GetUser(context.Background(), createdUser.ID)
			if err != nil {
				return false
			}
			if user.Username != "cluster-user-renamed" || user.Profile != `{"display_name":"Merged Profile"}` {
				return false
			}
		}
		return true
	})
}

func TestThreeNodeDeleteWinsOverConcurrentUpdate(t *testing.T) {
	t.Parallel()

	lnA := mustListen(t)
	lnB := mustListen(t)
	lnC := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: "node-b", URL: wsURL(lnB)},
		{NodeID: "node-c", URL: wsURL(lnC)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
		{NodeID: "node-c", URL: wsURL(lnC)},
	})
	nodeC := newClusterTestNode(t, "node-c", 3, lnC, []Peer{
		{NodeID: "node-a", URL: wsURL(lnA)},
		{NodeID: "node-b", URL: wsURL(lnB)},
	})

	nodeA.start(t)
	nodeB.start(t)
	nodeC.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer("node-b") && nodeA.activePeer("node-c") &&
			nodeB.activePeer("node-a") && nodeB.activePeer("node-c") &&
			nodeC.activePeer("node-a") && nodeC.activePeer("node-b")
	})

	var createdUser struct {
		ID int64 `json:"id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username":      "delete-user",
		"password_hash": "hash-1",
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, errB := nodeB.store.GetUser(context.Background(), createdUser.ID)
		_, errC := nodeC.store.GetUser(context.Background(), createdUser.ID)
		return errB == nil && errC == nil
	})

	errCh := make(chan error, 2)
	start := make(chan struct{})

	go func() {
		<-start
		_, err := doJSONRequest(nodeB.apiBaseURL, http.MethodPatch, "/users/"+strconv.FormatInt(createdUser.ID, 10), map[string]any{
			"profile": map[string]any{
				"display_name": "Should Lose To Delete",
			},
		}, http.StatusOK)
		errCh <- err
	}()

	go func() {
		<-start
		_, err := doJSONRequest(nodeC.apiBaseURL, http.MethodDelete, "/users/"+strconv.FormatInt(createdUser.ID, 10), nil, http.StatusOK)
		errCh <- err
	}()

	close(start)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent delete/update request failed: %v", err)
		}
	}

	waitFor(t, 5*time.Second, func() bool {
		for _, node := range []*testNode{nodeA, nodeB, nodeC} {
			if _, err := node.store.GetUser(context.Background(), createdUser.ID); err != store.ErrNotFound {
				return false
			}
		}
		return true
	})
}

type testNode struct {
	id         string
	store      *store.Store
	manager    *Manager
	apiBaseURL string
	server     *http.Server
	ln         net.Listener
}

func newClusterTestNode(t *testing.T, nodeID string, slot uint16, ln net.Listener, peers []Peer) *testNode {
	t.Helper()
	return newClusterTestNodeWithWindowAndSkew(t, nodeID, slot, ln, peers, store.DefaultMessageWindowSize, 0, DefaultMaxClockSkewMs)
}

func newClusterTestNodeWithWindow(t *testing.T, nodeID string, slot uint16, ln net.Listener, peers []Peer, messageWindowSize int) *testNode {
	t.Helper()
	return newClusterTestNodeWithWindowAndSkew(t, nodeID, slot, ln, peers, messageWindowSize, 0, DefaultMaxClockSkewMs)
}

func newClusterTestNodeWithWindowAndSkew(t *testing.T, nodeID string, slot uint16, ln net.Listener, peers []Peer, messageWindowSize int, skewMs int64, maxClockSkewMs int64) *testNode {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), nodeID+".db")
	sharedClock := clock.NewClockWithSource(slot, func() int64 {
		return time.Now().UTC().UnixMilli() + skewMs
	})
	st, err := store.Open(dbPath, store.Options{
		NodeID:            nodeID,
		NodeSlot:          slot,
		MessageWindowSize: messageWindowSize,
		Clock:             sharedClock,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	manager, err := NewManager(Config{
		NodeID:            nodeID,
		NodeSlot:          slot,
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		Peers:             peers,
		MessageWindowSize: messageWindowSize,
		MaxClockSkewMs:    maxClockSkewMs,
	}, st)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	svc := api.New(st, manager)
	rootMux := http.NewServeMux()
	rootMux.Handle("/", api.NewHTTP(svc).Handler())
	rootMux.Handle(manager.AdvertisePath(), manager.Handler())
	server := &http.Server{Handler: rootMux}

	node := &testNode{
		id:         nodeID,
		store:      st,
		manager:    manager,
		apiBaseURL: "http://" + ln.Addr().String(),
		server:     server,
		ln:         ln,
	}

	t.Cleanup(func() {
		_ = node.manager.Close()
		_ = node.server.Close()
		_ = node.ln.Close()
		_ = node.store.Close()
	})
	return node
}

func (n *testNode) start(t *testing.T) {
	t.Helper()

	n.manager.Start(context.Background())

	go func() {
		err := n.server.Serve(n.ln)
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

	data, err := doJSONRequest(baseURL, method, path, body, wantStatus)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return data
}

func doJSONRequest(baseURL, method, path string, body any, wantStatus int) ([]byte, error) {
	var requestBody *bytes.Reader
	if body == nil {
		requestBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, baseURL+path, requestBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data := new(bytes.Buffer)
	if _, err := data.ReadFrom(resp.Body); err != nil {
		return nil, err
	}

	if resp.StatusCode != wantStatus {
		return nil, errors.New("unexpected status for " + method + " " + path + ": got=" + strconv.Itoa(resp.StatusCode) + " want=" + strconv.Itoa(wantStatus) + " body=" + data.String())
	}
	return data.Bytes(), nil
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
		NodeID:            "node-a",
		NodeSlot:          1,
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: "node-b", URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	return mgr
}

func newReplicationTestStore(t *testing.T, nodeID string, slot uint16) *store.Store {
	t.Helper()
	return newReplicationTestStoreWithWindow(t, nodeID, slot, store.DefaultMessageWindowSize)
}

func newReplicationTestStoreWithWindow(t *testing.T, nodeID string, slot uint16, messageWindowSize int) *store.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), nodeID+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:            nodeID,
		NodeSlot:          slot,
		MessageWindowSize: messageWindowSize,
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
	return newReplicationTestManagerWithWindow(t, st, store.DefaultMessageWindowSize)
}

func newReplicationTestManagerWithWindow(t *testing.T, st *store.Store, messageWindowSize int) *Manager {
	t.Helper()

	mgr, err := NewManager(Config{
		NodeID:            "node-b",
		NodeSlot:          2,
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: messageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: "node-a", URL: "ws://127.0.0.1:9080/internal/cluster/ws"},
		},
	}, st)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	t.Cleanup(func() {
		if mgr.cancel != nil {
			mgr.cancel()
		}
	})
	return mgr
}

func mustHelloEnvelope(t *testing.T, envelopeNodeID string, hello *internalproto.Hello) *internalproto.Envelope {
	t.Helper()
	return &internalproto.Envelope{
		NodeId: envelopeNodeID,
		Body: &internalproto.Envelope_Hello{
			Hello: hello,
		},
	}
}
