package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"notifier/internal/api"
	"notifier/internal/auth"
	"notifier/internal/clock"
	internalproto "notifier/internal/proto"
	"notifier/internal/store"
)

func testNodeID(slot uint16) int64 {
	return int64(slot) << 12
}

func clusterUserKey(nodeID, userID int64) store.UserKey {
	return store.UserKey{NodeID: nodeID, UserID: userID}
}

func clusterUserPath(nodeID, userID int64) string {
	return "/nodes/" + strconv.FormatInt(nodeID, 10) + "/users/" + strconv.FormatInt(userID, 10)
}

func clusterUserMessagesPath(nodeID, userID int64) string {
	return clusterUserPath(nodeID, userID) + "/messages"
}

func TestActivateSessionPrefersExpectedDirection(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: testNodeID(2), URL: "ws://127.0.0.1:9999/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}

	inbound := &session{manager: mgr, conn: nil, peerID: testNodeID(2), outbound: false, send: make(chan *internalproto.Envelope, 1)}
	outbound := &session{manager: mgr, conn: nil, peerID: testNodeID(2), outbound: true, send: make(chan *internalproto.Envelope, 1)}

	if !mgr.activateSession(inbound) {
		t.Fatalf("expected first session to activate")
	}
	if !mgr.activateSession(outbound) {
		t.Fatalf("expected preferred outbound session to replace inbound")
	}

	mgr.mu.Lock()
	active := mgr.peers[testNodeID(2)].active
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
		envelopeNodeID   int64
		wantActivePeerID int64
	}{
		{
			name: "unknown peer",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:            testNodeID(3),
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				MessageWindowSize: store.DefaultMessageWindowSize,
			},
			envelopeNodeID: testNodeID(3),
		},
		{
			name: "protocol mismatch",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:            testNodeID(2),
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   "v0",
				MessageWindowSize: store.DefaultMessageWindowSize,
			},
			envelopeNodeID: testNodeID(2),
		},
		{
			name: "self peer",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:            testNodeID(1),
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				MessageWindowSize: store.DefaultMessageWindowSize,
			},
			envelopeNodeID: testNodeID(1),
		},
		{
			name: "outbound peer mismatch",
			session: &session{
				manager:          mgr,
				outbound:         true,
				configuredPeerID: testNodeID(3),
				send:             make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:            testNodeID(2),
				AdvertiseAddr:     websocketPath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				MessageWindowSize: store.DefaultMessageWindowSize,
			},
			envelopeNodeID: testNodeID(2),
		},
		{
			name: "invalid message window size",
			session: &session{
				manager: mgr,
				send:    make(chan *internalproto.Envelope, 1),
			},
			hello: &internalproto.Hello{
				NodeId:          testNodeID(2),
				AdvertiseAddr:   "",
				ProtocolVersion: internalproto.ProtocolVersion,
			},
			envelopeNodeID: testNodeID(2),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envelope := mustHelloEnvelope(t, tt.envelopeNodeID, tt.hello)
			if err := mgr.handleHello(tt.session, envelope); err == nil {
				t.Fatalf("expected handshake error")
			}
			if mgr.hasActivePeer(testNodeID(2)) {
				t.Fatalf("expected invalid handshake to leave peer inactive")
			}
		})
	}
}

func TestHandleHelloAllowsMismatchedMessageWindowSizes(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: 5,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: testNodeID(2), URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
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
	envelope := mustHelloEnvelope(t, testNodeID(2), &internalproto.Hello{
		NodeId:            testNodeID(2),
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		MessageWindowSize: 2,
	})

	if err := mgr.handleHello(sess, envelope); err != nil {
		t.Fatalf("handle hello with mismatched window: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		return mgr.hasActivePeer(testNodeID(2))
	})
}

func TestVerifyEnvelopeRejectsInvalidHMAC(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	hello := mustHelloEnvelope(t, testNodeID(2), &internalproto.Hello{
		NodeId:            testNodeID(2),
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		SnapshotVersion:   internalproto.SnapshotVersion,
		MessageWindowSize: store.DefaultMessageWindowSize,
	})
	if err := mgr.verifyEnvelope(hello); err == nil {
		t.Fatalf("expected missing hmac to fail")
	}

	hello.Hmac = []byte("bad")
	if err := mgr.verifyEnvelope(hello); err == nil {
		t.Fatalf("expected invalid hello hmac to fail")
	}

	eventBatch := &internalproto.Envelope{
		NodeId:    testNodeID(2),
		Sequence:  1,
		SentAtHlc: clock.NewClock(testNodeID(2)).Now().String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: []*internalproto.ReplicatedEvent{{
					EventId:       1,
					Kind:          "user.created",
					AggregateType: "user",
					AggregateId:   1,
					Hlc:           clock.NewClock(testNodeID(2)).Now().String(),
					OriginNodeId:  testNodeID(2),
					Payload:       []byte(`{"user":{"id":1,"username":"root","password_hash":"hash-root","profile":"{}","role":"super_admin","system_reserved":true,"created_at":"0000000000001-00000-0000000000000008192","updated_at":"0000000000001-00000-0000000000000008192","version_username":"0000000000001-00000-0000000000000008192","version_password_hash":"0000000000001-00000-0000000000000008192","version_profile":"0000000000001-00000-0000000000000008192","version_role":"0000000000001-00000-0000000000000008192","origin_node_id":8192}}`),
				}},
			},
		},
		Hmac: []byte("bad"),
	}
	if err := mgr.verifyEnvelope(eventBatch); err == nil {
		t.Fatalf("expected invalid event batch hmac to fail")
	}
}

func TestSignedEnvelopeVerifiesSuccessfully(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	data, err := mgr.marshalSignedEnvelope(mustHelloEnvelope(t, testNodeID(2), &internalproto.Hello{
		NodeId:            testNodeID(2),
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		SnapshotVersion:   internalproto.SnapshotVersion,
		MessageWindowSize: store.DefaultMessageWindowSize,
	}))
	if err != nil {
		t.Fatalf("marshal signed envelope: %v", err)
	}

	var envelope internalproto.Envelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal signed envelope: %v", err)
	}
	if err := mgr.verifyEnvelope(&envelope); err != nil {
		t.Fatalf("verify signed envelope: %v", err)
	}
}

func TestTokenSignedByNodeAIsAcceptedByNodeB(t *testing.T) {
	t.Parallel()

	storeA := newReplicationTestStore(t, "node-a", 1)
	storeB := newReplicationTestStore(t, "node-b", 2)
	for _, st := range []*store.Store{storeA, storeB} {
		if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
			Username:     "root",
			PasswordHash: mustHashPassword(t, "root-password"),
		}); err != nil {
			t.Fatalf("ensure bootstrap admin: %v", err)
		}
	}
	events, err := storeA.ListEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("list node A bootstrap events: %v", err)
	}
	for _, event := range events {
		if err := storeB.ApplyReplicatedEvent(context.Background(), store.ToReplicatedEvent(event)); err != nil {
			t.Fatalf("replicate node A bootstrap admin to node B: %v", err)
		}
	}

	signerA, err := auth.NewSigner("shared-token-secret")
	if err != nil {
		t.Fatalf("new signer A: %v", err)
	}
	signerB, err := auth.NewSigner("shared-token-secret")
	if err != nil {
		t.Fatalf("new signer B: %v", err)
	}

	serverA := httptest.NewServer(api.NewHTTP(api.New(storeA, nil), api.HTTPOptions{
		NodeID:   testNodeID(1),
		Signer:   signerA,
		TokenTTL: time.Hour,
	}).Handler())
	defer serverA.Close()
	serverB := httptest.NewServer(api.NewHTTP(api.New(storeB, nil), api.HTTPOptions{
		NodeID:   testNodeID(2),
		Signer:   signerB,
		TokenTTL: time.Hour,
	}).Handler())
	defer serverB.Close()

	var loginResp struct {
		Token string `json:"token"`
	}
	mustJSON(t, doJSON(t, serverA.URL, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  testNodeID(1),
		"user_id":  store.BootstrapAdminUserID,
		"password": "root-password",
	}, http.StatusOK), &loginResp)
	if loginResp.Token == "" {
		t.Fatalf("expected login token from node A")
	}

	_, err = doJSONRequestWithHeaders(serverB.URL, http.MethodGet, clusterUserPath(testNodeID(1), store.BootstrapAdminUserID), nil, http.StatusOK, map[string]string{
		"Authorization": "Bearer " + loginResp.Token,
	})
	if err != nil {
		t.Fatalf("request node B with node A token: %v", err)
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

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId:    testNodeID(1),
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

	replicatedUser, err := targetStore.GetUser(context.Background(), user.Key())
	if err != nil {
		t.Fatalf("expected replicated user: %v", err)
	}
	if replicatedUser.Username != user.Username {
		t.Fatalf("unexpected replicated user: %+v", replicatedUser)
	}

	cursor, err := targetStore.GetPeerCursor(context.Background(), testNodeID(1))
	if err != nil {
		t.Fatalf("get peer cursor: %v", err)
	}
	if cursor.AppliedSequence != int64(event.Sequence) {
		t.Fatalf("unexpected applied sequence: got=%d want=%d", cursor.AppliedSequence, event.Sequence)
	}

	select {
	case ackEnvelope := <-sess.send:
		if ackEnvelope.NodeId != testNodeID(2) {
			t.Fatalf("unexpected ack envelope node id: %d", ackEnvelope.NodeId)
		}
		ack := ackEnvelope.GetAck()
		if ack == nil {
			t.Fatalf("expected ack envelope body")
		}
		if ack.NodeId != testNodeID(2) {
			t.Fatalf("unexpected ack node id: %d", ack.NodeId)
		}
		if ack.AckedSequence != uint64(event.Sequence) {
			t.Fatalf("unexpected ack sequence: got=%d want=%d", ack.AckedSequence, event.Sequence)
		}
	default:
		t.Fatalf("expected ack to be enqueued")
	}
}

func TestHandleEventBatchDuplicateDeliveryIsIdempotentAndAcks(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	user, event, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "duplicate-batch-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 2),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId:    testNodeID(1),
		Sequence:  uint64(event.Sequence),
		SentAtHlc: event.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: []*internalproto.ReplicatedEvent{store.ToReplicatedEvent(event)},
			},
		},
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if err := mgr.handleEventBatch(sess, envelope); err != nil {
			t.Fatalf("handle event batch attempt %d: %v", attempt, err)
		}

		select {
		case ackEnvelope := <-sess.send:
			ack := ackEnvelope.GetAck()
			if ack == nil {
				t.Fatalf("expected ack on attempt %d, got %+v", attempt, ackEnvelope)
			}
			if ack.AckedSequence != uint64(event.Sequence) {
				t.Fatalf("unexpected ack sequence on attempt %d: got=%d want=%d", attempt, ack.AckedSequence, event.Sequence)
			}
		default:
			t.Fatalf("expected ack to be enqueued on attempt %d", attempt)
		}
	}

	users, err := targetStore.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("list target users: %v", err)
	}
	if len(users) != 1 || users[0].ID != user.ID {
		t.Fatalf("expected one replicated user after duplicate delivery, got %+v", users)
	}

	events, err := targetStore.ListEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("list target events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected one replicated event after duplicate delivery, got %+v", events)
	}
}

func TestHandleEventBatchDoesNotAckFailedApply(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId:    testNodeID(1),
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

	cursor, err := targetStore.GetPeerCursor(context.Background(), testNodeID(1))
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
	if err := targetStore.RecordPeerApplied(context.Background(), testNodeID(1), 2); err != nil {
		t.Fatalf("record peer applied: %v", err)
	}
	mgr := newReplicationTestManager(t, targetStore)

	sess := &session{
		manager: mgr,
		send:    make(chan *internalproto.Envelope, 1),
	}
	hello := &internalproto.Hello{
		NodeId:            testNodeID(1),
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		LastSequence:      4,
		MessageWindowSize: store.DefaultMessageWindowSize,
	}

	if err := mgr.handleHello(sess, mustHelloEnvelope(t, testNodeID(1), hello)); err != nil {
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
		UserKey: user.Key(),
		Sender:  "orders",
		Body:    "first",
	}); err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  "orders",
		Body:    "second",
	}); err != nil {
		t.Fatalf("create second message: %v", err)
	}

	expected, err := sourceStore.ListEvents(ctx, 1, 2)
	if err != nil {
		t.Fatalf("list expected events: %v", err)
	}

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId: testNodeID(1),
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

func TestBuildHelloIncludesSnapshotVersion(t *testing.T) {
	t.Parallel()

	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)

	envelope, err := mgr.buildHelloEnvelope()
	if err != nil {
		t.Fatalf("build hello envelope: %v", err)
	}
	hello := envelope.GetHello()
	if hello == nil {
		t.Fatalf("expected hello body")
	}
	if hello.SnapshotVersion != internalproto.SnapshotVersion {
		t.Fatalf("unexpected snapshot version: got=%q want=%q", hello.SnapshotVersion, internalproto.SnapshotVersion)
	}
}

func TestHandleHelloRejectsMismatchedSnapshotVersion(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	sess := &session{
		manager: mgr,
		send:    make(chan *internalproto.Envelope, 1),
	}
	hello := &internalproto.Hello{
		NodeId:            testNodeID(2),
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		SnapshotVersion:   "snapshot-v0",
		MessageWindowSize: store.DefaultMessageWindowSize,
	}

	if err := mgr.handleHello(sess, mustHelloEnvelope(t, testNodeID(2), hello)); err == nil {
		t.Fatalf("expected snapshot version mismatch to fail")
	}
}

func TestHandleSnapshotDigestRequestsUsersBeforeMessages(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	ctx := context.Background()
	user, _, err := sourceStore.CreateUser(ctx, store.CreateUserParams{
		Username:     "snapshot-digest-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  "orders",
		Body:    "snapshot message",
	}); err != nil {
		t.Fatalf("create source message: %v", err)
	}

	remoteDigest, err := sourceStore.BuildSnapshotDigest(ctx, []int64{testNodeID(1), testNodeID(2)})
	if err != nil {
		t.Fatalf("build remote digest: %v", err)
	}
	remoteDigest.SnapshotVersion = internalproto.SnapshotVersion

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	if err := mgr.handleSnapshotDigest(sess, snapshotDigestEnvelope(testNodeID(1), remoteDigest)); err != nil {
		t.Fatalf("handle snapshot digest: %v", err)
	}
	assertSnapshotRequest(t, sess, store.SnapshotUsersPartition)

	userChunk, err := sourceStore.BuildSnapshotChunk(ctx, store.SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build users chunk: %v", err)
	}
	if err := targetStore.ApplySnapshotChunk(ctx, userChunk); err != nil {
		t.Fatalf("apply users chunk: %v", err)
	}

	sess = readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	if err := mgr.handleSnapshotDigest(sess, snapshotDigestEnvelope(testNodeID(1), remoteDigest)); err != nil {
		t.Fatalf("handle snapshot digest after user repair: %v", err)
	}
	assertSnapshotRequest(t, sess, store.MessageSnapshotPartition(testNodeID(1)))
}

func TestHandleAckPersistsPeerCursor(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()
	envelope := &internalproto.Envelope{
		NodeId: testNodeID(1),
		Body: &internalproto.Envelope_Ack{
			Ack: &internalproto.Ack{
				NodeId:        testNodeID(1),
				AckedSequence: 7,
			},
		},
	}

	if err := mgr.handleAck(sess, envelope); err != nil {
		t.Fatalf("handle ack: %v", err)
	}

	cursor, err := targetStore.GetPeerCursor(context.Background(), testNodeID(1))
	if err != nil {
		t.Fatalf("get peer cursor: %v", err)
	}
	if cursor.AckedSequence != 7 {
		t.Fatalf("unexpected acked sequence: got=%d want=7", cursor.AckedSequence)
	}
}

func TestStatusReportsPeerReplicationAndWriteGate(t *testing.T) {
	t.Parallel()

	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)

	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status before session: %v", err)
	}
	if status.WriteGateReady {
		t.Fatalf("expected write gate to be closed before trusted clock sync")
	}
	if len(status.Peers) != 1 || status.Peers[0].NodeID != testNodeID(1) || status.Peers[0].Connected {
		t.Fatalf("unexpected disconnected peer status: %+v", status)
	}

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	sess.outbound = true
	sess.noteRemoteLastSequence(9)
	if !sess.beginPendingPull(3) {
		t.Fatalf("expected pending pull to start")
	}
	if !sess.beginSnapshotRequest(store.SnapshotUsersPartition) {
		t.Fatalf("expected snapshot request to start")
	}
	now := time.Now().UTC()
	mgr.mu.Lock()
	peer := mgr.peers[testNodeID(1)]
	peer.active = sess
	peer.trustedSession = sess
	peer.lastAck = 4
	peer.clockOffsetMs = 12
	peer.lastClockSync = now
	peer.snapshotDigestsSent = 1
	peer.snapshotDigestsReceived = 2
	peer.snapshotChunksSent = 3
	peer.snapshotChunksReceived = 4
	peer.lastSnapshotDigestAt = now
	peer.lastSnapshotChunkAt = now
	mgr.mu.Unlock()

	status, err = mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("status after session: %v", err)
	}
	if !status.WriteGateReady {
		t.Fatalf("expected write gate to be ready after trusted clock sync")
	}
	peerStatus := status.Peers[0]
	if !peerStatus.Connected ||
		peerStatus.SessionDirection != "outbound" ||
		peerStatus.LastAck != 4 ||
		peerStatus.RemoteLastSequence != 9 ||
		!peerStatus.PendingCatchup ||
		peerStatus.PendingSnapshotPartitions != 1 ||
		peerStatus.RemoteSnapshotVersion != internalproto.SnapshotVersion ||
		peerStatus.RemoteMessageWindowSize != store.DefaultMessageWindowSize ||
		peerStatus.ClockOffsetMs != 12 ||
		peerStatus.LastClockSync == nil ||
		peerStatus.SnapshotDigestsSentTotal != 1 ||
		peerStatus.SnapshotDigestsRecvTotal != 2 ||
		peerStatus.SnapshotChunksSentTotal != 3 ||
		peerStatus.SnapshotChunksRecvTotal != 4 ||
		peerStatus.LastSnapshotDigestAt == nil ||
		peerStatus.LastSnapshotChunkAt == nil {
		t.Fatalf("unexpected connected peer status: %+v", peerStatus)
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
		peerID:  testNodeID(1),
		send:    make(chan *internalproto.Envelope, 1),
	}
	sess.markReplicationReady()

	envelope := &internalproto.Envelope{
		NodeId:   testNodeID(1),
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
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	})

	nodeA.start(t)
	doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username": "gated-user",
		"password": "password-1",
	}, http.StatusServiceUnavailable)

	nodeB.start(t)
	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeB.activePeer(testNodeID(1))
	})

	doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username": "synced-user",
		"password": "password-1",
	}, http.StatusCreated)
}

func TestSkewedPeerRejectedWhenMaxClockSkewExceeded(t *testing.T) {
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindowAndSkew(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	}, store.DefaultMessageWindowSize, 0, DefaultMaxClockSkewMs)
	nodeB := newClusterTestNodeWithWindowAndSkew(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	}, store.DefaultMessageWindowSize, 5000, DefaultMaxClockSkewMs)

	nodeA.start(t)
	nodeB.start(t)

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if nodeA.activePeer(testNodeID(2)) || nodeB.activePeer(testNodeID(1)) {
			t.Fatalf("expected skewed peer to be rejected")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSkewedPeerAllowedWhenMaxClockSkewDisabled(t *testing.T) {
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindowAndSkew(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	}, store.DefaultMessageWindowSize, 0, 0)
	nodeB := newClusterTestNodeWithWindowAndSkew(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	}, store.DefaultMessageWindowSize, 5000, 0)

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeB.activePeer(testNodeID(1))
	})
}

func TestTwoNodeReplicationOverWebSocket(t *testing.T) {
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	})

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeB.activePeer(testNodeID(1))
	})

	createUserBody := map[string]any{
		"username": "alice",
		"password": "password-1",
		"profile": map[string]any{
			"display_name": "Alice",
		},
	}
	var createdUser struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", createUserBody, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		return err == nil
	})

	createMessageBody := map[string]any{
		"sender": "orders",
		"body":   "replicated payload",
		"metadata": map[string]any{
			"order_id": "A1001",
		},
	}
	doJSON(t, nodeA.apiBaseURL, http.MethodPost, clusterUserMessagesPath(createdUser.NodeID, createdUser.UserID), createMessageBody, http.StatusCreated)

	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID), 10)
		return err == nil && len(messages) == 1 && messages[0].Body == "replicated payload"
	})

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.lastAck(testNodeID(2)) > 0
	})

	oldSession := nodeA.currentSession(testNodeID(2))
	if oldSession == nil {
		t.Fatalf("expected active session before reconnect test")
	}
	oldSession.close()

	waitFor(t, 5*time.Second, func() bool {
		newSession := nodeA.currentSession(testNodeID(2))
		return newSession != nil && newSession != oldSession
	})
}

func TestTwoNodeMessageWindowConvergesWhenSizesMatch(t *testing.T) {
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindow(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	}, 2)
	nodeB := newClusterTestNodeWithWindow(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	}, 2)

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeB.activePeer(testNodeID(1))
	})

	var createdUser struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username": "window-user",
		"password": "password-1",
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		return err == nil
	})

	for i := 1; i <= 4; i++ {
		doJSON(t, nodeA.apiBaseURL, http.MethodPost, clusterUserMessagesPath(createdUser.NodeID, createdUser.UserID), map[string]any{
			"sender": "orders",
			"body":   "message-" + strconv.Itoa(i),
		}, http.StatusCreated)
	}

	waitFor(t, 5*time.Second, func() bool {
		expected := []string{"message-4", "message-3"}
		for _, node := range []*testNode{nodeA, nodeB} {
			messages, err := node.store.ListMessagesByUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID), 10)
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
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindow(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	}, 3)
	nodeB := newClusterTestNodeWithWindow(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	}, 2)

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeB.activePeer(testNodeID(1))
	})

	var createdUser struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username": "window-mismatch-user",
		"password": "password-1",
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		return err == nil
	})

	for i := 1; i <= 4; i++ {
		doJSON(t, nodeA.apiBaseURL, http.MethodPost, clusterUserMessagesPath(createdUser.NodeID, createdUser.UserID), map[string]any{
			"sender": "orders",
			"body":   "message-" + strconv.Itoa(i),
		}, http.StatusCreated)
	}

	waitFor(t, 5*time.Second, func() bool {
		aMessages, err := nodeA.store.ListMessagesByUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID), 10)
		if err != nil || len(aMessages) != 3 {
			return false
		}
		bMessages, err := nodeB.store.ListMessagesByUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID), 10)
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
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
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
		UserKey: user.Key(),
		Sender:  "orders",
		Body:    "missed while offline",
	}); err != nil {
		t.Fatalf("create offline message: %v", err)
	}

	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), user.Key())
		return err == nil
	})
	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), user.Key(), 10)
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
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
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
			UserKey: user.Key(),
			Sender:  "orders",
			Body:    "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create backlog message %d: %v", i, err)
		}
	}

	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		_, err := nodeB.store.GetUser(context.Background(), user.Key())
		return err == nil
	})
	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), user.Key(), 500)
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
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNodeWithWindow(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	}, 5)
	nodeB := newClusterTestNodeWithWindow(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
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
			UserKey: user.Key(),
			Sender:  "orders",
			Body:    "message-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("create offline message %d: %v", i, err)
		}
	}

	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		messages, err := nodeB.store.ListMessagesByUser(context.Background(), user.Key(), 10)
		if err != nil || len(messages) != 2 {
			return false
		}
		return messages[0].Body == "message-5" && messages[1].Body == "message-4"
	})
}

func TestSnapshotRepairOverWebSocketRepairsRowsOutsideEventLog(t *testing.T) {
	lnA := mustListen(t)
	lnB := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
	})

	ctx := context.Background()
	seedStore := newReplicationTestStore(t, "node-a", 1)
	user, _, err := seedStore.CreateUser(ctx, store.CreateUserParams{
		Username:     "snapshot-only-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create seed user: %v", err)
	}
	if _, _, err := seedStore.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  "orders",
		Body:    "snapshot-only-message",
	}); err != nil {
		t.Fatalf("create seed message: %v", err)
	}

	userChunk, err := seedStore.BuildSnapshotChunk(ctx, store.SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build seed user snapshot chunk: %v", err)
	}
	if err := nodeA.store.ApplySnapshotChunk(ctx, userChunk); err != nil {
		t.Fatalf("apply seed user snapshot chunk to node A: %v", err)
	}
	messageChunk, err := seedStore.BuildSnapshotChunk(ctx, store.MessageSnapshotPartition(testNodeID(1)))
	if err != nil {
		t.Fatalf("build seed message snapshot chunk: %v", err)
	}
	if err := nodeA.store.ApplySnapshotChunk(ctx, messageChunk); err != nil {
		t.Fatalf("apply seed message snapshot chunk to node A: %v", err)
	}

	nodeAEvents, err := nodeA.store.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list node A events before repair: %v", err)
	}
	if len(nodeAEvents) != 0 {
		t.Fatalf("expected node A seed data to be outside event log, got %+v", nodeAEvents)
	}

	nodeA.start(t)
	nodeB.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeB.activePeer(testNodeID(1))
	})

	waitFor(t, 5*time.Second, func() bool {
		if sess := nodeA.currentSession(testNodeID(2)); sess != nil {
			nodeA.manager.sendSnapshotDigest(sess)
		}

		if _, err := nodeB.store.GetUser(ctx, user.Key()); err != nil {
			return false
		}
		messages, err := nodeB.store.ListMessagesByUser(ctx, user.Key(), 10)
		return err == nil && len(messages) == 1 && messages[0].Body == "snapshot-only-message"
	})

	nodeBEvents, err := nodeB.store.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list node B events after repair: %v", err)
	}
	if len(nodeBEvents) != 0 {
		t.Fatalf("expected snapshot repair to avoid event log replay, got %+v", nodeBEvents)
	}
}

func TestThreeNodeFieldLevelConvergence(t *testing.T) {
	lnA := mustListen(t)
	lnB := mustListen(t)
	lnC := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
		{NodeID: testNodeID(3), URL: wsURL(lnC)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
		{NodeID: testNodeID(3), URL: wsURL(lnC)},
	})
	nodeC := newClusterTestNode(t, "node-c", 3, lnC, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})

	nodeA.start(t)
	nodeB.start(t)
	nodeC.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeA.activePeer(testNodeID(3)) &&
			nodeB.activePeer(testNodeID(1)) && nodeB.activePeer(testNodeID(3)) &&
			nodeC.activePeer(testNodeID(1)) && nodeC.activePeer(testNodeID(2))
	})

	var createdUser struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username": "cluster-user",
		"password": "password-1",
		"profile": map[string]any{
			"display_name": "Before Merge",
		},
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, errB := nodeB.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		_, errC := nodeC.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		return errB == nil && errC == nil
	})

	errCh := make(chan error, 2)
	start := make(chan struct{})

	go func() {
		<-start
		_, err := doJSONRequest(nodeB.apiBaseURL, http.MethodPatch, clusterUserPath(createdUser.NodeID, createdUser.UserID), map[string]any{
			"username": "cluster-user-renamed",
		}, http.StatusOK)
		errCh <- err
	}()

	go func() {
		<-start
		_, err := doJSONRequest(nodeC.apiBaseURL, http.MethodPatch, clusterUserPath(createdUser.NodeID, createdUser.UserID), map[string]any{
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
			user, err := node.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
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
	lnA := mustListen(t)
	lnB := mustListen(t)
	lnC := mustListen(t)

	nodeA := newClusterTestNode(t, "node-a", 1, lnA, []Peer{
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
		{NodeID: testNodeID(3), URL: wsURL(lnC)},
	})
	nodeB := newClusterTestNode(t, "node-b", 2, lnB, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
		{NodeID: testNodeID(3), URL: wsURL(lnC)},
	})
	nodeC := newClusterTestNode(t, "node-c", 3, lnC, []Peer{
		{NodeID: testNodeID(1), URL: wsURL(lnA)},
		{NodeID: testNodeID(2), URL: wsURL(lnB)},
	})

	nodeA.start(t)
	nodeB.start(t)
	nodeC.start(t)

	waitFor(t, 5*time.Second, func() bool {
		return nodeA.activePeer(testNodeID(2)) && nodeA.activePeer(testNodeID(3)) &&
			nodeB.activePeer(testNodeID(1)) && nodeB.activePeer(testNodeID(3)) &&
			nodeC.activePeer(testNodeID(1)) && nodeC.activePeer(testNodeID(2))
	})

	var createdUser struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSON(t, nodeA.apiBaseURL, http.MethodPost, "/users", map[string]any{
		"username": "delete-user",
		"password": "password-1",
	}, http.StatusCreated), &createdUser)

	waitFor(t, 5*time.Second, func() bool {
		_, errB := nodeB.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		_, errC := nodeC.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID))
		return errB == nil && errC == nil
	})

	errCh := make(chan error, 2)
	start := make(chan struct{})

	go func() {
		<-start
		_, err := doJSONRequest(nodeB.apiBaseURL, http.MethodPatch, clusterUserPath(createdUser.NodeID, createdUser.UserID), map[string]any{
			"profile": map[string]any{
				"display_name": "Should Lose To Delete",
			},
		}, http.StatusOK)
		errCh <- err
	}()

	go func() {
		<-start
		_, err := doJSONRequest(nodeC.apiBaseURL, http.MethodDelete, clusterUserPath(createdUser.NodeID, createdUser.UserID), nil, http.StatusOK)
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
			if _, err := node.store.GetUser(context.Background(), clusterUserKey(createdUser.NodeID, createdUser.UserID)); err != store.ErrNotFound {
				return false
			}
		}
		return true
	})
}

type testNode struct {
	id         int64
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
	numericNodeID := testNodeID(slot)
	sharedClock := clock.NewClockWithSource(numericNodeID, func() int64 {
		return time.Now().UTC().UnixMilli() + skewMs
	})
	st, err := store.Open(dbPath, store.Options{
		NodeID:            numericNodeID,
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
		NodeID:            numericNodeID,
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
		id:         numericNodeID,
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

func (n *testNode) activePeer(peerID int64) bool {
	return n.manager.hasActivePeer(peerID)
}

func (n *testNode) lastAck(peerID int64) uint64 {
	n.manager.mu.Lock()
	defer n.manager.mu.Unlock()

	peer, ok := n.manager.peers[peerID]
	if !ok {
		return 0
	}
	return peer.lastAck
}

func (n *testNode) currentSession(peerID int64) *session {
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
	if timeout < 10*time.Second {
		timeout = 10 * time.Second
	}

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
	return doJSONRequestWithHeaders(baseURL, method, path, body, wantStatus, nil)
}

func doJSONRequestWithHeaders(baseURL, method, path string, body any, wantStatus int, headers map[string]string) ([]byte, error) {
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
	for key, value := range headers {
		req.Header.Set(key, value)
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
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: testNodeID(2), URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
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
		NodeID:            testNodeID(slot),
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

func mustHashPassword(t *testing.T, password string) string {
	t.Helper()

	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return hash
}

func newReplicationTestManagerWithWindow(t *testing.T, st *store.Store, messageWindowSize int) *Manager {
	t.Helper()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(2),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: messageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		Peers: []Peer{
			{NodeID: testNodeID(1), URL: "ws://127.0.0.1:9080/internal/cluster/ws"},
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

func readySnapshotTestSession(mgr *Manager, peerID int64, remoteMessageWindowSize int) *session {
	sess := &session{
		manager:                 mgr,
		peerID:                  peerID,
		remoteSnapshotVersion:   internalproto.SnapshotVersion,
		remoteMessageWindowSize: remoteMessageWindowSize,
		send:                    make(chan *internalproto.Envelope, 4),
	}
	sess.markReplicationReady()
	return sess
}

func snapshotDigestEnvelope(nodeID int64, digest *internalproto.SnapshotDigest) *internalproto.Envelope {
	return &internalproto.Envelope{
		NodeId: nodeID,
		Body: &internalproto.Envelope_SnapshotDigest{
			SnapshotDigest: digest,
		},
	}
}

func assertSnapshotRequest(t *testing.T, sess *session, partition string) {
	t.Helper()

	select {
	case envelope := <-sess.send:
		chunk := envelope.GetSnapshotChunk()
		if chunk == nil {
			t.Fatalf("expected snapshot chunk request, got %+v", envelope)
		}
		if !chunk.Request {
			t.Fatalf("expected snapshot chunk request flag")
		}
		if chunk.Partition != partition {
			t.Fatalf("unexpected snapshot partition: got=%q want=%q", chunk.Partition, partition)
		}
	default:
		t.Fatalf("expected snapshot chunk request for %s", partition)
	}
}

func mustHelloEnvelope(t *testing.T, envelopeNodeID int64, hello *internalproto.Hello) *internalproto.Envelope {
	t.Helper()
	if hello != nil && hello.SnapshotVersion == "" {
		hello.SnapshotVersion = internalproto.SnapshotVersion
	}
	return &internalproto.Envelope{
		NodeId: envelopeNodeID,
		Body: &internalproto.Envelope_Hello{
			Hello: hello,
		},
	}
}
