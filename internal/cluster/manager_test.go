package cluster

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/api"
	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func testNodeID(slot uint16) int64 {
	return int64(slot) << 12
}

func clusterUserKey(nodeID, userID int64) store.UserKey {
	return store.UserKey{NodeID: nodeID, UserID: userID}
}

func clusterSenderKey(slot uint16, userID int64) store.UserKey {
	return store.UserKey{NodeID: testNodeID(slot), UserID: userID}
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
			{URL: "ws://127.0.0.1:9999/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	mgr.peers[testNodeID(2)] = &peerState{}

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

func TestQueryLoggedInUsersWithoutMeshReturnsUnavailable(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)

	_, err := mgr.QueryLoggedInUsers(context.Background(), testNodeID(2))
	if err == nil {
		t.Fatalf("expected query logged-in users failure without mesh")
	}
	if !errors.Is(err, app.ErrServiceUnavailable) || !strings.Contains(err.Error(), "not reachable") {
		t.Fatalf("unexpected query logged-in users error: %v", err)
	}

	mgr.mu.Lock()
	pendingCount := len(mgr.pendingLoggedInUsers)
	mgr.mu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("expected pending logged-in users query to be cleared, got %d", pendingCount)
	}
}

func TestActivateSessionLogsPeerReconnectForKnownPeer(t *testing.T) {
	mgr := newHandshakeTestManager(t)
	mgr.peers[testNodeID(2)] = &peerState{joinedLogged: true}

	logOutput := captureClusterLogs(t)

	sess := &session{
		manager: mgr,
		peerID:  testNodeID(2),
		send:    make(chan *internalproto.Envelope, 1),
	}

	if !mgr.activateSession(sess) {
		t.Fatalf("expected known peer session to activate")
	}
	if !strings.Contains(logOutput.String(), `"event":"peer_reconnected"`) {
		t.Fatalf("expected peer reconnect log, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"peer_node_id":8192`) {
		t.Fatalf("expected peer reconnect log to include peer node id, got %q", logOutput.String())
	}
}

func TestRequestCatchupIfNeededLogsRequestedPull(t *testing.T) {
	t.Parallel()

	mgr := newReplicationTestManager(t, newReplicationTestStore(t, "node-b", 2))
	logOutput := captureClusterLogs(t)

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	sess.noteRemoteOriginEvent(testNodeID(1), 7)

	requested, err := mgr.requestCatchupIfNeeded(sess)
	if err != nil {
		t.Fatalf("request catchup: %v", err)
	}
	if !requested {
		t.Fatalf("expected catchup request")
	}
	if !strings.Contains(logOutput.String(), `"event":"catchup_requested"`) {
		t.Fatalf("expected catchup log, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"origin_node_id":4096`) {
		t.Fatalf("expected catchup origin in log, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"request_id":1`) {
		t.Fatalf("expected catchup request id in log, got %q", logOutput.String())
	}
}

func TestHandleSnapshotDigestLogsPartitionMismatch(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)
	logOutput := captureClusterLogs(t)

	ctx := context.Background()
	user, _, err := sourceStore.CreateUser(ctx, store.CreateUserParams{
		Username:     "snapshot-log-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  clusterSenderKey(9, 1),
		Body:    []byte("snapshot message"),
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
	if !strings.Contains(logOutput.String(), `"event":"snapshot_partition_mismatch"`) {
		t.Fatalf("expected snapshot mismatch log, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"event":"snapshot_partition_requested"`) {
		t.Fatalf("expected snapshot request log, got %q", logOutput.String())
	}
}

func TestRouteTransientPacketLogsRetryQueueEntry(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	logOutput := captureClusterLogs(t)

	if err := mgr.RouteTransientPacket(context.Background(), store.TransientPacket{
		PacketID:     11,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    clusterUserKey(testNodeID(3), 9),
		Sender:       clusterSenderKey(9, 1),
		Body:         []byte("retry-me"),
		DeliveryMode: store.DeliveryModeRouteRetry,
		TTLHops:      8,
	}); err != nil {
		t.Fatalf("route transient packet: %v", err)
	}
	if !strings.Contains(logOutput.String(), `"event":"transient_packet_queued"`) {
		t.Fatalf("expected transient queue log, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"packet_id":11`) {
		t.Fatalf("expected packet id in transient queue log, got %q", logOutput.String())
	}
}

func TestRouteTransientPacketWithoutMeshDeliversLocalTarget(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	delivered := make(chan store.TransientPacket, 1)
	mgr.SetTransientHandler(func(packet store.TransientPacket) bool {
		delivered <- packet
		return true
	})

	packet := store.TransientPacket{
		PacketID:     12,
		SourceNodeID: testNodeID(2),
		TargetNodeID: testNodeID(1),
		Recipient:    clusterUserKey(testNodeID(1), 9),
		Sender:       clusterSenderKey(2, 1),
		Body:         []byte("local"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      4,
	}
	if err := mgr.RouteTransientPacket(context.Background(), packet); err != nil {
		t.Fatalf("route local transient packet: %v", err)
	}

	select {
	case got := <-delivered:
		if got.PacketID != packet.PacketID || got.TargetNodeID != packet.TargetNodeID || string(got.Body) != "local" {
			t.Fatalf("unexpected delivered packet: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected local transient packet delivery")
	}
}

func TestRouteTransientPacketWithoutMeshDoesNotFallbackToLegacySession(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	legacy := &session{
		manager:      mgr,
		peerID:       testNodeID(3),
		connectionID: 1,
		send:         make(chan *internalproto.Envelope, 1),
	}
	mgr.peers[testNodeID(3)] = &peerState{
		active:   legacy,
		sessions: map[uint64]*session{legacy.connectionID: legacy},
	}

	if err := mgr.RouteTransientPacket(context.Background(), store.TransientPacket{
		PacketID:     13,
		SourceNodeID: testNodeID(1),
		TargetNodeID: testNodeID(3),
		Recipient:    clusterUserKey(testNodeID(3), 9),
		Sender:       clusterSenderKey(1, 1),
		Body:         []byte("no-fallback"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      4,
	}); err != nil {
		t.Fatalf("route remote transient packet: %v", err)
	}

	select {
	case envelope := <-legacy.send:
		t.Fatalf("remote transient packet should not fall back to legacy session send: %+v", envelope)
	default:
	}
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
					AggregateType: "user",
					AggregateId:   1,
					Hlc:           clock.NewClock(testNodeID(2)).Now().String(),
					OriginNodeId:  testNodeID(2),
					Body: &internalproto.ReplicatedEvent_UserCreated{
						UserCreated: &internalproto.UserCreatedEvent{
							NodeId:              testNodeID(2),
							UserId:              1,
							Username:            "root",
							PasswordHash:        "hash-root",
							Profile:             "{}",
							Role:                "super_admin",
							SystemReserved:      true,
							CreatedAtHlc:        "0000000000001-00000-0000000000000008192",
							UpdatedAtHlc:        "0000000000001-00000-0000000000000008192",
							VersionUsername:     "0000000000001-00000-0000000000000008192",
							VersionPasswordHash: "0000000000001-00000-0000000000000008192",
							VersionProfile:      "0000000000001-00000-0000000000000008192",
							VersionRole:         "0000000000001-00000-0000000000000008192",
							OriginNodeId:        testNodeID(2),
						},
					},
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

	serverA := newClusterHTTPTestServer(t, api.NewHTTP(api.New(storeA, nil), api.HTTPOptions{
		NodeID:   testNodeID(1),
		Signer:   signerA,
		TokenTTL: time.Hour,
	}).Handler())
	serverB := newClusterHTTPTestServer(t, api.NewHTTP(api.New(storeB, nil), api.HTTPOptions{
		NodeID:   testNodeID(2),
		Signer:   signerB,
		TokenTTL: time.Hour,
	}).Handler())

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

func TestHandleEventBatchAckMeansAppliedToLocalState(t *testing.T) {
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
		t.Fatalf("expected replicated user after local apply: %v", err)
	}
	if replicatedUser.Username != user.Username {
		t.Fatalf("expected replicated user to match source after local apply, got %+v", replicatedUser)
	}

	cursor, err := targetStore.GetOriginCursor(context.Background(), testNodeID(1))
	if err != nil {
		t.Fatalf("get origin cursor: %v", err)
	}
	if cursor.AppliedEventID != event.EventID {
		t.Fatalf("expected local origin cursor to advance after apply, got=%d want=%d", cursor.AppliedEventID, event.EventID)
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
		if ack.OriginNodeId != testNodeID(1) {
			t.Fatalf("unexpected ack origin node id: %d", ack.OriginNodeId)
		}
		if ack.AckedEventId != uint64(event.EventID) {
			t.Fatalf("unexpected ack event id: got=%d want=%d", ack.AckedEventId, event.EventID)
		}
	default:
		t.Fatalf("expected ack to be enqueued")
	}
}

func TestHandleEventBatchDuplicateDeliveryIsIdempotentAndStillAcks(t *testing.T) {
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
			if ack.OriginNodeId != testNodeID(1) {
				t.Fatalf("unexpected ack origin on attempt %d: got=%d want=%d", attempt, ack.OriginNodeId, testNodeID(1))
			}
			if ack.AckedEventId != uint64(event.EventID) {
				t.Fatalf("unexpected ack event id on attempt %d: got=%d want=%d", attempt, ack.AckedEventId, event.EventID)
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
		t.Fatalf("expected duplicate delivery to converge to one local user, got %+v", users)
	}

	events, err := targetStore.ListEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("list target events: %v", err)
	}
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("expected duplicate delivery to be absorbed idempotently, got %+v", events)
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

	cursor, err := targetStore.GetOriginCursor(context.Background(), testNodeID(1))
	if err != nil {
		t.Fatalf("get origin cursor: %v", err)
	}
	if cursor.AppliedEventID != 0 {
		t.Fatalf("expected failed apply to keep applied event id at 0, got %d", cursor.AppliedEventID)
	}
}

func TestHandlePullEventsReturnsRequestedRange(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, sourceStore)

	ctx := context.Background()
	user, userEvent, err := sourceStore.CreateUser(ctx, store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  clusterSenderKey(9, 1),
		Body:    []byte("first"),
	}); err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if _, _, err := sourceStore.CreateMessage(ctx, store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  clusterSenderKey(9, 1),
		Body:    []byte("second"),
	}); err != nil {
		t.Fatalf("create second message: %v", err)
	}

	expected, err := sourceStore.ListEventsByOrigin(ctx, testNodeID(2), userEvent.EventID, 2)
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
				OriginNodeId: testNodeID(2),
				AfterEventId: uint64(userEvent.EventID),
				Limit:        2,
				RequestId:    7,
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
		if batch.PullRequestId != 7 {
			t.Fatalf("unexpected pull request id: got=%d want=7", batch.PullRequestId)
		}
		if batch.OriginNodeId != testNodeID(2) {
			t.Fatalf("unexpected origin node id: got=%d want=%d", batch.OriginNodeId, testNodeID(2))
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
		Sender:  clusterSenderKey(9, 1),
		Body:    []byte("snapshot message"),
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
				NodeId:       testNodeID(1),
				OriginNodeId: testNodeID(2),
				AckedEventId: 7,
			},
		},
	}

	if err := mgr.handleAck(sess, envelope); err != nil {
		t.Fatalf("handle ack: %v", err)
	}

	cursor, err := targetStore.GetPeerAckCursor(context.Background(), testNodeID(1), testNodeID(2))
	if err != nil {
		t.Fatalf("get peer ack cursor: %v", err)
	}
	if cursor.AckedEventID != 7 {
		t.Fatalf("unexpected acked event id: got=%d want=7", cursor.AckedEventID)
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
	if len(status.Peers) != 1 || status.Peers[0].NodeID != 0 || status.Peers[0].Connected {
		t.Fatalf("unexpected disconnected peer status: %+v", status)
	}

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	sess.outbound = true
	sess.noteRemoteOriginEvent(testNodeID(1), 9)
	if _, ok := sess.beginPendingPull(testNodeID(1), 3); !ok {
		t.Fatalf("expected pending pull to start")
	}
	if !sess.beginSnapshotRequest(store.SnapshotUsersPartition) {
		t.Fatalf("expected snapshot request to start")
	}
	now := time.Now().UTC()
	mgr.mu.Lock()
	mgr.peers[testNodeID(1)] = &peerState{}
	mgr.configuredPeers[0].nodeID = testNodeID(1)
	peer := mgr.peers[testNodeID(1)]
	peer.active = sess
	peer.trustedSession = sess
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
		len(peerStatus.Origins) != 1 ||
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
	if peerStatus.Origins[0].OriginNodeID != testNodeID(1) ||
		peerStatus.Origins[0].RemoteLastEventID != 9 ||
		!peerStatus.Origins[0].PendingCatchup {
		t.Fatalf("unexpected origin peer status: %+v", peerStatus.Origins[0])
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

func TestPerformTimeSyncKeepsPeerObservingOnSlowSample(t *testing.T) {
	t.Parallel()

	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{
			offsetMs: 17,
			rttMs:    mgr.cfg.ClockCredibleRttMs + 500,
		}, nil
	}

	if err := mgr.performTimeSync(sess); err != nil {
		t.Fatalf("perform time sync: %v", err)
	}

	state, reason := mgr.peerClockState(sess.peerID)
	if state != string(clockStateObserving) {
		t.Fatalf("expected peer to stay observing after slow sample, got state=%s reason=%s", state, reason)
	}
}

func TestPerformTimeSyncRejectsAfterRepeatedConfirmedSkew(t *testing.T) {
	t.Parallel()

	st := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, st)
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{
			offsetMs: mgr.cfg.MaxClockSkewMs + 500,
			rttMs:    10,
		}, nil
	}

	for i := 0; i < mgr.cfg.ClockRejectAfterSkewSamples-1; i++ {
		if err := mgr.performTimeSync(sess); err != nil {
			t.Fatalf("unexpected early rejection on sample %d: %v", i+1, err)
		}
	}
	if err := mgr.performTimeSync(sess); err == nil {
		t.Fatalf("expected confirmed skew to reject peer")
	}

	state, reason := mgr.peerClockState(sess.peerID)
	if state != string(clockStateRejected) {
		t.Fatalf("expected rejected peer state, got state=%s reason=%s", state, reason)
	}
}

func TestHandleEventBatchRejectsWhenNodeClockBecomesUnwritable(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	_, event, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "clock-guard-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	mgr.mu.Lock()
	peer := mgr.peers[testNodeID(1)]
	peer.trustedSession = nil
	peer.clockState = clockStateObserving
	mgr.lastTrustedClockSync = time.Now().UTC().Add(-2 * mgr.clockWriteGateGraceWindow())
	mgr.refreshNodeClockStateLocked()
	mgr.mu.Unlock()

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

	if err := mgr.handleEventBatch(sess, envelope); !errors.Is(err, errClockProtectionRejected) {
		t.Fatalf("expected clock protection rejection, got %v", err)
	}
	select {
	case envelope := <-sess.send:
		t.Fatalf("did not expect ack after unwritable rejection: %+v", envelope)
	default:
	}
}

func TestHandleSnapshotChunkRejectsFutureTimestamp(t *testing.T) {
	t.Parallel()

	sourceStore := newReplicationTestStore(t, "node-a", 1)
	targetStore := newReplicationTestStore(t, "node-b", 2)
	mgr := newReplicationTestManager(t, targetStore)

	user, _, err := sourceStore.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "snapshot-future",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	chunk, err := sourceStore.BuildSnapshotChunk(context.Background(), store.SnapshotUsersPartition)
	if err != nil {
		t.Fatalf("build snapshot chunk: %v", err)
	}
	row := chunk.GetRows()[0].GetUser()
	future := clock.Timestamp{
		WallTimeMs: mgr.clock.WallTimeMs() + mgr.cfg.MaxClockSkewMs + 2000,
		Logical:    0,
		NodeID:     user.NodeID,
	}
	row.UpdatedAtHlc = future.String()

	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	err = mgr.handleSnapshotChunk(sess, &internalproto.Envelope{
		NodeId: testNodeID(1),
		Body: &internalproto.Envelope_SnapshotChunk{
			SnapshotChunk: chunk,
		},
	})
	if err == nil {
		t.Fatalf("expected future snapshot chunk to be rejected")
	}
}
