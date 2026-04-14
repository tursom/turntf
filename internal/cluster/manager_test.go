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
				manager:  mgr,
				outbound: true,
				configuredPeer: &configuredPeer{
					URL:    "ws://127.0.0.1:9081/internal/cluster/ws",
					nodeID: testNodeID(3),
				},
				send: make(chan *internalproto.Envelope, 1),
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

func TestHandleQueryLoggedInUsersRequestRespondsWithLocalUsers(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	mgr.SetLoggedInUsersProvider(func(context.Context) ([]app.LoggedInUserSummary, error) {
		return []app.LoggedInUserSummary{{
			NodeID:   testNodeID(1),
			UserID:   1025,
			Username: "alice",
		}}, nil
	})
	sess := &session{
		manager:         mgr,
		peerID:          testNodeID(2),
		connectionID:    1,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	mgr.peers[testNodeID(2)] = &peerState{
		active:   sess,
		sessions: map[uint64]*session{sess.connectionID: sess},
	}
	mgr.routingTable[testNodeID(2)] = routeEntry{
		destinationNodeID:    testNodeID(2),
		nextHopPeer:          testNodeID(2),
		selectedConnectionID: sess.connectionID,
	}

	err := mgr.handleQueryLoggedInUsersRequest(sess, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: &internalproto.QueryLoggedInUsersRequest{
				RequestId:     7,
				TargetNodeId:  testNodeID(1),
				OriginNodeId:  testNodeID(2),
				RemainingHops: defaultLoggedInUsersQueryMaxHops,
			},
		},
	})
	if err != nil {
		t.Fatalf("handle query logged-in users request: %v", err)
	}

	select {
	case envelope := <-sess.send:
		resp := envelope.GetQueryLoggedInUsersResponse()
		if resp == nil || resp.RequestId != 7 || resp.TargetNodeId != testNodeID(1) || resp.OriginNodeId != testNodeID(2) || len(resp.Items) != 1 {
			t.Fatalf("unexpected query logged-in users response: %+v", resp)
		}
		if resp.Items[0].GetUserId() != 1025 || resp.Items[0].GetUsername() != "alice" {
			t.Fatalf("unexpected logged-in user item: %+v", resp.Items[0])
		}
	case <-time.After(time.Second):
		t.Fatalf("expected query logged-in users response")
	}
}

func TestQueryLoggedInUsersUsesBestRoute(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	targetNodeID := testNodeID(2)
	sess := &session{
		manager:         mgr,
		peerID:          targetNodeID,
		connectionID:    1,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	mgr.peers[targetNodeID] = &peerState{
		active:   sess,
		sessions: map[uint64]*session{sess.connectionID: sess},
	}
	mgr.routingTable[targetNodeID] = routeEntry{
		destinationNodeID:    targetNodeID,
		nextHopPeer:          targetNodeID,
		selectedConnectionID: sess.connectionID,
	}

	resultCh := make(chan []app.LoggedInUserSummary, 1)
	errCh := make(chan error, 1)
	go func() {
		users, err := mgr.QueryLoggedInUsers(context.Background(), targetNodeID)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- users
	}()

	var requestID uint64
	select {
	case envelope := <-sess.send:
		req := envelope.GetQueryLoggedInUsersRequest()
		if req == nil || req.TargetNodeId != targetNodeID || req.OriginNodeId != testNodeID(1) || req.RemainingHops != defaultLoggedInUsersQueryMaxHops {
			t.Fatalf("unexpected query request: %+v", envelope)
		}
		requestID = req.RequestId
	case <-time.After(time.Second):
		t.Fatalf("expected query request to be enqueued")
	}

	if err := mgr.handleQueryLoggedInUsersResponse(sess, &internalproto.Envelope{
		NodeId: targetNodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersResponse{
			QueryLoggedInUsersResponse: &internalproto.QueryLoggedInUsersResponse{
				RequestId:     requestID,
				TargetNodeId:  targetNodeID,
				OriginNodeId:  testNodeID(1),
				RemainingHops: defaultLoggedInUsersQueryMaxHops,
				Items: []*internalproto.ClusterLoggedInUser{{
					NodeId:   targetNodeID,
					UserId:   2048,
					Username: "bob",
				}},
			},
		},
	}); err != nil {
		t.Fatalf("handle query logged-in users response: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("query logged-in users returned error: %v", err)
	case users := <-resultCh:
		if len(users) != 1 || users[0].NodeID != targetNodeID || users[0].UserID != 2048 || users[0].Username != "bob" {
			t.Fatalf("unexpected query logged-in users result: %+v", users)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected query logged-in users result")
	}
}

func TestQueryLoggedInUsersTimesOutLocally(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	targetNodeID := testNodeID(2)
	sess := &session{
		manager:         mgr,
		peerID:          targetNodeID,
		connectionID:    1,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	mgr.peers[targetNodeID] = &peerState{
		active:   sess,
		sessions: map[uint64]*session{sess.connectionID: sess},
	}
	mgr.routingTable[targetNodeID] = routeEntry{
		destinationNodeID:    targetNodeID,
		nextHopPeer:          targetNodeID,
		selectedConnectionID: sess.connectionID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := mgr.QueryLoggedInUsers(ctx, targetNodeID)
	if err == nil {
		t.Fatalf("expected query logged-in users timeout")
	}
	if !errors.Is(err, app.ErrServiceUnavailable) || !strings.Contains(err.Error(), "timed out querying node") {
		t.Fatalf("unexpected query logged-in users error: %v", err)
	}

	select {
	case envelope := <-sess.send:
		req := envelope.GetQueryLoggedInUsersRequest()
		if req == nil || req.TargetNodeId != targetNodeID {
			t.Fatalf("unexpected query request: %+v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected query request before local timeout")
	}

	mgr.mu.Lock()
	pendingCount := len(mgr.pendingLoggedInUsers)
	mgr.mu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("expected pending logged-in users query to be cleared after timeout, got %d", pendingCount)
	}
}

func TestHandleQueryLoggedInUsersRequestForwardsToNextHop(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	incoming := &session{
		manager:         mgr,
		peerID:          testNodeID(4),
		connectionID:    4,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	nextHop := &session{
		manager:         mgr,
		peerID:          testNodeID(2),
		connectionID:    2,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	mgr.peers[testNodeID(2)] = &peerState{
		active:   nextHop,
		sessions: map[uint64]*session{nextHop.connectionID: nextHop},
	}
	mgr.routingTable[testNodeID(3)] = routeEntry{
		destinationNodeID:    testNodeID(3),
		nextHopPeer:          testNodeID(2),
		selectedConnectionID: nextHop.connectionID,
	}

	err := mgr.handleQueryLoggedInUsersRequest(incoming, &internalproto.Envelope{
		NodeId: testNodeID(4),
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: &internalproto.QueryLoggedInUsersRequest{
				RequestId:     11,
				TargetNodeId:  testNodeID(3),
				OriginNodeId:  testNodeID(4),
				RemainingHops: 3,
			},
		},
	})
	if err != nil {
		t.Fatalf("handle query logged-in users request: %v", err)
	}

	select {
	case envelope := <-nextHop.send:
		req := envelope.GetQueryLoggedInUsersRequest()
		if req == nil || req.RequestId != 11 || req.TargetNodeId != testNodeID(3) || req.OriginNodeId != testNodeID(4) || req.RemainingHops != 2 {
			t.Fatalf("unexpected forwarded query request: %+v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected forwarded query request")
	}
}

func TestHandleQueryLoggedInUsersRequestReturnsHopLimitError(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	incoming := &session{
		manager:         mgr,
		peerID:          testNodeID(4),
		connectionID:    4,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	origin := &session{
		manager:         mgr,
		peerID:          testNodeID(2),
		connectionID:    2,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	mgr.peers[testNodeID(2)] = &peerState{
		active:   origin,
		sessions: map[uint64]*session{origin.connectionID: origin},
	}
	mgr.routingTable[testNodeID(2)] = routeEntry{
		destinationNodeID:    testNodeID(2),
		nextHopPeer:          testNodeID(2),
		selectedConnectionID: origin.connectionID,
	}

	err := mgr.handleQueryLoggedInUsersRequest(incoming, &internalproto.Envelope{
		NodeId: testNodeID(4),
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: &internalproto.QueryLoggedInUsersRequest{
				RequestId:     12,
				TargetNodeId:  testNodeID(3),
				OriginNodeId:  testNodeID(2),
				RemainingHops: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("handle query logged-in users request: %v", err)
	}

	select {
	case envelope := <-origin.send:
		resp := envelope.GetQueryLoggedInUsersResponse()
		if resp == nil || resp.RequestId != 12 || resp.OriginNodeId != testNodeID(2) || resp.TargetNodeId != testNodeID(3) || resp.ErrorCode != "service_unavailable" || !strings.Contains(resp.ErrorMessage, "hop limit") {
			t.Fatalf("unexpected hop limit response: %+v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected hop limit response")
	}
}

func TestHandleQueryLoggedInUsersResponseForwardsByRoute(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	incoming := &session{
		manager:         mgr,
		peerID:          testNodeID(3),
		connectionID:    3,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	originRoute := &session{
		manager:         mgr,
		peerID:          testNodeID(2),
		connectionID:    2,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}
	mgr.peers[testNodeID(2)] = &peerState{
		active:   originRoute,
		sessions: map[uint64]*session{originRoute.connectionID: originRoute},
	}
	mgr.routingTable[testNodeID(4)] = routeEntry{
		destinationNodeID:    testNodeID(4),
		nextHopPeer:          testNodeID(2),
		selectedConnectionID: originRoute.connectionID,
	}

	err := mgr.handleQueryLoggedInUsersResponse(incoming, &internalproto.Envelope{
		NodeId: testNodeID(3),
		Body: &internalproto.Envelope_QueryLoggedInUsersResponse{
			QueryLoggedInUsersResponse: &internalproto.QueryLoggedInUsersResponse{
				RequestId:     13,
				TargetNodeId:  testNodeID(5),
				OriginNodeId:  testNodeID(4),
				RemainingHops: 2,
				Items: []*internalproto.ClusterLoggedInUser{{
					NodeId:   testNodeID(5),
					UserId:   4097,
					Username: "routed",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("handle query logged-in users response: %v", err)
	}

	select {
	case envelope := <-originRoute.send:
		resp := envelope.GetQueryLoggedInUsersResponse()
		if resp == nil || resp.RequestId != 13 || resp.OriginNodeId != testNodeID(4) || resp.RemainingHops != 1 || len(resp.Items) != 1 {
			t.Fatalf("unexpected forwarded query response: %+v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected forwarded query response")
	}

	select {
	case envelope := <-incoming.send:
		t.Fatalf("response should not be returned on incoming session: %+v", envelope)
	default:
	}
}

func TestHandleQueryLoggedInUsersResponseIgnoresLateOriginResponse(t *testing.T) {
	t.Parallel()

	logOutput := captureClusterLogs(t)
	mgr := newHandshakeTestManager(t)
	incoming := &session{
		manager:         mgr,
		peerID:          testNodeID(2),
		connectionID:    2,
		send:            make(chan *internalproto.Envelope, 1),
		supportsRouting: true,
	}

	err := mgr.handleQueryLoggedInUsersResponse(incoming, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_QueryLoggedInUsersResponse{
			QueryLoggedInUsersResponse: &internalproto.QueryLoggedInUsersResponse{
				RequestId:     99,
				TargetNodeId:  testNodeID(2),
				OriginNodeId:  testNodeID(1),
				RemainingHops: 3,
				Items: []*internalproto.ClusterLoggedInUser{{
					NodeId:   testNodeID(2),
					UserId:   2048,
					Username: "late",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("handle late query logged-in users response: %v", err)
	}
	if !strings.Contains(logOutput.String(), `"event":"query_logged_in_users_response_ignored"`) {
		t.Fatalf("expected late response ignore log, got %s", logOutput.String())
	}

	mgr.mu.Lock()
	pendingCount := len(mgr.pendingLoggedInUsers)
	mgr.mu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("expected no pending origin query after late response, got %d", pendingCount)
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
			{URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
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

func TestHandleHelloLogsPeerJoinWhenPeerBecomesActive(t *testing.T) {
	mgr := newHandshakeTestManager(t)

	logOutput := captureClusterLogs(t)

	sess := &session{
		manager: mgr,
		send:    make(chan *internalproto.Envelope, 1),
	}
	envelope := mustHelloEnvelope(t, testNodeID(2), &internalproto.Hello{
		NodeId:            testNodeID(2),
		AdvertiseAddr:     websocketPath,
		ProtocolVersion:   internalproto.ProtocolVersion,
		SnapshotVersion:   internalproto.SnapshotVersion,
		MessageWindowSize: store.DefaultMessageWindowSize,
	})

	if err := mgr.handleHello(sess, envelope); err != nil {
		t.Fatalf("handle hello: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		return strings.Contains(logOutput.String(), `"event":"peer_joined"`)
	})
	if !strings.Contains(logOutput.String(), `"peer_node_id":8192`) {
		t.Fatalf("expected peer join log to include peer node id, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"direction":"inbound"`) {
		t.Fatalf("expected peer join log to include direction, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"local_node_id":4096`) {
		t.Fatalf("expected peer join log to include local node id, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"event":"peer_hello_accepted"`) {
		t.Fatalf("expected hello accepted log, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"event":"time_sync_succeeded"`) {
		t.Fatalf("expected time sync success log, got %q", logOutput.String())
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

func TestHandleHelloEnqueuesPullEventsWhenBehind(t *testing.T) {
	t.Parallel()

	targetStore := newReplicationTestStore(t, "node-b", 2)
	if err := targetStore.RecordOriginApplied(context.Background(), testNodeID(1), 2); err != nil {
		t.Fatalf("record origin applied: %v", err)
	}
	mgr := newReplicationTestManager(t, targetStore)

	sess := &session{
		manager: mgr,
		send:    make(chan *internalproto.Envelope, 1),
	}
	hello := &internalproto.Hello{
		NodeId:          testNodeID(1),
		AdvertiseAddr:   websocketPath,
		ProtocolVersion: internalproto.ProtocolVersion,
		OriginProgress: []*internalproto.OriginProgress{{
			OriginNodeId: testNodeID(1),
			LastEventId:  4,
		}},
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
		if pull.OriginNodeId != testNodeID(1) {
			t.Fatalf("unexpected pull origin node id: got=%d want=%d", pull.OriginNodeId, testNodeID(1))
		}
		if pull.AfterEventId != 2 {
			t.Fatalf("unexpected pull after event id: got=%d want=2", pull.AfterEventId)
		}
		if pull.Limit != pullBatchSize {
			t.Fatalf("unexpected pull limit: got=%d want=%d", pull.Limit, pullBatchSize)
		}
		if pull.RequestId == 0 {
			t.Fatalf("expected pull request id")
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
