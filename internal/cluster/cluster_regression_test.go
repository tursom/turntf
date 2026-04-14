package cluster

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/auth"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// Phase 2 fixed regression groups keep proven distributed semantics together:
// replication/ack, late catch-up, snapshot repair, message windows, concurrent
// convergence, multi-hop routing, and clock safety. Protocol-only regressions,
// including duplicate delivery idempotence and future timestamp rejection, stay
// in manager_test.go with the session-level state machine tests.

func TestRegressionTwoNodeReplicationAckAndReconnect(t *testing.T) {
	nodes := newClusterTestNodes(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := nodes["node-a"]
	nodeB := nodes["node-b"]

	startClusterTestNodes(t, nodeA, nodeB)
	waitForPeersActive(t, 5*time.Second,
		expectClusterPeer(nodeA, testNodeID(2)),
		expectClusterPeer(nodeB, testNodeID(1)),
	)

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
	mustJSON(t, doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", createUserBody, http.StatusCreated), &createdUser)
	userKey := clusterUserKey(createdUser.NodeID, createdUser.UserID)
	waitForReplicatedUser(t, 5*time.Second, nodeB, userKey)

	if _, _, err := nodeA.service.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: userKey,
		Sender:  clusterSenderKey(1, store.BootstrapAdminUserID),
		Body:    []byte("replicated payload"),
	}); err != nil {
		t.Fatalf("create message: %v", err)
	}
	waitForMessagesEqual(t, 5*time.Second, nodeB, userKey, 10, "replicated payload")
	assertPeerAckReached(t, 5*time.Second, nodeA, testNodeID(2), 1)

	oldSession := nodeA.CurrentSession(testNodeID(2))
	if oldSession == nil {
		t.Fatalf("expected active session before reconnect test")
	}
	oldSession.close()
	waitForSessionReconnect(t, 5*time.Second, nodeA, testNodeID(2), oldSession)
}

func TestRegressionLateJoinerCatchup(t *testing.T) {
	t.Run("without-duplicates", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		nodeA.Start(t)

		user, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "alice",
			PasswordHash: "hash-1",
		})
		if err != nil {
			t.Fatalf("create offline user: %v", err)
		}
		if _, _, err := nodeA.store.CreateMessage(context.Background(), store.CreateMessageParams{
			UserKey: user.Key(),
			Sender:  clusterSenderKey(9, 1),
			Body:    []byte("missed while offline"),
		}); err != nil {
			t.Fatalf("create offline message: %v", err)
		}

		nodeB.Start(t)
		waitForReplicatedUser(t, 5*time.Second, nodeB, user.Key())
		waitForMessagesEqual(t, 5*time.Second, nodeB, user.Key(), 10, "missed while offline")
		waitForEventCount(t, 5*time.Second, nodeB, 0, 10, 2)
	})

	t.Run("across-multiple-pull-batches", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		nodeA.Start(t)

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
				Sender:  clusterSenderKey(9, 1),
				Body:    []byte("message-" + strconv.Itoa(i)),
			}); err != nil {
				t.Fatalf("create backlog message %d: %v", i, err)
			}
		}

		nodeB.Start(t)
		waitForReplicatedUser(t, 5*time.Second, nodeB, user.Key())
		waitForReplicatedMessages(t, 5*time.Second, nodeB, user.Key(), 500, func(messages []store.Message) bool {
			return len(messages) == pullBatchSize+12
		})
		waitForEventCount(t, 5*time.Second, nodeB, 0, 500, pullBatchSize+13)
	})

	t.Run("converges-to-local-message-window", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}, MessageWindowSize: 5},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}, MessageWindowSize: 2},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		nodeA.Start(t)

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
				Sender:  clusterSenderKey(9, 1),
				Body:    []byte("message-" + strconv.Itoa(i)),
			}); err != nil {
				t.Fatalf("create offline message %d: %v", i, err)
			}
		}

		nodeB.Start(t)
		waitForMessagesEqual(t, 5*time.Second, nodeB, user.Key(), 10, "message-5", "message-4")
	})
}

func TestRegressionSnapshotRepair(t *testing.T) {
	nodes := newClusterTestNodes(t,
		clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
		clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
	)
	nodeA := nodes["node-a"]
	nodeB := nodes["node-b"]

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
		Sender:  clusterSenderKey(9, 1),
		Body:    []byte("snapshot-only-message"),
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

	startClusterTestNodes(t, nodeA, nodeB)
	waitForPeersActive(t, 5*time.Second,
		expectClusterPeer(nodeA, testNodeID(2)),
		expectClusterPeer(nodeB, testNodeID(1)),
	)

	eventually(t, 5*time.Second, func() bool {
		if sess := nodeA.CurrentSession(testNodeID(2)); sess != nil {
			nodeA.manager.sendSnapshotDigest(sess)
		}

		if _, err := nodeB.store.GetUser(ctx, user.Key()); err != nil {
			return false
		}
		messages, err := nodeB.store.ListMessagesByUser(ctx, user.Key(), 10)
		return err == nil && len(messages) == 1 && string(messages[0].Body) == "snapshot-only-message"
	})

	nodeBEvents, err := nodeB.store.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list node B events after repair: %v", err)
	}
	if len(nodeBEvents) != 0 {
		t.Fatalf("expected snapshot repair to avoid event log replay, got %+v", nodeBEvents)
	}
}

func TestRegressionMessageWindowConvergence(t *testing.T) {
	t.Run("same-size", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}, MessageWindowSize: 2},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}, MessageWindowSize: 2},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		startClusterTestNodes(t, nodeA, nodeB)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeB, testNodeID(1)),
		)

		var createdUser struct {
			NodeID int64 `json:"node_id"`
			UserID int64 `json:"user_id"`
		}
		mustJSON(t, doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", map[string]any{
			"username": "window-user",
			"password": "password-1",
		}, http.StatusCreated), &createdUser)
		userKey := clusterUserKey(createdUser.NodeID, createdUser.UserID)
		waitForReplicatedUser(t, 5*time.Second, nodeB, userKey)

		for i := 1; i <= 4; i++ {
			if _, _, err := nodeA.service.CreateMessage(context.Background(), store.CreateMessageParams{
				UserKey: userKey,
				Sender:  clusterSenderKey(1, store.BootstrapAdminUserID),
				Body:    []byte("message-" + strconv.Itoa(i)),
			}); err != nil {
				t.Fatalf("create message %d: %v", i, err)
			}
		}

		waitForMessagesEqual(t, 5*time.Second, nodeA, userKey, 10, "message-4", "message-3")
		waitForMessagesEqual(t, 5*time.Second, nodeB, userKey, 10, "message-4", "message-3")
	})

	t.Run("mismatch-size", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}, MessageWindowSize: 3},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}, MessageWindowSize: 2},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		startClusterTestNodes(t, nodeA, nodeB)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeB, testNodeID(1)),
		)

		var createdUser struct {
			NodeID int64 `json:"node_id"`
			UserID int64 `json:"user_id"`
		}
		mustJSON(t, doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", map[string]any{
			"username": "window-mismatch-user",
			"password": "password-1",
		}, http.StatusCreated), &createdUser)
		userKey := clusterUserKey(createdUser.NodeID, createdUser.UserID)
		waitForReplicatedUser(t, 5*time.Second, nodeB, userKey)

		for i := 1; i <= 4; i++ {
			if _, _, err := nodeA.service.CreateMessage(context.Background(), store.CreateMessageParams{
				UserKey: userKey,
				Sender:  clusterSenderKey(1, store.BootstrapAdminUserID),
				Body:    []byte("message-" + strconv.Itoa(i)),
			}); err != nil {
				t.Fatalf("create message %d: %v", i, err)
			}
		}

		waitForMessagesEqual(t, 5*time.Second, nodeA, userKey, 10, "message-4", "message-3", "message-2")
		waitForMessagesEqual(t, 5*time.Second, nodeB, userKey, 10, "message-4", "message-3")
	})
}

func TestRegressionThreeNodeConcurrentConvergence(t *testing.T) {
	t.Run("field-level-lww", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b", "node-c"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}},
			clusterTestNodeSpec{Name: "node-c", Slot: 3, PeerNames: []string{"node-a", "node-b"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]
		nodeC := nodes["node-c"]

		startClusterTestNodes(t, nodeA, nodeB, nodeC)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeA, testNodeID(3)),
			expectClusterPeer(nodeB, testNodeID(1)),
			expectClusterPeer(nodeB, testNodeID(3)),
			expectClusterPeer(nodeC, testNodeID(1)),
			expectClusterPeer(nodeC, testNodeID(2)),
		)

		var createdUser struct {
			NodeID int64 `json:"node_id"`
			UserID int64 `json:"user_id"`
		}
		mustJSON(t, doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", map[string]any{
			"username": "cluster-user",
			"password": "password-1",
			"profile": map[string]any{
				"display_name": "Before Merge",
			},
		}, http.StatusCreated), &createdUser)
		userKey := clusterUserKey(createdUser.NodeID, createdUser.UserID)

		waitForReplicatedUser(t, 5*time.Second, nodeB, userKey)
		waitForReplicatedUser(t, 5*time.Second, nodeC, userKey)

		errCh := make(chan error, 2)
		start := make(chan struct{})

		go func() {
			<-start
			_, err := doJSONRequest(nodeB.APIBaseURL(), http.MethodPatch, clusterUserPath(createdUser.NodeID, createdUser.UserID), map[string]any{
				"username": "cluster-user-renamed",
			}, http.StatusOK)
			errCh <- err
		}()

		go func() {
			<-start
			_, err := doJSONRequest(nodeC.APIBaseURL(), http.MethodPatch, clusterUserPath(createdUser.NodeID, createdUser.UserID), map[string]any{
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

		waitForUsersConverged(t, 5*time.Second, userKey, "cluster-user-renamed", `{"display_name":"Merged Profile"}`, nodeA, nodeB, nodeC)
	})

	t.Run("delete-wins", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b", "node-c"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}},
			clusterTestNodeSpec{Name: "node-c", Slot: 3, PeerNames: []string{"node-a", "node-b"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]
		nodeC := nodes["node-c"]

		startClusterTestNodes(t, nodeA, nodeB, nodeC)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeA, testNodeID(3)),
			expectClusterPeer(nodeB, testNodeID(1)),
			expectClusterPeer(nodeB, testNodeID(3)),
			expectClusterPeer(nodeC, testNodeID(1)),
			expectClusterPeer(nodeC, testNodeID(2)),
		)

		var createdUser struct {
			NodeID int64 `json:"node_id"`
			UserID int64 `json:"user_id"`
		}
		mustJSON(t, doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", map[string]any{
			"username": "delete-user",
			"password": "password-1",
		}, http.StatusCreated), &createdUser)
		userKey := clusterUserKey(createdUser.NodeID, createdUser.UserID)
		waitForReplicatedUser(t, 5*time.Second, nodeB, userKey)
		waitForReplicatedUser(t, 5*time.Second, nodeC, userKey)

		errCh := make(chan error, 2)
		start := make(chan struct{})

		go func() {
			<-start
			_, err := doJSONRequest(nodeB.APIBaseURL(), http.MethodPatch, clusterUserPath(createdUser.NodeID, createdUser.UserID), map[string]any{
				"profile": map[string]any{
					"display_name": "Should Lose To Delete",
				},
			}, http.StatusOK)
			errCh <- err
		}()

		go func() {
			<-start
			_, err := doJSONRequest(nodeC.APIBaseURL(), http.MethodDelete, clusterUserPath(createdUser.NodeID, createdUser.UserID), nil, http.StatusOK)
			errCh <- err
		}()

		close(start)
		for range 2 {
			if err := <-errCh; err != nil {
				t.Fatalf("concurrent delete/update request failed: %v", err)
			}
		}

		waitForUserDeletedEverywhere(t, 5*time.Second, userKey, nodeA, nodeB, nodeC)
	})
}

func TestRegressionMultiHopRoutingAndLoggedInUsers(t *testing.T) {
	t.Run("transient-packet", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}},
			clusterTestNodeSpec{Name: "node-c", Slot: 3, PeerNames: []string{"node-b"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]
		nodeC := nodes["node-c"]

		startClusterTestNodes(t, nodeA, nodeB, nodeC)
		waitForPeersActive(t, 10*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeB, testNodeID(1)),
			expectClusterPeer(nodeB, testNodeID(3)),
			expectClusterPeer(nodeC, testNodeID(2)),
		)

		passwordHash, err := auth.HashPassword("charlie-password")
		if err != nil {
			t.Fatalf("hash password: %v", err)
		}
		charlie, _, err := nodeC.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "charlie",
			PasswordHash: passwordHash,
			Role:         store.RoleUser,
		})
		if err != nil {
			t.Fatalf("create charlie: %v", err)
		}

		serverC := newClusterHTTPTestServer(t, nodeC.server.Handler)
		conn := dialClusterClientWebSocket(t, serverC.URL)
		defer conn.Close()
		writeClusterClientEnvelope(t, conn, &internalproto.ClientEnvelope{
			Body: &internalproto.ClientEnvelope_Login{
				Login: &internalproto.LoginRequest{
					User:     &internalproto.UserRef{NodeId: charlie.NodeID, UserId: charlie.ID},
					Password: "charlie-password",
				},
			},
		})
		if loginResp := readClusterServerEnvelope(t, conn).GetLoginResponse(); loginResp == nil {
			t.Fatalf("expected login response")
		}

		nodeC.manager.broadcastRoutingUpdate()
		nodeB.manager.broadcastRoutingUpdate()
		waitForClusterRoutes(t, 10*time.Second, nodeA, nodeC.id)

		if err := nodeA.manager.RouteTransientPacket(context.Background(), store.TransientPacket{
			PacketID:     1,
			SourceNodeID: nodeA.id,
			TargetNodeID: nodeC.id,
			Recipient:    charlie.Key(),
			Sender:       clusterSenderKey(9, 1),
			Body:         []byte("over-mesh"),
			DeliveryMode: store.DeliveryModeBestEffort,
			TTLHops:      8,
		}); err != nil {
			t.Fatalf("route transient packet: %v", err)
		}

		packet := readClusterServerEnvelope(t, conn).GetPacketPushed()
		if packet == nil || packet.Packet == nil || string(packet.Packet.GetBody()) != "over-mesh" {
			t.Fatalf("unexpected routed packet: %+v", packet)
		}
	})

	t.Run("logged-in-users-query", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a", "node-c"}},
			clusterTestNodeSpec{Name: "node-c", Slot: 3, PeerNames: []string{"node-b"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]
		nodeC := nodes["node-c"]

		startClusterTestNodes(t, nodeA, nodeB, nodeC)
		waitForPeersActive(t, 10*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeB, testNodeID(1)),
			expectClusterPeer(nodeB, testNodeID(3)),
			expectClusterPeer(nodeC, testNodeID(2)),
		)

		passwordHash, err := auth.HashPassword("alice-password")
		if err != nil {
			t.Fatalf("hash alice password: %v", err)
		}
		alice, _, err := nodeA.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "alice",
			PasswordHash: passwordHash,
			Role:         store.RoleUser,
		})
		if err != nil {
			t.Fatalf("create alice: %v", err)
		}

		charlieHash, err := auth.HashPassword("charlie-password")
		if err != nil {
			t.Fatalf("hash charlie password: %v", err)
		}
		charlie, _, err := nodeC.store.CreateUser(context.Background(), store.CreateUserParams{
			Username:     "charlie",
			PasswordHash: charlieHash,
			Role:         store.RoleUser,
		})
		if err != nil {
			t.Fatalf("create charlie: %v", err)
		}

		serverC := newClusterHTTPTestServer(t, nodeC.server.Handler)
		connC := dialClusterClientWebSocket(t, serverC.URL)
		defer connC.Close()
		writeClusterClientEnvelope(t, connC, &internalproto.ClientEnvelope{
			Body: &internalproto.ClientEnvelope_Login{
				Login: &internalproto.LoginRequest{
					User:     &internalproto.UserRef{NodeId: charlie.NodeID, UserId: charlie.ID},
					Password: "charlie-password",
				},
			},
		})
		if loginResp := readClusterServerEnvelope(t, connC).GetLoginResponse(); loginResp == nil {
			t.Fatalf("expected login response for charlie")
		}

		nodeC.manager.broadcastRoutingUpdate()
		nodeB.manager.broadcastRoutingUpdate()
		waitForClusterRoutes(t, 10*time.Second, nodeA, nodeC.id)

		serverA := newClusterHTTPTestServer(t, nodeA.server.Handler)
		connA := dialClusterClientWebSocket(t, serverA.URL)
		defer connA.Close()
		writeClusterClientEnvelope(t, connA, &internalproto.ClientEnvelope{
			Body: &internalproto.ClientEnvelope_Login{
				Login: &internalproto.LoginRequest{
					User:     &internalproto.UserRef{NodeId: alice.NodeID, UserId: alice.ID},
					Password: "alice-password",
				},
			},
		})
		if loginResp := readClusterServerEnvelope(t, connA).GetLoginResponse(); loginResp == nil {
			t.Fatalf("expected login response for alice")
		}

		writeClusterClientEnvelope(t, connA, &internalproto.ClientEnvelope{
			Body: &internalproto.ClientEnvelope_ListNodeLoggedInUsers{
				ListNodeLoggedInUsers: &internalproto.ListNodeLoggedInUsersRequest{
					RequestId: 1005,
					NodeId:    nodeC.id,
				},
			},
		})

		resp := readClusterServerEnvelope(t, connA).GetListNodeLoggedInUsersResponse()
		if resp == nil || resp.RequestId != 1005 || resp.TargetNodeId != nodeC.id || resp.Count != 1 || len(resp.Items) != 1 {
			t.Fatalf("unexpected logged-in users response: %+v", resp)
		}
		if resp.Items[0].GetNodeId() != charlie.NodeID || resp.Items[0].GetUserId() != charlie.ID || resp.Items[0].GetUsername() != "charlie" {
			t.Fatalf("unexpected logged-in user item: %+v", resp.Items[0])
		}
	})
}

func TestRegressionClockSafety(t *testing.T) {
	t.Run("write-gate-blocked-until-initial-clock-sync", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		nodeA.Start(t)
		doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", map[string]any{
			"username": "gated-user",
			"password": "password-1",
		}, http.StatusServiceUnavailable)

		nodeB.Start(t)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeB, testNodeID(1)),
		)

		doJSON(t, nodeA.APIBaseURL(), http.MethodPost, "/users", map[string]any{
			"username": "synced-user",
			"password": "password-1",
		}, http.StatusCreated)
	})

	t.Run("skewed-peer-rejected", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}, ClockSkewMs: 5000},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		startClusterTestNodes(t, nodeA, nodeB)

		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if nodeA.ActivePeer(testNodeID(2)) || nodeB.ActivePeer(testNodeID(1)) {
				t.Fatalf("expected skewed peer to be rejected")
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	t.Run("skew-disabled-allows-peer", func(t *testing.T) {
		nodes := newClusterTestNodes(t,
			clusterTestNodeSpec{Name: "node-a", Slot: 1, PeerNames: []string{"node-b"}, MaxClockSkewMs: -1},
			clusterTestNodeSpec{Name: "node-b", Slot: 2, PeerNames: []string{"node-a"}, ClockSkewMs: 5000, MaxClockSkewMs: -1},
		)
		nodeA := nodes["node-a"]
		nodeB := nodes["node-b"]

		startClusterTestNodes(t, nodeA, nodeB)
		waitForPeersActive(t, 5*time.Second,
			expectClusterPeer(nodeA, testNodeID(2)),
			expectClusterPeer(nodeB, testNodeID(1)),
		)
	})
}
