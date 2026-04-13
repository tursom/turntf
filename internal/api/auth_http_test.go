package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type authenticatedTestAPI struct {
	handler http.Handler
	http    *HTTP
}

type fakeClusterStatusSink struct {
	status app.ClusterStatus
}

func (s fakeClusterStatusSink) Publish(store.Event) {}

func (s fakeClusterStatusSink) Status(context.Context) (app.ClusterStatus, error) {
	return s.status, nil
}

func (s fakeClusterStatusSink) ConfiguredPeerNodeIDs() []int64 {
	ids := make([]int64, 0, len(s.status.Peers))
	for _, peer := range s.status.Peers {
		if peer.NodeID > 0 {
			ids = append(ids, peer.NodeID)
		}
	}
	return ids
}

type fakeLoggedInUsersSink struct {
	fakeClusterStatusSink
	query func(context.Context, int64) ([]app.LoggedInUserSummary, error)
}

func (s fakeLoggedInUsersSink) QueryLoggedInUsers(ctx context.Context, nodeID int64) ([]app.LoggedInUserSummary, error) {
	if s.query == nil {
		return nil, nil
	}
	return s.query(ctx, nodeID)
}

func TestAuthenticatedHTTPLoginAndAuthorization(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "unauthorized",
		"password": "unauthorized-password",
	}, nil, http.StatusUnauthorized)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  testNodeID(1),
		"user_id":  store.BootstrapAdminUserID,
		"password": "wrong",
	}, nil, http.StatusUnauthorized)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/metrics", nil, nil, http.StatusUnauthorized)
	metrics := doPlain(t, testAPI.handler, http.MethodGet, "/metrics", map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	if !strings.Contains(metrics, "notifier_write_gate_ready") {
		t.Fatalf("metrics missing write gate gauge: %s", metrics)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK)

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userPath(adminKey.NodeID, adminKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "bob",
		"password": "bob-password",
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("hello"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(adminKey.NodeID, adminKey.UserID), map[string]any{
		"body": []byte("forbidden"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	var channel struct {
		NodeID int64  `json:"node_id"`
		UserID int64  `json:"user_id"`
		Role   string `json:"role"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "alerts",
		"role":     store.RoleChannel,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &channel)
	if channel.Role != store.RoleChannel {
		t.Fatalf("expected channel role, got %+v", channel)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  channel.NodeID,
		"user_id":  channel.UserID,
		"password": "anything",
	}, nil, http.StatusUnauthorized)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channel.NodeID, channel.UserID), map[string]any{
		"body": []byte("not subscribed"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"channel_node_id": channel.NodeID,
		"channel_user_id": channel.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channel.NodeID, channel.UserID), map[string]any{
		"body": []byte("channel message"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	broadcastPath := userMessagesPath(testNodeID(1), store.BroadcastUserID)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, broadcastPath, map[string]any{
		"body": []byte("ordinary broadcast"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, broadcastPath, map[string]any{
		"body": []byte("admin broadcast"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	var aliceMessages struct {
		Items []authMessageItem `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID)+"?limit=20", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &aliceMessages)
	if !responseMessagesContainBody(aliceMessages.Items, "channel message") || !responseMessagesContainBody(aliceMessages.Items, "admin broadcast") {
		t.Fatalf("expected alice message list to include channel and broadcast: %+v", aliceMessages)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(channel.NodeID, channel.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(channel.NodeID, channel.UserID), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
}

func TestClusterNodesHTTPRequiresAuthenticationAndAllowsUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeClusterStatusSink{
		status: app.ClusterStatus{
			NodeID: testNodeID(1),
			Peers: []app.ClusterPeerStatus{
				{NodeID: testNodeID(2), ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws", Connected: true},
				{NodeID: testNodeID(3), ConfiguredURL: "ws://127.0.0.1:9082/internal/cluster/ws", Connected: false},
				{ConfiguredURL: "ws://127.0.0.1:9083/internal/cluster/ws", Connected: true},
			},
		},
	})

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes", nil, nil, http.StatusUnauthorized)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	var nodes struct {
		Nodes []struct {
			NodeID        int64  `json:"node_id"`
			IsLocal       bool   `json:"is_local"`
			ConfiguredURL string `json:"configured_url"`
		} `json:"nodes"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &nodes)
	if len(nodes.Nodes) != 2 {
		t.Fatalf("unexpected cluster nodes: %+v", nodes)
	}
	if !nodes.Nodes[0].IsLocal || nodes.Nodes[0].NodeID != testNodeID(1) || nodes.Nodes[0].ConfiguredURL != "" {
		t.Fatalf("unexpected local cluster node: %+v", nodes.Nodes[0])
	}
	if nodes.Nodes[1].IsLocal || nodes.Nodes[1].NodeID != testNodeID(2) || nodes.Nodes[1].ConfiguredURL != "ws://127.0.0.1:9081/internal/cluster/ws" {
		t.Fatalf("unexpected peer cluster node: %+v", nodes.Nodes[1])
	}
}

func TestNodeLoggedInUsersHTTPRequiresAuthenticationAndReturnsDeduplicatedUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/4096/logged-in-users", nil, nil, http.StatusUnauthorized)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "bob", "bob-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	connA1 := dialClientWebSocket(t, server.URL)
	defer connA1.Close()
	loginClientWebSocket(t, connA1, aliceKey, "alice-password")

	connA2 := dialClientWebSocket(t, server.URL)
	defer connA2.Close()
	loginClientWebSocket(t, connA2, aliceKey, "alice-password")

	connB := dialClientWebSocket(t, server.URL)
	defer connB.Close()
	loginClientWebSocket(t, connB, bobKey, "bob-password")

	var resp struct {
		TargetNodeID int64 `json:"target_node_id"`
		Count        int   `json:"count"`
		Items        []struct {
			NodeID   int64  `json:"node_id"`
			UserID   int64  `json:"user_id"`
			Username string `json:"username"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/4096/logged-in-users", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &resp)
	if resp.TargetNodeID != testNodeID(1) || resp.Count != 2 || len(resp.Items) != 2 {
		t.Fatalf("unexpected logged-in users response: %+v", resp)
	}
	if resp.Items[0].NodeID != aliceKey.NodeID || resp.Items[0].UserID != aliceKey.UserID || resp.Items[0].Username != "alice" {
		t.Fatalf("unexpected first logged-in user: %+v", resp.Items[0])
	}
	if resp.Items[1].NodeID != bobKey.NodeID || resp.Items[1].UserID != bobKey.UserID || resp.Items[1].Username != "bob" {
		t.Fatalf("unexpected second logged-in user: %+v", resp.Items[1])
	}
}

func TestNodeLoggedInUsersHTTPReturns503WhenRemoteNodeUnavailable(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeLoggedInUsersSink{
		query: func(context.Context, int64) ([]app.LoggedInUserSummary, error) {
			return nil, fmt.Errorf("%w: node 8192 is not connected", app.ErrServiceUnavailable)
		},
	})

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/8192/logged-in-users", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusServiceUnavailable)
}

func newAuthenticatedTestAPI(t *testing.T) authenticatedTestAPI {
	t.Helper()
	return newAuthenticatedTestAPIWithSink(t, nil)
}

func newAuthenticatedTestAPIWithSink(t *testing.T, sink EventSink) authenticatedTestAPI {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "auth-api.db")
	st, err := store.Open(dbPath, store.Options{
		NodeID: testNodeID(1),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: mustHashPassword(t, "root-password"),
	}); err != nil {
		t.Fatalf("ensure bootstrap admin: %v", err)
	}

	signer, err := auth.NewSigner("token-secret")
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}

	httpAPI := NewHTTP(New(st, sink), HTTPOptions{
		NodeID:   testNodeID(1),
		Signer:   signer,
		TokenTTL: time.Hour,
	})
	return authenticatedTestAPI{
		handler: httpAPI.Handler(),
		http:    httpAPI,
	}
}

func TestClientWebSocketLoginAndPushesBytesMessages(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	body := []byte{0xff, 0x00, 'x'}
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": body,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password: "alice-password",
			},
		},
	})

	loginResp := readServerEnvelope(t, conn).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != aliceKey.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}
	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || !senderMatchesRef(pushed.Message.GetSender(), adminKey) || string(pushed.Message.GetBody()) != string(body) {
		t.Fatalf("unexpected pushed message: %+v", pushed)
	}
}

func TestClientWebSocketListClusterNodesAllowsUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeClusterStatusSink{
		status: app.ClusterStatus{
			NodeID: testNodeID(1),
			Peers: []app.ClusterPeerStatus{
				{NodeID: testNodeID(2), ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws", Connected: true},
				{NodeID: testNodeID(3), ConfiguredURL: "ws://127.0.0.1:9082/internal/cluster/ws", Connected: false},
			},
		},
	})
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListClusterNodes{
			ListClusterNodes: &internalproto.ListClusterNodesRequest{RequestId: 99},
		},
	})
	resp := readServerEnvelope(t, conn).GetListClusterNodesResponse()
	if resp == nil || resp.RequestId != 99 || resp.Count != 2 {
		t.Fatalf("unexpected list cluster nodes response: %+v", resp)
	}
	if resp.Items[0].GetNodeId() != testNodeID(1) || !resp.Items[0].GetIsLocal() || resp.Items[0].GetConfiguredUrl() != "" {
		t.Fatalf("unexpected local cluster node: %+v", resp.Items[0])
	}
	if resp.Items[1].GetNodeId() != testNodeID(2) || resp.Items[1].GetIsLocal() || resp.Items[1].GetConfiguredUrl() != "ws://127.0.0.1:9081/internal/cluster/ws" {
		t.Fatalf("unexpected peer cluster node: %+v", resp.Items[1])
	}
}

func TestClientWebSocketListNodeLoggedInUsersAllowsUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "bob", "bob-password", store.RoleUser)

	connAlice := dialClientWebSocket(t, server.URL)
	defer connAlice.Close()
	loginClientWebSocket(t, connAlice, aliceKey, "alice-password")

	connAliceDup := dialClientWebSocket(t, server.URL)
	defer connAliceDup.Close()
	loginClientWebSocket(t, connAliceDup, aliceKey, "alice-password")

	connBob := dialClientWebSocket(t, server.URL)
	defer connBob.Close()
	loginClientWebSocket(t, connBob, bobKey, "bob-password")

	writeClientEnvelope(t, connAlice, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListNodeLoggedInUsers{
			ListNodeLoggedInUsers: &internalproto.ListNodeLoggedInUsersRequest{
				RequestId: 77,
				NodeId:    testNodeID(1),
			},
		},
	})
	resp := readServerEnvelope(t, connAlice).GetListNodeLoggedInUsersResponse()
	if resp == nil || resp.RequestId != 77 || resp.TargetNodeId != testNodeID(1) || resp.Count != 2 {
		t.Fatalf("unexpected list node logged-in users response: %+v", resp)
	}
	if resp.Items[0].GetUserId() != aliceKey.UserID || resp.Items[0].GetUsername() != "alice" {
		t.Fatalf("unexpected first logged-in user: %+v", resp.Items[0])
	}
	if resp.Items[1].GetUserId() != bobKey.UserID || resp.Items[1].GetUsername() != "bob" {
		t.Fatalf("unexpected second logged-in user: %+v", resp.Items[1])
	}
}

func TestClientWebSocketListNodeLoggedInUsersReturnsErrorWhenRemoteNodeUnavailable(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeLoggedInUsersSink{
		query: func(context.Context, int64) ([]app.LoggedInUserSummary, error) {
			return nil, fmt.Errorf("%w: node 8192 is not connected", app.ErrServiceUnavailable)
		},
	})
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListNodeLoggedInUsers{
			ListNodeLoggedInUsers: &internalproto.ListNodeLoggedInUsersRequest{
				RequestId: 88,
				NodeId:    testNodeID(2),
			},
		},
	})
	resp := readServerEnvelope(t, conn).GetError()
	if resp == nil || resp.RequestId != 88 || resp.Code != "service_unavailable" {
		t.Fatalf("unexpected websocket error response: %+v", resp)
	}
}

func TestClientWebSocketSeenCursorAndSendMessage(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	var created struct {
		NodeID int64 `json:"node_id"`
		Seq    int64 `json:"seq"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("already seen"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &created)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password: "alice-password",
				SeenMessages: []*internalproto.MessageCursor{{
					NodeId: created.NodeID,
					Seq:    created.Seq,
				}},
			},
		},
	})
	if loginResp := readServerEnvelope(t, conn).GetLoginResponse(); loginResp == nil {
		t.Fatalf("expected login response")
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 42,
				Target: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				Body: []byte{0x00, 0x01, 0xfe},
			},
		},
	})
	sendResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 42 || string(sendResp.GetMessage().GetBody()) != string([]byte{0x00, 0x01, 0xfe}) {
		t.Fatalf("unexpected send response: %+v", sendResp)
	}
	if !senderMatchesRef(sendResp.GetMessage().GetSender(), aliceKey) {
		t.Fatalf("unexpected send response sender: %+v", sendResp)
	}
}

func TestClientWebSocketTransientSendMessage(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:    43,
				Target:       &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:         []byte("ephemeral"),
				DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
				DeliveryMode: internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT,
			},
		},
	})
	var (
		gotSendResp bool
		gotPacket   bool
	)
	for range 2 {
		envelope := readServerEnvelope(t, conn)
		if sendResp := envelope.GetSendMessageResponse(); sendResp != nil {
			if sendResp.RequestId != 43 || sendResp.GetTransientAccepted() == nil {
				t.Fatalf("unexpected transient send response: %+v", sendResp)
			}
			gotSendResp = true
			continue
		}
		if packet := envelope.GetPacketPushed(); packet != nil {
			if packet.Packet == nil || string(packet.Packet.GetBody()) != "ephemeral" || packet.Packet.GetRecipient().GetUserId() != aliceKey.UserID || !senderMatchesRef(packet.Packet.GetSender(), aliceKey) {
				t.Fatalf("unexpected transient packet push: %+v", packet)
			}
			gotPacket = true
			continue
		}
		t.Fatalf("unexpected websocket envelope: %+v", envelope)
	}
	if !gotSendResp || !gotPacket {
		t.Fatalf("expected transient send response and packet push, got response=%v packet=%v", gotSendResp, gotPacket)
	}

	var listed struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + loginToken(t, testAPI.handler, aliceKey, "alice-password"),
	}, http.StatusOK), &listed)
	if listed.Count != 0 {
		t.Fatalf("expected transient ws send to avoid persistence, got %+v", listed)
	}
}

func TestTransientHTTPAndWebSocketPacket(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password: "alice-password",
			},
		},
	})
	if loginResp := readServerEnvelope(t, conn).GetLoginResponse(); loginResp == nil {
		t.Fatalf("expected login response")
	}

	var accepted struct {
		Mode         string `json:"mode"`
		PacketID     uint64 `json:"packet_id"`
		TargetNodeID int64  `json:"target_node_id"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body":          []byte("transient"),
		"delivery_kind": string(deliveryKindTransient),
		"delivery_mode": string(store.DeliveryModeBestEffort),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusAccepted), &accepted)
	if accepted.Mode != "transient" || accepted.PacketID == 0 || accepted.TargetNodeID != aliceKey.NodeID {
		t.Fatalf("unexpected accepted response: %+v", accepted)
	}

	packet := readServerEnvelope(t, conn).GetPacketPushed()
	if packet == nil || packet.Packet == nil || string(packet.Packet.GetBody()) != "transient" || packet.Packet.GetRecipient().GetUserId() != aliceKey.UserID || !senderMatchesRef(packet.Packet.GetSender(), aliceKey) {
		t.Fatalf("unexpected packet push: %+v", packet)
	}

	var listed struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &listed)
	if listed.Count != 0 {
		t.Fatalf("expected transient packet to avoid persistence, got %+v", listed)
	}
}

func TestTransientRequiresLoginRecipient(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	channelKey := createUserAs(t, testAPI.handler, adminToken, "orders", "", store.RoleChannel)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channelKey.NodeID, channelKey.UserID), map[string]any{
		"body":          []byte("transient"),
		"delivery_kind": string(deliveryKindTransient),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusBadRequest)
}

func TestClientWebSocketAdminRPCProvidesFullHTTPCapabilities(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, adminKey, "root-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_CreateUser{
			CreateUser: &internalproto.CreateUserRequest{
				RequestId:   11,
				Username:    "rpc-alice",
				Password:    "rpc-password",
				ProfileJson: []byte(`{"display_name":"RPC Alice"}`),
				Role:        store.RoleUser,
			},
		},
	})
	createResp := readServerEnvelope(t, conn).GetCreateUserResponse()
	if createResp == nil || createResp.RequestId != 11 || createResp.User.GetUserId() == 0 || string(createResp.User.GetProfileJson()) != `{"display_name":"RPC Alice"}` {
		t.Fatalf("unexpected create user response: %+v", createResp)
	}
	createdKey := store.UserKey{NodeID: createResp.User.GetNodeId(), UserID: createResp.User.GetUserId()}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpdateUser{
			UpdateUser: &internalproto.UpdateUserRequest{
				RequestId: 12,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
				Username:  &internalproto.StringField{Value: "rpc-alice-updated"},
				ProfileJson: &internalproto.BytesField{
					Value: []byte(`{"display_name":"RPC Alice Updated"}`),
				},
			},
		},
	})
	updateResp := readServerEnvelope(t, conn).GetUpdateUserResponse()
	if updateResp == nil || updateResp.RequestId != 12 || updateResp.User.GetUsername() != "rpc-alice-updated" {
		t.Fatalf("unexpected update user response: %+v", updateResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 13,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
			},
		},
	})
	getResp := readServerEnvelope(t, conn).GetGetUserResponse()
	if getResp == nil || getResp.RequestId != 13 || getResp.User.GetUsername() != "rpc-alice-updated" {
		t.Fatalf("unexpected get user response: %+v", getResp)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(createdKey.NodeID, createdKey.UserID), map[string]any{
		"body": []byte("rpc hello"),
	}, map[string]string{
		"Authorization": "Bearer " + loginToken(t, testAPI.handler, adminKey, "root-password"),
	}, http.StatusCreated)

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 14,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
				Limit:     10,
			},
		},
	})
	listMessages := readServerEnvelope(t, conn).GetListMessagesResponse()
	if listMessages == nil || listMessages.RequestId != 14 || listMessages.Count != 1 || string(listMessages.Items[0].GetBody()) != "rpc hello" {
		t.Fatalf("unexpected list messages response: %+v", listMessages)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListEvents{
			ListEvents: &internalproto.ListEventsRequest{
				RequestId: 15,
				After:     0,
				Limit:     20,
			},
		},
	})
	listEvents := readServerEnvelope(t, conn).GetListEventsResponse()
	if listEvents == nil || listEvents.RequestId != 15 || listEvents.Count < 3 || len(listEvents.Items[0].GetEventJson()) == 0 {
		t.Fatalf("unexpected list events response: %+v", listEvents)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_OperationsStatus{
			OperationsStatus: &internalproto.OperationsStatusRequest{RequestId: 16},
		},
	})
	opsResp := readServerEnvelope(t, conn).GetOperationsStatusResponse()
	if opsResp == nil || opsResp.RequestId != 16 || opsResp.Status.GetNodeId() != testNodeID(1) {
		t.Fatalf("unexpected operations status response: %+v", opsResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Metrics{
			Metrics: &internalproto.MetricsRequest{RequestId: 17},
		},
	})
	metricsResp := readServerEnvelope(t, conn).GetMetricsResponse()
	if metricsResp == nil || metricsResp.RequestId != 17 || !strings.Contains(metricsResp.Text, "notifier_write_gate_ready") {
		t.Fatalf("unexpected metrics response: %+v", metricsResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUser{
			DeleteUser: &internalproto.DeleteUserRequest{
				RequestId: 18,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
			},
		},
	})
	deleteResp := readServerEnvelope(t, conn).GetDeleteUserResponse()
	if deleteResp == nil || deleteResp.RequestId != 18 || deleteResp.Status != "deleted" {
		t.Fatalf("unexpected delete user response: %+v", deleteResp)
	}
}

func TestClientWebSocketRPCRespectsUserAuthorizationAndSubscriptions(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := httptest.NewServer(testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "orders", "", store.RoleChannel)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 21,
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
			},
		},
	})
	getResp := readServerEnvelope(t, conn).GetGetUserResponse()
	if getResp == nil || getResp.RequestId != 21 || getResp.User.GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected self get user response: %+v", getResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 22,
				User:      &internalproto.UserRef{NodeId: adminKey.NodeID, UserId: adminKey.UserID},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 22 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected get user forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 230,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("seed"),
			},
		},
	})
	sendSelfResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendSelfResp == nil || sendSelfResp.RequestId != 230 || string(sendSelfResp.GetMessage().GetBody()) != "seed" {
		t.Fatalf("unexpected self send response: %+v", sendSelfResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 23,
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Limit:     10,
			},
		},
	})
	listMessages := readServerEnvelope(t, conn).GetListMessagesResponse()
	if listMessages == nil || listMessages.RequestId != 23 || listMessages.Count != 1 || string(listMessages.Items[0].GetBody()) != "seed" {
		t.Fatalf("unexpected self list messages response: %+v", listMessages)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListEvents{
			ListEvents: &internalproto.ListEventsRequest{RequestId: 24, After: 0, Limit: 10},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 24 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected list events forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SubscribeChannel{
			SubscribeChannel: &internalproto.SubscribeChannelRequest{
				RequestId: 25,
				Subscriber: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				Channel: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
			},
		},
	})
	subscribeResp := readServerEnvelope(t, conn).GetSubscribeChannelResponse()
	if subscribeResp == nil || subscribeResp.RequestId != 25 || subscribeResp.Subscription.GetChannel().GetUserId() != channelKey.UserID {
		t.Fatalf("unexpected subscribe response: %+v", subscribeResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListSubscriptions{
			ListSubscriptions: &internalproto.ListSubscriptionsRequest{
				RequestId: 26,
				Subscriber: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
			},
		},
	})
	listSubs := readServerEnvelope(t, conn).GetListSubscriptionsResponse()
	if listSubs == nil || listSubs.RequestId != 26 || listSubs.Count != 1 || listSubs.Items[0].GetChannel().GetUserId() != channelKey.UserID {
		t.Fatalf("unexpected list subscriptions response: %+v", listSubs)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 27,
				Target: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
				Body: []byte("channel payload"),
			},
		},
	})
	sendResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 27 || string(sendResp.GetMessage().GetBody()) != "channel payload" {
		t.Fatalf("unexpected send to channel response: %+v", sendResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UnsubscribeChannel{
			UnsubscribeChannel: &internalproto.UnsubscribeChannelRequest{
				RequestId: 28,
				Subscriber: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				Channel: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
			},
		},
	})
	unsubscribeResp := readServerEnvelope(t, conn).GetUnsubscribeChannelResponse()
	if unsubscribeResp == nil || unsubscribeResp.RequestId != 28 || unsubscribeResp.Subscription.GetDeletedAt() == "" {
		t.Fatalf("unexpected unsubscribe response: %+v", unsubscribeResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 29,
				Target: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
				Body: []byte("forbidden after unsubscribe"),
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 29 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected send forbidden error: %+v", rpcErr)
	}
}

func loginClientWebSocket(t *testing.T, conn *websocket.Conn, key store.UserKey, password string) {
	t.Helper()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
				Password: password,
			},
		},
	})
	loginResp := readServerEnvelope(t, conn).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != key.UserID {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}
}

func senderMatchesRef(ref *internalproto.UserRef, key store.UserKey) bool {
	return ref != nil && ref.GetNodeId() == key.NodeID && ref.GetUserId() == key.UserID
}

func loginToken(t *testing.T, handler http.Handler, key store.UserKey, password string) string {
	t.Helper()

	var response struct {
		Token string `json:"token"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  key.NodeID,
		"user_id":  key.UserID,
		"password": password,
	}, nil, http.StatusOK), &response)
	if response.Token == "" {
		t.Fatalf("expected login token")
	}
	return response.Token
}

func createUserAs(t *testing.T, handler http.Handler, token, username, password, role string) store.UserKey {
	t.Helper()

	var response struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/users", map[string]any{
		"username": username,
		"password": password,
		"role":     role,
	}, map[string]string{
		"Authorization": "Bearer " + token,
	}, http.StatusCreated), &response)
	key := store.UserKey{NodeID: response.NodeID, UserID: response.UserID}
	if err := key.Validate(); err != nil {
		t.Fatalf("expected created user id")
	}
	return key
}

func subscriptionsPath(nodeID, userID int64) string {
	return userPath(nodeID, userID) + "/subscriptions"
}

type authMessageItem struct {
	Body []byte `json:"body"`
}

func responseMessagesContainBody(messages []authMessageItem, body string) bool {
	for _, message := range messages {
		if string(message.Body) == body {
			return true
		}
	}
	return false
}

func dialClientWebSocket(t *testing.T, serverURL string) *websocket.Conn {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	parsed.Scheme = "ws"
	parsed.Path = "/ws/client"
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		t.Fatalf("dial client websocket: %v", err)
	}
	return conn
}

func writeClientEnvelope(t *testing.T, conn *websocket.Conn, envelope *internalproto.ClientEnvelope) {
	t.Helper()
	data, err := gproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal client envelope: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write client envelope: %v", err)
	}
}

func readServerEnvelope(t *testing.T, conn *websocket.Conn) *internalproto.ServerEnvelope {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read server envelope: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("unexpected websocket message type: %d", messageType)
	}
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal server envelope: %v", err)
	}
	return &envelope
}

func mustHashPassword(t *testing.T, password string) string {
	t.Helper()

	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return hash
}

func doJSONWithHeaders(t *testing.T, handler http.Handler, method, path string, body any, headers map[string]string, wantStatus int) []byte {
	t.Helper()

	var reqBody *bytes.Reader
	if body == nil {
		reqBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reqBody = bytes.NewReader(payload)
	}

	req := httptest.NewRequest(method, path, reqBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != wantStatus {
		t.Fatalf("unexpected status for %s %s: got=%d want=%d body=%s", method, path, rr.Code, wantStatus, rr.Body.String())
	}
	return rr.Body.Bytes()
}
