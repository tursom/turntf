package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	"notifier/internal/auth"
	internalproto "notifier/internal/proto"
	"notifier/internal/store"
)

type authenticatedTestAPI struct {
	handler http.Handler
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
		"sender": "alice",
		"body":   []byte("hello"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(adminKey.NodeID, adminKey.UserID), map[string]any{
		"sender": "alice",
		"body":   []byte("forbidden"),
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
		"sender": "alice",
		"body":   []byte("not subscribed"),
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
		"sender": "alice",
		"body":   []byte("channel message"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	broadcastPath := userMessagesPath(testNodeID(1), store.BroadcastUserID)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, broadcastPath, map[string]any{
		"sender": "alice",
		"body":   []byte("ordinary broadcast"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, broadcastPath, map[string]any{
		"sender": "admin",
		"body":   []byte("admin broadcast"),
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

func newAuthenticatedTestAPI(t *testing.T) authenticatedTestAPI {
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

	return authenticatedTestAPI{
		handler: NewHTTP(New(st, nil), HTTPOptions{
			NodeID:   testNodeID(1),
			Signer:   signer,
			TokenTTL: time.Hour,
		}).Handler(),
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
		"sender": "binary",
		"body":   body,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				NodeId:   aliceKey.NodeID,
				UserId:   aliceKey.UserID,
				Password: "alice-password",
			},
		},
	})

	loginResp := readServerEnvelope(t, conn).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != aliceKey.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}
	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message.GetSender() != "binary" || string(pushed.Message.GetBody()) != string(body) {
		t.Fatalf("unexpected pushed message: %+v", pushed)
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
		"sender": "seed",
		"body":   []byte("already seen"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &created)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				NodeId:   aliceKey.NodeID,
				UserId:   aliceKey.UserID,
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
				Sender: "alice",
				Body:   []byte{0x00, 0x01, 0xfe},
			},
		},
	})
	sendResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 42 || string(sendResp.GetMessage().GetBody()) != string([]byte{0x00, 0x01, 0xfe}) {
		t.Fatalf("unexpected send response: %+v", sendResp)
	}
}

func TestNodeIngressHTTPAndWebSocketTransientPacket(t *testing.T) {
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
				NodeId:   aliceKey.NodeID,
				UserId:   aliceKey.UserID,
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
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(testNodeID(1), store.NodeIngressUserID), map[string]any{
		"sender": "relay",
		"body":   []byte("transient"),
		"relay_target": map[string]any{
			"node_id": aliceKey.NodeID,
			"user_id": aliceKey.UserID,
		},
		"delivery_mode": string(store.DeliveryModeBestEffort),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusAccepted), &accepted)
	if accepted.Mode != "transient" || accepted.PacketID == 0 || accepted.TargetNodeID != aliceKey.NodeID {
		t.Fatalf("unexpected accepted response: %+v", accepted)
	}

	packet := readServerEnvelope(t, conn).GetPacketPushed()
	if packet == nil || packet.Packet == nil || string(packet.Packet.GetBody()) != "transient" || packet.Packet.GetRelayTarget().GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected packet push: %+v", packet)
	}
}

func TestNodeIngressRequiresRelayTarget(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(testNodeID(1), store.NodeIngressUserID), map[string]any{
		"sender": "relay",
		"body":   []byte("transient"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)
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
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
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
