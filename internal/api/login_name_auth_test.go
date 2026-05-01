package api

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestHTTPLoginByLoginName(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAsWithLoginName(t, testAPI.handler, adminToken, "alice", "alice.login", "alice-password", store.RoleUser)

	var loginResp struct {
		Token string `json:"token"`
		User  struct {
			NodeID    int64  `json:"node_id"`
			UserID    int64  `json:"user_id"`
			LoginName string `json:"login_name"`
		} `json:"user"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"login_name": "alice.login",
		"password":   "alice-password",
	}, nil, http.StatusOK), &loginResp)
	if loginResp.Token == "" {
		t.Fatalf("expected login token")
	}
	if loginResp.User.NodeID != aliceKey.NodeID || loginResp.User.UserID != aliceKey.UserID || loginResp.User.LoginName != "alice.login" {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":    aliceKey.NodeID,
		"user_id":    aliceKey.UserID,
		"login_name": "alice.login",
		"password":   "alice-password",
	}, nil, http.StatusBadRequest)

	var getResp struct {
		LoginName string `json:"login_name"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + loginResp.Token,
	}, http.StatusOK), &getResp)
	if getResp.LoginName != "alice.login" {
		t.Fatalf("unexpected user login name: %+v", getResp)
	}

	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"login_name": "",
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &getResp)
	if getResp.LoginName != "" {
		t.Fatalf("expected cleared login name, got %+v", getResp)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"login_name": "alice.login",
		"password":   "alice-password",
	}, nil, http.StatusUnauthorized)
}

func TestClientWebSocketLoginByLoginName(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAsWithLoginName(t, testAPI.handler, adminToken, "alice", "alice.login", "alice-password", store.RoleUser)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL)+"/ws/client", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.BinaryMessage, mustMarshalProto(t, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				LoginName: "alice.login",
				Password:  "alice-password",
			},
		},
	})); err != nil {
		t.Fatalf("write login request: %v", err)
	}
	loginResp := readServerEnvelope(t, conn).GetLoginResponse()
	if loginResp == nil {
		t.Fatalf("expected login response")
	}
	if loginResp.User.GetUserId() != aliceKey.UserID || loginResp.User.GetLoginName() != "alice.login" {
		t.Fatalf("unexpected websocket login response: %+v", loginResp)
	}

	var usersResp struct {
		Items []struct {
			UserID    int64  `json:"user_id"`
			LoginName string `json:"login_name"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/"+itoa10(aliceKey.NodeID)+"/logged-in-users", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &usersResp)
	if len(usersResp.Items) != 1 || usersResp.Items[0].UserID != aliceKey.UserID || usersResp.Items[0].LoginName != "alice.login" {
		t.Fatalf("unexpected logged-in users response: %+v", usersResp)
	}
}

func TestClientWebSocketRejectsMixedLoginSelectors(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAsWithLoginName(t, testAPI.handler, adminToken, "alice", "alice.login", "alice-password", store.RoleUser)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL)+"/ws/client", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.BinaryMessage, mustMarshalProto(t, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				LoginName: "alice.login",
				Password:  "alice-password",
			},
		},
	})); err != nil {
		t.Fatalf("write mixed login request: %v", err)
	}
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.Code != "unauthorized" {
		t.Fatalf("expected unauthorized login error, got %+v", rpcErr)
	}
}

func createUserAsWithLoginName(t *testing.T, handler http.Handler, token, username, loginName, password, role string) store.UserKey {
	t.Helper()

	var response struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/users", map[string]any{
		"username":   username,
		"login_name": loginName,
		"password":   password,
		"role":       role,
	}, map[string]string{
		"Authorization": "Bearer " + token,
	}, http.StatusCreated), &response)
	key := store.UserKey{NodeID: response.NodeID, UserID: response.UserID}
	if err := key.Validate(); err != nil {
		t.Fatalf("expected created user id")
	}
	return key
}

func mustMarshalProto(t *testing.T, msg gproto.Message) []byte {
	t.Helper()

	data, err := gproto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	return data
}

func itoa10(value int64) string {
	return strconv.FormatInt(value, 10)
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}
