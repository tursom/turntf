package api

import (
	"net/http"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestClientWebSocketReconnectTokenSkipsPasswordAuthentication(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "reconnect-alice", "alice-password", store.RoleUser)

	passwordConn := dialClientWebSocket(t, server.URL)
	writeClientEnvelope(t, passwordConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "alice-password",
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	passwordLogin := readServerEnvelope(t, passwordConn).GetLoginResponse()
	_ = passwordConn.Close()
	if passwordLogin == nil || passwordLogin.ReconnectToken == "" {
		t.Fatalf("expected password login to issue reconnect token, got %+v", passwordLogin)
	}
	if passwordLogin.ReconnectTokenExpiresAtUnix <= time.Now().Unix() {
		t.Fatalf("expected reconnect token expiry in the future, got %d", passwordLogin.ReconnectTokenExpiresAtUnix)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/users", nil, map[string]string{
		"Authorization": "Bearer " + passwordLogin.ReconnectToken,
	}, http.StatusUnauthorized)

	reconnectConn := dialClientWebSocket(t, server.URL)
	defer reconnectConn.Close()
	writeClientEnvelope(t, reconnectConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				ReconnectToken:  passwordLogin.ReconnectToken,
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	reconnectLogin := readServerEnvelope(t, reconnectConn).GetLoginResponse()
	if reconnectLogin == nil || reconnectLogin.User.GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected reconnect login response: %+v", reconnectLogin)
	}
	if reconnectLogin.ReconnectToken == "" {
		t.Fatalf("expected reconnect login to refresh reconnect token: %+v", reconnectLogin)
	}
}

func TestClientWebSocketReconnectTokenExpiresWhenPasswordChanges(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "reconnect-password-change", "alice-password", store.RoleUser)

	passwordConn := dialClientWebSocket(t, server.URL)
	writeClientEnvelope(t, passwordConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "alice-password",
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	reconnectToken := readServerEnvelope(t, passwordConn).GetLoginResponse().GetReconnectToken()
	_ = passwordConn.Close()
	if reconnectToken == "" {
		t.Fatal("expected reconnect token")
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"password": "alice-password-updated",
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)

	reconnectConn := dialClientWebSocket(t, server.URL)
	defer reconnectConn.Close()
	writeClientEnvelope(t, reconnectConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				ReconnectToken:  reconnectToken,
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	rpcErr := readServerEnvelope(t, reconnectConn).GetError()
	if rpcErr == nil || rpcErr.Code != "unauthorized" {
		t.Fatalf("expected password change to invalidate reconnect token, got %+v", rpcErr)
	}
}
