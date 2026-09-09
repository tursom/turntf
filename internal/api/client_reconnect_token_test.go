package api

import (
	"context"
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
	waitForNoLocalClientSessions(t, testAPI.http, aliceKey)

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

func waitForNoLocalClientSessions(t *testing.T, httpAPI *HTTP, user store.UserKey) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sessions, err := httpAPI.ListLocalUserSessions(context.Background(), user)
		if err != nil {
			t.Fatalf("list local sessions: %v", err)
		}
		if len(sessions) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for local sessions to close for user %+v", user)
}

func TestClientWebSocketInvalidReconnectTokenNeverFallsBackToPassword(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "reconnect-no-fallback-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "reconnect-no-fallback-bob", "bob-password", store.RoleUser)

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
	waitForNoLocalClientSessions(t, testAPI.http, aliceKey)

	tests := []struct {
		name     string
		user     store.UserKey
		password string
		token    string
	}{
		{name: "invalid token with correct password", user: aliceKey, password: "alice-password", token: reconnectToken + "corrupt"},
		{name: "token selector mismatch with correct password", user: bobKey, password: "bob-password", token: reconnectToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialClientWebSocket(t, server.URL)
			defer conn.Close()
			writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
				Body: &internalproto.ClientEnvelope_Login{
					Login: &internalproto.LoginRequest{
						User:            &internalproto.UserRef{NodeId: tt.user.NodeID, UserId: tt.user.UserID},
						Password:        tt.password,
						ReconnectToken:  tt.token,
						ProtocolVersion: internalproto.ClientProtocolVersion,
					},
				},
			})
			if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.Code != "unauthorized" {
				t.Fatalf("expected reconnect token rejection, got %+v", rpcErr)
			}
			sessions, err := testAPI.http.ListLocalUserSessions(context.Background(), tt.user)
			if err != nil {
				t.Fatalf("list local sessions: %v", err)
			}
			if len(sessions) != 0 {
				t.Fatalf("rejected reconnect token registered a session: %+v", sessions)
			}
		})
	}
}
