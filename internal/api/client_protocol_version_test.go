package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const requiredClientProtocolVersion = "client-v1alpha5"

func TestClientWebSocketRejectsUnsupportedProtocolVersions(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/ws/client", "/ws/realtime"} {
		path := path
		for _, version := range []string{"", "client-v1alpha4", "client-v99"} {
			version := version
			t.Run(path+"/"+version, func(t *testing.T) {
				t.Parallel()

				testAPI := newAuthenticatedTestAPI(t)
				server := newIPv4TestServer(t, testAPI.handler)
				defer server.Close()

				adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
				adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
				aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
				if _, _, err := testAPI.http.service.CreateMessage(context.Background(), store.CreateMessageParams{
					UserKey: aliceKey,
					Sender:  adminKey,
					Body:    []byte("must not be replayed before protocol admission"),
				}); err != nil {
					t.Fatalf("create pending message: %v", err)
				}
				password := "alice-password"
				if version == "client-v1alpha4" {
					password = "wrong-password"
				}

				conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL)+path, nil)
				if err != nil {
					t.Fatalf("dial websocket: %v", err)
				}
				defer conn.Close()

				writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
					Body: &internalproto.ClientEnvelope_Login{
						Login: &internalproto.LoginRequest{
							User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
							Password:        password,
							ProtocolVersion: version,
						},
					},
				})

				rpcErr := readServerEnvelope(t, conn).GetError()
				if rpcErr == nil || rpcErr.Code != "unsupported_protocol_version" || rpcErr.RequestId != 0 {
					t.Fatalf("expected unsupported protocol error, got %+v", rpcErr)
				}
				if !strings.Contains(rpcErr.Message, "got=") || !strings.Contains(rpcErr.Message, "want=") || !strings.Contains(rpcErr.Message, requiredClientProtocolVersion) {
					t.Fatalf("expected got/want protocol details, got %q", rpcErr.Message)
				}

				if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatalf("set read deadline: %v", err)
				}
				if _, _, err := conn.ReadMessage(); err == nil {
					t.Fatal("expected server to close connection after protocol rejection")
				}

				sessions, err := testAPI.http.ListLocalUserSessions(context.Background(), aliceKey)
				if err != nil {
					t.Fatalf("list local sessions: %v", err)
				}
				if len(sessions) != 0 {
					t.Fatalf("protocol-rejected client must not register a session: %+v", sessions)
				}
			})
		}
	}
}

func TestClientWebSocketValidProtocolStillRejectsBadCredentialsAsUnauthorized(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "wrong-password",
				ProtocolVersion: requiredClientProtocolVersion,
			},
		},
	})

	rpcErr := readServerEnvelope(t, conn).GetError()
	if rpcErr == nil || rpcErr.Code != "unauthorized" || rpcErr.RequestId != 0 {
		t.Fatalf("expected unauthorized login error, got %+v", rpcErr)
	}
}
