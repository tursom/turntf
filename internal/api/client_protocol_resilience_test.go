package api

import (
	"net/http"
	"testing"

	"github.com/gorilla/websocket"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestClientWebSocketRecoversFromInvalidFramesAndHonorsAck(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "protocol-resilience", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	if err := conn.WriteMessage(websocket.TextMessage, []byte("not protobuf")); err != nil {
		t.Fatalf("write text websocket frame: %v", err)
	}
	assertClientProtocolError(t, readServerEnvelope(t, conn), "invalid_frame")

	if err := conn.WriteMessage(websocket.BinaryMessage, []byte{0xff}); err != nil {
		t.Fatalf("write invalid protobuf frame: %v", err)
	}
	assertClientProtocolError(t, readServerEnvelope(t, conn), "invalid_protobuf")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{})
	assertClientProtocolError(t, readServerEnvelope(t, conn), "invalid_message")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "alice-password",
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	assertClientProtocolError(t, readServerEnvelope(t, conn), "already_authenticated")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_AckMessage{
			AckMessage: &internalproto.AckMessage{
				Cursor: &internalproto.MessageCursor{NodeId: testNodeID(1), Seq: 1},
			},
		},
	})
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Ping{Ping: &internalproto.Ping{RequestId: 707}},
	})
	if pong := readServerEnvelope(t, conn).GetPong(); pong == nil || pong.RequestId != 707 {
		t.Fatalf("expected session to remain usable after protocol errors, got %+v", pong)
	}

	first := createProtocolTestMessage(t, testAPI.handler, adminToken, aliceKey, "acknowledged-before-push")
	if first.NodeID != testNodeID(1) || first.Seq != 1 {
		t.Fatalf("unexpected first message cursor: %+v", first)
	}
	second := createProtocolTestMessage(t, testAPI.handler, adminToken, aliceKey, "visible-after-ack")
	if second.NodeID != testNodeID(1) || second.Seq != 2 {
		t.Fatalf("unexpected second message cursor: %+v", second)
	}

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || pushed.Message.NodeId != second.NodeID || pushed.Message.Seq != second.Seq || string(pushed.Message.Body) != "visible-after-ack" {
		t.Fatalf("expected acked cursor to be skipped and next message to be pushed, got %+v", pushed)
	}
}

func assertClientProtocolError(t *testing.T, envelope *internalproto.ServerEnvelope, code string) {
	t.Helper()
	rpcErr := envelope.GetError()
	if rpcErr == nil || rpcErr.Code != code || rpcErr.RequestId != 0 {
		t.Fatalf("unexpected client protocol error: got=%+v want_code=%q", rpcErr, code)
	}
}

func createProtocolTestMessage(t *testing.T, handler http.Handler, token string, user store.UserKey, body string) struct {
	NodeID int64 `json:"node_id"`
	Seq    int64 `json:"seq"`
} {
	t.Helper()
	var created struct {
		NodeID int64 `json:"node_id"`
		Seq    int64 `json:"seq"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, userMessagesPath(user.NodeID, user.UserID), map[string]any{
		"body": []byte(body),
	}, map[string]string{
		"Authorization": "Bearer " + token,
	}, http.StatusCreated), &created)
	return created
}
