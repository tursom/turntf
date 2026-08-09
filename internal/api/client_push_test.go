package api

import (
	"context"
	"testing"

	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestPushInitialMessagesRetainsDistinctEncodedPayloadsAndSkipsSeen(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	user := mustCreatePersistentDispatchUser(t, testAPI.http, "initial-history-encoding")
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}

	messages := make([]store.Message, 0, 3)
	for _, body := range [][]byte{[]byte("oldest"), []byte("seen"), []byte("newest")} {
		message, _, err := testAPI.http.service.CreateMessage(context.Background(), store.CreateMessageParams{
			UserKey: user.Key(),
			Sender:  adminKey,
			Body:    body,
		})
		if err != nil {
			t.Fatalf("create initial history message: %v", err)
		}
		messages = append(messages, message)
	}

	conn := &persistentPayloadCaptureConn{}
	session := newPersistentPayloadCaptureSession(testAPI.http, user, conn)
	session.markSeen(messages[1].NodeID, messages[1].Seq)
	if err := session.pushInitialMessages(context.Background()); err != nil {
		t.Fatalf("push initial messages: %v", err)
	}

	conn.mu.Lock()
	payloads := append([][]byte(nil), conn.payloads...)
	conn.mu.Unlock()
	if len(payloads) != 2 {
		t.Fatalf("initial history payload count: got %d want 2", len(payloads))
	}
	if len(payloads[0]) == 0 || len(payloads[1]) == 0 || &payloads[0][0] == &payloads[1][0] {
		t.Fatal("initial history payloads must retain independent encoded buffers")
	}

	want := []store.Message{messages[0], messages[2]}
	for i, payload := range payloads {
		var envelope internalproto.ServerEnvelope
		if err := gproto.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("unmarshal initial history payload %d: %v", i, err)
		}
		message := envelope.GetMessagePushed().GetMessage()
		if message == nil ||
			message.GetRecipient().GetNodeId() != want[i].Recipient.NodeID ||
			message.GetRecipient().GetUserId() != want[i].Recipient.UserID ||
			message.GetSender().GetNodeId() != want[i].Sender.NodeID ||
			message.GetSender().GetUserId() != want[i].Sender.UserID ||
			message.GetNodeId() != want[i].NodeID ||
			message.GetSeq() != want[i].Seq ||
			string(message.GetBody()) != string(want[i].Body) ||
			message.GetCreatedAtHlc() != want[i].CreatedAt.String() {
			t.Fatalf("unexpected initial history payload %d: %+v", i, message)
		}
	}

	if err := session.pushInitialMessages(context.Background()); err != nil {
		t.Fatalf("push duplicate initial messages: %v", err)
	}
	if got := conn.payloadCount(); got != len(payloads) {
		t.Fatalf("duplicate initial history changed payload count: got %d want %d", got, len(payloads))
	}
}
