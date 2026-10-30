package api

import (
	"bytes"
	"testing"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	gproto "google.golang.org/protobuf/proto"
)

func TestPersistentEncodingPreservesWireAndOwnsPayload(t *testing.T) {
	message := store.Message{
		Recipient: store.UserKey{NodeID: 1, UserID: 2}, Sender: store.UserKey{NodeID: 3, UserID: 4},
		NodeID: 5, Seq: 6, Body: bytes.Repeat([]byte("m"), 4096),
		CreatedAt: clock.Timestamp{WallTimeMs: 1700000000000, NodeID: 1}, TraceID: "0123456789abcdef0123456789abcdef",
	}
	want, err := gproto.Marshal(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_MessagePushed{
			MessagePushed: &internalproto.MessagePushed{Message: clientProtoMessage(message)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodePersistentMessage(message)
	if err != nil || !bytes.Equal(got.payload, want) || got.traceID != message.TraceID || got.recipient != message.Recipient ||
		got.cursor.nodeID != message.NodeID || got.cursor.seq != message.Seq {
		t.Fatalf("encoded message differs: message=%+v err=%v", got, err)
	}
	for i := range message.Body {
		message.Body[i] = 'x'
	}
	if !bytes.Equal(got.payload, want) {
		t.Fatal("encoded payload still aliases the caller's body")
	}
}
