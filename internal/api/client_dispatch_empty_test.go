package api

import (
	"context"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/store"
)

func TestPersistentDispatcherOfflineTargetDoesNotStopLaterDelivery(t *testing.T) {
	testAPI, _, conn, _, aliceKey := openControlledPersistentDispatcherTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	offline := mustCreatePersistentDispatchUser(t, testAPI.http, "dispatcher-offline-target")
	after, err := testAPI.http.service.LastEventSequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fallback := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		testAPI.http.runPersistentDispatcherWithFallback(ctx, after, nil, fallback)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("dispatcher did not stop")
		}
	})
	admin := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	for _, target := range []store.UserKey{offline.Key(), aliceKey} {
		body := "offline-message"
		if target == aliceKey {
			body = "online-message-after-offline-target"
		}
		if _, _, err := testAPI.http.service.CreateMessage(ctx, store.CreateMessageParams{
			UserKey: target, Sender: admin, Body: []byte(body),
		}); err != nil {
			t.Fatal(err)
		}
	}
	fallback <- time.Now()
	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.Body) != "online-message-after-offline-target" {
		t.Fatalf("unexpected delivery after an empty candidate set: %+v", pushed)
	}
}

func TestPersistentDispatcherOfflineTargetStillReachesAdmin(t *testing.T) {
	a := newAuthenticatedTestAPI(t)
	ctx := context.Background()
	offline := mustCreatePersistentDispatchUser(t, a.http, "dispatcher-offline-admin-visible")
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	admin, err := a.http.service.GetUser(ctx, adminKey)
	if err != nil {
		t.Fatal(err)
	}
	conn := &persistentPayloadCaptureConn{}
	session := newPersistentPayloadCaptureSession(a.http, admin, conn)
	registerPersistentPayloadCaptureSessions(t, a.http, session)
	if err := a.http.dispatchPersistentMessage(ctx, 100, store.Message{
		Recipient: offline.Key(), Sender: adminKey, NodeID: adminKey.NodeID, Seq: 1,
		Body: []byte("offline-target-admin-copy"), CreatedAt: a.http.service.store.Clock().Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if len(conn.singlePayload(t)) == 0 {
		t.Fatal("admin did not receive the offline target's message")
	}
}
