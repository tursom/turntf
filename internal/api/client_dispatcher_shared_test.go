package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestPersistentDispatcherMessageCommitWakeDrainsAllBatches(t *testing.T) {
	testAPI, _, conn, adminToken, aliceKey := openControlledPersistentDispatcherTest(t)
	afterSequence, err := testAPI.http.service.LastEventSequence(context.Background())
	if err != nil {
		t.Fatalf("load dispatcher test watermark: %v", err)
	}

	wake, unsubscribe := testAPI.http.service.subscribeMessageCommits()
	t.Cleanup(unsubscribe)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		testAPI.http.runPersistentDispatcherWithFallback(ctx, afterSequence, wake, nil)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("persistent dispatcher did not stop after cancellation")
		}
	})

	for idx := 0; idx < clientWSPollBatchSize; idx++ {
		if _, _, err := testAPI.http.service.CreateUser(context.Background(), store.CreateUserParams{
			Username:     fmt.Sprintf("dispatcher-gap-%03d", idx),
			PasswordHash: "!",
			Role:         store.RoleUser,
		}); err != nil {
			t.Fatalf("create dispatcher gap event %d: %v", idx, err)
		}
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("event-wake-after-full-batch"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "event-wake-after-full-batch" {
		t.Fatalf("unexpected event-woken message push: %+v", pushed)
	}
}

func TestPersistentDispatcherReplicatedMessageCommitWake(t *testing.T) {
	testAPI, _, conn, _, aliceKey := openControlledPersistentDispatcherTest(t)
	afterSequence, err := testAPI.http.service.LastEventSequence(context.Background())
	if err != nil {
		t.Fatalf("load replicated dispatcher watermark: %v", err)
	}

	wake, unsubscribe := testAPI.http.service.subscribeMessageCommits()
	t.Cleanup(unsubscribe)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go testAPI.http.runPersistentDispatcherWithFallback(ctx, afterSequence, wake, nil)

	now := testAPI.http.service.store.Clock().Now()
	event := store.Event{
		EventID:         1,
		Aggregate:       "message",
		AggregateNodeID: testNodeID(2),
		AggregateID:     1,
		HLC:             now,
		OriginNodeID:    testNodeID(2),
		Body: &internalproto.MessageCreatedEvent{
			Recipient:    &internalproto.ClusterUserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
			NodeId:       testNodeID(2),
			Seq:          1,
			Sender:       &internalproto.ClusterUserRef{NodeId: testNodeID(2), UserId: store.BootstrapAdminUserID},
			Body:         []byte("replicated-event-wake"),
			CreatedAtHlc: now.String(),
		},
	}
	if err := testAPI.http.service.store.ApplyReplicatedEvent(context.Background(), store.ToReplicatedEvent(event)); err != nil {
		t.Fatalf("apply replicated dispatcher message: %v", err)
	}

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "replicated-event-wake" {
		t.Fatalf("unexpected replicated event push: %+v", pushed)
	}
}

func TestPersistentDispatcherFallbackTrigger(t *testing.T) {
	testAPI, _, conn, _, aliceKey := openControlledPersistentDispatcherTest(t)
	afterSequence, err := testAPI.http.service.LastEventSequence(context.Background())
	if err != nil {
		t.Fatalf("load fallback dispatcher watermark: %v", err)
	}

	fallback := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go testAPI.http.runPersistentDispatcherWithFallback(ctx, afterSequence, nil, fallback)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	if _, _, err := testAPI.http.service.store.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: aliceKey,
		Sender:  adminKey,
		Body:    []byte("fallback-push"),
	}); err != nil {
		t.Fatalf("create fallback dispatcher message: %v", err)
	}
	fallback <- time.Now()

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "fallback-push" {
		t.Fatalf("unexpected fallback event push: %+v", pushed)
	}
}

func openControlledPersistentDispatcherTest(t *testing.T) (authenticatedTestAPI, *httptest.Server, *websocket.Conn, string, store.UserKey) {
	t.Helper()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	t.Cleanup(server.Close)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "dispatcher-alice", "alice-password", store.RoleUser)

	// Keep registerPersistentSession from starting the production ticker-driven
	// dispatcher so each test controls its wake and fallback channels.
	testAPI.http.dispatcherMu.Lock()
	testAPI.http.dispatcherCancel = func() {}
	testAPI.http.dispatcherMu.Unlock()

	conn := dialClientWebSocket(t, server.URL)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	loginClientWebSocket(t, conn, aliceKey, "alice-password")
	return testAPI, server, conn, adminToken, aliceKey
}
