package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type persistentPayloadCaptureConn struct {
	mu       sync.Mutex
	payloads [][]byte
	sendErr  error
	closed   bool
}

func (c *persistentPayloadCaptureConn) Send(_ context.Context, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sendErr != nil {
		return c.sendErr
	}
	c.payloads = append(c.payloads, payload)
	return nil
}

func (c *persistentPayloadCaptureConn) Receive(context.Context) ([]byte, error) {
	return nil, errors.New("receive is not implemented for persistent payload capture")
}

func (c *persistentPayloadCaptureConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *persistentPayloadCaptureConn) RemoteAddr() string { return "capture" }

func (c *persistentPayloadCaptureConn) Transport() string { return "capture" }

func (c *persistentPayloadCaptureConn) singlePayload(t *testing.T) []byte {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.payloads) != 1 {
		t.Fatalf("expected one persistent payload, got %d", len(c.payloads))
	}
	return c.payloads[0]
}

func TestPersistentDispatcherSharesEncodedBroadcastAcrossReadyAndQueuedSessions(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	ctx := context.Background()
	userA := mustCreatePersistentDispatchUser(t, testAPI.http, "dispatcher-broadcast-a")
	userB := mustCreatePersistentDispatchUser(t, testAPI.http, "dispatcher-broadcast-b")
	userC := mustCreatePersistentDispatchUser(t, testAPI.http, "dispatcher-broadcast-c")

	connA := &persistentPayloadCaptureConn{}
	connB := &persistentPayloadCaptureConn{}
	connC := &persistentPayloadCaptureConn{}
	sessA := newPersistentPayloadCaptureSession(testAPI.http, userA, connA)
	sessB := newPersistentPayloadCaptureSession(testAPI.http, userB, connB)
	sessC := newPersistentPayloadCaptureSession(testAPI.http, userC, connC)
	sessC.persistentReady = false
	registerPersistentPayloadCaptureSessions(t, testAPI.http, sessA, sessB, sessC)

	message := store.Message{
		Recipient: store.UserKey{NodeID: testNodeID(1), UserID: store.BroadcastUserID},
		NodeID:    testNodeID(1),
		Seq:       42,
		Sender:    store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID},
		Body:      []byte("shared-broadcast-payload"),
		CreatedAt: testAPI.http.service.store.Clock().Now(),
	}
	testAPI.http.dispatchPersistentMessage(ctx, 100, message)

	payloadA := connA.singlePayload(t)
	payloadB := connB.singlePayload(t)
	if len(payloadA) == 0 || len(payloadB) == 0 {
		t.Fatal("expected non-empty persistent payloads")
	}
	if &payloadA[0] != &payloadB[0] {
		t.Fatal("expected broadcast sessions to share one encoded payload")
	}
	if connC.payloadCount() != 0 {
		t.Fatal("expected queued session to wait until persistent dispatch is enabled")
	}
	if err := sessC.enablePersistentDispatch(); err != nil {
		t.Fatalf("enable queued persistent dispatch: %v", err)
	}
	payloadC := connC.singlePayload(t)
	if len(payloadC) == 0 || &payloadA[0] != &payloadC[0] {
		t.Fatal("expected queued session to retain the shared encoded payload")
	}

	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(payloadA, &envelope); err != nil {
		t.Fatalf("unmarshal shared persistent payload: %v", err)
	}
	pushed := envelope.GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "shared-broadcast-payload" {
		t.Fatalf("unexpected shared persistent payload: %+v", pushed)
	}

	testAPI.http.dispatchPersistentMessage(ctx, 100, message)
	if connA.payloadCount() != 1 || connB.payloadCount() != 1 || connC.payloadCount() != 1 {
		t.Fatal("expected duplicate event sequence to be skipped")
	}
	testAPI.http.dispatchPersistentMessage(ctx, 101, message)
	if connA.payloadCount() != 1 || connB.payloadCount() != 1 || connC.payloadCount() != 1 {
		t.Fatal("expected duplicate message cursor to be skipped")
	}
}

func (c *persistentPayloadCaptureConn) payloadCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.payloads)
}

func (c *persistentPayloadCaptureConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func TestPersistentDispatcherContinuesSharedBroadcastAfterSessionWriteFailure(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	ctx := context.Background()
	failingUser := mustCreatePersistentDispatchUser(t, testAPI.http, "dispatcher-failing-broadcast")
	healthyUser := mustCreatePersistentDispatchUser(t, testAPI.http, "dispatcher-healthy-broadcast")

	failingConn := &persistentPayloadCaptureConn{sendErr: errors.New("write failed")}
	healthyConn := &persistentPayloadCaptureConn{}
	failingSession := newPersistentPayloadCaptureSession(testAPI.http, failingUser, failingConn)
	healthySession := newPersistentPayloadCaptureSession(testAPI.http, healthyUser, healthyConn)
	registerPersistentPayloadCaptureSessions(t, testAPI.http, failingSession, healthySession)

	testAPI.http.dispatchPersistentMessage(ctx, 101, store.Message{
		Recipient: store.UserKey{NodeID: testNodeID(1), UserID: store.BroadcastUserID},
		NodeID:    testNodeID(1),
		Seq:       43,
		Sender:    store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID},
		Body:      []byte("broadcast-after-write-failure"),
		CreatedAt: testAPI.http.service.store.Clock().Now(),
	})

	if !failingConn.isClosed() {
		t.Fatal("expected failed persistent session to be closed")
	}
	payload := healthyConn.singlePayload(t)
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("unmarshal healthy persistent payload: %v", err)
	}
	if pushed := envelope.GetMessagePushed(); pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "broadcast-after-write-failure" {
		t.Fatalf("unexpected healthy persistent payload: %+v", envelope.GetMessagePushed())
	}
}

func mustCreatePersistentDispatchUser(t *testing.T, httpAPI *HTTP, username string) store.User {
	t.Helper()
	user, _, err := httpAPI.service.CreateUser(context.Background(), store.CreateUserParams{
		Username:     username,
		PasswordHash: "!",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create persistent dispatch user %q: %v", username, err)
	}
	return user
}

func newPersistentPayloadCaptureSession(httpAPI *HTTP, user store.User, conn *persistentPayloadCaptureConn) *clientWSSession {
	return &clientWSSession{
		http:            httpAPI,
		conn:            conn,
		protocol:        "capture",
		remoteAddr:      "capture",
		principal:       &requestPrincipal{User: user},
		seen:            make(map[clientMessageCursor]struct{}),
		persistentReady: true,
		blacklistCache:  make(map[store.UserKey]clientBoolCacheEntry),
	}
}

func registerPersistentPayloadCaptureSessions(t *testing.T, httpAPI *HTTP, sessions ...*clientWSSession) {
	t.Helper()
	for _, sess := range sessions {
		if err := httpAPI.persistentSessions.Register(context.Background(), sess); err != nil {
			t.Fatalf("register persistent payload capture session: %v", err)
		}
		t.Cleanup(func() {
			httpAPI.persistentSessions.Unregister(sess)
		})
	}
}

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

func TestPersistentDispatcherRetriesChannelEventAfterSubscriptionReloadFailure(t *testing.T) {
	testAPI, _, conn, adminToken, aliceKey := openControlledPersistentDispatcherTest(t)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "dispatcher-channel", "channel-password", store.RoleChannel)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"channel_node_id": channelKey.NodeID,
		"channel_user_id": channelKey.UserID,
	}, map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusCreated)

	afterSequence, err := testAPI.http.service.LastEventSequence(context.Background())
	if err != nil {
		t.Fatalf("load channel retry dispatcher watermark: %v", err)
	}

	index := testAPI.http.persistentSessions
	originalLoad := index.load
	reloadStarted := make(chan struct{})
	releaseReload := make(chan struct{})
	firstLoad := true
	index.load = func(ctx context.Context, subscriber store.UserKey) ([]store.Subscription, error) {
		if firstLoad {
			firstLoad = false
			close(reloadStarted)
			<-releaseReload
			return nil, fmt.Errorf("temporary subscription reload failure: %w", store.ErrNotFound)
		}
		return originalLoad(ctx, subscriber)
	}
	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: aliceKey,
		Channel:    channelKey,
		Reload:     true,
	}})

	fallback := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go testAPI.http.runPersistentDispatcherWithFallback(ctx, afterSequence, nil, fallback)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	if _, _, err := testAPI.http.service.store.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: channelKey,
		Sender:  adminKey,
		Body:    []byte("channel-reload-retry"),
	}); err != nil {
		t.Fatalf("create channel retry message: %v", err)
	}
	fallback <- time.Now()
	<-reloadStarted
	close(releaseReload)
	fallback <- time.Now()

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "channel-reload-retry" {
		t.Fatalf("unexpected retried channel event push: %+v", pushed)
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
