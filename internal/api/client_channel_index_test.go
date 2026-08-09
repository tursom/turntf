package api

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/tursom/turntf/internal/store"
)

func TestPersistentChannelDispatchTracksCommittedOnlineSubscriptions(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	ctx := context.Background()
	channel, _, err := testAPI.http.service.CreateUser(ctx, store.CreateUserParams{
		Username: "indexed-dispatch-channel",
		Role:     store.RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	userA := mustCreatePersistentDispatchUser(t, testAPI.http, "indexed-dispatch-a")
	userB := mustCreatePersistentDispatchUser(t, testAPI.http, "indexed-dispatch-b")
	admin, err := testAPI.http.service.GetUser(ctx, store.UserKey{
		NodeID: testNodeID(1),
		UserID: store.BootstrapAdminUserID,
	})
	if err != nil {
		t.Fatalf("load bootstrap admin: %v", err)
	}
	if _, _, err := testAPI.http.service.SubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: userA.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe first user: %v", err)
	}

	connA := &persistentPayloadCaptureConn{}
	connB := &persistentPayloadCaptureConn{}
	adminConn := &persistentPayloadCaptureConn{}
	sessA := newPersistentPayloadCaptureSession(testAPI.http, userA, connA)
	sessB := newPersistentPayloadCaptureSession(testAPI.http, userB, connB)
	adminSession := newPersistentPayloadCaptureSession(testAPI.http, admin, adminConn)
	for _, sess := range []*clientWSSession{sessA, sessB, adminSession} {
		if err := testAPI.http.persistentSessions.Register(ctx, sess); err != nil {
			t.Fatalf("register indexed persistent session: %v", err)
		}
		defer testAPI.http.persistentSessions.Unregister(sess)
	}

	dispatchChannelTestMessage(t, testAPI.http, channel.Key(), 100, 1)
	if connA.payloadCount() != 1 || connB.payloadCount() != 0 || adminConn.payloadCount() != 1 {
		t.Fatalf("unexpected initial channel delivery counts: a=%d b=%d admin=%d", connA.payloadCount(), connB.payloadCount(), adminConn.payloadCount())
	}

	if _, _, err := testAPI.http.service.UnsubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: userA.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("unsubscribe first user: %v", err)
	}
	dispatchChannelTestMessage(t, testAPI.http, channel.Key(), 101, 2)
	if connA.payloadCount() != 1 || connB.payloadCount() != 0 || adminConn.payloadCount() != 2 {
		t.Fatalf("unexpected post-unsubscribe delivery counts: a=%d b=%d admin=%d", connA.payloadCount(), connB.payloadCount(), adminConn.payloadCount())
	}

	if _, _, err := testAPI.http.service.UpsertAttachment(ctx, store.UpsertAttachmentParams{
		Owner:      userB.Key(),
		Subject:    channel.Key(),
		Type:       store.AttachmentTypeChannelSubscription,
		ConfigJSON: "{}",
	}); err != nil {
		t.Fatalf("subscribe second user through generic attachment write: %v", err)
	}
	dispatchChannelTestMessage(t, testAPI.http, channel.Key(), 102, 3)
	if connA.payloadCount() != 1 || connB.payloadCount() != 1 || adminConn.payloadCount() != 3 {
		t.Fatalf("unexpected generic-subscribe delivery counts: a=%d b=%d admin=%d", connA.payloadCount(), connB.payloadCount(), adminConn.payloadCount())
	}
	if got := testAPI.http.persistentCandidateCount.Load(); got != 5 {
		t.Fatalf("unexpected total channel candidates: got=%d want=5", got)
	}
}

func TestPersistentChannelDispatchTracksReplicatedAndSnapshotSubscriptions(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	ctx := context.Background()
	source, err := store.Open(filepath.Join(t.TempDir(), "channel-index-source.db"), store.Options{NodeID: testNodeID(2)})
	if err != nil {
		t.Fatalf("open source store: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if err := source.Init(ctx); err != nil {
		t.Fatalf("init source store: %v", err)
	}

	subscriber, subscriberEvent, err := source.CreateUser(ctx, store.CreateUserParams{
		Username:     "replicated-index-subscriber",
		PasswordHash: "!",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create source subscriber: %v", err)
	}
	channel, channelEvent, err := source.CreateUser(ctx, store.CreateUserParams{
		Username: "replicated-index-channel",
		Role:     store.RoleChannel,
	})
	if err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	for _, event := range []store.Event{subscriberEvent, channelEvent} {
		if err := testAPI.http.service.store.ApplyReplicatedEvent(ctx, store.ToReplicatedEvent(event)); err != nil {
			t.Fatalf("apply replicated user prerequisite: %v", err)
		}
	}

	conn := &persistentPayloadCaptureConn{}
	sess := newPersistentPayloadCaptureSession(testAPI.http, subscriber, conn)
	if err := testAPI.http.persistentSessions.Register(ctx, sess); err != nil {
		t.Fatalf("register replicated subscription session: %v", err)
	}
	defer testAPI.http.persistentSessions.Unregister(sess)

	_, subscribedEvent, err := source.SubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	})
	if err != nil {
		t.Fatalf("subscribe source channel: %v", err)
	}
	if err := testAPI.http.service.store.ApplyReplicatedEvent(ctx, store.ToReplicatedEvent(subscribedEvent)); err != nil {
		t.Fatalf("apply replicated subscription: %v", err)
	}
	dispatchChannelTestMessage(t, testAPI.http, channel.Key(), 200, 11)
	if conn.payloadCount() != 1 {
		t.Fatalf("expected replicated subscription delivery, got %d payloads", conn.payloadCount())
	}

	_, unsubscribedEvent, err := source.UnsubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	})
	if err != nil {
		t.Fatalf("unsubscribe source channel: %v", err)
	}
	if err := testAPI.http.service.store.ApplyReplicatedEvent(ctx, store.ToReplicatedEvent(unsubscribedEvent)); err != nil {
		t.Fatalf("apply replicated unsubscribe: %v", err)
	}
	dispatchChannelTestMessage(t, testAPI.http, channel.Key(), 201, 12)
	if conn.payloadCount() != 1 {
		t.Fatalf("expected replicated unsubscribe to stop delivery, got %d payloads", conn.payloadCount())
	}

	if _, _, err := source.SubscribeChannel(ctx, store.ChannelSubscriptionParams{
		Subscriber: subscriber.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("resubscribe source channel: %v", err)
	}
	chunk, err := source.BuildSnapshotChunk(ctx, store.SnapshotAttachmentsPartition)
	if err != nil {
		t.Fatalf("build source attachments snapshot: %v", err)
	}
	if err := testAPI.http.service.store.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply subscription repair snapshot: %v", err)
	}
	dispatchChannelTestMessage(t, testAPI.http, channel.Key(), 202, 13)
	if conn.payloadCount() != 2 {
		t.Fatalf("expected snapshot-repaired subscription delivery, got %d payloads", conn.payloadCount())
	}
}

func dispatchChannelTestMessage(t *testing.T, httpAPI *HTTP, channel store.UserKey, eventSequence, messageSequence int64) {
	t.Helper()
	httpAPI.dispatchPersistentMessage(context.Background(), eventSequence, store.Message{
		Recipient: channel,
		NodeID:    testNodeID(1),
		Seq:       messageSequence,
		Sender:    store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID},
		Body:      []byte("indexed-channel-message"),
		CreatedAt: httpAPI.service.store.Clock().Now(),
	})
}
