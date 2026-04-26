package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestPebbleStoreEventLogAndMessageProjection(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "node-a", 1, 2)

	user, userEvent, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if userEvent.Sequence != 1 {
		t.Fatalf("unexpected user event sequence: %d", userEvent.Sequence)
	}

	for _, body := range []string{"message-1", "message-2", "message-3"} {
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte(body),
		}); err != nil {
			t.Fatalf("create message %q: %v", body, err)
		}
	}

	lastSequence, err := st.LastEventSequence(ctx)
	if err != nil {
		t.Fatalf("last event sequence: %v", err)
	}
	if lastSequence != 4 {
		t.Fatalf("unexpected last sequence: got=%d want=4", lastSequence)
	}

	events, err := st.ListEvents(ctx, 1, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 || events[0].Sequence != 2 || events[2].Sequence != 4 {
		t.Fatalf("unexpected events after sequence 1: %+v", events)
	}

	progress, err := st.ListOriginProgress(ctx)
	if err != nil {
		t.Fatalf("list origin progress: %v", err)
	}
	if len(progress) != 1 || progress[0].OriginNodeID != st.NodeID() || progress[0].LastEventID != events[2].EventID {
		t.Fatalf("unexpected origin progress: %+v", progress)
	}

	messages, err := st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after trim, got %d", len(messages))
	}
	if string(messages[0].Body) != "message-3" || string(messages[1].Body) != "message-2" {
		t.Fatalf("unexpected messages after trim: %+v", messages)
	}
}

func TestPebbleMessageSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	source := openPebbleTestStore(t, "source", 1, 2)
	target := openPebbleTestStore(t, "target", 2, 2)

	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(userEvent)); err != nil {
		t.Fatalf("apply user event: %v", err)
	}

	for _, body := range []string{"message-1", "message-2", "message-3"} {
		if _, _, err := source.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte(body),
		}); err != nil {
			t.Fatalf("create message %q: %v", body, err)
		}
	}

	chunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition(source.NodeID()))
	if err != nil {
		t.Fatalf("build message snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply message snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply duplicate message snapshot chunk: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil {
		t.Fatalf("list target messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages after snapshot trim, got %d", len(messages))
	}
	if string(messages[0].Body) != "message-3" || string(messages[1].Body) != "message-2" {
		t.Fatalf("unexpected target messages: %+v", messages)
	}
}

func TestPebbleDeferredTrimKeepsVisibleWindowBounded(t *testing.T) {
	const windowSize = 64

	ctx := context.Background()
	st := openPebbleTestStore(t, "deferred-trim", 1, windowSize)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "deferred-trim-user",
		PasswordHash: "hash-deferred-trim",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := 1; i <= 80; i++ {
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte(fmt.Sprintf("message-%02d", i)),
		}); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	messages, err := st.ListMessagesByUser(ctx, user.Key(), 100)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != windowSize {
		t.Fatalf("expected visible window to stay bounded at %d messages, got %d", windowSize, len(messages))
	}
	if string(messages[0].Body) != "message-80" || string(messages[len(messages)-1].Body) != "message-17" {
		t.Fatalf("unexpected visible message window: first=%q last=%q", messages[0].Body, messages[len(messages)-1].Body)
	}

	chunk, err := st.BuildSnapshotChunk(ctx, MessageSnapshotPartition(st.NodeID()))
	if err != nil {
		t.Fatalf("build message snapshot chunk: %v", err)
	}
	if len(chunk.Rows) != windowSize {
		t.Fatalf("expected message snapshot rows to stay bounded at %d, got %d", windowSize, len(chunk.Rows))
	}

	stats, err := st.OperationsStats(ctx, nil)
	if err != nil {
		t.Fatalf("operations stats: %v", err)
	}
	if stats.MessageTrim.TrimmedTotal != 0 {
		t.Fatalf("expected trim to remain deferred below threshold, got %+v", stats.MessageTrim)
	}
}

func TestPebbleCreateMessageRejectsBlockedSender(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "blocked-sender", 1, DefaultMessageWindowSize)

	recipient, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "recipient",
		PasswordHash: "hash-recipient",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	sender, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "sender",
		PasswordHash: "hash-sender",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	if _, _, err := st.BlockUser(ctx, BlacklistParams{
		Owner:   recipient.Key(),
		Blocked: sender.Key(),
	}); err != nil {
		t.Fatalf("block sender: %v", err)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: recipient.Key(),
		Sender:  sender.Key(),
		Body:    []byte("blocked"),
	}); err != ErrBlockedByBlacklist {
		t.Fatalf("expected blacklist rejection, got %v", err)
	}
}

func TestPebbleListMessagesByUserPreservesChannelAndBroadcastSemantics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "login-inbox-semantics", 1, DefaultMessageWindowSize)

	alice, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-bob",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("before subscription"),
	}); err != nil {
		t.Fatalf("create pre-subscription channel message: %v", err)
	}
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: alice.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe alice: %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("after subscription"),
	}); err != nil {
		t.Fatalf("create post-subscription channel message: %v", err)
	}
	broadcastKey := UserKey{NodeID: st.NodeID(), UserID: BroadcastUserID}
	if _, err := st.GetUser(ctx, broadcastKey); err != nil {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("get broadcast user: %v", err)
		}
		now := st.Clock().Now()
		broadcast := User{
			NodeID:              st.NodeID(),
			ID:                  BroadcastUserID,
			Username:            "broadcast",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleBroadcast,
			SystemReserved:      true,
			CreatedAt:           now,
			UpdatedAt:           now,
			VersionUsername:     now,
			VersionPasswordHash: now,
			VersionProfile:      now,
			VersionRole:         now,
			OriginNodeID:        st.NodeID(),
		}
		if err := st.ApplyReplicatedEvent(ctx, ToReplicatedEvent(Event{
			EventID:         9_001,
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: broadcast.NodeID,
			AggregateID:     broadcast.ID,
			HLC:             now,
			OriginNodeID:    testNodeID(9),
			Body:            userCreatedProtoFromUser(broadcast),
		})); err != nil {
			t.Fatalf("seed broadcast user for pebble test: %v", err)
		}
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: broadcastKey,
		Sender:  testSenderKey(9, 1),
		Body:    []byte("broadcast message"),
	}); err != nil {
		t.Fatalf("create broadcast message: %v", err)
	}

	aliceMessages, err := st.ListMessagesByUser(ctx, alice.Key(), 10)
	if err != nil {
		t.Fatalf("list alice messages: %v", err)
	}
	if !messagesContainBody(aliceMessages, "after subscription") || !messagesContainBody(aliceMessages, "broadcast message") {
		t.Fatalf("expected alice to see channel and broadcast messages: %+v", aliceMessages)
	}
	if messagesContainBody(aliceMessages, "before subscription") {
		t.Fatalf("alice should not see channel history before subscription: %+v", aliceMessages)
	}

	bobMessages, err := st.ListMessagesByUser(ctx, bob.Key(), 10)
	if err != nil {
		t.Fatalf("list bob messages: %v", err)
	}
	if !messagesContainBody(bobMessages, "broadcast message") || messagesContainBody(bobMessages, "after subscription") {
		t.Fatalf("unexpected bob messages: %+v", bobMessages)
	}

	charlie, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "charlie",
		PasswordHash: "hash-charlie",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create charlie: %v", err)
	}
	charlieMessages, err := st.ListMessagesByUser(ctx, charlie.Key(), 10)
	if err != nil {
		t.Fatalf("list charlie messages: %v", err)
	}
	if !messagesContainBody(charlieMessages, "broadcast message") {
		t.Fatalf("future user should see retained broadcast messages: %+v", charlieMessages)
	}

	if _, _, err := st.UnsubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: alice.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("unsubscribe alice: %v", err)
	}
	aliceMessages, err = st.ListMessagesByUser(ctx, alice.Key(), 10)
	if err != nil {
		t.Fatalf("list alice messages after unsubscribe: %v", err)
	}
	if messagesContainBody(aliceMessages, "after subscription") {
		t.Fatalf("alice should not see channel messages after unsubscribe: %+v", aliceMessages)
	}
}

func TestPebbleChannelTrimDeletesSubscriberInboxEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "channel-inbox-trim", 1, 1)

	alice, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-alice",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	channel, _, err := st.CreateUser(ctx, CreateUserParams{
		Username: "alerts",
		Role:     RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{
		Subscriber: alice.Key(),
		Channel:    channel.Key(),
	}); err != nil {
		t.Fatalf("subscribe alice: %v", err)
	}

	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("first"),
	}); err != nil {
		t.Fatalf("create first channel message: %v", err)
	}
	if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: channel.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("second"),
	}); err != nil {
		t.Fatalf("create second channel message: %v", err)
	}

	messages, err := st.ListMessagesByUser(ctx, alice.Key(), 10)
	if err != nil {
		t.Fatalf("list alice messages: %v", err)
	}
	if len(messages) != 1 || string(messages[0].Body) != "second" {
		t.Fatalf("expected trimmed subscriber inbox to keep only latest channel message, got %+v", messages)
	}
}

func TestPebbleCreateMessageUsesConfiguredSyncModeByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	noSyncStore := openPebbleTestStoreWithSyncMode(t, "default-no-sync", 1, DefaultMessageWindowSize, PebbleMessageSyncModeNoSync)
	noSyncUser, _, err := noSyncStore.CreateUser(ctx, CreateUserParams{
		Username:     "default-no-sync-user",
		PasswordHash: "hash-default-no-sync",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create no-sync user: %v", err)
	}
	if _, _, err := noSyncStore.CreateMessage(ctx, CreateMessageParams{
		UserKey: noSyncUser.Key(),
		Sender:  noSyncUser.Key(),
		Body:    []byte("default-no-sync-message"),
	}); err != nil {
		t.Fatalf("create default no-sync message: %v", err)
	}
	noSyncStats := pebbleLocalMessageBatchStatsForTest(t, noSyncStore)
	if noSyncStats.NoSyncBatches != 1 || noSyncStats.ForceSyncBatches != 0 {
		t.Fatalf("unexpected default no-sync stats: %+v", noSyncStats)
	}

	forceSyncStore := openPebbleTestStoreWithSyncMode(t, "default-force-sync", 2, DefaultMessageWindowSize, PebbleMessageSyncModeForceSync)
	forceSyncUser, _, err := forceSyncStore.CreateUser(ctx, CreateUserParams{
		Username:     "default-force-sync-user",
		PasswordHash: "hash-default-force-sync",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create force-sync user: %v", err)
	}
	if _, _, err := forceSyncStore.CreateMessage(ctx, CreateMessageParams{
		UserKey: forceSyncUser.Key(),
		Sender:  forceSyncUser.Key(),
		Body:    []byte("default-force-sync-message"),
	}); err != nil {
		t.Fatalf("create default force-sync message: %v", err)
	}
	forceSyncStats := pebbleLocalMessageBatchStatsForTest(t, forceSyncStore)
	if forceSyncStats.ForceSyncBatches != 1 || forceSyncStats.NoSyncBatches != 0 {
		t.Fatalf("unexpected default force-sync stats: %+v", forceSyncStats)
	}
}

func TestPebbleCreateMessageRequestSyncModeOverridesDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	forceSyncStore := openPebbleTestStoreWithSyncMode(t, "override-to-no-sync", 1, DefaultMessageWindowSize, PebbleMessageSyncModeForceSync)
	forceSyncUser, _, err := forceSyncStore.CreateUser(ctx, CreateUserParams{
		Username:     "override-force-sync-user",
		PasswordHash: "hash-override-force-sync",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create force-sync user: %v", err)
	}
	if _, _, err := forceSyncStore.CreateMessage(ctx, CreateMessageParams{
		UserKey:               forceSyncUser.Key(),
		Sender:                forceSyncUser.Key(),
		Body:                  []byte("override-no-sync-message"),
		PebbleMessageSyncMode: PebbleMessageSyncModeNoSync,
	}); err != nil {
		t.Fatalf("create override no-sync message: %v", err)
	}
	forceSyncStats := pebbleLocalMessageBatchStatsForTest(t, forceSyncStore)
	if forceSyncStats.NoSyncBatches != 1 || forceSyncStats.ForceSyncBatches != 0 {
		t.Fatalf("unexpected override-to-no-sync stats: %+v", forceSyncStats)
	}

	noSyncStore := openPebbleTestStoreWithSyncMode(t, "override-to-force-sync", 2, DefaultMessageWindowSize, PebbleMessageSyncModeNoSync)
	noSyncUser, _, err := noSyncStore.CreateUser(ctx, CreateUserParams{
		Username:     "override-no-sync-user",
		PasswordHash: "hash-override-no-sync",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create no-sync user: %v", err)
	}
	if _, _, err := noSyncStore.CreateMessage(ctx, CreateMessageParams{
		UserKey:               noSyncUser.Key(),
		Sender:                noSyncUser.Key(),
		Body:                  []byte("override-force-sync-message"),
		PebbleMessageSyncMode: PebbleMessageSyncModeForceSync,
	}); err != nil {
		t.Fatalf("create override force-sync message: %v", err)
	}
	noSyncStats := pebbleLocalMessageBatchStatsForTest(t, noSyncStore)
	if noSyncStats.ForceSyncBatches != 1 || noSyncStats.NoSyncBatches != 0 {
		t.Fatalf("unexpected override-to-force-sync stats: %+v", noSyncStats)
	}
}

func TestPebbleLocalMessageSyncModeSegmentsPreserveOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStoreWithSyncMode(t, "mixed-sync-batches", 1, DefaultMessageWindowSize, PebbleMessageSyncModeNoSync)
	backend := requirePebbleBackend(t, st)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "mixed-sync-user",
		PasswordHash: "hash-mixed-sync",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	requests := []pebbleLocalMessageWriteRequest{
		{
			params: CreateMessageParams{
				UserKey:               user.Key(),
				Sender:                user.Key(),
				Body:                  []byte("m1"),
				PebbleMessageSyncMode: PebbleMessageSyncModeForceSync,
			},
			response: make(chan pebbleLocalMessageWriteResult, 1),
		},
		{
			params: CreateMessageParams{
				UserKey:               user.Key(),
				Sender:                user.Key(),
				Body:                  []byte("m2"),
				PebbleMessageSyncMode: PebbleMessageSyncModeNoSync,
			},
			response: make(chan pebbleLocalMessageWriteResult, 1),
		},
		{
			params: CreateMessageParams{
				UserKey:               user.Key(),
				Sender:                user.Key(),
				Body:                  []byte("m3"),
				PebbleMessageSyncMode: PebbleMessageSyncModeNoSync,
			},
			response: make(chan pebbleLocalMessageWriteResult, 1),
		},
		{
			params: CreateMessageParams{
				UserKey:               user.Key(),
				Sender:                user.Key(),
				Body:                  []byte("m4"),
				PebbleMessageSyncMode: PebbleMessageSyncModeForceSync,
			},
			response: make(chan pebbleLocalMessageWriteResult, 1),
		},
	}

	pending := requests
	for len(pending) > 0 {
		segmentEnd := contiguousLocalMessageSyncModePrefix(pending)
		backend.processLocalMessageBatch(pending[:segmentEnd])
		pending = pending[segmentEnd:]
	}

	for i, request := range requests {
		result := <-request.response
		if result.err != nil {
			t.Fatalf("request %d failed: %v", i, result.err)
		}
		if result.message.Seq != int64(i+1) {
			t.Fatalf("unexpected message seq for request %d: %+v", i, result.message)
		}
	}

	stats := pebbleLocalMessageBatchStatsForTest(t, st)
	if stats.ForceSyncBatches != 2 || stats.NoSyncBatches != 1 {
		t.Fatalf("unexpected mixed-sync batch stats: %+v", stats)
	}
}

func TestPebbleBackgroundTrimEventuallyUpdatesMessageUserState(t *testing.T) {
	t.Parallel()

	const windowSize = 64

	ctx := context.Background()
	st := openPebbleTestStore(t, "background-trim", 1, windowSize)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "background-trim-user",
		PasswordHash: "hash-background-trim",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := 1; i <= 110; i++ {
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte(fmt.Sprintf("message-%03d", i)),
		}); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	waitForPebbleCondition(t, time.Second, func() bool {
		state := readPebbleMessageUserStateForTest(t, st, user.Key())
		stats, err := st.OperationsStats(ctx, nil)
		if err != nil {
			return false
		}
		return state.StoredCount == int64(windowSize) && !state.TrimNeeded && stats.MessageTrim.TrimmedTotal > 0
	})
}

func TestPebbleSnapshotApplyRepairsMessageUserState(t *testing.T) {
	t.Parallel()

	const windowSize = 4

	ctx := context.Background()
	source := openPebbleTestStore(t, "snapshot-state-source", 1, windowSize)
	target := openPebbleTestStore(t, "snapshot-state-target", 2, windowSize)

	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "snapshot-state-user",
		PasswordHash: "hash-snapshot-state",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(userEvent)); err != nil {
		t.Fatalf("apply user event: %v", err)
	}

	for i := 1; i <= 8; i++ {
		if _, _, err := source.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  testSenderKey(9, 1),
			Body:    []byte(fmt.Sprintf("snapshot-state-%02d", i)),
		}); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	chunk, err := source.BuildSnapshotChunk(ctx, MessageSnapshotPartition(source.NodeID()))
	if err != nil {
		t.Fatalf("build snapshot chunk: %v", err)
	}
	if err := target.ApplySnapshotChunk(ctx, chunk); err != nil {
		t.Fatalf("apply snapshot chunk: %v", err)
	}

	state := readPebbleMessageUserStateForTest(t, target, user.Key())
	if state.StoredCount != int64(windowSize) || state.TrimNeeded {
		t.Fatalf("unexpected repaired message user state: %+v", state)
	}
}

func TestPebbleMessageSequencePersistsAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	first := openPersistentPebbleTestStore(t, dir, "restart", 1, DefaultMessageWindowSize)
	user, _, err := first.CreateUser(ctx, CreateUserParams{
		Username:     "restart-sequence-user",
		PasswordHash: "hash-restart-sequence",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	message, _, err := first.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("first"),
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if message.Seq != 1 {
		t.Fatalf("unexpected first message seq: %+v", message)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	second := openPersistentPebbleTestStore(t, dir, "restart", 1, DefaultMessageWindowSize)
	defer second.Close()

	restartedMessage, _, err := second.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("second"),
	})
	if err != nil {
		t.Fatalf("create second message after restart: %v", err)
	}
	if restartedMessage.Seq != 2 {
		t.Fatalf("expected restarted store to continue from seq 2, got %+v", restartedMessage)
	}
}

func TestPebbleMessageSequenceSeedsFromLegacySQLiteCounter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "legacy-sequence-counter", 1, DefaultMessageWindowSize)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "legacy-sequence-user",
		PasswordHash: "hash-legacy-sequence",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if _, err := st.db.ExecContext(ctx, `
INSERT INTO message_sequence_counters(user_node_id, user_id, node_id, next_seq)
VALUES(?, ?, ?, ?)
`, user.NodeID, user.ID, st.NodeID(), 41); err != nil {
		t.Fatalf("seed legacy message sequence counter: %v", err)
	}

	first, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("legacy-first"),
	})
	if err != nil {
		t.Fatalf("create first message from legacy counter: %v", err)
	}
	second, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("legacy-second"),
	})
	if err != nil {
		t.Fatalf("create second message from pebble sequence: %v", err)
	}

	if first.Seq != 41 || second.Seq != 42 {
		t.Fatalf("expected legacy counter to seed pebble seqs 41/42, got first=%+v second=%+v", first, second)
	}

	var nextSeq int64
	if err := st.db.QueryRowContext(ctx, `
SELECT next_seq
FROM message_sequence_counters
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, user.NodeID, user.ID, st.NodeID()).Scan(&nextSeq); err != nil {
		t.Fatalf("read legacy message sequence counter: %v", err)
	}
	if nextSeq != 41 {
		t.Fatalf("expected pebble create message to stop updating SQLite counter, got %d", nextSeq)
	}
}

func TestPebblePruneEventLogKeepsLatestEventsPerOrigin(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStoreWithRetention(t, "node-a", 1, 10, 2)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "pebble-prune",
		PasswordHash: "hash-pebble",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, firstMessageEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("first"),
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	_, secondMessageEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("second"),
	})
	if err != nil {
		t.Fatalf("create second message: %v", err)
	}
	_, thirdMessageEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  testSenderKey(9, 1),
		Body:    []byte("third"),
	})
	if err != nil {
		t.Fatalf("create third message: %v", err)
	}

	result, err := st.PruneEventLogOnce(ctx)
	if err != nil {
		t.Fatalf("prune event log: %v", err)
	}
	if result.TrimmedEvents != 2 || result.OriginsAffected != 1 {
		t.Fatalf("unexpected prune result: %+v", result)
	}

	retained, err := st.ListEventsByOrigin(ctx, st.NodeID(), 0, 10)
	if err != nil {
		t.Fatalf("list retained events: %v", err)
	}
	if len(retained) != 2 || retained[0].EventID != secondMessageEvent.EventID || retained[1].EventID != thirdMessageEvent.EventID {
		t.Fatalf("unexpected retained events: %+v", retained)
	}

	truncatedBefore, err := st.EventLogTruncatedBefore(ctx, st.NodeID())
	if err != nil {
		t.Fatalf("read truncated boundary: %v", err)
	}
	if truncatedBefore != firstMessageEvent.EventID {
		t.Fatalf("unexpected truncated boundary: got=%d want=%d", truncatedBefore, firstMessageEvent.EventID)
	}

	stats, err := st.eventLogTrimStats(ctx)
	if err != nil {
		t.Fatalf("event log trim stats: %v", err)
	}
	if stats.TrimmedTotal != 2 || stats.LastTrimmedAt == nil {
		t.Fatalf("unexpected event log trim stats: %+v", stats)
	}
}

func TestPebblePeerAndOriginCursorsBypassSQLite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "pebble-cursors", 1, DefaultMessageWindowSize)

	if err := st.RecordPeerAck(ctx, testNodeID(2), testNodeID(3), 7); err != nil {
		t.Fatalf("record peer ack: %v", err)
	}
	if err := st.RecordPeerAck(ctx, testNodeID(2), testNodeID(3), 5); err != nil {
		t.Fatalf("record peer ack monotonic update: %v", err)
	}
	ackCursor, err := st.GetPeerAckCursor(ctx, testNodeID(2), testNodeID(3))
	if err != nil {
		t.Fatalf("get peer ack cursor: %v", err)
	}
	if ackCursor.AckedEventID != 7 {
		t.Fatalf("expected monotonic peer ack cursor, got %+v", ackCursor)
	}

	if err := st.RecordOriginApplied(ctx, testNodeID(3), 11); err != nil {
		t.Fatalf("record origin applied: %v", err)
	}
	if err := st.RecordOriginApplied(ctx, testNodeID(3), 9); err != nil {
		t.Fatalf("record origin applied monotonic update: %v", err)
	}
	originCursor, err := st.GetOriginCursor(ctx, testNodeID(3))
	if err != nil {
		t.Fatalf("get origin cursor: %v", err)
	}
	if originCursor.AppliedEventID != 11 {
		t.Fatalf("expected monotonic origin cursor, got %+v", originCursor)
	}

	var peerAckRows int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peer_ack_cursors`).Scan(&peerAckRows); err != nil {
		t.Fatalf("count sqlite peer ack rows: %v", err)
	}
	if peerAckRows != 0 {
		t.Fatalf("expected pebble peer ack cursor writes to bypass SQLite, found %d rows", peerAckRows)
	}

	var originRows int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM origin_cursors`).Scan(&originRows); err != nil {
		t.Fatalf("count sqlite origin cursor rows: %v", err)
	}
	if originRows != 0 {
		t.Fatalf("expected pebble origin cursor writes to bypass SQLite, found %d rows", originRows)
	}
}

func TestPebblePendingProjectionsBypassSQLite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := openPebbleTestStore(t, "pebble-pending-projections", 1, DefaultMessageWindowSize)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "pending-user",
		PasswordHash: "hash-pending-user",
		Role:         RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, event, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: user.Key(),
		Sender:  user.Key(),
		Body:    []byte("pending"),
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}

	if err := st.recordPendingProjection(ctx, event, errors.New("projection failed once")); err != nil {
		t.Fatalf("record pending projection first time: %v", err)
	}
	if err := st.recordPendingProjection(ctx, event, errors.New("projection failed twice")); err != nil {
		t.Fatalf("record pending projection second time: %v", err)
	}

	stats, err := st.projectionStats(ctx)
	if err != nil {
		t.Fatalf("projection stats: %v", err)
	}
	if stats.PendingTotal != 1 || stats.LastFailedAt == nil {
		t.Fatalf("unexpected pebble pending projection stats: %+v", stats)
	}

	events, err := st.listPendingProjectionEvents(ctx, 10)
	if err != nil {
		t.Fatalf("list pending projection events: %v", err)
	}
	if len(events) != 1 || events[0].OriginNodeID != event.OriginNodeID || events[0].EventID != event.EventID {
		t.Fatalf("unexpected pending projection events: %+v", events)
	}

	var sqliteRows int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_projections`).Scan(&sqliteRows); err != nil {
		t.Fatalf("count sqlite pending projections: %v", err)
	}
	if sqliteRows != 0 {
		t.Fatalf("expected pebble pending projections to bypass SQLite, found %d rows", sqliteRows)
	}

	if err := st.clearPendingProjection(ctx, event.OriginNodeID, event.EventID); err != nil {
		t.Fatalf("clear pending projection: %v", err)
	}
	stats, err = st.projectionStats(ctx)
	if err != nil {
		t.Fatalf("projection stats after clear: %v", err)
	}
	if stats.PendingTotal != 0 {
		t.Fatalf("expected pending projections to be cleared, got %+v", stats)
	}
}

func openPebbleTestStore(t *testing.T, name string, nodeSlot uint16, messageWindowSize int) *Store {
	t.Helper()
	return openPebbleTestStoreWithOptions(t, name, nodeSlot, messageWindowSize, DefaultEventLogMaxEventsPerOrigin, PebbleMessageSyncModeNoSync)
}

func openPebbleTestStoreWithRetention(t *testing.T, name string, nodeSlot uint16, messageWindowSize int, maxEventsPerOrigin int) *Store {
	t.Helper()
	return openPebbleTestStoreWithOptions(t, name, nodeSlot, messageWindowSize, maxEventsPerOrigin, PebbleMessageSyncModeNoSync)
}

func openPebbleTestStoreWithSyncMode(t *testing.T, name string, nodeSlot uint16, messageWindowSize int, syncMode PebbleMessageSyncMode) *Store {
	t.Helper()
	return openPebbleTestStoreWithOptions(t, name, nodeSlot, messageWindowSize, DefaultEventLogMaxEventsPerOrigin, syncMode)
}

func openPebbleTestStoreWithOptions(t *testing.T, name string, nodeSlot uint16, messageWindowSize int, maxEventsPerOrigin int, syncMode PebbleMessageSyncMode) *Store {
	t.Helper()

	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, name+".db"), Options{
		NodeID:                     testNodeID(nodeSlot),
		Engine:                     EnginePebble,
		PebblePath:                 filepath.Join(dir, name+".pebble"),
		PebbleMessageSyncMode:      syncMode,
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	})
	if err != nil {
		t.Fatalf("open pebble store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close pebble store: %v", err)
		}
	})
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init pebble store: %v", err)
	}
	return st
}

func openPersistentPebbleTestStore(t *testing.T, dir, name string, nodeSlot uint16, messageWindowSize int) *Store {
	t.Helper()

	st, err := Open(filepath.Join(dir, name+".db"), Options{
		NodeID:                     testNodeID(nodeSlot),
		Engine:                     EnginePebble,
		PebblePath:                 filepath.Join(dir, name+".pebble"),
		PebbleMessageSyncMode:      PebbleMessageSyncModeNoSync,
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: DefaultEventLogMaxEventsPerOrigin,
	})
	if err != nil {
		t.Fatalf("open persistent pebble store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		t.Fatalf("init persistent pebble store: %v", err)
	}
	return st
}

func readPebbleMessageUserStateForTest(tb testing.TB, st *Store, key UserKey) pebbleMessageUserState {
	tb.Helper()

	backend := requirePebbleBackend(tb, st)
	state, ok, err := backend.messageProjectionRepo.readMessageUserStateLocked(key)
	if err != nil {
		tb.Fatalf("read pebble message user state: %v", err)
	}
	if !ok {
		tb.Fatalf("expected pebble message user state for %+v", key)
	}
	return state
}

func waitForPebbleCondition(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not satisfied within %s", timeout)
}

func pebbleLocalMessageBatchStatsForTest(tb testing.TB, st *Store) pebbleLocalMessageBatchStatsSnapshot {
	tb.Helper()
	return requirePebbleBackend(tb, st).localMessageStats.snapshot()
}
