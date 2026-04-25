package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
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

func openPebbleTestStore(t *testing.T, name string, nodeSlot uint16, messageWindowSize int) *Store {
	t.Helper()
	return openPebbleTestStoreWithRetention(t, name, nodeSlot, messageWindowSize, DefaultEventLogMaxEventsPerOrigin)
}

func openPebbleTestStoreWithRetention(t *testing.T, name string, nodeSlot uint16, messageWindowSize int, maxEventsPerOrigin int) *Store {
	t.Helper()

	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, name+".db"), Options{
		NodeID:                     testNodeID(nodeSlot),
		Engine:                     EnginePebble,
		PebblePath:                 filepath.Join(dir, name+".pebble"),
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
