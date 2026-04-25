package store

import (
	"context"
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
