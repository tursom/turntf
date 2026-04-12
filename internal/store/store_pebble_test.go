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
			Sender:  "system",
			Body:    body,
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
	if messages[0].Body != "message-3" || messages[1].Body != "message-2" {
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
			Sender:  "system",
			Body:    body,
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
	if messages[0].Body != "message-3" || messages[1].Body != "message-2" {
		t.Fatalf("unexpected target messages: %+v", messages)
	}
}

func openPebbleTestStore(t *testing.T, name string, nodeSlot uint16, messageWindowSize int) *Store {
	t.Helper()

	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, name+".db"), Options{
		NodeID:            testNodeID(nodeSlot),
		Engine:            EnginePebble,
		PebblePath:        filepath.Join(dir, name+".pebble"),
		MessageWindowSize: messageWindowSize,
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
