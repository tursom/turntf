package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestLocalUserCRUDAndEventLog(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, createEvent, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Alice"}`,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if createEvent.Kind != "user.created" {
		t.Fatalf("unexpected create event kind: %s", createEvent.Kind)
	}

	loaded, err := st.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if loaded.Username != "alice" {
		t.Fatalf("unexpected username: %s", loaded.Username)
	}

	newUsername := "alice-updated"
	newProfile := `{"display_name":"Alice Updated"}`
	newPasswordHash := "hash-2"
	updated, updateEvent, err := st.UpdateUser(ctx, UpdateUserParams{
		UserID:       user.ID,
		Username:     &newUsername,
		Profile:      &newProfile,
		PasswordHash: &newPasswordHash,
	})
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	if updateEvent.Kind != "user.updated" {
		t.Fatalf("unexpected update event kind: %s", updateEvent.Kind)
	}
	if updated.Username != newUsername || updated.Profile != newProfile || updated.PasswordHash != newPasswordHash {
		t.Fatalf("unexpected updated user: %+v", updated)
	}

	users, err := st.ListUsers(ctx)
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 active user, got %d", len(users))
	}

	deleteEvent, err := st.DeleteUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if deleteEvent.Kind != "user.deleted" {
		t.Fatalf("unexpected delete event kind: %s", deleteEvent.Kind)
	}

	if _, err := st.GetUser(ctx, user.ID); err != ErrNotFound {
		t.Fatalf("expected not found after delete, got %v", err)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Sequence >= events[1].Sequence || events[1].Sequence >= events[2].Sequence {
		t.Fatalf("events not ordered by sequence: %+v", events)
	}
}

func TestLocalMessageWriteAndQuery(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	first, firstEvent, err := st.CreateMessage(ctx, CreateMessageParams{
		UserID:   user.ID,
		Sender:   "orders",
		Body:     "first message",
		Metadata: `{"kind":"first"}`,
	})
	if err != nil {
		t.Fatalf("create first message: %v", err)
	}
	if firstEvent.Kind != "message.created" {
		t.Fatalf("unexpected event kind: %s", firstEvent.Kind)
	}

	second, _, err := st.CreateMessage(ctx, CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "second message",
	})
	if err != nil {
		t.Fatalf("create second message: %v", err)
	}

	messages, err := st.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].ID != second.ID || messages[1].ID != first.ID {
		t.Fatalf("messages not ordered newest-first: %+v", messages)
	}

	events, err := st.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events (user + 2 message), got %d", len(events))
	}
}

func TestDuplicateActiveUsernameRejected(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	ctx := context.Background()
	if _, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "carol",
		PasswordHash: "hash-1",
	}); err != nil {
		t.Fatalf("create user: %v", err)
	}

	if _, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "carol",
		PasswordHash: "hash-2",
	}); err != ErrConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestApplyReplicatedEventIsIdempotent(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, event, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-user",
		PasswordHash: "hash-1",
		Profile:      `{"display_name":"Replicated"}`,
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	replicated := ToReplicatedEvent(event)
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply replicated event second time: %v", err)
	}

	loaded, err := target.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("get replicated user: %v", err)
	}
	if loaded.Username != "replicated-user" {
		t.Fatalf("unexpected replicated user: %+v", loaded)
	}

	events, err := target.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list target events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 replicated event, got %d", len(events))
	}
}

func TestApplyReplicatedMessageIsIdempotent(t *testing.T) {
	t.Parallel()

	source := openNamedTestStore(t, "node-a", 1)
	defer source.Close()

	target := openNamedTestStore(t, "node-b", 2)
	defer target.Close()

	ctx := context.Background()
	user, userEvent, err := source.CreateUser(ctx, CreateUserParams{
		Username:     "replicated-message-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, ToReplicatedEvent(userEvent)); err != nil {
		t.Fatalf("apply user event: %v", err)
	}

	message, messageEvent, err := source.CreateMessage(ctx, CreateMessageParams{
		UserID:   user.ID,
		Sender:   "orders",
		Body:     "hello cluster",
		Metadata: `{"kind":"replicated"}`,
	})
	if err != nil {
		t.Fatalf("create source message: %v", err)
	}

	replicated := ToReplicatedEvent(messageEvent)
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply message event: %v", err)
	}
	if err := target.ApplyReplicatedEvent(ctx, replicated); err != nil {
		t.Fatalf("apply message event second time: %v", err)
	}

	messages, err := target.ListMessagesByUser(ctx, user.ID, 10)
	if err != nil {
		t.Fatalf("list replicated messages: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != message.ID {
		t.Fatalf("unexpected replicated messages: %+v", messages)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()

	return openNamedTestStore(t, "node-a", 1)
}

func openNamedTestStore(t *testing.T, nodeID string, nodeSlot uint16) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "distributed.db")
	st, err := Open(dbPath, Options{
		NodeID:   nodeID,
		NodeSlot: nodeSlot,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return st
}
