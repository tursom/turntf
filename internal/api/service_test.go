package api

import (
	"context"
	"path/filepath"
	"testing"

	"notifier/internal/store"
)

func testNodeID(slot uint16) int64 {
	return int64(slot) << 12
}

type recordingSink struct {
	events []store.Event
}

func (s *recordingSink) Publish(event store.Event) {
	s.events = append(s.events, event)
}

func TestServicePublishesOnlySuccessfulWrites(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dbPath, store.Options{
		NodeID: testNodeID(1),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	sink := &recordingSink{}
	svc := New(st, sink)

	user, userEvent, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected one published event, got %d", len(sink.events))
	}
	if sink.events[0].EventID != userEvent.EventID || sink.events[0].Kind != "user.created" {
		t.Fatalf("unexpected published user event: %+v", sink.events[0])
	}

	duplicateUser, duplicateEvent, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-2",
	})
	if err != nil {
		t.Fatalf("expected duplicate username create to succeed, got %v", err)
	}
	if duplicateUser.ID == 0 || duplicateUser.ID == user.ID {
		t.Fatalf("expected distinct duplicate user, got %+v", duplicateUser)
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected duplicate create to publish, got %d events", len(sink.events))
	}
	if sink.events[1].EventID != duplicateEvent.EventID || sink.events[1].Kind != "user.created" {
		t.Fatalf("unexpected published duplicate user event: %+v", sink.events[1])
	}

	message, messageEvent, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "package shipped",
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}
	if message.UserID != user.ID || message.NodeID != testNodeID(1) || message.Seq != 1 {
		t.Fatalf("unexpected message: %+v", message)
	}
	if len(sink.events) != 3 {
		t.Fatalf("expected successful message publish, got %d events", len(sink.events))
	}
	if sink.events[2].EventID != messageEvent.EventID || sink.events[2].Kind != "message.created" {
		t.Fatalf("unexpected published message event: %+v", sink.events[2])
	}

	if _, _, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserID: 9999,
		Sender: "orders",
		Body:   "missing user",
	}); err != store.ErrNotFound {
		t.Fatalf("expected create message not found, got %v", err)
	}
	if len(sink.events) != 3 {
		t.Fatalf("expected failed message create to avoid publishing, got %d events", len(sink.events))
	}
}
