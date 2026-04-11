package api

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"notifier/internal/store"
)

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
		NodeID:   "node-a",
		NodeSlot: 1,
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

	if _, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-2",
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected create user conflict, got %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected failed create user to avoid publishing, got %d events", len(sink.events))
	}

	message, messageEvent, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserID: user.ID,
		Sender: "orders",
		Body:   "package shipped",
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}
	if message.UserID != user.ID {
		t.Fatalf("unexpected message: %+v", message)
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected successful message publish, got %d events", len(sink.events))
	}
	if sink.events[1].EventID != messageEvent.EventID || sink.events[1].Kind != "message.created" {
		t.Fatalf("unexpected published message event: %+v", sink.events[1])
	}

	if _, _, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserID: 9999,
		Sender: "orders",
		Body:   "missing user",
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected create message not found, got %v", err)
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected failed message create to avoid publishing, got %d events", len(sink.events))
	}
}
