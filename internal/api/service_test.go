package api

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/tursom/turntf/internal/store"
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

type recordingTransientReceiver struct {
	packets []store.TransientPacket
}

func (r *recordingTransientReceiver) ReceiveTransientPacket(packet store.TransientPacket) bool {
	r.packets = append(r.packets, packet)
	return true
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
	if sink.events[0].EventID != userEvent.EventID || sink.events[0].EventType != store.EventTypeUserCreated {
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
	if sink.events[1].EventID != duplicateEvent.EventID || sink.events[1].EventType != store.EventTypeUserCreated {
		t.Fatalf("unexpected published duplicate user event: %+v", sink.events[1])
	}

	message, messageEvent, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: user.Key(),
		Sender:  "orders",
		Body:    []byte("package shipped"),
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
	if sink.events[2].EventID != messageEvent.EventID || sink.events[2].EventType != store.EventTypeMessageCreated {
		t.Fatalf("unexpected published message event: %+v", sink.events[2])
	}

	if _, _, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: store.UserKey{NodeID: user.NodeID, UserID: 9999},
		Sender:  "orders",
		Body:    []byte("missing user"),
	}); err != store.ErrNotFound {
		t.Fatalf("expected create message not found, got %v", err)
	}
	if len(sink.events) != 3 {
		t.Fatalf("expected failed message create to avoid publishing, got %d events", len(sink.events))
	}
}

func TestServiceDispatchTransientPacketDoesNotPublishEvents(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "service-transient.db")
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
	receiver := &recordingTransientReceiver{}
	svc.SetTransientPacketReceiver(receiver)

	user, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected create user event, got %d", len(sink.events))
	}

	packet, err := svc.DispatchTransientPacket(context.Background(), user.Key(), "relay", []byte("ephemeral"), store.DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("dispatch transient packet: %v", err)
	}
	if packet.PacketID == 0 || packet.Recipient != user.Key() {
		t.Fatalf("unexpected transient packet: %+v", packet)
	}
	if len(receiver.packets) != 1 || receiver.packets[0].PacketID != packet.PacketID {
		t.Fatalf("expected local transient delivery, got %+v", receiver.packets)
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected transient dispatch to avoid publishing events, got %d events", len(sink.events))
	}

	messages, err := svc.ListMessagesByUser(context.Background(), user.Key(), 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected transient dispatch to avoid persistence, got %+v", messages)
	}
}
