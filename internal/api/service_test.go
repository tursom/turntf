package api

import (
	"context"
	"errors"
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

type routingSink struct {
	presence []store.OnlineNodePresence
	sessions []store.OnlineSession
	routed   []store.TransientPacket
}

func (s *routingSink) Publish(store.Event) {}

func (s *routingSink) RouteTransientPacket(_ context.Context, packet store.TransientPacket) error {
	s.routed = append(s.routed, packet)
	return nil
}

func (s *routingSink) QueryOnlineUserPresence(context.Context, store.UserKey) ([]store.OnlineNodePresence, error) {
	return s.presence, nil
}

func (s *routingSink) ResolveUserSessions(context.Context, store.UserKey) ([]store.OnlineSession, error) {
	return s.sessions, nil
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
		Sender:  user.Key(),
		Body:    []byte("package shipped"),
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}
	if message.Recipient.UserID != user.ID || message.Recipient.NodeID != user.NodeID || message.NodeID != testNodeID(1) || message.Seq != 1 {
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
		Sender:  user.Key(),
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

	packet, err := svc.DispatchTransientPacket(context.Background(), user.Key(), user.Key(), []byte("ephemeral"), store.DeliveryModeBestEffort)
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

func TestServiceDispatchTransientPacketUsesDedicatedPacketIDNamespace(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "service-transient-packet-id.db")
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

	svc := New(st, &recordingSink{})
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

	packet, err := svc.DispatchTransientPacket(context.Background(), user.Key(), user.Key(), []byte("ephemeral"), store.DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("dispatch transient packet: %v", err)
	}
	if packet.PacketID < transientPacketIDNamespace {
		t.Fatalf("expected transient packet id to use dedicated namespace, got %d", packet.PacketID)
	}
}

func TestServiceDispatchTransientPacketRespectsBlacklist(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "service-transient-blacklist.db")
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

	svc := New(st, &recordingSink{})
	receiver := &recordingTransientReceiver{}
	svc.SetTransientPacketReceiver(receiver)

	alice, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-2",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	if _, _, err := svc.BlockUser(context.Background(), store.BlacklistParams{
		Owner:   alice.Key(),
		Blocked: bob.Key(),
	}); err != nil {
		t.Fatalf("block bob: %v", err)
	}

	if _, err := svc.DispatchTransientPacket(context.Background(), alice.Key(), bob.Key(), []byte("ephemeral"), store.DeliveryModeBestEffort); !errors.Is(err, store.ErrBlockedByBlacklist) {
		t.Fatalf("expected transient dispatch to be blocked, got %v", err)
	}
	if len(receiver.packets) != 0 {
		t.Fatalf("expected blocked transient packet to avoid delivery, got %+v", receiver.packets)
	}
	if svc.BlacklistHitsTotal() != 1 {
		t.Fatalf("expected blacklist hit counter to increment, got %d", svc.BlacklistHitsTotal())
	}
}

func TestServiceQueriesUsersAndConversationMessages(t *testing.T) {
	t.Parallel()

	st, err := store.Open(filepath.Join(t.TempDir(), "service-queries.db"), store.Options{
		NodeID: testNodeID(1),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	svc := New(st, nil)
	alice, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "alice",
		PasswordHash: "hash-1",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "bob",
		PasswordHash: "hash-2",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	users, err := svc.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if len(users) != 2 || users[0].Key() != alice.Key() || users[1].Key() != bob.Key() {
		t.Fatalf("unexpected users: %+v", users)
	}

	created, _, err := svc.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: bob.Key(),
		Sender:  alice.Key(),
		Body:    []byte("hello bob"),
	})
	if err != nil {
		t.Fatalf("create conversation message: %v", err)
	}
	messages, err := svc.ListMessagesBySession(
		context.Background(),
		store.MessageSession(alice.Key(), bob.Key()),
		alice.Key(),
		10,
	)
	if err != nil {
		t.Fatalf("list conversation messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Recipient != bob.Key() || messages[0].Sender != alice.Key() || messages[0].Seq != created.Seq || string(messages[0].Body) != "hello bob" {
		t.Fatalf("unexpected conversation messages: %+v", messages)
	}
}

func TestServiceReportsSubscriptionAndBlacklistMetricState(t *testing.T) {
	t.Parallel()

	st, err := store.Open(filepath.Join(t.TempDir(), "service-state.db"), store.Options{
		NodeID: testNodeID(1),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	svc := New(st, nil)
	subscriber, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "subscriber",
		PasswordHash: "hash-1",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create subscriber: %v", err)
	}
	channel, _, err := svc.CreateUser(context.Background(), store.CreateUserParams{
		Username: "channel",
		Role:     store.RoleChannel,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	subscribed, err := svc.IsSubscribedToChannel(context.Background(), subscriber.Key(), channel.Key())
	if err != nil {
		t.Fatalf("check initial subscription: %v", err)
	}
	if subscribed {
		t.Fatal("new user should not be subscribed")
	}
	params := store.ChannelSubscriptionParams{Subscriber: subscriber.Key(), Channel: channel.Key()}
	if _, _, err := svc.SubscribeChannel(context.Background(), params); err != nil {
		t.Fatalf("subscribe channel: %v", err)
	}
	subscribed, err = svc.IsSubscribedToChannel(context.Background(), subscriber.Key(), channel.Key())
	if err != nil {
		t.Fatalf("check active subscription: %v", err)
	}
	if !subscribed {
		t.Fatal("active subscription should be reported")
	}
	if _, _, err := svc.UnsubscribeChannel(context.Background(), params); err != nil {
		t.Fatalf("unsubscribe channel: %v", err)
	}
	subscribed, err = svc.IsSubscribedToChannel(context.Background(), subscriber.Key(), channel.Key())
	if err != nil {
		t.Fatalf("check deleted subscription: %v", err)
	}
	if subscribed {
		t.Fatal("deleted subscription should not be reported")
	}

	svc.RecordBlacklistHit()
	svc.RecordBlacklistHit()
	if got := svc.BlacklistHitsTotal(); got != 2 {
		t.Fatalf("unexpected explicit blacklist hit count: got %d want 2", got)
	}
}

func TestServiceTransientRoutingDeduplicatesOnlineNodes(t *testing.T) {
	t.Parallel()

	st, err := store.Open(filepath.Join(t.TempDir(), "service-presence-routing.db"), store.Options{
		NodeID: testNodeID(1),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	recipient, _, err := st.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "recipient",
		PasswordHash: "hash-1",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	sender, _, err := st.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "sender",
		PasswordHash: "hash-2",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}

	remoteNodeID := testNodeID(2)
	sink := &routingSink{presence: []store.OnlineNodePresence{
		{User: recipient.Key(), ServingNodeID: st.NodeID(), SessionCount: 1},
		{User: recipient.Key(), ServingNodeID: remoteNodeID, SessionCount: 1},
		{User: recipient.Key(), ServingNodeID: remoteNodeID, SessionCount: 2},
		{User: recipient.Key(), ServingNodeID: 0, SessionCount: 1},
	}}
	svc := New(st, sink)
	receiver := &recordingTransientReceiver{}
	svc.SetTransientPacketReceiver(receiver)

	packet, err := svc.DispatchTransientPacket(
		context.Background(),
		recipient.Key(),
		sender.Key(),
		[]byte("online fanout"),
		store.DeliveryModeBestEffort,
	)
	if err != nil {
		t.Fatalf("dispatch transient packet: %v", err)
	}
	if packet.TargetNodeID != st.NodeID() {
		t.Fatalf("unexpected representative packet target: %+v", packet)
	}
	if len(receiver.packets) != 1 || receiver.packets[0].TargetNodeID != st.NodeID() {
		t.Fatalf("expected one local delivery, got %+v", receiver.packets)
	}
	if len(sink.routed) != 1 || sink.routed[0].TargetNodeID != remoteNodeID {
		t.Fatalf("expected one deduplicated remote route, got %+v", sink.routed)
	}
	if receiver.packets[0].PacketID == sink.routed[0].PacketID {
		t.Fatalf("fanout packets must have distinct ids: local=%d remote=%d", receiver.packets[0].PacketID, sink.routed[0].PacketID)
	}
}

func TestServiceTransientRoutingFallsBackToSessionsAndValidatesTarget(t *testing.T) {
	t.Parallel()

	st, err := store.Open(filepath.Join(t.TempDir(), "service-session-routing.db"), store.Options{
		NodeID: testNodeID(1),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	recipient, _, err := st.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "recipient",
		PasswordHash: "hash-1",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	sender, _, err := st.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "sender",
		PasswordHash: "hash-2",
		Role:         store.RoleUser,
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}

	remoteNodeID := testNodeID(2)
	target := store.SessionRef{ServingNodeID: remoteNodeID, SessionID: "session-a"}
	sink := &routingSink{sessions: []store.OnlineSession{
		{User: recipient.Key(), SessionRef: target, TransientCapable: true},
		{User: recipient.Key(), SessionRef: store.SessionRef{ServingNodeID: remoteNodeID, SessionID: "session-b"}, TransientCapable: true},
		{User: recipient.Key(), SessionRef: store.SessionRef{SessionID: "invalid"}, TransientCapable: true},
	}}
	svc := New(st, sink)

	packet, err := svc.DispatchTransientPacket(
		context.Background(),
		recipient.Key(),
		sender.Key(),
		[]byte("session fallback"),
		store.DeliveryModeRouteRetry,
	)
	if err != nil {
		t.Fatalf("dispatch via session fallback: %v", err)
	}
	if packet.TargetNodeID != remoteNodeID || len(sink.routed) != 1 || sink.routed[0].TargetNodeID != remoteNodeID {
		t.Fatalf("expected one deduplicated session route, packet=%+v routed=%+v", packet, sink.routed)
	}

	targeted, err := svc.DispatchTransientPacketTo(
		context.Background(),
		recipient.Key(),
		sender.Key(),
		[]byte("targeted"),
		store.DeliveryModeBestEffort,
		target,
	)
	if err != nil {
		t.Fatalf("dispatch to existing session: %v", err)
	}
	if targeted.TargetSession != target || targeted.TargetNodeID != remoteNodeID || len(sink.routed) != 2 {
		t.Fatalf("unexpected targeted route: packet=%+v routed=%+v", targeted, sink.routed)
	}

	missing := store.SessionRef{ServingNodeID: remoteNodeID, SessionID: "missing"}
	if _, err := svc.DispatchTransientPacketTo(
		context.Background(),
		recipient.Key(),
		sender.Key(),
		[]byte("missing target"),
		store.DeliveryModeBestEffort,
		missing,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing target should return not found, got %v", err)
	}
	if len(sink.routed) != 2 {
		t.Fatalf("missing target must not route a packet: %+v", sink.routed)
	}
}
