package api

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/tursom/turntf/internal/store"
)

type EventSink interface {
	Publish(event store.Event)
}

type TransientPacketRouter interface {
	RouteTransientPacket(context.Context, store.TransientPacket) error
}

type TransientPacketReceiver interface {
	ReceiveTransientPacket(store.TransientPacket) bool
}

type noopEventSink struct{}

func (noopEventSink) Publish(store.Event) {}

type WriteGate interface {
	AllowWrite(context.Context) error
}

type Service struct {
	store           *store.Store
	eventSink       EventSink
	writeGate       WriteGate
	transientRouter TransientPacketRouter
	transientRecvMu sync.RWMutex
	transientRecv   TransientPacketReceiver
	nextTransientID atomic.Uint64
}

func New(st *store.Store, eventSink EventSink) *Service {
	if eventSink == nil {
		eventSink = noopEventSink{}
	}

	var writeGate WriteGate
	if gate, ok := eventSink.(WriteGate); ok {
		writeGate = gate
	}
	var transientRouter TransientPacketRouter
	if router, ok := eventSink.(TransientPacketRouter); ok {
		transientRouter = router
	}
	return &Service{
		store:           st,
		eventSink:       eventSink,
		writeGate:       writeGate,
		transientRouter: transientRouter,
	}
}

func (s *Service) CreateUser(ctx context.Context, params store.CreateUserParams) (store.User, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.User{}, store.Event{}, err
	}
	user, event, err := s.store.CreateUser(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return user, event, err
}

func (s *Service) UpdateUser(ctx context.Context, params store.UpdateUserParams) (store.User, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.User{}, store.Event{}, err
	}
	user, event, err := s.store.UpdateUser(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return user, event, err
}

func (s *Service) DeleteUser(ctx context.Context, key store.UserKey) (store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Event{}, err
	}
	event, err := s.store.DeleteUser(ctx, key)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return event, err
}

func (s *Service) GetUser(ctx context.Context, key store.UserKey) (store.User, error) {
	return s.store.GetUser(ctx, key)
}

func (s *Service) AuthenticateUser(ctx context.Context, key store.UserKey, password string) (store.User, error) {
	return s.store.AuthenticateUser(ctx, key, password)
}

func (s *Service) ListUsers(ctx context.Context) ([]store.User, error) {
	return s.store.ListUsers(ctx)
}

func (s *Service) CreateMessage(ctx context.Context, params store.CreateMessageParams) (store.Message, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Message{}, store.Event{}, err
	}
	message, event, err := s.store.CreateMessage(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return message, event, err
}

func (s *Service) SetTransientPacketReceiver(receiver TransientPacketReceiver) {
	s.transientRecvMu.Lock()
	defer s.transientRecvMu.Unlock()
	s.transientRecv = receiver
}

func (s *Service) DispatchTransientPacket(ctx context.Context, target, relayTarget store.UserKey, sender string, body []byte, mode store.DeliveryMode) (store.TransientPacket, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.TransientPacket{}, err
	}
	if err := target.Validate(); err != nil {
		return store.TransientPacket{}, err
	}
	if err := relayTarget.Validate(); err != nil {
		return store.TransientPacket{}, err
	}
	if target.UserID != store.NodeIngressUserID {
		return store.TransientPacket{}, store.ErrInvalidInput
	}
	if relayTarget.NodeID != target.NodeID {
		return store.TransientPacket{}, store.ErrInvalidInput
	}
	if sender == "" || len(body) == 0 {
		return store.TransientPacket{}, store.ErrInvalidInput
	}
	relayUser, err := s.store.GetUser(ctx, relayTarget)
	if err != nil {
		return store.TransientPacket{}, err
	}
	if !relayUser.CanLogin() {
		return store.TransientPacket{}, store.ErrForbidden
	}
	packet := store.TransientPacket{
		PacketID:     s.nextTransientID.Add(1),
		SourceNodeID: s.store.NodeID(),
		TargetNodeID: target.NodeID,
		RelayTarget:  relayTarget,
		Sender:       sender,
		Body:         append([]byte(nil), body...),
		DeliveryMode: mode,
		TTLHops:      8,
	}
	if packet.TargetNodeID == s.store.NodeID() {
		s.deliverTransientPacket(packet)
		return packet, nil
	}
	if s.transientRouter != nil {
		if err := s.transientRouter.RouteTransientPacket(ctx, packet); err != nil {
			return store.TransientPacket{}, err
		}
	}
	return packet, nil
}

func (s *Service) deliverTransientPacket(packet store.TransientPacket) bool {
	s.transientRecvMu.RLock()
	receiver := s.transientRecv
	s.transientRecvMu.RUnlock()
	if receiver == nil {
		return false
	}
	return receiver.ReceiveTransientPacket(packet)
}

func (s *Service) ListMessagesByUser(ctx context.Context, key store.UserKey, limit int) ([]store.Message, error) {
	return s.store.ListMessagesByUser(ctx, key, limit)
}

func (s *Service) SubscribeChannel(ctx context.Context, params store.ChannelSubscriptionParams) (store.Subscription, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Subscription{}, store.Event{}, err
	}
	subscription, event, err := s.store.SubscribeChannel(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return subscription, event, err
}

func (s *Service) UnsubscribeChannel(ctx context.Context, params store.ChannelSubscriptionParams) (store.Subscription, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Subscription{}, store.Event{}, err
	}
	subscription, event, err := s.store.UnsubscribeChannel(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return subscription, event, err
}

func (s *Service) ListChannelSubscriptions(ctx context.Context, key store.UserKey) ([]store.Subscription, error) {
	return s.store.ListChannelSubscriptions(ctx, key)
}

func (s *Service) IsSubscribedToChannel(ctx context.Context, subscriber, channel store.UserKey) (bool, error) {
	return s.store.IsSubscribedToChannel(ctx, subscriber, channel)
}

func (s *Service) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]store.Event, error) {
	return s.store.ListEvents(ctx, afterSequence, limit)
}

func (s *Service) allowWrite(ctx context.Context) error {
	if s.writeGate == nil {
		return nil
	}
	return s.writeGate.AllowWrite(ctx)
}
