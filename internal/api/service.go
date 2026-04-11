package api

import (
	"context"

	"notifier/internal/store"
)

type EventSink interface {
	Publish(event store.Event)
}

type noopEventSink struct{}

func (noopEventSink) Publish(store.Event) {}

type WriteGate interface {
	AllowWrite(context.Context) error
}

type Service struct {
	store     *store.Store
	eventSink EventSink
	writeGate WriteGate
}

func New(st *store.Store, eventSink EventSink) *Service {
	if eventSink == nil {
		eventSink = noopEventSink{}
	}

	var writeGate WriteGate
	if gate, ok := eventSink.(WriteGate); ok {
		writeGate = gate
	}
	return &Service{
		store:     st,
		eventSink: eventSink,
		writeGate: writeGate,
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

func (s *Service) DeleteUser(ctx context.Context, userID int64) (store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Event{}, err
	}
	event, err := s.store.DeleteUser(ctx, userID)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return event, err
}

func (s *Service) GetUser(ctx context.Context, userID int64) (store.User, error) {
	return s.store.GetUser(ctx, userID)
}

func (s *Service) AuthenticateUser(ctx context.Context, userID int64, password string) (store.User, error) {
	return s.store.AuthenticateUser(ctx, userID, password)
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

func (s *Service) ListMessagesByUser(ctx context.Context, userID int64, limit int) ([]store.Message, error) {
	return s.store.ListMessagesByUser(ctx, userID, limit)
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
