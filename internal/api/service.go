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

type Service struct {
	store     *store.Store
	eventSink EventSink
}

func New(st *store.Store, eventSink EventSink) *Service {
	if eventSink == nil {
		eventSink = noopEventSink{}
	}
	return &Service{
		store:     st,
		eventSink: eventSink,
	}
}

func (s *Service) CreateUser(ctx context.Context, params store.CreateUserParams) (store.User, store.Event, error) {
	user, event, err := s.store.CreateUser(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return user, event, err
}

func (s *Service) UpdateUser(ctx context.Context, params store.UpdateUserParams) (store.User, store.Event, error) {
	user, event, err := s.store.UpdateUser(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return user, event, err
}

func (s *Service) DeleteUser(ctx context.Context, userID int64) (store.Event, error) {
	event, err := s.store.DeleteUser(ctx, userID)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return event, err
}

func (s *Service) GetUser(ctx context.Context, userID int64) (store.User, error) {
	return s.store.GetUser(ctx, userID)
}

func (s *Service) ListUsers(ctx context.Context) ([]store.User, error) {
	return s.store.ListUsers(ctx)
}

func (s *Service) CreateMessage(ctx context.Context, params store.CreateMessageParams) (store.Message, store.Event, error) {
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
