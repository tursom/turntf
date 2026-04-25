package store

import (
	"context"
	"database/sql"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
)

func (s *Store) LastEventSequence(ctx context.Context) (int64, error) {
	return s.backend.EventLog().LastEventSequence(ctx)
}

func (s *Store) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	return s.backend.EventLog().ListOriginProgress(ctx)
}

func (s *Store) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	return s.backend.EventLog().ListEventsByOrigin(ctx, originNodeID, afterEventID, limit)
}

func (s *Store) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	return s.backend.EventLog().ListEvents(ctx, afterSequence, limit)
}

func (s *Store) insertEvent(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	return s.backend.InsertLocalEventTx(ctx, tx, event)
}

func eventLogValue(event Event) ([]byte, error) {
	replicated := ToReplicatedEvent(event)
	if replicated == nil {
		return nil, fmt.Errorf("%w: event cannot be marshaled", ErrInvalidInput)
	}
	value, err := gproto.Marshal(replicated)
	if err != nil {
		return nil, fmt.Errorf("marshal event value: %w", err)
	}
	return value, nil
}
