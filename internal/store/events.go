package store

import (
	"context"
	"database/sql"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
)

func (s *Store) LastEventSequence(ctx context.Context) (int64, error) {
	return s.eventLog.LastEventSequence(ctx)
}

func (s *Store) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	return s.eventLog.ListOriginProgress(ctx)
}

func (s *Store) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	return s.eventLog.ListEventsByOrigin(ctx, originNodeID, afterEventID, limit)
}

func (s *Store) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	return s.eventLog.ListEvents(ctx, afterSequence, limit)
}

func (s *Store) insertEvent(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	if s.engine == EnginePebble {
		return s.eventLog.Append(ctx, event)
	}

	event.EventID = s.ids.Next()
	event.OriginNodeID = s.nodeID

	value, err := eventLogValue(event)
	if err != nil {
		return Event{}, err
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`, event.EventID, event.OriginNodeID, value)
	if err != nil {
		return Event{}, fmt.Errorf("insert event: %w", err)
	}

	event.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, fmt.Errorf("read event sequence: %w", err)
	}
	if err := s.upsertOriginCursorTx(ctx, tx, event.OriginNodeID, event.EventID, s.clock.Now().String()); err != nil {
		return Event{}, fmt.Errorf("record local origin cursor: %w", err)
	}
	return event, nil
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
