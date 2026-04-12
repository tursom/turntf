package store

import (
	"context"
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
