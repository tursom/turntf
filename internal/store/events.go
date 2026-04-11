package store

import (
	"context"
	"fmt"
)

func (s *Store) LastEventSequence(ctx context.Context) (int64, error) {
	var sequence int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM event_log`).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("query last event sequence: %w", err)
	}
	return sequence, nil
}
