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

func (s *Store) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT origin_node_id, COALESCE(MAX(event_id), 0)
FROM event_log
GROUP BY origin_node_id
ORDER BY origin_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list origin progress: %w", err)
	}
	defer rows.Close()

	var progress []OriginProgress
	for rows.Next() {
		var item OriginProgress
		if err := rows.Scan(&item.OriginNodeID, &item.LastEventID); err != nil {
			return nil, fmt.Errorf("scan origin progress: %w", err)
		}
		progress = append(progress, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate origin progress: %w", err)
	}
	return progress, nil
}

func (s *Store) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	if originNodeID <= 0 {
		return nil, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT sequence, event_id, origin_node_id, value
FROM event_log
WHERE origin_node_id = ? AND event_id > ?
ORDER BY event_id ASC
LIMIT ?
`, originNodeID, afterEventID, limit)
	if err != nil {
		return nil, fmt.Errorf("list events by origin: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate origin events: %w", err)
	}
	return events, nil
}
