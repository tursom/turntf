package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/tursom/turntf/internal/clock"
)

const eventLogTrimStatsScope = "global"

type EventLogTrimStats struct {
	TrimmedTotal  int64
	LastTrimmedAt *clock.Timestamp
}

type EventLogPruneResult struct {
	TrimmedEvents      int64
	OriginsAffected    int
	MaxEventsPerOrigin int
}

func (s *Store) PruneEventLogOnce(ctx context.Context) (EventLogPruneResult, error) {
	maxEvents := normalizeEventLogMaxEventsPerOrigin(s.eventLogMaxEventsPerOrigin)
	progress, err := s.eventLog.ListOriginProgress(ctx)
	if err != nil {
		return EventLogPruneResult{}, err
	}

	result := EventLogPruneResult{MaxEventsPerOrigin: maxEvents}
	for _, item := range progress {
		if item.OriginNodeID <= 0 {
			continue
		}
		trimmed, err := s.pruneEventLogOrigin(ctx, item.OriginNodeID, maxEvents)
		if err != nil {
			return EventLogPruneResult{}, err
		}
		if trimmed == 0 {
			continue
		}
		result.TrimmedEvents += trimmed
		result.OriginsAffected++
	}
	return result, nil
}

func (s *Store) EventLogTruncatedBefore(ctx context.Context, originNodeID int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}

	var truncatedBefore int64
	err := s.db.QueryRowContext(ctx, `
SELECT truncated_before_event_id
FROM event_log_truncation_meta
WHERE origin_node_id = ?
`, originNodeID).Scan(&truncatedBefore)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query event log truncation meta: %w", err)
	}
	return truncatedBefore, nil
}

func (s *Store) eventLogTrimStats(ctx context.Context) (EventLogTrimStats, error) {
	var stats EventLogTrimStats
	var lastTrimmedAtRaw sql.NullString

	err := s.db.QueryRowContext(ctx, `
SELECT trimmed_total, last_trimmed_at_hlc
FROM event_log_trim_stats
WHERE scope = ?
`, eventLogTrimStatsScope).Scan(&stats.TrimmedTotal, &lastTrimmedAtRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EventLogTrimStats{}, nil
		}
		return EventLogTrimStats{}, fmt.Errorf("query event log trim stats: %w", err)
	}
	if lastTrimmedAtRaw.Valid && lastTrimmedAtRaw.String != "" {
		parsed, err := clock.ParseTimestamp(lastTrimmedAtRaw.String)
		if err != nil {
			return EventLogTrimStats{}, fmt.Errorf("parse event log trim timestamp: %w", err)
		}
		stats.LastTrimmedAt = &parsed
	}
	return stats, nil
}

func (s *Store) pruneEventLogOrigin(ctx context.Context, originNodeID int64, maxEvents int) (int64, error) {
	switch s.engine {
	case EngineSQLite:
		return s.pruneEventLogOriginSQLite(ctx, originNodeID, maxEvents)
	case EnginePebble:
		return s.pruneEventLogOriginPebble(ctx, originNodeID, maxEvents)
	default:
		return 0, fmt.Errorf("%w: unsupported store engine %q", ErrInvalidInput, s.engine)
	}
}

func (s *Store) pruneEventLogOriginSQLite(ctx context.Context, originNodeID int64, maxEvents int) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin sqlite event log prune: %w", err)
	}
	defer tx.Rollback()

	var count int64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM event_log
WHERE origin_node_id = ?
`, originNodeID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count sqlite event log rows: %w", err)
	}
	if count <= int64(maxEvents) {
		return 0, nil
	}

	trimCount := count - int64(maxEvents)
	var truncatedBefore int64
	if err := tx.QueryRowContext(ctx, `
SELECT event_id
FROM event_log
WHERE origin_node_id = ?
ORDER BY event_id ASC
LIMIT 1 OFFSET ?
`, originNodeID, trimCount-1).Scan(&truncatedBefore); err != nil {
		return 0, fmt.Errorf("query sqlite event log truncation boundary: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
DELETE FROM event_log
WHERE origin_node_id = ? AND event_id <= ?
`, originNodeID, truncatedBefore)
	if err != nil {
		return 0, fmt.Errorf("delete sqlite event log rows: %w", err)
	}
	trimmed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted sqlite event log rows: %w", err)
	}
	if trimmed > 0 {
		now := s.clock.Now().String()
		if err := upsertEventLogTruncationTx(ctx, tx, originNodeID, truncatedBefore, now); err != nil {
			return 0, err
		}
		if err := recordEventLogTrimTx(ctx, tx, trimmed, now); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit sqlite event log prune: %w", err)
	}
	return trimmed, nil
}

func (s *Store) pruneEventLogOriginPebble(ctx context.Context, originNodeID int64, maxEvents int) (int64, error) {
	if s.pebbleDB == nil {
		return 0, fmt.Errorf("pebble event log prune requires pebble db")
	}
	if s.pebbleWrites != nil {
		if err := s.pebbleWrites.Flush(); err != nil {
			return 0, err
		}
	}

	total, err := countPebbleOriginEvents(ctx, s.pebbleDB, originNodeID)
	if err != nil {
		return 0, err
	}
	if total <= int64(maxEvents) {
		return 0, nil
	}

	trimCount := total - int64(maxEvents)
	truncatedBefore, err := nthPebbleOriginEventID(ctx, s.pebbleDB, originNodeID, trimCount-1)
	if err != nil {
		return 0, err
	}
	if err := s.upsertEventLogTruncation(ctx, originNodeID, truncatedBefore); err != nil {
		return 0, err
	}

	trimmed, err := deletePebbleOriginEvents(ctx, s.pebbleDB, originNodeID, trimCount)
	if err != nil {
		return 0, err
	}
	if trimmed > 0 {
		if err := s.recordEventLogTrim(ctx, trimmed); err != nil {
			return 0, err
		}
	}
	return trimmed, nil
}

func countPebbleOriginEvents(ctx context.Context, db *pebble.DB, originNodeID int64) (int64, error) {
	prefix := []byte(fmt.Sprintf("event/origin/%020d/", originNodeID))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open pebble event count iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate pebble event count: %w", err)
	}
	return count, nil
}

func nthPebbleOriginEventID(ctx context.Context, db *pebble.DB, originNodeID, offset int64) (int64, error) {
	prefix := []byte(fmt.Sprintf("event/origin/%020d/", originNodeID))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open pebble truncation boundary iterator: %w", err)
	}
	defer iter.Close()

	var index int64
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if index == offset {
			_, eventID, err := parsePebbleOriginKey(iter.Key())
			if err != nil {
				return 0, err
			}
			return eventID, nil
		}
		index++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate pebble truncation boundary: %w", err)
	}
	return 0, fmt.Errorf("pebble event offset %d for origin %d not found", offset, originNodeID)
}

func deletePebbleOriginEvents(ctx context.Context, db *pebble.DB, originNodeID, limit int64) (int64, error) {
	prefix := []byte(fmt.Sprintf("event/origin/%020d/", originNodeID))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open pebble delete iterator: %w", err)
	}
	defer iter.Close()

	const batchSize = 1024
	var trimmed int64
	batch := db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	pending := 0
	commitBatch := func() error {
		if pending == 0 {
			return nil
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("commit pebble event log prune batch: %w", err)
		}
		if err := batch.Close(); err != nil {
			return fmt.Errorf("close pebble event log prune batch: %w", err)
		}
		batch = db.NewBatch()
		pending = 0
		return nil
	}

	for valid := iter.First(); valid && trimmed < limit; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		_, eventID, err := parsePebbleOriginKey(iter.Key())
		if err != nil {
			return 0, err
		}
		sequence := decodeInt64(iter.Value())
		if err := batch.Delete(pebbleEventOriginKey(originNodeID, eventID), nil); err != nil {
			return 0, fmt.Errorf("delete pebble origin event key: %w", err)
		}
		if err := batch.Delete(pebbleEventSeqKey(sequence), nil); err != nil {
			return 0, fmt.Errorf("delete pebble event sequence key: %w", err)
		}
		trimmed++
		pending++
		if pending >= batchSize {
			if err := commitBatch(); err != nil {
				return 0, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate pebble delete iterator: %w", err)
	}
	if err := commitBatch(); err != nil {
		return 0, err
	}
	return trimmed, nil
}

func (s *Store) upsertEventLogTruncation(ctx context.Context, originNodeID, truncatedBefore int64) error {
	now := s.clock.Now().String()
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO event_log_truncation_meta(origin_node_id, truncated_before_event_id, updated_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(origin_node_id) DO UPDATE SET
    truncated_before_event_id = CASE
        WHEN excluded.truncated_before_event_id > event_log_truncation_meta.truncated_before_event_id THEN excluded.truncated_before_event_id
        ELSE event_log_truncation_meta.truncated_before_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, originNodeID, truncatedBefore, now); err != nil {
		return fmt.Errorf("upsert event log truncation meta: %w", err)
	}
	return nil
}

func upsertEventLogTruncationTx(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, originNodeID, truncatedBefore int64, updatedAt string) error {
	if _, err := exec.ExecContext(ctx, `
INSERT INTO event_log_truncation_meta(origin_node_id, truncated_before_event_id, updated_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(origin_node_id) DO UPDATE SET
    truncated_before_event_id = CASE
        WHEN excluded.truncated_before_event_id > event_log_truncation_meta.truncated_before_event_id THEN excluded.truncated_before_event_id
        ELSE event_log_truncation_meta.truncated_before_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, originNodeID, truncatedBefore, updatedAt); err != nil {
		return fmt.Errorf("upsert event log truncation meta: %w", err)
	}
	return nil
}

func (s *Store) recordEventLogTrim(ctx context.Context, trimmed int64) error {
	if trimmed <= 0 {
		return nil
	}
	now := s.clock.Now().String()
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO event_log_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = event_log_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, eventLogTrimStatsScope, trimmed, now); err != nil {
		return fmt.Errorf("record event log trim stats: %w", err)
	}
	return nil
}

func recordEventLogTrimTx(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, trimmed int64, updatedAt string) error {
	if trimmed <= 0 {
		return nil
	}
	if _, err := exec.ExecContext(ctx, `
INSERT INTO event_log_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = event_log_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, eventLogTrimStatsScope, trimmed, updatedAt); err != nil {
		return fmt.Errorf("record event log trim stats: %w", err)
	}
	return nil
}
