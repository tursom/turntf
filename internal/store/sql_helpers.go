package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

func activeUsernameExists(ctx context.Context, tx *sql.Tx, username string, excludeUserID int64) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM users
WHERE username = ? AND deleted_at_hlc IS NULL AND user_id != ?
`, username, excludeUserID).Scan(&count); err != nil {
		return false, fmt.Errorf("check active username: %w", err)
	}
	return count > 0, nil
}

type tombstoneRecord struct {
	EntityType   string
	EntityNodeID int64
	EntityID     int64
	DeletedAt    clock.Timestamp
	OriginNodeID int64
}

func (s *Store) getTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, key UserKey) (tombstoneRecord, bool, error) {
	if err := key.Validate(); err != nil {
		return tombstoneRecord{}, false, err
	}
	row := tx.QueryRowContext(ctx, `
SELECT entity_type, entity_node_id, entity_id, deleted_at_hlc, origin_node_id
FROM tombstones
WHERE entity_type = ? AND entity_node_id = ? AND entity_id = ?
`, entityType, key.NodeID, key.UserID)

	var record tombstoneRecord
	var deletedAtRaw string
	if err := row.Scan(&record.EntityType, &record.EntityNodeID, &record.EntityID, &deletedAtRaw, &record.OriginNodeID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tombstoneRecord{}, false, nil
		}
		return tombstoneRecord{}, false, fmt.Errorf("get tombstone: %w", err)
	}

	deletedAt, err := clock.ParseTimestamp(deletedAtRaw)
	if err != nil {
		return tombstoneRecord{}, false, fmt.Errorf("parse tombstone deleted_at: %w", err)
	}
	record.DeletedAt = deletedAt
	return record, true, nil
}

func (s *Store) upsertTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, key UserKey, deletedAt clock.Timestamp, originNodeID int64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tombstones(entity_type, entity_node_id, entity_id, deleted_at_hlc, expires_at_hlc, origin_node_id)
VALUES(?, ?, ?, ?, NULL, ?)
ON CONFLICT(entity_type, entity_node_id, entity_id) DO UPDATE SET
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc > tombstones.deleted_at_hlc THEN excluded.deleted_at_hlc
        ELSE tombstones.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc > tombstones.deleted_at_hlc THEN excluded.origin_node_id
        ELSE tombstones.origin_node_id
    END
`, entityType, key.NodeID, key.UserID, deletedAt.String(), originNodeID); err != nil {
		return fmt.Errorf("upsert tombstone: %w", err)
	}
	return nil
}

func (s *Store) applyUserDeleteTx(ctx context.Context, tx *sql.Tx, key UserKey, deletedAt clock.Timestamp, originNodeID int64, requireActive bool) error {
	user, err := s.getUserByIDTx(ctx, tx, key, true)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		if requireActive {
			return ErrNotFound
		}
	default:
		return err
	}

	if err == nil {
		if user.isProtectedBootstrapAdmin() {
			if requireActive {
				return fmt.Errorf("%w: bootstrap admin cannot be deleted", ErrForbidden)
			}
			return nil
		}
		if user.isProtectedBroadcastUser() {
			if requireActive {
				return fmt.Errorf("%w: broadcast user cannot be deleted", ErrForbidden)
			}
			return nil
		}
		if requireActive && user.DeletedAt != nil {
			return ErrNotFound
		}

		shouldUpdateRow := user.DeletedAt == nil
		if !shouldUpdateRow && user.VersionDeleted != nil && deletedAt.Compare(*user.VersionDeleted) > 0 {
			shouldUpdateRow = true
		}

		if shouldUpdateRow {
			updatedAt := user.UpdatedAt
			if deletedAt.Compare(updatedAt) > 0 {
				updatedAt = deletedAt
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE users
SET deleted_at_hlc = ?, updated_at_hlc = ?, version_deleted = ?
WHERE node_id = ? AND user_id = ?
`, deletedAt.String(), updatedAt.String(), deletedAt.String(), key.NodeID, key.UserID); err != nil {
				return fmt.Errorf("delete user: %w", err)
			}
		}
	}

	if err := s.upsertTombstoneTx(ctx, tx, "user", key, deletedAt, originNodeID); err != nil {
		return err
	}
	if err := s.reconcileBootstrapAdminsTx(ctx, tx); err != nil {
		return err
	}
	return nil
}

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *Store) trimMessagesForUserTx(ctx context.Context, tx *sql.Tx, key UserKey) error {
	if err := key.Validate(); err != nil {
		return err
	}
	windowSize := normalizeMessageWindowSize(s.messageWindowSize)
	result, err := tx.ExecContext(ctx, `
DELETE FROM messages
WHERE user_node_id = ? AND user_id = ?
  AND (node_id, seq) IN (
    SELECT node_id, seq
    FROM messages
    WHERE user_node_id = ? AND user_id = ?
    ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
    LIMIT -1 OFFSET ?
  )
`, key.NodeID, key.UserID, key.NodeID, key.UserID, windowSize)
	if err != nil {
		return fmt.Errorf("trim messages for user %d:%d: %w", key.NodeID, key.UserID, err)
	}
	trimmed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count trimmed messages for user %d:%d: %w", key.NodeID, key.UserID, err)
	}
	if trimmed > 0 {
		if err := s.recordMessageTrimTx(ctx, tx, trimmed); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) recordMessageTrimTx(ctx context.Context, tx *sql.Tx, trimmed int64) error {
	if trimmed <= 0 {
		return nil
	}
	now := s.clock.Now().String()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO message_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES('global', ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = message_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, trimmed, now); err != nil {
		return fmt.Errorf("record message trim stats: %w", err)
	}
	return nil
}
