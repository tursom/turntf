package store

import (
	"context"
	"database/sql"
	"fmt"

	"notifier/internal/clock"
)

func (s *Store) GetPeerCursor(ctx context.Context, peerNodeID int64) (PeerCursor, error) {
	if peerNodeID <= 0 {
		return PeerCursor{}, fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}

	row := s.db.QueryRowContext(ctx, `
SELECT peer_node_id, acked_sequence, applied_sequence, updated_at_hlc
FROM peer_cursors
WHERE peer_node_id = ?
`, peerNodeID)

	cursor, err := scanPeerCursor(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return PeerCursor{PeerNodeID: peerNodeID}, nil
		}
		return PeerCursor{}, fmt.Errorf("get peer cursor: %w", err)
	}
	return cursor, nil
}

func (s *Store) RecordPeerAck(ctx context.Context, peerNodeID int64, ackedSequence int64) error {
	if ackedSequence < 0 {
		return fmt.Errorf("%w: acked sequence cannot be negative", ErrInvalidInput)
	}
	return s.upsertPeerCursor(ctx, peerNodeID, ackedSequence, 0)
}

func (s *Store) RecordPeerApplied(ctx context.Context, peerNodeID int64, appliedSequence int64) error {
	if appliedSequence < 0 {
		return fmt.Errorf("%w: applied sequence cannot be negative", ErrInvalidInput)
	}
	return s.upsertPeerCursor(ctx, peerNodeID, 0, appliedSequence)
}

func (s *Store) upsertPeerCursor(ctx context.Context, peerNodeID int64, ackedSequence, appliedSequence int64) error {
	if peerNodeID <= 0 {
		return fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}

	updatedAt := s.clock.Now().String()
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO peer_cursors(peer_node_id, acked_sequence, applied_sequence, updated_at_hlc)
VALUES(?, ?, ?, ?)
ON CONFLICT(peer_node_id) DO UPDATE SET
    acked_sequence = CASE
        WHEN excluded.acked_sequence > peer_cursors.acked_sequence THEN excluded.acked_sequence
        ELSE peer_cursors.acked_sequence
    END,
    applied_sequence = CASE
        WHEN excluded.applied_sequence > peer_cursors.applied_sequence THEN excluded.applied_sequence
        ELSE peer_cursors.applied_sequence
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, peerNodeID, ackedSequence, appliedSequence, updatedAt); err != nil {
		return fmt.Errorf("upsert peer cursor: %w", err)
	}
	return nil
}

func scanPeerCursor(scanner interface {
	Scan(dest ...any) error
}) (PeerCursor, error) {
	var cursor PeerCursor
	var updatedAtRaw string

	if err := scanner.Scan(
		&cursor.PeerNodeID,
		&cursor.AckedSequence,
		&cursor.AppliedSequence,
		&updatedAtRaw,
	); err != nil {
		return PeerCursor{}, err
	}

	updatedAt, err := clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return PeerCursor{}, fmt.Errorf("parse peer cursor updated_at: %w", err)
	}
	cursor.UpdatedAt = updatedAt
	return cursor, nil
}
