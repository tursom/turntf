package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tursom/turntf/internal/clock"
)

// GetPeerAckCursor 获取某个 peer 对某个来源节点的确认游标，不存在时返回零值游标。
func (s *Store) GetPeerAckCursor(ctx context.Context, peerNodeID, originNodeID int64) (PeerAckCursor, error) {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.peerAckCursors != nil {
		return pebbleBackend.peerAckCursors.Get(ctx, peerNodeID, originNodeID)
	}
	if peerNodeID <= 0 {
		return PeerAckCursor{}, fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}
	if originNodeID <= 0 {
		return PeerAckCursor{}, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}

	row := s.db.QueryRowContext(ctx, `
SELECT peer_node_id, origin_node_id, acked_event_id, updated_at_hlc
FROM peer_ack_cursors
WHERE peer_node_id = ? AND origin_node_id = ?
`, peerNodeID, originNodeID)

	cursor, err := scanPeerAckCursor(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return PeerAckCursor{PeerNodeID: peerNodeID, OriginNodeID: originNodeID}, nil
		}
		return PeerAckCursor{}, fmt.Errorf("get peer ack cursor: %w", err)
	}
	return cursor, nil
}

// ListPeerAckCursors 列出所有 peer 确认游标。
func (s *Store) ListPeerAckCursors(ctx context.Context) ([]PeerAckCursor, error) {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.peerAckCursors != nil {
		return pebbleBackend.peerAckCursors.List(ctx)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT peer_node_id, origin_node_id, acked_event_id, updated_at_hlc
FROM peer_ack_cursors
ORDER BY peer_node_id ASC, origin_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list peer ack cursors: %w", err)
	}
	defer rows.Close()

	var cursors []PeerAckCursor
	for rows.Next() {
		cursor, err := scanPeerAckCursor(rows)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cursor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate peer ack cursors: %w", err)
	}
	return cursors, nil
}

// RecordPeerAck 记录某个 peer 已确认收到某来源节点的某个事件。
func (s *Store) RecordPeerAck(ctx context.Context, peerNodeID, originNodeID, ackedEventID int64) error {
	if ackedEventID < 0 {
		return fmt.Errorf("%w: acked event id cannot be negative", ErrInvalidInput)
	}
	return s.upsertPeerAckCursor(ctx, peerNodeID, originNodeID, ackedEventID)
}

// upsertPeerAckCursor 分发 peer ack cursor 的 upsert 到 Pebble 或 SQLite 实现。
func (s *Store) upsertPeerAckCursor(ctx context.Context, peerNodeID, originNodeID, ackedEventID int64) error {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.peerAckCursors != nil {
		return pebbleBackend.peerAckCursors.Upsert(ctx, peerNodeID, originNodeID, ackedEventID)
	}
	if peerNodeID <= 0 {
		return fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}
	if originNodeID <= 0 {
		return fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}

	updatedAt := s.clock.Now().String()
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO peer_ack_cursors(peer_node_id, origin_node_id, acked_event_id, updated_at_hlc)
VALUES(?, ?, ?, ?)
ON CONFLICT(peer_node_id, origin_node_id) DO UPDATE SET
    acked_event_id = CASE
        WHEN excluded.acked_event_id > peer_ack_cursors.acked_event_id THEN excluded.acked_event_id
        ELSE peer_ack_cursors.acked_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, peerNodeID, originNodeID, ackedEventID, updatedAt); err != nil {
		return fmt.Errorf("upsert peer ack cursor: %w", err)
	}
	return nil
}

// GetOriginCursor 获取本地节点对某来源节点的应用游标，不存在时返回零值游标。
func (s *Store) GetOriginCursor(ctx context.Context, originNodeID int64) (OriginCursor, error) {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.originCursors != nil {
		return pebbleBackend.originCursors.Get(ctx, originNodeID)
	}
	if originNodeID <= 0 {
		return OriginCursor{}, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}

	row := s.db.QueryRowContext(ctx, `
SELECT origin_node_id, applied_event_id, updated_at_hlc
FROM origin_cursors
WHERE origin_node_id = ?
`, originNodeID)

	cursor, err := scanOriginCursor(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return OriginCursor{OriginNodeID: originNodeID}, nil
		}
		return OriginCursor{}, fmt.Errorf("get origin cursor: %w", err)
	}
	return cursor, nil
}

// ListOriginCursors 列出所有来源节点的应用游标。
func (s *Store) ListOriginCursors(ctx context.Context) ([]OriginCursor, error) {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.originCursors != nil {
		return pebbleBackend.originCursors.List(ctx)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT origin_node_id, applied_event_id, updated_at_hlc
FROM origin_cursors
ORDER BY origin_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list origin cursors: %w", err)
	}
	defer rows.Close()

	var cursors []OriginCursor
	for rows.Next() {
		cursor, err := scanOriginCursor(rows)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cursor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate origin cursors: %w", err)
	}
	return cursors, nil
}

// RecordOriginApplied 记录本地已应用某来源节点的某个事件。
func (s *Store) RecordOriginApplied(ctx context.Context, originNodeID, appliedEventID int64) error {
	if appliedEventID < 0 {
		return fmt.Errorf("%w: applied event id cannot be negative", ErrInvalidInput)
	}
	return s.upsertOriginCursor(ctx, originNodeID, appliedEventID)
}

// upsertOriginCursor 分发 origin cursor 的 upsert 到 Pebble 或 SQLite 实现。
func (s *Store) upsertOriginCursor(ctx context.Context, originNodeID, appliedEventID int64) error {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.originCursors != nil {
		return pebbleBackend.originCursors.Upsert(ctx, originNodeID, appliedEventID)
	}
	if originNodeID <= 0 {
		return fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}

	updatedAt := s.clock.Now().String()
	if err := upsertOriginCursorTx(ctx, s.db, originNodeID, appliedEventID, updatedAt); err != nil {
		return fmt.Errorf("upsert origin cursor: %w", err)
	}
	return nil
}

func (s *Store) upsertOriginCursorTx(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, originNodeID, appliedEventID int64, updatedAt string) error {
	return upsertOriginCursorTx(ctx, exec, originNodeID, appliedEventID, updatedAt)
}

// upsertOriginCursorTx 在 SQLite 中 upsert origin cursor，仅当新 event_id 更大时更新。
func upsertOriginCursorTx(ctx context.Context, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, originNodeID, appliedEventID int64, updatedAt string) error {
	if _, err := exec.ExecContext(ctx, `
INSERT INTO origin_cursors(origin_node_id, applied_event_id, updated_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(origin_node_id) DO UPDATE SET
    applied_event_id = CASE
        WHEN excluded.applied_event_id > origin_cursors.applied_event_id THEN excluded.applied_event_id
        ELSE origin_cursors.applied_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`, originNodeID, appliedEventID, updatedAt); err != nil {
		return err
	}
	return nil
}

// scanPeerAckCursor 从 SQL scanner 扫描一行到 PeerAckCursor，解析 HLC 时间戳。
func scanPeerAckCursor(scanner interface {
	Scan(dest ...any) error
}) (PeerAckCursor, error) {
	var cursor PeerAckCursor
	var updatedAtRaw string

	if err := scanner.Scan(
		&cursor.PeerNodeID,
		&cursor.OriginNodeID,
		&cursor.AckedEventID,
		&updatedAtRaw,
	); err != nil {
		return PeerAckCursor{}, err
	}

	updatedAt, err := clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return PeerAckCursor{}, fmt.Errorf("parse peer ack cursor updated_at: %w", err)
	}
	cursor.UpdatedAt = updatedAt
	return cursor, nil
}

// scanOriginCursor 从 SQL scanner 扫描一行到 OriginCursor，解析 HLC 时间戳。
func scanOriginCursor(scanner interface {
	Scan(dest ...any) error
}) (OriginCursor, error) {
	var cursor OriginCursor
	var updatedAtRaw string

	if err := scanner.Scan(
		&cursor.OriginNodeID,
		&cursor.AppliedEventID,
		&updatedAtRaw,
	); err != nil {
		return OriginCursor{}, err
	}

	updatedAt, err := clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return OriginCursor{}, fmt.Errorf("parse origin cursor updated_at: %w", err)
	}
	cursor.UpdatedAt = updatedAt
	return cursor, nil
}
