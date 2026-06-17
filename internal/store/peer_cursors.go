package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tursom/turntf/internal/clock"
)

// GetPeerAckCursor 获取某个 peer 对某个来源节点的确认（acknowledged）游标。
//
// 该游标记录了 peerNodeID 节点已确认收到 originNodeID 来源的哪个事件（ackedEventID），
// 用于复制协议中对端进度追踪。当游标不存在时返回零值游标（ackedEventID=0，
// 表示该 peer 尚未确认任何事件）。
//
// 双后端路由：
//  1. 优先从 Pebble 后端的内存型 peer ack cursor 仓储查询
//  2. 回退到 SQLite 的 peer_ack_cursors 表查询
//
// 返回 ErrInvalidInput 当 peerNodeID 或 originNodeID <= 0（Pebble 路径跳过验证）。
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

// ListPeerAckCursors 列出所有 peer 在各个来源上的确认游标。
// 返回按 peer_node_id ASC、origin_node_id ASC 排序的游标列表。
// 双后端路由：优先从 Pebble 后端读取，否则回退 SQLite 全表扫描。
// 此方法在运维统计（OperationsStats）中用于聚合 peer 同步进度。
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
// 当有对端节点发来 ack 消息时调用。ackedEventID 表示该 peer 已收到并确认的
// 最新事件的 ID。该值必须 >= 0，否则返回 ErrInvalidInput。
// 底层调用 upsertPeerAckCursor 持久化更新，确认游标单调递增（仅允许增大）。
func (s *Store) RecordPeerAck(ctx context.Context, peerNodeID, originNodeID, ackedEventID int64) error {
	if ackedEventID < 0 {
		return fmt.Errorf("%w: acked event id cannot be negative", ErrInvalidInput)
	}
	return s.upsertPeerAckCursor(ctx, peerNodeID, originNodeID, ackedEventID)
}

// upsertPeerAckCursor 分发 peer ack cursor 的插入或更新操作。
// 双后端路由：
//  1. 优先使用 Pebble 后端的 peerAckCursors 仓储（内存型，持久化到 Pebble）
//  2. 否则回退到 SQLite 的 peer_ack_cursors 表
//
// 确认游标使用 ON CONFLICT + CASE 比较确保 acked_event_id 只增不减——这是
// 复制协议的核心约束，防止因网络乱序导致的 ack 回退。
// 在 SQLite 路径中校验 peerNodeID 和 originNodeID 必须 > 0。
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

// GetOriginCursor 获取本地节点在某来源节点上的事件应用（applied）游标。
//
// 应用游标记录本节点已从 originNodeID 来源应用（即重放处理）到哪个事件，
// 用于跨节点复制时的进度追踪和断点续传。当游标不存在时返回零值
// OriginCursor（AppliedEventID=0，表示尚未应用任何事件）。
//
// 双后端路由：
//  1. 优先从 Pebble 后端的内存型 origin cursor 仓储查询
//  2. 回退到 SQLite 的 origin_cursors 表查询
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

// ListOriginCursors 列出所有来源节点的事件应用游标。
// 返回按 origin_node_id ASC 排序的游标列表。
// 双后端路由：优先从 Pebble 后端读取，否则回退 SQLite 全表扫描。
// 此方法在运维统计和复制恢复流程中用于了解所有来源的处理进度。
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

// RecordOriginApplied 记录本地节点已成功应用（重放处理）某来源节点的某个事件。
// 当事件处理完成（如创建用户、投递消息等副作用已执行完毕）后调用。
// appliedEventID 必须是 >= 0 的值，否则返回 ErrInvalidInput。
// 底层调用 upsertOriginCursor 持久化更新，应用游标单调递增（仅允许增大）。
func (s *Store) RecordOriginApplied(ctx context.Context, originNodeID, appliedEventID int64) error {
	if appliedEventID < 0 {
		return fmt.Errorf("%w: applied event id cannot be negative", ErrInvalidInput)
	}
	return s.upsertOriginCursor(ctx, originNodeID, appliedEventID)
}

// upsertOriginCursor 分发 origin cursor 的插入或更新操作。
// 双后端路由：
//  1. 优先使用 Pebble 后端的 originCursors 仓储
//  2. 否则回退到 SQLite 的 origin_cursors 表
//
// 应用游标使用 ON CONFLICT + CASE 比较确保 applied_event_id 只增不减——
// 这确保在跨节点复制中事件的应用进度不会因乱序而回退。
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

// upsertOriginCursorTx 是 Store 方法层面的 upsertOriginCursorTx 包装。
// 实际将调用转发给包级函数 upsertOriginCursorTx，保持两个变体（方法 vs 包级函数）
// 的 SQL 逻辑一致。此方法供 RecordOriginApplied → upsertOriginCursor 调用链使用，
// 在需要事务 exec 接口的上下文中使用。

// upsertOriginCursorTx 在 SQLite 中 upsert origin cursor，仅当新 applied_event_id
// 更大时才更新，否则保持原值。
// 这是包级函数，被 Store.upsertOriginCursorTx 和 upsertOriginCursor 调用。
// 使用 ON CONFLICT + CASE 比较确保 applied_event_id 单调递增，防止因网络乱序
// 导致事件应用进度回退。updated_at_hlc 总是更新为最新值。
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

// scanPeerAckCursor 从 SQL scanner 扫描 peer_ack_cursors 表的一行到 PeerAckCursor，
// 解析 updated_at_hlc 字符串为 clock.Timestamp 结构体。
// 扫描字段：peer_node_id、origin_node_id、acked_event_id、updated_at_hlc。
// 非并发安全——调用方保证 scanner 的并发安全性。
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

// scanOriginCursor 从 SQL scanner 扫描 origin_cursors 表的一行到 OriginCursor，
// 解析 updated_at_hlc 字符串为 clock.Timestamp 结构体。
// 扫描字段：origin_node_id、applied_event_id、updated_at_hlc。
// 非并发安全——调用方保证 scanner 的并发安全性。
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
