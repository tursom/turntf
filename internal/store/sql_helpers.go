package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

// activeUsernameExists 在事务中检查指定用户名是否已被其他活跃（未软删除）用户占用。
// excludeUserID 参数排除自身，用于更新用户时检查新用户名是否与其他人冲突。
// 查询条件：username 匹配且 deleted_at_hlc IS NULL 且不是 excludeUserID 的用户。
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

// tombstoneRecord 代表一个已软删除实体的墓碑记录，存储在 tombstones 表中。
// 墓碑机制用于跨节点复制：当一个实体在一个节点上被删除时，其他节点通过
// 复制协议收到 tombstone 后可以同步该删除操作，确保删除在整个 mesh 中一致。
// 复合主键为 (entity_type, entity_node_id, entity_id)。
type tombstoneRecord struct {
	// EntityType 实体类型标识（如 "user"），用于区分不同实体表。
	EntityType string
	// EntityNodeID 被删除实体所在的节点 ID。
	EntityNodeID int64
	// EntityID 被删除实体的本地 ID。
	EntityID int64
	// DeletedAt 删除操作的 HLC 时间戳，用于因果排序和冲突解决。
	DeletedAt clock.Timestamp
	// OriginNodeID 发起该删除操作的来源节点 ID，用于复制时的归属追踪。
	OriginNodeID int64
}

// getTombstoneTx 在事务中根据实体类型和 UserKey 查询墓碑记录。
// 第一个返回值为查询到的墓碑记录（不存在时为零值），
// 第二个返回值为 bool 标记是否存在。当数据库返回 sql.ErrNoRows 时视为不存在。
// 用于在复制事件处理时判断某个实体是否已被其他节点删除。
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

// upsertTombstoneTx 在事务中插入或更新实体墓碑记录。
// 使用 ON CONFLICT 策略确保仅当新 deleted_at_hlc 更大时才更新——这保证了
// 删除操作的因果顺序（Happened-Before），防止旧删除覆盖新删除。
// 当新 deleted_at_hlc 更大时，origin_node_id 也同步更新为新的来源节点。
// expires_at_hlc 固定为 NULL（永不过期）。
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

// applyUserDeleteTx 在事务中执行用户软删除操作。
// 核心操作：
//  1. 将用户 row 的 deleted_at_hlc 和 version_deleted 设置为删除时间戳
//  2. 在 tombstones 表中记录墓碑，用于跨节点复制删除操作
//  3. 刷新 bootstrap admin 缓存（reconcileBootstrapAdminsTx）
//
// 保护机制（不可删除的用户）：
//   - bootstrap admin（系统管理员）
//   - broadcast user（广播消息用户）
//   - node-ingress user（节点入口用户）
//
// requireActive 参数含义：
//   - true：已删除用户返回 ErrNotFound，受保护用户返回 ErrForbidden
//   - false：允许对已删除或受保护用户调用（静默忽略，仅记录 tombstone）
//
// 只在 deleted_at 发生变化（nil→set 或 version_deleted 推进）时才实际执行 UPDATE。
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

// nullIfEmpty 将空字符串（或全空格）转为 nil，用于传递给 SQL 时表示为 NULL。
// 非空字符串原样返回。常用于可选字符串字段的写入前处理。
func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

// boolToInt 将 Go 布尔值转为 SQLite 兼容的整数表示（true→1, false→0）。
// SQLite 没有原生布尔类型，使用 0/1 整数表示布尔字段。
func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// trimMessagesForUserTx 在事务中裁剪指定用户的消息，仅保留最近 messageWindowSize 条。
//
// 裁剪策略：
//  1. 按 created_at_hlc DESC（最新优先）、node_id ASC、seq DESC 综合排序
//  2. 保留前 windowSize 条，删除其余更旧的消息
//  3. 删除的 LIMIT -1 OFFSET ? 语法在 SQLite 中表示：保留前 ? 条，删除剩余所有
//
// 使用场景：每次创建新消息后调用，确保用户消息数不超过配置窗口大小。
// 裁剪的消息数会通过 recordMessageTrimTx 累加到全局消息裁剪统计。
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

// recordMessageTrimTx 在事务中记录消息裁剪的全局累计统计数据到 message_trim_stats 表。
// 使用 ON CONFLICT 合并策略：trimmed_total 累加新增裁剪数，
// last_trimmed_at_hlc 更新为当前时间。scope='global' 为固定标识。
// 当 trimmed <= 0 时直接返回 nil，不执行数据库写入。
// 由 trimMessagesForUserTx 在每次实际裁剪后调用。
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
