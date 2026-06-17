package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

// EnsureBootstrapAdmin 确保超级管理员用户存在且配置正确。
// 创建或修复 bootstrap admin、broadcast 和 node-ingress 三个系统用户。
// 处理多节点场景下超级管理员角色的协调（仅保留一个节点的 admin 为超级管理员）。
// 此流程在一个事务中完成：先确保引导管理员存在/修复，再绑定登录名（如有配置），
// 然后协调多节点角色、确保广播用户和节点入口用户存在，最后提交事务并使缓存失效。
func (s *Store) EnsureBootstrapAdmin(ctx context.Context, cfg BootstrapAdminConfig) error {
	// 验证并保存引导管理员配置到 Store 实例
	username := strings.TrimSpace(cfg.Username)
	passwordHash := strings.TrimSpace(cfg.PasswordHash)
	if username == "" {
		return fmt.Errorf("%w: bootstrap admin username cannot be empty", ErrInvalidInput)
	}
	if passwordHash == "" {
		return fmt.Errorf("%w: bootstrap admin password hash cannot be empty", ErrInvalidInput)
	}
	s.bootstrapAdmin = BootstrapAdminConfig{Username: username, PasswordHash: passwordHash, LoginName: strings.TrimSpace(cfg.LoginName)}

	// 启动事务，确保三个系统用户作为一个原子操作
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ensure bootstrap admin: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	key := UserKey{NodeID: s.nodeID, UserID: BootstrapAdminUserID}
	// 清除可能的 tombstone 记录，防止用户曾被删除后的残留阻止重建
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete bootstrap admin tombstone: %w", err)
	}

	var bootstrapUser User
	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
		// 用户不存在，创建新的引导管理员，初始角色为 RoleSuperAdmin
		user := User{
			NodeID:              s.nodeID,
			ID:                  BootstrapAdminUserID,
			Username:            username,
			PasswordHash:        passwordHash,
			Profile:             "{}",
			Role:                RoleSuperAdmin,
			SystemReserved:      true,
			CreatedAt:           now,
			UpdatedAt:           now,
			VersionUsername:     now,
			VersionPasswordHash: now,
			VersionProfile:      now,
			VersionRole:         now,
			OriginNodeID:        s.nodeID,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert bootstrap admin: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
			return err
		}
		bootstrapUser = user
	case err != nil:
		return err
	default:
		// 用户已存在，逐字段检查一致性，修复可能因手动修改或数据损坏导致的偏差
		updated := current
		changed := false
		// 检查用户名是否与配置一致
		if updated.Username != username {
			updated.Username = username
			updated.VersionUsername = now
			changed = true
		}
		// 检查是否处于软删除状态，若是则恢复（清除删除时间戳和版本号）
		if updated.DeletedAt != nil || updated.VersionDeleted != nil {
			updated.DeletedAt = nil
			updated.VersionDeleted = nil
			changed = true
		}
		// 检查角色是否为超级管理员
		if updated.Role != RoleSuperAdmin {
			updated.Role = RoleSuperAdmin
			updated.VersionRole = now
			changed = true
		}
		// 检查系统保留标记
		if !updated.SystemReserved {
			updated.SystemReserved = true
			changed = true
		}
		// 任一字段发生变化时，执行 UPDATE 并记录事件
		if changed {
			updated.UpdatedAt = latestUserVersion(updated)
			if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, created_at_hlc = ?, updated_at_hlc = ?,
    deleted_at_hlc = NULL, version_username = ?, version_password_hash = ?, version_profile = ?,
    version_role = ?, version_deleted = NULL, origin_node_id = ?
WHERE node_id = ? AND user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
				updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
				updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
				updated.OriginNodeID, updated.NodeID, updated.ID); err != nil {
				return fmt.Errorf("repair bootstrap admin: %w", err)
			}
			if _, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserUpdated,
				Aggregate:       "user",
				AggregateNodeID: updated.NodeID,
				AggregateID:     updated.ID,
				HLC:             updated.UpdatedAt,
				Body:            userUpdatedProtoFromUser(updated),
			}); err != nil {
				return err
			}
		}
		bootstrapUser = updated
	}

	if loginName := strings.TrimSpace(cfg.LoginName); loginName != "" {
		if _, _, _, err := s.bindUserLoginNameTx(ctx, tx, bootstrapUser, loginName, now, s.nodeID); err != nil {
			return fmt.Errorf("bind bootstrap admin login name: %w", err)
		}
	}
	if err := s.reconcileBootstrapAdminsTx(ctx, tx); err != nil {
		return err
	}
	if err := s.ensureBroadcastUserTx(ctx, tx, now); err != nil {
		return err
	}
	if err := s.ensureNodeIngressUserTx(ctx, tx, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ensure bootstrap admin: %w", err)
	}
	s.invalidateUserCache()
	return nil
}

// normalizeAnyRole 将角色字符串规范化，支持所有角色类型（包括系统保留角色）。
// 空字符串默认返回 RoleUser。仅接受预定义的几种角色值，其他返回 ErrInvalidInput。
func normalizeAnyRole(role string) (string, error) {
	normalized := strings.TrimSpace(role)
	if normalized == "" {
		return RoleUser, nil
	}
	switch normalized {
	case RoleSuperAdmin, RoleAdmin, RoleUser, RoleChannel, RoleBroadcast, RoleNode:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: unsupported role %q", ErrInvalidInput, role)
	}
}

// normalizeMutableRole 将角色字符串规范化，但排除不可通过普通 API 手动分配的角色。
// 阻止外部 API 将用户设置为超级管理员（RoleSuperAdmin）、广播频道（RoleBroadcast）
// 或节点入口（RoleNode）等系统保留角色。
func normalizeMutableRole(role string) (string, error) {
	normalized, err := normalizeAnyRole(role)
	if err != nil {
		return "", err
	}
	if normalized == RoleSuperAdmin {
		return "", fmt.Errorf("%w: role %q cannot be assigned through this API", ErrInvalidInput, role)
	}
	if normalized == RoleBroadcast {
		return "", fmt.Errorf("%w: role %q cannot be assigned through this API", ErrInvalidInput, role)
	}
	if normalized == RoleNode {
		return "", fmt.Errorf("%w: role %q cannot be assigned through this API", ErrInvalidInput, role)
	}
	return normalized, nil
}

// isLoginRole 判断该角色是否属于可登录用户（非频道/广播/节点等系统账户）。
func isLoginRole(role string) bool {
	return role != RoleChannel && role != RoleBroadcast && role != RoleNode
}

// applyReservedUserInvariants 对保留用户执行角色和密码一致性校正。
// 强制 broadcast 用户为 RoleBroadcast、node-ingress 用户为 RoleNode，
// bootstrap admin 为 RoleSuperAdmin，并确保系统保留标记正确。
// 非保留用户一律清除系统保留标记。
func (s *Store) applyReservedUserInvariants(user User) User {
	if user.ID == BroadcastUserID && user.SystemReserved {
		user.Role = RoleBroadcast
		user.SystemReserved = true
		user.PasswordHash = disabledPasswordHash
		return user
	}
	if user.ID == NodeIngressUserID && user.SystemReserved {
		user.Role = RoleNode
		user.SystemReserved = true
		user.PasswordHash = disabledPasswordHash
		return user
	}
	if user.ID != BootstrapAdminUserID || !user.SystemReserved {
		user.SystemReserved = false
		return user
	}
	user.Role = RoleSuperAdmin
	user.SystemReserved = true
	if configured := strings.TrimSpace(s.bootstrapAdmin.Username); configured != "" {
		user.Username = configured
	}
	return user
}

// isProtectedBootstrapAdmin 判断该用户是否是受保护的系统引导管理员，禁止删除。
func (u User) isProtectedBootstrapAdmin() bool {
	return u.ID == BootstrapAdminUserID && u.SystemReserved
}

// isProtectedBroadcastUser 判断该用户是否是受保护的广播系统用户，禁止删除。
func (u User) isProtectedBroadcastUser() bool {
	return u.ID == BroadcastUserID && u.SystemReserved && u.Role == RoleBroadcast
}

// isProtectedNodeIngressUser 判断该用户是否是受保护的节点入口系统用户，禁止删除。
func (u User) isProtectedNodeIngressUser() bool {
	return u.ID == NodeIngressUserID && u.SystemReserved && u.Role == RoleNode
}

// ensureBroadcastUserTx 在事务中确保 broadcast 系统用户存在且配置正确。
// broadcast 是用于接收全局广播消息的特殊系统账户，密码已禁用，不可登录。
func (s *Store) ensureBroadcastUserTx(ctx context.Context, tx *sql.Tx, now clock.Timestamp) error {
	key := UserKey{NodeID: s.nodeID, UserID: BroadcastUserID}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete broadcast user tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
		user := User{
			NodeID:              s.nodeID,
			ID:                  BroadcastUserID,
			Username:            "broadcast",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleBroadcast,
			SystemReserved:      true,
			CreatedAt:           now,
			UpdatedAt:           now,
			VersionUsername:     now,
			VersionPasswordHash: now,
			VersionProfile:      now,
			VersionRole:         now,
			OriginNodeID:        s.nodeID,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert broadcast user: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
			return err
		}
		return nil
	case err != nil:
		return err
	}

	changed := current.Username != "broadcast" ||
		current.PasswordHash != disabledPasswordHash ||
		current.Profile == "" ||
		current.Role != RoleBroadcast ||
		!current.SystemReserved ||
		current.DeletedAt != nil ||
		current.VersionDeleted != nil
	if !changed {
		return nil
	}

	updated := current
	updated.Username = "broadcast"
	updated.PasswordHash = disabledPasswordHash
	updated.Profile = defaultJSON(updated.Profile)
	updated.Role = RoleBroadcast
	updated.SystemReserved = true
	updated.DeletedAt = nil
	updated.VersionDeleted = nil
	updated.UpdatedAt = now
	updated.VersionUsername = now
	updated.VersionPasswordHash = now
	updated.VersionProfile = now
	updated.VersionRole = now
	updated.OriginNodeID = s.nodeID
	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, created_at_hlc = ?, updated_at_hlc = ?,
    deleted_at_hlc = NULL, version_username = ?, version_password_hash = ?, version_profile = ?,
    version_role = ?, version_deleted = NULL, origin_node_id = ?
WHERE node_id = ? AND user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
		updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
		updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
		updated.OriginNodeID, updated.NodeID, updated.ID); err != nil {
		return fmt.Errorf("repair broadcast user: %w", err)
	}
	if _, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUpdated,
		Aggregate:       "user",
		AggregateNodeID: updated.NodeID,
		AggregateID:     updated.ID,
		HLC:             updated.UpdatedAt,
		Body:            userUpdatedProtoFromUser(updated),
	}); err != nil {
		return err
	}
	return nil
}

// ensureNodeIngressUserTx 在事务中确保 node-ingress 系统用户存在且配置正确。
// node-ingress 是用于节点间通信的系统账户，密码已禁用，不可登录。
func (s *Store) ensureNodeIngressUserTx(ctx context.Context, tx *sql.Tx, now clock.Timestamp) error {
	key := UserKey{NodeID: s.nodeID, UserID: NodeIngressUserID}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete node ingress user tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
		user := User{
			NodeID:              s.nodeID,
			ID:                  NodeIngressUserID,
			Username:            "node",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleNode,
			SystemReserved:      true,
			CreatedAt:           now,
			UpdatedAt:           now,
			VersionUsername:     now,
			VersionPasswordHash: now,
			VersionProfile:      now,
			VersionRole:         now,
			OriginNodeID:        s.nodeID,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert node ingress user: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
			return err
		}
		return nil
	case err != nil:
		return err
	}

	changed := current.Username != "node" ||
		current.PasswordHash != disabledPasswordHash ||
		current.Profile == "" ||
		current.Role != RoleNode ||
		!current.SystemReserved ||
		current.DeletedAt != nil ||
		current.VersionDeleted != nil
	if !changed {
		return nil
	}

	updated := current
	updated.Username = "node"
	updated.PasswordHash = disabledPasswordHash
	updated.Profile = defaultJSON(updated.Profile)
	updated.Role = RoleNode
	updated.SystemReserved = true
	updated.DeletedAt = nil
	updated.VersionDeleted = nil
	updated.UpdatedAt = now
	updated.VersionUsername = now
	updated.VersionPasswordHash = now
	updated.VersionProfile = now
	updated.VersionRole = now
	updated.OriginNodeID = s.nodeID
	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, created_at_hlc = ?, updated_at_hlc = ?,
    deleted_at_hlc = NULL, version_username = ?, version_password_hash = ?, version_profile = ?,
    version_role = ?, version_deleted = NULL, origin_node_id = ?
WHERE node_id = ? AND user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
		updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
		updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
		updated.OriginNodeID, updated.NodeID, updated.ID); err != nil {
		return fmt.Errorf("repair node ingress user: %w", err)
	}
	if _, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUpdated,
		Aggregate:       "user",
		AggregateNodeID: updated.NodeID,
		AggregateID:     updated.ID,
		HLC:             updated.UpdatedAt,
		Body:            userUpdatedProtoFromUser(updated),
	}); err != nil {
		return err
	}
	return nil
}

// reconcileBootstrapAdminsTx 在事务中协调多节点场景下的超级管理员角色。
// 规则：所有 user_id 为 BootstrapAdminUserID 的用户中，仅 node_id 最小的节点持有 RoleSuperAdmin，
// 其他节点的引导管理员降级为 RoleUser，同时清除系统保留标记。
func (s *Store) reconcileBootstrapAdminsTx(ctx context.Context, tx *sql.Tx) error {
	var minNodeID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT MIN(node_id)
FROM users
WHERE user_id = ? AND deleted_at_hlc IS NULL
`, BootstrapAdminUserID).Scan(&minNodeID); err != nil {
		return fmt.Errorf("find bootstrap admin owner: %w", err)
	}
	if !minNodeID.Valid {
		return nil
	}

	rows, err := tx.QueryContext(ctx, `
SELECT node_id, role, system_reserved, updated_at_hlc, version_role
FROM users
WHERE user_id = ? AND deleted_at_hlc IS NULL
  AND ((node_id = ? AND (role != ? OR system_reserved != 1))
       OR (node_id != ? AND (role = ? OR system_reserved != 0)))
ORDER BY node_id ASC
`, BootstrapAdminUserID, minNodeID.Int64, RoleSuperAdmin, minNodeID.Int64, RoleSuperAdmin)
	if err != nil {
		return fmt.Errorf("query bootstrap admins for reconciliation: %w", err)
	}
	defer rows.Close()

	type bootstrapAdminReconciliation struct {
		// nodeID 节点 ID
		nodeID int64
		// role 当前角色
		role string
		// systemReserved 是否标记为系统保留
		systemReserved bool
		// updatedAt updated_at HLC 时间戳
		updatedAt clock.Timestamp
		// versionRole version_role HLC 时间戳
		versionRole clock.Timestamp
	}
	reconciliations := make([]bootstrapAdminReconciliation, 0)
	for rows.Next() {
		var item bootstrapAdminReconciliation
		var systemReserved int
		var updatedAtRaw, versionRoleRaw string
		if err := rows.Scan(&item.nodeID, &item.role, &systemReserved, &updatedAtRaw, &versionRoleRaw); err != nil {
			return fmt.Errorf("scan bootstrap admin for reconciliation: %w", err)
		}
		item.systemReserved = systemReserved != 0
		item.updatedAt, err = clock.ParseTimestamp(updatedAtRaw)
		if err != nil {
			return fmt.Errorf("parse bootstrap admin updated_at: %w", err)
		}
		item.versionRole, err = clock.ParseTimestamp(versionRoleRaw)
		if err != nil {
			return fmt.Errorf("parse bootstrap admin version_role: %w", err)
		}
		reconciliations = append(reconciliations, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate bootstrap admins for reconciliation: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close bootstrap admin reconciliation rows: %w", err)
	}

	// 遍历需要协调的引导管理员，确保仅最小 node_id 的节点持有超级管理员角色
	for _, item := range reconciliations {
		targetRole := item.role
		targetSystemReserved := item.systemReserved
		// 最小 node_id 的节点应持有 RoleSuperAdmin，其他节点降级
		if item.nodeID == minNodeID.Int64 {
			targetRole = RoleSuperAdmin
			targetSystemReserved = true
		} else {
			if item.role == RoleSuperAdmin {
				targetRole = RoleUser
			}
			targetSystemReserved = false
		}

		roleChanged := targetRole != item.role
		updatedAt := nextUserInvariantTimestamp(item.updatedAt, item.versionRole)
		versionRole := item.versionRole
		if roleChanged {
			versionRole = updatedAt
		}

		if _, err := tx.ExecContext(ctx, `
UPDATE users
SET role = ?, system_reserved = ?, updated_at_hlc = ?, version_role = ?
WHERE node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL
`, targetRole, boolToInt(targetSystemReserved), updatedAt.String(), versionRole.String(), item.nodeID, BootstrapAdminUserID); err != nil {
			return fmt.Errorf("reconcile bootstrap admin %d: %w", item.nodeID, err)
		}
	}
	return nil
}

// nextUserInvariantTimestamp 在 updatedAt 和 versionRole 中取较大值，生成下一个确定性 HLC 时间戳。
// 用于版本冲突时生成单调递增的时间戳，防止版本号回退。
func nextUserInvariantTimestamp(updatedAt, versionRole clock.Timestamp) clock.Timestamp {
	if versionRole.Compare(updatedAt) > 0 {
		return nextDeterministicTimestamp(versionRole)
	}
	return nextDeterministicTimestamp(updatedAt)
}

// nextDeterministicTimestamp 基于给定的 HLC 时间戳生成下一个确定性时间戳。
// 逻辑计数器递增，溢出时进位到 WallTimeMs。
func nextDeterministicTimestamp(base clock.Timestamp) clock.Timestamp {
	next := base
	if next.Logical == ^uint16(0) {
		next.WallTimeMs++
		next.Logical = 0
		return next
	}
	next.Logical++
	return next
}
