package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

func (s *Store) EnsureBootstrapAdmin(ctx context.Context, cfg BootstrapAdminConfig) error {
	username := strings.TrimSpace(cfg.Username)
	passwordHash := strings.TrimSpace(cfg.PasswordHash)
	if username == "" {
		return fmt.Errorf("%w: bootstrap admin username cannot be empty", ErrInvalidInput)
	}
	if passwordHash == "" {
		return fmt.Errorf("%w: bootstrap admin password hash cannot be empty", ErrInvalidInput)
	}
	s.bootstrapAdmin = BootstrapAdminConfig{Username: username, PasswordHash: passwordHash}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ensure bootstrap admin: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	key := UserKey{NodeID: s.nodeID, UserID: BootstrapAdminUserID}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete bootstrap admin tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
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
	case err != nil:
		return err
	default:
		updated := current
		changed := false
		if updated.Username != username {
			updated.Username = username
			updated.VersionUsername = now
			changed = true
		}
		if updated.DeletedAt != nil || updated.VersionDeleted != nil {
			updated.DeletedAt = nil
			updated.VersionDeleted = nil
			changed = true
		}
		if updated.Role != RoleSuperAdmin {
			updated.Role = RoleSuperAdmin
			updated.VersionRole = now
			changed = true
		}
		if !updated.SystemReserved {
			updated.SystemReserved = true
			changed = true
		}
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

func isLoginRole(role string) bool {
	return role != RoleChannel && role != RoleBroadcast && role != RoleNode
}

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

func (u User) isProtectedBootstrapAdmin() bool {
	return u.ID == BootstrapAdminUserID && u.SystemReserved
}

func (u User) isProtectedBroadcastUser() bool {
	return u.ID == BroadcastUserID && u.SystemReserved && u.Role == RoleBroadcast
}

func (u User) isProtectedNodeIngressUser() bool {
	return u.ID == NodeIngressUserID && u.SystemReserved && u.Role == RoleNode
}

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
		nodeID         int64
		role           string
		systemReserved bool
		updatedAt      clock.Timestamp
		versionRole    clock.Timestamp
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

	for _, item := range reconciliations {
		targetRole := item.role
		targetSystemReserved := item.systemReserved
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

func nextUserInvariantTimestamp(updatedAt, versionRole clock.Timestamp) clock.Timestamp {
	if versionRole.Compare(updatedAt) > 0 {
		return nextDeterministicTimestamp(versionRole)
	}
	return nextDeterministicTimestamp(updatedAt)
}

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
