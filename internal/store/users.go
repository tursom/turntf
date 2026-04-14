package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/auth"
)

func (s *Store) CreateUser(ctx context.Context, params CreateUserParams) (User, Event, error) {
	username := strings.TrimSpace(params.Username)
	if username == "" {
		return User{}, Event{}, fmt.Errorf("%w: username cannot be empty", ErrInvalidInput)
	}

	profile := strings.TrimSpace(params.Profile)
	if profile == "" {
		profile = "{}"
	}
	role, err := normalizeMutableRole(params.Role)
	if err != nil {
		return User{}, Event{}, err
	}
	passwordHash := strings.TrimSpace(params.PasswordHash)
	if isLoginRole(role) && passwordHash == "" {
		return User{}, Event{}, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
	}
	if !isLoginRole(role) {
		passwordHash = disabledPasswordHash
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("begin create user: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	userID, err := s.nextUserIDTx(ctx, tx, s.nodeID)
	if err != nil {
		return User{}, Event{}, err
	}
	user := User{
		NodeID:              s.nodeID,
		ID:                  userID,
		Username:            username,
		PasswordHash:        passwordHash,
		Profile:             profile,
		Role:                role,
		SystemReserved:      false,
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
		return User{}, Event{}, fmt.Errorf("insert user: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserCreated,
		Aggregate:       "user",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             now,
		Body:            userCreatedProtoFromUser(user),
	})
	if err != nil {
		return User{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return User{}, Event{}, fmt.Errorf("commit create user: %w", err)
	}
	s.cacheUser(user)
	return user, event, nil
}

func (s *Store) UpdateUser(ctx context.Context, params UpdateUserParams) (User, Event, error) {
	if err := params.Key.Validate(); err != nil {
		return User{}, Event{}, err
	}
	if params.Username == nil && params.PasswordHash == nil && params.Profile == nil && params.Role == nil {
		return User{}, Event{}, fmt.Errorf("%w: at least one field must be updated", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("begin update user: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getUserTx(ctx, tx, params.Key, false)
	if err != nil {
		return User{}, Event{}, err
	}
	if current.isProtectedBroadcastUser() {
		return User{}, Event{}, fmt.Errorf("%w: broadcast user cannot be updated", ErrForbidden)
	}
	if current.isProtectedNodeIngressUser() {
		return User{}, Event{}, fmt.Errorf("%w: node ingress user cannot be updated", ErrForbidden)
	}

	now := s.clock.Now()
	changed := false

	if params.Username != nil {
		if current.isProtectedBootstrapAdmin() {
			return User{}, Event{}, fmt.Errorf("%w: bootstrap admin username cannot be changed", ErrForbidden)
		}
		nextUsername := strings.TrimSpace(*params.Username)
		if nextUsername == "" {
			return User{}, Event{}, fmt.Errorf("%w: username cannot be empty", ErrInvalidInput)
		}
		if nextUsername != current.Username {
			current.Username = nextUsername
			current.VersionUsername = now
			changed = true
		}
	}

	if params.PasswordHash != nil {
		if !current.CanLogin() {
			return User{}, Event{}, fmt.Errorf("%w: channel users cannot set passwords", ErrForbidden)
		}
		nextHash := strings.TrimSpace(*params.PasswordHash)
		if nextHash == "" {
			return User{}, Event{}, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
		}
		if nextHash != current.PasswordHash {
			current.PasswordHash = nextHash
			current.VersionPasswordHash = now
			changed = true
		}
	}

	if params.Profile != nil {
		nextProfile := strings.TrimSpace(*params.Profile)
		if nextProfile == "" {
			nextProfile = "{}"
		}
		if nextProfile != current.Profile {
			current.Profile = nextProfile
			current.VersionProfile = now
			changed = true
		}
	}
	if params.Role != nil {
		if current.isProtectedBootstrapAdmin() {
			return User{}, Event{}, fmt.Errorf("%w: bootstrap admin role cannot be changed", ErrForbidden)
		}
		nextRole, err := normalizeMutableRole(*params.Role)
		if err != nil {
			return User{}, Event{}, err
		}
		if nextRole != current.Role {
			if isLoginRole(nextRole) && !isLoginRole(current.Role) && strings.TrimSpace(current.PasswordHash) == disabledPasswordHash {
				return User{}, Event{}, fmt.Errorf("%w: password hash is required before enabling login", ErrInvalidInput)
			}
			current.Role = nextRole
			current.VersionRole = now
			changed = true
		}
	}

	if !changed {
		return current, Event{}, fmt.Errorf("%w: no changes detected", ErrInvalidInput)
	}
	current.UpdatedAt = now
	current = s.applyReservedUserInvariants(current)

	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, updated_at_hlc = ?,
    version_username = ?, version_password_hash = ?, version_profile = ?, version_role = ?
WHERE node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL
`, current.Username, current.PasswordHash, current.Profile, current.Role, boolToInt(current.SystemReserved),
		current.UpdatedAt.String(), current.VersionUsername.String(), current.VersionPasswordHash.String(),
		current.VersionProfile.String(), current.VersionRole.String(), current.NodeID, current.ID); err != nil {
		return User{}, Event{}, fmt.Errorf("update user: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUpdated,
		Aggregate:       "user",
		AggregateNodeID: current.NodeID,
		AggregateID:     current.ID,
		HLC:             now,
		Body:            userUpdatedProtoFromUser(current),
	})
	if err != nil {
		return User{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return User{}, Event{}, fmt.Errorf("commit update user: %w", err)
	}
	s.cacheUser(current)
	return current, event, nil
}

func (s *Store) DeleteUser(ctx context.Context, key UserKey) (Event, error) {
	if err := key.Validate(); err != nil {
		return Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin delete user: %w", err)
	}
	defer tx.Rollback()

	user, err := s.getUserTx(ctx, tx, key, false)
	if err != nil {
		return Event{}, err
	}
	if user.isProtectedBootstrapAdmin() {
		return Event{}, fmt.Errorf("%w: bootstrap admin cannot be deleted", ErrForbidden)
	}
	if user.isProtectedBroadcastUser() {
		return Event{}, fmt.Errorf("%w: broadcast user cannot be deleted", ErrForbidden)
	}
	if user.isProtectedNodeIngressUser() {
		return Event{}, fmt.Errorf("%w: node ingress user cannot be deleted", ErrForbidden)
	}

	now := s.clock.Now()
	if err := s.applyUserDeleteTx(ctx, tx, key, now, s.nodeID, true); err != nil {
		return Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserDeleted,
		Aggregate:       "user",
		AggregateNodeID: key.NodeID,
		AggregateID:     key.UserID,
		HLC:             now,
		Body:            userDeletedProtoFromKey(key, now),
	})
	if err != nil {
		return Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit delete user: %w", err)
	}
	s.invalidateCachedUser(key)
	return event, nil
}

func (s *Store) GetUser(ctx context.Context, key UserKey) (User, error) {
	return s.userRepository.GetUser(ctx, key, false)
}

func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
WHERE deleted_at_hlc IS NULL
ORDER BY node_id ASC, user_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate users: %w", err)
	}
	return users, nil
}

func (s *Store) getUser(ctx context.Context, key UserKey, includeDeleted bool) (User, error) {
	return s.userRepository.GetUser(ctx, key, includeDeleted)
}

func (s *Store) getUserTx(ctx context.Context, tx *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	return s.userRepository.GetUserTx(ctx, tx, key, includeDeleted)
}

func (s *Store) cacheUser(user User) {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.StoreUser(user)
	}
}

func (s *Store) invalidateCachedUser(key UserKey) {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.InvalidateUser(key)
	}
}

func (s *Store) invalidateUserCache() {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.InvalidateAll()
	}
}

func (s *Store) nextUserIDTx(ctx context.Context, tx *sql.Tx, nodeID int64) (int64, error) {
	if nodeID <= 0 {
		return 0, fmt.Errorf("%w: node id is required for user sequence", ErrInvalidInput)
	}
	var userID int64
	if err := tx.QueryRowContext(ctx, `
SELECT CASE
    WHEN COALESCE(MAX(user_id), 0) < ? THEN ?
    ELSE MAX(user_id)
END + 1
FROM users
WHERE node_id = ?
`, ReservedUserIDMax, ReservedUserIDMax, nodeID).Scan(&userID); err != nil {
		return 0, fmt.Errorf("read next user id: %w", err)
	}
	return userID, nil
}

func (s *Store) AuthenticateUser(ctx context.Context, key UserKey, password string) (User, error) {
	if err := key.Validate(); err != nil {
		return User{}, err
	}
	if strings.TrimSpace(password) == "" {
		return User{}, fmt.Errorf("%w: password cannot be empty", ErrInvalidInput)
	}
	user, err := s.GetUser(ctx, key)
	if err != nil {
		return User{}, err
	}
	if !user.CanLogin() {
		return User{}, ErrNotFound
	}
	if err := auth.VerifyPassword(user.PasswordHash, password); err != nil {
		return User{}, ErrNotFound
	}
	return user, nil
}
