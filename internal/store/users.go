package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/auth"
)

// CreateUser 创建用户并以事件溯源方式写入。返回创建的用户和主要事件（UserCreated）。
// 这是 CreateUserWithEvents 的便捷封装，仅返回主要用户事件。
func (s *Store) CreateUser(ctx context.Context, params CreateUserParams) (User, Event, error) {
	user, events, err := s.CreateUserWithEvents(ctx, params)
	if err != nil {
		return user, Event{}, err
	}
	return user, primaryUserEvent(events), nil
}

// CreateUserWithEvents 创建用户并返回所有生成的事件（包括登录名绑定事件）。
// 在校验参数后，在事务中分配用户 ID、插入用户行、生成事件并处理登录名绑定。
func (s *Store) CreateUserWithEvents(ctx context.Context, params CreateUserParams) (User, []Event, error) {
	username := strings.TrimSpace(params.Username)
	if username == "" {
		return User{}, nil, fmt.Errorf("%w: username cannot be empty", ErrInvalidInput)
	}
	loginName := normalizeLoginName(params.LoginName)

	profile := strings.TrimSpace(params.Profile)
	if profile == "" {
		profile = "{}"
	}
	role, err := normalizeMutableRole(params.Role)
	if err != nil {
		return User{}, nil, err
	}
	passwordHash := strings.TrimSpace(params.PasswordHash)
	if isLoginRole(role) && passwordHash == "" {
		return User{}, nil, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
	}
	if !isLoginRole(role) {
		passwordHash = disabledPasswordHash
	}
	if loginName != "" && !isLoginRole(role) {
		return User{}, nil, fmt.Errorf("%w: login_name requires a login-enabled user", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, nil, fmt.Errorf("begin create user: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	userID, err := s.nextUserIDTx(ctx, tx, s.nodeID)
	if err != nil {
		return User{}, nil, err
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
		return User{}, nil, fmt.Errorf("insert user: %w", err)
	}

	events := make([]Event, 0, 2)
	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserCreated,
		Aggregate:       "user",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             now,
		Body:            userCreatedProtoFromUser(user),
	})
	if err != nil {
		return User{}, nil, err
	}
	events = append(events, event)

	if loginName != "" {
		cleared, binding, changed, err := s.bindUserLoginNameTx(ctx, tx, user, loginName, now, s.nodeID)
		if err != nil {
			return User{}, nil, err
		}
		for _, item := range cleared {
			loginEvent, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserLoginNameDeleted,
				Aggregate:       "login_name",
				AggregateNodeID: item.User.NodeID,
				AggregateID:     item.User.UserID,
				HLC:             now,
				Body:            userLoginNameDeletedProtoFromBinding(item),
			})
			if err != nil {
				return User{}, nil, err
			}
			events = append(events, loginEvent)
		}
		if changed {
			loginEvent, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserLoginNameUpserted,
				Aggregate:       "login_name",
				AggregateNodeID: binding.User.NodeID,
				AggregateID:     binding.User.UserID,
				HLC:             now,
				Body:            userLoginNameUpsertedProtoFromBinding(binding),
			})
			if err != nil {
				return User{}, nil, err
			}
			events = append(events, loginEvent)
		}
	}

	if err := tx.Commit(); err != nil {
		return User{}, nil, fmt.Errorf("commit create user: %w", err)
	}
	s.cacheUser(user)
	return user, events, nil
}

// UpdateUser 部分更新用户。指针字段 nil 表示不修改，非 nil 表示更新为新值。
// 返回更新后的用户和主要事件。不修改受保护的系统用户。
func (s *Store) UpdateUser(ctx context.Context, params UpdateUserParams) (User, Event, error) {
	user, events, err := s.UpdateUserWithEvents(ctx, params)
	if err != nil {
		return user, Event{}, err
	}
	return user, primaryUserEvent(events), nil
}

// UpdateUserWithEvents 部分更新用户并返回所有生成的事件（包括登录名变更事件）。
func (s *Store) UpdateUserWithEvents(ctx context.Context, params UpdateUserParams) (User, []Event, error) {
	if err := params.Key.Validate(); err != nil {
		return User{}, nil, err
	}
	if params.Username == nil && params.LoginName == nil && params.PasswordHash == nil && params.Profile == nil && params.Role == nil {
		return User{}, nil, fmt.Errorf("%w: at least one field must be updated", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, nil, fmt.Errorf("begin update user: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getUserTx(ctx, tx, params.Key, false)
	if err != nil {
		return User{}, nil, err
	}
	if current.isProtectedBroadcastUser() {
		return User{}, nil, fmt.Errorf("%w: broadcast user cannot be updated", ErrForbidden)
	}
	if current.isProtectedNodeIngressUser() {
		return User{}, nil, fmt.Errorf("%w: node ingress user cannot be updated", ErrForbidden)
	}

	now := s.clock.Now()
	userChanged := false

	if params.Username != nil {
		if current.isProtectedBootstrapAdmin() {
			return User{}, nil, fmt.Errorf("%w: bootstrap admin username cannot be changed", ErrForbidden)
		}
		nextUsername := strings.TrimSpace(*params.Username)
		if nextUsername == "" {
			return User{}, nil, fmt.Errorf("%w: username cannot be empty", ErrInvalidInput)
		}
		if nextUsername != current.Username {
			current.Username = nextUsername
			current.VersionUsername = now
			userChanged = true
		}
	}

	if params.PasswordHash != nil {
		if !current.CanLogin() {
			return User{}, nil, fmt.Errorf("%w: channel users cannot set passwords", ErrForbidden)
		}
		nextHash := strings.TrimSpace(*params.PasswordHash)
		if nextHash == "" {
			return User{}, nil, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
		}
		if nextHash != current.PasswordHash {
			current.PasswordHash = nextHash
			current.VersionPasswordHash = now
			userChanged = true
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
			userChanged = true
		}
	}
	if params.Role != nil {
		if current.isProtectedBootstrapAdmin() {
			return User{}, nil, fmt.Errorf("%w: bootstrap admin role cannot be changed", ErrForbidden)
		}
		nextRole, err := normalizeMutableRole(*params.Role)
		if err != nil {
			return User{}, nil, err
		}
		if nextRole != current.Role {
			if isLoginRole(nextRole) && !isLoginRole(current.Role) && strings.TrimSpace(current.PasswordHash) == disabledPasswordHash {
				return User{}, nil, fmt.Errorf("%w: password hash is required before enabling login", ErrInvalidInput)
			}
			current.Role = nextRole
			current.VersionRole = now
			userChanged = true
		}
	}

	desiredLoginName := ""
	manageLoginName := false
	clearLoginNames := false
	if params.LoginName != nil {
		manageLoginName = true
		desiredLoginName = normalizeLoginName(*params.LoginName)
		clearLoginNames = desiredLoginName == ""
		if desiredLoginName != "" && !current.CanLogin() {
			return User{}, nil, fmt.Errorf("%w: login_name requires a login-enabled user", ErrInvalidInput)
		}
	} else if !current.CanLogin() {
		clearLoginNames = true
	}

	events := make([]Event, 0, 3)
	if userChanged {
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
			return User{}, nil, fmt.Errorf("update user: %w", err)
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
			return User{}, nil, err
		}
		events = append(events, event)
	} else {
		current = s.applyReservedUserInvariants(current)
	}

	if clearLoginNames {
		cleared, err := s.clearUserLoginNamesTx(ctx, tx, current.Key(), now, s.nodeID)
		if err != nil {
			return User{}, nil, err
		}
		for _, item := range cleared {
			loginEvent, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserLoginNameDeleted,
				Aggregate:       "login_name",
				AggregateNodeID: item.User.NodeID,
				AggregateID:     item.User.UserID,
				HLC:             now,
				Body:            userLoginNameDeletedProtoFromBinding(item),
			})
			if err != nil {
				return User{}, nil, err
			}
			events = append(events, loginEvent)
		}
	}
	if manageLoginName && desiredLoginName != "" {
		cleared, binding, changed, err := s.bindUserLoginNameTx(ctx, tx, current, desiredLoginName, now, s.nodeID)
		if err != nil {
			return User{}, nil, err
		}
		for _, item := range cleared {
			loginEvent, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserLoginNameDeleted,
				Aggregate:       "login_name",
				AggregateNodeID: item.User.NodeID,
				AggregateID:     item.User.UserID,
				HLC:             now,
				Body:            userLoginNameDeletedProtoFromBinding(item),
			})
			if err != nil {
				return User{}, nil, err
			}
			events = append(events, loginEvent)
		}
		if changed {
			loginEvent, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserLoginNameUpserted,
				Aggregate:       "login_name",
				AggregateNodeID: binding.User.NodeID,
				AggregateID:     binding.User.UserID,
				HLC:             now,
				Body:            userLoginNameUpsertedProtoFromBinding(binding),
			})
			if err != nil {
				return User{}, nil, err
			}
			events = append(events, loginEvent)
		}
	}

	if !userChanged && len(events) == 0 {
		return current, nil, fmt.Errorf("%w: no changes detected", ErrInvalidInput)
	}

	if err := tx.Commit(); err != nil {
		return User{}, nil, fmt.Errorf("commit update user: %w", err)
	}
	s.cacheUser(current)
	return current, events, nil
}

// DeleteUser 软删除用户。受保护的系统用户（bootstrap admin、broadcast、node ingress）不可删除。
func (s *Store) DeleteUser(ctx context.Context, key UserKey) (Event, error) {
	events, err := s.DeleteUserWithEvents(ctx, key)
	if err != nil {
		return Event{}, err
	}
	return primaryUserEvent(events), nil
}

// DeleteUserWithEvents 软删除用户并返回所有生成的事件（包括登录名删除事件）。
func (s *Store) DeleteUserWithEvents(ctx context.Context, key UserKey) ([]Event, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin delete user: %w", err)
	}
	defer tx.Rollback()

	user, err := s.getUserTx(ctx, tx, key, false)
	if err != nil {
		return nil, err
	}
	if user.isProtectedBootstrapAdmin() {
		return nil, fmt.Errorf("%w: bootstrap admin cannot be deleted", ErrForbidden)
	}
	if user.isProtectedBroadcastUser() {
		return nil, fmt.Errorf("%w: broadcast user cannot be deleted", ErrForbidden)
	}
	if user.isProtectedNodeIngressUser() {
		return nil, fmt.Errorf("%w: node ingress user cannot be deleted", ErrForbidden)
	}

	now := s.clock.Now()
	if err := s.applyUserDeleteTx(ctx, tx, key, now, s.nodeID, true); err != nil {
		return nil, err
	}

	events := make([]Event, 0, 2)
	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserDeleted,
		Aggregate:       "user",
		AggregateNodeID: key.NodeID,
		AggregateID:     key.UserID,
		HLC:             now,
		Body:            userDeletedProtoFromKey(key, now),
	})
	if err != nil {
		return nil, err
	}
	events = append(events, event)

	cleared, err := s.clearUserLoginNamesTx(ctx, tx, key, now, s.nodeID)
	if err != nil {
		return nil, err
	}
	for _, item := range cleared {
		loginEvent, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserLoginNameDeleted,
			Aggregate:       "login_name",
			AggregateNodeID: item.User.NodeID,
			AggregateID:     item.User.UserID,
			HLC:             now,
			Body:            userLoginNameDeletedProtoFromBinding(item),
		})
		if err != nil {
			return nil, err
		}
		events = append(events, loginEvent)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit delete user: %w", err)
	}
	s.invalidateCachedUser(key)
	return events, nil
}

// GetUser 根据 UserKey 获取活跃（非删除）用户，优先使用缓存。
func (s *Store) GetUser(ctx context.Context, key UserKey) (User, error) {
	return s.userRepository.GetUser(ctx, key, false)
}

// ListUsers 列出所有活跃用户，按 (node_id, user_id) 升序排列。
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

// getUser 获取用户，委托给 UserRepository。
func (s *Store) getUser(ctx context.Context, key UserKey, includeDeleted bool) (User, error) {
	return s.userRepository.GetUser(ctx, key, includeDeleted)
}

// getUserTx 在给定事务中获取用户。
func (s *Store) getUserTx(ctx context.Context, tx *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	return s.userRepository.GetUserTx(ctx, tx, key, includeDeleted)
}

// cacheUser 将用户写入缓存（如果 userRepository 是 cachedUserRepository）。
func (s *Store) cacheUser(user User) {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.StoreUser(user)
	}
}

// invalidateCachedUser 使指定用户的缓存失效。
func (s *Store) invalidateCachedUser(key UserKey) {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.InvalidateUser(key)
	}
}

// invalidateUserCache 使所有用户缓存失效。
func (s *Store) invalidateUserCache() {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.InvalidateAll()
	}
}

// nextUserIDTx 在事务中为给定节点分配下一个用户 ID。
// 使用 ReservedUserIDMax 作为起始偏移，确保系统预留 ID 不会被分配给普通用户。
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

// AuthenticateUser 验证用户的密码。验证失败或用户不可登录时返回 ErrNotFound。
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

// primaryUserEvent 从事件列表中过滤出第一个用户相关的事件（Created/Updated/Deleted）。
func primaryUserEvent(events []Event) Event {
	for _, event := range events {
		switch event.EventType {
		case EventTypeUserCreated, EventTypeUserUpdated, EventTypeUserDeleted:
			return event
		}
	}
	return Event{}
}
