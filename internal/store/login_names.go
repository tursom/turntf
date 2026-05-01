package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

func normalizeLoginName(raw string) string {
	return strings.TrimSpace(raw)
}

func validateLoginNameUser(user User) error {
	if !user.CanLogin() {
		return fmt.Errorf("%w: login name owner must be a login user", ErrInvalidInput)
	}
	return nil
}

func (s *Store) GetUserLoginName(ctx context.Context, key UserKey) (string, error) {
	if err := key.Validate(); err != nil {
		return "", err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT login_name
FROM user_login_names
WHERE user_node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL
ORDER BY bound_at_hlc DESC, login_name ASC
LIMIT 1
`, key.NodeID, key.UserID)

	var loginName string
	if err := row.Scan(&loginName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("get user login name: %w", err)
	}
	return loginName, nil
}

func (s *Store) ResolveLoginName(ctx context.Context, loginName string) (UserKey, error) {
	normalized := normalizeLoginName(loginName)
	if normalized == "" {
		return UserKey{}, fmt.Errorf("%w: login_name cannot be empty", ErrInvalidInput)
	}
	row := s.db.QueryRowContext(ctx, `
SELECT user_node_id, user_id
FROM user_login_names
WHERE login_name = ? AND deleted_at_hlc IS NULL
`, normalized)

	var key UserKey
	if err := row.Scan(&key.NodeID, &key.UserID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return UserKey{}, ErrNotFound
		}
		return UserKey{}, fmt.Errorf("resolve login name: %w", err)
	}
	if err := key.Validate(); err != nil {
		return UserKey{}, err
	}
	return key, nil
}

func (s *Store) AuthenticateUserByLoginName(ctx context.Context, loginName, password string) (User, error) {
	key, err := s.ResolveLoginName(ctx, loginName)
	if err != nil {
		return User{}, err
	}
	return s.AuthenticateUser(ctx, key, password)
}

func scanUserLoginNameRaw(scanner interface {
	Scan(dest ...any) error
}) (UserLoginName, error) {
	var (
		item         UserLoginName
		boundAtRaw   string
		deletedAtRaw sql.NullString
	)
	if err := scanner.Scan(
		&item.LoginName,
		&item.User.NodeID,
		&item.User.UserID,
		&boundAtRaw,
		&deletedAtRaw,
		&item.OriginNodeID,
	); err != nil {
		return UserLoginName{}, err
	}
	boundAt, err := clock.ParseTimestamp(boundAtRaw)
	if err != nil {
		return UserLoginName{}, fmt.Errorf("parse login name bound_at: %w", err)
	}
	item.BoundAt = boundAt
	if deletedAtRaw.Valid {
		deletedAt, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return UserLoginName{}, fmt.Errorf("parse login name deleted_at: %w", err)
		}
		item.DeletedAt = &deletedAt
	}
	if item.LoginName == "" {
		return UserLoginName{}, fmt.Errorf("%w: login_name cannot be empty", ErrInvalidInput)
	}
	if err := item.User.Validate(); err != nil {
		return UserLoginName{}, err
	}
	if item.OriginNodeID <= 0 {
		return UserLoginName{}, fmt.Errorf("%w: login name origin node id is required", ErrInvalidInput)
	}
	return item, nil
}

func (s *Store) getUserLoginNameByNameTx(ctx context.Context, tx *sql.Tx, loginName string) (UserLoginName, error) {
	row := tx.QueryRowContext(ctx, `
SELECT login_name, user_node_id, user_id, bound_at_hlc, deleted_at_hlc, origin_node_id
FROM user_login_names
WHERE login_name = ?
`, loginName)
	item, err := scanUserLoginNameRaw(row)
	if errors.Is(err, sql.ErrNoRows) {
		return UserLoginName{}, ErrNotFound
	}
	if err != nil {
		return UserLoginName{}, err
	}
	return item, nil
}

func (s *Store) listActiveUserLoginNamesTx(ctx context.Context, tx *sql.Tx, key UserKey, excludeLoginName string) ([]UserLoginName, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	query := `
SELECT login_name, user_node_id, user_id, bound_at_hlc, deleted_at_hlc, origin_node_id
FROM user_login_names
WHERE user_node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL`
	args := []any{key.NodeID, key.UserID}
	if excludeLoginName != "" {
		query += ` AND login_name != ?`
		args = append(args, excludeLoginName)
	}
	query += ` ORDER BY bound_at_hlc ASC, login_name ASC`

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active user login names: %w", err)
	}
	defer rows.Close()

	items := make([]UserLoginName, 0)
	for rows.Next() {
		item, err := scanUserLoginNameRaw(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active user login names: %w", err)
	}
	return items, nil
}

func (s *Store) upsertUserLoginNameTx(ctx context.Context, tx *sql.Tx, item UserLoginName) error {
	item.LoginName = normalizeLoginName(item.LoginName)
	if item.LoginName == "" {
		return fmt.Errorf("%w: login_name cannot be empty", ErrInvalidInput)
	}
	if err := item.User.Validate(); err != nil {
		return err
	}
	if item.BoundAt == (clock.Timestamp{}) {
		return fmt.Errorf("%w: bound_at is required", ErrInvalidInput)
	}
	if item.OriginNodeID <= 0 {
		return fmt.Errorf("%w: login name origin node id is required", ErrInvalidInput)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO user_login_names(login_name, user_node_id, user_id, bound_at_hlc, deleted_at_hlc, origin_node_id)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(login_name) DO UPDATE SET
    user_node_id = CASE
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.bound_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.bound_at_hlc >= user_login_names.bound_at_hlc THEN excluded.user_node_id
        ELSE user_login_names.user_node_id
    END,
    user_id = CASE
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.bound_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.bound_at_hlc >= user_login_names.bound_at_hlc THEN excluded.user_id
        ELSE user_login_names.user_id
    END,
    bound_at_hlc = CASE
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.bound_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.bound_at_hlc > user_login_names.bound_at_hlc THEN excluded.bound_at_hlc
        ELSE user_login_names.bound_at_hlc
    END,
    deleted_at_hlc = CASE
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.bound_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.bound_at_hlc >= user_login_names.bound_at_hlc THEN excluded.deleted_at_hlc
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.deleted_at_hlc >= user_login_names.bound_at_hlc THEN excluded.deleted_at_hlc
        ELSE user_login_names.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.bound_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.bound_at_hlc >= user_login_names.bound_at_hlc THEN excluded.origin_node_id
        WHEN (
            user_login_names.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_login_names.deleted_at_hlc
        ) AND excluded.deleted_at_hlc >= user_login_names.bound_at_hlc THEN excluded.origin_node_id
        ELSE user_login_names.origin_node_id
    END
`, item.LoginName, item.User.NodeID, item.User.UserID, item.BoundAt.String(), nullableTimestampString(item.DeletedAt), item.OriginNodeID); err != nil {
		if isUniqueConstraint(err) {
			return ErrConflict
		}
		return fmt.Errorf("upsert user login name: %w", err)
	}
	return nil
}

func (s *Store) clearOtherActiveUserLoginNamesTx(ctx context.Context, tx *sql.Tx, key UserKey, excludeLoginName string, deletedAt clock.Timestamp, originNodeID int64) ([]UserLoginName, error) {
	items, err := s.listActiveUserLoginNamesTx(ctx, tx, key, excludeLoginName)
	if err != nil {
		return nil, err
	}
	cleared := make([]UserLoginName, 0, len(items))
	for _, item := range items {
		if item.BoundAt.Compare(deletedAt) > 0 {
			continue
		}
		updated := item
		updated.DeletedAt = &deletedAt
		updated.OriginNodeID = originNodeID
		if err := s.upsertUserLoginNameTx(ctx, tx, updated); err != nil {
			return nil, err
		}
		cleared = append(cleared, updated)
	}
	return cleared, nil
}

func (s *Store) clearUserLoginNamesTx(ctx context.Context, tx *sql.Tx, key UserKey, deletedAt clock.Timestamp, originNodeID int64) ([]UserLoginName, error) {
	return s.clearOtherActiveUserLoginNamesTx(ctx, tx, key, "", deletedAt, originNodeID)
}

func (s *Store) bindUserLoginNameTx(ctx context.Context, tx *sql.Tx, user User, loginName string, boundAt clock.Timestamp, originNodeID int64) ([]UserLoginName, UserLoginName, bool, error) {
	if err := validateLoginNameUser(user); err != nil {
		return nil, UserLoginName{}, false, err
	}
	normalized := normalizeLoginName(loginName)
	if normalized == "" {
		return nil, UserLoginName{}, false, fmt.Errorf("%w: login_name cannot be empty", ErrInvalidInput)
	}

	existing, err := s.getUserLoginNameByNameTx(ctx, tx, normalized)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, UserLoginName{}, false, err
	}
	if err == nil && existing.DeletedAt == nil && existing.User != user.Key() {
		return nil, UserLoginName{}, false, ErrConflict
	}

	cleared, err := s.clearOtherActiveUserLoginNamesTx(ctx, tx, user.Key(), normalized, boundAt, originNodeID)
	if err != nil {
		return nil, UserLoginName{}, false, err
	}

	if err == nil && existing.DeletedAt == nil && existing.User == user.Key() {
		return cleared, existing, len(cleared) > 0, nil
	}

	item := UserLoginName{
		LoginName:    normalized,
		User:         user.Key(),
		BoundAt:      boundAt,
		OriginNodeID: originNodeID,
	}
	if upsertErr := s.upsertUserLoginNameTx(ctx, tx, item); upsertErr != nil {
		return nil, UserLoginName{}, false, upsertErr
	}
	return cleared, item, true, nil
}
