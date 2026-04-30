package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
)

const (
	userMetadataKeyMaxLength      = 128
	defaultUserMetadataScanLimit  = 100
	maxUserMetadataScanLimit      = 1000
	userMetadataExpiresAtFormat   = "2006-01-02T15:04:05.000000000Z07:00"
)

func NormalizeUserMetadataKey(raw string) (string, error) {
	return normalizeUserMetadataKeyFragment(raw, "key", false)
}

func normalizeUserMetadataKeyFragment(raw, field string, allowEmpty bool) (string, error) {
	if raw == "" {
		if allowEmpty {
			return "", nil
		}
		return "", fmt.Errorf("%w: %s cannot be empty", ErrInvalidInput, field)
	}
	if len(raw) > userMetadataKeyMaxLength {
		return "", fmt.Errorf("%w: %s exceeds %d characters", ErrInvalidInput, field, userMetadataKeyMaxLength)
	}
	for _, ch := range raw {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '.', ch == '_', ch == ':', ch == '-':
		default:
			return "", fmt.Errorf("%w: %s contains unsupported character %q", ErrInvalidInput, field, ch)
		}
	}
	return raw, nil
}

func normalizeUserMetadataScanLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultUserMetadataScanLimit, nil
	}
	if limit < 0 {
		return 0, fmt.Errorf("%w: limit must be positive", ErrInvalidInput)
	}
	if limit > maxUserMetadataScanLimit {
		return 0, fmt.Errorf("%w: limit cannot exceed %d", ErrInvalidInput, maxUserMetadataScanLimit)
	}
	return limit, nil
}

func ParseUserMetadataExpiresAt(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: expires_at must be valid RFC3339", ErrInvalidInput)
	}
	expiresAt = expiresAt.UTC()
	return &expiresAt, nil
}

func normalizedUserMetadataExpiresAt(expiresAt *time.Time) *time.Time {
	if expiresAt == nil {
		return nil
	}
	normalized := expiresAt.UTC()
	return &normalized
}

func FormatUserMetadataExpiresAt(expiresAt time.Time) string {
	return expiresAt.UTC().Format(userMetadataExpiresAtFormat)
}

func nullableUserMetadataExpiresAt(expiresAt *time.Time) any {
	if expiresAt == nil {
		return nil
	}
	return FormatUserMetadataExpiresAt(*expiresAt)
}

func currentUserMetadataWallTime(clk *clock.Clock) time.Time {
	return time.UnixMilli(clk.WallTimeMs()).UTC()
}

func currentUserMetadataExpiresAtBoundary(clk *clock.Clock) string {
	return FormatUserMetadataExpiresAt(currentUserMetadataWallTime(clk))
}

func userMetadataPrefixUpperBound(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	bytes := []byte(prefix)
	for idx := len(bytes) - 1; idx >= 0; idx-- {
		if bytes[idx] == 0xff {
			continue
		}
		next := make([]byte, idx+1)
		copy(next, bytes[:idx+1])
		next[idx]++
		return string(next), true
	}
	return "", false
}

func validateUserMetadataOwner(user User) error {
	if !user.CanLogin() {
		return fmt.Errorf("%w: metadata owner must be a login user", ErrInvalidInput)
	}
	return nil
}

func (s *Store) validateUserMetadataOwner(ctx context.Context, owner UserKey) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	user, err := s.GetUser(ctx, owner)
	if err != nil {
		return err
	}
	return validateUserMetadataOwner(user)
}

func (s *Store) validateUserMetadataOwnerTx(ctx context.Context, tx *sql.Tx, owner UserKey) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	user, err := s.getUserByIDTx(ctx, tx, owner, false)
	if err != nil {
		return err
	}
	return validateUserMetadataOwner(user)
}

func (s *Store) UpsertUserMetadata(ctx context.Context, params UpsertUserMetadataParams) (UserMetadata, Event, error) {
	if err := params.Owner.Validate(); err != nil {
		return UserMetadata{}, Event{}, err
	}
	key, err := NormalizeUserMetadataKey(params.Key)
	if err != nil {
		return UserMetadata{}, Event{}, err
	}
	expiresAt := normalizedUserMetadataExpiresAt(params.ExpiresAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return UserMetadata{}, Event{}, fmt.Errorf("begin upsert user metadata: %w", err)
	}
	defer tx.Rollback()

	if err := s.validateUserMetadataOwnerTx(ctx, tx, params.Owner); err != nil {
		return UserMetadata{}, Event{}, err
	}

	now := s.clock.Now()
	metadata := UserMetadata{
		Owner:        params.Owner,
		Key:          key,
		Value:        append([]byte(nil), params.Value...),
		UpdatedAt:    now,
		ExpiresAt:    expiresAt,
		OriginNodeID: s.nodeID,
	}
	if err := s.upsertUserMetadataTx(ctx, tx, metadata); err != nil {
		return UserMetadata{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserMetadataUpserted,
		Aggregate:       "user_metadata",
		AggregateNodeID: params.Owner.NodeID,
		AggregateID:     params.Owner.UserID,
		HLC:             now,
		Body:            userMetadataUpsertedProtoFromUserMetadata(metadata),
	})
	if err != nil {
		return UserMetadata{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return UserMetadata{}, Event{}, fmt.Errorf("commit upsert user metadata: %w", err)
	}
	return metadata, event, nil
}

func (s *Store) GetUserMetadata(ctx context.Context, owner UserKey, key string) (UserMetadata, error) {
	if err := s.validateUserMetadataOwner(ctx, owner); err != nil {
		return UserMetadata{}, err
	}
	key, err := NormalizeUserMetadataKey(key)
	if err != nil {
		return UserMetadata{}, err
	}
	return s.getVisibleUserMetadata(ctx, owner, key)
}

func (s *Store) DeleteUserMetadata(ctx context.Context, params DeleteUserMetadataParams) (UserMetadata, Event, error) {
	if err := params.Owner.Validate(); err != nil {
		return UserMetadata{}, Event{}, err
	}
	key, err := NormalizeUserMetadataKey(params.Key)
	if err != nil {
		return UserMetadata{}, Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return UserMetadata{}, Event{}, fmt.Errorf("begin delete user metadata: %w", err)
	}
	defer tx.Rollback()

	if err := s.validateUserMetadataOwnerTx(ctx, tx, params.Owner); err != nil {
		return UserMetadata{}, Event{}, err
	}
	current, err := s.getVisibleUserMetadataTx(ctx, tx, params.Owner, key)
	if err != nil {
		return UserMetadata{}, Event{}, err
	}

	now := s.clock.Now()
	current.DeletedAt = &now
	current.OriginNodeID = s.nodeID
	if err := s.upsertUserMetadataTx(ctx, tx, current); err != nil {
		return UserMetadata{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserMetadataDeleted,
		Aggregate:       "user_metadata",
		AggregateNodeID: params.Owner.NodeID,
		AggregateID:     params.Owner.UserID,
		HLC:             now,
		Body:            userMetadataDeletedProtoFromUserMetadata(current),
	})
	if err != nil {
		return UserMetadata{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return UserMetadata{}, Event{}, fmt.Errorf("commit delete user metadata: %w", err)
	}
	return current, event, nil
}

func (s *Store) ScanUserMetadata(ctx context.Context, params ScanUserMetadataParams) (UserMetadataScanResult, error) {
	if err := s.validateUserMetadataOwner(ctx, params.Owner); err != nil {
		return UserMetadataScanResult{}, err
	}
	prefix, err := normalizeUserMetadataKeyFragment(params.Prefix, "prefix", true)
	if err != nil {
		return UserMetadataScanResult{}, err
	}
	after, err := normalizeUserMetadataKeyFragment(params.After, "after", true)
	if err != nil {
		return UserMetadataScanResult{}, err
	}
	limit, err := normalizeUserMetadataScanLimit(params.Limit)
	if err != nil {
		return UserMetadataScanResult{}, err
	}
	if prefix != "" && after != "" && !strings.HasPrefix(after, prefix) {
		return UserMetadataScanResult{}, fmt.Errorf("%w: after must use the same prefix", ErrInvalidInput)
	}

	query := `
SELECT owner_node_id, owner_user_id, key, value, updated_at_hlc, deleted_at_hlc, expires_at, origin_node_id
FROM user_metadata
WHERE owner_node_id = ? AND owner_user_id = ?
  AND deleted_at_hlc IS NULL
  AND (expires_at IS NULL OR expires_at > ?)`
	args := []any{params.Owner.NodeID, params.Owner.UserID, currentUserMetadataExpiresAtBoundary(s.clock)}
	if prefix != "" {
		query += ` AND key >= ?`
		args = append(args, prefix)
		if upper, ok := userMetadataPrefixUpperBound(prefix); ok {
			query += ` AND key < ?`
			args = append(args, upper)
		}
	}
	if after != "" {
		query += ` AND key > ?`
		args = append(args, after)
	}
	query += ` ORDER BY key ASC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return UserMetadataScanResult{}, fmt.Errorf("scan user metadata: %w", err)
	}
	defer rows.Close()

	items := make([]UserMetadata, 0, limit+1)
	for rows.Next() {
		item, err := scanUserMetadata(rows)
		if err != nil {
			return UserMetadataScanResult{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return UserMetadataScanResult{}, fmt.Errorf("iterate user metadata: %w", err)
	}

	result := UserMetadataScanResult{}
	if len(items) > limit {
		result.NextAfter = items[limit-1].Key
		items = items[:limit]
	}
	result.Items = items
	return result, nil
}

func (s *Store) getVisibleUserMetadata(ctx context.Context, owner UserKey, key string) (UserMetadata, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT owner_node_id, owner_user_id, key, value, updated_at_hlc, deleted_at_hlc, expires_at, origin_node_id
FROM user_metadata
WHERE owner_node_id = ? AND owner_user_id = ? AND key = ?
  AND deleted_at_hlc IS NULL
  AND (expires_at IS NULL OR expires_at > ?)
`, owner.NodeID, owner.UserID, key, currentUserMetadataExpiresAtBoundary(s.clock))
	metadata, err := scanUserMetadata(row)
	if err == sql.ErrNoRows {
		return UserMetadata{}, ErrNotFound
	}
	if err != nil {
		return UserMetadata{}, fmt.Errorf("get user metadata: %w", err)
	}
	return metadata, nil
}

func (s *Store) getVisibleUserMetadataTx(ctx context.Context, tx *sql.Tx, owner UserKey, key string) (UserMetadata, error) {
	row := tx.QueryRowContext(ctx, `
SELECT owner_node_id, owner_user_id, key, value, updated_at_hlc, deleted_at_hlc, expires_at, origin_node_id
FROM user_metadata
WHERE owner_node_id = ? AND owner_user_id = ? AND key = ?
  AND deleted_at_hlc IS NULL
  AND (expires_at IS NULL OR expires_at > ?)
`, owner.NodeID, owner.UserID, key, currentUserMetadataExpiresAtBoundary(s.clock))
	metadata, err := scanUserMetadata(row)
	if err == sql.ErrNoRows {
		return UserMetadata{}, ErrNotFound
	}
	if err != nil {
		return UserMetadata{}, fmt.Errorf("get user metadata: %w", err)
	}
	return metadata, nil
}

func (s *Store) upsertUserMetadataTx(ctx context.Context, tx *sql.Tx, metadata UserMetadata) error {
	if err := metadata.Owner.Validate(); err != nil {
		return err
	}
	key, err := NormalizeUserMetadataKey(metadata.Key)
	if err != nil {
		return err
	}
	metadata.Key = key
	metadata.ExpiresAt = normalizedUserMetadataExpiresAt(metadata.ExpiresAt)
	if metadata.OriginNodeID <= 0 {
		return fmt.Errorf("%w: metadata origin node id is required", ErrInvalidInput)
	}
	if metadata.DeletedAt == nil && metadata.UpdatedAt == (clock.Timestamp{}) {
		return fmt.Errorf("%w: metadata updated_at is required", ErrInvalidInput)
	}
	if metadata.DeletedAt != nil && metadata.UpdatedAt == (clock.Timestamp{}) {
		metadata.UpdatedAt = *metadata.DeletedAt
	}
	if metadata.Value == nil {
		metadata.Value = []byte{}
	}

	deletedAt := nullableTimestampString(metadata.DeletedAt)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO user_metadata(
    owner_node_id, owner_user_id, key, value, updated_at_hlc, deleted_at_hlc, expires_at, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(owner_node_id, owner_user_id, key) DO UPDATE SET
    value = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.updated_at_hlc > user_metadata.deleted_at_hlc
        ) AND excluded.updated_at_hlc >= user_metadata.updated_at_hlc THEN excluded.value
        ELSE user_metadata.value
    END,
    updated_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.updated_at_hlc > user_metadata.deleted_at_hlc
        ) AND excluded.updated_at_hlc > user_metadata.updated_at_hlc THEN excluded.updated_at_hlc
        ELSE user_metadata.updated_at_hlc
    END,
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.updated_at_hlc > user_metadata.deleted_at_hlc
        ) THEN NULL
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_metadata.deleted_at_hlc
        ) AND excluded.deleted_at_hlc >= user_metadata.updated_at_hlc THEN excluded.deleted_at_hlc
        ELSE user_metadata.deleted_at_hlc
    END,
    expires_at = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.updated_at_hlc > user_metadata.deleted_at_hlc
        ) AND excluded.updated_at_hlc >= user_metadata.updated_at_hlc THEN excluded.expires_at
        ELSE user_metadata.expires_at
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.updated_at_hlc > user_metadata.deleted_at_hlc
        ) AND excluded.updated_at_hlc >= user_metadata.updated_at_hlc THEN excluded.origin_node_id
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            user_metadata.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_metadata.deleted_at_hlc
        ) THEN excluded.origin_node_id
        ELSE user_metadata.origin_node_id
    END
`, metadata.Owner.NodeID, metadata.Owner.UserID, metadata.Key, metadata.Value,
		metadata.UpdatedAt.String(), deletedAt, nullableUserMetadataExpiresAt(metadata.ExpiresAt), metadata.OriginNodeID); err != nil {
		return fmt.Errorf("upsert user metadata: %w", err)
	}
	return nil
}
