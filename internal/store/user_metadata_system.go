package store

import (
	"context"
	"fmt"
	"strings"
)

const (
	// UserMetadataSystemKeyPrefix 是系统保留 metadata key 的命名空间前缀。
	UserMetadataSystemKeyPrefix = "system."
	// UserMetadataKeyVisibleToOthers 控制用户是否会出现在其他普通用户的可见列表中。
	UserMetadataKeyVisibleToOthers = "system.visible_to_others"
)

type userMetadataValueKind string

const (
	userMetadataValueKindBool userMetadataValueKind = "bool"
)

type userMetadataSystemSpec struct {
	valueKind userMetadataValueKind
	allowTTL  bool
}

var userMetadataSystemSpecs = map[string]userMetadataSystemSpec{
	UserMetadataKeyVisibleToOthers: {
		valueKind: userMetadataValueKindBool,
		allowTTL:  false,
	},
}

func isSystemUserMetadataKey(key string) bool {
	return strings.HasPrefix(key, UserMetadataSystemKeyPrefix)
}

func lookupUserMetadataSystemSpec(key string) (userMetadataSystemSpec, bool) {
	spec, ok := userMetadataSystemSpecs[key]
	return spec, ok
}

func validateUserMetadataKeyPolicy(key string) error {
	if !isSystemUserMetadataKey(key) {
		return nil
	}
	if _, ok := lookupUserMetadataSystemSpec(key); ok {
		return nil
	}
	return fmt.Errorf("%w: unsupported system metadata key %q", ErrInvalidInput, key)
}

func parseUserMetadataBoolValue(raw []byte) (bool, error) {
	switch strings.TrimSpace(string(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%w: metadata value must be boolean", ErrInvalidInput)
	}
}

func userMetadataVisibleToOthers(raw []byte) bool {
	value, err := parseUserMetadataBoolValue(raw)
	if err != nil {
		// 不能稳定解释时按默认可见处理，避免脏数据把用户永久隐藏。
		return true
	}
	return value
}

func validateUserMetadataPolicy(metadata UserMetadata) error {
	if err := validateUserMetadataKeyPolicy(metadata.Key); err != nil {
		return err
	}
	spec, ok := lookupUserMetadataSystemSpec(metadata.Key)
	if !ok || metadata.DeletedAt != nil {
		return nil
	}
	if metadata.ExpiresAt != nil && !spec.allowTTL {
		return fmt.Errorf("%w: metadata key %q does not allow expires_at", ErrInvalidInput, metadata.Key)
	}
	switch spec.valueKind {
	case userMetadataValueKindBool:
		if _, err := parseUserMetadataBoolValue(metadata.Value); err != nil {
			return fmt.Errorf("%w: metadata key %q requires a boolean value", ErrInvalidInput, metadata.Key)
		}
	}
	return nil
}

func registeredSystemMetadataPrefix(prefix string) bool {
	for key := range userMetadataSystemSpecs {
		if strings.HasPrefix(key, prefix) || strings.HasPrefix(prefix, key) {
			return true
		}
	}
	return false
}

func validateUserMetadataScanSystemPrefix(prefix, after string) error {
	for _, item := range []struct {
		value string
		name  string
	}{
		{value: prefix, name: "prefix"},
		{value: after, name: "after"},
	} {
		if !isSystemUserMetadataKey(item.value) {
			continue
		}
		if registeredSystemMetadataPrefix(item.value) {
			continue
		}
		return fmt.Errorf("%w: unsupported %s %q", ErrInvalidInput, item.name, item.value)
	}
	return nil
}

func (s *Store) listUsersHiddenFromOthers(ctx context.Context, candidates []User) (map[UserKey]struct{}, error) {
	hidden := make(map[UserKey]struct{})
	if len(candidates) == 0 {
		return hidden, nil
	}

	query := strings.Builder{}
	query.WriteString(`
SELECT owner_node_id, owner_user_id, value
FROM user_metadata
WHERE key = ?
  AND deleted_at_hlc IS NULL
  AND (expires_at IS NULL OR expires_at > ?)
  AND (`)

	args := make([]any, 0, 2+len(candidates)*2)
	args = append(args, UserMetadataKeyVisibleToOthers, currentUserMetadataExpiresAtBoundary(s.clock))
	first := true
	for _, candidate := range candidates {
		if candidate.SystemReserved {
			continue
		}
		if !first {
			query.WriteString(` OR `)
		}
		first = false
		query.WriteString(`(owner_node_id = ? AND owner_user_id = ?)`)
		args = append(args, candidate.NodeID, candidate.ID)
	}
	if first {
		return hidden, nil
	}
	query.WriteString(`)`)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list hidden users metadata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key UserKey
		var value []byte
		if err := rows.Scan(&key.NodeID, &key.UserID, &value); err != nil {
			return nil, fmt.Errorf("scan hidden users metadata: %w", err)
		}
		if !userMetadataVisibleToOthers(value) {
			hidden[key] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hidden users metadata: %w", err)
	}
	return hidden, nil
}
