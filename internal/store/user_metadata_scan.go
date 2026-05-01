package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
)

// scanUserMetadata 从 SQL scanner 扫描一行到 UserMetadata，解析 HLC 时间戳和过期时间。
func scanUserMetadata(scanner interface{ Scan(...any) error }) (UserMetadata, error) {
	var (
		metadata      UserMetadata
		updatedAtRaw  string
		deletedAtRaw  sql.NullString
		expiresAtRaw  sql.NullString
	)
	if err := scanner.Scan(
		&metadata.Owner.NodeID,
		&metadata.Owner.UserID,
		&metadata.Key,
		&metadata.Value,
		&updatedAtRaw,
		&deletedAtRaw,
		&expiresAtRaw,
		&metadata.OriginNodeID,
	); err != nil {
		return UserMetadata{}, err
	}

	key, err := NormalizeUserMetadataKey(metadata.Key)
	if err != nil {
		return UserMetadata{}, err
	}
	metadata.Key = key
	metadata.UpdatedAt, err = clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return UserMetadata{}, fmt.Errorf("parse metadata updated_at: %w", err)
	}
	if deletedAtRaw.Valid && strings.TrimSpace(deletedAtRaw.String) != "" {
		deletedAt, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return UserMetadata{}, fmt.Errorf("parse metadata deleted_at: %w", err)
		}
		metadata.DeletedAt = &deletedAt
	}
	if expiresAtRaw.Valid && strings.TrimSpace(expiresAtRaw.String) != "" {
		expiresAt, err := time.Parse(time.RFC3339Nano, expiresAtRaw.String)
		if err != nil {
			return UserMetadata{}, fmt.Errorf("parse metadata expires_at: %w", err)
		}
		expiresAt = expiresAt.UTC()
		metadata.ExpiresAt = &expiresAt
	}
	return metadata, nil
}
