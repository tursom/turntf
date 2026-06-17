package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
)

// scanUserMetadata 从 SQL scanner 扫描一行到 UserMetadata，解析 HLC 时间戳和过期时间。
// 扫描字段包括 owner、key、value、updated_at、deleted_at、expires_at 和 origin_node_id。
// 对 key 进行规范化，解析 HLC 时间戳和 RFC3339 过期时间。
func scanUserMetadata(scanner interface{ Scan(...any) error }) (UserMetadata, error) {
	var (
		// metadata 扫描结果的目标结构体
		metadata UserMetadata
		// updatedAtRaw 从数据库读出的 updated_at 原始字符串
		updatedAtRaw string
		// deletedAtRaw 从数据库读出的 deleted_at（可能为 NULL）
		deletedAtRaw sql.NullString
		// expiresAtRaw 从数据库读出的 expires_at（可能为 NULL）
		expiresAtRaw sql.NullString
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
	// 解析 updated_at HLC 时间戳
	metadata.UpdatedAt, err = clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return UserMetadata{}, fmt.Errorf("parse metadata updated_at: %w", err)
	}
	// deleted_at 可为 NULL，非空时解析 HLC 时间戳
	if deletedAtRaw.Valid && strings.TrimSpace(deletedAtRaw.String) != "" {
		deletedAt, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return UserMetadata{}, fmt.Errorf("parse metadata deleted_at: %w", err)
		}
		metadata.DeletedAt = &deletedAt
	}
	// expires_at 可为 NULL，非空时按 RFC3339Nano 格式解析并转为 UTC
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
